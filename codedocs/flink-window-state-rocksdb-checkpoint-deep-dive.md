# Flink Window State in RocksDB: State Storage, S3 Checkpointing, and Sliding Window Internals

A deep dive into the Flink source code covering three topics:
1. How window functions save state in RocksDB
2. How RocksDB uploads data files to S3 at each checkpoint
3. How sliding windows update state (with a concrete example)

---

## Architecture Overview

Three subsystems collaborate to make a windowed RocksDB pipeline work: the **window operator** (decides which window an event belongs to and when it fires), the **keyed state backend** (stores per-(key, window) data in RocksDB), and the **snapshot strategy** (periodically uploads RocksDB SST files to durable storage). The diagram below shows the runtime relationship between them.

```
                                ┌─────────────────────────────────────────────┐
   user code:                   │  StreamTask  (one per subtask)              │
   stream.keyBy(..)             │  ┌─────────────────────────────────────┐    │
        .window(SlidingEvent..) │  │  WindowOperator<K,IN,ACC,OUT,W>     │    │
        .reduce(..)             │  │  ─ processElement(StreamRecord)     │    │
                                │  │  ─ onEventTime(InternalTimer)       │    │
                                │  │  ─ onProcessingTime(InternalTimer)  │    │
                                │  │                                     │    │
                                │  │     windowState  ──► add / get / clear
                                │  │     trigger      ──► FIRE / PURGE / CONTINUE
                                │  └────────┬────────────────────────────┘    │
                                │           │ uses                            │
                                │  ┌────────▼────────────────────────────┐    │
                                │  │  RocksDBKeyedStateBackend           │    │
                                │  │   ─ holds RocksDB instance (1/task) │    │
                                │  │   ─ creates per-state-desc CFs      │    │
                                │  │   ─ snapshot(checkpointId, ...)     │    │
                                │  └────────┬────────────────────────────┘    │
                                │           │                                 │
                                │  ┌────────▼────────────────────────────┐    │
                                │  │  RocksIncrementalSnapshotStrategy   │◀──CheckpointCoordinator (JM)
                                │  │   ─ syncPrepareResources()  [sync]  │   triggers via barrier
                                │  │   ─ asyncSnapshot()         [async] │
                                │  │   ─ uses RocksDBStateUploader       │
                                │  └────────┬────────────────────────────┘    │
                                └───────────┼─────────────────────────────────┘
                                            │
                                            ▼
                                  ┌──────────────────────┐         ┌──────────────────────┐
                                  │ local RocksDB dir    │ hard    │ remote DFS (S3)      │
                                  │  /tmp/<uid>/*.sst    │ links   │  shared/    (SSTs)   │
                                  │  /tmp/chk-N/*.sst    │────────▶│  exclusive/ (MANIFEST│
                                  └──────────────────────┘ upload  │             CURRENT) │
                                                                    └──────────────────────┘
```

### Class hierarchy

The state classes that windows use share a common base, so the same `clear()` / namespace handling is reused for every state type.

```
                State (interface)                          Triggerable<K,W> (interface)
                       ▲                                            ▲
                       │                                            │
        InternalKvState<K,N,V> (interface)                AbstractUdfStreamOperator
                       ▲                                            ▲
                       │                                            │
              AbstractRocksDBState<K,N,V>                  WindowOperator<K,IN,ACC,OUT,W>
              ─ setCurrentNamespace                                │ holds
              ─ clear()  → db.delete(cf, ws, compositeKey)         │   windowState : AppendingState<IN,ACC>
              ─ serializeCurrentKeyWithGroupAndNamespace           │   trigger     : Trigger<IN,W>
                       ▲                                           │   windowAssigner : WindowAssigner<IN,W>
                       │                                           ▼
              ┌────────┴────────────────────────┐              uses one of the three
              │                                 │              concrete states below
   RocksDBListState         AbstractRocksDBAppendingState
   (InternalListState)                ▲
   ─ add() → db.merge       ┌─────────┴──────────┐
                            │                    │
                  RocksDBReducingState   RocksDBAggregatingState
                  (InternalReducingState) (InternalAggregatingState)
                  ─ add() → get + reduce ─ add() → get + aggFn.add
                            + put                  + put
```

Key facts encoded in this hierarchy:

- **`clear()` lives on `AbstractRocksDBState` (line 113-121).** Every concrete state type inherits the same `db.delete(columnFamily, writeOptions, compositeKey)` implementation — the window operator only ever calls `windowState.clear()` and never thinks about RocksDB directly.
- **`ListState` is the odd one out.** It uses RocksDB's `merge` operator (delimiter-appended bytes via `StringAppendOperator`) so `add()` is a single write. Reducing and aggregating both go through `AbstractRocksDBAppendingState` which does a read-modify-write per call.
- **The window itself is the *namespace*.** `AbstractRocksDBState.setCurrentNamespace(N)` sets the suffix bytes for the next composite key; for windowed operators `N == TimeWindow`.

### Memtable → SST → checkpoint lifecycle

Every `windowState.add(value)` call ends in RocksDB's in-RAM memtable. The path from memtable to S3 is what makes "incremental" checkpoints fast.

```
   Flink                       RocksDB (per task, per column family)         Local FS                  Remote DFS (S3)

   windowState.add(elem)
        │
        ▼
   db.merge / db.put ──────▶  ┌───────────────┐
                              │  Memtable     │  (in-RAM skiplist;
                              │  (active)     │   no WAL by default)
                              └──────┬────────┘
                                     │ memtable full → swap & flush
                                     ▼
                              ┌───────────────┐
                              │ Immutable mt  │
                              └──────┬────────┘
                                     │ background flush thread
                                     ▼              writes
                              ┌───────────────┐ ─────────▶ /tmp/<jobUID>/000042.sst   (live SST)
                              │ SST file on   │
                              │ disk (L0..Ln) │
                              └───────────────┘
                                                            │
                                                            │ Checkpoint N triggered
                                                            │ Checkpoint.create(db)
                                                            │   .createCheckpoint(...)
                                                            ▼
                                                  /tmp/chk-N/000042.sst              (hard link)
                                                            │
                                                            │ async upload — only NEW SSTs
                                                            ▼
                                                                                 ─────▶ s3://…/shared/000042.sst
                                                                                 ─────▶ s3://…/exclusive/MANIFEST-…
                                                                                 ─────▶ s3://…/exclusive/CURRENT
```

The hard link is the trick: even if RocksDB compacts and deletes the original SST, the link under `/tmp/chk-N/` keeps the inode alive until the async upload finishes.

---

## Operator-to-RocksDB Mapping: Subtasks, DBs, and Column Families

A common mental-model question is *"does each stateful operator get one RocksDB table?"* — and the answer needs unpacking because RocksDB doesn't have tables, it has **Column Families**. The actual mapping is one level finer than "one operator → one container":

> **One subtask of one stateful keyed operator → ONE RocksDB instance (one on-disk DB).
> Each state descriptor registered by that operator → ONE Column Family inside that DB.**

### Hierarchy from job to bytes on disk

```
Flink Job
  └── multiple parallel subtasks per operator
        └── each (operator, subtaskIndex) runs in one Task on a TaskManager
              └── if the operator is stateful & keyed:
                    ───────────────────────────────────────────────────────
                       ONE  RocksDBKeyedStateBackend  ◄── per subtask
                       ONE  RocksDB instance (one DB)
                       ONE  on-disk directory  /<tm-tmp>/<jobUID>/<uid>/db
                    ───────────────────────────────────────────────────────
                          │
                          │  contains
                          ▼
                    ┌────────────────────────────────────────────────────┐
                    │  RocksDB database                                  │
                    │                                                    │
                    │   Column Family "default"        (unused by Flink) │
                    │   Column Family "window-contents"      ◄── one CF  │
                    │   Column Family "user-count"           ◄── per     │
                    │   Column Family "session-state"        ◄── state   │
                    │   Column Family "_timer_state_…"       ◄── desc    │
                    │   …                                                │
                    │                                                    │
                    │  All CFs share the same DB, write-buffer manager,  │
                    │  block cache, MANIFEST, and SST files on disk.     │
                    └────────────────────────────────────────────────────┘
                          │
                          │  inside each Column Family
                          ▼
                    ┌────────────────────────────────────────────────────┐
                    │  Composite-key entries:                            │
                    │  [keyGroupPrefix][userKey][namespace] → bytes      │
                    │                                                    │
                    │  For window state, "namespace" = the TimeWindow.   │
                    └────────────────────────────────────────────────────┘
```

### Where this is encoded in the code

- **One RocksDB instance per subtask of a stateful operator.** The backend holds a single DB field:

  ```java
  /**
   * Our RocksDB database, this is used by the actual subclasses of {@link AbstractRocksDBState}
   * to store state. The different k/v states that we have don't each have their own RocksDB
   * instance. They all write to this instance but to their own column family.
   */
  protected final RocksDB db;
  ```
  *— `RocksDBKeyedStateBackend.java:263-268`*

- **One Column Family per state descriptor name.** The backend maintains a `LinkedHashMap` keyed by the state name (`RocksDBKeyedStateBackend.java:227`):

  ```java
  private final LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation;
  ```

  When user code calls `getReducingState(...)`, `getListState(...)`, etc., `tryRegisterKvStateInformation(...)` either reuses an existing `RocksDbKvStateInfo` or creates a new one (`RocksDBKeyedStateBackend.java:750-824`). A new one ultimately calls `RocksDBOperationUtils.createColumnFamilyDescriptor(...)` (`RocksDBOperationUtils.java:197-225`), and the **CF name on disk is literally the bytes of the state-descriptor name**:

  ```java
  byte[] nameBytes = metaInfoBase.getName().getBytes(ConfigConstants.DEFAULT_CHARSET);
  // → e.g. "window-contents", "user-count", …
  ```
  *— `RocksDBOperationUtils.java:203`*

- **Chained operators do NOT share a DB.** Two stateful operators chained into the same Task each create their own `RocksDBKeyedStateBackend` during `initializeState()`, so they each open a separate RocksDB instance under a different `instanceBasePath`. The `localDirectoryName` is derived from `backendUID` (`RocksDBSnapshotStrategyBase.java:141`), making each backend's snapshot directory unique even when chained.

### Concrete example: what the §1/§3 window operator's DB looks like

For the worked example used throughout this doc:

```
   User code                                              Result on disk
   ─────────                                              ──────────────
   stream
     .keyBy(e -> e.userId)
     .window(SlidingEventTimeWindows.of(60s, 20s))        One RocksDBKeyedStateBackend per
     .reduce((a, b) -> ...)                               subtask of the WindowOperator
                                                          ─────────────────────────────────
                                                          /tmp/<job>/<uid>/db/
                                                             ├── CURRENT
                                                             ├── MANIFEST-…
                                                             ├── 000042.sst        one DB,
                                                             ├── 000043.sst        one set
                                                             └── …                 of SSTs
       internally registers the descriptor                 ─────────────────────────────────
       "window-contents" (ReducingStateDescriptor —        CFs in that DB:
       see WindowOperatorBuilder.java:178-182)                default                (unused)
                                                              window-contents        (used by
                                                                                      this op)
                                                              _timer_state/event…    (timers,
                                                                                      if RocksDB
                                                                                      priority-
                                                                                      queue
                                                                                      factory is
                                                                                      enabled)
```

Every `(userId, TimeWindow{...})` row you saw in §3 lives in the **same** `window-contents` Column Family of the **same** RocksDB instance. If the same operator added a second stateful field — say `.process(...)` reading a `ValueState<Long> seenCount` — that descriptor would get its own Column Family (e.g. `"seenCount"`) in the *same* DB.

### What else shares the DB

Within one operator subtask, the DB typically holds:

| Column Family | Source | Notes |
|---|---|---|
| One CF per **keyed state descriptor** | every `getValueState/getListState/getReducingState/getAggregatingState/getMapState` call | The CF name is the descriptor name. |
| One CF per **internal timer service** | `RocksDBPriorityQueueSetFactory` when `taskmanager.state.rocksdb.timer-service.factory: rocksdb` | With the default `HEAP` factory, timers stay in JVM heap and never touch RocksDB. |
| `default` CF | created by RocksDB itself on open | Unused by Flink, kept open only so it can be closed cleanly (`RocksDBKeyedStateBackend.java:234-240`). |

### What does NOT go in RocksDB

- **Operator state** (non-keyed; used by sources/sinks for offsets, by buffers for union/list state) lives in `DefaultOperatorStateBackend`, which is heap-based with on-checkpoint serialization. So in a job like `source → keyBy → window`, the source's Kafka/Kinesis offsets and the window operator's keyed state are in **two completely different backends**.
- **Broadcast state** also uses operator state, not RocksDB.
- **In-flight network buffers, accumulators, watermark state** — none of these are in RocksDB.

### Implications you can derive from this mapping

1. **Memory is shared across CFs of one DB.** RocksDB's write buffer manager, block cache, and arena pool are sized **per RocksDB instance**, so all CFs of one operator subtask compete for the same pool. Tuning options like `state.backend.rocksdb.memory.managed: true` partition memory at the *DB* level, not at the state-descriptor level.
2. **Each checkpoint produces one `IncrementalRemoteKeyedStateHandle` per backend per subtask.** Because the DB is the unit of snapshot (one native checkpoint, one set of SST hard links), the JM receives exactly one handle per (operator, subtaskIndex) — not one per state descriptor. Inside the handle, the SST files cover all CFs simultaneously (the LSM tree doesn't separate them at the file level).
3. **Removing a state descriptor doesn't shrink an SST.** When the operator's `kvStateInformation` no longer contains a descriptor, its CF can be dropped on restore, but historical SSTs in S3 that contain rows from that CF stay until those SSTs are compacted away and the new compacted versions are uploaded.
4. **Two stateful operators in the same job pipeline ≠ one RocksDB.** Even chained on the same Task, they have separate DBs, separate directories, and separate checkpoint handles. The only "sharing" between them is the TaskManager process itself.

---

## 1. How Window Functions Save State in RocksDB

### State Descriptor Creation

When you define a window operation (e.g., `.reduce()`, `.aggregate()`, `.apply()`), Flink creates a corresponding state descriptor in `WindowOperatorBuilder`:

- **ReduceFunction** -> `ReducingStateDescriptor` (stores a single pre-reduced value per window)
- **AggregateFunction** -> `AggregatingStateDescriptor` (stores an accumulator per window)
- **ProcessWindowFunction** -> `ListStateDescriptor` (stores all elements in a list per window)

All use the state name `"window-contents"`.

**Source:** `WindowOperatorBuilder.java:178-182` (reduce), `285-289` (aggregate), `398-399` (list)

### RocksDB Key Structure

Each state entry in RocksDB uses a **composite key** built by `SerializedCompositeKeyBuilder` (`SerializedCompositeKeyBuilder.java:120-128`, `184-214`). The byte-level layout is:

```
┌────────────────────┬──────────────────────┬───────────────────────────────────┐
│  key-group prefix  │    serialized key    │   serialized namespace            │
│   1 or 2 bytes     │       N bytes        │        M bytes                    │
│                    │                      │                                   │
│  written by        │  written by          │  written by                       │
│  CompositeKey-     │  user-supplied       │  namespaceSerializer              │
│  SerializationUtils│  keySerializer       │  (TimeWindow.Serializer           │
│  .writeKeyGroup    │                      │   for windowed ops)               │
└────────────────────┴──────────────────────┴───────────────────────────────────┘
        ▲                       ▲                        ▲
   keyGroupPrefixBytes      "userA"                  TimeWindow{start,end}
   1 byte if maxParallelism                          (the window IS the
   ≤ 128, else 2 bytes                                namespace)
```

Concretely, for `maxParallelism=128`, key `"userA"`, window `[1000, 2000)` the bytes are roughly:

```
[0x02]["userA"][0x00..00 0x00..03E8  0x00..00 0x00..07D0]
 ▲      ▲                 ▲                   ▲
 KG=2   utf-encoded key   long start=1000      long end=2000
```

Notes:

- **keyGroupPrefix** — determines which key group (and thus which subtask) owns this key. Used for key-group-based partitioning and range scans during rescaling.
- **serializedKey** — the user's keyed-stream key (e.g., a word in word-count).
- **serializedNamespace** — the **window** itself. For windowed operators, the namespace serializer is `TimeWindow.Serializer` (8 bytes start + 8 bytes end).
- **Disambiguating prefix** — if the namespace serializer could produce ambiguous boundaries (`isAmbiguousCompositeKeyPossible == true`), a variable-int length of the key segment is inserted between key and namespace to ensure prefix scans remain unambiguous (`SerializedCompositeKeyBuilder.java:205-212`).

### Step-by-Step: `WindowOperator.processElement`

The `WindowOperator.processElement()` method (`WindowOperator.java:292-447`) is the entry point for every keyed event. The non-merging path (sliding, tumbling, global windows) is shown below in execution order.

```
   StreamRecord arrives
          │
          ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │  1. windowAssigner.assignWindows(value, ts, ctx)        line 294-296     │
   │     For SlidingEventTimeWindows: returns size/slide TimeWindows.         │
   ├──────────────────────────────────────────────────────────────────────────┤
   │  2. key = getKeyedStateBackend().getCurrentKey()        line 301         │
   │     Captured for use in triggerContext below.                            │
   ├──────────────────────────────────────────────────────────────────────────┤
   │  3. Branch on windowAssigner type                       line 303         │
   │     ─ MergingWindowAssigner (session)   → merging branch (304-403)       │
   │     ─ otherwise (tumbling/sliding/...)  → non-merging branch (404-432)   │
   ├──────────────────────────────────────────────────────────────────────────┤
   │  4. For each assigned window:                                            │
   │       a. isWindowLate(window)?  drop and continue.       line 408-410    │
   │       b. windowState.setCurrentNamespace(window)         line 413        │
   │          → AbstractRocksDBState stores N as the namespace                │
   │            suffix for subsequent composite-key builds.                   │
   │       c. windowState.add(value)                          line 414        │
   │          → triggers the per-state-type RocksDB op (see table below).    │
   │       d. triggerContext.onElement(...)                   line 419        │
   │          → delegates to Trigger#onElement; EventTimeTrigger              │
   │            registers a timer at window.maxTimestamp().                   │
   │       e. if (triggerResult.isFire()) {                                   │
   │              ACC contents = windowState.get();           line 422        │
   │              emitWindowContents(window, contents); }     line 424        │
   │       f. if (triggerResult.isPurge()) windowState.clear()  line 428-430  │
   │          → AbstractRocksDBState.clear() runs db.delete(cf, ws, ck).      │
   │       g. registerCleanupTimer(window)                    line 431        │
   │          → event-time timer at cleanupTime = max + allowedLateness.      │
   ├──────────────────────────────────────────────────────────────────────────┤
   │  5. if (isSkippedElement && isElementLate(element))      line 440-446    │
   │       → emit to side-output OR drop and inc numLateRecordsDropped.       │
   └──────────────────────────────────────────────────────────────────────────┘
```

The merging branch (session windows) inserts steps that may merge overlapping in-flight windows via `MergingWindowSet.addWindow(...)` and consolidate their state with `windowMergingState.mergeNamespaces(stateWindowResult, mergedStateWindows)` (line 363-364). Sliding windows never take this path.

### How `windowState.add` Translates to RocksDB

The actual RocksDB write depends on which `StateDescriptor` was created in step 4c:

| State Type | RocksDB Operation | What's Stored |
|---|---|---|
| `RocksDBListState.add()` (line 125-133) | `backend.db.merge(cf, writeOptions, key, value)` | Appends element via `StringAppendOperator` (delimiter-separated bytes) |
| `RocksDBReducingState.add()` (line 92-98) | `db.get()` → `reduceFunction.reduce(old, new)` → `db.put()` | Single pre-reduced value |
| `RocksDBAggregatingState.add()` (line 99-105) | `db.get()` → `aggFunction.add(value, acc)` → `db.put()` | Single accumulator |

The composite key passed to RocksDB is the one assembled by `SerializedCompositeKeyBuilder` shown in the previous section: `[keyGroupPrefix][serializedKey][serializedNamespace=TimeWindow]`.

Key detail: **all writes go straight into RocksDB's memtable** — there is no Flink-level WAL; durability is handled by checkpoints (see §2). When the memtable fills, RocksDB's background flush thread writes it out as a new SST file on local disk.

### Window Firing and State Cleanup

For event-time windows, two timers are registered per `(key, window)` pair:

| Timer | Registered by | Fires at | Effect |
|---|---|---|---|
| **fire timer** | `EventTimeTrigger.onElement` (line 44) | `window.maxTimestamp()` | `Trigger.onEventTime` returns `FIRE` |
| **cleanup timer** | `WindowOperator.registerCleanupTimer` (line 631-643) | `window.maxTimestamp() + allowedLateness` | `clearAllState` deletes the entry |

When `allowedLateness == 0` (the default) both timestamps coincide, the timer service deduplicates, and a single `onEventTime` invocation does both jobs — the doc's sliding example below assumes this case. With `allowedLateness > 0`, the fire happens first, late events can still update the window state, and the cleanup runs later.

The sequence inside `WindowOperator.onEventTime` (line 449-494):

1. `triggerContext.window = timer.getNamespace()` — recover the window from the timer's namespace.
2. `windowState.setCurrentNamespace(triggerContext.window)` (line 468) — point the composite-key builder at this window.
3. `triggerResult = triggerContext.onEventTime(timer.getTimestamp())` — delegate to the user trigger.
4. If `triggerResult.isFire()`: `windowState.get()` (RocksDB `get`) → `emitWindowContents(...)`.
5. If `triggerResult.isPurge()`: `windowState.clear()` (RocksDB `delete`).
6. If `isCleanupTime(window, timer.getTimestamp())` (line 485-488): `clearAllState(window, windowState, mergingWindows)` which calls `windowState.clear()` plus trigger / process-context cleanup (line 560-571).

All paths that delete window state eventually call `AbstractRocksDBState.clear()` (line 113-121), which issues `backend.db.delete(columnFamily, writeOptions, compositeKey)`.

**Source:** `WindowOperator.java:449-541` (onEventTime/onProcessingTime handlers), `AbstractRocksDBState.java:113-121` (clear).

---

## 2. How RocksDB Uploads Data Files to S3 at Each Checkpoint

### End-to-End Checkpoint Flow

A checkpoint touches six components. The sequence diagram below shows the full path from `CheckpointCoordinator` triggering checkpoint N to the JobManager committing it. Step numbers in the diagram are referenced by the detail table that follows.

```
Actors:
   JM = JobManager (CheckpointCoordinator)
   ST = StreamTask
   BE = RocksDBKeyedStateBackend
   SS = RocksIncrementalSnapshotStrategy
   UP = RocksDBStateUploader
   S3 = S3 / DFS  (checkpoint storage)

═══════════════════════════════════════════════════════════════════════════
  PHASE 1 — SYNC   (operator is blocked)
═══════════════════════════════════════════════════════════════════════════

  [1]  JM ──► ST     triggerCheckpointAsync(N)
  [2]  ST            inject CheckpointBarrier(N) into output buffers
  [3]  ST ──► BE     snapshot(N, ts, streamFactory, options)
  [4]  BE            writeBatchWrapper.flush()
  [5]  BE ──► SS     SnapshotStrategyRunner.snapshot(...)
  [6]  SS            syncPrepareResources(N)
                       ├─ prepareLocalSnapshotDirectory(N)    → /tmp/<uid>/chk-N
                       ├─ snapshotMetaData(N)                 → PreviousSnapshot
                       └─ takeDBNativeCheckpoint(snapshotDir)
                            ├─ ResourceGuard.Lease acquire
                            └─ Checkpoint.create(db)
                               .createCheckpoint(/tmp/chk-N)  → HARD LINKS .sst
  [7]  SS ──► BE ──► ST    returns RunnableFuture<SnapshotResult>

────────────────── BARRIER PIVOT ──────────────────────────────────────────
        operator resumes processing input records concurrently
        with the upload that runs in the snapshot executor thread
───────────────────────────────────────────────────────────────────────────

═══════════════════════════════════════════════════════════════════════════
  PHASE 2 — ASYNC  (background snapshot thread)
═══════════════════════════════════════════════════════════════════════════

  [8]  SS            RocksDBIncrementalSnapshotOperation.get()
  [9]  SS            materializeMetaData()
  [10] SS            createUploadFilePaths(/tmp/chk-N/*)
                       ├─ .sst AND previously uploaded → REUSE existing handle
                       └─ otherwise                    → schedule for upload
  [11] SS ──► UP     uploadFilesToCheckpointFs(newSstPaths, SHARED)
  [12] UP ──► S3     parallel PUTs (one future per file):
                         shared/000042.sst
                         shared/000043.sst
                         exclusive/MANIFEST-N
                         exclusive/CURRENT
  [13] SS            uploadedSstFiles.put(N, sstFiles)
  [14] SS            new IncrementalRemoteKeyedStateHandle(
                         backendUID, keyGroupRange, N,
                         sstFiles, miscFiles, metaHandle, size)
  [15] SS ──► BE ──► ST    returns SnapshotResult<KeyedStateHandle>
  [16] ST ──► JM     AcknowledgeCheckpoint(N, handle)

═══════════════════════════════════════════════════════════════════════════
  PHASE 3 — COMMIT
═══════════════════════════════════════════════════════════════════════════

  [17] JM            aggregates acks from all subtasks, writes _metadata
  [18] JM ──► BE ──► SS    notifyCheckpointComplete(N)
                       ├─ guard: id > lastCompletedCheckpointId
                       │         && uploadedSstFiles.containsKey(id)
                       │         (skip savepoints — FLINK-23949)
                       ├─ drop uploadedSstFiles where id < N
                       └─ lastCompletedCheckpointId = N
```

**Step-by-step detail.** Step numbers below match the `autonumber` markers in the diagram above.

| # | Phase | Component | Method | What happens | Source |
|---|---|---|---|---|---|
| 1 | trigger | JM → StreamTask | `triggerCheckpointAsync(N)` | CheckpointCoordinator RPCs the source subtasks to start checkpoint N. | `CheckpointCoordinator` |
| 2 | trigger | StreamTask | inject `CheckpointBarrier(N)` | Barrier is written into outgoing record buffers so downstream operators see ckp N in-band. | `SubtaskCheckpointCoordinator` |
| 3 | sync entry | StreamTask → backend | `snapshot(N, ts, streamFactory, options)` | Called once per keyed state backend on the task. | `RocksDBKeyedStateBackend.java:682-698` |
| 4 | sync | RocksDBKeyedStateBackend | `writeBatchWrapper.flush()` | Drains pending writes into the memtable so the native checkpoint sees the latest state. | `RocksDBKeyedStateBackend.java:690` |
| 5 | sync | RocksDBKeyedStateBackend → strategy | `SnapshotStrategyRunner.snapshot(...)` | Wraps the strategy as a `RunnableFuture`; runs the sync part inline, schedules async part on the `ASYNCHRONOUS` executor. | `SnapshotStrategyRunner` |
| 6 | sync | strategy | `syncPrepareResources(N)` | Prepares the local snapshot dir, computes `PreviousSnapshot` from `uploadedSstFiles.tailMap(lastCompleted)`, and calls `takeDBNativeCheckpoint`. | `RocksDBSnapshotStrategyBase.java:150-165` |
| 6a | sync | strategy | `prepareLocalSnapshotDirectory(N)` | Creates `/tmp/<jobUID>/chk-N` (or the task-local recovery path). | `RocksDBSnapshotStrategyBase.java:186-229` |
| 6b | sync | strategy | `snapshotMetaData(N)` | Snapshots all `kvStateInformation` state-meta-info; builds a `PreviousSnapshot` from the last completed checkpoint's SST handles. | `RocksIncrementalSnapshotStrategy.java:195-222` |
| 6c | sync | strategy | `takeDBNativeCheckpoint(dir)` | Acquires `ResourceGuard.Lease` (blocks DB close); `Checkpoint.create(db).createCheckpoint(dir)` HARD-LINKS the live `.sst` files into `/tmp/chk-N/`. | `RocksDBSnapshotStrategyBase.java:170-184` |
| 7 | sync→async pivot | strategy → backend → task | returns `SnapshotResultSupplier` (a `Future`) | The sync work is done; the operator can resume. The supplier closure carries the snapshot dir + previous snapshot. | `RocksIncrementalSnapshotStrategy.java:124-164` |
| — | barrier | StreamTask | barrier forwarded downstream | Operator resumes processing input records concurrently with the upload. | – |
| 8 | async entry | snapshot thread | `RocksDBIncrementalSnapshotOperation.get()` | Runs the async portion on the `ASYNCHRONOUS` executor. | `RocksIncrementalSnapshotStrategy.java:252-333` |
| 9 | async | strategy | `materializeMetaData(...)` | Serializes the state-meta-info into the checkpoint storage as a small EXCLUSIVE stream. | `RocksDBSnapshotOperation.materializeMetaData` |
| 10 | async | strategy | `createUploadFilePaths(files, ...)` | For each file in `/tmp/chk-N/`: if `.sst` AND `previousSnapshot.getUploaded(name).isPresent() && couldReuseStateHandle` → REUSE the existing handle; otherwise → schedule for upload. Non-`.sst` files (MANIFEST, CURRENT, OPTIONS-…) always upload. | `RocksIncrementalSnapshotStrategy.java:413-433` |
| 11 | async | strategy → uploader | `uploadFilesToCheckpointFs(newSstPaths, factory, SHARED/EXCLUSIVE)` | Submits one upload future per file to the transfer executor; `FutureUtils.waitForAll(...)` blocks until they all finish. | `RocksDBStateUploader.java:71-106` |
| 12 | async | uploader | `uploadLocalFileToCheckpointFs(path, ...)` | Opens local `InputStream`, asks the factory for a `CheckpointStateOutputStream`, copies in **16 KB** chunks, then `closeAndGetHandle()` returns the `StreamStateHandle` (S3 path + length). | `RocksDBStateUploader.java:130-181` |
| 13 | async | strategy | `uploadedSstFiles.put(N, sstFiles)` | Remembers which SST handles belong to ckp N so future incremental ckps can reuse them. Guarded by `synchronized (uploadedSstFiles)`. | `RocksIncrementalSnapshotStrategy.java:393-408` |
| 14 | async | strategy | `new IncrementalRemoteKeyedStateHandle(...)` | Bundles `sstFiles` (shared) + `miscFiles` (exclusive) + `metaStateHandle` into the handle reported to the JM. | `RocksIncrementalSnapshotStrategy.java:300-308` |
| 15 | async | strategy → backend → task | returns `SnapshotResult<KeyedStateHandle>` | Completes the future that StreamTask is waiting on. | – |
| 16 | ack | StreamTask → JM | `AcknowledgeCheckpoint(N, handle)` | RPC back to the CheckpointCoordinator. | `RpcCheckpointResponder` |
| 17 | commit | JM | aggregates all subtask acks, commits ckp N | JM writes the `_metadata` file and marks N as the latest completed checkpoint. | `CheckpointCoordinator` |
| 18 | commit | JM → backend → strategy | `notifyCheckpointComplete(N)` | Guards against savepoint ids (FLINK-23949); drops `uploadedSstFiles.keySet() < N`; sets `lastCompletedCheckpointId = N`. | `RocksIncrementalSnapshotStrategy.java:166-180` |

**The pivot point is the SYNC/ASYNC boundary at step 7.** As soon as `syncPrepareResources` returns, the operator can keep processing new records while the hard-linked snapshot directory is uploaded in the background. This is what makes Flink's RocksDB checkpoints "incremental" *and* "asynchronous": only NEW SSTs since the last completed checkpoint cross the network, and the operator never blocks on the network at all.

### Overview: Two Phases (Sync + Async)

Flink's RocksDB checkpoint has a **synchronous preparation** phase (blocks the operator) and an **asynchronous upload** phase (runs in a background thread while the operator continues processing).

### Phase 1: Synchronous - Create Local Native Checkpoint

**`RocksDBSnapshotStrategyBase.syncPrepareResources()`** (line 151-165):

```java
// 1. Prepare local snapshot directory
SnapshotDirectory snapshotDirectory = prepareLocalSnapshotDirectory(checkpointId);
// 2. Snapshot metadata (state descriptors, etc.)
PreviousSnapshot previousSnapshot = snapshotMetaData(checkpointId, stateMetaInfoSnapshots);
// 3. Create RocksDB native checkpoint (hard links!)
takeDBNativeCheckpoint(snapshotDirectory);
```

**`takeDBNativeCheckpoint()`** (line 170-184):
```java
try (ResourceGuard.Lease ignored = rocksDBResourceGuard.acquireResource();
     Checkpoint checkpoint = Checkpoint.create(db)) {
    checkpoint.createCheckpoint(outputDirectory.getDirectory().toString());
}
```

This calls RocksDB's native `Checkpoint` API which creates **hard links** to the current live SST files. This is very fast (no data copy), and the hard links ensure the SST files survive even if RocksDB compacts and deletes the originals.

The `ResourceGuard.Lease` is important: it prevents the `RocksDBKeyedStateBackend` from closing the underlying `RocksDB` instance (e.g., on task cancellation) while the native checkpoint is being created. Without the lease, a concurrent close could yank the database out from under `createCheckpoint(...)`.

### Phase 2: Asynchronous - Upload SST Files to S3

**`RocksDBIncrementalSnapshotOperation.get()`** (line 252-333) runs in a background thread:

#### Step 1: Determine Which Files Need Uploading

**`createUploadFilePaths()`** (line 413-433):

```java
for (Path filePath : files) {
    String fileName = filePath.getFileName().toString();
    if (fileName.endsWith(".sst")) {
        Optional<StreamStateHandle> uploaded = previousSnapshot.getUploaded(fileName);
        if (uploaded.isPresent() && checkpointStreamFactory.couldReuseStateHandle(uploaded.get())) {
            sstFiles.add(HandleAndLocalPath.of(uploaded.get(), fileName));  // REUSE - skip upload
        } else {
            sstFilePaths.add(filePath);  // NEW - needs upload
        }
    } else {
        miscFilePaths.add(filePath);  // CURRENT, OPTIONS, MANIFEST etc - always upload
    }
}
```

**Key insight for incremental checkpoints:** SST files in RocksDB are **immutable** once written. If an SST file name (e.g., `000042.sst`) was already uploaded in a previous checkpoint, Flink reuses the existing S3 handle rather than re-uploading.

The `PreviousSnapshot` object tracks all SST files from the last completed checkpoint via the `uploadedSstFiles` TreeMap (`RocksIncrementalSnapshotStrategy.java:85`).

#### Step 2: Upload New Files in Parallel

**`RocksDBStateUploader.uploadFilesToCheckpointFs()`** (line 71-106):

```java
// Creates parallel upload futures - one per file
List<CompletableFuture<HandleAndLocalPath>> futures = createUploadFutures(files, ...);
FutureUtils.waitForAll(futures).get();  // Wait for all uploads
```

Each individual file upload (`uploadLocalFileToCheckpointFs()`, line 130-181):
```java
// 1. Open local file as InputStream
inputStream = Files.newInputStream(filePath);
// 2. Create output stream to checkpoint storage (S3)
outputStream = checkpointStreamFactory.createCheckpointStateOutputStream(stateScope);
// 3. Copy in 16KB chunks
while ((numBytes = inputStream.read(buffer)) != -1) {
    outputStream.write(buffer, 0, numBytes);
}
// 4. Close and get handle (S3 path reference)
StreamStateHandle result = outputStream.closeAndGetHandle();
return HandleAndLocalPath.of(result, filePath.getFileName().toString());
```

The `CheckpointStreamFactory` (typically `FsCheckpointStreamFactory`) creates streams that write to the configured checkpoint directory (e.g., `s3://bucket/checkpoints/chk-42/`).

**State scopes:**
- `SHARED`: SST files in incremental checkpoints - stored in a shared directory, can be referenced by multiple checkpoints
- `EXCLUSIVE`: Misc files (MANIFEST, CURRENT, OPTIONS) + all files in full checkpoints - deleted when checkpoint is discarded

#### Step 3: Build State Handle and Record Uploaded Files

```java
// Line 300-308: Create the state handle reported to JobManager
IncrementalRemoteKeyedStateHandle jmHandle = new IncrementalRemoteKeyedStateHandle(
    backendUID, keyGroupRange, checkpointId,
    sstFiles,    // shared state (SST files - reusable)
    miscFiles,   // private state (MANIFEST etc - exclusive)
    metaStateHandle, checkpointedSize);

// Line 393-398: Remember uploaded files for next checkpoint
uploadedSstFiles.put(checkpointId, Collections.unmodifiableList(sstFiles));
```

#### Step 4: Checkpoint Completion

When JobManager confirms the checkpoint (`notifyCheckpointComplete()`, line 167-180):
```java
synchronized (uploadedSstFiles) {
    // FLINK-23949: skip if the notified id is a savepoint (not in our map),
    // otherwise the next checkpoint would degenerate into a full checkpoint.
    if (completedCheckpointId > lastCompletedCheckpointId
            && uploadedSstFiles.containsKey(completedCheckpointId)) {
        uploadedSstFiles
                .keySet()
                .removeIf(id -> id < completedCheckpointId);
        lastCompletedCheckpointId = completedCheckpointId;
    }
}
```

The savepoint guard matters: when JM completes a savepoint, the savepoint id does not appear in `uploadedSstFiles` (savepoints don't share SSTs incrementally). Without the guard, the strategy would clear its incremental history and the next regular checkpoint would be forced to re-upload every SST as if from scratch.

The strategy also handles aborted checkpoints (`notifyCheckpointAborted`, line 183-187):
```java
synchronized (uploadedSstFiles) {
    uploadedSstFiles.keySet().remove(abortedCheckpointId);  // discard tracking for this id
}
```

### Visual Summary

```
Checkpoint N triggered
  |
  v
[SYNC] RocksDB Checkpoint.create() -> hard links SST files to local /tmp/chk-N/
  |
  v  (operator resumes processing)
  |
[ASYNC] Compare /tmp/chk-N/*.sst against previousSnapshot
  |
  |-- 000001.sst: in previousSnapshot -> REUSE (no upload)
  |-- 000002.sst: in previousSnapshot -> REUSE (no upload)
  |-- 000005.sst: NEW file            -> UPLOAD to s3://bucket/shared/000005.sst
  |-- 000007.sst: NEW file            -> UPLOAD to s3://bucket/shared/000007.sst
  |-- MANIFEST:   always              -> UPLOAD to s3://bucket/exclusive/MANIFEST
  |-- CURRENT:    always              -> UPLOAD to s3://bucket/exclusive/CURRENT
  |
  v
Report IncrementalRemoteKeyedStateHandle to JobManager
  |
  v
[JM confirms] -> notifyCheckpointComplete(N) -> update uploadedSstFiles tracking
```

---

## 3. Sliding Windows: How Flink Updates State (Concrete Example)

### Per-Element Write Path: Tumbling vs Sliding

Before diving into the multi-event sliding example, it helps to see the per-element path side by side with a tumbling window. The two paths share the same code (`WindowOperator.processElement`), the same state class (`RocksDBReducingState`), and the same composite key layout — the only difference is **how many windows `assignWindows` returns**, which multiplies every downstream RocksDB op by `N = size / slide` for sliding.

#### Figure 1 — Tumbling window: one element, one window, one RocksDB entry

```
USER CODE
─────────
stream
    .keyBy(e -> e.userId)                                                ── KeySelector
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(60)))         ── WindowAssigner  (size=60s)
    .reduce((a, b) -> new Event(a.userId, a.value + b.value))            ── ReduceFunction


ELEMENT ARRIVES  ─── (userA, value=3, ts=25_000ms)
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│ WindowOperator.processElement                          WindowOperator.java:292-447       │
│                                                                                          │
│  ① Collection<TimeWindow> elementWindows =                                                │
│        windowAssigner.assignWindows(value, 25000, ctx)                                   │
│                                                                                          │
│     TumblingEventTimeWindows.assignWindows           TumblingEventTimeWindows.java:68-86 │
│     ──────────────────────────────────────                                               │
│       start = getWindowStartWithOffset(25000, 0, 60000)                                  │
│             = 25000 - (25000 % 60000)                                                    │
│             = 0                                                                          │
│       return Collections.singletonList(new TimeWindow(0, 60000));                        │
│                                                                                          │
│     ┌──────────────────────────────────────┐                                             │
│     │  data structure:                     │                                             │
│     │  Collection<TimeWindow> = singleton  │   ← ALWAYS 1 element for tumbling           │
│     │    [ TimeWindow{0, 60000} ]          │                                             │
│     └──────────────────────────────────────┘                                             │
│                                                                                          │
│  ② for (TimeWindow window : elementWindows)  ─── loop runs once                           │
│       windowState.setCurrentNamespace(window);      ── line 413                          │
│       windowState.add(value);                       ── line 414                          │
│                                                                                          │
│       windowState is a RocksDBReducingState<K, TimeWindow, Event>                        │
│       (built from ReducingStateDescriptor "window-contents"                              │
│        — WindowOperatorBuilder.java:178-182)                                             │
└──────────────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                  RocksDBReducingState.add(value)                  RocksDBReducingState.java:92-98
                  ───────────────────────────────
                    byte[] key   = serializeCurrentKeyWithGroupAndNamespace();
                    V oldValue   = db.get(cf, key);            ─── RocksDB GET
                    V newValue   = (oldValue == null) ? value
                                                       : reduceFunction.reduce(oldValue, value);
                    db.put(cf, writeOptions, key, serialize(newValue));   ─── RocksDB PUT
                                │
                                ▼
                  composite key bytes built by
                  SerializedCompositeKeyBuilder.buildCompositeKeyNamespace
                  (SerializedCompositeKeyBuilder.java:119-128)

                  ┌─────────────┬────────────┬──────────────────────────┐
                  │  KG prefix  │   "userA"  │ TimeWindow{0, 60000}     │
                  │  [0x?? ..]  │  utf bytes │  long 0   ║  long 60000  │
                  └─────────────┴────────────┴──────────────────────────┘
                                                       │
                                                       ▼
                                       in-RAM RocksDB memtable
                                                       │
                                                       │ flush when full
                                                       ▼
                                              /tmp/<jobUID>/00xxxx.sst


RocksDB column family "window-contents" after this single add:

   ┌────────────────────────────────────────────────┬─────────────┐
   │ KEY                                            │ VALUE       │
   ├────────────────────────────────────────────────┼─────────────┤
   │ [KG | userA | TimeWindow{0, 60000}]            │  Event(3)   │   ← ONE entry per element
   └────────────────────────────────────────────────┴─────────────┘

Timers registered for (userA, TimeWindow{0, 60000}):
   • fire    timer @ 59_999  (window.maxTimestamp())
   • cleanup timer @ 59_999  (max + allowedLateness=0; deduplicates with fire)
```

#### Figure 2 — Sliding window: one element, N=size/slide windows, N RocksDB entries

```
USER CODE
─────────
stream
    .keyBy(e -> e.userId)
    .window(SlidingEventTimeWindows.of(Duration.ofSeconds(60),                ── size=60s
                                       Duration.ofSeconds(20)))               ── slide=20s
    .reduce((a, b) -> new Event(a.userId, a.value + b.value))


ELEMENT ARRIVES  ─── (userA, value=3, ts=25_000ms)
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│ WindowOperator.processElement                          WindowOperator.java:292-447       │
│                                                                                          │
│  ① Collection<TimeWindow> elementWindows =                                                │
│        windowAssigner.assignWindows(value, 25000, ctx)                                   │
│                                                                                          │
│     SlidingEventTimeWindows.assignWindows          SlidingEventTimeWindows.java:76-91    │
│     ─────────────────────────────────────                                                │
│       List<TimeWindow> windows = new ArrayList<>((int)(size/slide));   ← capacity = 3    │
│       lastStart = getWindowStartWithOffset(25000, 0, 20000) = 20000;                     │
│       for (start = 20000; start > 25000-60000; start -= 20000):                          │
│           windows.add(new TimeWindow(start, start + 60000));                             │
│       return windows;                                                                    │
│                                                                                          │
│     ┌─────────────────────────────────────────────────────────┐                          │
│     │  data structure:                                        │                          │
│     │  List<TimeWindow> (ArrayList, size = size/slide = 3)    │   ← N elements!          │
│     │    [ TimeWindow{20000,  80000} ,                        │                          │
│     │      TimeWindow{    0,  60000} ,                        │                          │
│     │      TimeWindow{-20000, 40000} ]                        │                          │
│     └─────────────────────────────────────────────────────────┘                          │
│                                                                                          │
│  ② for (TimeWindow window : elementWindows)  ─── loop runs N=3 times                      │
│       windowState.setCurrentNamespace(window);    ── line 413  ┐                         │
│       windowState.add(value);                     ── line 414  │  one read-modify-write  │
│                                                                │  per window             │
│                                                                ┘                         │
└──────────────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                  Three independent RocksDBReducingState.add(value) calls,
                  each rebuilding a different composite key:

                  ┌─────────────┬────────────┬───────────────────────────────┐
       call 1:    │  KG prefix  │   "userA"  │ TimeWindow{ 20000,  80000}    │ ← namespace
                  └─────────────┴────────────┴───────────────────────────────┘
                  ┌─────────────┬────────────┬───────────────────────────────┐
       call 2:    │  KG prefix  │   "userA"  │ TimeWindow{     0,  60000}    │   differs
                  └─────────────┴────────────┴───────────────────────────────┘
                  ┌─────────────┬────────────┬───────────────────────────────┐
       call 3:    │  KG prefix  │   "userA"  │ TimeWindow{-20000,  40000}    │   per call
                  └─────────────┴────────────┴───────────────────────────────┘

                  Per call:  GET → reduce(old, new) → PUT
                             (3× RocksDB GET + 3× PUT for ONE input element)


RocksDB column family "window-contents" after this single add:

   ┌────────────────────────────────────────────────┬─────────────┐
   │ KEY                                            │ VALUE       │
   ├────────────────────────────────────────────────┼─────────────┤
   │ [KG | userA | TimeWindow{-20000,  40000}]      │  Event(3)   │ ← N=3 entries
   │ [KG | userA | TimeWindow{     0,  60000}]      │  Event(3)   │    per element
   │ [KG | userA | TimeWindow{ 20000,  80000}]      │  Event(3)   │
   └────────────────────────────────────────────────┴─────────────┘

Timers registered (one fire + one cleanup per window; with allowedLateness=0 they dedupe):
   • 39_999   for window [-20s, 40s)
   • 59_999   for window [  0s, 60s)
   • 79_999   for window [ 20s, 80s)
```

#### Side-by-side comparison

| Aspect | Tumbling (60s) | Sliding (60s, 20s) |
|---|---|---|
| Windows assigned per element | **1** | **N = size/slide = 3** |
| Return type of `assignWindows` | `Collections.singletonList(...)` | `ArrayList` of capacity N |
| Iterations in `processElement` loop | **1** | **N** |
| RocksDB ops per element (reducing) | 1× GET + 1× PUT | N× GET + N× PUT |
| Distinct composite keys touched | 1 | N (same key prefix, different namespace) |
| Concurrent `TimeWindow` namespaces per key | grows by ≤1 per event in a window's life | always ≤ N concurrent windows per key |
| Timers per element (with `allowedLateness=0`) | 1 (fire+cleanup dedupe) | N (one fire+cleanup pair per window) |

The cost asymmetry is structural: sliding windows multiply every RocksDB op by `size/slide`. That's why `reduce` / `aggregate` matter more on sliding (one accumulator per window) than `apply` / `process` with `ListState` (every element stored N times).

### How Sliding Window Assignment Works

**`SlidingEventTimeWindows.assignWindows()`** (`SlidingEventTimeWindows.java:77-91`):

```java
long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
for (long start = lastStart; start > timestamp - size; start -= slide) {
    windows.add(new TimeWindow(start, start + size));
}
```

Each element is assigned to `size / slide` windows (e.g., 60s/20s = 3 windows per element).

### Concrete Example

**Configuration:** Sliding window of **size=60s, slide=20s** over a `KeyedStream` keyed by `userId`, using a sum reduce function.

```java
stream
    .keyBy(event -> event.userId)
    .window(SlidingEventTimeWindows.of(Duration.ofSeconds(60), Duration.ofSeconds(20)))
    .reduce((a, b) -> new Event(a.userId, a.value + b.value))
```

(In current Flink, `SlidingEventTimeWindows.of` only accepts `java.time.Duration`; the older `Time.seconds(...)` overload has been removed — see `SlidingEventTimeWindows.java:119,142`.)

### Step-by-Step Timeline

```
═══════════════════════════════════════════════════════════════════════════
Event 1: (userA, value=3, timestamp=25s)
═══════════════════════════════════════════════════════════════════════════

  Window assignment:
    lastStart = 25000 - (25000 % 20000) = 20000
    Loop: start=20000 > 25000-60000=-35000 → [20s, 80s)
          start=0     > -35000             → [0s, 60s)
          start=-20000 > -35000            → [-20s, 40s)
          start=-40000 > -35000? NO → stop

  Assigned to 3 windows: [-20s, 40s), [0s, 60s), [20s, 80s)

  RocksDB writes (ReducingState: read old, reduce, write new):
    key=[KG|userA|[-20000,40000]]  PUT value=3     (new window, no old value)
    key=[KG|userA|[0,60000]]       PUT value=3     (new window)
    key=[KG|userA|[20000,80000]]   PUT value=3     (new window)

  Timers registered: 39999ms, 59999ms, 79999ms
    (each (key, window) actually registers two event-time timers:
     ─ fire timer    at window.maxTimestamp()
     ─ cleanup timer at window.maxTimestamp() + allowedLateness
     With the default allowedLateness=0 they share the same timestamp
     and the timer service deduplicates to one entry per window.)

═══════════════════════════════════════════════════════════════════════════
Event 2: (userA, value=5, timestamp=35s)
═══════════════════════════════════════════════════════════════════════════

  Window assignment:
    lastStart = 35000 - (35000 % 20000) = 20000
    Same 3 windows: [-20s, 40s), [0s, 60s), [20s, 80s)

  RocksDB writes (read-modify-write for each window):
    key=[KG|userA|[-20000,40000]]  GET→3, reduce(3,5)=8, PUT value=8
    key=[KG|userA|[0,60000]]       GET→3, reduce(3,5)=8, PUT value=8
    key=[KG|userA|[20000,80000]]   GET→3, reduce(3,5)=8, PUT value=8

═══════════════════════════════════════════════════════════════════════════
Event 3: (userA, value=2, timestamp=42s)
═══════════════════════════════════════════════════════════════════════════

  Window assignment:
    lastStart = 42000 - (42000 % 20000) = 40000
    Loop: start=40000 > 42000-60000=-18000 → [40s, 100s)
          start=20000 > -18000             → [20s, 80s)
          start=0     > -18000             → [0s, 60s)
          start=-20000 > -18000? NO → stop

  Assigned to 3 windows: [0s, 60s), [20s, 80s), [40s, 100s)

  Note: this element does NOT go into [-20s, 40s) because 42s >= 40s

  RocksDB writes:
    key=[KG|userA|[0,60000]]       GET→8, reduce(8,2)=10, PUT value=10
    key=[KG|userA|[20000,80000]]   GET→8, reduce(8,2)=10, PUT value=10
    key=[KG|userA|[40000,100000]]  PUT value=2           (new window)

  Timer registered: 99999ms

═══════════════════════════════════════════════════════════════════════════
Watermark advances past 39999ms → Timer fires for window [-20s, 40s)
═══════════════════════════════════════════════════════════════════════════

  EventTimeTrigger.onEventTime(39999) returns FIRE:
    → windowState.setCurrentNamespace([-20000, 40000])
    → windowState.get() → returns 8
    → emit downstream: (userA, 8) with timestamp 39999
    → cleanup: windowState.clear()
      → RocksDB DELETE key=[KG|userA|[-20000,40000]]
    → delete cleanup timer

═══════════════════════════════════════════════════════════════════════════
RocksDB state snapshot after all above:
═══════════════════════════════════════════════════════════════════════════

  key=[KG|userA|[0,60000]]        value=10    (timer at 59999)
  key=[KG|userA|[20000,80000]]    value=10    (timer at 79999)
  key=[KG|userA|[40000,100000]]   value=2     (timer at 99999)

  Window [-20s, 40s) has been fired and cleaned up — its state is gone.
```

### Key Observations for Sliding Windows

1. **Each element is written to `size/slide` windows independently.** For 60s/20s, that's 3 RocksDB writes per element. This is the main cost of sliding windows.

2. **Each (key, window) pair is a separate RocksDB entry.** The window is encoded as the namespace in the composite key. Windows don't "share" state.

3. **ReducingState/AggregatingState does a read-modify-write per window.** This is why `reduce()`/`aggregate()` is much more efficient than `apply()` for large windows — `apply()` uses `ListState` which stores every element.

4. **State is cleaned up independently per window.** When window `[-20s, 40s)` fires, only its entry is deleted. The overlapping windows `[0s, 60s)` and `[20s, 80s)` still retain their state.

5. **During a checkpoint**, all these RocksDB entries (across all keys and all windows) are snapshotted together as part of the RocksDB native checkpoint. There is no per-window checkpoint logic — it's all just key-value pairs in RocksDB.

6. **Sliding windows are NOT merging windows.** They take the simpler non-merging path in `WindowOperator.processElement()` (lines 404-432). Session windows are the only built-in merging window type.
