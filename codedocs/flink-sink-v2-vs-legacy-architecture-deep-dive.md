# Flink Sink Architecture Deep Dive: Legacy vs Sink V2, Using Iceberg as the Example

This doc walks the two coexisting Flink sink architectures end-to-end, using Apache Iceberg as the concrete example because it implements **both** flavors in two different versions of the connector:

| Architecture | Iceberg connector source tree | Flink runtime side |
|---|---|---|
| **Legacy** (`SinkFunction` / hand-rolled operator chain) | `/Users/ehaowan/src/myiceberg/flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/FlinkSink.java` | `flink-runtime`'s `AbstractStreamOperator` lifecycle hooks (`snapshotState`, `notifyCheckpointComplete`) |
| **Sink V2** (FLIP-143 / FLIP-191) | `/Users/ehaowan/src/myiceberg/flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergSink.java` | `flink-runtime`'s `SinkWriterOperator` + `CommitterOperator` + `GlobalCommitterOperator` |

The dominant question is: in Sink V2, **when** is `Committer.commit(Collection<CommitRequest<IcebergCommittable>>)` actually called? The short answer: from `CommitterOperator.notifyCheckpointComplete(long)` after the JobManager confirms checkpoint N globally complete — not at all from the user-facing `IcebergSink` class. Sections 4 and 5 trace that call path with file:line citations.

---

## Table of Contents

- [Part 1: Why Two Architectures Coexist](#part-1-why-two-architectures-coexist)
- [Part 2: Legacy Sink Architecture (Iceberg v1.20 `FlinkSink`)](#part-2-legacy-sink-architecture-iceberg-v120-flinksink)
- [Part 3: Sink V2 Architecture (Iceberg v2.1 `IcebergSink`)](#part-3-sink-v2-architecture-iceberg-v21-icebergsink)
- [Part 4: The Critical Question — When Is `commit(...)` Called?](#part-4-the-critical-question--when-is-commit-called)
- [Part 5: Full Call Hierarchy with File:Line Citations](#part-5-full-call-hierarchy-with-fileline-citations)
- [Part 6: Example Code Walk](#part-6-example-code-walk)
- [Part 7: Retry, Idempotency, and `CommitRequest` State Machine](#part-7-retry-idempotency-and-commitrequest-state-machine)
- [Part 8: Side-by-Side Comparison](#part-8-side-by-side-comparison)
- [Part 9: Where the "Global Committer" Fits (and Why Iceberg Doesn't Use It for Data Commits)](#part-9-where-the-global-committer-fits-and-why-iceberg-doesnt-use-it-for-data-commits)
- [Part 10: Topology Translation — How `IcebergSink` Becomes Operators](#part-10-topology-translation--how-icebergsink-becomes-operators)
- [Appendix A: Key Classes Index](#appendix-a-key-classes-index)

---

## Part 1: Why Two Architectures Coexist

The legacy model evolved organically: the user wrote a `SinkFunction` (or, like Iceberg, hand-rolled a chain of `AbstractStreamOperator` subclasses), bolted on Flink's checkpoint lifecycle hooks, and stitched the operators together with explicit `DataStream.transform(...)` calls. Two-phase commit was implemented **inside** the committer operator: each operator stored its own pending state via `ListStateDescriptor`, and `notifyCheckpointComplete(long)` on that same operator was the commit trigger.

FLIP-143 (Sink V1, Flink 1.12) and then FLIP-191 (Sink V2, Flink 1.15+) split the sink into three orthogonal contracts:

```
+----------------------+   committable +-----------+   external
|  CommittingSinkWriter|---------------+| Committer |---system
+----------------------+               +-----------+
       Sink<InputT>  <-- creates -->   SupportsCommitter<CommittableT>
```

- `Sink<InputT>` creates `SinkWriter<InputT>`s (one per parallel subtask).
- `SupportsCommitter<CommittableT>` is a mixin declaring the sink emits committables that need a second-phase commit.
- `CommittingSinkWriter` is `SinkWriter` plus `prepareCommit()` — called by the runtime between `flush(false)` and the checkpoint barrier.
- `Committer` is the second-phase commit interface. The runtime handles **when** to call `commit(...)` (after `notifyCheckpointComplete`), **how** to batch committables per checkpoint, and **how** to retry. The user does not implement that machinery anymore.

Iceberg v1.20 ships the legacy `FlinkSink` builder. Iceberg v2.1 ships **both** the legacy `FlinkSink` (for backward compatibility) and the new `IcebergSink` (Sink V2 implementation). The two coexist in the same connector jar.

---

## Part 2: Legacy Sink Architecture (Iceberg v1.20 `FlinkSink`)

### 2.1 The user-side DAG

The legacy `FlinkSink.Builder.chainIcebergOperators` builds the DAG explicitly using `DataStream.transform(...)` calls. Source: `iceberg/flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/FlinkSink.java:465-477`.

```
                          parallel writers (writerParallelism)        single-parallelism committer        sink-marker
                      ┌──────────────────────────────────────┐        ┌─────────────────────────────┐    ┌─────────────────┐
inputDataStream ──▶  distributeStream  ──▶  IcebergStreamWriter ──▶  IcebergFilesCommitter         ──▶  DiscardingSink     │
   (RowData)         (shuffled by                    │                          │                       (parallelism = 1) │
                     distribution-                   │                          │                                          │
                     mode/equality)              FlinkWriteResult           Void output                                    │
                                                                              │
                                                              writes to Iceberg table here
```

The classes are:

| Operator | Source path | Role |
|---|---|---|
| `IcebergStreamWriter<RowData>` | v1.20 `.../sink/IcebergStreamWriter.java` | One per parallel subtask. Wraps an Iceberg `TaskWriter`. Emits a `FlinkWriteResult` (checkpointId + `WriteResult`) per `prepareSnapshotPreBarrier`. |
| `IcebergFilesCommitter` | v1.20 `.../sink/IcebergFilesCommitter.java` | **Parallelism = 1.** Buffers `WriteResult`s per checkpoint, stages them to Avro manifests at `snapshotState`, and **commits to Iceberg in `notifyCheckpointComplete`**. |
| `DiscardingSink<Void>` | flink-runtime | A no-op terminal that just satisfies "every DAG needs a sink." |

The writer emits `FlinkWriteResult` records — a transient envelope `(long checkpointId, WriteResult result)` — into normal DataStream channels. The committer downstream collects them; nothing about the V2 `CommittableMessage` envelope is involved here.

### 2.2 Two-phase commit, implemented inside `IcebergFilesCommitter`

The legacy committer is one operator that owns the entire 2PC dance. Its lifecycle (`v1.20/.../IcebergFilesCommitter.java`):

```
        ┌─────────────────────────────────────────────────────────────────────────────┐
        │  IcebergFilesCommitter (parallelism = 1)                                    │
        │                                                                             │
        │  open()  +  initializeState(StateInitializationContext)                     │
        │     │       │                                                               │
        │     │       └─ if restored:                                                 │
        │     │           - reads jobIdState + checkpointsState (Flink ListState)     │
        │     │           - SinkUtil.getMaxCommittedCheckpointId(...) — walks the     │
        │     │             Iceberg snapshot chain backward                           │
        │     │           - commitUpToCheckpoint(uncommitted, ...) for any pending    │
        │     │             entries (PRE-PHASE recovery)                              │
        │     │                                                                       │
        │  processElement(FlinkWriteResult)                                           │
        │     └─ writeResultsSinceLastSnapshot[checkpointId].add(result)              │
        │        // pure in-memory buffer                                             │
        │                                                                             │
        │  snapshotState(StateSnapshotContext)            <-- PRE-COMMIT (phase 1)    │
        │     ├─ writeToManifestUptoLatestCheckpoint(ckpId)                           │
        │     │     └─ FlinkManifestUtil.writeCompletedFiles(...) — writes the staging│
        │     │       Avro manifest file to Iceberg's metadata location               │
        │     └─ checkpointsState.update(dataFilesPerCheckpoint)                      │
        │       jobIdState.update(flinkJobId)                                         │
        │                                                                             │
        │  notifyCheckpointComplete(long ckpId)           <-- COMMIT (phase 2)        │
        │     └─ if ckpId > maxCommittedCheckpointId:                                 │
        │           commitUpToCheckpoint(dataFilesPerCheckpoint, ..., ckpId)          │
        │             ├─ rebuilds WriteResults by reading the staging Avro manifests  │
        │             ├─ AppendFiles / RowDelta / ReplacePartitions  on  Iceberg      │
        │             ├─ operation.set("flink.max-committed-checkpoint-id", ckpId)    │
        │             └─ operation.commit()   <-- writes new Iceberg snapshot         │
        │           maxCommittedCheckpointId = ckpId                                  │
        │           FlinkManifestUtil.deleteCommittedManifests(...) (best-effort GC)  │
        │                                                                             │
        │  endInput()                                                                 │
        │     └─ writeToManifestUptoLatestCheckpoint(END_INPUT_CKP_ID = Long.MAX_VALUE)│
        │        commitUpToCheckpoint(...)                                            │
        └─────────────────────────────────────────────────────────────────────────────┘
```

Two key load-bearing facts you cannot miss:

1. **The 2PC contract is implemented by the user** — `snapshotState` for phase-1 staging, `notifyCheckpointComplete` for phase-2 commit. Recovery is also the user's job (`SinkUtil.getMaxCommittedCheckpointId` walks the Iceberg snapshot chain backward looking for the latest `flink.max-committed-checkpoint-id` from the same `flink.job-id` / `flink.operator-id`; covered in detail in `flink-iceberg-sink-connector-deep-dive.md` Part 6).
2. **`processElement` accumulates into a `writeResultsSinceLastSnapshot` map keyed by `checkpointId`.** The writer's `FlinkWriteResult` carries the checkpointId explicitly because in the legacy world there's no built-in `CommittableSummary` framing — the committer needs to figure out which checkpoint a writer's output belongs to.

### 2.3 Legacy 2PC sequence diagram

```
JobManager                Writer subtasks                Committer (par=1)         Iceberg
   │                            │                                │                     │
   │ triggerCheckpoint(N)       │                                │                     │
   ├──────────────────────────▶ │                                │                     │
   │                            │ prepareSnapshotPreBarrier(N):  │                     │
   │                            │   writer.complete() → result   │                     │
   │                            │   emit FlinkWriteResult(N, r)  │                     │
   │                            ├──────────────────────────────▶ │                     │
   │                            │                                │ processElement      │
   │                            │                                │  buffers into       │
   │                            │                                │  writeResultsSinceLastSnapshot[N]
   │ inject barrier             │                                │                     │
   │                            │ (barrier flows downstream)     │                     │
   │                            │                                │ snapshotState(N):   │
   │                            │                                │   write Avro to     │
   │                            │                                │   metadata location ───▶ (Avro file)
   │                            │                                │   put manifest      │
   │                            │                                │   bytes into        │
   │                            │                                │   dataFilesPerCheckpoint[N]
   │                            │                                │   checkpointsState.update(...)
   │                            │ acknowledgeCheckpoint(N)       │                     │
   │ ◀────────────────────────  │                                │                     │
   │                            │                                │ acknowledgeCheckpoint(N)
   │ ◀────────────────────────────────────────────────────────── │                     │
   │ (all subtasks acked)       │                                │                     │
   │ notifyCheckpointComplete(N)│                                │                     │
   ├──────────────────────────▶ │                                │                     │
   ├────────────────────────────────────────────────────────────▶│ notifyCheckpointComplete(N):
   │                            │                                │   read Avros back   │
   │                            │                                │   AppendFiles /     │
   │                            │                                │   RowDelta /        │
   │                            │                                │   ReplacePartitions │
   │                            │                                │   op.commit() ──────▶ new Iceberg snapshot
   │                            │                                │                       with summary:
   │                            │                                │                       flink.max-committed-checkpoint-id = N
```

### 2.4 What's awkward about the legacy model

- The 2PC bookkeeping (per-checkpoint maps, recovery walk, retries) lives inside every sink. Each connector reimplements it; bugs are connector-specific.
- The "pre-commit avro staging" decision is forced by the topology shape: the writer must hand off raw `WriteResult` records to a downstream `AbstractStreamOperator` because there is no built-in "batch up by checkpoint" abstraction. Avro manifests serve that role.
- `parallelism = 1` is enforced by builder code (`appendCommitter` line 547). There's no first-class concept of "global committer."
- There is no built-in `CommitRequest` API for "I already committed this, mark it done" — the recovery code has to detect that itself.

Sink V2 absorbs all four pain points into Flink's runtime.

---

## Part 3: Sink V2 Architecture (Iceberg v2.1 `IcebergSink`)

### 3.1 The SPI: four interfaces in `flink-core/src/main/java/org/apache/flink/api/connector/sink2/`

| Interface | File | Required? | Purpose |
|---|---|---|---|
| `Sink<InputT>` | `Sink.java` | yes | Top-level factory. Just `createWriter(WriterInitContext) → SinkWriter<InputT>`. |
| `SinkWriter<InputT>` | `SinkWriter.java` | yes | `write(elem, ctx)`, `flush(endOfInput)`, `writeWatermark(...)`. The actual writing component. |
| `CommittingSinkWriter<InputT, CommT>` | `CommittingSinkWriter.java` | for 2PC | Extends `SinkWriter`, adds `prepareCommit() → Collection<CommT>`. Called by the runtime after `flush(false)` and before `snapshotState`. |
| `SupportsCommitter<CommT>` | `SupportsCommitter.java` | for 2PC | Mixin on `Sink`. Declares `createCommitter(CommitterInitContext) → Committer<CommT>` and `getCommittableSerializer()`. |
| `Committer<CommT>` | `Committer.java` | for 2PC | `commit(Collection<CommitRequest<CommT>>)`. This is the entry point the rest of this doc traces. |
| `SupportsPreWriteTopology<InputT>` | `flink-runtime/.../sink2/...` | optional | Lets the sink insert a `DataStream` topology *before* the writer (e.g. partitioning/shuffling). |
| `SupportsPreCommitTopology<WriteResultT, CommT>` | same | optional | Lets the sink insert a topology *between* the writer and the committer (e.g. an aggregator). Iceberg uses this. |
| `SupportsPostCommitTopology<CommT>` | same | optional | Lets the sink insert a topology *after* the committer. Iceberg uses this for compaction (`compactMode`). |

### 3.2 `IcebergSink` implements the V2 contract

From `iceberg/flink/v2.1/.../sink/IcebergSink.java:139-145`:

```java
public class IcebergSink
    implements Sink<RowData>,
        SupportsPreWriteTopology<RowData>,
        SupportsCommitter<IcebergCommittable>,
        SupportsPreCommitTopology<WriteResult, IcebergCommittable>,
        SupportsPostCommitTopology<IcebergCommittable>,
        SupportsConcurrentExecutionAttempts {
```

The three factory methods (`IcebergSink.java:209-247`) are tiny:

```java
@Override
public SinkWriter<RowData> createWriter(WriterInitContext context) {
  return new IcebergSinkWriter(...);                     // CommittingSinkWriter<RowData, WriteResult>
}

@Override
public Committer<IcebergCommittable> createCommitter(CommitterInitContext context) {
  return new IcebergCommitter(tableLoader, branch, ...); // Committer<IcebergCommittable>
}

@Override
public SimpleVersionedSerializer<IcebergCommittable> getCommittableSerializer() {
  return new IcebergCommittableSerializer();
}
```

### 3.3 The Iceberg V2 sink topology (after expansion)

The Iceberg-side ASCII diagram in `IcebergSink.java:120-136` shows:

```
                              Flink sink
              ┌────────────────────────────────────────────────────────────────────────────────┐
              │                                                                                │
+-------+     │ +----------+                              +-------------+    +----------------+ │
| Map 1 | ──▶ │ | writer 1 |                              | committer 1 |───▶| post commit 1  | │
+-------+     │ +----------+ \                          / +-------------+ \  +----------------+ │
              │               \                        /                   \                     │
              │                \  +------------------+/    +-------------+   \ +---------------+ │
+-------+     │ +----------+    ▶ | commit aggregator |    | committer 2 |     | post commit 2 | │
| Map 2 | ──▶ │ | writer 2 |     +------------------+      +-------------+     +---------------+ │
+-------+     │ +----------+      (parallelism = 1)         Commit only on                       │
              │                                              committer 1                         │
              └────────────────────────────────────────────────────────────────────────────────┘
```

What the runtime actually instantiates after the translator expands the user-facing `IcebergSink`:

```
              ┌─── SinkWriterOperator ───┐    pre-commit topology         downstream
              │  wraps IcebergSinkWriter │    (Iceberg-specific)
              │  (CommittingSinkWriter)  │    parallelism = 1
   input ──▶ ─┤  emits CommittableMessage├──▶ IcebergWriteAggregator ──▶ CommitterOperator ──▶ (optional)
              │  <WriteResult>           │    writes Avro manifest        wraps                CommittableToTableChangeConverter
              │  parallelism = writer    │    emits CommittableMessage    IcebergCommitter      → maintenance pipeline
              └──────────────────────────┘    <IcebergCommittable>        (Committer<...>)
                  many subtasks                                            parallelism =
                                                                           inferred (typically
                                                                           also 1 because of
                                                                           upstream .global())
```

Note an important structural point:

- The **pre-commit aggregator (`IcebergWriteAggregator`)** is the operator that writes the staging Avro manifest in Sink V2 — it sits *between* writer and committer. Writers emit pure in-memory `WriteResult` objects. **Writers do not write Avro manifests in V2.** (See `flink-iceberg-sink-connector-deep-dive.md` Part 3 for the cross-check.)
- The `CommitterOperator` is a **Flink runtime class** (`flink-runtime/src/main/java/org/apache/flink/streaming/runtime/operators/sink/CommitterOperator.java`). It is not connector code. It is generic; it just calls `committer.commit(...)`. This is where the call to `IcebergCommitter.commit` originates.

### 3.4 The two `CommittableMessage` types crossing the wires

```java
sealed CommittableMessage<CommT>
   ├── CommittableSummary<CommT>     // (subtaskId, numberOfSubtasks, checkpointId, numCommittables, ...)
   └── CommittableWithLineage<CommT> // (committable, checkpointId, subtaskId)
```

Every "batch of committables for checkpoint N from subtask S" is framed as:

```
StreamRecord(CommittableSummary(S, numSubtasks, N, k, ...))
StreamRecord(CommittableWithLineage(commT_1, N, S))
StreamRecord(CommittableWithLineage(commT_2, N, S))
...
StreamRecord(CommittableWithLineage(commT_k, N, S))
```

These framing records are what the `CommitterOperator` reads in `processElement` and feeds to its internal `CommittableCollector`. The collector knows exactly when "all committables for checkpoint N have arrived" because it has counted them against `numberOfCommittables` from the summary. This per-checkpoint batching is the part that the **legacy connector had to implement itself** with `writeResultsSinceLastSnapshot`.

---

## Part 4: The Critical Question — When Is `commit(...)` Called?

In Sink V2, `IcebergCommitter.commit(Collection<CommitRequest<IcebergCommittable>>)` is called by **`CommitterOperator`** in three places, all in `flink-runtime/.../sink/CommitterOperator.java`:

### 4.1 Entry point #1 — `notifyCheckpointComplete` (steady-state streaming)

This is the dominant path. `CommitterOperator.notifyCheckpointComplete` at line 159-162:

```java
@Override
public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    commitAndEmitCheckpoints(Math.max(lastCompletedCheckpointId, checkpointId));
}
```

`commitAndEmitCheckpoints` at line 164-174:

```java
private void commitAndEmitCheckpoints(long checkpointId)
        throws IOException, InterruptedException {
    lastCompletedCheckpointId = checkpointId;
    for (CheckpointCommittableManager<CommT> checkpointManager :
            committableCollector.getCheckpointCommittablesUpTo(checkpointId)) {
        commitAndEmit(checkpointManager);                         // <─ committer.commit lives here
        committableCollector.remove(checkpointManager);
    }
}
```

`commitAndEmit` at line 176-182 calls `CheckpointCommittableManagerImpl.commit(committer, maxRetries)`, which is at `flink-runtime/.../sink/committables/CheckpointCommittableManagerImpl.java:145-161`:

```java
@Override
public void commit(Committer<CommT> committer, int maxRetries)
        throws IOException, InterruptedException {
    Collection<CommitRequestImpl<CommT>> requests =
            getPendingRequests().collect(Collectors.toList());
    for (int retry = 0; !requests.isEmpty() && retry <= maxRetries; retry++) {
        requests.forEach(CommitRequestImpl::setSelected);
        committer.commit(Collections.unmodifiableCollection(requests));   // <-- THE CALL
        requests.forEach(CommitRequestImpl::setCommittedIfNoError);
        requests = requests.stream().filter(r -> !r.isFinished()).collect(Collectors.toList());
    }
    if (!requests.isEmpty()) {
        throw new IOException(String.format("Failed to commit %s committables after %s retries: %s", ...));
    }
}
```

So `committer.commit(...)` (i.e. `IcebergCommitter.commit`) runs **synchronously, on the task thread (mailbox), inside `notifyCheckpointComplete`** — not from a background pool. If it throws, the operator fails.

### 4.2 Entry point #2 — `initializeState` (recovery)

`CommitterOperator.initializeState` at line 120-141. The interesting block at line 135-140:

```java
if (checkpointId.isPresent()) {
    committableCollectorState.get().forEach(cc -> committableCollector.merge(cc));
    lastCompletedCheckpointId = checkpointId.getAsLong();
    // try to re-commit recovered transactions as quickly as possible
    commitAndEmitCheckpoints(lastCompletedCheckpointId);
}
```

When a task restores from a checkpoint, the `committableCollectorState` holds any committables that were emitted from upstream writers but **not yet committed** to the external system. On restart, the runtime immediately calls `commitAndEmitCheckpoints` to re-attempt them.

This is also where `IcebergCommitter.commit`'s defensive `signalAlreadyCommitted(...)` matters most: the recovered batch may have already been successfully committed in the prior run — the Iceberg snapshot chain says so. Iceberg's committer walks the snapshot chain (`SinkUtil.getMaxCommittedCheckpointId`) to learn `maxCommittedCheckpointId`, then marks recovered requests with `checkpointId <= maxCommittedCheckpointId` as `signalAlreadyCommitted` and **does not re-commit them**. See `IcebergCommitter.java:114-138`:

```java
@Override
public void commit(Collection<CommitRequest<IcebergCommittable>> commitRequests)
    throws IOException, InterruptedException {
  if (commitRequests.isEmpty()) {
    return;
  }
  NavigableMap<Long, CommitRequest<IcebergCommittable>> commitRequestMap = Maps.newTreeMap();
  for (CommitRequest<IcebergCommittable> request : commitRequests) {
    commitRequestMap.put(request.getCommittable().checkpointId(), request);
  }
  IcebergCommittable last = commitRequestMap.lastEntry().getValue().getCommittable();
  long maxCommittedCheckpointId =
      SinkUtil.getMaxCommittedCheckpointId(table, last.jobId(), last.operatorId(), branch);
  // Mark the already committed FilesCommittable(s) as finished
  commitRequestMap
      .headMap(maxCommittedCheckpointId, true)
      .values()
      .forEach(CommitRequest::signalAlreadyCommitted);
  NavigableMap<Long, CommitRequest<IcebergCommittable>> uncommitted =
      commitRequestMap.tailMap(maxCommittedCheckpointId, false);
  if (!uncommitted.isEmpty()) {
    commitPendingRequests(uncommitted, last.jobId(), last.operatorId());
  }
}
```

### 4.3 Entry point #3 — `endInput` (batch / no checkpointing)

`CommitterOperator.endInput` at line 151-156:

```java
@Override
public void endInput() throws Exception {
    if (!isCheckpointingEnabled || isBatchMode) {
        // There will be no final checkpoint, all committables should be committed here
        commitAndEmitCheckpoints(Long.MAX_VALUE);
    }
}
```

For batch jobs or streaming jobs with checkpointing disabled (rare), the only commit signal is end-of-input. The runtime flushes everything by passing `Long.MAX_VALUE` so `getCheckpointCommittablesUpTo` returns every pending batch.

### 4.4 Summary

The user-facing **`IcebergSink`** class **never calls `IcebergCommitter.commit` directly** — it just supplies the `Committer` instance via `createCommitter(...)`. The runtime decides when to call it. The three entry points:

```
                       ┌────────────────────────────────────────────────────────────────┐
                       │            CommitterOperator (flink-runtime)                   │
                       │                                                                │
                       │  notifyCheckpointComplete(N)  ◀── from CheckpointCoordinator   │
                       │      │                            after global ACK             │
                       │      ▼                                                         │
                       │  commitAndEmitCheckpoints(N)                                   │
                       │      │                                                         │
                       │  initializeState(restoredCkp)  ◀── on task restart             │
                       │      │                                                         │
                       │      ▼                                                         │
                       │  commitAndEmitCheckpoints(restoredCkp)                         │
                       │      │                                                         │
                       │  endInput()  ◀── on stream end with no checkpointing/batch     │
                       │      │                                                         │
                       │      ▼                                                         │
                       │  commitAndEmitCheckpoints(Long.MAX_VALUE)                      │
                       │      │                                                         │
                       │      ▼                                                         │
                       │  for each CheckpointCommittableManager up-to N:                │
                       │      mgr.commit(committer, maxRetries)  ───────▶  IcebergCommitter.commit(...)
                       └────────────────────────────────────────────────────────────────┘
```

---

## Part 5: Full Call Hierarchy with File:Line Citations

Steady-state path (checkpoint completes → Iceberg gets a new snapshot):

```
JobManager mailbox (CheckpointCoordinator)
  └─ allTasksAcked(checkpointN) → broadcast notifyCheckpointComplete(N)
      flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/CheckpointCoordinator.java
  (RPC via JobMasterGateway → TaskExecutor → Task → StreamTask)
      │
      ▼
TaskExecutor side (each subtask)
  StreamTask.notifyCheckpointCompleteAsync(checkpointN)
      flink-runtime/src/main/java/org/apache/flink/streaming/runtime/tasks/StreamTask.java
      │
      ▼ (enqueued on mailbox)
  OperatorChain.notifyCheckpointComplete(N)
      └─ for each operator in chain order:
         AbstractStreamOperator.notifyCheckpointComplete(N)
      │
      ▼
  CommitterOperator.notifyCheckpointComplete(N)
      flink-runtime/.../sink/CommitterOperator.java:159
      │  super.notifyCheckpointComplete(N);
      │  commitAndEmitCheckpoints(Math.max(lastCompletedCheckpointId, N));
      ▼
  CommitterOperator.commitAndEmitCheckpoints(N)
      flink-runtime/.../sink/CommitterOperator.java:164
      │  for each CheckpointCommittableManager mgr in collector.getCheckpointCommittablesUpTo(N):
      │      commitAndEmit(mgr);
      ▼
  CommitterOperator.commitAndEmit(mgr)
      flink-runtime/.../sink/CommitterOperator.java:176
      │  mgr.commit(committer, maxRetries);
      ▼
  CheckpointCommittableManagerImpl.commit(committer, maxRetries)
      flink-runtime/.../sink/committables/CheckpointCommittableManagerImpl.java:145
      │  for retry = 0..maxRetries:
      │      committer.commit(Collections.unmodifiableCollection(requests));   <── THE CALL
      ▼
  IcebergCommitter.commit(Collection<CommitRequest<IcebergCommittable>>)
      iceberg/flink/v2.1/.../sink/IcebergCommitter.java:114
      │  group requests by checkpointId
      │  maxCommittedCheckpointId = SinkUtil.getMaxCommittedCheckpointId(table, jobId, operatorId, branch)
      │  mark already-committed as signalAlreadyCommitted
      │  commitPendingRequests(uncommitted, jobId, operatorId)
      ▼
  IcebergCommitter.commitPendingRequests(...)
      iceberg/flink/v2.1/.../sink/IcebergCommitter.java:152
      │  for each CommitRequest: read its Avro DeltaManifest → WriteResult
      │  commitPendingResult(pendingResults, summary, jobId, operatorId)
      ▼
  IcebergCommitter.commitDeltaTxn / replacePartitions
      iceberg/flink/v2.1/.../sink/IcebergCommitter.java:194 / 214 / 235
      │  build AppendFiles / RowDelta / ReplacePartitions
      │  operation.set("flink.max-committed-checkpoint-id", checkpointId)
      │  operation.set("flink.job-id", flinkJobId)
      │  operation.set("flink.operator-id", operatorId)
      ▼
  Iceberg core: SnapshotProducer.commit()
      myiceberg core (.../apache/iceberg/SnapshotProducer.java)
      │  CAS on the catalog → new metadata.json + new manifest list + new snapshot
      ▼
  FlinkManifestUtil.deleteCommittedManifests(table, manifests, jobId, ckpId)
      iceberg/flink/v2.1/.../sink/FlinkManifestUtil.java
      │  best-effort delete of the staging Avro manifests now made redundant
```

Recovery path (restart from checkpoint M):

```
StreamTask.beforeInvoke
  └─ OperatorChain.initializeStateAndOpenOperators
      └─ CommitterOperator.initializeState(StateInitializationContext ctx)
            flink-runtime/.../sink/CommitterOperator.java:120
            │  committerSupplier.apply(initContext) → new IcebergCommitter(...)
            │  load committableCollectorState (deserialize via CommittableCollectorSerializer)
            │  if ctx.getRestoredCheckpointId() is present:
            │      lastCompletedCheckpointId = restored
            │      commitAndEmitCheckpoints(restored)        <── same path as steady state
```

End-of-input path (batch / streaming-no-ckp):

```
StreamTask.endInput()
  └─ CommitterOperator.endInput()
        flink-runtime/.../sink/CommitterOperator.java:151
        │  if (!isCheckpointingEnabled || isBatchMode):
        │      commitAndEmitCheckpoints(Long.MAX_VALUE)
```

The pre-commit half (where committables are produced) is symmetric:

```
StreamTask.triggerCheckpoint(N)
  │
  ▼
SubtaskCheckpointCoordinatorImpl
  └─ for each operator: AbstractStreamOperator.prepareSnapshotPreBarrier(N)
      ▼
SinkWriterOperator.prepareSnapshotPreBarrier(N)
      flink-runtime/.../sink/SinkWriterOperator.java:188
      │  sinkWriter.flush(false);
      │  emitCommittables(N);
      ▼
SinkWriterOperator.emitCommittables(N)
      flink-runtime/.../sink/SinkWriterOperator.java:215
      │  Collection<CommT> committables = ((CommittingSinkWriter<?, CommT>) sinkWriter).prepareCommit();
      │  emit(subtaskId, numSubtasks, N, committables);
      ▼
IcebergSinkWriter.prepareCommit()
      iceberg/flink/v2.1/.../sink/IcebergSinkWriter.java:99
      │  WriteResult result = writer.complete();
      │  return Lists.newArrayList(result);
      ▼
(emits CommittableSummary + CommittableWithLineage<WriteResult>)
      ▼
IcebergWriteAggregator (pre-commit topology, parallelism=1)
      iceberg/flink/v2.1/.../sink/IcebergWriteAggregator.java
      │  processElement: collects WriteResults
      │  prepareSnapshotPreBarrier(N): writes the Avro manifest, emits
      │      CommittableWithLineage<IcebergCommittable>
      ▼
CommitterOperator.processElement
      flink-runtime/.../sink/CommitterOperator.java:210
      │  committableCollector.addMessage(env.getValue());
      ▼
(committables now buffered in CommittableCollector, keyed by checkpointId)
      ▼
... barrier flows through, checkpoint completes ...
      ▼
... eventually notifyCheckpointComplete(N) is delivered (Part 5 top) ...
```

---

## Part 6: Example Code Walk

### 6.1 Production example: `IcebergSink` itself

A minimal user-side use of the V2 sink:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(60_000);

DataStream<RowData> rowDataStream = ...;

IcebergSink.forRowData(rowDataStream)
    .tableLoader(TableLoader.fromHadoopTable("s3://bucket/db/table"))
    .upsert(true)
    .equalityFieldColumns(Arrays.asList("id"))
    .append();
```

`append()` calls `rowDataStream.sinkTo(icebergSink)` which creates a `SinkTransformation`. The Flink translator (`SinkTransformationTranslator`) **expands** that single `SinkTransformation` into the multi-operator topology at `JobGraph` translation time. See Part 10 for the expansion.

### 6.2 Test example: `TestIcebergCommitter`

The connector ships an integration test for the committer at:

```
/Users/ehaowan/src/myiceberg/flink/v2.1/flink/src/test/java/org/apache/iceberg/flink/sink/TestIcebergCommitter.java
```

This is the only direct caller of `IcebergCommitter.commit(...)` in the codebase — and it exists only because the test bypasses the runtime. In production no one calls `IcebergCommitter.commit` directly; only `CheckpointCommittableManagerImpl.commit` does.

The test builds a fake `CommitRequest` via:

```
/Users/ehaowan/src/myflink/flink-core/src/test/java/org/apache/flink/api/connector/sink2/mocks/MockCommitRequest.java
```

Pattern (simplified):

```java
IcebergCommitter committer = new IcebergCommitter(
    tableLoader, "main", snapshotProps, /*replacePartitions=*/false, /*workerPool=*/2,
    "sink-uuid", /*metrics=*/null, /*compactMode=*/false);

IcebergCommittable c = new IcebergCommittable(manifestBytes, "job-id", "op-id", /*ckpId=*/3L);
MockCommitRequest<IcebergCommittable> req = new MockCommitRequest<>(c);
committer.commit(Collections.singleton(req));

// Assert Iceberg now has a snapshot with summary flink.max-committed-checkpoint-id=3
```

### 6.3 What does a real `CommitRequest` look like at runtime?

At runtime, `CommitterOperator` instantiates `CommitRequestImpl<CommT>`, file `flink-runtime/.../sink/committables/CommitRequestImpl.java`. Each commit request is constructed inside `SubtaskCommittableManager` (called from `CheckpointCommittableManagerImpl.addCommittable` → `SubtaskCommittableManager.add`). The state machine:

```
        RECEIVED ───── committer.signalAlreadyCommitted ──▶ COMMITTED (terminal)
        │  │
        │  └─── (no error after commit()) setCommittedIfNoError ──▶ COMMITTED (terminal)
        │
        ├── retryLater / updateAndRetryLater ──▶ RETRY ──▶ next loop iteration sets it back to RECEIVED
        ├── signalFailedWithKnownReason ──▶ FAILED (terminal, logged & discarded)
        └── signalFailedWithUnknownReason ──▶ throws IllegalStateException → fails the job
```

(`CommitRequestImpl.java:76-118`). The retry loop in `CheckpointCommittableManagerImpl.commit:149-154` filters out terminal states between iterations.

---

## Part 7: Retry, Idempotency, and `CommitRequest` State Machine

### 7.1 The retry loop

`CheckpointCommittableManagerImpl.commit` retries up to `maxRetries` times. Default is `SinkOptions.COMMITTER_RETRIES` (currently 10; see `flink-core/src/main/java/org/apache/flink/configuration/SinkOptions.java`). Each iteration:

1. Marks all not-yet-finished requests as `RECEIVED` (i.e. "to be processed this round").
2. Calls `committer.commit(unmodifiableCollection(requests))`.
3. Marks any request still in `RECEIVED` after the call as `COMMITTED` (the committer didn't say otherwise, so it must have succeeded).
4. Filters out terminal-state requests for the next iteration.

If `committer.commit` itself throws, the operator dies and the job restarts — at which point the recovery path (Part 4.2) replays the committables.

### 7.2 Idempotency — required by contract

From `Committer.java:30-34`:

> A commit must be idempotent: If some failure occurs in Flink during commit phase, Flink will restart from previous checkpoint and re-attempt to commit all committables. Thus, some or all committables may have already been committed. These CommitRequests must not change the external system and implementers are asked to signal `CommitRequest#signalAlreadyCommitted()`.

`IcebergCommitter` satisfies this via the snapshot-chain walk. The contract is enforceable by inspection: the `flink.max-committed-checkpoint-id` written into each Iceberg snapshot summary is what allows the recovery code to know which checkpointIds were already committed. See `flink-iceberg-sink-connector-deep-dive.md` Part 6 for the deep mechanics.

### 7.3 Per-request signaling

The committer is expected to call exactly one of these methods on **each** request:

| Method | Effect |
|---|---|
| (nothing) | Runtime assumes success after `commit` returns without exception. |
| `signalAlreadyCommitted()` | Terminal success. Doesn't re-execute. Increments `numCommittablesAlreadyCommitted` metric. |
| `retryLater()` | Re-tried on next loop iteration up to `maxRetries`. Increments retry counter. |
| `updateAndRetryLater(newCommT)` | Same as `retryLater` but swaps the committable (e.g. after partial success). |
| `signalFailedWithKnownReason(t)` | Terminal failure, logged, dropped. Doesn't fail the job. |
| `signalFailedWithUnknownReason(t)` | Throws `IllegalStateException` — fails the job. |

Iceberg's committer uses only `signalAlreadyCommitted()` (explicit) and the implicit "no exception → success" path. It doesn't retry inside a single `commit()` call; on a real failure it throws and lets Flink restart the job.

---

## Part 8: Side-by-Side Comparison

| Aspect | Legacy (`FlinkSink`) | Sink V2 (`IcebergSink`) |
|---|---|---|
| User implements | `AbstractStreamOperator` lifecycle in person | `Sink` + `Committer` (data classes, mostly) |
| Topology built by | User code (`DataStream.transform`, `setParallelism`, `setMaxParallelism`) | Runtime translator (`SinkTransformationTranslator`) |
| Where 2PC bookkeeping lives | In every connector (`writeResultsSinceLastSnapshot`, `dataFilesPerCheckpoint`) | In Flink runtime (`CommittableCollector` + `CheckpointCommittableManagerImpl`) |
| Where committables are buffered | User's HashMap in the committer operator | Runtime's `CommittableCollector`, keyed by `(checkpointId, subtaskId)` |
| Operator that calls Iceberg `op.commit()` | `IcebergFilesCommitter.notifyCheckpointComplete` (Iceberg code) | `IcebergCommitter.commit` invoked **by** `CheckpointCommittableManagerImpl.commit:151` (Flink runtime calls Iceberg code) |
| Parallelism enforcement for committer | User must call `setParallelism(1).setMaxParallelism(1)` in builder | Runtime infers from upstream; Iceberg upstream uses `.global()` to force fan-in to one |
| Pre-commit staging (Avro manifests) | Inside committer's `snapshotState` | Inside `IcebergWriteAggregator.prepareSnapshotPreBarrier` (a separate operator from writer **and** committer) |
| Recovery is invoked when | Inside `initializeState`, user code walks Iceberg + state | Inside `CommitterOperator.initializeState` (runtime), Iceberg `commit` is called with restored requests |
| `CommitRequest` API | none | rich state machine: `signalAlreadyCommitted` / `retryLater` / `updateAndRetryLater` / `signalFailedWithKnownReason` / `signalFailedWithUnknownReason` |
| Built-in retry loop | none (user must wrap manually) | `CheckpointCommittableManagerImpl.commit` retries up to `SinkOptions.COMMITTER_RETRIES` |
| Topology hooks | none (user free-form) | `SupportsPreWriteTopology`, `SupportsPreCommitTopology`, `SupportsPostCommitTopology` |
| Concurrent-execution-attempts support | n/a | mixin `SupportsConcurrentExecutionAttempts` |
| State migration | full ad-hoc state (`ListState<SortedMap<Long, byte[]>>`) | runtime-managed `committableCollectorState` (see `STREAMING_COMMITTER_RAW_STATES_DESC` and `SinkV1WriterCommittableSerializer` for V1→V2 migration path) |
| Where the writer emits | `FlinkWriteResult(ckpId, WriteResult)` — connector-defined | `CommittableSummary` + `CommittableWithLineage<WriteResult>` — runtime-defined |
| Where Avro staging lives | committer operator | dedicated **pre-commit aggregator** operator |

---

## Part 9: Where the "Global Committer" Fits (and Why Iceberg Doesn't Use It for Data Commits)

Flink has **two** commit-side runtime operators:

| Operator | File | Used when |
|---|---|---|
| `CommitterOperator` | `flink-runtime/.../sink/CommitterOperator.java` | Default. Commits per-subtask, in parallel with other commit subtasks. Triggered by `notifyCheckpointComplete`. |
| `GlobalCommitterOperator` | `flink-runtime/.../sink/GlobalCommitterOperator.java` | When the sink calls `StandardSinkTopologies.addGlobalCommitter(...)`. Always parallelism=1. Can commit either on `notifyCheckpointComplete` (no upstream committer) or `processElement` (when upstream is a committer that already provides ack-implicit ordering). |

Iceberg's data commits use **`CommitterOperator`**, *not* `GlobalCommitterOperator`. The way it forces parallelism-1 is by `.global()` on the pre-commit aggregator output (`IcebergSink.java:325`):

```java
return writeResults
    .global()
    .transform(preCommitAggregatorUid, typeInformation, new IcebergWriteAggregator(tableLoader))
    .uid(preCommitAggregatorUid)
    .setParallelism(1)
    .setMaxParallelism(1)
    .global();   // <-- and a SECOND .global() so the downstream CommitterOperator
                 //     also receives all committables on subtask 0
```

So even though the runtime instantiates `CommitterOperator` per the downstream parallelism, all `CommittableMessage`s land on subtask 0. The two `.global()` calls plus the comment ("Once upstream Flink provides the capability of setting committer operator parallelism to 1, this can be removed.") are a workaround.

Iceberg **does** use `GlobalCommitterOperator` indirectly via `addPostCommitTopology` when `compactMode = true`. There the post-commit graph contains a `CommittableToTableChangeConverter.process(...)` rather than a real `GlobalCommitter` — see `IcebergSink.java:250-295`. That path is for compaction maintenance, not the data commit.

### 9.1 `GlobalCommitterOperator` commit timing

For completeness, here is when `GlobalCommitterOperator` would invoke `commit(...)` (it doesn't apply to Iceberg's data path):

```java
// GlobalCommitterOperator.java:200-205
public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    if (!commitOnInput) {
        commit(checkpointId);
    }
}

// GlobalCommitterOperator.java:219-230
public void processElement(StreamRecord<CommittableMessage<CommT>> element) throws Exception {
    committableCollector.addMessage(element.getValue());
    if (commitOnInput) {
        commit(element.getValue().getCheckpointIdOrEOI());
    }
}
```

`commitOnInput` is `true` when there is **already a `CommitterOperator` upstream** — in that case, by the time committables show up, the upstream committer has implicitly received `notifyCheckpointComplete` and the global committer can commit synchronously on receipt. When `commitOnInput` is `false` (no upstream committer, streaming, checkpointing enabled), the global committer must still wait for `notifyCheckpointComplete`. This logic lives in `GlobalCommitterTransformationTranslator.translateInternal:77`:

```java
boolean commitOnInput = batch || !checkpointingEnabled || hasUpstreamCommitter(inputStream);
```

---

## Part 10: Topology Translation — How `IcebergSink` Becomes Operators

The user-level call:

```java
inputDataStream.sinkTo(new IcebergSink(...))
```

creates a single `SinkTransformation` in the `StreamGraph`. At `StreamGraph → JobGraph` translation time, `SinkTransformationTranslator` (`flink-runtime/src/main/java/org/apache/flink/streaming/runtime/translators/SinkTransformationTranslator.java`) expands it. Specifically `addCommittingTopology` at line 260-308:

```
                                                       ┌─ inputStream
                                                       │
                if sink instanceof SupportsPreWriteTopology:
                     adjustTransformations(inputStream, sink::addPreWriteTopology, ...)
                                                       │
                                                       ▼  (prewritten)

                if sink instanceof SupportsCommitter:
                     addCommittingTopology(sink, prewritten):

                         if sink instanceof SupportsPreCommitTopology:
                             writerResult = addWriter(sink, inputStream, writeResultTypeInfo)
                                              │
                                              ▼
                             precommitted = adjustTransformations(
                                                writerResult,
                                                preCommittingSink::addPreCommitTopology,
                                                ...)
                                              │
                                              ▼
                         else:
                             precommitted = addWriter(sink, inputStream, committableTypeInfo)
                                              │
                                              ▼
                         committed = adjustTransformations(
                                          precommitted,
                                          pc -> pc.transform(COMMITTER_NAME,
                                                             committableTypeInformation,
                                                             new CommitterOperatorFactory<>(
                                                                 committingSink,
                                                                 isBatchMode,
                                                                 isCheckpointingEnabled)),
                                          ...)
                                          │
                                          ▼
                         if sink instanceof SupportsPostCommitTopology:
                             postcommitted = addFailOverRegion(committed)
                             adjustTransformations(
                                 postcommitted,
                                 pc -> sink.addPostCommitTopology(pc), ...)

                else:    (no committer — just a writer)
                     adjustTransformations(
                         prewritten,
                         input -> input.transform(WRITER_NAME, noOutput, new SinkWriterOperatorFactory<>(sink)),
                         ...)
```

`addWriter` (`SinkTransformationTranslator.java:310-326`) is the call that materializes the `SinkWriterOperator`:

```java
private <WriteResultT> DataStream<CommittableMessage<WriteResultT>> addWriter(
        Sink<T> sink,
        DataStream<T> inputStream,
        TypeInformation<CommittableMessage<WriteResultT>> typeInformation) {
    DataStream<CommittableMessage<WriteResultT>> written =
            adjustTransformations(
                    inputStream,
                    input ->
                            input.transform(
                                    ConfigConstants.WRITER_NAME,
                                    typeInformation,
                                    new SinkWriterOperatorFactory<>(sink)),
                    false,
                    sink instanceof SupportsConcurrentExecutionAttempts);
    return addFailOverRegion(written);
}
```

What ends up in the `JobGraph` for Iceberg V2:

```
  +-------------+     +---------------------+     +------------------------+     +----------------------------+     +------------------------------------------+
  | inputStream | ──▶ | distributeDataStream | ──▶ | SinkWriterOperator     | ──▶ | IcebergWriteAggregator     | ──▶ | CommitterOperator(IcebergCommitter)      |
  | (RowData)   |     | (Sink's              |     |  wraps IcebergSinkWriter│     |  (pre-commit topology;    |     | (Flink runtime; calls committer.commit) |
  +-------------+     | addPreWriteTopology) |     |  parallelism=writer)   |     |  parallelism=1)            |     |  (parallelism inherited, but every msg   |
                      +---------------------+     +------------------------+     +----------------------------+     |   lands on subtask 0 due to .global())   |
                                                                                                                   +------------------------------------------+
                                                                                                                                       │
                                                                                       if compactMode:                                 │
                                                                                                                       ┌──────────────┴───────────────┐
                                                                                                                       ▼                              ▼
                                                                                              CommittableToTableChangeConverter         (nothing — sink ends here in
                                                                                              (post-commit, parallelism=1)               non-compact mode)
                                                                                                          │
                                                                                                          ▼
                                                                                                 TableMaintenance pipeline
```

Notable: the writer-to-aggregator edge and aggregator-to-committer edge are both `BATCH` exchange mode because of `addFailOverRegion` (`SinkTransformationTranslator.java:331-338`), which inserts a `PartitionTransformation` with `StreamExchangeMode.BATCH`. This is a no-op in streaming but materializes the output in batch mode so an upstream failure doesn't lose downstream-committed state.

Also: `disallowUnalignedCheckpoint` (line 218-250) walks the sink subtree and disables unaligned checkpoints on all incoming edges of the writer and below. This is mandatory because `notifyCheckpointComplete` requires that all committables have arrived at the committer **before** the corresponding barrier — and unaligned checkpoints would let barriers overtake records, violating that requirement.

---

## Part 11: Failure-Mode Catalog Across the Checkpoint Lifecycle

This section enumerates every failure point on the checkpoint timeline and walks the recovery code for both architectures. Each failure mode answers four questions explicitly:

1. **What is the state of the system at the moment of failure?** (what's in memory, what's durable, what's on Iceberg)
2. **Who detects the failure and triggers recovery?** (CheckpointCoordinator, ResourceManager, etc.)
3. **Who calls the recovery handler and from where in the runtime?** (the caller chain from the JobMaster mailbox down to the operator method)
4. **What does the handler do, step by step, with which state?**

Failure modes are ordered most-critical first. **F1** is the canonical "lost notify" — the failure that most stresses 2PC correctness; **F8** is the most benign (orphan files, no commit issue).

### 11.1 Prerequisite — lifecycle hooks: when each one is called, by whom

The recovery logic in both architectures hangs off these standard `StreamOperator` lifecycle hooks. Knowing who calls them and on which thread is the prerequisite for reading the failure-mode walks.

| Hook | Caller chain (top → operator) | When | Thread | What it does in our sinks |
|---|---|---|---|---|
| `initializeState(StateInitializationContext)` | `StreamTask.beforeInvoke` → `OperatorChain.initializeStateAndOpenOperators` → each `StreamOperatorStateHandler.initializeOperatorState` → `AbstractStreamOperator.initializeState` → **user `initializeState` override** | Once per task attempt, before any record is processed. On a fresh start, called with `context.isRestored() == false`. On restart, called with restored state handles. | Task mailbox thread, during task startup. | **Legacy**: `IcebergFilesCommitter.initializeState` reads `checkpointsState` + `jobIdState`, walks Iceberg snapshot chain, **may issue a recovery commit right here**. **V2**: `CommitterOperator.initializeState` instantiates `IcebergCommitter` via the `committerSupplier`, loads `committableCollectorState`, and **calls `commitAndEmitCheckpoints(restoredCheckpointId)`** — which calls `IcebergCommitter.commit`. |
| `processElement(StreamRecord<T>)` | Upstream operator's `Output.collect` → routed by `OperatorChain`'s chained-output → operator's `processElement` | Each input record. | Mailbox thread. | **Legacy**: `IcebergFilesCommitter.processElement` appends `FlinkWriteResult` into `writeResultsSinceLastSnapshot[checkpointId]`. **V2**: `CommitterOperator.processElement` calls `committableCollector.addMessage(msg)`. |
| `prepareSnapshotPreBarrier(long checkpointId)` | `StreamTask.performCheckpoint` → `SubtaskCheckpointCoordinatorImpl.checkpointState` → `OperatorChain.prepareSnapshotPreBarrier` → operator's `prepareSnapshotPreBarrier` | When the JM-issued checkpoint trigger reaches a source subtask and the barrier starts propagating. Called immediately before the operator's output emits the barrier downstream. | Mailbox thread. | **Legacy writer (`IcebergStreamWriter`)**: closes current `TaskWriter` and emits `FlinkWriteResult(N, result)`. **V2 writer**: not used directly — `SinkWriterOperator.prepareSnapshotPreBarrier` calls `sinkWriter.flush(false)` then emits `CommittableSummary` + `CommittableWithLineage`. **V2 aggregator (`IcebergWriteAggregator`)**: writes Avro manifest and emits `IcebergCommittable`. |
| `snapshotState(StateSnapshotContext)` | `SubtaskCheckpointCoordinatorImpl.checkpointState` → `OperatorChain.snapshotState` → `StreamOperatorStateHandler.snapshotState` → operator's `snapshotState` | Phase-1 of checkpoint N. Runs after `prepareSnapshotPreBarrier` and the barrier emission. | Mailbox thread for the synchronous portion; the returned `RunnableFuture` runs on the `AsyncOperationsThreadPool`. | **Legacy**: `IcebergFilesCommitter.snapshotState` calls `writeToManifestUptoLatestCheckpoint(N)` (writes Avro manifest to Iceberg's metadata dir) and persists `dataFilesPerCheckpoint` + `flinkJobId` to Flink state. **V2**: `CommitterOperator.snapshotState` persists a deep copy of `committableCollector` into `committableCollectorState`. |
| `notifyCheckpointComplete(long checkpointId)` | JM's `CheckpointCoordinator.receiveAcknowledgeMessage` → on full ACK, broadcasts via `Execution.notifyCheckpointComplete` → RPC to `TaskExecutor.confirmCheckpoint` → `Task.notifyCheckpointComplete` → `StreamTask.notifyCheckpointCompleteAsync` (enqueues a mailbox letter) → mailbox runs `OperatorChain.notifyCheckpointComplete` → operator's `notifyCheckpointComplete` | After all subtasks have ACKed checkpoint N and the JM has persisted `CompletedCheckpoint(N)` in `CompletedCheckpointStore`. **Best-effort delivery** — no JM retry if the RPC drops. | Mailbox thread. | **Legacy**: `IcebergFilesCommitter.notifyCheckpointComplete` calls `commitUpToCheckpoint(...)` if `N > maxCommittedCheckpointId`. **V2**: `CommitterOperator.notifyCheckpointComplete` calls `commitAndEmitCheckpoints(Math.max(lastCompletedCheckpointId, N))`. |
| `notifyCheckpointAborted(long checkpointId)` | Same path as `notifyCheckpointComplete` but for `CheckpointCoordinator.abortPendingCheckpoint` → `Execution.notifyCheckpointAborted`. | When `CheckpointCoordinator` decides N is aborted (timeout, declined, expired, or a subtask failure). | Mailbox thread. | Both architectures **inherit the no-op default** from `AbstractStreamOperator.java:462-464`. See section 11.12 for why this is safe. |
| `endInput()` | `StreamTask.runMailboxLoop` on EOF → `OperatorChain.endInput` → operator's `endInput` | Once per task, on end-of-stream. Streaming jobs hit this on `stop-with-savepoint --drain`; batch jobs hit it as the normal terminal. | Mailbox thread. | **Legacy**: `IcebergFilesCommitter.endInput` commits everything in `dataFilesPerCheckpoint` using a sentinel `END_INPUT_CHECKPOINT_ID = Long.MAX_VALUE`. **V2**: `CommitterOperator.endInput` calls `commitAndEmitCheckpoints(Long.MAX_VALUE)` only when checkpointing is disabled or batch mode is set; otherwise the regular `notifyCheckpointComplete` path handles draining. |

### 11.2 The phase timeline

The numbered failure points F1-F8 are placed where they can occur. Highest-priority first this time: F1 = the most critical, F8 = the most benign.

```
   Writers (parallel)             Pre-commit aggregator (V2 only)         Committer (par=1)              JobManager
    │                                          │                              │                              │
    │ ─── F7: crash before barrier reaches writer (mid-record write)
    │   data files for ckpN partially written to object store → orphan files
    │
    │ prepareSnapshotPreBarrier(N)             │                              │                              │
    │   writer.complete()                      │                              │                              │
    │   emit committables for N                │                              │                              │
    │ ──────────────────────────────────────▶  │                              │                              │
    │ ─── F8: crash AFTER writer.complete() but BEFORE downstream sees the records
    │   records were in transit → lost; writer re-does work on restart        │
    │                                          │ processElement: buffer       │                              │
    │                                          │   WriteResults for N         │                              │
    │ inject barrier                           │ prepareSnapshotPreBarrier(N) │                              │
    │                                          │   write Avro manifest        │                              │
    │                                          │   emit IcebergCommittable    │                              │
    │                                          │ ──────────────────────────▶  │                              │
    │                                          │                              │ processElement: buffer       │
    │                                          │                              │   CommittableMessages for N  │
    │                                          │                              │                              │
    │                                          │                              │ snapshotState(N)             │
    │                                          │                              │   persist collector state    │
    │                                          │                              │                              │
    │ acknowledgeCheckpoint(N) ───────────────────────────────────────────────────────────────────────────▶ │
    │ ─── F6: ANOTHER subtask fails to ACK → checkpoint aborts at JM
    │                                                                                                       │ allTasksAcked
    │ ─── F5: JM crashes BEFORE CompletedCheckpoint(N) is persisted (and BEFORE broadcasting)
    │                                                                                                       │ persist CompletedCheckpoint(N)
    │ ─── F1: JM crashes AFTER persisting CompletedCheckpoint(N) but BEFORE sending notifyCheckpointComplete
    │                                                                                                       │ broadcast notifyCheckpointComplete(N)
    │                                                                          ◀───────────────────────────│
    │ ─── F3: notifyCheckpointComplete RPC drops to ONE subtask (committer never gets called for N)
    │                                                                          │ notifyCheckpointComplete(N)│
    │                                                                          │   commitAndEmitCheckpoints │
    │                                                                          │     committer.commit(req)  │
    │ ─── F2: committer crashes MID-COMMIT (Iceberg commits some but not all checkpoints in batch)
    │                                                                          │     succeeded for K        │
    │                                                                          │     throws for K+1         │
    │                                                                          │   operator dies → task fails
    │ ─── F4: commit succeeded on Iceberg side, but operator dies BEFORE returning to JM (caller doesn't know)
    │                                                                          │
```

### 11.3 Summary catalog (priority-ordered)

| # | What happens | Restart restores from | Severity | Recovery primary mechanism |
|---|---|---|---|---|
| **F1** | JM crash after persisting CompletedCheckpoint(N), before broadcasting notify | **N** | **Critical** — without correct handling, all of N's data is lost | Legacy: `initializeState` reads `dataFilesPerCheckpoint[N]`, walks Iceberg snapshots → commits at restore time. V2: `CommitterOperator.initializeState` restores `committableCollectorState`, calls `commitAndEmitCheckpoints(N)` → `IcebergCommitter.commit` |
| **F2** | Committer fails partway through a multi-checkpoint batch (e.g. delta-txn: K committed, K+1 throws) | N | **Critical** — without idempotency, K is re-committed on retry → duplicates | Operator dies → task restart → `initializeState` retries the same batch. `IcebergCommitter.commit` walks Iceberg snapshots, finds K committed, marks request K as `signalAlreadyCommitted`, retries K+1 |
| **F3** | `notifyCheckpointComplete(N)` RPC drops to the committer subtask | n/a (no restart) | High — without "next ckp subsumes", N is delayed indefinitely | Next `notifyCheckpointComplete(N+1)` commits both N and N+1 via `headMap(checkpointId, true)` / `getCheckpointCommittablesUpTo(checkpointId)` |
| **F4** | Iceberg `op.commit()` succeeded but operator dies before runtime sees the return | N | High — without idempotency, the same Iceberg snapshot is created twice → tx conflicts | Same as F1 + F2: snapshot-chain walk on restart sees Iceberg already has the snapshot with `flink.max-committed-checkpoint-id = N`; recovery is a no-op |
| **F5** | JM crashes before persisting CompletedCheckpoint(N) | M-1 (last successful) | Low — N never "happened" from any perspective | Restart redoes everything from M-1. Writer rewrites data for N. |
| **F6** | Checkpoint N aborts at JM (e.g. one subtask failed during alignment) | M-1 (last successful) | Low — implicit cleanup via region failover | Region failover restarts the whole sink region. Buffered in-memory state for N is dropped. `notifyCheckpointAborted` not needed. |
| **F7** | Writer crashes mid-record before any barrier | M-1 | Low — only consequence is orphan data files | Iceberg orphan-file cleanup (separate maintenance task) |
| **F8** | Writer completes `prepareSnapshotPreBarrier(N)` and emits committables, then crashes before downstream sees them | M-1 | Low — similar to F7 | Records lost in transit. Writer redoes work on restart → more orphan files. Iceberg snapshot history unchanged. |

The cross-cutting invariant for F1-F4 is: **Iceberg's snapshot summary `flink.max-committed-checkpoint-id` is the only authoritative `lastCommitted` cursor**, never anything in Flink state. Both architectures consult it on restart.

---

### 11.4 F1 — JM crashes after persisting `CompletedCheckpoint(N)`, before broadcasting `notifyCheckpointComplete`

**Scenario.** The JobManager's `CheckpointCoordinator` received ACKs from every subtask for checkpoint N. It atomically persisted `CompletedCheckpoint(N)` into `CompletedCheckpointStore` (ZooKeeper / K8s ConfigMap). The next step would be to broadcast `notifyCheckpointComplete(N)` to every operator subtask via RPC. The JM crashes before issuing that broadcast.

**State at the moment of failure.**

| Where | State |
|---|---|
| Writer subtask | TaskWriter for N+1 is open (new writer was created right after the barrier). Data being written is for N+1. |
| Aggregator (V2) | In-memory `results` set is empty for N (already emitted). Buffering for N+1 begins. |
| Committer in-memory | **Legacy**: `dataFilesPerCheckpoint` includes `N → manifestBytes`. `maxCommittedCheckpointId` is still M-1 (`notifyCheckpointComplete(N)` never ran). **V2**: `committableCollector` includes `N`'s `CheckpointCommittableManager`. `lastCompletedCheckpointId = M-1`. |
| Flink durable state | `CompletedCheckpoint(N)` is persisted. Includes the committer's snapshot state at the end of `snapshotState(N)`. **Legacy**: `checkpointsState` contains `dataFilesPerCheckpoint` with N's entry. **V2**: `committableCollectorState` contains N's committables. |
| Iceberg table | **NO** snapshot with `flink.max-committed-checkpoint-id = N`. Latest matching snapshot summary still shows M-1 (or earlier). Avro manifests for N exist in the metadata location (written by `snapshotState(N)`). |

**Who triggers recovery.** New JM is elected (HA). The new JM reads `CompletedCheckpointStore`, sees N, and restores the job from CompletedCheckpoint(N). All operators are deployed with restored state handles. For each task, `StreamTask.beforeInvoke` runs, which invokes `OperatorChain.initializeStateAndOpenOperators`.

**Legacy recovery walk.**

`IcebergFilesCommitter.initializeState` is called once by the runtime as part of operator init. The call chain is:

```
StreamTask.beforeInvoke
  → OperatorChain.initializeStateAndOpenOperators
    → StreamOperatorStateHandler.initializeOperatorState
      → AbstractStreamOperator.initializeState
        → IcebergFilesCommitter.initializeState   ← user override; runs on mailbox thread
```

Inside the override:

1. `flinkJobId = getContainingTask().getEnvironment().getJobID().toString()` — the **current** run's job ID.
2. `operatorUniqueId = getRuntimeContext().getOperatorUniqueID()`.
3. Loads two Flink `ListState`s previously written in `snapshotState(N)`:
   - `checkpointsState: ListState<SortedMap<Long, byte[]>>` — `dataFilesPerCheckpoint` at N.
   - `jobIdState: ListState<String>` — the **prior** run's `flinkJobId`.
4. `restoredFlinkJobId = jobIdState.get().iterator().next()`.
5. `maxCommittedCheckpointId = SinkUtil.getMaxCommittedCheckpointId(table, restoredFlinkJobId, operatorUniqueId, branch)`. Walks Iceberg snapshots backward looking for a matching jobId+operatorId with `flink.max-committed-checkpoint-id` set. Because N was never committed, this returns M-1 (or `-1` if there's no prior snapshot for this committer identity).
6. `uncommittedDataFiles = checkpointsState.get().iterator().next().tailMap(maxCommittedCheckpointId, false)` — entries strictly greater than M-1. Includes `N`.
7. `commitUpToCheckpoint(uncommittedDataFiles, restoredFlinkJobId, operatorUniqueId, maxUncommittedCheckpointId=N)` — re-reads the Avro manifests from Iceberg metadata, rebuilds `WriteResult`, calls `AppendFiles` / `RowDelta` / `ReplacePartitions`, sets snapshot summary properties including `flink.max-committed-checkpoint-id=N`, and calls `op.commit()`.

```java
// v1.20/.../IcebergFilesCommitter.java:142-201
@Override
public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    this.operatorUniqueId = getRuntimeContext().getOperatorUniqueID();
    // ... open table, initialize manifestOutputFileFactory ...
    this.maxCommittedCheckpointId = INITIAL_CHECKPOINT_ID;  // = -1

    this.checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
    this.jobIdState = context.getOperatorStateStore().getListState(JOB_ID_DESCRIPTOR);

    if (context.isRestored()) {
        String restoredFlinkJobId = jobIdState.get().iterator().next();
        this.maxCommittedCheckpointId =
            SinkUtil.getMaxCommittedCheckpointId(
                table, restoredFlinkJobId, operatorUniqueId, branch);

        NavigableMap<Long, byte[]> uncommittedDataFiles =
            Maps.newTreeMap(checkpointsState.get().iterator().next())
                .tailMap(maxCommittedCheckpointId, false);

        if (!uncommittedDataFiles.isEmpty()) {
            long maxUncommittedCheckpointId = uncommittedDataFiles.lastKey();
            // RECOVERY COMMIT — runs synchronously here, on mailbox thread
            commitUpToCheckpoint(
                uncommittedDataFiles,
                restoredFlinkJobId, operatorUniqueId,
                maxUncommittedCheckpointId);
        }
    }
}
```

**V2 recovery walk.**

`CommitterOperator.initializeState` is called once by the runtime through the same call chain:

```
StreamTask.beforeInvoke
  → OperatorChain.initializeStateAndOpenOperators
    → StreamOperatorStateHandler.initializeOperatorState
      → AbstractStreamOperator.initializeState
        → CommitterOperator.initializeState   ← runs on mailbox thread
```

Inside the override:

1. `OptionalLong checkpointId = context.getRestoredCheckpointId()` — present with value N.
2. Constructs `CommitterInitContextImpl` and calls `committer = committerSupplier.apply(initContext)`. The `committerSupplier` was constructed inside `CommitterOperatorFactory.createStreamOperator`: it closes over the user's `Sink` instance and calls `((SupportsCommitter<CommT>) sink).createCommitter(ctx)`. For our case that's `IcebergSink.createCommitter` which returns a new `IcebergCommitter` (opens the Iceberg table, builds the worker thread pool).
3. Initializes `committableCollectorState` as a `SimpleVersionedListState` backed by the Flink operator state store.
4. `committableCollectorState.get().forEach(cc -> committableCollector.merge(cc))` — rebuilds the in-memory `CommittableCollector` from the restored state. This collector now has a `CheckpointCommittableManager` for N with all of N's `CommittableWithLineage<IcebergCommittable>` records.
5. `lastCompletedCheckpointId = N`.
6. `commitAndEmitCheckpoints(N)` — iterates pending checkpoints `<= N` via `getCheckpointCommittablesUpTo(N)`. For each, calls `mgr.commit(committer, maxRetries)` → `CheckpointCommittableManagerImpl.commit:151` → `committer.commit(unmodifiableCollection(requests))` → **`IcebergCommitter.commit`**.
7. Inside `IcebergCommitter.commit`: `maxCommittedCheckpointId = SinkUtil.getMaxCommittedCheckpointId(table, last.jobId(), last.operatorId(), branch)` returns M-1. Requests with ckp ≤ M-1 get `signalAlreadyCommitted` (none in this case). Uncommitted = N. Calls `commitPendingRequests`, which reads the Avro manifest, calls `AppendFiles`/`RowDelta`, and commits to Iceberg with `flink.max-committed-checkpoint-id=N`.

```java
// flink-runtime/.../CommitterOperator.java:120-141
@Override
public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    OptionalLong checkpointId = context.getRestoredCheckpointId();
    CommitterInitContext initContext =
        new CommitterInitContextImpl(getRuntimeContext(), metricGroup, checkpointId);
    committer = committerSupplier.apply(initContext);     // new IcebergCommitter(...)
    committableCollectorState = new SimpleVersionedListState<>(
        context.getOperatorStateStore().getListState(STREAMING_COMMITTER_RAW_STATES_DESC),
        new CommittableCollectorSerializer<>(committableSerializer, ...));
    if (checkpointId.isPresent()) {
        committableCollectorState.get().forEach(cc -> committableCollector.merge(cc));
        lastCompletedCheckpointId = checkpointId.getAsLong();
        // RECOVERY COMMIT — runs synchronously here, on mailbox thread
        commitAndEmitCheckpoints(lastCompletedCheckpointId);
    }
}
```

```java
// iceberg/v2.1/.../IcebergCommitter.java:114-138
@Override
public void commit(Collection<CommitRequest<IcebergCommittable>> commitRequests)
        throws IOException, InterruptedException {
    if (commitRequests.isEmpty()) return;
    NavigableMap<Long, CommitRequest<IcebergCommittable>> commitRequestMap = Maps.newTreeMap();
    for (CommitRequest<IcebergCommittable> request : commitRequests) {
        commitRequestMap.put(request.getCommittable().checkpointId(), request);
    }
    IcebergCommittable last = commitRequestMap.lastEntry().getValue().getCommittable();
    long maxCommittedCheckpointId =
        SinkUtil.getMaxCommittedCheckpointId(table, last.jobId(), last.operatorId(), branch);
    commitRequestMap.headMap(maxCommittedCheckpointId, true)
        .values().forEach(CommitRequest::signalAlreadyCommitted);
    NavigableMap<Long, CommitRequest<IcebergCommittable>> uncommitted =
        commitRequestMap.tailMap(maxCommittedCheckpointId, false);
    if (!uncommitted.isEmpty()) {
        commitPendingRequests(uncommitted, last.jobId(), last.operatorId());
    }
}
```

**Why exactly-once is preserved.** The Iceberg snapshot chain is the single source of truth for `lastCommitted`. The Flink-side restored `dataFilesPerCheckpoint` / `committableCollectorState` may be ahead of what Iceberg has (Flink thinks N might be committed; Iceberg knows it isn't). The walk reconciles, then commits exactly what's missing.

---

### 11.5 F2 — Committer fails partway through a multi-checkpoint commit batch

**Scenario.** Several checkpoints (K, K+1, K+2) are pending commit (perhaps because of F3-style RPC drops earlier). `notifyCheckpointComplete(K+2)` fires; the committer iterates them. Commit succeeds for K (a new Iceberg snapshot exists with `flink.max-committed-checkpoint-id = K`). The commit for K+1 throws (transient catalog error, network failure to S3, concurrent modification conflict, …).

**State at the moment of failure.**

| Where | State |
|---|---|
| Committer in-memory | **Legacy**: `dataFilesPerCheckpoint` still has K, K+1, K+2 because `pendingMap.clear()` runs only at the end of `commitUpToCheckpoint`, AFTER all commits — exception aborts that. **V2**: `CommittableCollector` has K, K+1, K+2 unchanged (commit threw before `committableCollector.remove(mgr)` runs for K+1 / K+2; but K was already removed by `committableCollector.remove(checkpointManager)` — see Cleanup below). |
| Flink durable state | Last `snapshotState` saved at some checkpoint S ≥ K+2 (or earlier, if no later checkpoint has succeeded). |
| Iceberg table | **Has a snapshot for K** (`flink.max-committed-checkpoint-id = K`). Snapshots for K+1 and K+2 do **not** exist. |

**Cleanup detail for V2**: `CommitterOperator.commitAndEmitCheckpoints` removes a manager only after `commitAndEmit(mgr)` returns successfully:

```java
// flink-runtime/.../CommitterOperator.java:164-174
private void commitAndEmitCheckpoints(long checkpointId) throws IOException, InterruptedException {
    lastCompletedCheckpointId = checkpointId;
    for (CheckpointCommittableManager<CommT> checkpointManager :
            committableCollector.getCheckpointCommittablesUpTo(checkpointId)) {
        commitAndEmit(checkpointManager);
        committableCollector.remove(checkpointManager);  // <─ K is removed; throw on K+1 skips this for K+1
    }
}
```

K's manager is removed from the in-memory collector. K+1 and K+2 remain. The exception bubbles up out of `commitAndEmitCheckpoints` → `notifyCheckpointComplete` (or `initializeState`) → operator dies → task fails.

**Who triggers recovery.** Operator throws from a mailbox letter. `StreamTask.runMailboxLoop` catches it and fails the task with `Task.failExternally`. The `Execution` reports the failure to `JobMaster`, which invokes `DefaultScheduler.handleTaskFailure`. The pipelined-region failover strategy restarts the entire sink region (writer + aggregator + committer) from the last successful checkpoint S.

**Legacy recovery walk.**

Restart deploys all sink-region tasks. `IcebergFilesCommitter.initializeState` runs (caller chain same as F1):

1. Loads `checkpointsState` from CompletedCheckpoint(S). This includes ALL pending K, K+1, K+2 entries (because the legacy committer's in-memory `dataFilesPerCheckpoint` was persisted to state at `snapshotState(S)` BEFORE the failed commit attempt).
2. `SinkUtil.getMaxCommittedCheckpointId(...)` walks Iceberg backward. The first matching snapshot has `flink.max-committed-checkpoint-id = K` (the partial-success result). Returns K.
3. `tailMap(K, false)` = {K+1, K+2}. K is implicitly skipped because it's `<= maxCommittedCheckpointId`.
4. `commitUpToCheckpoint(uncommitted={K+1, K+2}, ..., maxUncommittedCheckpointId=K+2)` retries K+1 and K+2 only.

**V2 recovery walk.**

Restart deploys all sink-region tasks. `CommitterOperator.initializeState`:

1. Loads `committableCollectorState` from CompletedCheckpoint(S). This includes all of K, K+1, K+2 — because the V2 committer's `snapshotState(S)` was called BEFORE the failed `notifyCheckpointComplete`, which means the persisted collector still had all three.

   **Important subtlety**: even though K was removed from the in-memory collector during the failed commit attempt, that mutation is in-memory only. The persisted state was snapshotted earlier and still has K. On restart, K reappears in the collector.

2. `commitAndEmitCheckpoints(restoredCheckpointId)` iterates managers K, K+1, K+2.
3. For each manager, calls `mgr.commit(committer, maxRetries)` → `IcebergCommitter.commit` with the full batch {K, K+1, K+2}.
4. `SinkUtil.getMaxCommittedCheckpointId` returns K. K is marked `signalAlreadyCommitted` (so `CommitRequestImpl` state goes to `COMMITTED`). K+1, K+2 go through `commitPendingRequests`. New Iceberg snapshot(s) created.

```java
// flink-runtime/.../sink/committables/CheckpointCommittableManagerImpl.java:145-161
@Override
public void commit(Committer<CommT> committer, int maxRetries)
        throws IOException, InterruptedException {
    Collection<CommitRequestImpl<CommT>> requests =
        getPendingRequests().collect(Collectors.toList());
    for (int retry = 0; !requests.isEmpty() && retry <= maxRetries; retry++) {
        requests.forEach(CommitRequestImpl::setSelected);          // mark RECEIVED
        committer.commit(Collections.unmodifiableCollection(requests));
        requests.forEach(CommitRequestImpl::setCommittedIfNoError); // only RECEIVED→COMMITTED
        requests = requests.stream().filter(r -> !r.isFinished()).collect(Collectors.toList());
    }
    if (!requests.isEmpty()) {
        throw new IOException(String.format(
            "Failed to commit %s committables after %s retries: %s", ...));
    }
}
```

**Why the inner retry loop alone is NOT sufficient.** When `committer.commit` throws, `setCommittedIfNoError` is **skipped** for any request that was already successful in this call (K). The exception terminates the loop. So the retry happens via the task-failure → restart path, not the in-memory retry loop. The in-memory retry loop only handles cases where the committer **returns** (no exception) but some requests are still in `RECEIVED` or `RETRY` state — Iceberg's committer doesn't use those signals, so the inner retry loop is effectively a no-op for Iceberg.

**Why exactly-once is preserved.** Per-checkpoint `commitOperation` is one Iceberg snapshot per checkpointId (row-delta path) or one merged Iceberg snapshot (`AppendFiles` path) — both atomic by Iceberg's CAS-based commit semantics. There's no in-between state where K is half-committed.

---

### 11.6 F3 — `notifyCheckpointComplete(N)` RPC drops to the committer subtask

**Scenario.** Checkpoint N completes globally. JM broadcasts `notifyCheckpointComplete(N)`. The RPC to the committer's TaskExecutor drops (transient network failure, GC pause exceeds timeout). The committer's `notifyCheckpointComplete(N)` **never runs**. The job continues without restart.

**State.** Same as during steady state, except the committer's `lastCompletedCheckpointId` (V2) or `maxCommittedCheckpointId` (legacy) is still pre-N; pending committables for N sit in the collector / `dataFilesPerCheckpoint`.

**Who detects.** Nobody — `notifyCheckpointComplete` is best-effort. The JM doesn't track delivery success and doesn't retry. The failure is silent.

**Recovery mechanism.** Both architectures **subsume the missed N into the next successful `notifyCheckpointComplete(N+1)` (or N+k, for any k≥1)** via `headMap(checkpointId, true)` / `getCheckpointCommittablesUpTo(checkpointId)`.

**Legacy.**

When `IcebergFilesCommitter.notifyCheckpointComplete(N+1)` fires (caller chain: `CheckpointCoordinator.receiveAcknowledgeMessage` → `Execution.notifyCheckpointComplete` → RPC → `TaskExecutor.confirmCheckpoint` → `Task.notifyCheckpointComplete` → `StreamTask.notifyCheckpointCompleteAsync` → mailbox runs `OperatorChain.notifyCheckpointComplete` → `AbstractStreamOperator.notifyCheckpointComplete` → `IcebergFilesCommitter.notifyCheckpointComplete`):

```java
// v1.20/.../IcebergFilesCommitter.java:228-251
@Override
public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    // Late or duplicate notifies are safe — we always keep maxCommittedCheckpointId monotonic.
    if (checkpointId > maxCommittedCheckpointId) {
        commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, operatorUniqueId, checkpointId);
        this.maxCommittedCheckpointId = checkpointId;
    }
}
```

`commitUpToCheckpoint(..., N+1)` uses `headMap(N+1, true)` which returns `{N, N+1}` — both are committed in this call:

```java
// v1.20/.../IcebergFilesCommitter.java:253-259
private void commitUpToCheckpoint(
        NavigableMap<Long, byte[]> deltaManifestsMap,
        String newFlinkJobId, String operatorId, long checkpointId) throws IOException {
    NavigableMap<Long, byte[]> pendingMap = deltaManifestsMap.headMap(checkpointId, true);
    // ... iterate pendingMap → AppendFiles / RowDelta / ReplacePartitions
}
```

**V2.**

When `CommitterOperator.notifyCheckpointComplete(N+1)` fires (caller chain identical):

```java
// flink-runtime/.../CommitterOperator.java:159-162
@Override
public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    commitAndEmitCheckpoints(Math.max(lastCompletedCheckpointId, checkpointId));
}
```

The `Math.max` guards against out-of-order delivery (e.g., notify(N+1) arrived before notify(N), then notify(N) arrives late — the second call with N would otherwise rewind `lastCompletedCheckpointId`). `commitAndEmitCheckpoints(N+1)` uses `getCheckpointCommittablesUpTo(N+1)`:

```java
// flink-runtime/.../sink/committables/CommittableCollector.java:139-142
public Collection<? extends CheckpointCommittableManager<CommT>> getCheckpointCommittablesUpTo(
        long checkpointId) {
    return new ArrayList<>(checkpointCommittables.headMap(checkpointId, true).values());
}
```

Returns managers for `{N, N+1}`. Both committed in this single notify cycle.

**Why exactly-once is preserved.** Same reason as F1: Iceberg snapshot chain remains the authoritative cursor. If N somehow already got committed (e.g., a partial retry), `IcebergCommitter.commit` (V2) marks it `signalAlreadyCommitted` and the legacy guard `if (checkpointId > maxCommittedCheckpointId)` skips it.

---

### 11.7 F4 — Iceberg commit succeeded but the operator dies before returning OK to runtime

**Scenario.** Inside `IcebergCommitter.commit` (V2) or `commitUpToCheckpoint` (legacy), `op.commit()` succeeded — Iceberg has a new snapshot with `flink.max-committed-checkpoint-id = N`. Before the call returns, the JVM dies (OOM, kernel kill, GC death, host failure). The Flink runtime doesn't see the success.

**State.**

| Where | State |
|---|---|
| Flink durable state | Last `snapshotState(N)` is persisted. Includes N's committables. |
| Iceberg table | Has a snapshot with `flink.max-committed-checkpoint-id = N`. |
| In-memory: post-commit cleanup didn't run | **Legacy**: `pendingMap.clear()` skipped; `FlinkManifestUtil.deleteCommittedManifests` skipped (Avro stays). **V2**: `committableCollector.remove(checkpointManager)` skipped; `setCommittedIfNoError` skipped. |

**Who triggers recovery.** JM detects the operator's failure (heartbeat timeout or RPC error from the dead TaskExecutor), `DefaultScheduler.handleTaskFailure` initiates restart from N (latest CompletedCheckpoint).

**Legacy recovery walk.**

`IcebergFilesCommitter.initializeState`:

1. Loads `checkpointsState` — includes N.
2. `SinkUtil.getMaxCommittedCheckpointId(...)` returns **N** (Iceberg has the snapshot).
3. `tailMap(N, false)` = empty.
4. No recovery commit needed.

**V2 recovery walk.**

`CommitterOperator.initializeState`:

1. Loads `committableCollectorState` — includes N's manager.
2. Calls `commitAndEmitCheckpoints(N)`.
3. For each manager (N), calls `IcebergCommitter.commit({N's requests})`.
4. `SinkUtil.getMaxCommittedCheckpointId(...)` returns N.
5. `commitRequestMap.headMap(N, true)` = `{N}` → marks N's request as `signalAlreadyCommitted`.
6. `tailMap(N, false)` = empty → no `commitPendingRequests` call.
7. `CommitRequestImpl.signalAlreadyCommitted()` sets state to `COMMITTED`.
8. Back in `CheckpointCommittableManagerImpl.commit`, the retry loop filters out the now-terminal request → empty next iteration → loop exits.
9. `commitAndEmitCheckpoints` calls `committableCollector.remove(mgr)` → cleanup of in-memory state.

**Why exactly-once is preserved.** Iceberg snapshot chain shows N is committed; the recovery path detects this and avoids a duplicate Iceberg commit (which would otherwise fail anyway because Iceberg's CAS would reject the conflicting snapshot ID).

**Orphan Avro manifests.** The legacy `FlinkManifestUtil.deleteCommittedManifests` and the V2 `FlinkManifestUtil.deleteCommittedManifests` (best-effort GC of staging Avros after commit) didn't run. The Avro files remain in Iceberg's metadata directory as orphans. Iceberg's orphan-file cleanup maintenance task removes them later. No correctness issue.

---

### 11.8 F5 — JM crashes BEFORE persisting `CompletedCheckpoint(N)`

**Scenario.** All subtasks have ACKed N. JM is about to call `CompletedCheckpointStore.addCheckpoint(N)` (or is mid-call when atomicity isn't yet achieved). JM crashes.

**State.**

| Where | State |
|---|---|
| Flink durable state | No `CompletedCheckpoint(N)` in `CompletedCheckpointStore`. M-1 is the latest. |
| Committer in-memory | `snapshotState(N)` completed → in-memory was just snapshotted — but the snapshot **was not persisted**. From the perspective of restart, that snapshot didn't happen. |
| Iceberg table | No snapshot for N. Avro manifests for N exist (because `snapshotState(N)` did write them — that's a side-effect on the external system, not on Flink state). |

**Who triggers recovery.** New JM is elected. Reads `CompletedCheckpointStore`, sees M-1 as latest, restarts the job from M-1.

**Recovery walk (both architectures).**

Restart restores from M-1. `IcebergFilesCommitter.initializeState` / `CommitterOperator.initializeState` runs with `context.getRestoredCheckpointId() == M-1`.

- **Legacy**: `checkpointsState` reflects state at end of `snapshotState(M-1)`. Pending entries are those ≤ M-1 that haven't been committed. The Iceberg-side cursor walk catches up if M-1 was already committed; otherwise commits M-1.
- **V2**: Same — `committableCollectorState` reflects state at end of `snapshotState(M-1)`. `commitAndEmitCheckpoints(M-1)`.

N is effectively rewound. Writers re-do all work from M-1 onward, including re-emitting committables for N+. Iceberg's `getMaxCommittedCheckpointId` returns the cursor as of the rewind point; new ckp IDs start fresh.

**Orphan handling.** The Avro manifests written by `snapshotState(N)` are orphaned (they reference data files written by writers from before the rewind, which are also orphaned). Iceberg orphan-file cleanup removes them.

**Why exactly-once is preserved.** From the external (Iceberg) viewpoint, N never happened. The new run's commits for N (different snapshot) will succeed because they're distinct from anything pre-existing.

---

### 11.9 F6 — Checkpoint N aborts at the JobManager (partial subtask failure during alignment)

**Scenario.** Some writer subtask S fails during checkpoint N (failed `prepareSnapshotPreBarrier` due to disk full; barrier alignment timed out; etc.). `CheckpointCoordinator.abortPendingCheckpoint(N, reason)` is invoked.

**State.**

| Where | State |
|---|---|
| Writer subtasks ≠ S | Some have completed `prepareSnapshotPreBarrier(N)` and emitted committables; some haven't. |
| Aggregator (V2) | May have buffered some `WriteResult`s for N from the writers that did complete. |
| Committer | May have buffered some `CommittableMessages` (V2) / `FlinkWriteResults` (legacy) for N. |
| Flink durable state | No new snapshot for N. M-1 is latest. |
| Iceberg table | No snapshot for N. Avro for N was NOT written (committer's `snapshotState(N)` does not run for an aborted ckp). |

**Who detects.** `CheckpointCoordinator` itself, based on `CheckpointFailureManager` rules. It calls `Execution.notifyCheckpointAborted` for each subtask.

**Recovery mechanism.** The failing subtask S triggers region failover via `DefaultScheduler.handleTaskFailure` — restarts the whole sink region from M-1 (last successful). Buffered in-memory state for N in writer, aggregator, committer is **lost cleanly** because it was never persisted.

Both architectures rely on the no-op default `notifyCheckpointAborted`:

```java
// flink-runtime/.../AbstractStreamOperator.java:462-464
@Override
public void notifyCheckpointAborted(long checkpointId) throws Exception {
    stateHandler.notifyCheckpointAborted(checkpointId);   // default — no committable cleanup
}
```

The committer subtask receives `notifyCheckpointAborted(N)` via the standard mailbox path (`CheckpointCoordinator.abortPendingCheckpoint` → `Execution.notifyCheckpointAborted` → RPC → `TaskExecutor.abortCheckpointOnBarrier` (or similar) → `Task` → `StreamTask.notifyCheckpointAbortAsync` → mailbox → `OperatorChain.notifyCheckpointAborted` → operator). It's effectively a no-op for the committer — by the time it runs, the region failover is already in motion and the operator will soon be destroyed.

**Why this is safe.** Aborted N's committables only existed in volatile in-memory state. Region failover wipes the operator instance. New instances initialize from M-1's CompletedCheckpoint, which doesn't include any of N's partial committables. The next checkpoint M+k will start cleanly.

**Subtle case**: what if the aborted N's committables had somehow been included in the persisted state of a *later* successful checkpoint M' > N (impossible in the standard flow because `snapshotState(M')` only runs as part of M''s checkpoint procedure; but consider some odd race)? Both architectures' `commit-up-to-checkpoint` semantics would commit those committables alongside M' on the next `notifyCheckpointComplete`. No correctness issue, just delayed visibility — and the question is moot anyway because the writers re-emit fresh committables on restart.

---

### 11.10 F7 — Writer crashes mid-record (before barrier)

**Scenario.** A writer subtask is in `processElement` calling `writer.write(record)`. The Iceberg `TaskWriter` is writing to S3 / HDFS / local FS. The JVM dies. There's no checkpoint barrier active.

**State.**

| Where | State |
|---|---|
| Writer's local memory | Buffered uncommitted rows in the TaskWriter's internal buffers — lost. |
| Object store | Partially-written data files may exist (e.g., S3 multipart upload that completed for the first N parts but not the last). |
| Flink durable state | Last `snapshotState(M-1)` reflects state up to M-1. |
| Iceberg table | No snapshot includes the orphaned partial files. |

**Who triggers recovery.** TaskExecutor detects writer's death (heartbeat timeout or task thread exception). Reports to JM. `DefaultScheduler.handleTaskFailure` initiates region failover. Sink region restarts from M-1.

**Recovery walk.** No special sink code involved. `IcebergSinkWriter` / `IcebergStreamWriter` is reconstructed via the factory; it builds a fresh `TaskWriter`; data after M-1 is reprocessed from upstream. The orphaned partial files are not referenced by any Iceberg snapshot.

**Cleanup.** Orphan files persist until Iceberg's orphan-file cleanup maintenance task removes them. From Iceberg's `RemoveOrphanFiles` action: it scans the table's storage path and deletes files not referenced by any snapshot or manifest, with a configurable age threshold.

**Why no commit issue.** No `notifyCheckpointComplete` was involved. No Avro manifests for any pending checkpoint were written. The crash is invisible to the commit pipeline.

---

### 11.11 F8 — Writer completes/emits committables, then crashes before downstream sees them

**Scenario.** Writer subtask S completed `prepareSnapshotPreBarrier(N)` successfully — `writer.complete()` returned a `WriteResult`; `output.collect(new StreamRecord<>(...))` was called to emit the committable downstream. The records are in flight (in network buffer / Netty queue). Before the downstream operator's `processElement` sees them, the writer's JVM dies.

**State.**

| Where | State |
|---|---|
| Object store | Data files written by S for N exist. They are NOT yet referenced by any Iceberg snapshot. |
| Network buffer | In-flight `StreamRecord<FlinkWriteResult>` (legacy) / `StreamRecord<CommittableMessage<WriteResult>>` (V2) — lost when the TaskExecutor process dies. |
| Aggregator (V2) | Hasn't received S's committable for N. |
| Committer | Hasn't received S's `FlinkWriteResult` for N. |
| Flink durable state | M-1 is the latest CompletedCheckpoint. The barrier for N never reached all downstream tasks. |
| Iceberg table | No snapshot for N. |

**Who triggers recovery.** Same as F7 — TaskExecutor failure detected → region failover → restart from M-1.

**Recovery walk.** Same as F7. Writer restart reprocesses records from M-1 onward, re-creates the data files with a fresh `subtaskId-attemptNumber` prefix (this prefix is determined by `getRuntimeContext().getTaskInfo().getAttemptNumber()`, so the new attempt's files have a different path), and re-emits committables on the next checkpoint.

**Orphan files.** The data files from the failed attempt are orphaned (referenced by no snapshot). Same cleanup as F7.

**Why no commit issue.** The barrier for N never propagated. From Iceberg's view, N never happened. The next successful checkpoint M' starts the commit cycle fresh.

---

### 11.12 The `notifyCheckpointAborted` story (and why both committers can ignore it)

Neither `CommitterOperator` (V2) nor `IcebergFilesCommitter` (legacy) overrides `notifyCheckpointAborted`. The default at `AbstractStreamOperator.java:462-464`:

```java
@Override
public void notifyCheckpointAborted(long checkpointId) throws Exception {
    stateHandler.notifyCheckpointAborted(checkpointId);    // just lets state cleanup run
}
```

This is safe because of three invariants that hold for both architectures:

1. **An aborted ckp N's committables are never persisted.** `snapshotState(N)` is only invoked as part of a successful checkpoint cycle; an aborted N's `snapshotState` does not run. Persisted state never sees N's pending committables.
2. **An aborted N triggers region failover.** Checkpoint abort almost always implies subtask failure. The failover restarts the sink region; in-memory buffer for N is wiped.
3. **`commit-up-to-checkpoint` semantics.** Even if pending committables for an aborted N somehow leaked into a later successful checkpoint's state, the next `notifyCheckpointComplete(M)` (M > N) would commit them via `headMap(M, true)` / `getCheckpointCommittablesUpTo(M)` semantics.

### 11.13 Cross-job-ID restart (savepoint into a fresh job)

`JobID` changes when starting a new job from an external savepoint. Both architectures handle this identically by treating Iceberg's snapshot chain as the cross-job cursor.

**Legacy**: `SinkUtil.getMaxCommittedCheckpointId(table, restoredFlinkJobId, operatorUniqueId, branch)` uses the **restored** `flinkJobId` (loaded from `jobIdState`), not the current run's ID. This matters because the current run's ID is brand new — no Iceberg snapshot has it yet, so a search by current ID would return `INITIAL_CHECKPOINT_ID = -1` and force re-commit of everything pending.

```java
// v1.20/.../IcebergFilesCommitter.java:179-189
String restoredFlinkJobId = jobIdState.get().iterator().next();
this.maxCommittedCheckpointId =
    SinkUtil.getMaxCommittedCheckpointId(table, restoredFlinkJobId, operatorUniqueId, branch);
```

Then `snapshotState` overwrites `jobIdState` with the current job ID. Identity carries forward — the **next** restart will know to look up by the **new** current ID.

**V2**: The `IcebergCommittable` carries `jobId` taken from the current run at aggregator time (`IcebergWriteAggregator.java:112`):

```java
IcebergCommittable committable = new IcebergCommittable(
    writeToManifest(results, checkpointId),
    getContainingTask().getEnvironment().getJobID().toString(),  // CURRENT job id
    getRuntimeContext().getOperatorUniqueID(),
    checkpointId);
```

For committables emitted in the new job, `last.jobId()` is the new ID. The snapshot-chain walk by the new ID returns `-1` (no prior matching snapshot). **All** restored committables go through `commitPendingRequests` and create fresh Iceberg snapshots tagged with the new job ID. There's no "skip via signalAlreadyCommitted" path because the new ID never appears in pre-existing Iceberg snapshots.

This is correct because the savepoint restore means committables in the restored state were never committed (otherwise they'd be removed from the collector). Committing them as if they were the first commit is exactly right.

**What's NOT supported**: two concurrent jobs targeting the same Iceberg branch with the same operator UID. Iceberg's CAS commit will reject the loser → exception → operator dies → failover. Documented in `IcebergSink.java:51-56`:

> There is no other writer which would generate another commit to the same branch with the same flink.job-id and flink.operator-id

### 11.14 What if `flink.max-committed-checkpoint-id` is null on the latest matching snapshot?

`SinkUtil.getMaxCommittedCheckpointId` returns `INITIAL_CHECKPOINT_ID = -1`. Both architectures interpret -1 as "nothing yet committed":

- **Legacy**: `tailMap(-1, false)` returns the entire `dataFilesPerCheckpoint` → everything gets committed.
- **V2**: `headMap(-1, true)` returns empty (no committables marked already-committed) → all pending requests proceed to `commitPendingRequests` → all committed.

Both interpretations are correct: if no prior commit cursor exists, then this is the first commit, so commit everything pending. Covered in depth in `flink-iceberg-sink-connector-deep-dive.md` Part 6 ("When Can `flink.max-committed-checkpoint-id` Be Null?").

### 11.15 Summary diagram — recovery handlers per failure point

```
   ┌───────────────────────────────────────────────────────────────────────────────┐
   │ Failure point → Handler called → State read → Action                          │
   ├───────────────────────────────────────────────────────────────────────────────┤
   │ F1: JM crash, ckpN persisted but not broadcast                                │
   │     Legacy: IcebergFilesCommitter.initializeState (mailbox, task startup)     │
   │             reads checkpointsState + jobIdState; walks Iceberg chain;         │
   │             commitUpToCheckpoint(uncommitted) → AppendFiles/RowDelta          │
   │     V2:     CommitterOperator.initializeState (mailbox, task startup)         │
   │             reads committableCollectorState; commitAndEmitCheckpoints(N)      │
   │             → IcebergCommitter.commit → walk chain, commit uncommitted        │
   ├───────────────────────────────────────────────────────────────────────────────┤
   │ F2: committer fails partway through batch                                     │
   │     Operator dies → task fails → region failover → restart from durable ckp   │
   │     → same as F1 path; idempotent via snapshot-chain walk                     │
   ├───────────────────────────────────────────────────────────────────────────────┤
   │ F3: notifyCheckpointComplete RPC drops                                        │
   │     NO restart. Next notifyCheckpointComplete(N+1) absorbs N via              │
   │     headMap(checkpointId, true) (legacy) /                                    │
   │     getCheckpointCommittablesUpTo(checkpointId) (V2).                         │
   │     Legacy guard: if (ckpId > maxCommittedCheckpointId).                      │
   │     V2 guard:     Math.max(lastCompletedCheckpointId, ckpId).                 │
   ├───────────────────────────────────────────────────────────────────────────────┤
   │ F4: Iceberg commit succeeded, operator died before returning                  │
   │     Restart from ckpN. initializeState path runs same as F1.                  │
   │     Iceberg chain shows N already committed → recovery is no-op.              │
   ├───────────────────────────────────────────────────────────────────────────────┤
   │ F5: JM crash before persisting CompletedCheckpoint(N)                         │
   │     N never happened. Restart from M-1. Writers re-do.                        │
   ├───────────────────────────────────────────────────────────────────────────────┤
   │ F6: Checkpoint N aborts at JM                                                 │
   │     Region failover. In-memory N state wiped. notifyCheckpointAborted is      │
   │     no-op (inherited from AbstractStreamOperator). Restart from M-1.          │
   ├───────────────────────────────────────────────────────────────────────────────┤
   │ F7: Writer mid-record crash                                                   │
   │     Region failover. Writer restarts. Orphan files cleaned by Iceberg         │
   │     orphan-file maintenance.                                                  │
   ├───────────────────────────────────────────────────────────────────────────────┤
   │ F8: Writer emitted committables then crashed before downstream saw them       │
   │     Same as F7.                                                               │
   └───────────────────────────────────────────────────────────────────────────────┘
```

The two architectures handle the **same** set of failure modes via the **same** authoritative cursor (Iceberg's `flink.max-committed-checkpoint-id`). The differences:

- **Legacy**: orchestration lives inside Iceberg's `IcebergFilesCommitter`. The connector itself writes the recovery loop, the Avro staging, the `dataFilesPerCheckpoint` map, and the guard `if (checkpointId > maxCommittedCheckpointId)`. The recovery commit happens **inside `initializeState`**.
- **V2**: orchestration lives inside Flink's `CommitterOperator` + `CheckpointCommittableManagerImpl`. The connector implements only `IcebergCommitter.commit` (with idempotency via `signalAlreadyCommitted`). Flink runtime handles batching, retrying, replaying on restart, draining at end-of-input. The recovery commit also happens inside `CommitterOperator.initializeState`, but the connector is just the callee.

---

## Appendix A: Key Classes Index

### Flink runtime (Sink V2 framework)

| Class | Path | Role |
|---|---|---|
| `Sink<InputT>` | `flink-core/.../sink2/Sink.java` | SPI: factory for `SinkWriter`s. |
| `SinkWriter<InputT>` | `flink-core/.../sink2/SinkWriter.java` | SPI: per-subtask writer. |
| `CommittingSinkWriter<InputT, CommT>` | `flink-core/.../sink2/CommittingSinkWriter.java` | Writer that participates in 2PC; adds `prepareCommit()`. |
| `Committer<CommT>` | `flink-core/.../sink2/Committer.java` | SPI: second-phase commit; `commit(Collection<CommitRequest<CommT>>)`. |
| `Committer.CommitRequest<CommT>` | (nested) | Per-committable signaling: `signalAlreadyCommitted`, `retryLater`, etc. |
| `SupportsCommitter<CommT>` | `flink-core/.../sink2/SupportsCommitter.java` | Mixin on `Sink` declaring 2PC. |
| `SupportsPreWriteTopology<InputT>` | `flink-runtime/.../sink2/SupportsPreWriteTopology.java` | Mixin: insert topology before writer. |
| `SupportsPreCommitTopology<WriteResultT, CommT>` | `flink-runtime/.../sink2/SupportsPreCommitTopology.java` | Mixin: insert topology between writer and committer (Iceberg uses for aggregator). |
| `SupportsPostCommitTopology<CommT>` | `flink-runtime/.../sink2/SupportsPostCommitTopology.java` | Mixin: insert topology after committer (Iceberg uses for compaction). |
| `CommittableMessage<CommT>` | `flink-runtime/.../sink2/CommittableMessage.java` | Sealed: `CommittableSummary` + `CommittableWithLineage`. |
| `SinkWriterOperator` | `flink-runtime/.../sink/SinkWriterOperator.java` | Runtime operator wrapping `SinkWriter`. Emits `CommittableMessage`s. |
| `CommitterOperator` | `flink-runtime/.../sink/CommitterOperator.java` | **The operator that calls `Committer.commit`.** Single-subtask state, mailbox-thread invocation. |
| `GlobalCommitterOperator` | `flink-runtime/.../sink/GlobalCommitterOperator.java` | Parallelism-1 commit variant; used when sink calls `addGlobalCommitter`. |
| `CommittableCollector` | `flink-runtime/.../sink/committables/CommittableCollector.java` | Buffers committables by `(checkpointId, subtaskId)`. |
| `CheckpointCommittableManagerImpl` | `.../committables/CheckpointCommittableManagerImpl.java` | One per checkpointId; orchestrates the retry loop and calls `committer.commit`. |
| `CommitRequestImpl` | `.../committables/CommitRequestImpl.java` | The actual `CommitRequest` runtime instance with state machine. |
| `SinkTransformationTranslator` | `.../translators/SinkTransformationTranslator.java` | Expands the user-facing `SinkTransformation` into the multi-operator topology. |
| `GlobalCommitterTransformationTranslator` | `.../translators/GlobalCommitterTransformationTranslator.java` | Translator for `addGlobalCommitter(...)`. |
| `StandardSinkTopologies` | `.../sink2/StandardSinkTopologies.java` | Helper: `addGlobalCommitter(committables, factory, serializer)`. |
| `SinkOptions.COMMITTER_RETRIES` | `flink-core/.../configuration/SinkOptions.java` | Max retries before `CheckpointCommittableManagerImpl.commit` throws. |

### Iceberg V2.1 (Sink V2 implementation)

| Class | Path | Role |
|---|---|---|
| `IcebergSink` | `iceberg/flink/v2.1/.../sink/IcebergSink.java` | User-facing sink. Implements `Sink + SupportsCommitter + SupportsPreWriteTopology + SupportsPreCommitTopology + SupportsPostCommitTopology + SupportsConcurrentExecutionAttempts`. |
| `IcebergSinkWriter` | `.../sink/IcebergSinkWriter.java` | `CommittingSinkWriter<RowData, WriteResult>`. Wraps Iceberg `TaskWriter`. |
| `IcebergWriteAggregator` | `.../sink/IcebergWriteAggregator.java` | Pre-commit operator (parallelism=1). Collects `WriteResult`s, writes Avro manifest, emits `IcebergCommittable`. |
| `IcebergCommittable` | `.../sink/IcebergCommittable.java` | `(byte[] manifest, String jobId, String operatorId, long checkpointId)`. |
| `IcebergCommitter` | `.../sink/IcebergCommitter.java` | `Committer<IcebergCommittable>`. **This is what `CheckpointCommittableManagerImpl.commit` calls.** |
| `IcebergCommittableSerializer` | `.../sink/IcebergCommittableSerializer.java` | `SimpleVersionedSerializer<IcebergCommittable>`. |
| `CommittableToTableChangeConverter` | `.../sink/CommittableToTableChangeConverter.java` | Post-commit operator (when `compactMode=true`). |
| `FlinkManifestUtil` | `.../sink/FlinkManifestUtil.java` | Writes/reads/deletes the staging Avro manifests. |
| `SinkUtil` | `.../sink/SinkUtil.java` | `getMaxCommittedCheckpointId(table, jobId, operatorId, branch)` — Iceberg snapshot chain walk. |

### Iceberg V1.20 (Legacy implementation)

| Class | Path | Role |
|---|---|---|
| `FlinkSink` | `iceberg/flink/v1.20/.../sink/FlinkSink.java` | Legacy builder. `chainIcebergOperators()` builds DAG. |
| `IcebergStreamWriter` | `.../sink/IcebergStreamWriter.java` | `AbstractStreamOperator<FlinkWriteResult>`. Wraps Iceberg `TaskWriter`. Emits `FlinkWriteResult(checkpointId, WriteResult)`. |
| `IcebergFilesCommitter` | `.../sink/IcebergFilesCommitter.java` | `AbstractStreamOperator<Void>` (parallelism=1). Owns 2PC: `processElement` buffers; `snapshotState` writes Avro manifests; `notifyCheckpointComplete` commits to Iceberg. |
| `FlinkWriteResult` | `.../sink/FlinkWriteResult.java` | Connector-defined envelope `(long, WriteResult)`. |

### Cross-reference

Companion deep-dives in `codedocs/`:

- [`flink-iceberg-sink-connector-deep-dive.md`](flink-iceberg-sink-connector-deep-dive.md) — focuses on **idempotency** and the Iceberg-side state machinery (snapshot summary `flink.max-committed-checkpoint-id`, Avro manifests, recovery walk). Use this doc together with that one — that one is about *what* Iceberg remembers; this one is about *when* Flink calls it.
- [`flink-iceberg-source-connector-deep-dive.md`](flink-iceberg-source-connector-deep-dive.md) — the source side.
- [`flink-sink-patterns-comparison.md`](flink-sink-patterns-comparison.md) — broader survey of sink patterns across connectors (Iceberg, Kafka, FileSink, JDBC).
- [`flink-exactly-once-checkpointing-deep-dive.md`](flink-exactly-once-checkpointing-deep-dive.md) — barrier alignment, `notifyCheckpointComplete` delivery semantics, two-phase commit at the Flink runtime layer.
