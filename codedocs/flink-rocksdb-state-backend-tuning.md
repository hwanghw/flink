# RocksDB State Backend: Architecture, Memory, and Tuning

A unified guide combining the Flink docs on
[State Backends (RocksDB details)](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/state/state_backends/#rocksdb-state-backend-details)
and [Large State Tuning](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/state/large_state_tuning/#tuning-rocksdb).

---

## 1. How RocksDB Fits Into Flink

Each Flink TaskManager slot runs one or more operators. When using `EmbeddedRocksDBStateBackend`,
every keyed state in every operator maps to a **Column Family** inside a shared RocksDB instance
per slot. RocksDB stores data on local disk (not JVM heap), making it suitable for state that
exceeds available memory.

```
┌─────────────────────────── TaskManager ───────────────────────────┐
│                                                                   │
│  ┌──────────────────────── Slot 0 ────────────────────────────┐   │
│  │                                                            │   │
│  │  ┌─── Operator A ───┐     ┌─── Operator B ───┐            │   │
│  │  │  ValueState "s1" │     │  MapState "m1"   │            │   │
│  │  │  ListState  "s2" │     │  ValueState "v1" │            │   │
│  │  └──────────────────┘     └──────────────────┘            │   │
│  │          │    │                    │    │                   │   │
│  │          ▼    ▼                    ▼    ▼                   │   │
│  │  ┌─────────────────── RocksDB Instance ──────────────────┐ │   │
│  │  │                                                       │ │   │
│  │  │  ColumnFamily    ColumnFamily    ColumnFamily    CF    │ │   │
│  │  │   "op-A/s1"      "op-A/s2"      "op-B/m1"    "op-B…" │ │   │
│  │  │                                                       │ │   │
│  │  │  Each CF has: MemTable(s) + SST files on disk         │ │   │
│  │  └───────────────────────────────────────────────────────┘ │   │
│  │                         │                                  │   │
│  │  ┌─────── Shared Memory (from Managed Memory) ──────────┐ │   │
│  │  │  Block Cache  │  Write Buffer Manager  │  Index/Filter│ │   │
│  │  └───────────────┴────────────────────────┴──────────────┘ │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌──────────────────────── Slot 1 ────────────────────────────┐   │
│  │  (separate RocksDB instance, separate managed memory)      │   │
│  └────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────┘
```

**Key rule:** 1 state descriptor per operator = 1 Column Family. An application with 4 operators,
each having 3 states, creates 12 Column Families in one RocksDB instance per slot. More Column
Families = more MemTables = more memory needed.

---

## 2. Memory Model: The Three Consumers

RocksDB allocates memory **outside the JVM heap** (native memory). Flink controls it through
**managed memory**, which caps the total RocksDB memory per slot. This is enabled by default:

```
state.backend.rocksdb.memory.managed: true   # default
```

Managed memory is split among three consumers:

```
                    Managed Memory Budget (per slot)
                    ════════════════════════════════
                    e.g., taskmanager.memory.managed.fraction = 0.4

    ┌───────────────────────────────────────────────────────────┐
    │                                                           │
    │  ┌─────────────────────────────────┐                      │
    │  │      Write Buffer Manager       │  50% (default)       │
    │  │                                 │                      │
    │  │  Controls total MemTable memory │  write-buffer-ratio  │
    │  │  across ALL Column Families     │  = 0.5               │
    │  └─────────────────────────────────┘                      │
    │                                                           │
    │  ┌─────────────────────────────────┐                      │
    │  │         Block Cache             │  40% (remainder)     │
    │  │                                 │                      │
    │  │  Caches SST data blocks for     │                      │
    │  │  reads. Shared across all CFs.  │                      │
    │  └─────────────────────────────────┘                      │
    │                                                           │
    │  ┌─────────────────────────────────┐                      │
    │  │  Index & Filter (high-prio)     │  10% (default)       │
    │  │                                 │                      │
    │  │  Bloom filters + SST indexes.   │  high-prio-pool-ratio│
    │  │  Pinned in cache, not evicted   │  = 0.1               │
    │  │  by data block pressure.        │                      │
    │  └─────────────────────────────────┘                      │
    │                                                           │
    └───────────────────────────────────────────────────────────┘
```

### What Each Consumer Does

| Consumer | What it stores | What happens when it's too small |
|----------|---------------|----------------------------------|
| **Write Buffer Manager** | MemTables — the in-memory buffer for new writes. Each CF has its own MemTable(s), but the manager enforces a global cap. | Frequent MemTable flushes → more L0 SST files → more compactions → write stalls |
| **Block Cache** | Decompressed SST data blocks read from disk. Shared LRU cache across all CFs. | More disk reads → higher read latency, higher IOPS |
| **Index & Filter** | SST file indexes (for locating keys) and bloom filters (for skipping files that don't contain a key). Pinned in the high-priority pool. | Without the high-prio pool, index/filters compete with data blocks and get evicted → every read must reload indexes from disk |

### Decision Tree: When to Tune What

```
Performance problem?
│
├── Write latency / write stalls / frequent compactions?
│   │
│   ├── First: increase managed memory (taskmanager.memory.managed.fraction)
│   │
│   └── If memory is already large:
│       increase write-buffer-ratio (e.g., 0.5 → 0.7)
│       OR reduce arena-block-size if you have many CFs
│
├── Read latency / high disk IOPS?
│   │
│   ├── First: increase managed memory
│   │
│   └── If memory is already large:
│       decrease write-buffer-ratio (more room for block cache)
│       ensure high-prio-pool-ratio > 0 (never set to 0!)
│
└── Out-of-memory kills (container/YARN)?
    │
    └── RocksDB is using more native memory than expected
        ├── Enable managed memory if disabled
        ├── Reduce managed memory fraction
        └── Or use fixed-per-slot: state.backend.rocksdb.memory.fixed-per-slot
```

---

## 3. Incremental Checkpoints

**This should be one of the first things you enable for large state.**

```
state.backend.incremental: true
```

Or programmatically:
```java
new EmbeddedRocksDBStateBackend(true);
```

### How It Works

RocksDB stores data as immutable **SST (Sorted String Table)** files. Between checkpoints, some
SSTs are unchanged, some are new (from MemTable flushes), and some are replaced (by compaction).
Incremental checkpoints only upload the **new/changed SSTs**.

```
Checkpoint N                     Checkpoint N+1
═══════════                      ══════════════

SST-1  ──────────────────────►   SST-1  (unchanged, not re-uploaded)
SST-2  ──────────────────────►   SST-2  (unchanged, not re-uploaded)
SST-3  ──── compacted ───┐
SST-4  ──── compacted ───┤
                          ├──►   SST-7  (new from compaction → uploaded)
                          │
                  (flush) └──►   SST-8  (new from MemTable flush → uploaded)

Uploaded at checkpoint N+1:  only SST-7 and SST-8 (delta)
Full checkpoint would upload: SST-1 + SST-2 + SST-7 + SST-8 (everything)
```

### Recovery Trade-offs

| Scenario | Full checkpoint | Incremental checkpoint |
|----------|----------------|----------------------|
| **Checkpoint duration** | Long (upload everything) | Short (upload delta only) |
| **Recovery when network is bottleneck** | Faster (one self-contained download) | Slower (may need multiple deltas) |
| **Recovery when CPU/IOPs is bottleneck** | Slower (must rebuild RocksDB tables from Flink's canonical format) | Faster (SSTs are already in native RocksDB format) |
| **Web UI "Checkpointed Data Size"** | Shows total state size | Shows delta size only |

Old checkpoints are automatically pruned — the history doesn't grow indefinitely because
RocksDB compaction consolidates SSTs over time.

### Task-Local Recovery

When combined with task-local recovery, incremental checkpoints are especially efficient:
the secondary local copy uses **hard links** (no extra disk space), and on failover Flink can
recover from the local copy instead of downloading from the distributed filesystem.

### When a Full Checkpoint Is Taken (Even with Incremental Enabled)

Incremental checkpoints work by computing a **delta** against the last completed checkpoint's
SST files. When there is no valid base to diff against, Flink falls back to uploading all SST
files — effectively a full checkpoint.

```
 Checkpoint chain (normal incremental operation):

 CP-1 (full)  ──►  CP-2 (delta)  ──►  CP-3 (delta)  ──►  CP-4 (delta)
 [all SSTs]        [new SSTs only]     [new SSTs only]     [new SSTs only]
     ▲                                                          │
     │                                                          │
     └──────── base SSTs are shared/reused ─────────────────────┘


 Chain breaks → next checkpoint becomes full:

 CP-4 (delta) ──► SAVEPOINT ──► CP-5 (FULL!)  ──►  CP-6 (delta)
                  [NO_SHARING]  [no base exists]    [normal again]
                  breaks chain
```

**The 6 conditions that force a full checkpoint:**

| # | Condition | Why | Source |
|---|-----------|-----|--------|
| 1 | **First checkpoint after job start** | No previous checkpoint exists. `lastCompletedCheckpointId = -1`, `uploadedSstFiles` is empty. | `RocksIncrementalSnapshotStrategy.java` |
| 2 | **Savepoints** | Savepoints always use `SharingFilesStrategy.NO_SHARING` — all SST files are uploaded as exclusive (not reusable by future checkpoints). This makes savepoints self-contained and portable. | `SavepointType.java:86` |
| 3 | **First checkpoint after a savepoint** | When a savepoint completes, it is NOT registered in `uploadedSstFiles` (because it used `NO_SHARING`). So the next checkpoint can't find a base and must upload everything. (FLINK-23949) | `RocksIncrementalSnapshotStrategy.java:167-180` |
| 4 | **First checkpoint after restore** (from non-incremental source) | If restored from a full savepoint or native savepoint, `restoredSstFiles` is empty — no base to build on. If restored from an incremental checkpoint, the restored SSTs ARE tracked and the first checkpoint can be incremental. | `RocksDBIncrementalRestoreOperation.java:685-697` |
| 5 | **After rescaling** | Key groups are repartitioned across new subtasks. The old SST files belong to different key group ranges, so they can't serve as a base. | `RocksDBKeyedStateBackendBuilder.java` |
| 6 | **Base SST files not found** | If previously uploaded SST files are no longer available on checkpoint storage (e.g., storage failure, manual deletion), they must be re-uploaded. | `RocksIncrementalSnapshotStrategy.java:413-433` |

**How the strategy switch works** (from `RocksIncrementalSnapshotStrategy.java:142-155`):

```java
final CheckpointType.SharingFilesStrategy sharingFilesStrategy =
        checkpointOptions.getCheckpointType().getSharingFilesStrategy();

switch (sharingFilesStrategy) {
    case FORWARD_BACKWARD:
        // Normal incremental: reuse SSTs from previous checkpoints
        previousSnapshot = snapshotResources.previousSnapshot;
        break;
    case FORWARD:
    case NO_SHARING:
        // Full checkpoint: no base, upload everything
        previousSnapshot = EMPTY_PREVIOUS_SNAPSHOT;
        break;
}
```

| Checkpoint type | Strategy | Incremental? |
|----------------|----------|-------------|
| Regular checkpoint | `FORWARD_BACKWARD` | Yes (reuses previous SSTs) |
| Full checkpoint (`FULL_CHECKPOINT`) | `FORWARD` | No (uploads all, but files are shareable) |
| Savepoint | `NO_SHARING` | No (uploads all, files are exclusive/not shareable) |

**Practical implication:** If you take frequent savepoints (e.g., before deployments), be aware
that each savepoint + its following checkpoint will both be full-size uploads. For large state,
this can cause a temporary spike in checkpoint duration and storage I/O.

---

## 4. Timers: Heap vs RocksDB

Timers schedule future actions (window triggers, ProcessFunction callbacks). With
`EmbeddedRocksDBStateBackend`, timers are stored **in RocksDB by default**.

```
state.backend.rocksdb.timer-service.factory: rocksdb   # default
state.backend.rocksdb.timer-service.factory: heap      # alternative
```

### When to Use Each

```
                    ┌──────────────────┐
                    │  How many timers │
                    │  does your app   │
                    │  have?           │
                    └────────┬─────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
         Few timers                   Many timers
   (no windows, few                 (windows, complex
    ProcessFunction                  event processing)
    timers)                                 │
              │                             │
              ▼                             ▼
     ┌────────────────┐          ┌────────────────────┐
     │  Heap timers   │          │  RocksDB timers    │
     │                │          │  (default)         │
     │  + Faster      │          │                    │
     │  + Lower CPU   │          │  + Scales beyond   │
     │                │          │    available memory │
     │  - Limited by  │          │  + Robust          │
     │    JVM heap    │          │                    │
     │  - No async    │          │  - Higher CPU cost │
     │    snapshot    │          │    per timer op    │
     │    for timers  │          │                    │
     └────────────────┘          └────────────────────┘
```

**Limitations of heap timers with RocksDB backend:**
- Timer state snapshots are **synchronous** (blocks the main thread), even though keyed state
  snapshots remain asynchronous
- Checkpointing and savepointing will fail if operators write to **raw keyed state** (advanced use
  case — most applications are unaffected)

---

## 5. Tuning the Write Path

Understanding the write path helps diagnose performance issues:

```
 Application
     │
     │  state.update(value)
     ▼
┌──────────┐
│ MemTable │  In-memory sorted buffer (one per ColumnFamily)
│          │  ← controlled by Write Buffer Manager
└────┬─────┘
     │  MemTable full → flush to disk
     ▼
┌──────────┐
│  L0 SST  │  Level-0: SST files directly from flushes
│  files   │  (may have overlapping key ranges)
└────┬─────┘
     │  Too many L0 files → compaction
     ▼
┌──────────┐
│  L1 SST  │  Level-1+: compacted, non-overlapping key ranges
│  files   │  Each level is ~10x larger than the previous
└────┬─────┘
     │  continues to deeper levels...
     ▼
┌──────────┐
│  L2+ SST │
│  files   │
└──────────┘
```

### Symptoms of Write-Side Bottleneck

- High write latency in metrics
- Frequent compactions visible in RocksDB logs
- RocksDB **write stalls** (RocksDB pauses writes when L0 has too many files)
- Checkpoint size increases sharply (many new SSTs per checkpoint)

### Fixes

**1. Increase managed memory** — the simplest and most effective fix.
The default `taskmanager.memory.managed.fraction = 0.4` is conservative. For TaskManagers with
multi-GB process sizes, increasing this to 0.5–0.7 is common.

**2. Increase write-buffer-ratio** — if you've already maximized managed memory:
```
state.backend.rocksdb.memory.write-buffer-ratio: 0.7   # default is 0.5
```
This gives MemTables more room, reducing flush frequency. Trade-off: less block cache for reads.

**3. Reduce arena block size** — for applications with many Column Families:
Each MemTable pre-allocates memory in arena blocks (default 8 MB). With 12 CFs, that's
12 × 8 MB = 96 MB just for arenas. Reducing to 1 MB:

```java
public class MyOptionsFactory implements ConfigurableRocksDBOptionsFactory {
    @Override
    public ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions currentOptions,
            Collection<AutoCloseable> handlesToClose) {
        // Reduce arena block size from default 8MB to 1MB
        // Helps when you have many Column Families competing for write buffer
        currentOptions.setArenaBlockSize(1024 * 1024);

        // Optionally increase max background flushes for many-CF setups
        return currentOptions;
    }

    @Override
    public DBOptions createDBOptions(
            DBOptions currentOptions,
            Collection<AutoCloseable> handlesToClose) {
        // Increase flush threads for setups with many Column Families
        currentOptions.setMaxBackgroundFlushes(4);
        return currentOptions;
    }
}
```

Register via config:
```
state.backend.rocksdb.options-factory: com.example.MyOptionsFactory
```

---

## 6. Tuning the Read Path

```
 Application
     │
     │  state.value()
     ▼
┌──────────┐   found?
│ MemTable │ ──────────► return value (fastest path)
└────┬─────┘
     │ not found
     ▼
┌──────────────┐
│ Block Cache  │   found?
│ (shared LRU) │ ──────────► return value (memory read, fast)
└──────┬───────┘
       │ cache miss
       ▼
┌──────────────┐
│ Bloom Filter │   key definitely    skip this SST file
│ (per SST)    │ ──not in file──►   (avoids disk I/O)
└──────┬───────┘
       │ key might be in file
       ▼
┌──────────────┐
│   SST Index  │   locate block
│ (per SST)    │ ──containing key──►  read block from disk
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Disk Read   │   decompress block → put in block cache → return value
│  (slowest)   │
└──────────────┘
```

### Key Tuning Levers for Reads

**1. Block cache size** — the most impactful setting for reads. Controlled indirectly by
the managed memory budget and `write-buffer-ratio`. Less write buffer ratio = more block cache.

**2. Index & filter high-priority pool** — **never set `high-prio-pool-ratio` to zero!**
Without it, bloom filters and SST indexes compete with data blocks for cache space and get evicted.
This forces disk reads just to locate keys, destroying read performance.

```
state.backend.rocksdb.memory.high-prio-pool-ratio: 0.1   # default, don't lower
```

**3. Compression** — reduces SST file size on disk, but costs CPU on read (decompression) and
write (compression). Flink's snapshot compression setting has **no effect on incremental
checkpoints** because incremental checkpoints use RocksDB's native SST format, which always uses
snappy compression internally.

```
# This only affects FULL (non-incremental) snapshots:
execution.checkpointing.snapshot-compression: true
```

---

## 7. Compaction Deep Dive

Compaction is the background process that keeps RocksDB healthy. It merges and sorts SST files,
removes deleted/overwritten entries, and controls disk space. **Most Flink users never need to
tune compaction** — the defaults work well. But understanding it helps diagnose write stalls,
high disk usage, and checkpoint slowness.

### Why Compaction Exists

RocksDB is an **LSM-tree** (Log-Structured Merge-tree). Writes go to an in-memory MemTable,
which is flushed to disk as an immutable **L0 SST file**. Without compaction, you'd accumulate
thousands of overlapping SST files and reads would need to check all of them.

Compaction merges SSTs, eliminates duplicates, and organizes data into sorted, non-overlapping
levels:

```
 MemTable (in memory)
     │
     │  flush
     ▼
 ┌─────────────────────────────────────────────────────┐
 │  L0:  SST-a  SST-b  SST-c  SST-d                   │  ← SSTs from flushes
 │       (may have overlapping key ranges)             │     (unsorted relative
 │                                                     │      to each other)
 └────────────────────┬────────────────────────────────┘
                      │ compaction (merge + sort)
                      ▼
 ┌─────────────────────────────────────────────────────┐
 │  L1:  SST-1     SST-2     SST-3                     │  ← non-overlapping
 │       [a-f]     [g-m]     [n-z]                     │     sorted ranges
 └────────────────────┬────────────────────────────────┘
                      │ compaction
                      ▼
 ┌─────────────────────────────────────────────────────┐
 │  L2:  SST-10  SST-11  SST-12  SST-13  SST-14       │  ← ~10x larger than L1
 │       [a-c]   [d-f]   [g-k]   [l-p]   [q-z]       │
 └─────────────────────────────────────────────────────┘
                      │
                      ▼  ... deeper levels (each ~10x larger)
```

### Write Amplification

Every byte written by the application is rewritten multiple times as data moves through
compaction levels. This is called **write amplification**:

```
User writes 1 MB
     │
     ├──► MemTable → flush to L0        (1 MB written to disk)
     ├──► L0 → compacted into L1        (1 MB rewritten)
     ├──► L1 → compacted into L2        (1 MB rewritten)
     └──► L2 → compacted into L3        (1 MB rewritten)
                                         ─────────────────
                                Total:   ~4 MB disk writes
                                         for 1 MB of user data
                                         (write amplification ≈ 4x)
```

High write amplification means more disk I/O, more CPU, and more work for background threads.

### Compaction Styles

Flink exposes RocksDB's compaction style via:
```
state.backend.rocksdb.compaction.style: LEVEL   # default
```

| Style | How it works | Write amp | Read amp | Space amp | Best for |
|-------|-------------|-----------|----------|-----------|----------|
| **LEVEL** (default) | Each level has a size limit. When exceeded, SSTs are merged into the next level. Produces sorted, non-overlapping files per level. | Medium-High | Low (1 file per level to check) | Low (little wasted space) | Most Flink workloads — good read performance |
| **UNIVERSAL** | Compaction triggers when there are enough sorted runs of similar size. Merges all or a subset of runs at once. | Low | High (more files to check) | Higher (temporary space during compaction) | Write-heavy workloads where read latency is less critical |
| **FIFO** | SSTs are deleted when total size exceeds a limit. No merging — oldest files are simply dropped. | None | Very high | Low | Time-series / TTL-only data where old data is expendable |

**For most Flink applications, `LEVEL` is the right choice.** Only consider `UNIVERSAL` if
you are severely write-bound and can tolerate higher read latency.

### Dynamic Level Sizing

By default, each level has a fixed size target (L1 = `max-size-level-base`, L2 = L1 × 10, etc.).
This can lead to inefficiency when total data size is small or changes over time.

**Dynamic level sizing** lets RocksDB adjust level targets based on actual data size:

```
state.backend.rocksdb.compaction.level.use-dynamic-size: true
```

```
Fixed level sizes (default):           Dynamic level sizes:

L1 target = 256 MB (always)            L1 target = adjusted so that
L2 target = 2.5 GB (always)            total data fills levels evenly
L3 target = 25 GB  (always)
                                       If total data = 500 MB:
If total data = 500 MB:                  L1 target = 50 MB
  L1 = 256 MB (mostly empty!)            L2 target = 500 MB
  L2 = 244 MB                           → fewer levels, less write amp
  → data spread across many levels
  → unnecessary write amplification
```

**Recommendation:** Enable this for large or variable-size state. Already enabled in
`SPINNING_DISK_OPTIMIZED` and `SPINNING_DISK_OPTIMIZED_HIGH_MEM` predefined profiles.

### Key Compaction Tuning Knobs

| Config | Default | SPINNING_DISK_HIGH_MEM | What it controls |
|--------|---------|------------------------|------------------|
| `state.backend.rocksdb.compaction.style` | `LEVEL` | `LEVEL` | Compaction algorithm |
| `state.backend.rocksdb.compaction.level.use-dynamic-size` | `false` | `true` | Auto-adjust level targets |
| `state.backend.rocksdb.compaction.level.target-file-size-base` | `64 MB` | `256 MB` | Size of individual SST files in L1 |
| `state.backend.rocksdb.compaction.level.max-size-level-base` | `256 MB` | `1 GB` | Total size target for L1 |
| `state.backend.rocksdb.writebuffer.size` | `64 MB` | `64 MB` | MemTable size before flush |
| `state.backend.rocksdb.writebuffer.count` | `2` | `4` | Max MemTables in memory per CF |
| `state.backend.rocksdb.writebuffer.number-to-merge` | `1` | `3` | MemTables to merge before flushing |
| `state.backend.rocksdb.thread.num` | `2` | `4` | Background threads for flush + compaction |

**What `number-to-merge` does:**

```
number-to-merge = 1 (default):        number-to-merge = 3:

MemTable full → flush immediately      MemTable-1 full → wait
                                       MemTable-2 full → wait
                                       MemTable-3 full → merge all 3
                                                         then flush

Pros: lower memory usage              Pros: fewer, larger SSTs
Cons: many small L0 SSTs              Cons: needs more MemTable memory
      → more compaction work                (count must be >= merge + 1)
```

**What `target-file-size-base` does:**

Larger SST files (e.g., 256 MB instead of 64 MB) mean:
- Fewer files → fewer file handles, less metadata overhead
- Each compaction moves more data → less frequent but heavier compactions
- Better for HDD (fewer seeks), but slightly worse for SSD (larger rewrites)

### TTL Compaction Filter

When using **state TTL** (`StateTtlConfig`), Flink installs a custom compaction filter that
removes expired entries during compaction. Two related configs:

```
# How often RocksDB forces compaction of old files to clean up TTL'd entries
# (files older than this are guaranteed to be re-compacted)
state.backend.rocksdb.compaction.filter.periodic-compaction-time: 30d   # default

# JNI optimization: how many entries to process before querying current time
# (reduces JNI overhead during compaction)
state.backend.rocksdb.compaction.filter.query-time-after-num-entries: 1000   # default
```

Without periodic compaction, TTL'd entries in cold SST files (files that are never touched by
normal compaction) would stay on disk indefinitely. The 30-day default ensures even the coldest
files are eventually cleaned up.

### Monitoring Compaction

Enable these metrics to diagnose compaction issues:

```
state.backend.rocksdb.metrics.compaction-pending: true
state.backend.rocksdb.metrics.estimate-pending-compaction-bytes: true
state.backend.rocksdb.metrics.num-running-compactions: true
state.backend.rocksdb.metrics.num-running-flushes: true
state.backend.rocksdb.metrics.stall-micros: true
state.backend.rocksdb.metrics.compaction-read-bytes: true
state.backend.rocksdb.metrics.compaction-write-bytes: true
```

| Metric | What to look for |
|--------|-----------------|
| `compaction-pending` | Stuck at `1` → compaction can't keep up with writes |
| `estimate-pending-compaction-bytes` | Growing over time → compaction is falling behind |
| `num-running-compactions` | Consistently at max → increase `thread.num` |
| `stall-micros` | Non-zero → RocksDB is **pausing writes** waiting for compaction |
| `compaction-read-bytes / write-bytes` | Ratio indicates write amplification |

### Decision Tree: Do I Need to Tune Compaction?

```
Are you seeing write stalls (stall-micros > 0)?
│
├── YES
│   │
│   ├── Is compaction-pending always 1?
│   │   │
│   │   ├── YES → increase thread.num (e.g., 2 → 4)
│   │   │         and/or increase max-size-level-base
│   │   │
│   │   └── NO → problem is likely MemTable pressure
│   │            (see Section 5: Tuning Write Path)
│   │
│   └── Is estimate-pending-compaction-bytes growing?
│       │
│       └── YES → enable dynamic level sizing
│                 and/or increase target-file-size-base
│
├── NO, but disk usage is high
│   │
│   └── Are you using state TTL?
│       │
│       ├── YES → check periodic-compaction-time
│       │         (default 30d may be too long)
│       │
│       └── NO → normal for large state; compaction
│                 will clean up obsolete entries over time
│
└── NO, everything is fine
    │
    └── Don't tune compaction. The defaults work.
```

---

## 8. Predefined Options Profiles

Flink provides ready-made RocksDB configuration profiles for common hardware setups:

```
state.backend.rocksdb.predefined-options: DEFAULT
```

| Profile | Best for | What it tunes |
|---------|----------|---------------|
| `DEFAULT` | General purpose (default) | Standard RocksDB settings |
| `SPINNING_DISK_OPTIMIZED` | HDD storage | Higher write buffer, longer compaction intervals |
| `SPINNING_DISK_OPTIMIZED_HIGH_MEM` | HDD + ample memory | Above + larger caches |
| `FLASH_SSD_OPTIMIZED` | SSD storage | Optimized compaction for SSD IOPS patterns |

Or set programmatically:
```java
EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
backend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
```

**Priority order:** `RocksDBOptionsFactory` > `PredefinedOptions` > config file defaults.

---

## 9. Expert Mode: Manual Memory Management

If you need full control, disable managed memory:

```
state.backend.rocksdb.memory.managed: false
```

In this mode, each ColumnFamily allocates its own MemTable and block cache independently. Memory
usage grows proportionally with the number of states:

```
Rough upper bound ≈ 140 MB × num-states-across-all-tasks × num-slots
```

You can also set a fixed memory limit independent of Flink's managed memory:
```
state.backend.rocksdb.memory.fixed-per-slot: 256mb     # per slot
state.backend.rocksdb.memory.fixed-per-tm: 2gb          # per TaskManager
```

**Warning:** In manual mode, you are responsible for ensuring the container/YARN has enough
native memory for RocksDB. If the total of JVM heap + RocksDB native memory exceeds the
container limit, the process will be killed. The fix: decrease `taskmanager.memory.task.heap.size`
to leave room for RocksDB.

---

## 10. Checkpoint Interval Tuning

While not RocksDB-specific, checkpoint interval has a major impact on RocksDB backends:

**`setMinPauseBetweenCheckpoints(millis)`** — ensures a minimum gap between the end of one
checkpoint and the start of the next. This prevents constant checkpointing that starves
the application of resources (CPU, I/O, network).

```java
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000); // 30 seconds
```

If checkpoints regularly take longer than the interval, they pile up and RocksDB spends more
time flushing for checkpoints than processing data.

---

## 11. RocksDB Native Metrics

RocksDB exposes internal metrics (compaction stats, block cache hit rates, stall counts) through
Flink's metrics system. Disabled by default because they add overhead:

```
state.backend.rocksdb.metrics.actual-delayed-write-rate: true
state.backend.rocksdb.metrics.compaction-pending: true
state.backend.rocksdb.metrics.num-running-compactions: true
state.backend.rocksdb.metrics.block-cache-hit: true
state.backend.rocksdb.metrics.block-cache-miss: true
```

Use these to diagnose issues:
- `block-cache-hit / (hit + miss)` → cache hit rate (should be > 90%)
- `compaction-pending` → whether compaction is falling behind
- `actual-delayed-write-rate` → whether write stalls are occurring

---

## 12. Configuration Quick Reference

| Config | Default | Description |
|--------|---------|-------------|
| `state.backend.incremental` | `false` | Enable incremental checkpoints |
| `state.backend.rocksdb.memory.managed` | `true` | Use Flink managed memory for RocksDB |
| `state.backend.rocksdb.memory.write-buffer-ratio` | `0.5` | Fraction of managed memory for write buffers |
| `state.backend.rocksdb.memory.high-prio-pool-ratio` | `0.1` | Fraction of block cache for index/filters |
| `state.backend.rocksdb.memory.fixed-per-slot` | (none) | Fixed memory per slot (overrides managed) |
| `state.backend.rocksdb.memory.fixed-per-tm` | (none) | Fixed memory per TaskManager |
| `state.backend.rocksdb.timer-service.factory` | `rocksdb` | `rocksdb` or `heap` for timer storage |
| `state.backend.rocksdb.predefined-options` | `DEFAULT` | Predefined CF options profile |
| `state.backend.rocksdb.options-factory` | (none) | Custom `RocksDBOptionsFactory` class |
| `taskmanager.memory.managed.fraction` | `0.4` | Fraction of total Flink memory for managed memory |
| `state.backend.rocksdb.compaction.style` | `LEVEL` | Compaction algorithm: LEVEL, UNIVERSAL, FIFO |
| `state.backend.rocksdb.compaction.level.use-dynamic-size` | `false` | Auto-adjust level size targets |
| `state.backend.rocksdb.compaction.level.target-file-size-base` | `64 MB` | SST file size target in L1 |
| `state.backend.rocksdb.compaction.level.max-size-level-base` | `256 MB` | Total size target for L1 |
| `state.backend.rocksdb.writebuffer.size` | `64 MB` | MemTable size before flush |
| `state.backend.rocksdb.writebuffer.count` | `2` | Max MemTables in memory per CF |
| `state.backend.rocksdb.writebuffer.number-to-merge` | `1` | MemTables to merge before flush |
| `state.backend.rocksdb.thread.num` | `2` | Background threads for flush + compaction |
| `state.backend.rocksdb.compaction.filter.periodic-compaction-time` | `30d` | Force re-compaction of old files (for TTL cleanup) |
| `execution.checkpointing.snapshot-compression` | `false` | Snappy compression for full snapshots (not incremental) |

### Recommended Starting Configuration for Large State

```yaml
# Enable incremental checkpoints (most impactful single change)
state.backend.incremental: true

# Increase managed memory for RocksDB if you have multi-GB TaskManagers
taskmanager.memory.managed.fraction: 0.5

# Keep defaults for most memory ratios
state.backend.rocksdb.memory.managed: true
state.backend.rocksdb.memory.write-buffer-ratio: 0.5
state.backend.rocksdb.memory.high-prio-pool-ratio: 0.1

# Heap timers if your app has few timers and you want lower checkpoint overhead
# state.backend.rocksdb.timer-service.factory: heap
```
