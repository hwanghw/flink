# Flink Iceberg Connector Deep Dive

How the Flink Iceberg connector writes data files directly to object storage, commits them
atomically to Iceberg tables, achieves end-to-end exactly-once semantics, and handles
upserts via merge-on-read — with source-code references from both v1.20 and v2.1.

Based on the
[Iceberg Flink Writes docs](https://iceberg.apache.org/docs/nightly/flink-writes/),
the source code in `myiceberg/flink/v1.20` and `myiceberg/flink/v2.1`, and the Flink
Sink V2 API in `flink-core`.

---

# Part 1: Overview

## What the Connector Does

The Flink Iceberg connector lets Flink streaming/batch jobs write directly to Iceberg
tables on S3, HDFS, or any Hadoop-compatible filesystem. Key properties:

- **No intermediate staging** — data files are written directly to the table's data directory
- **Atomic commits** — all files from a Flink checkpoint become visible in a single Iceberg snapshot
- **Exactly-once** — checkpoint IDs stored in Iceberg snapshot properties prevent duplicate commits
- **Upsert support** — equality delete files enable merge-on-read updates (Iceberg format V2)

This document focuses on the **IcebergSink (Sink V2)** implementation. The legacy
FlinkSink is covered in Appendix A.

---

# Part 2: Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                     IcebergSink (Sink V2) PIPELINE                    │
│                                                                       │
│  IcebergSink implements:                                              │
│    Sink<RowData>, SupportsPreWriteTopology, SupportsCommitter,       │
│    SupportsPreCommitTopology, SupportsPostCommitTopology,            │
│    SupportsConcurrentExecutionAttempts                                │
│                                                                       │
│  Input DataStream<RowData>                                            │
│       │                                                               │
│       ▼                                                               │
│  ┌─────────────────────────────────────────┐                         │
│  │  PreWriteTopology (optional)             │                         │
│  │  DataStatisticsOperator + RangePartitioner                        │
│  │  (collects data distribution statistics  │                         │
│  │   for balanced writes)                   │                         │
│  └──────────────────┬──────────────────────┘                         │
│                      ▼                                                │
│  ┌─────────────────────────────────────────┐                         │
│  │  IcebergSinkWriter  (parallelism = N)    │                         │
│  │  implements CommittingSinkWriter          │                         │
│  │                                          │                         │
│  │  write(row) → TaskWriter → data files   │                         │
│  │  flush() → close open files             │                         │
│  │  prepareCommit() → Collection<WriteResult>                        │
│  └──────────────────┬──────────────────────┘                         │
│                      │  WriteResult                                   │
│                      ▼                                                │
│  ┌─────────────────────────────────────────┐                         │
│  │  PreCommitTopology:                      │                         │
│  │  IcebergWriteAggregator (parallelism=1)  │                         │
│  │                                          │                         │
│  │  Aggregates N WriteResults into 1        │                         │
│  │  IcebergCommittable:                     │                         │
│  │    { DeltaManifests, jobId, operatorId,  │                         │
│  │      checkpointId }                      │                         │
│  └──────────────────┬──────────────────────┘                         │
│                      │  IcebergCommittable                            │
│                      ▼                                                │
│  ┌─────────────────────────────────────────┐                         │
│  │  IcebergCommitter  (parallelism = 1)     │                         │
│  │  implements Committer<IcebergCommittable> │                         │
│  │                                          │                         │
│  │  commit(CommitRequest<IcebergCommittable>)│                        │
│  │    → read DeltaManifests                 │                         │
│  │    → table.newAppend() / newRowDelta()   │                         │
│  │    → set snapshot properties             │                         │
│  │    → operation.commit()                  │                         │
│  └──────────────────┬──────────────────────┘                         │
│                      │                                                │
│                      ▼                                                │
│  ┌─────────────────────────────────────────┐                         │
│  │  PostCommitTopology (optional)           │                         │
│  │  TableMaintenance:                       │                         │
│  │    - RewriteDataFiles (compaction)       │                         │
│  │    - ExpireSnapshots                     │                         │
│  └─────────────────────────────────────────┘                         │
└──────────────────────────────────────────────────────────────────────┘
```

Source: `IcebergSink.java`, `IcebergSinkWriter.java`,
`IcebergWriteAggregator.java`, `IcebergCommitter.java`

---

# Part 3: Direct File Append to Iceberg

## How Files Are Created

Flink writers create Parquet/ORC/Avro files **directly on the table's data directory** in
S3/HDFS. There is no staging area or temporary location — files land where readers expect them.

```
┌──────────────────────────────────────────────────────────────────────┐
│  FILE CREATION PIPELINE                                               │
│                                                                       │
│  RowData record                                                       │
│       │                                                               │
│       ▼                                                               │
│  RowDataTaskWriterFactory.create()                                    │
│       │                                                               │
│       ├── Unpartitioned table?                                        │
│       │     → UnpartitionedDeltaWriter                                │
│       │         (single RowDataDeltaWriter)                           │
│       │                                                               │
│       └── Partitioned table?                                          │
│             → PartitionedDeltaWriter                                  │
│                 (one RowDataDeltaWriter per partition)                 │
│                                                                       │
│  RowDataDeltaWriter                                                   │
│       │                                                               │
│       ├── write(row) → appends to data file writer                   │
│       │     → s3://bucket/db/table/data/part=A/00001-abc.parquet     │
│       │                                                               │
│       └── delete(row) → appends to delete file writer (if upsert)    │
│             → s3://bucket/db/table/data/part=A/00001-abc-deletes.parquet │
│                                                                       │
│  On checkpoint barrier:                                               │
│    writer.complete() → closes all open files                         │
│       │                                                               │
│       ▼                                                               │
│  WriteResult {                                                        │
│    DataFile[] dataFiles;       // completed data files                │
│    DeleteFile[] deleteFiles;   // completed equality/position deletes │
│  }                                                                    │
│                                                                       │
│  Files exist on S3 but are NOT YET visible to readers.               │
│  They become visible only after Iceberg metadata commit.              │
└──────────────────────────────────────────────────────────────────────┘
```

## From WriteResult to Iceberg Snapshot

WriteResults are serialized into **DeltaManifests** — compact manifest files that list
which data/delete files belong to this checkpoint:

```
WriteResult (per writer subtask)
    │
    │  FlinkManifestUtil.writeDataFiles() → manifest file on S3
    │  FlinkManifestUtil.writeDeleteFiles() → manifest file on S3
    ▼
DeltaManifests {
    ManifestFile dataManifest;     // points to manifest listing DataFiles
    ManifestFile deleteManifest;   // points to manifest listing DeleteFiles
    ReferencedDataFile[];          // for DV (deletion vector) support
}
    │
    │  serialized to byte[] → stored in operator state (checkpoint)
    ▼
On commit:
    Read manifests back → extract DataFile[] and DeleteFile[]
    │
    ├── Append-only (no deletes):
    │     AppendFiles operation = table.newAppend();
    │     dataFiles.forEach(operation::appendFile);
    │     operation.commit();
    │
    └── Has deletes (upsert mode):
          RowDelta operation = table.newRowDelta();
          dataFiles.forEach(operation::addRows);
          deleteFiles.forEach(operation::addDeletes);
          operation.commit();
```

Source: `RowDataTaskWriterFactory.java`, `BaseDeltaTaskWriter.java`,
`FlinkManifestUtil.java`, `DeltaManifests.java`

---

# Part 4: How Iceberg Remembers the Checkpoint

Iceberg tracks Flink checkpoint identity at **two layers**, with very different roles:

| Layer | Where it lives | Lifetime | Role |
|-------|----------------|----------|------|
| **Durable** | Iceberg **snapshot summary** (key/value map in table metadata JSON) | Permanent — survives across JVMs, jobs, and catalog migrations | The single source of truth for "what has been committed". Read on recovery to compute `maxCommittedCheckpointId`. |
| **Transient** | Flink **staging manifest Avro filenames** (under `<metadata-dir>/` or `flink.manifests.location`) | From `prepareCommit()` until `notifyCheckpointComplete()` — deleted right after a successful Iceberg commit | Carries the in-flight `(jobId, operatorId, subTaskId, attemptNumber, checkpointId)` from writer → committer without filename collisions across writers, attempts, restarts, or sibling sinks. |

Parts 4.1–4.4 cover the **durable** layer (snapshot summary). Part 4.5 covers the
**transient** layer (the encoded Avro filename — what the user actually sees on S3
between checkpoint and commit).

---

## Part 4.1: Durable — Snapshot Summary Properties

### The Key Insight: Checkpoint ID, Not Kafka Offset

The connector does **not** store Kafka consumer offsets directly in Iceberg. Instead, it
stores the **Flink checkpoint ID** — which transitively captures Kafka offsets because
Flink's checkpoint mechanism snapshots the Kafka consumer position.

```
┌──────────────────────────────────────────────────────────────────────┐
│  OFFSET TRACKING CHAIN                                                │
│                                                                       │
│  Kafka Partition Offsets                                               │
│    topic-0: offset=15000                                              │
│    topic-1: offset=22000                                              │
│    topic-2: offset=18000                                              │
│       │                                                               │
│       │  Flink checkpoints Kafka source state                         │
│       │  (offsets saved in checkpoint state backend)                   │
│       ▼                                                               │
│  Flink Checkpoint ID = 100                                             │
│    Contains: Kafka offsets + all operator states                      │
│       │                                                               │
│       │  IcebergCommitter writes to snapshot:                          │
│       ▼                                                               │
│  Iceberg Snapshot #987 properties:                                    │
│    "flink.max-committed-checkpoint-id" = "100"                        │
│    "flink.job-id" = "abc-def-123"                                    │
│    "flink.operator-id" = "sink-op-456"                               │
│                                                                       │
│  This means: Snapshot #987 contains ALL data that was                │
│  consumed up to Kafka offsets {15000, 22000, 18000}.                 │
│  Any checkpoint > 42 will have later offsets.                         │
└──────────────────────────────────────────────────────────────────────┘
```

## Part 4.2: Snapshot Properties Set on Every Commit

```java
// IcebergCommitter — on each commit:
operation.set("flink.max-committed-checkpoint-id", Long.toString(checkpointId));
operation.set("flink.job-id", newFlinkJobId);
operation.set("flink.operator-id", operatorId);
```

| Property | Value | Purpose |
|----------|-------|---------|
| `flink.max-committed-checkpoint-id` | Checkpoint ID (long) | Identifies latest committed checkpoint for idempotent recovery |
| `flink.job-id` | Flink job UUID | Distinguishes snapshots from different jobs writing to same table |
| `flink.operator-id` | Operator UUID | Distinguishes snapshots from multiple sinks in same job |

## Part 4.3: Why Store These in Iceberg? Isn't the Flink Checkpoint Enough?

Flink's checkpoint state alone is **not sufficient** to prevent duplicate commits. The
problem is a gap between what Flink knows and what Iceberg knows:

```
┌──────────────────────────────────────────────────────────────────────┐
│  WHY FLINK CHECKPOINT STATE IS NOT ENOUGH                             │
│                                                                       │
│  THE GAP PROBLEM:                                                     │
│                                                                       │
│  1. Committer calls operation.commit()  → Iceberg snapshot created   │
│  2. Committer removes checkpoint from pending state                  │
│  3. Flink checkpoints the committer's updated state                  │
│                                                                       │
│  What if the job CRASHES between step 1 and step 3?                  │
│                                                                       │
│  ┌────────┐     ┌─────────┐     ┌────────────────┐                  │
│  │ COMMIT │────▶│ REMOVE  │──✗──│ CHECKPOINT     │                  │
│  │ to     │     │ from    │     │ updated state  │                  │
│  │ Iceberg│     │ pending │     │ to Flink       │                  │
│  └────────┘     │ state   │     └────────────────┘                  │
│    ✓ done       └─────────┘       CRASH HERE!                        │
│                                                                       │
│  On restart:                                                          │
│    Flink state says: checkpoint 100 is STILL PENDING (old state)      │
│    Iceberg says: checkpoint 100 was ALREADY COMMITTED (snapshot)      │
│                                                                       │
│  Without Iceberg snapshot properties:                                │
│    → Committer would re-commit checkpoint 100 → DUPLICATE DATA!      │
│                                                                       │
│  With Iceberg snapshot properties:                                    │
│    → Committer reads flink.max-committed-checkpoint-id = 100          │
│    → Knows checkpoint 100 was already committed                       │
│    → Calls signalAlreadyCommitted() → SKIP                          │
│    → NO DUPLICATES ✓                                                 │
└──────────────────────────────────────────────────────────────────────┘
```

There are also scenarios where Flink checkpoint state is entirely absent:

```
┌──────────────────────────────────────────────────────────────────────┐
│  OTHER SCENARIOS WHERE FLINK STATE CAN'T HELP                        │
│                                                                       │
│  1. JOB CANCELLED + RESTARTED (new job ID)                           │
│     Old checkpoint state is discarded. New job has no memory of      │
│     what was committed. Only Iceberg snapshots know.                 │
│                                                                       │
│  2. SAVEPOINT RESTORE TO EARLIER POINT                               │
│     User restores from savepoint taken before checkpoint 42.         │
│     Flink state says nothing was committed after the savepoint.      │
│     But Iceberg already has checkpoint 42's data.                    │
│     → flink.job-id + max-committed-checkpoint-id prevents re-commit. │
│                                                                       │
│  3. MULTIPLE JOBS WRITING TO SAME TABLE                              │
│     Job A and Job B both write to table T.                           │
│     Job A's Flink state knows nothing about Job B's commits.         │
│     → flink.job-id in snapshot distinguishes whose commits are whose.│
│                                                                       │
│  4. MULTIPLE SINKS IN SAME JOB                                       │
│     One Flink job has two IcebergSink operators writing to table T.  │
│     Each sink's state only knows about its own commits.              │
│     → flink.operator-id distinguishes sink-1's commits from sink-2's.│
│                                                                       │
│  In all cases: Iceberg snapshot properties are the GROUND TRUTH      │
│  that survives across job restarts, savepoint restores, and          │
│  multi-writer scenarios. Flink checkpoint state is supplementary.     │
└──────────────────────────────────────────────────────────────────────┘
```

## Part 4.4: Recovery: Finding the Last Committed Checkpoint

On job restart, `SinkUtil.getMaxCommittedCheckpointId()` scans Iceberg snapshots:

```
SinkUtil.getMaxCommittedCheckpointId(table, flinkJobId, operatorId, branch)
    │
    │  Walk snapshot chain backward (newest → oldest):
    │
    │  Snapshot #989: job-id=abc-def-123, operator-id=sink-op-456
    │                 max-committed-checkpoint-id = 44  ← FOUND!
    │
    │  Return 44 → only commit checkpoints > 44
    │
    │  If no matching snapshot found:
    │  Return INITIAL_CHECKPOINT_ID (-1) → commit everything
```

This means if you need actual Kafka offsets, you embed them as columns in the RowData
or use Flink's checkpoint metadata. The connector itself only tracks the checkpoint ID
boundary.

Source: `SinkUtil.java`, `IcebergCommitter.java`

---

## Part 4.5: Transient — Flink Staging Manifest Avro Filenames

Between `prepareCommit()` and `notifyCheckpointComplete()` the connector materializes a
**Flink-private Avro "delta manifest"** for each checkpoint. This is **not** an Iceberg
manifest in the catalog yet — it's a staging file that lists the `DataFile`s and
`DeleteFile`s the writer produced for one checkpoint, so that the committer can read
them back later when it actually invokes `table.newAppend()` / `table.newRowDelta()`.

The filename is the connector's identity tag. It encodes everything needed to keep
parallel writers, restarts, retries, and sibling sinks from clobbering each other on
shared storage.

### Generator: Who Creates These Files, When, and Why

| Pipeline | Generator (operator) | Generator function | Called from | Output |
|---|---|---|---|---|
| **Sink V2 (`IcebergSink`)** | `IcebergWriteAggregator` (parallelism 1) | `IcebergWriteAggregator.writeToManifest(results, checkpointId)` → `FlinkManifestUtil.writeCompletedFiles(...)` | `prepareSnapshotPreBarrier(checkpointId)` — fires when the checkpoint barrier reaches the aggregator | Up to **2 Avro files** per checkpoint: one data manifest (if any `DataFile`s), one delete manifest (if any `DeleteFile`s). Wrapped in a `DeltaManifests` and serialized into `IcebergCommittable.manifest()` bytes (typically a few hundred bytes — just pointers to the Avro paths, not the file contents). |
| **Legacy (`FlinkSink`)** | `IcebergFilesCommitter` (parallelism 1) | `IcebergFilesCommitter.writeToManifest(checkpointId, writeResults)` → `FlinkManifestUtil.writeCompletedFiles(...)` | `snapshotState(StateSnapshotContext)` — fires at checkpoint barrier on the committer | Same shape. Bytes are stored in the operator's `ListState<SortedMap<Long, byte[]>>` keyed by checkpointId. |

The actual filename is built inside **`ManifestOutputFileFactory.generatePath(long checkpointId)`** (`ManifestOutputFileFactory.java:66-78`):

```java
private String generatePath(long checkpointId) {
    return FileFormat.AVRO.addExtension(           // appends ".avro"
        String.format(
            Locale.ROOT,
            "%s-%s-%05d-%d-%d-%05d%s",
            flinkJobId,
            operatorUniqueId,
            subTaskId,                              // %05d
            attemptNumber,
            checkpointId,
            fileCount.incrementAndGet(),            // %05d, monotonic per factory instance
            suffix != null ? "-" + suffix : ""));
}
```

The factory itself is constructed in the generator operator's `open()` (V2) /
`initializeState()` (legacy), captured fields fixed for the operator's lifetime
(jobId, operatorId, subtaskId, attemptNumber). The only thing that varies per call is
`checkpointId` and the internal `AtomicInteger fileCount` — so each call within the
same operator instance gets a monotonically increasing `fileCount`.

### The Format

```
{flinkJobId}-{operatorUniqueId}-{subTaskId:05d}-{attemptNumber}-{checkpointId}-{fileCount:05d}{-suffix}?.avro
```

Source: `ManifestOutputFileFactory.java:66-78` (Java source already shown in the
generator section above).

### Concrete Example

In the documented Sink V2 path, the only operator writing manifests is the
parallelism-1 `IcebergWriteAggregator` (`subTaskId` always `00000`). A typical
metadata directory therefore looks like:

```
s3://bucket/db/table/metadata/
  4a1b...-aggOp-00000-0-87-00001.avro    ← aggregator, ckpt 87, data manifest
  4a1b...-aggOp-00000-0-87-00002.avro    ← aggregator, ckpt 87, delete manifest (upsert mode)
  4a1b...-aggOp-00000-0-88-00001.avro    ← aggregator, ckpt 88, data manifest
  4a1b...-aggOp-00000-1-89-00001.avro    ← aggregator, ckpt 89, attempt 1 (after restart)
```

The `fileCount` counter (last segment) advances within one
`(operator, attempt)` lifetime — so you see `00001` then `00002` for the data and
delete manifests of the same checkpoint, then `00001` again after a restart since
a fresh factory is constructed. The legacy `IcebergFilesCommitter` produces the
exact same shape (also `subTaskId = 00000`, since it too is parallelism 1).

### What Each Component Buys You

| Component | Why it's in the filename |
|-----------|-------------------------|
| `flinkJobId` | Two parallel Flink jobs writing the same table never collide; survives a `jobId` change on restart because the *current* job's id is baked in at write time |
| `operatorUniqueId` | Two `IcebergSink` operators inside one job (e.g., fan-out to two tables in the same DAG) get disjoint filenames |
| `subTaskId` (%05d) | Reserved in the format for code paths where multiple subtasks might write manifests (e.g., dynamic sink). In the documented Sink V2 + legacy paths, the aggregator/committer at parallelism 1 always emits with `subTaskId = 00000`. |
| `attemptNumber` | After a task-local failure and restart, the retry writes to a fresh filename instead of overwriting the previous attempt's half-written file |
| `checkpointId` | Files for different in-flight checkpoints don't share a name; lets the committer disambiguate per-checkpoint manifests |
| `fileCount` (%05d) | One writer typically emits up to **two** staging manifests per checkpoint (one for data files, one for delete files — see `FlinkManifestUtil.writeCompletedFiles`); this counter guarantees uniqueness even within the same `(writer, attempt, checkpoint)` |
| `suffix` (optional) | Used by `dynamic` sinks (`flink/sink/dynamic/`) to tag manifests with a logical sub-table, so manifests from different downstream tables don't collide in shared metadata |

### Avro Filename vs Snapshot Summary — Pick the Right Layer

```
┌──────────────────────────────────────────────────────────────────────┐
│   IDENTITY TRACKING LAYERS                                            │
│                                                                       │
│                       checkpoint barrier N                            │
│                              │                                        │
│   writer subtask 0  ─────────┼──── WriteResult ─┐                     │
│   writer subtask 1  ─────────┼──── WriteResult ─┤  (in-memory records │
│   writer subtask 2  ─────────┼──── WriteResult ─┤   on Flink dataflow,│
│                              │                  │   NOT Avro files)   │
│                              ▼                  ▼                     │
│                       ┌──────────────────────────────────────┐        │
│                       │ IcebergWriteAggregator (parallelism 1)│        │
│                       │                                       │        │
│                       │ collects WriteResults since last      │        │
│                       │ barrier, then in prepareSnapshotPre-  │        │
│                       │ Barrier(N):                           │        │
│                       │                                       │        │
│                       │ → writes ONE Avro manifest:           │        │
│                       │   jobA-aggOp-00000-0-87-00001.avro    │        │
│                       │   (+ second one for delete files,     │        │
│                       │    if any: ...-87-00002.avro)         │        │
│                       │                                       │        │
│                       │ → emits IcebergCommittable with the   │        │
│                       │   serialized DeltaManifests pointer   │        │
│                       └─────────────────┬─────────────────────┘        │
│                                         │                              │
│                          notifyCheckpointComplete                      │
│                                         │                              │
│                                         ▼                              │
│                       ┌────────────────────────────────────┐           │
│                       │ IcebergCommitter (parallelism 1)   │           │
│                       │                                    │           │
│                       │ reads Avro back via table.io(),    │           │
│                       │ table.newAppend()/newRowDelta(),   │           │
│                       │ then operation.commit() →           │           │
│                       │                                    │           │
│                       │ DURABLE (Iceberg snapshot summary):│           │
│                       │ snapshot #987 properties:          │           │
│                       │   flink.max-committed-checkpoint-id│           │
│                       │     = "87"                         │           │
│                       │   flink.job-id     = "jobA"        │           │
│                       │   flink.operator-id = "aggOp"      │           │
│                       └─────────────────┬──────────────────┘           │
│                                         │                              │
│                       FlinkManifestUtil.deleteCommittedManifests       │
│                                         │  (best-effort GC)            │
│                                         ▼                              │
│                       transient Avro files removed                     │
│                       from S3 (failure is logged, not fatal)           │
└──────────────────────────────────────────────────────────────────────┘
```

**The Avro filename is NOT how Iceberg "remembers" the checkpoint** — the snapshot
summary is. The filename is an *operational* signal:

- **Collision avoidance** during in-flight writes
- **Crash forensics** — if a job dies between commit and manifest GC, the leftover Avro file's
  name tells you exactly which `(jobId, operatorId, subTaskId, attemptNumber, checkpointId)`
  produced it
- **Idempotent staging** — the writer can re-emit the same checkpoint after a task restart
  with a higher `attemptNumber` and the old half-written file stays out of the way

### Why Stage to Avro at All — Why Not Commit to Iceberg Directly?

A reasonable instinct on first read is: "the writers already produced the data files —
why not just have each writer call `table.newAppend().commit()` and skip the Avro
staging step?" Four constraints make direct commits the wrong design.

**1. Iceberg snapshots are atomic per *commit*, not per *record*.** Every commit
produces exactly one new snapshot. It rewrites the table-metadata JSON, writes a new
manifest list, and CAS-swaps the catalog pointer (or `version-hint.text` on a
HadoopCatalog). That CAS is the atomicity boundary — all-or-nothing.

A single Flink checkpoint typically spans **dozens to hundreds of parallel writer
subtasks**, each producing its own data files. All of them belong to the same logical
batch and must appear together. So there is a fan-in problem: N writers → 1 commit.
The aggregator/committer at parallelism 1 is the join point. The Avro file is just
the format the aggregator uses to hand its merged file-list to the committer.

If every writer subtask committed directly, you'd get N parallel `AppendFiles.commit()`
calls racing on the same metadata pointer. Most would lose the CAS, retry, lose again,
and throughput would collapse under optimistic-concurrency thrash. Even if you
serialized them, you'd produce N snapshots per checkpoint instead of 1, which makes
time-travel queries and downstream incremental scans wildly noisier.

**2. Flink's two-phase commit forbids visibility before `notifyCheckpointComplete`.**
Flink's exactly-once contract has a hard separation:

- **Phase 1** (`snapshotState` / `prepareSnapshotPreBarrier`): persist enough durable
  state to survive a crash, but **do not** make changes externally visible.
- **Phase 2** (`notifyCheckpointComplete`): now it is safe to publish.

If you commit to Iceberg in Phase 1 and the Flink checkpoint itself then fails
(JobManager crash, network partition during barrier alignment), on restart Flink
replays from the **prior** checkpoint. That replay re-processes the same source
offsets → produces a **second** Iceberg snapshot for the same data → duplicates.

The whole reason for the Avro file is to anchor the prepare result *somewhere durable*
(object storage) so the committer can replay it in Phase 2 even if the JVM died in
between. "Update Iceberg metadata directly on the barrier" merges Phase 1 and Phase 2
into one step, which breaks the guarantee.

**3. Inlining `DataFile` metadata into Flink checkpoint state is too expensive.** The
obvious alternative to spilling is putting `DataFile[]` + `DeleteFile[]` straight into
the committer's operator state. But a single `DataFile` record carries: path,
partition tuple, record count, file size, **per-column lower bounds**, **per-column
upper bounds**, null counts, NaN counts, value counts, optional distinct counts,
split offsets, sort order, key metadata. For a wide table (200 columns) with thousands
of files per checkpoint, that is easily **tens of MB per checkpoint per writer**.

Multiply by parallelism × checkpoint retention. Every barrier round-trips that
through the Flink state backend (often RocksDB on object storage), and the JobManager
has to ship state handles around on rescale. The `IcebergCommittable.manifest()`
byte[] is, by contrast, **a few hundred bytes** — just the pointers to the Avro file
plus the `(jobId, operatorId, checkpointId)` triplet.

**4. Catalog commits are heavy and often globally serialized.** Hive, Glue, JDBC, and
Nessie catalogs all enforce a per-table lock or CAS pattern. Glue in particular
rate-limits `UpdateTable` calls per account-region and is the bottleneck in many
production Iceberg pipelines. Committing **once per checkpoint per table** — the
maximum batch the protocol allows — amortizes lock acquisition, catalog round-trips,
metadata-JSON rewrites, and `RewriteFiles` planning over thousands of data files.
Committing per record or per writer would push commit rate up by 2–4 orders of
magnitude.

### Why Avro Specifically, and Not JSON or Protobuf?

The staging file is written with
`ManifestFiles.write(formatVersion, spec, outputFile, DUMMY_SNAPSHOT_ID)` — **the same
writer Iceberg uses internally for its own manifests**. At commit time it is read
back via `ManifestFiles.read(...)`, the same path. So the staging file is already a
valid Iceberg manifest with a placeholder snapshot id; the commit does not need to
translate formats. The cost of writing it is negligible relative to the data files
already produced.

### Why the Iceberg Metadata Directory, and Not Flink's Checkpoint Storage?

A natural follow-up: granted we need to spill *somewhere*, why does the file land
under `<table>/metadata/` (or `flink.manifests.location`) instead of inside Flink's
own checkpoint blob? Six reasons, in roughly the order they bite in practice:

**1. The committer reads it back with Iceberg's `FileIO`, not Flink's state backend.**
Look at the actual recovery path:

```java
// IcebergCommitter.commitPendingRequests(), IcebergFilesCommitter.commitUpToCheckpoint()
DeltaManifests dm = SimpleVersionedSerialization.readVersionAndDeSerialize(...);
WriteResult wr = FlinkManifestUtil.readCompletedFiles(dm, table.io(), table.specs());
//                                                         ^^^^^^^^^^
//                                          Iceberg's FileIO — the same one that
//                                          wrote the data files (Parquet/ORC)
```

`table.io()` is the FileIO Iceberg has already fully configured for the table:
credentials, region, endpoint, TLS, retry, encryption (KMS keys), assumed-role —
all of it. Flink's checkpoint storage is a **separate** abstraction
(`StateBackend.createCheckpointStorage(...)`), often pointed at a different
bucket/account with different IAM. If the staging manifest lived in checkpoint
storage, the committer would need both FileIO **and** Flink's filesystem layer just
to recover. Keeping it in the table's storage means one IO surface, one auth model,
one set of bugs.

**2. Iceberg owns the cleanup of its directory.** Iceberg ships
`RemoveOrphanFilesAction` and `expire-snapshots`, both of which already know how to
enumerate `<table>/metadata/` and `<table>/data/`. An orphan staging Avro left
behind by a crash (the `deleteCommittedManifests()` GC is best-effort) gets swept by
the same maintenance tooling that handles orphan data files. If staging manifests
lived in Flink checkpoint state, you'd need a separate, Flink-aware cleanup
mechanism — and Iceberg's tools would have no way to even see them.

**3. Globally readable, regardless of which TaskManager recovers.** A staging
manifest written by TaskManager A must be readable by TaskManager B after
rescheduling. Iceberg's table storage (S3/HDFS/GCS) is by definition globally
readable across the cluster — that's a precondition for the table existing at all.
Flink checkpoint state is also globally readable in most setups, but it's
operator-state-handle-shaped: you'd need to fan out the right state handle to the
right subtask, instead of just passing a path string. The path-in-state design keeps
the committer dumb: "give me a URL, I'll fetch it."

**4. Flink checkpoint blobs are the wrong granularity.** A Flink checkpoint is one
logical atomic snapshot per barrier, persisted as a structured collection of state
handles. Per-checkpoint manifests would have to be either (a) materialized into
operator state (defeats the purpose — see "Why stage to Avro at all" Point 3 above)
or (b) referenced via Flink's BLOB store, which is designed for user-jar / library
distribution rather than per-checkpoint write traffic. Iceberg's metadata directory
is a flat write-once filesystem prefix, exactly the shape the workload wants.

**5. Crash forensics and ad-hoc inspection.** When something goes sideways, the
first thing on-call does is `aws s3 ls s3://bucket/db/table/metadata/`. Staging
manifests show up there with filenames that decode to
`(jobId, operatorId, subTaskId, attemptNumber, checkpointId)` — you can see at a
glance which checkpoint failed to commit and from which writer. If those files
lived inside a Flink checkpoint blob, you'd need to know the exact checkpoint
storage layout, possibly a state-backend-specific tool (RocksDB SST reader, custom
serializer), and the operator UID, just to find them.

**6. Forward-compatible with manifest-level commits.** The current Flink code
decomposes the staged manifest into individual `DataFile` objects and calls
`appendFiles.appendFile(dataFile)` per file (`IcebergCommitter.java:248`,
`IcebergFilesCommitter.java:345`). But because the file is already a valid Iceberg
manifest, an optimized commit path could call `AppendFiles.appendManifest(...)`
directly to skip the per-file rewrite. That optimization only works if the manifest
already lives under the same `FileIO` as the data files — which it does, because
it's in `metadata/`. Putting it inside Flink's checkpoint store would foreclose
this optimization permanently.

### The `flink.manifests.location` Escape Hatch

The default location is the table's metadata directory, but
`ManifestOutputFileFactory.java:81-91` honors a table-property override:

```java
String flinkManifestDir = props.get(FLINK_MANIFEST_LOCATION);  // "flink.manifests.location"
if (Strings.isNullOrEmpty(flinkManifestDir)) {
    newManifestFullPath = ops.metadataFileLocation(generatePath(checkpointId));
} else {
    newManifestFullPath = stripTrailingSlash(flinkManifestDir) + "/" + generatePath(...);
}
```

Set this in **table properties** when:

- The table's metadata bucket is throttled or has tight lifecycle rules and you
  want to keep Flink's churn off it.
- You want to apply a short lifecycle policy (e.g., 24h TTL) to staging files
  without affecting Iceberg's own manifests.
- You want a separate prefix that's easy to grep when auditing.

Whatever you choose, the constraint is the same: the location must be reachable via
the same `FileIO` instance the table uses, since that's what the committer hands to
`ManifestFiles.read(...)` at recovery time.

### The Mental Model

```
Flink barrier  →  WriteResult (small, in-memory) flows to aggregator
                  Aggregator spills it as an Avro manifest (cheap, ~KB)
                  Avro path goes into Flink checkpoint state (tiny, ~bytes)
                  ↓
                 (durability boundary — survives crashes)
                  ↓
notifyCheckpointComplete  →  Committer reads Avro back → ONE Iceberg commit
                              → snapshot summary records ckpt id → Avro GC'd
```

The Avro file is not a workaround — it is the **data-durability layer** of the
two-phase commit. Iceberg's metadata is the **visibility layer**. They are separate
by design because exactly-once needs both, and folding them would collapse the
guarantee.

### Lifecycle 1 — Use During Normal Sink Write (No Failure)

```
┌──────────────────────────────────────────────────────────────────────┐
│  WRITE-PATH LIFECYCLE OF ONE STAGING AVRO FILE (Sink V2)              │
│                                                                       │
│  T0  Writer subtasks receive records, RowDataTaskWriter writes        │
│      Parquet/ORC data files directly to <table>/data/                 │
│      (no Avro yet)                                                    │
│                                                                       │
│  T1  Checkpoint barrier N reaches IcebergSinkWriter:                  │
│        prepareCommit() → writer.complete() → WriteResult              │
│        (lists DataFiles + DeleteFiles by path; ~KB per writer)        │
│                                                                       │
│  T2  WriteResult flows downstream as CommittableMessage to the         │
│      IcebergWriteAggregator (parallelism = 1).                        │
│                                                                       │
│  T3  Checkpoint barrier N reaches IcebergWriteAggregator:             │
│        prepareSnapshotPreBarrier(N) {                                 │
│          // collect WriteResults received since last barrier          │
│          icebergManifestOutputFileFactory.create(N)                   │
│            → ManifestOutputFileFactory.generatePath(N) builds         │
│              "fa343...-9dfve-00000-0-N-00001.avro"                    │
│          ManifestWriter writes a single Avro file listing all         │
│            DataFiles; (optionally a second one for DeleteFiles)       │
│          → ManifestFile objects wrapped in DeltaManifests             │
│          → serialized (just paths + metadata) into IcebergCommittable │
│        }                                                              │
│        At this point the Avro EXISTS on object storage but no         │
│        Iceberg snapshot points to it yet.                             │
│                                                                       │
│  T4  IcebergCommittable (the *bytes*, not the avro file) is emitted   │
│      to IcebergCommitter. Flink also includes these bytes in          │
│      operator state as part of checkpoint N.                          │
│                                                                       │
│  T5  Flink checkpoint N completes → notifyCheckpointComplete(N) fires │
│      on IcebergCommitter:                                             │
│        for each pending request:                                      │
│          DeltaManifests dm = deserialize(committable.manifest())      │
│          WriteResult wr = FlinkManifestUtil.readCompletedFiles(dm, …) │
│            ← THIS reads the Avro file back from object storage        │
│              into in-memory DataFile[]/DeleteFile[]                   │
│          table.newAppend()/newRowDelta().appendFile(...).commit()     │
│            ← new Iceberg snapshot is created; data is now visible     │
│          set snapshot summary: flink.max-committed-checkpoint-id = N  │
│                                                                       │
│  T6  After successful commit:                                         │
│        FlinkManifestUtil.deleteCommittedManifests(table, manifests…)  │
│          → io.deleteFile(manifest.path()) for each Avro path          │
│            ← THE AVRO FILE IS DELETED                                 │
│        Failure here is logged, NOT fatal (the commit is durable).     │
│                                                                       │
│  Net: each staging Avro file is born at T3, read once at T5, deleted  │
│  at T6 — typical lifetime is one Flink checkpoint interval.           │
└──────────────────────────────────────────────────────────────────────┘
```

So the avro file is used as a **handoff buffer** between two operators that don't
share JVM memory at restart time. The aggregator could in principle pass the full
DataFile/DeleteFile list to the committer in `IcebergCommittable.manifest()` bytes —
but those lists can be **very large** (thousands of file paths × ~1KB metadata each),
and Flink committer state is checkpointed on every barrier. Spilling them to an Avro
file and shipping only the manifest *pointer* keeps checkpoint-state bytes small.

### Lifecycle 2 — Use During Flink Restart / Replay

A restart can hit the committer in three distinct states. The Avro file plays
different roles in each:

**Case A: checkpoint N was fully committed before the crash.**

```
Before crash:
  T3 → T6 all completed for checkpoint N
  Iceberg has snapshot with flink.max-committed-checkpoint-id = N
  The Avro file was deleted at T6

After restart:
  Committer state (from checkpoint N-1 or earlier) contains nothing for N.
  → No reference to the deleted Avro. No problem.
```

**Case B: checkpoint N was written to state, but commit failed (crashed between T4 and T5).**

```
Before crash:
  T3 succeeded → Avro file "...-N-00001.avro" exists on S3
  T4 succeeded → IcebergCommittable bytes in Flink state
  T5 NOT REACHED → no Iceberg snapshot for N
  T6 NOT REACHED → Avro file still on S3

After restart from checkpoint N:
  1. IcebergCommitter restores state, sees pending IcebergCommittable for N.
  2. SinkUtil.getMaxCommittedCheckpointId(...) walks Iceberg snapshot chain
     backward, finds no snapshot with matching (jobId, operatorId) for N
     → returns N-1 (or whatever was last actually committed, possibly -1).
  3. Because N > maxCommittedCheckpointId, the committer proceeds:
       FlinkManifestUtil.readCompletedFiles(deserialize(committable.manifest()),…)
       ← REREADS the same "...-N-00001.avro" file from S3
     → extracts DataFile[]/DeleteFile[]
     → commits → snapshot summary now has flink.max-committed-checkpoint-id = N
     → deletes the Avro file (T6)

This is the ONE scenario where the staging Avro file is load-bearing across a
restart. Lose the file here and you cannot recover checkpoint N's commit —
the data files (Parquet/ORC) still exist as orphans under <table>/data/, but
the connector has lost the index of which ones belong to N. (Recovery: drop
checkpoint N's committable via allowNonRestoredState, replay from the source.)
```

**Case C: checkpoint N was committed AND Avro deleted, but Flink state somehow still references it.**

```
This is the case behind the "file not found" error from the previous question.
It happens after a state/snapshot mismatch — e.g., job restored from an older
savepoint, or operator state corruption.

After restart:
  1. IcebergCommitter restores state with pending IcebergCommittable for N.
  2. SinkUtil.getMaxCommittedCheckpointId(...) returns M ≥ N
     (Iceberg snapshot summary already records N as committed).
  3. The commit() method does:
       commitRequestMap.headMap(maxCommittedCheckpointId, true)
                       .forEach(CommitRequest::signalAlreadyCommitted);
     → signals "already committed" WITHOUT calling readCompletedFiles
     → the deleted Avro file is NEVER touched. No error.

But: if a code path attempts to read the manifest BEFORE the
signalAlreadyCommitted check (e.g., custom subclass, or a path in
IcebergFilesCommitter.commitUpToCheckpoint that deserializes before checking),
you get FileNotFoundException for the already-GC'd Avro file.
```

### How `IcebergFilesCommitter` (Legacy) Differs on Restart

The legacy operator does one extra thing: it persists the **prior job's** `flinkJobId`
in operator state (`jobIdState`). On restart with a new jobId, it reads
`restoredFlinkJobId` from state and calls
`SinkUtil.getMaxCommittedCheckpointId(table, restoredFlinkJobId, operatorUniqueId, …)`.
This is what lets a job restored from a savepoint find its own prior commits even when
`pipeline.job-id` was not pinned. Sink V2 relies on the user pinning `pipeline.job-id`
or on the `flink.job-id` recorded in the snapshot summary matching the current job.

Then it walks the restored `SortedMap<Long, byte[]>` of pending manifests:

```java
NavigableMap<Long, byte[]> uncommittedDataFiles =
    Maps.newTreeMap(checkpointsState.get().iterator().next())
        .tailMap(maxCommittedCheckpointId, false);   // strictly > committed
if (!uncommittedDataFiles.isEmpty()) {
    commitUpToCheckpoint(uncommittedDataFiles, restoredFlinkJobId, operatorUniqueId,
                         uncommittedDataFiles.lastKey());
}
```

Inside `commitUpToCheckpoint`, deserializing the byte[] → `DeltaManifests` → calling
`readCompletedFiles` is what brings the **Avro file** back into play. If the file is
missing here (Case C above), this is the line that throws `FileNotFoundException`:
`IcebergFilesCommitter.java:271-277`.

### Cleanup: Best-Effort, Failures Are Logged

After a successful `operation.commit()`, the committer calls
`FlinkManifestUtil.deleteCommittedManifests(...)` which iterates the manifests and
calls `io.deleteFile(manifest.path())`. **A delete failure does not abort the
checkpoint** — it logs a warning and moves on:

```java
} catch (Exception e) {
    // The flink manifests cleaning failure shouldn't abort the completed checkpoint.
    LOG.warn(
        "The iceberg transaction has been committed, but we failed to clean "
        + "the temporary flink manifests: {}", details, e);
}
```

So in a long-running job, **occasional orphan `.avro` files in the metadata directory
are expected** — they're not corrupted data; they're staging files whose GC step
failed. They're safe to delete by name once you confirm the matching
`flink.max-committed-checkpoint-id` is set on a later snapshot.

### V1 (Legacy FlinkSink) vs V2 (IcebergSink) — Same Filename, Different Owners

Both pipelines use the same `ManifestOutputFileFactory`, but the **owner subtask** of
the manifest differs:

| Pipeline | Manifest written by | `subTaskId` in filename |
|----------|---------------------|------------------------|
| **Legacy `IcebergFilesCommitter`** | The single committer operator (parallelism 1) writes ONE merged manifest per checkpoint, after collecting WriteResults from all upstream writers | Always `00000` |
| **Sink V2 `IcebergWriteAggregator`** | The aggregator operator (parallelism 1) merges WriteResults and writes ONE manifest | Always `00000` (the aggregator asserts `subTaskId == 0`) |
| **Sink V2 `IcebergSinkWriter`** | Writers themselves emit `WriteResult`s — they do **not** write Avro manifests; the aggregator does | N/A — writers don't create manifests |

So in practice on S3 you'll see Avro filenames where the `subTaskId` field is mostly
`00000` (because the aggregator/committer writes them). The `subTaskId` field exists
in the format for the **dynamic-sink** code path and for testability, not because
multiple writers race on manifest creation.

Source: `ManifestOutputFileFactory.java`, `FlinkManifestUtil.java`,
`IcebergWriteAggregator.java:79-89`, `IcebergFilesCommitter.java:161-165`

---

# Part 5: End-to-End Exactly-Once Semantics

## Yes, the Connector Supports Exactly-Once

The exactly-once guarantee is achieved through Flink's checkpoint-based **two-phase
commit** protocol combined with Iceberg's **atomic snapshot commits**.

```
┌──────────────────────────────────────────────────────────────────────┐
│  TWO-PHASE COMMIT PROTOCOL                                           │
│                                                                       │
│  PHASE 1: PREPARE (on checkpoint barrier)                             │
│  ════════════════════════════════════════                              │
│                                                                       │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐                  │
│  │ Writer-0   │    │ Writer-1   │    │ Writer-2   │                  │
│  │            │    │            │    │            │                  │
│  │ flush()    │    │ flush()    │    │ flush()    │                  │
│  │ close open │    │ close open │    │ close open │                  │
│  │ data files │    │ data files │    │ data files │                  │
│  │            │    │            │    │            │                  │
│  │ complete() │    │ complete() │    │ complete() │                  │
│  │ → Write-   │    │ → Write-   │    │ → Write-   │                  │
│  │   Result   │    │   Result   │    │   Result   │                  │
│  └─────┬──────┘    └─────┬──────┘    └─────┬──────┘                  │
│        │                 │                 │                          │
│        │  At this point: data files exist on S3                      │
│        │  but are NOT in any Iceberg snapshot.                        │
│        │  They are "orphan" files until committed.                    │
│        ▼                 ▼                 ▼                          │
│  ┌─────────────────────────────────────────────┐                     │
│  │  Committer (parallelism=1)                   │                     │
│  │  Stores DeltaManifests in operator state     │                     │
│  │  → checkpointed by Flink                     │                     │
│  └─────────────────────────────────────────────┘                     │
│                                                                       │
│  PHASE 2: COMMIT (on notifyCheckpointComplete)                        │
│  ═════════════════════════════════════════════                         │
│                                                                       │
│  ┌─────────────────────────────────────────────┐                     │
│  │  Committer receives checkpoint-complete:      │                     │
│  │                                               │                     │
│  │  1. Read DeltaManifests for this checkpoint   │                     │
│  │  2. Extract DataFile[] and DeleteFile[]       │                     │
│  │  3. Create AppendFiles or RowDelta            │                     │
│  │  4. Set snapshot properties:                  │                     │
│  │     flink.max-committed-checkpoint-id = 42    │                     │
│  │     flink.job-id = abc-def                    │                     │
│  │  5. operation.commit()                        │                     │
│  │     → ATOMIC: new Iceberg snapshot visible    │                     │
│  │                                               │                     │
│  │  Now: data files are part of the table.       │                     │
│  │  Readers see all records from this checkpoint │                     │
│  │  at once (snapshot isolation).                │                     │
│  └─────────────────────────────────────────────┘                     │
└──────────────────────────────────────────────────────────────────────┘
```

## Why This Is Exactly-Once (Not At-Least-Once)

Three guarantees combine:

| Guarantee | Mechanism |
|-----------|-----------|
| **No data loss** | Flink checkpoints Kafka offsets. On failure, replay from checkpoint. |
| **No duplicate files** | Checkpoint ID in snapshot properties prevents re-committing same data. |
| **Atomic visibility** | Iceberg snapshot commit is all-or-nothing. Partial writes never visible. |

Source: `IcebergCommitter.java`, `IcebergSinkWriter.java`, `SinkWriterOperator.java`

---

# Part 6: Idempotent Recovery

## Three-Tier Deduplication

When a Flink job restarts from checkpoint, the committer may see WriteResults that were
already committed. The connector uses three tiers to prevent duplicate commits:

```
┌──────────────────────────────────────────────────────────────────────┐
│  THREE-TIER DEDUPLICATION ON RECOVERY                                 │
│                                                                       │
│  TIER 1: MANIFEST STATE IN CHECKPOINT                                │
│  ─────────────────────────────────────                                │
│  Flink operator state stores:                                         │
│    Map<Long, byte[]> dataFilesPerCheckpoint                          │
│      key: checkpointId                                               │
│      value: serialized DeltaManifests                                │
│                                                                       │
│  On restore from checkpoint:                                          │
│    → pending (uncommitted) manifests are replayed                    │
│    → already-committed ones have been removed from the map           │
│                                                                       │
│  TIER 2: SNAPSHOT HISTORY QUERY                                       │
│  ──────────────────────────────                                       │
│  On initializeState():                                                │
│    maxCommittedCheckpointId =                                        │
│      SinkUtil.getMaxCommittedCheckpointId(                           │
│        table, restoredFlinkJobId, operatorId, branch)                │
│                                                                       │
│  Walks Iceberg snapshot chain backward:                               │
│    Finds snapshot with matching job-id + operator-id                 │
│    Extracts flink.max-committed-checkpoint-id                        │
│    → Now knows exactly which checkpoints were already committed      │
│                                                                       │
│  TIER 3: CONDITIONAL COMMIT                                          │
│  ──────────────────────────                                           │
│  commitUpToCheckpoint(checkpointId):                                  │
│    pendingMap = dataFilesPerCheckpoint.headMap(checkpointId, true)    │
│    for each entry where key > maxCommittedCheckpointId:              │
│      commit to Iceberg                                               │
│    for each entry where key ≤ maxCommittedCheckpointId:             │
│      SKIP (already committed)                                        │
│                                                                       │
│  In SinkV2: IcebergCommitter calls                                   │
│    commitRequest.signalAlreadyCommitted()                            │
│    for checkpoints that are already in the snapshot chain.            │
└──────────────────────────────────────────────────────────────────────┘
```

### Example Recovery Scenario

```
Before crash:
  Checkpoint 85: committed → snapshot #985
  Checkpoint 86: committed → snapshot #986
  Checkpoint 87: files written, manifests in state, NOT committed
  Checkpoint 88: files partially written
                                      ↑ CRASH HERE

On restart from checkpoint 87:
  Tier 1: State restored with manifests for checkpoint 87
  Tier 2: maxCommittedCheckpointId = 86 (from snapshot #986)
  Tier 3: Checkpoint 87 > 86 → COMMIT IT
          Any restored checkpoint ≤ 86 → SKIP

Result: Checkpoint 87 committed exactly once.
```

### Two Subtleties That Bite in Production

**(a) RowDelta is committed per-checkpoint, NOT merged.** When the table uses equality
deletes (V2 format / upsert), `IcebergCommitter.commitDeltaTxn()` walks the pending
results in checkpoint order and calls `commitOperation()` **once per checkpoint**:

```java
for (Map.Entry<Long, WriteResult> e : pendingResults.entrySet()) {
    RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
    commitOperation(rowDelta, "rowDelta", newFlinkJobId, operatorId, e.getKey());
}
```

The comment in `IcebergCommitter.java:256-269` explains why:

> Equality-delete files of txn2 are required to be applied to data files from txn1.
> Committing the merged one will lead to the incorrect delete semantic.

So on recovery, if 3 pending checkpoints (87, 88, 89) need committing in upsert mode,
you get **three separate Iceberg snapshots**, each with its own
`flink.max-committed-checkpoint-id`. A reader at any point sees a consistent prefix.
Append-only mode (V1 / no deletes), in contrast, *does* merge all pending checkpoints
into a single `AppendFiles` commit and a single snapshot.

**(b) Empty checkpoints still commit periodically.** If a checkpoint produces zero
data files and zero delete files, the committer skips the commit — but only up to
`flink.max-continuous-empty-commits` (default `10`) in a row. After that, it commits an
**empty snapshot** just to advance `flink.max-committed-checkpoint-id`:

```java
continuousEmptyCheckpoints = totalFiles == 0 ? continuousEmptyCheckpoints + 1 : 0;
if (totalFiles != 0 || continuousEmptyCheckpoints % maxContinuousEmptyCommits == 0) {
    // commit
}
```

Without this, an idle stream (no input data) would never advance the watermark stored
on Iceberg, and on recovery the committer would have nothing to compare against — it
would always replay from the *last* non-empty snapshot's checkpoint id. That replay
itself is still idempotent thanks to the snapshot summary check, but the periodic empty
commit keeps the watermark close to the actual job position.

Source: `IcebergCommitter.java` (commit, signalAlreadyCommitted, commitPendingResult,
commitDeltaTxn), `SinkUtil.java` (getMaxCommittedCheckpointId)

---

# Part 7: Concurrent Update Handling

## Iceberg's Optimistic Concurrency

Multiple writers (different Flink jobs, Spark jobs, or even multiple Flink sinks on the
same table) can write concurrently. Iceberg handles this with **optimistic concurrency**
at the metadata layer.

```
┌──────────────────────────────────────────────────────────────────────┐
│  CONCURRENT COMMITS — OPTIMISTIC CONCURRENCY                         │
│                                                                       │
│  Flink Job A                         Flink Job B                      │
│       │                                    │                          │
│  T0:  ├── Read metadata v5                 ├── Read metadata v5      │
│       │   (latest snapshot = #100)         │   (latest snapshot = #100)│
│       │                                    │                          │
│  T1:  ├── Write data files                 │                          │
│       │   file-A1.parquet                  ├── Write data files       │
│       │   file-A2.parquet                  │   file-B1.parquet        │
│       │                                    │                          │
│  T2:  ├── Create metadata v6              │                          │
│       │   snapshot #101 (adds A1, A2)      │                          │
│       │   based on v5                      │                          │
│       │                                    │                          │
│       ├── Commit: write v6 to catalog      │                          │
│       │   → SUCCESS ✓                      │                          │
│       │   (v5 → v6 atomic swap)            │                          │
│       │                                    │                          │
│  T3:  │                                    ├── Create metadata v6    │
│       │                                    │   snapshot #101 (adds B1)│
│       │                                    │   based on v5           │
│       │                                    │                          │
│       │                                    ├── Commit: write v6       │
│       │                                    │   → CONFLICT ✗          │
│       │                                    │   (v5 already replaced   │
│       │                                    │    by v6 from Job A)     │
│       │                                    │                          │
│  T4:  │                                    ├── RETRY automatically:  │
│       │                                    │   Re-read metadata v6   │
│       │                                    │   Create metadata v7    │
│       │                                    │   snapshot #102 (B1)    │
│       │                                    │   based on v6           │
│       │                                    │                          │
│       │                                    ├── Commit: write v7       │
│       │                                    │   → SUCCESS ✓           │
│       │                                    │                          │
│  Result: Both A and B's files are in the table.                       │
│  Snapshots: #100 → #101 (A's files) → #102 (B's files)              │
└──────────────────────────────────────────────────────────────────────┘
```

## How Retries Work

The retry logic lives in **Iceberg core** (not in the Flink connector):

| Aspect | Detail |
|--------|--------|
| Retry trigger | Metadata version mismatch (CAS failure) |
| Default retries | Up to 4 attempts |
| Backoff | Exponential with configurable min/max wait |
| Revalidation | On retry, re-read current metadata, re-apply changes on top |
| Non-retryable | `ValidationException` (e.g., conflicting delete on same rows) → task fails → Flink checkpoint recovery |

For **append-only** workloads, retries almost always succeed because appending files never
conflicts with other appends.

For **upsert/RowDelta** workloads, conflicts are possible if two writers produce equality
deletes for overlapping rows. In this case, `ValidationException` propagates to Flink,
the task fails, and Flink restarts from the last checkpoint.

Source: Iceberg core `SnapshotProducer.commit()`, `BaseTransaction.java`

---

# Part 8: Merge-on-Read (MOR) vs Copy-on-Write (COW)

## Both Are Supported

| Mode | When Used | Write Path | Read Cost |
|------|-----------|------------|-----------|
| **COW (append-only)** | Default, no upsert | Only DataFiles | Fast reads — no merge needed |
| **MOR (upsert)** | `upsert-enabled = true` | DataFiles + equality DeleteFiles | Slower reads — must resolve deletes |

## COW: Append-Only Mode

```
Input: INSERT records only

Writer produces:
  DataFile: 00001-abc.parquet (1000 rows)
  DataFile: 00002-def.parquet (1000 rows)

Commit: table.newAppend().appendFile(f1).appendFile(f2).commit()

Reader: scans data files directly. No merge overhead.
```

## MOR: Upsert Mode with Equality Deletes

When `upsert-enabled = true` and equality field columns are configured:

```
┌──────────────────────────────────────────────────────────────────────┐
│  UPSERT WRITE PATH (BaseDeltaTaskWriter)                              │
│                                                                       │
│  For each incoming record:                                            │
│                                                                       │
│  switch (row.getRowKind()) {                                          │
│                                                                       │
│    case INSERT:                                                       │
│    case UPDATE_AFTER:                                                 │
│      ┌──────────────────────────────────────┐                        │
│      │ 1. Write EQUALITY DELETE for the key  │                        │
│      │    (delete old row by key match)      │                        │
│      │                                       │                        │
│      │    delete file contains only key cols: │                        │
│      │    {user_id: 123}                     │                        │
│      │                                       │                        │
│      │ 2. Write DATA FILE with new row       │                        │
│      │    (insert the updated row)           │                        │
│      │                                       │                        │
│      │    data file contains full row:       │                        │
│      │    {user_id: 123, name: "Alice", ...} │                        │
│      └──────────────────────────────────────┘                        │
│                                                                       │
│    case UPDATE_BEFORE:                                                │
│      → IGNORED in upsert mode                                        │
│        (the equality delete handles it)                               │
│                                                                       │
│    case DELETE:                                                       │
│      → Write EQUALITY DELETE for the key                             │
│        {user_id: 123}                                                │
│  }                                                                    │
│                                                                       │
│  Commit: table.newRowDelta()                                          │
│            .addRows(dataFiles)                                        │
│            .addDeletes(deleteFiles)                                   │
│            .commit()                                                  │
│                                                                       │
│  At read time: reader merges data files with delete files.           │
│  Rows matching equality delete keys are filtered out,                │
│  leaving only the latest version of each row.                         │
└──────────────────────────────────────────────────────────────────────┘
```

### Trade-offs

```
┌──────────────────────────────────────────────────────────────────────┐
│  COW vs MOR TRADE-OFFS                                                │
│                                                                       │
│  Write Latency:                                                       │
│    COW: ████████████████████████████████  (must rewrite partitions)  │
│    MOR: ████████  (only append new + delete files)                   │
│                                                                       │
│  Read Latency:                                                        │
│    COW: ████████  (direct scan, no merge)                            │
│    MOR: ████████████████████████████████  (must resolve deletes)    │
│                                                                       │
│  Storage:                                                             │
│    COW: Lower (old data replaced in-place via compaction)            │
│    MOR: Higher (delete files accumulate until compaction)            │
│                                                                       │
│  Streaming:                                                           │
│    COW: Not practical (full partition rewrite on each checkpoint)    │
│    MOR: Ideal (lightweight appends of data + delete files)           │
│                                                                       │
│  Compaction cleans up MOR overhead:                                   │
│    PostCommitTopology → RewriteDataFiles merges data + deletes       │
│    into clean data files (effectively converting MOR → COW)          │
└──────────────────────────────────────────────────────────────────────┘
```

### Partition Constraint

When using upsert with a partitioned table, partition columns **must be a subset** of
the equality field columns. Otherwise, a row could move between partitions on update,
and the equality delete in partition A wouldn't match the old row in partition B.

Source: `BaseDeltaTaskWriter.java`, `RowDataTaskWriterFactory.java`,
`IcebergSink.java`

---

# Part 9: v1.20 vs v2.1 Differences

## Core Sink Logic: Identical

The fundamental architecture — writers, committers, exactly-once protocol, MOR/COW
support — is the same between v1.20 and v2.1. The differences are in how Flink APIs
are used and new features added around the edges.

## Key Differences

| Aspect | v1.20 (Flink 1.20) | v2.1 (Flink 2.1) |
|--------|-------------------|------------------|
| **Committer wiring** | Direct instantiation: `new IcebergFilesCommitter(...)` | Factory pattern: `IcebergFilesCommitterFactory` wraps committer |
| **Writer init API** | Deprecated `InitContext` (with `@SuppressWarnings`) | Modern `WriterInitContext` (no deprecation) |
| **Chaining strategy** | Set in `IcebergStreamWriter` constructor: `setChainingStrategy(ALWAYS)` | Managed by factory's `getChainingStrategy()` |
| **Operator lifecycle** | Direct field assignment in constructor | Uses `StreamOperatorParameters` for proper lifecycle init |
| **Variant type** | Not supported | `Flink: Support Variant to Flink 2.1 (#15265)` |
| **Coordinator Lock SQL** | Not available | `Flink: Add Coordinator Lock when using Flink SQL (#15459)` |
| **Dynamic Sink SQL** | Backported | Native: `Flink: SQL support for dynamic iceberg sink (#15279)` |
| **FormatModel API** | Backported | Native: `Core, Data, Flink: Moving Flink to use the new FormatModel API (#15329)` |
| **TableMaintenance Lock** | Backported | Native: `Flink: TableMaintenance Support Coordinator Lock (#15151)` |

## What's New in v2.1 (Not Backported to v1.20)

```
┌──────────────────────────────────────────────────────────────────────┐
│  v2.1-ONLY FEATURES                                                   │
│                                                                       │
│  1. VARIANT TYPE SUPPORT                                              │
│     Flink can read/write Iceberg Variant columns (semi-structured    │
│     data stored efficiently in columnar format).                      │
│     Commit: #15265                                                    │
│                                                                       │
│  2. OPERATOR FACTORY PATTERN                                          │
│     IcebergFilesCommitterFactory — cleaner integration with           │
│     Flink's operator initialization lifecycle.                        │
│     StreamOperatorParameters-based construction.                      │
│                                                                       │
│  3. MODERN SINK V2 API (no deprecation warnings)                     │
│     WriterInitContext, CommitterInitContext — proper typed init        │
│     without @SuppressWarnings("deprecation").                         │
│                                                                       │
│  Most v2.1 features are BACKPORTED to v1.20 (see git log for         │
│  "Backport" commits). The feature gap is small.                       │
└──────────────────────────────────────────────────────────────────────┘
```

Source: Git history `myiceberg/flink/v2.1/` vs `myiceberg/flink/v1.20/`,
`IcebergFilesCommitterFactory.java` (v2.1 only)

---

# Part 10: Configuration Reference

## Key Write Options

| Option | Default | Description |
|--------|---------|-------------|
| `write-format` | `parquet` | File format: `parquet`, `orc`, `avro` |
| `target-file-size-bytes` | `128MB` | Target size for each data file |
| `upsert-enabled` | `false` | Enable upsert with equality deletes (MOR) |
| `overwrite-enabled` | `false` | Enable `INSERT OVERWRITE` mode |
| `distribution-mode` | `none` | Write distribution: `none`, `hash`, `range` |
| `sink.parallelism` | — | Override write parallelism |

## Exactly-Once Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `sink.delivery-guarantee` | `exactly-once` | `exactly-once`, `at-least-once`, `none` |

## Table Properties for Writes

| Property | Default | Description |
|----------|---------|-------------|
| `format-version` | `1` | Must be `2` for equality deletes (MOR/upsert) |
| `write.metadata.delete-after-commit.enabled` | `false` | Auto-clean old metadata files |
| `commit.retry.num-retries` | `4` | Retries on concurrent commit conflicts |

---

# Part 11: Key Classes Reference

## IcebergSink (Sink V2) Classes

| Class | Role |
|-------|------|
| `IcebergSink` | Entry point — implements SupportsPreWriteTopology, SupportsCommitter, SupportsPreCommitTopology, SupportsPostCommitTopology |
| `IcebergSinkWriter` | Writer — implements CommittingSinkWriter, wraps TaskWriter, emits WriteResult on prepareCommit |
| `IcebergCommitter` | Committer — implements Committer\<IcebergCommittable\>, commits to Iceberg, handles signalAlreadyCommitted |
| `IcebergWriteAggregator` | Pre-commit topology — aggregates N WriteResults into 1 IcebergCommittable with DeltaManifests |
| `IcebergCommittable` | Immutable commit message: DeltaManifests + jobId + operatorId + checkpointId |

## Shared Writer Classes (used by both Sink V2 and Legacy)

| Class | Role |
|-------|------|
| `RowDataTaskWriterFactory` | Creates PartitionedDeltaWriter or UnpartitionedDeltaWriter based on table spec |
| `BaseDeltaTaskWriter` | Abstract base — routes records by RowKind (INSERT/UPDATE/DELETE) to data or delete writers |
| `PartitionedDeltaWriter` | One RowDataDeltaWriter per partition |
| `UnpartitionedDeltaWriter` | Single RowDataDeltaWriter for unpartitioned tables |
| `FlinkFileWriterFactory` | Delegates to Iceberg's file format writers (Parquet/ORC/Avro) |
| `FlinkManifestUtil` | Serializes DataFile[]/DeleteFile[] to manifest files on storage |
| `DeltaManifests` | Wrapper: data manifest + delete manifest + referenced data files |
| `SinkUtil` | Helpers: getMaxCommittedCheckpointId(), snapshot property constants |

## Flink Sink V2 Framework (`flink-core/.../api/connector/sink2/`)

| Class | Role |
|-------|------|
| `Sink` | Base interface — creates SinkWriter |
| `SinkWriter` | Writes records, flushes on checkpoint |
| `CommittingSinkWriter` | Extends SinkWriter — prepareCommit() returns committables |
| `Committer` | Commits committables atomically, must be idempotent |
| `SupportsPreWriteTopology` | Add custom topology before writers (e.g., range partitioning) |
| `SupportsPreCommitTopology` | Add custom topology between writers and committer (e.g., aggregation) |
| `SupportsPostCommitTopology` | Add custom topology after committer (e.g., compaction) |

## v2.1-Only Classes

| Class | Role |
|-------|------|
| `IcebergFilesCommitterFactory` | Factory wrapping IcebergFilesCommitter with StreamOperatorParameters lifecycle |

---

# Appendix A: Legacy Sink (FlinkSink)

The legacy `FlinkSink` uses custom Flink operators instead of the Sink V2 framework.
It shares the same TaskWriter and commit logic but wires them differently.

```
┌──────────────────────────────────────────────────────────────────────┐
│                     LEGACY SINK (FlinkSink)                           │
│                                                                       │
│  Input DataStream<RowData>                                            │
│       │                                                               │
│       │  keyBy(partitionKey or equalityKey)                           │
│       ▼                                                               │
│  ┌──────────────────────────────────────────────────────┐            │
│  │  IcebergStreamWriter  (parallelism = N)               │            │
│  │  extends AbstractStreamOperator                       │            │
│  │                                                       │            │
│  │  For each record:                                     │            │
│  │    TaskWriter.write(row)                              │            │
│  │      → writes to Parquet/ORC/Avro files on S3/HDFS   │            │
│  │                                                       │            │
│  │  On checkpoint barrier (prepareSnapshotPreBarrier):   │            │
│  │    writer.complete() → WriteResult                    │            │
│  │    emit FlinkWriteResult(checkpointId, WriteResult)   │            │
│  └──────────────────────┬───────────────────────────────┘            │
│                          │                                            │
│                          │  FlinkWriteResult (data files + deletes)   │
│                          ▼                                            │
│  ┌──────────────────────────────────────────────────────┐            │
│  │  IcebergFilesCommitter  (parallelism = 1)             │            │
│  │  extends AbstractStreamOperator                       │            │
│  │                                                       │            │
│  │  Collects: Map<checkpointId, DeltaManifests>         │            │
│  │  State: ListState<SortedMap<Long, byte[]>>           │            │
│  │                                                       │            │
│  │  On notifyCheckpointComplete(id):                     │            │
│  │    for each checkpoint ≤ id:                          │
│  │      read manifest files                              │            │
│  │      table.newAppend() or table.newRowDelta()         │            │
│  │      set snapshot properties:                         │            │
│  │        flink.max-committed-checkpoint-id = id         │            │
│  │        flink.job-id = <uuid>                          │            │
│  │      operation.commit()  → new Iceberg snapshot       │            │
│  └──────────────────────────────────────────────────────┘            │
└──────────────────────────────────────────────────────────────────────┘
```

## Flink Job ID and Iceberg Sink

### How Flink assigns Job IDs

By default, Flink auto-generates a random 128-bit UUID as the job ID each time a job is submitted. To set a **deterministic/fixed job ID** (Flink 1.17+):

```java
// Programmatically
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.getConfiguration().setString("pipeline.job-id", "550e8400-e29b-41d4-a716-446655440000");
```

Or in `flink-conf.yaml` / `config.yaml`:
```yaml
pipeline.job-id: 550e8400-e29b-41d4-a716-446655440000
```

Must be a valid UUID format.

### Does a different job ID get rejected?

**No — it is not rejected.** But the consequences depend on the scenario:

| Scenario | What Iceberg does | Risk |
|----------|-------------------|------|
| New job ID, no prior state | Scans snapshot history, finds no match → `maxCommittedCheckpointId = -1` → commits all checkpoints from scratch | Safe |
| New job ID, restored from savepoint | Same as above: treats it as a brand-new writer | **Duplicate data risk** — old checkpoints pending in savepoint state get re-committed as if never written |
| Same job ID restored from savepoint | Finds matching `flink.job-id` in snapshot history → skips already-committed checkpoints | Exactly-once, safe |

The key logic in `SinkUtil.getMaxCommittedCheckpointId`:

```
Walk snapshot chain backward:
  Find snapshot where flink.job-id == currentJobId AND flink.operator-id == currentOperatorId
  → return flink.max-committed-checkpoint-id

If no matching snapshot found → return -1 (commit everything)
```

### Practical recommendation

- **For production savepoint-based recovery**: pin `pipeline.job-id` to a fixed UUID so Iceberg can find prior commits and skip duplicates.
- **For a clean reprocessing run** (intentional re-ingest from scratch): use a new job ID deliberately — Iceberg will write fresh, but ensure the target table/branch is clean or you accept duplicates.

## Legacy vs Sink V2 Comparison

| Aspect | Legacy (FlinkSink) | Sink V2 (IcebergSink) |
|--------|-------------------|----------------------|
| Writer | `IcebergStreamWriter` (custom operator) | `IcebergSinkWriter` (CommittingSinkWriter) |
| Committer | `IcebergFilesCommitter` (custom operator) | `IcebergCommitter` (Committer interface) |
| Aggregation | Done inside IcebergFilesCommitter | Separate `IcebergWriteAggregator` (PreCommitTopology) |
| Pre-write | Manual keyBy | `SupportsPreWriteTopology` (range partitioning) |
| Post-commit | Not supported | `SupportsPostCommitTopology` (compaction, snapshot expiry) |
| Idempotency | Checks snapshot properties in `initializeState()` | Uses `signalAlreadyCommitted()` on CommitRequest |
| Entry point | `FlinkSink.forRowData(input).table(t).append()` | `IcebergSink.builder().table(t).build()` |

## Legacy Key Classes

| Class | Role |
|-------|------|
| `FlinkSink` | Entry point — builds operator pipeline |
| `IcebergStreamWriter` | Writer operator — wraps TaskWriter, emits FlinkWriteResult |
| `IcebergFilesCommitter` | Committer operator — collects manifests, commits on checkpoint complete |
| `FlinkWriteResult` | Wrapper: checkpointId + WriteResult |
