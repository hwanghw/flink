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

# Part 4: How Kafka Offsets Map to Iceberg Metadata

## The Key Insight: Checkpoint ID, Not Kafka Offset

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

## Snapshot Properties Set on Every Commit

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

## Why Store These in Iceberg? Isn't the Flink Checkpoint Enough?

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

## Recovery: Finding the Last Committed Checkpoint

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

Source: `IcebergCommitter.java` (commit, signalAlreadyCommitted),
`SinkUtil.java` (getMaxCommittedCheckpointId)

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
