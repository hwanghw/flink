# Flink Iceberg Source Connector Deep Dive

How the Flink Iceberg source connector discovers new snapshots, hands work to parallel
readers, tracks what's already been processed across crashes, applies equality and
position deletes at read time, and why it does (and doesn't) propagate row-level
updates — with source-code references from both v1.20 and v2.1.

Based on the
[Iceberg Flink Queries docs](https://iceberg.apache.org/docs/latest/flink-queries/),
the source code in `myiceberg/flink/v1.20` and `myiceberg/flink/v2.1`, and Flink's
[FLIP-27 Source V2 API](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27:+Refactor+Source+Interface).

---

# Part 1: Overview

## What the Connector Does

The Flink Iceberg source connector reads data from Iceberg tables on S3, HDFS, or any
Hadoop-compatible filesystem, in two modes:

- **Batch** — read a single snapshot (or a snapshot range) as a bounded stream.
- **Streaming** — continuously poll the Iceberg catalog for new append snapshots, emit
  newly-added files as splits, and let parallel readers consume them.

This document focuses on the **`IcebergSource` (FLIP-27 Source V2)** implementation.
The legacy `FlinkSource` / `StreamingMonitorFunction` (custom Flink operators) is
covered in Appendix A.

## Key properties

- **Snapshot-isolated reads** — a batch read sees a consistent point-in-time view of
  the table.
- **Incremental streaming via `IncrementalAppendScan`** — streaming polls for new
  snapshots and reads only the data files added since the last enumerated position.
- **Position-checkpointed replay** — both the *enumerator* (table-level cursor:
  snapshotId) and individual *splits* (file-level cursor: `fileOffset + recordOffset`)
  are checkpointed, so on crash readers resume exactly where they left off.
- **Merge-on-read at read time** — `FlinkDeleteFilter` applies equality and position
  deletes per data file when reading, so MOR tables produce correct row sets without
  extra work from the user.
- **Streaming reads see only APPEND snapshots** — a fundamental limitation: snapshots
  produced by upsert/RowDelta writers (operation = `overwrite`) are silently **skipped**
  by `IncrementalAppendScan`. See Part 9.

---

# Part 2: Architecture

The source connector is split across two JVMs by FLIP-27 design:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                  ICEBERG SOURCE (FLIP-27) PIPELINE                       │
│                                                                          │
│  JobManager JVM                                                          │
│  ────────────────                                                        │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │  SplitEnumerator (parallelism = 1, runs in coordinator)       │       │
│  │                                                                │       │
│  │  Continuous mode → ContinuousIcebergEnumerator                 │       │
│  │      uses ContinuousSplitPlannerImpl                           │       │
│  │      polls every monitorInterval (default 10s)                 │       │
│  │      → table.refresh() → table.currentSnapshot()               │       │
│  │      → IncrementalAppendScan.fromSnapshotExclusive(last)       │       │
│  │                              .toSnapshot(new) .planTasks()     │       │
│  │      → emits IcebergSourceSplits to SplitAssigner              │       │
│  │                                                                │       │
│  │  Batch mode → StaticIcebergEnumerator                          │       │
│  │      one-shot plan at job start, no polling                    │       │
│  │                                                                │       │
│  │  SplitAssigner (DefaultSplitAssigner / OrderedSplitAssigner)   │       │
│  │      queue of pending splits, hands one out per reader request │       │
│  │                                                                │       │
│  │  EnumerationHistory (3-slot circular buffer)                   │       │
│  │      pauses discovery when pending > recent total discovered    │       │
│  └────────────────────────┬─────────────────────────────────────┘       │
│                            │ SourceEvents:                                │
│                            │   reader → SplitRequestEvent                 │
│                            │   coordinator → assignSplit(split, subtask)  │
│                            ▼                                              │
│                                                                          │
│  TaskManager JVMs (parallelism = N)                                      │
│  ──────────────────────────────────                                      │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │  IcebergSourceReader (one per subtask)                        │       │
│  │      delegates to IcebergSourceSplitReader                    │       │
│  │      requests a new split when current one finishes           │       │
│  │                                                                │       │
│  │  IcebergSourceSplitReader                                      │       │
│  │      pulls split from local queue, opens via ReaderFunction    │       │
│  │      returns RecordsWithSplitIds batches                       │       │
│  │                                                                │       │
│  │  ReaderFunction (RowDataReaderFunction by default)             │       │
│  │      builds DataIterator over CombinedScanTask                 │       │
│  │      DataIterator wraps RowDataFileScanTaskReader             │       │
│  │        which wraps FormatModelRegistry reader (Parquet/ORC)   │       │
│  │        with FlinkDeleteFilter (equality + position deletes)   │       │
│  │      Position-aware: seek(fileOffset, recordOffset) on restore │       │
│  │                                                                │       │
│  │  Output: DataStream<RowData>                                   │       │
│  └──────────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────┘
```

Source: `IcebergSource.java`, `ContinuousIcebergEnumerator.java`,
`ContinuousSplitPlannerImpl.java`, `AbstractIcebergEnumerator.java`,
`IcebergSourceReader.java`, `IcebergSourceSplitReader.java`,
`RowDataReaderFunction.java`, `RowDataFileScanTaskReader.java`, `DataIterator.java`

## The Two Roles

| Role | Where it runs | What it does | Parallelism |
|---|---|---|---|
| **Enumerator** | JobManager (coordinator thread) | Polls table for new snapshots, plans splits, distributes them to readers | Always 1 |
| **Reader** | TaskManager subtasks | Receives split assignments, opens files via Iceberg readers, deserializes records, emits to downstream | Configurable, often N |

The clean separation is the whole point of FLIP-27: **work discovery on the coordinator,
work execution on the workers.** This lets a single source author code that runs
either batch or streaming, exactly-once or at-least-once, with localized state
management.

---

# Part 3: The Split Model

The unit of work between enumerator and reader is an **`IcebergSourceSplit`**
(`split/IcebergSourceSplit.java:42-94`):

```java
public class IcebergSourceSplit implements SourceSplit, Serializable {
  private final CombinedScanTask task;   // group of FileScanTasks bin-packed by Iceberg
  private int fileOffset;                // index into task.files() — for replay
  private long recordOffset;             // row count within the current file — for replay

  public static IcebergSourceSplit fromCombinedScanTask(CombinedScanTask combinedScanTask) {
    return fromCombinedScanTask(combinedScanTask, 0, 0L);
  }
  ...
}
```

A `CombinedScanTask` in Iceberg core is a bin-packed group of `FileScanTask`s — Iceberg's
split planner combines small data files together so that one Flink subtask reads
multiple files until it hits the configured `read.split.target-size` (default 128MB).

### Why the `fileOffset` + `recordOffset` Pair?

These two integers are the **per-split replay cursor**. They are updated by
`DataIterator` as it reads records and serialized into the Flink checkpoint along
with the split's other metadata. On restart, `DataIterator.seek(fileOffset, recordOffset)`
fast-forwards through the assigned files and records that were already emitted,
so the reader resumes exactly at the next un-emitted row. From `DataIterator.java:77-109`:

```java
public void seek(int startingFileOffset, long startingRecordOffset) {
  Preconditions.checkState(fileOffset == -1, "Seek should be called before any other iterator actions");
  // skip files
  for (long i = 0L; i < startingFileOffset; ++i) {
    tasks.next();
  }
  updateCurrentIterator();
  // skip records within the file
  for (long i = 0; i < startingRecordOffset; ++i) {
    if (currentFileHasNext() && hasNext()) {
      next();
    } else {
      throw new IllegalStateException(...);
    }
  }
  fileOffset = startingFileOffset;
  recordOffset = startingRecordOffset;
}
```

Note: `seek` is **linear** — to resume at record 1,000,000 of file 3, the reader
must `next()` through one million records. For wide tables this can be slow on
restart. Iceberg's columnar formats don't expose a cheap "skip to row N" API, so the
seek is a sequential scan that discards rows until it reaches the offset.

---

# Part 4: New File Detection — How the Enumerator Finds New Snapshots

This is the **heart of streaming.** The enumerator wakes up periodically, asks the
catalog "what's the latest snapshot?", and plans new splits for anything that landed
since the last cycle.

## The Polling Loop

`ContinuousIcebergEnumerator.start()` registers a periodic async callback
(`ContinuousIcebergEnumerator.java:91-99`):

```java
@Override
public void start() {
  super.start();
  enumeratorContext.callAsync(
      this::discoverSplits,                  // runs in IO thread pool
      this::processDiscoveredSplits,         // runs back in coordinator thread
      0L,                                    // initial delay
      scanContext.monitorInterval().toMillis());   // period (default 10s)
}
```

`callAsync` is Flink's split-coordinator helper: `discoverSplits` runs on an I/O thread
(so a slow catalog call doesn't block the coordinator), and `processDiscoveredSplits`
runs back on the coordinator thread (so split assignment and state mutation stay
single-threaded).

## What `discoverSplits()` Does

```java
// ContinuousIcebergEnumerator.java:118-133
private ContinuousEnumerationResult discoverSplits() {
  int pendingSplitCountFromAssigner = assigner.pendingSplitCount();
  if (enumerationHistory.shouldPauseSplitDiscovery(pendingSplitCountFromAssigner)) {
    // If the assigner already has many pending splits, it is better to pause split discovery.
    LOG.info("Pause split discovery as the assigner already has too many pending splits: {}",
             pendingSplitCountFromAssigner);
    return new ContinuousEnumerationResult(
        Collections.emptyList(), enumeratorPosition.get(), enumeratorPosition.get());
  } else {
    return splitPlanner.planSplits(enumeratorPosition.get());
  }
}
```

Two paths:

1. **Throttled** — if the assigner's queue is already very full, return an empty
   result without touching the table. See Part 6 for the throttling logic.
2. **Plan** — delegate to `splitPlanner.planSplits(lastPosition)` to do the real work.

## The Incremental Planning Step

`ContinuousSplitPlannerImpl.planSplits` is where Iceberg-core meets Flink
(`ContinuousSplitPlannerImpl.java:80-87`):

```java
@Override
public ContinuousEnumerationResult planSplits(IcebergEnumeratorPosition lastPosition) {
  table.refresh();                          // catalog round-trip — pulls latest metadata
  if (lastPosition != null) {
    return discoverIncrementalSplits(lastPosition);
  } else {
    return discoverInitialSplits();         // first call after job start
  }
}
```

The `table.refresh()` is the **catalog round-trip**. For a HiveCatalog or GlueCatalog,
this is the call that fetches the latest `metadata-v<N>.json` pointer and loads it.
For a HadoopCatalog, it reads `version-hint.text`. The cost of this call dominates
each polling cycle on slow catalogs.

`discoverIncrementalSplits` then compares the current snapshot to the last position
and plans only what's new (`ContinuousSplitPlannerImpl.java:105-144`):

```java
private ContinuousEnumerationResult discoverIncrementalSplits(IcebergEnumeratorPosition lastPosition) {
  Snapshot currentSnapshot = scanContext.branch() != null
      ? table.snapshot(scanContext.branch())
      : table.currentSnapshot();

  if (currentSnapshot == null) {
    // empty table
    return new ContinuousEnumerationResult(Collections.emptyList(), lastPosition, lastPosition);
  } else if (lastPosition.snapshotId() != null
             && currentSnapshot.snapshotId() == lastPosition.snapshotId()) {
    // no new commits since last poll
    LOG.info("Current table snapshot is already enumerated: {}", currentSnapshot.snapshotId());
    return new ContinuousEnumerationResult(Collections.emptyList(), lastPosition, lastPosition);
  } else {
    Long lastConsumedSnapshotId = lastPosition.snapshotId();
    Snapshot toSnapshotInclusive = toSnapshotInclusive(
        lastConsumedSnapshotId, currentSnapshot, scanContext.maxPlanningSnapshotCount());
    IcebergEnumeratorPosition newPosition = IcebergEnumeratorPosition.of(
        toSnapshotInclusive.snapshotId(), toSnapshotInclusive.timestampMillis());
    ScanContext incrementalScan = scanContext.copyWithAppendsBetween(
        lastPosition.snapshotId(), toSnapshotInclusive.snapshotId());
    List<IcebergSourceSplit> splits = FlinkSplitPlanner.planIcebergSourceSplits(
        table, incrementalScan, workerPool);
    LOG.info("Discovered {} splits from incremental scan: from {} to {}",
             splits.size(), lastPosition, newPosition);
    return new ContinuousEnumerationResult(splits, lastPosition, newPosition);
  }
}
```

Three cases handled:

| Case | What the enumerator does |
|---|---|
| Table is empty | Return no splits; keep position |
| Current snapshot id == last enumerated snapshot id | No new commits; return no splits; keep position |
| Current snapshot id is newer | Plan incremental scan from `lastSnapshotId` (exclusive) to `currentSnapshotId` (inclusive); return splits + advance position |

## The Iceberg-Core Scan: `IncrementalAppendScan`

`FlinkSplitPlanner.planIcebergSourceSplits` ultimately calls
`table.newIncrementalAppendScan().fromSnapshotExclusive(...).toSnapshot(...)`
(`FlinkSplitPlanner.java:84-138`):

```java
static CloseableIterable<CombinedScanTask> planTasks(Table table, ScanContext context, ExecutorService workerPool) {
  ScanMode scanMode = checkScanMode(context);
  if (scanMode == ScanMode.INCREMENTAL_APPEND_SCAN) {
    IncrementalAppendScan scan = table.newIncrementalAppendScan();
    scan = refineScanWithBaseConfigs(scan, context, workerPool);
    if (context.startSnapshotId() != null) {
      scan = scan.fromSnapshotExclusive(context.startSnapshotId());
    }
    if (context.endSnapshotId() != null) {
      scan = scan.toSnapshot(context.endSnapshotId());
    }
    return scan.planTasks();
  } else {
    TableScan scan = table.newScan();
    ...
    return scan.planTasks();
  }
}
```

**`IncrementalAppendScan` is a load-bearing semantic boundary.** It is implemented
in Iceberg core (`org.apache.iceberg.BaseIncrementalAppendScan`) to walk the snapshot
chain between two points and **return only data files added by snapshots whose
`operation` is `append`.** Snapshots whose operation is `overwrite`, `delete`,
`replace`, or anything else are silently filtered out. See Part 9 for what this
means for users with upstream upsert writers.

## Splits Returned, Position Advanced

Back in the enumerator (`ContinuousIcebergEnumerator.java:136-187`),
`processDiscoveredSplits` runs on the coordinator thread:

```java
private void processDiscoveredSplits(ContinuousEnumerationResult result, Throwable error) {
  if (error == null) {
    consecutiveFailures = 0;
    if (!Objects.equals(result.fromPosition(), enumeratorPosition.get())) {
      // Multiple discoverSplits() may have been triggered with the same starting snapshot.
      // Discovery result should only be accepted if the starting position matches the
      // current enumerator position (like compare-and-swap).
      LOG.info("Skip {} discovered splits because starting position doesn't match",
               result.splits().size());
    } else {
      if (!result.splits().isEmpty()) {
        assigner.onDiscoveredSplits(result.splits());      // hand to assigner
        enumerationHistory.add(result.splits().size());    // record for throttling
      }
      // update the enumerator position even if there is no split discovered
      enumeratorPosition.set(result.toPosition());
    }
  } else {
    consecutiveFailures++;
    if (scanContext.maxAllowedPlanningFailures() < 0
        || consecutiveFailures <= scanContext.maxAllowedPlanningFailures()) {
      LOG.error("Failed to discover new splits", error);
    } else {
      throw new RuntimeException("Failed to discover new splits", error);
    }
  }
}
```

Key invariants:

- **Compare-and-swap on `fromPosition`**: if two concurrent `discoverSplits()`
  callbacks both used the same starting position, only one's result is accepted;
  the other is dropped. Prevents double-discovery if monitor interval is too short.
- **Position advances even with no new splits**: keeps the `lastSnapshotTimestampMs`
  gauge accurate even on idle tables.
- **Failure tolerance**: `maxAllowedPlanningFailures` (default 3) — only fails the
  job after that many consecutive catalog failures, surviving transient outages.

---

# Part 5: Remembering What's Already Processed

The source tracks "what's been done" at **two layers**, each checkpointed separately
into Flink state:

| Layer | What it tracks | Type | Lives in |
|---|---|---|---|
| **Enumerator position** | Up to which Iceberg snapshot the enumerator has planned splits | `IcebergEnumeratorPosition(snapshotId, snapshotTimestampMs)` | Enumerator state on JM |
| **Per-split position** | Within an in-flight split, which file and which record the reader was emitting next | `(fileOffset, recordOffset)` inside `IcebergSourceSplit` | Reader's local state on TM, plus a copy in the enumerator's `pendingSplits` |

## Layer 1: The Enumerator Position

```java
// enumerator/IcebergEnumeratorPosition.java
class IcebergEnumeratorPosition {
  private final Long snapshotId;
  private final Long snapshotTimestampMs;   // mainly for info logging
  ...
}
```

Snapshot id alone would be sufficient for correctness; the timestamp is carried for
debugging visibility ("how far behind is the source now?").

The checkpointed enumerator state bundles position + pending splits + history:

```java
// enumerator/IcebergEnumeratorState.java
public class IcebergEnumeratorState implements Serializable {
  @Nullable private final IcebergEnumeratorPosition lastEnumeratedPosition;
  private final Collection<IcebergSourceSplitState> pendingSplits;
  private final int[] enumerationSplitCountHistory;     // for throttling
  ...
}
```

On every Flink checkpoint, `ContinuousIcebergEnumerator.snapshotState(checkpointId)`
returns this tuple (`ContinuousIcebergEnumerator.java:112-116`):

```java
@Override
public IcebergEnumeratorState snapshotState(long checkpointId) {
  return new IcebergEnumeratorState(
      enumeratorPosition.get(), assigner.state(), enumerationHistory.snapshot());
}
```

On restore, `IcebergSource.restoreEnumerator(...)` reconstructs the enumerator with
this state, and the polling loop resumes from `lastEnumeratedPosition.snapshotId()`.
Any snapshot committed during the downtime is automatically picked up on the first
post-restore poll.

## Layer 2: The Per-Split Position

When the reader runs out of records in the current file, `DataIterator` advances:

```java
// DataIterator.java:129-140
private void updateCurrentIterator() {
  try {
    while (!currentIterator.hasNext() && tasks.hasNext()) {
      currentIterator.close();
      currentIterator = openTaskIterator(tasks.next());
      fileOffset += 1;
      recordOffset = 0L;
    }
  } catch (IOException e) {
    throw new UncheckedIOException(e);
  }
}
```

And after every record is emitted:

```java
@Override
public T next() {
  updateCurrentIterator();
  recordOffset += 1;
  return currentIterator.next();
}
```

These offsets get written back into the split via `updatePosition` between
batches of records (in `ArrayPoolDataIteratorBatcher`). When Flink checkpoints, the
split with its updated `fileOffset` / `recordOffset` is written into the reader's
`SourceReader` state. On restart, the split is replayed via `DataIterator.seek(...)`
which fast-forwards to the recorded position.

## Why Both Layers?

Without the **per-split position**, after a crash the reader would re-read the
entire split from byte zero — potentially re-emitting hundreds of millions of rows
that the downstream operator already processed. With Flink's exactly-once contract,
re-emission means duplicates unless the downstream is also idempotent. The per-split
position eliminates this re-emission.

Without the **enumerator position**, on restart the enumerator would either:
(a) replan from the beginning of the table (massive re-discovery), or (b) plan
from `currentSnapshot()` (missing all commits that landed during the downtime).
The enumerator position pins the right starting point.

## Diagram

```
┌────────────────────────────────────────────────────────────────────┐
│  POSITION STATE — TWO LAYERS, CHECKPOINTED TOGETHER                 │
│                                                                     │
│  JobManager / enumerator state                                      │
│  ──────────────────────────────                                     │
│   IcebergEnumeratorState {                                          │
│     lastEnumeratedPosition: { snapshotId=#42, ts=... }              │
│     pendingSplits: [                                                │
│       splitA (status=UNASSIGNED, fileOffset=0, recordOffset=0),     │
│       splitB (status=UNASSIGNED, fileOffset=0, recordOffset=0),     │
│       ...                                                           │
│     ]                                                               │
│     enumerationSplitCountHistory: [12, 0, 5]                        │
│   }                                                                 │
│                                                                     │
│  TaskManager / reader state (per subtask)                           │
│  ──────────────────────────────────────────                          │
│   IcebergSourceSplit (currently assigned to subtask 3) {            │
│     task: CombinedScanTask { files: [f0, f1, f2] }                  │
│     fileOffset: 1                ← currently reading f1             │
│     recordOffset: 318420         ← next() will yield row 318421     │
│   }                                                                 │
│                                                                     │
│  On crash + restart:                                                │
│    Enumerator: resume polling from snapshot #42 (exclusive)         │
│    Reader subtask 3: DataIterator.seek(1, 318420) on splitX         │
│      → skips f0 entirely, opens f1, drops 318420 rows               │
│      → next() returns row 318421                                    │
└────────────────────────────────────────────────────────────────────┘
```

---

# Part 6: Split Discovery Throttling

A subtle production hazard: if the enumerator discovers splits **faster** than
readers can consume them, the assigner's pending queue grows unbounded. This blows
up enumerator memory and inflates the checkpoint size (because the entire pending
queue is serialized on every checkpoint).

The connector solves this with `EnumerationHistory` — a tiny circular buffer that
records how many splits each recent enumeration cycle produced, then pauses
discovery when the assigner has more pending splits than the recent total.

## The History Buffer

```java
// enumerator/EnumerationHistory.java
class EnumerationHistory {
  private final int[] history;   // size 3 (ENUMERATION_SPLIT_COUNT_HISTORY_SIZE)
  private int count;

  synchronized void add(int splitCount) {
    int pos = count % history.length;     // circular index
    history[pos] = splitCount;
    count += 1;
  }

  synchronized boolean shouldPauseSplitDiscovery(int pendingSplitCountFromAssigner) {
    if (count < history.length) {
      return false;                       // not enough history yet
    } else {
      int totalSplitCountFromRecentDiscovery = Arrays.stream(history).reduce(0, Integer::sum);
      return pendingSplitCountFromAssigner >= totalSplitCountFromRecentDiscovery;
    }
  }
}
```

The size is hard-coded in `ContinuousIcebergEnumerator.java:45`:

```java
private static final int ENUMERATION_SPLIT_COUNT_HISTORY_SIZE = 3;
```

## How It Works

Each non-empty enumeration cycle calls `enumerationHistory.add(splitCount)`. After
3 cycles of history exist, every subsequent `discoverSplits()` first asks
`shouldPauseSplitDiscovery(assigner.pendingSplitCount())`.

If the assigner's pending count meets or exceeds the **sum** of the last 3 cycles'
splits, planning is skipped this cycle. The next cycle re-checks; if readers have
caught up enough, planning resumes.

## Concrete Example

```
maxPlanningSnapshotCount = 10  (default — at most 10 snapshots planned per cycle)

Cycle 1: discovered 50 splits   → history = [50, _, _]
Cycle 2: discovered 30 splits   → history = [50, 30, _]
Cycle 3: discovered 40 splits   → history = [50, 30, 40]   total = 120
Cycle 4: assigner has 200 pending → 200 >= 120 → PAUSE. No catalog call.
         (Readers slowly drain the queue.)
Cycle 5: assigner has 130 pending → 130 >= 120 → PAUSE.
Cycle 6: assigner has 100 pending → 100 < 120 → RESUME. Plan up to 10 snapshots.
         Discovered 35 splits → history = [30, 40, 35]   total = 105
```

The reasoning: if you discovered 120 splits worth in 3 cycles, you don't need to
add more until your backlog drops below that — the readers are clearly behind.

The 3-slot history gives **rolling tolerance**: a single big cycle doesn't permanently
inflate the threshold; an idle cycle doesn't drop it to zero. Source:
[#6299 — Flink: support split discovery throttling for streaming read](https://github.com/apache/iceberg/pull/6299).

## The `maxPlanningSnapshotCount` Limit

There's a second throttle: per enumeration cycle, the planner limits how far ahead
it walks. From `ContinuousSplitPlannerImpl.java:89-103`:

```java
private Snapshot toSnapshotInclusive(
    Long lastConsumedSnapshotId, Snapshot currentSnapshot, int maxPlanningSnapshotCount) {
  // snapshots are in reverse order (latest snapshot first)
  List<Snapshot> snapshots = Lists.newArrayList(
      SnapshotUtil.ancestorsBetween(table, currentSnapshot.snapshotId(), lastConsumedSnapshotId));
  if (snapshots.size() <= maxPlanningSnapshotCount) {
    return currentSnapshot;
  } else {
    return snapshots.get(snapshots.size() - maxPlanningSnapshotCount);
  }
}
```

If 50 snapshots have committed since the last enumerated position but
`maxPlanningSnapshotCount = 10`, the planner only advances to 10 snapshots ahead
this cycle. The next cycle picks up where this one stopped. This protects against
"recovery storm" situations where the source was paused for hours and tries to
catch up in one bite.

---

# Part 7: Reader-Side Replay and FLIP-27 Coordination

The enumerator emits splits; readers consume them. The protocol between them is
small but precise.

## Reader Lifecycle

```java
// reader/IcebergSourceReader.java:50-57
@Override
public void start() {
  // We request a split only if we did not get splits during the checkpoint restore.
  // Otherwise, reader restarts will keep requesting more and more splits.
  if (getNumberOfCurrentlyAssignedSplits() == 0) {
    requestSplit(Collections.emptyList());
  }
}

@Override
protected void onSplitFinished(Map<String, IcebergSourceSplit> finishedSplitIds) {
  requestSplit(Lists.newArrayList(finishedSplitIds.keySet()));
}
```

The reader's behavior:

1. On start, if **no** splits were restored from checkpoint, send a `SplitRequestEvent`
   to the coordinator (with empty finished list).
2. After finishing a split, send another `SplitRequestEvent` carrying the finished
   split id.
3. While reading, emit `RowData` records via the framework.

## The Custom Split Request Event

`AbstractIcebergEnumerator.handleSourceEvent` (`AbstractIcebergEnumerator.java:86-102`):

```java
@Override
public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
  if (sourceEvent instanceof SplitRequestEvent) {
    SplitRequestEvent splitRequestEvent = (SplitRequestEvent) sourceEvent;
    assigner.onCompletedSplits(splitRequestEvent.finishedSplitIds());
    readersAwaitingSplit.put(subtaskId, splitRequestEvent.requesterHostname());
    assignSplits();
  } else {
    throw new IllegalArgumentException(...);
  }
}
```

Two things happen per request:

1. **Acknowledge completion**: `assigner.onCompletedSplits(...)` lets the assigner
   drop any tracking it had for those finished splits. (For `DefaultSplitAssigner`
   this is a no-op since it only tracks UNASSIGNED.)
2. **Try to assign**: if the assigner has a split available, dispatch it. If not,
   register the reader as awaiting and complete a future when more splits arrive.

## Mid-Split Replay

When the reader finishes a split, it advances. When it crashes mid-split, the
unfinished split is in the reader's state with its `fileOffset` + `recordOffset`
already updated. On restart, the split goes back through the SplitReader, which
calls the ReaderFunction to open a new `DataIterator` — and the first thing that
DataIterator does is `seek(fileOffset, recordOffset)`.

The seek pattern guarantees that no record is emitted twice across crashes,
modulo Flink's standard exactly-once contract. Splits that were in-flight but not
yet checkpointed go back to the assigner via `addSplitsBack` (Flink runtime calls
this when a subtask fails); the next attempt of the subtask gets them fresh.

---

# Part 8: MOR vs COW Reading — How Deletes Are Applied

The Iceberg format spec defines two delete file types for V2 tables:

- **Position deletes** — a delete file listing `(file_path, position)` pairs. The
  reader of `file_path` skips rows at those positions.
- **Equality deletes** — a delete file listing `(equality_field_value, ...)` tuples.
  The reader of any data file written *before* the equality delete drops rows whose
  equality fields match.

Iceberg's `FileScanTask` carries `task.deletes()` — the set of delete files that
apply to the data file in that task. The Flink source applies them at read time
via `FlinkDeleteFilter`.

## `FlinkDeleteFilter` Wraps the Data File Iterator

From `RowDataFileScanTaskReader.java:76-96`:

```java
@Override
public CloseableIterator<RowData> open(FileScanTask task, InputFilesDecryptor inputFilesDecryptor) {
  Map<Integer, ?> idToConstant = PartitionUtil.constantsMap(task, RowDataUtil::convertConstant);
  FlinkDeleteFilter deletes = new FlinkDeleteFilter(task, tableSchema, projectedSchema, inputFilesDecryptor);
  CloseableIterable<RowData> iterable =
      deletes.filter(newIterable(task, deletes.requiredSchema(), idToConstant, inputFilesDecryptor));

  // Project the RowData to remove the extra meta columns.
  if (!projectedSchema.sameSchema(deletes.requiredSchema())) {
    RowDataProjection rowDataProjection = RowDataProjection.create(
        deletes.requiredRowType(),
        deletes.requiredSchema().asStruct(),
        projectedSchema.asStruct());
    iterable = CloseableIterable.transform(iterable, rowDataProjection::wrap);
  }

  return iterable.iterator();
}
```

The flow:

1. Build a `FlinkDeleteFilter` over `task.deletes()` — the delete files the planner
   said apply to this data file.
2. Open the underlying Parquet/ORC/Avro reader with a possibly-expanded schema
   (`deletes.requiredSchema()` — includes the equality columns even if the user
   didn't project them, because they're needed to evaluate matches).
3. Wrap the data-file iterator in `deletes.filter(...)`, which:
   - For each row, asks position-delete bitmaps "is this position deleted?"
   - For each row, asks equality-delete sets "does this equality tuple match?"
   - Drops rows where either says yes.
4. If the user projected a subset of columns, project away the extra equality
   columns that were only loaded to evaluate the filter.

## The Underlying `DeleteFilter` (Iceberg Core)

`FlinkDeleteFilter` extends `org.apache.iceberg.data.DeleteFilter` (a generic),
which handles the deletion logic engine-agnostically. The Flink-specific subclass
just provides:

- `asStructLike(RowData)` — wraps a Flink `RowData` so the generic equality-match
  code can read its fields.
- `getInputFile(location)` — opens delete files using Flink's `InputFilesDecryptor`
  so encryption settings carry through.

## COW Tables: Same Code Path, No Deletes

For an append-only / COW table, `task.deletes()` is empty, the `FlinkDeleteFilter`
passes every row through unchanged, and the optional projection trims columns. The
overhead vs a hand-coded reader is negligible — the filter sees zero delete files
and short-circuits.

## Concrete MOR Example

Suppose an upsert sequence:

```
Snapshot #1 (append):    data file f1 with rows {(id=1,name=Alice), (id=2,name=Bob)}
Snapshot #2 (rowDelta):  data file f2 with rows {(id=1,name=Alicia)}
                         equality delete file d1 keyed on (id=1)
```

Reading the table after #2:

| Source | What the reader sees |
|---|---|
| `FileScanTask(f1)` | `task.deletes() = [d1]` → reader loads f1 + d1, drops `(id=1,name=Alice)`, emits `(id=2,name=Bob)` |
| `FileScanTask(f2)` | `task.deletes() = []` (no deletes apply to a newer data file) → emits `(id=1,name=Alicia)` |

Result: `{(id=1,name=Alicia), (id=2,name=Bob)}` — exactly one logical row per id,
with the latest value. The MOR resolution happens transparently at read time.

This works for **batch reads** (single snapshot point-in-time) because `TableScan`
uses the full snapshot view including delete files. For **streaming reads**, see
the major caveat in Part 9.

---

# Part 9: How Updates Are Handled — And Why Streaming Has a Caveat

## Batch Reads: Updates Work Correctly

For a `TableScan` (batch / point-in-time), the source sees every applicable
`DataFile` + `DeleteFile` pair at the requested snapshot. Updates from upstream
upsert writers resolve correctly via the MOR mechanism in Part 8.

## Streaming Reads: `IncrementalAppendScan` Skips Overwrite Snapshots

The streaming enumerator uses `table.newIncrementalAppendScan().fromSnapshotExclusive(...)`.
In Iceberg core (`org.apache.iceberg.BaseIncrementalAppendScan`), this scan walks
the snapshot chain between the two endpoints and **returns only data files added
in snapshots whose `operation` is `append`**. Snapshots with operations
`overwrite`, `delete`, `replace`, etc. are filtered out.

But the Flink IcebergSink for **upsert mode** uses `RowDelta` — which produces a
snapshot with operation `overwrite` (because it adds both data files and equality
delete files atomically). So:

```
Upstream Flink job using upsert mode → produces snapshot #501 (operation=overwrite)
Downstream Flink job using IcebergSource streaming  →
  IncrementalAppendScan walks [#500, #501] → operation filter rejects #501 →
  ZERO splits emitted for snapshot #501
```

**Net effect**: the streaming source silently *skips* the upstream's RowDelta
commits. Upserts and deletes from an upstream upsert pipeline are **not** delivered
to a streaming downstream reader.

## What the Streaming Source Does See

| Upstream commit shape | Operation | Streaming source picks up? |
|---|---|---|
| `AppendFiles` (append-only sink) | `append` | ✓ Yes — new data files are emitted as splits |
| `RowDelta` add-only (data files, no deletes) | depends — `append` if no deletes added | ✓ usually yes |
| `RowDelta` with deletes (upsert mode) | `overwrite` | ✗ **No — skipped entirely** |
| `ReplacePartitions` | `overwrite` | ✗ No |
| `OverwriteFiles` | `overwrite` | ✗ No |
| `RewriteDataFiles` (compaction) | `replace` | ✗ No |
| `RewriteManifests` | `replace` | ✗ No |
| `expireSnapshots` | (no snapshot) | n/a |
| Schema/spec evolution alone | (no snapshot) | n/a |

This is why the streaming Iceberg source is **not currently a CDC source** in the
classic sense — and why projects like Flink CDC build their own Iceberg connector
to handle changelog streams.

## What If You Really Want Updates to Stream?

Three real-world workarounds:

1. **Constrain upstream to append-only mode.** If you control the producer, write
   only inserts (no upserts) — perhaps emit a separate "tombstone" event with a
   logical operation column. The streaming source then sees every commit.
2. **Read with batch semantics on a schedule.** Replace continuous streaming with
   a Flink batch job that scheduled-reads the table every N minutes. Batch reads
   see deletes correctly. Downside: latency floored at N.
3. **Use a different connector.** The Iceberg community is iterating on streaming
   CDC support; check current release notes. As of the codebase here, FLIP-27
   streaming does not propagate row-level updates from upsert writers.

The classes you'd modify to extend support are
`FlinkSplitPlanner.checkScanMode(...)` and the choice of Iceberg-core scan in
`FlinkSplitPlanner.planTasks(...)`. There's no current `IncrementalChangelogScan`
wired in, but Iceberg-core has the building blocks.

---

# Part 10: Exactly-Once Semantics

## Yes — the FLIP-27 IcebergSource Is Exactly-Once

Exactly-once at the source layer means: under any combination of crash and restart,
every record in the source data is delivered to the downstream operator **exactly
one time**. Achieving this requires that the position captured at checkpoint `N` is
precise enough to replay from the next un-emitted record on recovery.

The Iceberg FLIP-27 source achieves this through **three state pieces** that are
checkpointed together at every Flink barrier, plus the FLIP-27 framework's
barrier-synchronized record emission. The legacy
`StreamingMonitorFunction` + `StreamingReaderOperator` pair, in contrast, is
**at-least-once only** (covered at the end of this section).

## The Three Checkpointed State Pieces

### Piece 1 — Enumerator Position (Snapshot-Level Cursor)

```java
// ContinuousIcebergEnumerator.java:112-116
@Override
public IcebergEnumeratorState snapshotState(long checkpointId) {
  return new IcebergEnumeratorState(
      enumeratorPosition.get(),       // (snapshotId, snapshotTimestampMs)
      assigner.state(),               // pending UNASSIGNED splits — Piece 2
      enumerationHistory.snapshot());
}
```

On restart, the enumerator resumes polling from `lastEnumeratedPosition.snapshotId()`
*exclusive*. Every snapshot that committed during downtime is picked up on the
first post-restart poll. Snapshots that were already planned before the crash are
not re-planned — their splits are either in `pendingSplits` (Piece 2) or in some
reader's state (Piece 3).

### Piece 2 — Pending Splits (Assigner State)

`DefaultSplitAssigner.state()` returns only the **unassigned queue** —
splits that were planned but not yet handed to any reader:

```java
// DefaultSplitAssigner.java:87-91
@Override
public synchronized Collection<IcebergSourceSplitState> state() {
  return pendingSplits.stream()
      .map(split -> new IcebergSourceSplitState(split, IcebergSourceSplitStatus.UNASSIGNED))
      .collect(Collectors.toList());
}
```

On restore, they go straight back into the assigner queue
(`IcebergSource.java:211-219`):

```java
if (enumState == null) {
  assigner = assignerFactory.createAssigner();
} else {
  assigner = assignerFactory.createAssigner(enumState.pendingSplits());
}
```

### Piece 3 — Per-Split Replay Cursor (Reader State)

This is the **load-bearing piece for record-level exactly-once.** Assigned splits
are checkpointed by the **reader** in its own state (managed by FLIP-27's
`SingleThreadMultiplexSourceReaderBase`). Each split carries its current
`(fileOffset, recordOffset)` updated by `DataIterator` as records flow
(`DataIterator.java:117-122`):

```java
@Override
public T next() {
  updateCurrentIterator();
  recordOffset += 1;                       // bumped BEFORE the read
  return currentIterator.next();
}
```

After consuming `N` records of file `F` in the split, the split's recorded
position is `(F, N)` — meaning "the next call should return record `N` of file `F`."

On restart, `DataIterator.seek(F, N)` fast-forwards through `F`'s first `N` records
and resumes at record `N+1` exactly. No duplicate, no skip
(`DataIterator.java:77-109`):

```java
public void seek(int startingFileOffset, long startingRecordOffset) {
  Preconditions.checkState(fileOffset == -1, "Seek should be called before any other iterator actions");
  for (long i = 0L; i < startingFileOffset; ++i) {     // skip whole files
    tasks.next();
  }
  updateCurrentIterator();
  for (long i = 0; i < startingRecordOffset; ++i) {    // discard within-file rows
    if (currentFileHasNext() && hasNext()) {
      next();
    } else {
      throw new IllegalStateException(...);
    }
  }
  fileOffset = startingFileOffset;
  recordOffset = startingRecordOffset;
}
```

The three pieces together form a **complete, disjoint partition** of "what work
exists in the system right now":

```
┌────────────────────────────────────────────────────────────────────┐
│  ALL SPLITS IN THE PIPELINE AT CHECKPOINT N                         │
│                                                                     │
│  Planned but not yet assigned   →  Piece 2: assigner.state()        │
│  Assigned, partly read          →  Piece 3: reader state            │
│                                       (with current fileOffset,    │
│                                        recordOffset)                │
│  Assigned, fully read           →  not in state — already finished, │
│                                       reported back via             │
│                                       SplitRequestEvent.finished    │
│                                                                     │
│  Not yet planned                →  Piece 1: enumeratorPosition      │
│                                       (cursor for future polling)   │
└────────────────────────────────────────────────────────────────────┘
```

## Failure Scenarios

| Scenario | What happens, in detail |
|---|---|
| **Reader subtask fails, job alive** | Flink calls `enumerator.addSplitsBack(splits, subtaskId)` → `assigner.onUnassignedSplits(splits)` (`AbstractIcebergEnumerator.java:112-116`). The failed subtask's in-progress splits (with their `fileOffset`/`recordOffset` from the **last successful checkpoint**) re-enter the assigner queue and get dispatched to a fresh reader, which seeks to the saved position. |
| **Whole job restart from checkpoint N** | Enumerator restored with Piece 1 + Piece 2 + history. Each reader subtask restored with Piece 3 (its assigned splits and their positions). Polling resumes from `snapshotId > position`. No record missed or duplicated. |
| **Restart from older checkpoint / savepoint rollback** | All three pieces restored to their older values. Records emitted between the old checkpoint and the crash are **re-emitted** — this is correct exactly-once semantics: from the old checkpoint's perspective, those records were never emitted. End-to-end exactly-once requires the downstream to either be transactionally checkpoint-aware or idempotent. |
| **Mid-batch checkpoint** | FLIP-27's `SourceOperator` emits records one at a time from the fetched batch (`RecordsWithSplitIds<RecordAndPosition<T>>`) and synchronizes with the barrier. The position captured at the barrier corresponds **exactly to the last record emitted before the barrier** — not the batch boundary. |
| **Enumerator restart while reader keeps running** (rare; e.g., JM failover with TM-side speculative recovery) | The framework's `OperatorCoordinator` re-creates the enumerator from the last checkpoint state. Already-assigned splits stay with their readers. The enumerator catches up via the next polling cycle. |

## The Position-Bump-Then-Read Ordering

A subtle but important detail in `DataIterator.next()`:

```java
public T next() {
  updateCurrentIterator();
  recordOffset += 1;                       // ← bump first
  return currentIterator.next();           // ← then read
}
```

The position is incremented **before** the record is fetched. So `recordOffset`
always represents the count of records that have been **consumed from this file
so far** — equivalently, the index of the next record to emit. This invariant is
what makes `seek(F, N)` work: it skips exactly `N` records (calling `next()` `N`
times) so that the subsequent `next()` returns the `(N+1)`-th record of file `F`.

If the bump happened *after* the read, a crash between read and bump would lose
the just-emitted record on replay (because the saved position would point to it
again, but the downstream wouldn't know it was a re-emission).

## End-to-End Exactly-Once Requires a Cooperating Sink

Source exactly-once alone is not end-to-end exactly-once. After a restart, the
source may legitimately **re-emit** records that were emitted before the last
durable checkpoint — that's how it recovers correctly from its perspective. The
downstream sink must be one of:

| Sink type | How it absorbs source re-emission |
|---|---|
| **Transactional / 2PC** | Buffers per-checkpoint changes, commits atomically on `notifyCheckpointComplete`. Re-emitted records before the next commit go into the next pre-commit buffer; nothing is double-committed. |
| **Idempotent** | Same input → same output, regardless of how many times applied. E.g., upserts keyed by a unique business id. |
| **Iceberg sink (`IcebergCommitter`)** | Combines both: pre-commit per Flink checkpoint, then deduplication on the Iceberg side via `flink.max-committed-checkpoint-id` snapshot summary. See the [Iceberg sink deep-dive](flink-iceberg-sink-connector-deep-dive.md) for the full mechanism. |
| **At-least-once sink** | End-to-end is only at-least-once. Records emitted between two checkpoints can land in the sink twice. |

An Iceberg-to-Iceberg pipeline (read from one table, write to another using
`IcebergSource` + `IcebergSink`) gets end-to-end exactly-once natively — both
sides use Flink checkpoints as the consistency boundary.

## Common Misconception: The Streaming Caveat Is NOT an Exactly-Once Violation

The Part 9 caveat — `IncrementalAppendScan` silently filtering out
`overwrite`-type snapshots from upstream upsert writers — is sometimes reported
as "the streaming source isn't exactly-once." That conflates two properties:

| Property | Definition | Streaming `IcebergSource` |
|---|---|---|
| **Exactly-once delivery** | Every record the source produces is delivered to downstream exactly once | ✓ Yes |
| **Completeness** | The source produces every row that was committed to the table | ✗ No — for upstream `RowDelta` (upsert) commits, no row is produced |

A source that delivers zero records exactly-once is technically exactly-once. The
caveat is a completeness problem (some commits are invisible), not a delivery
problem (re-emission or skipped records inside what *is* emitted).

The distinction matters because the fix is different:

- For a delivery bug, you fix the position-tracking code.
- For the completeness gap, you either (a) constrain the upstream writer to
  append-only, (b) switch the downstream to scheduled batch reads, or (c) wait
  for / build an `IncrementalChangelogScan`-based streaming source.

## Legacy Source Is At-Least-Once Only

The legacy `StreamingMonitorFunction` + `StreamingReaderOperator` pair is
**at-least-once**, not exactly-once. From Appendix A:

```
StreamingMonitorFunction:  ListState<Long> lastSnapshotIdState         ← one long
StreamingReaderOperator:   ListState<FlinkInputSplit> inputSplitsState
                                              │
                                              ▼
                                no per-split (fileOffset, recordOffset) cursor
```

A split's progress is `IDLE` or `RUNNING` — no byte-level or record-level offset.
If the operator crashes mid-split, on restart the split is replayed **from byte
zero**. Every record that was already delivered before the crash is re-emitted.
Downstream sees duplicates unless it's transactional or idempotent.

The legacy pipeline relies on the downstream sink being deduplicating (or
tolerant of duplicates) to bridge the gap. End-to-end exactly-once with the
legacy source path is achievable but **requires more from the sink** than the
FLIP-27 path does.

## Quick Reference Table

| Property | FLIP-27 `IcebergSource` | Legacy `FlinkSource` |
|---|---|---|
| Record-level exactly-once delivery | ✓ | ✗ (at-least-once) |
| Per-split replay cursor | ✓ `(fileOffset, recordOffset)` in split | ✗ none — split replayed from byte 0 |
| Survives reader subtask failure with zero duplicates | ✓ via `addSplitsBack` + seek | ✗ split replayed from byte 0 |
| Survives full job restart from checkpoint with zero duplicates | ✓ all three state pieces restored | ✗ partial — depends on whether a split finished pre-crash |
| Survives older-savepoint restore | ✓ (re-emits records between savepoint and crash; downstream must be checkpoint-aware) | ✓ (same caveat) |
| End-to-end exactly-once with Iceberg sink | ✓ native | △ depends on sink dedup; possible but more fragile |
| Streaming sees upstream upsert commits | ✗ (Part 9 caveat — orthogonal to delivery semantics) | ✗ (same caveat) |

---

# Part 11: Starting Strategies

For streaming, `StreamingStartingStrategy` picks the initial position. From
`StreamingStartingStrategy.java`:

| Strategy | Initial position | Inclusive of starting snapshot? |
|---|---|---|
| `TABLE_SCAN_THEN_INCREMENTAL` (default) | Current snapshot at job start — do a full batch scan, then stream new commits | First commit *exclusive* |
| `INCREMENTAL_FROM_LATEST_SNAPSHOT` | Current snapshot at job start | **Inclusive** — first batch is that snapshot's appended files |
| `INCREMENTAL_FROM_LATEST_SNAPSHOT_EXCLUSIVE` | Current snapshot at job start | **Exclusive** — wait for the next commit |
| `INCREMENTAL_FROM_EARLIEST_SNAPSHOT` | Oldest snapshot in the chain | Inclusive |
| `INCREMENTAL_FROM_SNAPSHOT_ID` | A specific snapshot id | Inclusive |
| `INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP` | First snapshot at-or-after the given timestamp | Inclusive |

## How "Inclusive" Is Implemented

Iceberg-core's `IncrementalAppendScan.fromSnapshotExclusive(id)` is naturally
**exclusive** of the start. To get *inclusive* behavior, the planner sets the
exclusive starting point to the **parent** snapshot of the desired start. From
`ContinuousSplitPlannerImpl.discoverInitialSplits()`:

```java
// For all "inclusive" modes:
// Use parentId to achieve the inclusive behavior. It is fine if parentId is null.
Long parentSnapshotId = startSnapshot.parentId();
if (parentSnapshotId != null) {
  Snapshot parentSnapshot = table.snapshot(parentSnapshotId);
  Long parentSnapshotTimestampMs = parentSnapshot != null ? parentSnapshot.timestampMillis() : null;
  toPosition = IcebergEnumeratorPosition.of(parentSnapshotId, parentSnapshotTimestampMs);
} else {
  toPosition = IcebergEnumeratorPosition.empty();
}
```

So `INCREMENTAL_FROM_LATEST_SNAPSHOT` actually stores **parentSnapshotId** as the
"last enumerated position." The first poll then plans `(parent, current]` which
includes the current snapshot's appends. Clever, simple.

## TABLE_SCAN_THEN_INCREMENTAL: Batch Then Stream

The default strategy is special — `discoverInitialSplits` calls
`FlinkSplitPlanner.planIcebergSourceSplits` directly with a **full table scan**
context, plans every file currently in the table as splits, then advances the
enumerator position to the current snapshot (exclusive) so future polls only
return newer commits:

```java
if (scanContext.streamingStartingStrategy() == StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL) {
  splits = FlinkSplitPlanner.planIcebergSourceSplits(
      table, scanContext.copyWithSnapshotId(startSnapshot.snapshotId()), workerPool);
  toPosition = IcebergEnumeratorPosition.of(startSnapshot.snapshotId(), startSnapshot.timestampMillis());
}
```

This produces a clean transition from "read the world" to "read the delta." It's
what you almost always want for "load + tail" pipelines.

---

# Part 12: Configuration Reference

## Key Read Options

| Option | Default | Description |
|---|---|---|
| `streaming` | `false` | If `true`, source is unbounded (FLIP-27 `CONTINUOUS_UNBOUNDED`); else bounded |
| `monitor-interval` | `10s` | How often the enumerator polls the table for new snapshots |
| `starting-strategy` | `TABLE_SCAN_THEN_INCREMENTAL` | See Part 11 |
| `start-snapshot-id` | — | Used with `INCREMENTAL_FROM_SNAPSHOT_ID` |
| `start-snapshot-timestamp` | — | Used with `INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP` |
| `end-snapshot-id` | — | Batch end position (exclusive at scan time, set in `ScanContext`) |
| `as-of-timestamp` | — | Batch read at a specific table timestamp |
| `snapshot-id` | — | Batch read at a specific snapshot |
| `branch` | — | Read from a non-`main` branch ref |
| `tag` | — | Read at a tag |
| `max-planning-snapshot-count` | `Integer.MAX_VALUE` (no limit) | Cap on snapshots planned per poll cycle — important for catch-up scenarios |
| `max-allowed-planning-failures` | `3` | Consecutive catalog/planning failures tolerated before failing the job |
| `split-size` | (table prop: `read.split.target-size`, default 128 MB) | Target byte size per `IcebergSourceSplit` |
| `split-lookback` | (table prop, default 10) | Bin-packing look-back |
| `split-file-open-cost` | (table prop, default 4 MB) | Per-file open cost for bin-packing |
| `case-sensitive` | `false` | Column name matching |
| `include-column-stats` | `false` | Include lower/upper bounds in scan tasks (needed for watermark column) |
| `limit` | — | Cap on rows emitted (batch) |

## Watermark Generation

If you set `watermark-column = some_ts_column` on the builder, the source
constructs a `ColumnStatsWatermarkExtractor` and uses the file-level min value
of that column from manifest metadata as the watermark for the split. This means
**watermarks are emitted once per split**, not per row — drastically cheaper than
the typical Flink watermark generator. The trade-off is that watermarks have
file-level granularity.

Setting `watermark-column` also forces the assigner to use
`OrderedSplitAssignerFactory(SplitComparators.watermark(...))` so splits are
dispatched in watermark order, not arbitrary order.

---

# Part 13: v1.20 vs v2.1 Differences

The Iceberg-side source connector is **almost identical** between v1.20 and v2.1.
Diffing the entire `source/` directory:

```
Files .../v2.1/.../FlinkSource.java and .../v1.20/.../FlinkSource.java differ:
< import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
> import org.apache.flink.streaming.api.functions.source.SourceFunction;
< import org.apache.flink.table.legacy.api.TableSchema;
> import org.apache.flink.table.api.TableSchema;
```

Same pattern for `IcebergSource.java`, `StreamingMonitorFunction.java`,
`StreamingReaderOperator.java`, `IcebergTableSource.java`, `RowDataRewriter.java`:
only **package renames** to track Flink 2.x's reshuffling of deprecated APIs.
The semantics are unchanged.

| Aspect | v1.20 (Flink 1.20) | v2.1 (Flink 2.1) |
|---|---|---|
| `SourceFunction` (legacy DataStream source) | `flink.streaming.api.functions.source.SourceFunction` | `flink.streaming.api.functions.source.legacy.SourceFunction` |
| `TableSchema` | `flink.table.api.TableSchema` | `flink.table.legacy.api.TableSchema` |
| FLIP-27 `IcebergSource` API | Same | Same |
| Enumerator / Reader logic | Same | Same |
| `ContinuousSplitPlannerImpl` | Same | Same |
| Starting strategies | Same | Same |
| MOR delete handling | Same | Same |

There are **no behavioral changes**. Existing pipelines port directly with
classpath updates only.

---

# Part 14: Key Classes Reference

## FLIP-27 (V2) Source Classes

| Class | Role |
|---|---|
| `IcebergSource` | Entry point — implements `Source<T, IcebergSourceSplit, IcebergEnumeratorState>` |
| `IcebergSource.Builder` | Fluent builder; offers `forRowData()`, `forOutputType(converter)` |
| `IcebergTableSource` | Table API adapter — produces an `IcebergSource` from a Table DDL |
| `ScanContext` | Carries all scan options (filters, projection, starting strategy, monitor interval, ...) |

## Enumerator Subsystem (JM-side)

| Class | Role |
|---|---|
| `AbstractIcebergEnumerator` | Base — manages `readersAwaitingSplit`, assigns splits, handles failures |
| `ContinuousIcebergEnumerator` | Streaming enumerator — periodic polling, maintains position + history |
| `StaticIcebergEnumerator` | Batch enumerator — one-shot plan at start, no polling |
| `ContinuousSplitPlannerImpl` | Performs the actual `table.refresh()` + `IncrementalAppendScan` |
| `IcebergEnumeratorPosition` | `(snapshotId, snapshotTimestampMs)` — durable cursor |
| `IcebergEnumeratorState` | Checkpointed bundle: position + pendingSplits + history |
| `IcebergEnumeratorStateSerializer` | (De)serializes enumerator state for checkpoints |
| `EnumerationHistory` | 3-slot circular buffer for split-discovery throttling |

## Split Subsystem

| Class | Role |
|---|---|
| `IcebergSourceSplit` | `CombinedScanTask + (fileOffset, recordOffset)` — the unit of work |
| `IcebergSourceSplitState` | Wraps split + status (`UNASSIGNED` / `ASSIGNED` / `COMPLETED`) |
| `IcebergSourceSplitSerializer` | (De)serializes splits |
| `SplitRequestEvent` | Custom `SourceEvent` — reader → coordinator, piggybacks finished split ids |

## Assigner Subsystem

| Class | Role |
|---|---|
| `SplitAssigner` | Interface — manages the pending queue |
| `DefaultSplitAssigner` | FIFO (or PriorityQueue if a comparator is set); tracks only UNASSIGNED |
| `SimpleSplitAssignerFactory` | Builds `DefaultSplitAssigner` |
| `OrderedSplitAssignerFactory` | Builds ordered variant — used when a watermark column is configured |

## Reader Subsystem (TM-side)

| Class | Role |
|---|---|
| `IcebergSourceReader` | FLIP-27 SourceReader — delegates to `IcebergSourceSplitReader`, requests splits |
| `IcebergSourceSplitReader` | Pulls split from local queue, opens via `ReaderFunction`, emits batches |
| `ReaderFunction<T>` | SAM that opens a split into a `CloseableIterator<RecordsWithSplitIds>` |
| `RowDataReaderFunction` | Default — produces `RowData` |
| `AvroGenericRecordReaderFunction` | Produces Avro `GenericRecord` |
| `ConverterReaderFunction` | User-provided `RowDataConverter<T>` |
| `MetaDataReaderFunction` | For reading Iceberg metadata tables |
| `DataIterator<T>` | Wraps a `CombinedScanTask` — opens files in order, supports `seek` |
| `RowDataFileScanTaskReader` | Opens a single data file with `FlinkDeleteFilter` applied |
| `FlinkDeleteFilter` | Subclass of Iceberg-core `DeleteFilter` — handles position + equality deletes |
| `ColumnStatsWatermarkExtractor` | Watermark from manifest min-stats on a column |

## Iceberg-Core Touch Points

| Class (org.apache.iceberg.*) | Role |
|---|---|
| `Table` | The Iceberg table object — `refresh()`, `currentSnapshot()`, `newScan()`, `newIncrementalAppendScan()` |
| `Snapshot` | A point in table history — `snapshotId()`, `parentId()`, `timestampMillis()`, `operation()` |
| `IncrementalAppendScan` | Returns data files added in **APPEND**-type snapshots only (Part 9 caveat) |
| `TableScan` | Full snapshot scan — used by batch reads |
| `FileScanTask` | A data file + its applicable delete files |
| `CombinedScanTask` | A bin-packed group of `FileScanTask`s — one unit of bin-packing |
| `DeleteFilter` (in `org.apache.iceberg.data`) | Generic position+equality delete application |
| `SnapshotUtil` | `ancestorsBetween`, `oldestAncestor`, `oldestAncestorAfter`, ... |

---

# Appendix A: Legacy Source (FlinkSource + StreamingMonitorFunction)

Before FLIP-27 landed, Iceberg-Flink used a pre-FLIP-27 source pipeline based on
custom Flink operators. It's still present in the codebase, used by some integrations,
and runs identically in v1.20 and v2.1 (modulo the package renames noted in Part 13).

## Pipeline Shape

```
┌────────────────────────────────────────────────────────────────────────┐
│              LEGACY ICEBERG SOURCE (PRE-FLIP-27)                        │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────┐          │
│  │  StreamingMonitorFunction extends RichSourceFunction      │          │
│  │      parallelism = 1                                       │          │
│  │      single thread, while(isRunning) loop                  │          │
│  │                                                            │          │
│  │  initializeState():                                        │          │
│  │    ListState<Long> lastSnapshotIdState                    │          │
│  │    if (restored) lastSnapshotId = state.get()              │          │
│  │    else if (start-snapshot-id set) lastSnapshotId = ...   │          │
│  │    else lastSnapshotId = INIT (-1L)                        │          │
│  │                                                            │          │
│  │  run(SourceContext):                                       │          │
│  │    while (isRunning) {                                     │          │
│  │      monitorAndForwardSplits();                            │          │
│  │      Thread.sleep(monitorInterval);                        │          │
│  │    }                                                       │          │
│  │                                                            │          │
│  │  monitorAndForwardSplits():                                │          │
│  │    table.refresh();                                        │          │
│  │    current = table.currentSnapshot();                      │          │
│  │    if (current.id() != lastSnapshotId):                    │          │
│  │      newScanContext = (lastSnapshotId == INIT)             │          │
│  │        ? scanContext.copyWithSnapshotId(current.id())      │          │
│  │        : scanContext.copyWithAppendsBetween(               │          │
│  │             lastSnapshotId, capped_current_id);            │          │
│  │      splits = FlinkSplitPlanner.planInputSplits(...)       │          │
│  │      synchronized (sourceContext.getCheckpointLock()) {    │          │
│  │        for (split : splits) sourceContext.collect(split);  │          │
│  │        lastSnapshotId = capped_current_id;                 │          │
│  │      }                                                     │          │
│  └──────────────────────┬───────────────────────────────────┘          │
│                          │ FlinkInputSplit records on Flink dataflow    │
│                          ▼                                              │
│  ┌──────────────────────────────────────────────────────────┐          │
│  │  StreamingReaderOperator extends AbstractStreamOperator   │          │
│  │      parallelism = N                                       │          │
│  │      uses Flink MailboxExecutor to interleave reads with   │          │
│  │      checkpoint barriers (no back-pressure)                │          │
│  │                                                            │          │
│  │  initializeState():                                        │          │
│  │    ListState<FlinkInputSplit> inputSplitsState             │          │
│  │    queue<FlinkInputSplit> splits = restored state          │          │
│  │    enqueueProcessSplits();                                 │          │
│  │                                                            │          │
│  │  processElement(StreamRecord<FlinkInputSplit>):            │          │
│  │    splits.add(element.getValue());                         │          │
│  │    enqueueProcessSplits();                                 │          │
│  │                                                            │          │
│  │  processSplits() (in MailboxExecutor):                    │          │
│  │    split = splits.poll();                                  │          │
│  │    format.open(split);                                     │          │
│  │    while (!format.reachedEnd())                            │          │
│  │      sourceContext.collect(format.nextRecord(...));        │          │
│  │    format.close();                                         │          │
│  │    enqueueProcessSplits();                                 │          │
│  │                                                            │          │
│  │  Output: DataStream<RowData>                               │          │
│  └──────────────────────────────────────────────────────────┘          │
└────────────────────────────────────────────────────────────────────────┘
```

## State Model (Legacy)

The legacy source's checkpointed state is much **smaller** than FLIP-27's, but
also less flexible:

| Operator | State | Type |
|---|---|---|
| `StreamingMonitorFunction` | `ListState<Long> lastSnapshotIdState` | A single long: the last snapshot id whose splits were forwarded |
| `StreamingReaderOperator` | `ListState<FlinkInputSplit> inputSplitsState` | Per-subtask: splits received but not yet read |

Notably **no per-split fileOffset/recordOffset** — the legacy operator finishes
one split at a time inside the MailboxExecutor, and the split state is
"queued / being processed / done." If a checkpoint barrier arrives mid-split,
the operator can wait for the current split to finish (or be interrupted), but the
checkpoint records splits that haven't started — there's no replay cursor within
a split. This means **larger duplicate-emission windows on crash** than the
FLIP-27 path.

## Why Two Operators?

The split is to decouple split discovery from reading:

- `StreamingMonitorFunction` runs at **parallelism 1** so polling and position
  tracking are centralized. Multiple parallel polls would race on `table.refresh()`
  and produce overlapping splits.
- `StreamingReaderOperator` runs at parallelism **N** for throughput. Splits are
  distributed by Flink's normal dataflow shuffling.

This is the same JM-side / TM-side split as FLIP-27, just implemented manually
with two custom operators instead of relying on the FLIP-27 framework.

## Use of `MailboxExecutor`

The legacy reader is clever in one specific way: it uses Flink's
`MailboxExecutor` to schedule reading. The comment in `StreamingReaderOperator.java:53-55`:

> As soon as a split descriptor is received, it is put in a queue, and use
> `MailboxExecutor` read the actual data of the split. This architecture allows
> the separation of the reading thread from the one split processing the
> checkpoint barriers, thus removing any potential back-pressure.

The trick: enqueue at most one reading task at a time (`SplitState.RUNNING`
guard). When a checkpoint barrier comes in, it competes with the read task in
the mailbox queue and gets serviced as soon as the current `processSplits()`
chunk returns. This prevents the operator from being stuck mid-read when a
barrier arrives.

## Legacy vs FLIP-27 Comparison

| Aspect | Legacy (`StreamingMonitorFunction` + `StreamingReaderOperator`) | FLIP-27 (`IcebergSource`) |
|---|---|---|
| Architecture | Two custom operators on the dataflow | Source + Enumerator (coordinator) + Reader (subtasks) |
| Discovery state | `lastSnapshotId` only | `IcebergEnumeratorPosition` + `pendingSplits` + `EnumerationHistory` |
| Per-split replay state | None — split is "queued" or "done" | `(fileOffset, recordOffset)` updated continuously |
| Throttling | None | `EnumerationHistory` 3-slot buffer |
| Split assignment | Flink dataflow routing | Custom `SplitAssigner` (FIFO or ordered) |
| Split request protocol | Implicit via dataflow | Explicit `SplitRequestEvent` |
| Watermark generation | None built-in | Optional column-stats watermark |
| Boundedness | Always continuous | Either bounded or continuous via `getBoundedness()` |
| MOR/COW handling | Same — uses same `RowDataFileScanTaskReader` underneath | Same |
| `IncrementalAppendScan` semantics | Same — same Part 9 caveat | Same |

## Legacy Entry Point

`FlinkSource.forRowData().tableLoader(loader).env(env).buildStream()` builds a
DataStream from the legacy pipeline. The newer code-paths prefer
`IcebergSource.forRowData().tableLoader(loader).buildStream(env)` — same shape,
backed by FLIP-27.

## When to Prefer Legacy

- You're maintaining a long-standing pipeline whose state is in the legacy format
  and don't want a savepoint migration.
- You're integrating with Flink components that haven't fully adopted FLIP-27.

For new code, prefer FLIP-27 — it has the per-split replay, throttling, and
explicit position tracking that production users need.

---

## Sources

- [Apache Iceberg — Flink Queries](https://iceberg.apache.org/docs/latest/flink-queries/)
- [Flink: support split discovery throttling for streaming read (#6299)](https://github.com/apache/iceberg/pull/6299)
- [Implements the Flink source based on the new FLIP-27 interface (#1626)](https://github.com/apache/iceberg/issues/1626)
- [FLIP-27: Refactor Source Interface](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27:+Refactor+Source+Interface)
- [FLIP-267: Iceberg Connector](https://cwiki.apache.org/confluence/display/FLINK/FLIP+267:+Iceberg+Connector)
- Source code: `myiceberg/flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/source/` and `myiceberg/flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/`
