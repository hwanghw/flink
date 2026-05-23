# Flink Cassandra Connector: WAL-Based Exactly-Once Sink Architecture

This document provides a deep dive into the Flink Cassandra connector (`flink-connector-cassandra`), focusing on how it uses a Write-Ahead Log (WAL) to achieve exactly-once end-to-end semantics with Flink checkpointing, and how timestamps and watermarks are (or are not) managed.

All connector code references point to files under:
`flink-connector-cassandra/src/main/java/org/apache/flink/streaming/connectors/cassandra/`

GenericWriteAheadSink and CheckpointCommitter are Flink core classes under:
`flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/`

---

# Part 1: Sink Types Overview

## 1.1 Two Approaches: At-Least-Once vs Exactly-Once

The Cassandra connector provides two fundamentally different sink architectures:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     CassandraSink.addSink(stream)                          │
│                              │                                              │
│              .enableWriteAheadLog()?                                        │
│                    /              \                                         │
│                  YES               NO                                       │
│                  │                  │                                        │
│     ┌────────────▼───────────┐   ┌──▼───────────────────────┐              │
│     │  WAL Sink              │   │  Non-WAL Sink             │              │
│     │  (exactly-once)        │   │  (at-least-once)          │              │
│     │                        │   │                           │              │
│     │  GenericWriteAheadSink │   │  CassandraSinkBase        │              │
│     │    ├ CassandraTuple    │   │    ├ CassandraTupleSink   │              │
│     │    │ WriteAheadSink    │   │    ├ CassandraRowSink     │              │
│     │    └ CassandraRow      │   │    ├ CassandraPojoSink    │              │
│     │      WriteAheadSink    │   │    └ CassandraScala       │              │
│     │                        │   │      ProductSink          │              │
│     │  + CassandraCommitter  │   │                           │              │
│     └────────────────────────┘   └───────────────────────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Builder routing** (from `CassandraSink.java`, lines 459-465):

```java
public CassandraSink<IN> build() throws Exception {
    sanityCheck();
    if (failureHandler == null) {
        failureHandler = new NoOpCassandraFailureHandler();
    }
    return isWriteAheadLogEnabled ? createWriteAheadSink() : createSink();
}
```

**WAL creation** (from `CassandraTupleSinkBuilder`, lines 511-528):

```java
@Override
protected CassandraSink<IN> createWriteAheadSink() throws Exception {
    return new CassandraSink<>(
            input.transform(
                    "Cassandra Sink",
                    null,
                    new CassandraTupleWriteAheadSink<>(
                            query,
                            serializer,
                            builder,
                            new CassandraCommitter(builder))));  // Default: stores commit info in Cassandra
}
```

**Limitation**: WAL (exactly-once) is only available for **Tuple** and **Row** types. POJO and Scala Product sinks throw:
```java
// CassandraPojoSinkBuilder, line 620
protected CassandraSink<IN> createWriteAheadSink() throws Exception {
    throw new IllegalArgumentException(
            "Exactly-once guarantees can only be provided for tuple types.");
}
```

## 1.2 Comparison

| Aspect | Non-WAL (At-Least-Once) | WAL (Exactly-Once) |
|--------|-------------------------|---------------------|
| **Base class** | `CassandraSinkBase` (RichSinkFunction) | `GenericWriteAheadSink` (AbstractStreamOperator) |
| **Guarantee** | At-least-once | Exactly-once (with idempotent writes) |
| **When data is sent** | Immediately on each `invoke()` | Only on `notifyCheckpointComplete()` |
| **Buffering** | None (in-flight only) | In Flink state backend |
| **Concurrency** | Semaphore-based (configurable) | Blocking per checkpoint batch |
| **Latency** | Low (async sends) | Higher (waits for checkpoint) |
| **Supported types** | Tuple, Row, POJO, Scala Product | Tuple, Row only |
| **Requires checkpointing** | No (but recommended) | Yes (throws if disabled) |
| **External metadata** | None | Cassandra auxiliary table |

---

# Part 2: GenericWriteAheadSink (Flink Core)

This is the foundational class that provides the WAL mechanism. It lives in Flink core, not in the connector.

**File**: `flink-streaming-java/.../operators/GenericWriteAheadSink.java`

## 2.1 The Core Idea

The problem: Cassandra does not support rollbacks. Once data is written, it cannot be "uncommitted." This makes it impossible to write during checkpoint and roll back if the checkpoint fails.

The solution: **Buffer all records in Flink's state backend** (the "write-ahead log"), and only flush to Cassandra **after** the checkpoint succeeds. An external `CheckpointCommitter` tracks which checkpoints have been flushed, so replays after failure can skip already-committed data.

## 2.2 Class Structure

```java
// GenericWriteAheadSink.java, lines 62-63
public abstract class GenericWriteAheadSink<IN> extends AbstractStreamOperator<IN>
        implements OneInputStreamOperator<IN, IN> {

    private final String id;                                  // Unique operator ID
    private final CheckpointCommitter committer;              // External commit tracker
    protected final TypeSerializer<IN> serializer;            // For serializing records to state

    private transient CheckpointStateOutputStream out;        // Current write buffer
    private transient CheckpointStorageWorkerView checkpointStorage;

    private transient ListState<PendingCheckpoint> checkpointedState;  // Operator state
    private final Set<PendingCheckpoint> pendingCheckpoints = new TreeSet<>();  // Sorted by checkpoint ID
```

**PendingCheckpoint** (inner class, lines 301-349):
```java
private static final class PendingCheckpoint
        implements Comparable<PendingCheckpoint>, Serializable {
    private final long checkpointId;
    private final int subtaskId;
    private final long timestamp;               // Wall-clock timestamp of the checkpoint
    private final StreamStateHandle stateHandle; // Handle to the buffered records in state backend
}
```

## 2.3 Record Buffering: processElement()

```java
// GenericWriteAheadSink.java, lines 292-299
@Override
public void processElement(StreamRecord<IN> element) throws Exception {
    IN value = element.getValue();
    // Generate initial operator state stream if needed
    if (out == null) {
        out = checkpointStorage.createTaskOwnedStateStream();
    }
    // Serialize the record into the state stream (the "WAL")
    serializer.serialize(value, new DataOutputViewStreamWrapper(out));
}
```

**Key point**: Records are **NOT sent to Cassandra here**. They are serialized into a `CheckpointStateOutputStream` — essentially appended to a byte stream that will be persisted as part of the checkpoint state.

## 2.4 Checkpoint Barrier: snapshotState()

When a checkpoint barrier arrives:

```java
// GenericWriteAheadSink.java, lines 174-205
@Override
public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);

    // Close the current write buffer and create a PendingCheckpoint
    saveHandleInState(context.getCheckpointId(), context.getCheckpointTimestamp());

    // Persist all pending checkpoints to operator state
    this.checkpointedState.update(new ArrayList<>(pendingCheckpoints));
}

// Lines 151-171
private void saveHandleInState(final long checkpointId, final long timestamp) throws Exception {
    if (out != null) {
        int subtaskIdx = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        StreamStateHandle handle = out.closeAndGetHandle();

        PendingCheckpoint pendingCheckpoint =
                new PendingCheckpoint(checkpointId, subtaskIdx, timestamp, handle);

        if (pendingCheckpoints.contains(pendingCheckpoint)) {
            // Already have a checkpoint for this ID (e.g., from state restore)
            handle.discardState();
        } else {
            pendingCheckpoints.add(pendingCheckpoint);
        }
        out = null;  // Reset for next checkpoint interval
    }
}
```

**Important**: The `timestamp` here is `context.getCheckpointTimestamp()` — the **wall-clock time** when the checkpoint was triggered, NOT event time. This timestamp will later be used as Cassandra's write timestamp for idempotency.

## 2.5 Checkpoint Complete: notifyCheckpointComplete() — THE CORE FLOW

This is where the actual writes to Cassandra happen:

```java
// GenericWriteAheadSink.java, lines 228-277
@Override
public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);

    synchronized (pendingCheckpoints) {
        Iterator<PendingCheckpoint> pendingCheckpointIt = pendingCheckpoints.iterator();
        while (pendingCheckpointIt.hasNext()) {
            PendingCheckpoint pendingCheckpoint = pendingCheckpointIt.next();

            long pastCheckpointId = pendingCheckpoint.checkpointId;
            int subtaskId = pendingCheckpoint.subtaskId;
            long timestamp = pendingCheckpoint.timestamp;
            StreamStateHandle streamHandle = pendingCheckpoint.stateHandle;

            if (pastCheckpointId <= checkpointId) {
                try {
                    // Step 1: Check if this checkpoint was already committed
                    if (!committer.isCheckpointCommitted(subtaskId, pastCheckpointId)) {

                        // Step 2: Deserialize buffered records from state handle
                        try (FSDataInputStream in = streamHandle.openInputStream()) {
                            boolean success = sendValues(
                                    new ReusingMutableToRegularIteratorWrapper<>(
                                            new InputViewIterator<>(
                                                    new DataInputViewStreamWrapper(in),
                                                    serializer),
                                            serializer),
                                    pastCheckpointId,
                                    timestamp);         // ← checkpoint wall-clock timestamp

                            if (success) {
                                // Step 3: Mark checkpoint as committed externally
                                committer.commitCheckpoint(subtaskId, pastCheckpointId);
                                // Step 4: Clean up state
                                streamHandle.discardState();
                                pendingCheckpointIt.remove();
                            }
                            // If sendValues() returns false → retry on next checkpoint complete
                        }
                    } else {
                        // Already committed → clean up
                        streamHandle.discardState();
                        pendingCheckpointIt.remove();
                    }
                } catch (Exception e) {
                    // MUST break to prevent committing a later checkpoint before this one
                    LOG.error("Could not commit checkpoint.", e);
                    break;
                }
            }
        }
    }
}
```

**Critical ordering guarantee**: Pending checkpoints are processed in **ascending order** (TreeSet). If a checkpoint fails to commit, the loop `break`s to prevent later checkpoints from being committed out of order.

## 2.6 Recovery: cleanRestoredHandles()

On restart, the operator restores pending checkpoints from state and cleans up already-committed ones:

```java
// GenericWriteAheadSink.java, lines 93-126
@Override
public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    checkpointedState = context.getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("pending-checkpoints", new JavaSerializer<>()));

    if (context.isRestored()) {
        for (PendingCheckpoint pendingCheckpoint : checkpointedState.get()) {
            this.pendingCheckpoints.add(pendingCheckpoint);
        }
    }
}

// Lines 212-225
private void cleanRestoredHandles() throws Exception {
    synchronized (pendingCheckpoints) {
        Iterator<PendingCheckpoint> pendingCheckpointIt = pendingCheckpoints.iterator();
        while (pendingCheckpointIt.hasNext()) {
            PendingCheckpoint pendingCheckpoint = pendingCheckpointIt.next();

            if (committer.isCheckpointCommitted(
                    pendingCheckpoint.subtaskId, pendingCheckpoint.checkpointId)) {
                // Already committed → discard buffered state
                pendingCheckpoint.stateHandle.discardState();
                pendingCheckpointIt.remove();
            }
        }
    }
}
```

After cleanup, any remaining pending checkpoints will be re-sent to Cassandra on the next `notifyCheckpointComplete()`.

---

# Part 3: CassandraTupleWriteAheadSink

**File**: `CassandraTupleWriteAheadSink.java`

This class extends `GenericWriteAheadSink` and implements the actual Cassandra write logic.

## 3.1 Construction and Open

```java
// CassandraTupleWriteAheadSink.java, lines 49, 62-72
public class CassandraTupleWriteAheadSink<IN extends Tuple> extends GenericWriteAheadSink<IN> {

    protected CassandraTupleWriteAheadSink(
            String insertQuery,
            TypeSerializer<IN> serializer,
            ClusterBuilder builder,
            CheckpointCommitter committer) throws Exception {
        super(committer, serializer, UUID.randomUUID().toString().replace("-", "_"));
        this.insertQuery = insertQuery;
        this.builder = builder;
    }
```

```java
// Lines 74-85
public void open() throws Exception {
    super.open();
    // CRITICAL: WAL requires checkpointing — the entire mechanism depends on it
    if (!getRuntimeContext().isCheckpointingEnabled()) {
        throw new IllegalStateException(
                "The write-ahead log requires checkpointing to be enabled.");
    }
    cluster = builder.getCluster();
    session = cluster.connect();
    preparedStatement = session.prepare(insertQuery);

    fields = new Object[((TupleSerializer<IN>) serializer).getArity()];
}
```

## 3.2 sendValues(): The Core Write Method

This is the only abstract method from `GenericWriteAheadSink` that the Cassandra sink implements. It's called from `notifyCheckpointComplete()` with the buffered records for a given checkpoint.

```java
// CassandraTupleWriteAheadSink.java, lines 107-169
@Override
protected boolean sendValues(Iterable<IN> values, long checkpointId, long timestamp)
        throws Exception {
    // Atomic counters for tracking async completion
    final AtomicInteger updatesCount = new AtomicInteger(0);
    final AtomicInteger updatesConfirmed = new AtomicInteger(0);
    final AtomicReference<Throwable> exception = new AtomicReference<>();

    // Callback registered on each async Cassandra write
    FutureCallback<ResultSet> callback = new FutureCallback<ResultSet>() {
        @Override
        public void onSuccess(ResultSet resultSet) {
            updatesConfirmed.incrementAndGet();
            if (updatesCount.get() > 0) {  // All updates have been sent
                if (updatesCount.get() == updatesConfirmed.get()) {
                    synchronized (updatesConfirmed) {
                        updatesConfirmed.notifyAll();  // Wake up the waiting thread
                    }
                }
            }
        }

        @Override
        public void onFailure(Throwable throwable) {
            if (exception.compareAndSet(null, throwable)) {
                LOG.error("Error while sending value.", throwable);
                synchronized (updatesConfirmed) {
                    updatesConfirmed.notifyAll();  // Wake up on first error
                }
            }
        }
    };

    // Send all buffered values to Cassandra
    int updatesSent = 0;
    for (IN value : values) {
        for (int x = 0; x < value.getArity(); x++) {
            fields[x] = value.getField(x);
        }
        BoundStatement s = preparedStatement.bind(fields);
        // ═══════════════════════════════════════════════════════════════
        // KEY: Set checkpoint wall-clock timestamp as Cassandra write timestamp
        // This ensures idempotent replays (same key + same timestamp = same cell)
        // ═══════════════════════════════════════════════════════════════
        s.setDefaultTimestamp(timestamp);
        ResultSetFuture result = session.executeAsync(s);
        updatesSent++;
        if (result != null) {
            Futures.addCallback(result, callback);
        }
    }
    updatesCount.set(updatesSent);

    // Block until all writes complete OR an error occurs
    synchronized (updatesConfirmed) {
        while (exception.get() == null && updatesSent != updatesConfirmed.get()) {
            updatesConfirmed.wait();
        }
    }

    if (exception.get() != null) {
        LOG.warn("Sending a value failed.", exception.get());
        return false;   // Checkpoint NOT committed → will be retried
    } else {
        return true;    // All writes succeeded → checkpoint will be committed
    }
}
```

---

# Part 4: The CheckpointCommitter Pattern — Deep Dive

**File**: `flink-streaming-java/.../operators/CheckpointCommitter.java`

This is a **general-purpose pattern** applicable to any non-transactional sink (Cassandra, Redis, HBase, Elasticsearch, etc.). Understanding its strengths and limitations is essential for evaluating the Cassandra connector's guarantees.

## 4.1 The Fundamental Problem: Three Impossible Timing Options

When writing to a system that doesn't support rollbacks, there are three places you could flush data — and **all three have problems**:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  OPTION A: Write during invoke() (immediately)                                      │
│  ─────────────────────────────────────────────                                      │
│                                                                                     │
│    record → write to Cassandra → checkpoint barrier → checkpoint succeeds           │
│                                                                                     │
│    Problem: If checkpoint FAILS (another subtask crashes), Flink replays from       │
│    last successful checkpoint. Records already written to Cassandra are              │
│    written AGAIN → DUPLICATES                                                       │
│                                                                                     │
│    Verdict: ❌ At-least-once (this is what non-WAL CassandraSinkBase does)          │
│                                                                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  OPTION B: Write during snapshotState() (at checkpoint barrier)                     │
│  ──────────────────────────────────────────────────────────────                      │
│                                                                                     │
│    record → buffer → checkpoint barrier → write to Cassandra → checkpoint succeeds  │
│                                                                                     │
│    Problem: Subtask 0 writes to Cassandra. Subtask 1 fails during its snapshot.     │
│    Entire job restarts from PREVIOUS checkpoint. Subtask 0's writes are             │
│    already in Cassandra but the checkpoint they belong to was never completed        │
│    → DUPLICATES on replay, and potentially INCONSISTENT data                        │
│                                                                                     │
│    Verdict: ❌ Even worse — partial writes from a failed checkpoint                  │
│                                                                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  OPTION C: Write on notifyCheckpointComplete() (after checkpoint succeeds)          │
│  ─────────────────────────────────────────────────────────────────────────           │
│                                                                                     │
│    record → buffer → checkpoint barrier → checkpoint succeeds → write to Cassandra  │
│                                                                                     │
│    Problem: Write to Cassandra succeeds, then CRASH before we can record that       │
│    fact. On recovery, Flink restores from this checkpoint and replays the write.     │
│    But we have NO KNOWLEDGE of whether it was already written.                      │
│    → Without external tracking: DUPLICATES on replay                                │
│    → With CheckpointCommitter: we CAN check and skip → SAFE (if idempotent)        │
│                                                                                     │
│    Verdict: ✅ Best option, but needs CheckpointCommitter to handle the gap         │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

The WAL pattern chooses **Option C** and adds `CheckpointCommitter` to handle the crash-between-write-and-record gap.

## 4.2 Why Flink's Own State Can't Track This

From the Javadoc (CheckpointCommitter.java, lines 30-44):

```java
/**
 * The current checkpointing mechanism is ill-suited for sinks relying on backends
 * that do not support roll-backs. When dealing with such a system, while trying to
 * get exactly-once semantics, one may neither commit data while creating the snapshot
 * (since another sink instance may fail, leading to a replay on the same data) nor
 * when receiving a checkpoint-complete notification (since a subsequent failure would
 * leave us with no knowledge as to whether data was committed or not).
 *
 * A CheckpointCommitter can be used to solve the second problem by saving whether
 * an instance committed all data belonging to a checkpoint. This data must be stored
 * in a backend that is persistent across retries (which rules out Flink's state
 * mechanism) and accessible from all machines, like a database or distributed file.
 */
```

The key paradox: Flink's state is committed AS PART OF the checkpoint. By the time `notifyCheckpointComplete()` fires, the state is already frozen. Any information written to Flink state during `notifyCheckpointComplete()` would belong to the NEXT checkpoint, not this one. So we can't use Flink state to record "I committed checkpoint N's data to Cassandra" — because that record would only be saved in checkpoint N+1.

```
                     Checkpoint N                        Checkpoint N+1
                         │                                    │
  ───────────────────────┼────────────────────────────────────┼──────
                         │                                    │
  State frozen ──────────┘                                    │
                         │                                    │
  notifyComplete(N) ─────┤                                    │
    │                    │                                    │
    ├─ sendValues() ─────┤  ← Can't write "committed=true"   │
    │                    │    to Flink state here — it's      │
    └─ commitCheckpoint()│    already frozen for checkpoint N  │
         │               │                                    │
         └─ Must write to EXTERNAL storage (Cassandra aux table)
```

## 4.3 Interface Contract

```java
// CheckpointCommitter.java, lines 46-115
public abstract class CheckpointCommitter implements Serializable {
    protected String jobId;
    protected String operatorId;

    // Set by the framework
    public void setJobId(String id) throws Exception { this.jobId = id; }
    public void setOperatorId(String id) throws Exception { this.operatorId = id; }

    // Lifecycle
    public abstract void open() throws Exception;
    public abstract void close() throws Exception;
    public abstract void createResource() throws Exception;  // Create table/file on first use

    // Core contract — only two methods:
    public abstract void commitCheckpoint(int subtaskIdx, long checkpointID) throws Exception;
    public abstract boolean isCheckpointCommitted(int subtaskIdx, long checkpointID) throws Exception;
}
```

## 4.4 Pros of This Pattern

| Advantage | Why It Matters |
|-----------|---------------|
| **Works with ANY non-transactional sink** | Cassandra, Redis, HBase, Elasticsearch — no transaction support required from the external system |
| **Simple to implement** | Only two methods: `commitCheckpoint()` and `isCheckpointCommitted()` |
| **Tiny external state** | One row per subtask storing only the last committed checkpoint ID |
| **Ascending-order invariant** | Since checkpoints are committed in order, only ONE number per subtask is needed (not a set of all committed IDs) |
| **No protocol changes** | External system doesn't need to know about Flink — just regular writes |
| **Idempotent by design** | With proper timestamp tagging, replayed writes produce the same result |

## 4.5 Cons and Limitations of This Pattern

### Limitation 1: NOT Truly Exactly-Once — It's At-Least-Once + Idempotency

The pattern provides **at-least-once delivery with idempotent writes** that LOOK like exactly-once. But the guarantee depends entirely on the write operation being idempotent.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  THE CRITICAL FAILURE WINDOW                                                    │
│  (GenericWriteAheadSink.java, lines 246-261)                                    │
│                                                                                 │
│  notifyCheckpointComplete(checkpointId=5):                                      │
│                                                                                 │
│    if (!committer.isCheckpointCommitted(subtaskId, 5)) {                        │
│        boolean success = sendValues(records, 5, timestamp);  // ← Writes data   │
│        ║                                                                        │
│        ║  ╔═══════════════════════════════════════════════════════════╗          │
│        ║  ║  DANGER ZONE: Data is in Cassandra but NOT marked as    ║          │
│        ║  ║  committed. If CRASH occurs here:                        ║          │
│        ║  ║                                                          ║          │
│        ║  ║  On recovery:                                            ║          │
│        ║  ║    isCheckpointCommitted(5) → FALSE                      ║          │
│        ║  ║    sendValues(same records, 5, same timestamp) → REPLAY  ║          │
│        ║  ║                                                          ║          │
│        ║  ║  Result: Records written TWICE to Cassandra              ║          │
│        ║  ║  → With idempotent INSERT + same timestamp: NO EFFECT    ║          │
│        ║  ║  → With counter update: DOUBLE COUNTING!                 ║          │
│        ║  ╚═══════════════════════════════════════════════════════════╝          │
│        ║                                                                        │
│        if (success) {                                                           │
│            committer.commitCheckpoint(subtaskId, 5);  // ← Marks committed      │
│            streamHandle.discardState();                                          │
│        }                                                                        │
│    }                                                                            │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Limitation 2: Non-Atomic Batch Writes

`sendValues()` writes records one-by-one asynchronously. If the process crashes **during** sendValues():

```
sendValues([r1, r2, r3, r4, r5], checkpointId=5, timestamp=T):

  r1 → Cassandra ✓   (written)
  r2 → Cassandra ✓   (written)
  r3 → Cassandra ✓   (written)
  r4 → CRASH!         ← process dies here
  r5 →                (never sent)

On recovery:
  isCheckpointCommitted(5) → FALSE  (commitCheckpoint was never called)
  sendValues([r1, r2, r3, r4, r5], 5, T) → ALL 5 records replayed

  Result: r1, r2, r3 written TWICE (but same timestamp → idempotent for INSERT)
          r4, r5 written once (correct)
```

This is safe for idempotent writes but means **partial state is visible** between the crash and recovery.

### Limitation 3: Increased Latency

```
                    Without WAL                          With WAL
                    ──────────                           ────────

  Record arrives ──► Immediately sent to ──► Visible    Record arrives ──► Buffered in state
                     Cassandra                                              │
                                                        Checkpoint barrier ──► Still buffered
                                                                              │
                                                        Checkpoint succeeds ──► NOW sent to Cassandra
                                                                                │
                                                        All writes complete ──► commitCheckpoint()
                                                                                │
                                                                             ──► Visible

  Latency: ~milliseconds                    Latency: checkpoint_interval + write_time
                                                     (typically seconds to minutes)
```

### Limitation 4: State Backend Pressure

All records are buffered in the state backend between checkpoints:

```
State size ≈ record_size × records_per_second × checkpoint_interval

Example: 1KB records × 100K records/sec × 60 sec checkpoint interval = 6 GB of state per subtask!
```

This puts pressure on the state backend (RocksDB, heap, etc.) and increases checkpoint size.

### Limitation 5: Non-Idempotent Operations Break the Guarantee

| CQL Operation | Idempotent? | Safe with WAL? |
|---------------|-------------|----------------|
| `INSERT INTO t (k, v) VALUES (?, ?)` | Yes (upsert) | ✅ Safe |
| `UPDATE t SET v = ? WHERE k = ?` | Yes (overwrite) | ✅ Safe |
| `UPDATE t SET counter = counter + 1 WHERE k = ?` | **No** (accumulative) | ❌ **BROKEN** — double counting on replay |
| `UPDATE t SET list = list + [?] WHERE k = ?` | **No** (append) | ❌ **BROKEN** — duplicate list entries |
| `UPDATE t SET set_col = set_col + {?} WHERE k = ?` | Yes (set add) | ✅ Safe (adding same element is idempotent) |
| `DELETE FROM t WHERE k = ?` | Yes | ✅ Safe (deleting twice = same result) |

### Limitation 6: External Committer Availability

The `CassandraCommitter` stores checkpoint metadata in Cassandra itself with `replication_factor=1` by default:

```java
// CassandraCommitter.java, line 84
"CREATE KEYSPACE IF NOT EXISTS %s with replication={'class':'SimpleStrategy', 'replication_factor':1};"
```

If the Cassandra node holding this auxiliary data goes down, recovery cannot determine which checkpoints were committed → potential duplicates. In production, this should be configured with a higher replication factor.

## 4.6 How the Cassandra Connector Overcomes These Limitations

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                     WHY THE WAL PATTERN WORKS WELL FOR CASSANDRA                    │
│                                                                                     │
│  Limitation                    │ How Cassandra Mitigates                             │
│  ──────────────────────────────┼───────────────────────────────────────────────────  │
│                                │                                                    │
│  1. Double-write window        │ INSERT is a natural upsert. Same PK + same         │
│     (crash between send        │ timestamp (via setDefaultTimestamp) = identical     │
│     and commit)                │ cell in Cassandra's SSTable. LWW resolution        │
│                                │ makes replay a no-op.                              │
│                                │                                                    │
│  2. Non-atomic batch           │ Same as above — each individual write is           │
│     (partial sendValues)       │ idempotent. Replaying the full batch just          │
│                                │ overwrites the already-written records with        │
│                                │ identical data + identical timestamps.             │
│                                │                                                    │
│  3. External committer         │ CassandraCommitter stores metadata in Cassandra    │
│     availability               │ itself — no additional infrastructure needed.       │
│                                │ (But replication_factor=1 is a concern)            │
│                                │                                                    │
│  4. Ascending checkpoint       │ Only ONE number per subtask stored. The            │
│     ordering                   │ `isCheckpointCommitted()` check is a single        │
│                                │ comparison: `checkpointId <= lastCommitted`.       │
│                                │                                                    │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### The Timestamp is the Secret Weapon

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  WHY setDefaultTimestamp() MAKES REPLAY SAFE                                        │
│                                                                                     │
│  First attempt (checkpoint 5, timestamp T=1703000000):                              │
│                                                                                     │
│    INSERT INTO orders (id, amount) VALUES ('order-42', 100)                         │
│    WITH TIMESTAMP 1703000000                                                        │
│                                                                                     │
│    Cassandra SSTable cell:                                                          │
│    ┌────────────┬─────────┬───────────────┐                                         │
│    │ id=order-42│amount=100│ ts=1703000000 │                                        │
│    └────────────┴─────────┴───────────────┘                                         │
│                                                                                     │
│  ═══ CRASH after sendValues(), before commitCheckpoint() ═══                        │
│                                                                                     │
│  Replay (same checkpoint 5, same timestamp T=1703000000):                           │
│                                                                                     │
│    INSERT INTO orders (id, amount) VALUES ('order-42', 100)                         │
│    WITH TIMESTAMP 1703000000                                                        │
│                                                                                     │
│    Cassandra SSTable cell (SAME cell, SAME timestamp):                              │
│    ┌────────────┬─────────┬───────────────┐                                         │
│    │ id=order-42│amount=100│ ts=1703000000 │  ← identical, no duplicate effect      │
│    └────────────┴─────────┴───────────────┘                                         │
│                                                                                     │
│  Without setDefaultTimestamp() (server timestamp):                                  │
│                                                                                     │
│    First attempt:  ts = 1703000000000 (server clock at attempt 1)                   │
│    Replay:         ts = 1703000005000 (server clock at attempt 2, 5s later)         │
│                                                                                     │
│    → Two DIFFERENT cells in Cassandra! The replay creates a new version.            │
│    → For simple INSERT this is still idempotent (same value overwrites)             │
│    → But for LWW conflicts with concurrent writers, having two timestamps           │
│      can cause subtle ordering issues.                                              │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## 4.7 Comparison: WAL+CheckpointCommitter vs Kafka 2PC

```
┌──────────────────────────────────────┬──────────────────────────────────────────────┐
│  WAL + CheckpointCommitter           │  Kafka 2PC (TwoPhaseCommittingSink)          │
│  (Cassandra connector)               │  (Kafka connector)                           │
├──────────────────────────────────────┼──────────────────────────────────────────────┤
│                                      │                                              │
│  Guarantee: At-least-once +          │  Guarantee: True exactly-once                │
│  idempotent writes                   │  (transaction isolation)                     │
│                                      │                                              │
│  Requires: Idempotent operations     │  Requires: Kafka transactions                │
│  + deterministic replay              │  (isolation.level=read_committed)             │
│                                      │                                              │
│  Buffering: In Flink state backend   │  Buffering: In Kafka (uncommitted txn)       │
│  (high state pressure)               │  (low state pressure)                        │
│                                      │                                              │
│  Visibility: After checkpoint        │  Visibility: After checkpoint                │
│  completes + sendValues finishes     │  completes + commitTransaction               │
│                                      │                                              │
│  Failure recovery: Replay from       │  Failure recovery: Abort uncommitted         │
│  state, skip committed checkpoints   │  transactions, fence old producers           │
│                                      │                                              │
│  External metadata: Yes              │  External metadata: No (Kafka tracks         │
│  (CheckpointCommitter table)         │  transactions internally)                    │
│                                      │                                              │
│  Rollback possible: No               │  Rollback possible: Yes                      │
│  (write is permanent)                │  (abortTransaction)                          │
│                                      │                                              │
│  Partial writes on crash: Yes        │  Partial writes on crash: No                 │
│  (visible to readers)                │  (invisible until commit)                    │
│                                      │                                              │
│  Complexity: Low                     │  Complexity: High                            │
│  (2 methods to implement)            │  (transaction lifecycle, fencing, etc.)       │
│                                      │                                              │
└──────────────────────────────────────┴──────────────────────────────────────────────┘
```

## 4.8 Known Issue: FLINK-4502

[FLINK-4502](https://issues.apache.org/jira/browse/FLINK-4502) flagged that the Cassandra connector documentation's claim of "exactly-once" was misleading. The original docs stated:

> `enableWriteAheadLog()` is an optional method, that allows exactly-once processing for non-deterministic algorithms.

This was corrected because:
1. The writes to Cassandra in `notifyCheckpointComplete()` are **NOT atomic**
2. The crash-between-send-and-commit window means records **CAN be duplicated**
3. The guarantee only holds if the CQL operation is **idempotent**

The current documentation more accurately states:
> Flink can provide exactly-once guarantees if the query is idempotent (meaning it can be applied multiple times without changing the result) and checkpointing is enabled.

---

# Part 5: CassandraCommitter

**File**: `CassandraCommitter.java`

The `CassandraCommitter` stores checkpoint commit metadata in a **separate Cassandra table**.

## 5.1 Table Schema

```java
// CassandraCommitter.java, lines 78-89
@Override
public void createResource() throws Exception {
    cluster = builder.getCluster();
    session = cluster.connect();

    // Create a dedicated keyspace for Flink auxiliary data
    session.execute(String.format(
            "CREATE KEYSPACE IF NOT EXISTS %s with replication="
            + "{'class':'SimpleStrategy', 'replication_factor':1};",
            keySpace));   // Default: "flink_auxiliary"

    // Create the checkpoint tracking table
    // Table name = "checkpoints_" + jobId
    session.execute(String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s "
            + "(sink_id text, sub_id int, checkpoint_id bigint, "
            + "PRIMARY KEY (sink_id, sub_id));",
            keySpace, table));
}
```

**Schema**: `flink_auxiliary.checkpoints_<jobId>`

| Column | Type | Description |
|--------|------|-------------|
| `sink_id` | text | Unique operator ID (UUID) |
| `sub_id` | int | Subtask index |
| `checkpoint_id` | bigint | Last committed checkpoint ID |

**Primary Key**: `(sink_id, sub_id)` — one row per operator instance per subtask.

## 5.2 Committing a Checkpoint

```java
// CassandraCommitter.java, lines 128-136
@Override
public void commitCheckpoint(int subtaskIdx, long checkpointId) {
    String statement = String.format(
            "UPDATE %s.%s set checkpoint_id=%d where sink_id='%s' and sub_id=%d;",
            keySpace, table, checkpointId, operatorId, subtaskIdx);

    session.execute(statement);
    lastCommittedCheckpoints.put(subtaskIdx, checkpointId);  // Update cache
}
```

This is a simple CQL UPDATE that sets the `checkpoint_id` column. Since checkpoints are committed in ascending order, only the latest ID needs to be stored.

## 5.3 Checking If a Checkpoint Was Committed

```java
// CassandraCommitter.java, lines 139-159
@Override
public boolean isCheckpointCommitted(int subtaskIdx, long checkpointId) {
    // Optimization: check in-memory cache first
    Long lastCommittedCheckpoint = lastCommittedCheckpoints.get(subtaskIdx);

    if (lastCommittedCheckpoint == null) {
        // Cache miss: query Cassandra
        String statement = String.format(
                "SELECT checkpoint_id FROM %s.%s where sink_id='%s' and sub_id=%d;",
                keySpace, table, operatorId, subtaskIdx);

        Iterator<Row> resultIt = session.execute(statement).iterator();
        if (resultIt.hasNext()) {
            lastCommittedCheckpoint = resultIt.next().getLong("checkpoint_id");
            lastCommittedCheckpoints.put(subtaskIdx, lastCommittedCheckpoint);
        }
    }
    // Checkpoints are committed in ascending order, so:
    // if checkpointId <= lastCommitted, it was already committed
    return lastCommittedCheckpoint != null && checkpointId <= lastCommittedCheckpoint;
}
```

**The ascending-order invariant** (from the comment on line 140-143):
> Pending checkpointed buffers are committed in ascending order of their checkpoint id.
> This way we can tell if a checkpointed buffer was committed just by asking the
> third-party storage system for the last checkpoint id committed by the specified subtask.

This means we only need to store **one number per subtask** — the last committed checkpoint ID — rather than a set of all committed IDs.

---

# Part 6: Why WAL + Idempotent Writes = Exactly-Once

## 6.1 The Problem With Non-Rollback Sinks

Unlike Kafka (which supports transactions), Cassandra cannot "undo" a write. Once data is written, it's permanent. This creates a challenge:

1. **Can't write during checkpoint**: If we write to Cassandra while taking a checkpoint and then another subtask fails, the entire job restarts from the checkpoint. But the data we already wrote to Cassandra is still there → **duplicates**.

2. **Can't write on checkpoint-complete and then crash**: If we write to Cassandra when notified that a checkpoint completed, and then crash immediately after, on recovery we don't know if the writes happened → potential **duplicates or data loss**.

## 6.2 The WAL Solution

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          WAL Exactly-Once Flow                              │
│                                                                             │
│  PHASE 1: Buffer                                                            │
│  ─────────────────                                                          │
│  Records arrive → serialized into CheckpointStateOutputStream               │
│                   (NOT sent to Cassandra)                                    │
│                                                                             │
│  PHASE 2: Checkpoint Barrier                                                │
│  ─────────────────────────                                                  │
│  Barrier arrives → close buffer → create PendingCheckpoint(id, timestamp,   │
│                    stateHandle) → persist in operator state                  │
│                                                                             │
│  PHASE 3: Checkpoint Completes (notifyCheckpointComplete)                   │
│  ──────────────────────────────────────────────────────────                  │
│  For each pending checkpoint (in ascending order):                          │
│    a. Is it already committed? (ask CassandraCommitter)                     │
│       YES → discard state, remove from pending list                         │
│       NO  → continue to (b)                                                 │
│    b. Deserialize records from stateHandle                                  │
│    c. sendValues(records, checkpointId, timestamp)                          │
│       → Async writes to Cassandra with setDefaultTimestamp(timestamp)        │
│       → Block until all callbacks complete                                  │
│    d. If all succeeded:                                                     │
│       → CassandraCommitter.commitCheckpoint(subtaskIdx, checkpointId)       │
│       → Discard state handle, remove from pending                           │
│    e. If any failed:                                                        │
│       → Return false, retry on next checkpoint-complete                     │
│                                                                             │
│  PHASE 4: Recovery (if crash occurs)                                        │
│  ────────────────────────────────────                                        │
│  Restore pending checkpoints from operator state                            │
│  cleanRestoredHandles():                                                    │
│    → For each pending: isCheckpointCommitted()?                             │
│      YES → discard (already in Cassandra)                                   │
│      NO  → keep for replay                                                  │
│  Next notifyCheckpointComplete() replays uncommitted buffers                │
│    → Same timestamp ensures idempotent writes                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 6.3 The Timestamp: Key to Idempotency

```java
// CassandraTupleWriteAheadSink.java, line 147
s.setDefaultTimestamp(timestamp);
```

This single line is what makes replay safe. Here's why:

### Cassandra's Last-Write-Wins (LWW) Semantics

In Cassandra, each cell (column value) has an associated **write timestamp** (microseconds). When two writes target the same cell:
- If the timestamps differ → the cell with the **higher timestamp wins**
- If the timestamps are **identical** → the writes are **idempotent** (same data, same timestamp = same cell)

### How This Ensures Exactly-Once

1. **First attempt**: Records are written with `timestamp = checkpoint wall-clock time`
2. **Crash and replay**: The same records are written again with the **same timestamp** (from the PendingCheckpoint stored in state)
3. **Cassandra sees**: Same key + same column + same timestamp = identical cell → no duplicate effect

This works because:
- CQL `INSERT` is an upsert (same primary key overwrites)
- `setDefaultTimestamp()` ensures Cassandra uses the Flink-provided timestamp, not the server clock
- The timestamp is deterministic per checkpoint (stored in PendingCheckpoint)

### Caveat: Requires Idempotent Queries

This only works for **upsert-style** CQL statements (e.g., `INSERT INTO ... VALUES (?, ?)`). Counter updates (`UPDATE ... SET counter = counter + 1`) are NOT idempotent and will produce wrong results on replay.

---

# Part 7: Non-WAL Sinks (At-Least-Once)

**File**: `CassandraSinkBase.java`

The non-WAL sinks provide **at-least-once** semantics with lower latency.

## 7.1 Architecture

```java
// CassandraSinkBase.java, lines 62-63
public abstract class CassandraSinkBase<IN, V> extends RichSinkFunction<IN>
        implements CheckpointedFunction {

    private Semaphore semaphore;              // Controls concurrent async requests
    private AtomicReference<Throwable> throwable;  // Captures first async error
    private FutureCallback<V> callback;       // Releases semaphore on completion
```

## 7.2 Record Writing: invoke()

Records are sent to Cassandra **immediately** (not buffered):

```java
// CassandraSinkBase.java, lines 146-157
@Override
public void invoke(IN value) throws Exception {
    checkAsyncErrors();            // Throw if a previous async write failed
    tryAcquire(1);                 // Acquire semaphore permit (blocks if at max concurrency)
    final ListenableFuture<V> result;
    try {
        result = send(value);      // Abstract: execute async CQL write
    } catch (Throwable e) {
        semaphore.release();       // Release permit on sync error
        throw e;
    }
    Futures.addCallback(result, callback);  // Callback releases permit on async completion
}
```

## 7.3 Checkpoint: Flush All Pending Writes

```java
// CassandraSinkBase.java, lines 139-143
@Override
public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
    checkAsyncErrors();
    flush();                       // Wait for all in-flight writes to complete
    checkAsyncErrors();
}

// Lines 180-183
private void flush() throws InterruptedException, TimeoutException {
    tryAcquire(config.getMaxConcurrentRequests());   // Acquire ALL permits (blocks until all released)
    semaphore.release(config.getMaxConcurrentRequests()); // Release them all
}
```

**How flush works**: By acquiring all `maxConcurrentRequests` permits, the flush blocks until every in-flight async write has completed and released its permit. Then all permits are released for the next checkpoint interval.

## 7.4 Callback: Semaphore Release

```java
// CassandraSinkBase.java, lines 90-103
this.callback = new FutureCallback<V>() {
    @Override
    public void onSuccess(V ignored) {
        semaphore.release();       // Release permit on success
    }

    @Override
    public void onFailure(Throwable t) {
        throwable.compareAndSet(null, t);  // Capture first error
        log.error("Error while sending value.", t);
        semaphore.release();       // Release permit even on failure
    }
};
```

## 7.5 Why Only At-Least-Once

After a failure, Flink replays records from the last checkpoint. Since the non-WAL sink writes records immediately on `invoke()`, some records may have been written to Cassandra before the checkpoint completed. These records will be written again on replay → **duplicates**.

For many Cassandra use cases this is acceptable because CQL `INSERT` is an upsert — writing the same row twice with the same primary key just overwrites. But unlike the WAL sink, the **timestamp is not controlled**, so the replay write may have a different server timestamp.

---

# Part 8: Watermark and Event Timestamp Management

## 8.1 Key Finding: No Built-in Watermark Support

The Cassandra connector provides **no built-in watermark generation or event timestamp extraction**. This is entirely the user's responsibility.

### Source Side (CassandraSource — FLIP-27)

The `CassandraSource` is a **bounded** source (batch). It does not emit watermarks:

```java
// CassandraSource.java
@Override
public Boundedness getBoundedness() {
    return Boundedness.BOUNDED;
}
```

The `CassandraRowEmitter` emits records without timestamps:
```java
// CassandraRowEmitter.java
public void emitRecord(CassandraRow element, SourceOutput<OUT> output, CassandraSplit splitState) {
    OUT record = converter.convert(element);
    output.collect(record);  // No timestamp — no output.collect(record, timestamp)
}
```

Users must provide a `WatermarkStrategy` when creating the stream:
```java
DataStream<Pojo> stream = env.fromSource(
        cassandraSource,
        WatermarkStrategy.noWatermarks(),  // User-provided
        "CassandraSource");
```

### Sink Side

The sinks do not interact with watermarks at all. The only "timestamp" involved is:

| Sink Type | Timestamp Used | Source of Timestamp |
|-----------|---------------|---------------------|
| WAL sinks | `setDefaultTimestamp(timestamp)` | **Checkpoint wall-clock time** (`StateSnapshotContext.getCheckpointTimestamp()`) |
| Non-WAL sinks | None | Cassandra uses server-side timestamp (coordinator clock) |
| POJO sink | Optional via `MapperOptions` | User-configured |

## 8.2 The WAL Timestamp Is NOT Event Time

A common misconception: the `timestamp` parameter in `sendValues(Iterable<IN> values, long checkpointId, long timestamp)` is **NOT** the event timestamp from `StreamRecord.getTimestamp()`.

It comes from `GenericWriteAheadSink.snapshotState()`:
```java
// GenericWriteAheadSink.java, line 181
saveHandleInState(context.getCheckpointId(), context.getCheckpointTimestamp());
//                                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//                                            Wall-clock time of checkpoint trigger
```

This is stored in `PendingCheckpoint.timestamp` and later passed to `sendValues()`. Its purpose is purely for **Cassandra write conflict resolution** (ensuring idempotent replays), not for event-time semantics.

## 8.3 How to Add Event Time to Cassandra Writes

If you need event-time timestamps in Cassandra, you must handle it at the application level:

```java
// Option 1: Include event time as a column in your CQL INSERT
CassandraSink.addSink(stream)
    .setQuery("INSERT INTO keyspace.table (id, event_time, value) VALUES (?, ?, ?);")
    .enableWriteAheadLog()
    .setClusterBuilder(...)
    .build();
// Where event_time is a field in your Tuple/Row

// Option 2: For POJO sink with custom timestamp (non-WAL only)
CassandraSink.addSink(stream)
    .setMapperOptions(() -> new Mapper.Option[] {
        Mapper.Option.timestamp(myTimestamp)
    })
    .setClusterBuilder(...)
    .build();
```

---

# Part 9: Complete Checkpoint Cycle Timeline

```
TIME ──────────────────────────────────────────────────────────────────────────►

Incoming Records:
  │ record1, record2, record3          record4, record5          record6
  │        │                                │                        │
  ▼        ▼                                ▼                        ▼
┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────┐
│ Checkpoint Interval 1│    │ Checkpoint Interval 2│    │ Interval 3       │
│                      │    │                      │    │                  │
│ processElement():    │    │ processElement():    │    │                  │
│   serialize r1,r2,r3 │    │   serialize r4,r5   │    │                  │
│   into state stream  │    │   into state stream  │    │                  │
│                      │    │                      │    │                  │
│   ┌─────────┐        │    │   ┌─────────┐        │    │                  │
│   │WAL:r1,r2│        │    │   │WAL:r4,r5│        │    │                  │
│   │     ,r3 │        │    │   │         │        │    │                  │
│   └─────────┘        │    │   └─────────┘        │    │                  │
└──────────┬───────────┘    └──────────┬───────────┘    └──────────────────┘
           │                           │
     Checkpoint 1                Checkpoint 2
     Barrier arrives             Barrier arrives
           │                           │
     snapshotState():            snapshotState():
       PendingCheckpoint(          PendingCheckpoint(
         id=1, ts=T1,                id=2, ts=T2,
         handle→[r1,r2,r3])          handle→[r4,r5])
           │                           │
           ▼                           ▼
     Checkpoint 1 completed      Checkpoint 2 completed
           │                           │
     notifyCheckpointComplete(1)  notifyCheckpointComplete(2)
           │                           │
           ▼                           ▼
     isCommitted(1)? NO           isCommitted(1)? YES (skip)
     sendValues([r1,r2,r3],1,T1)  isCommitted(2)? NO
       → INSERT r1 ts=T1          sendValues([r4,r5],2,T2)
       → INSERT r2 ts=T1            → INSERT r4 ts=T2
       → INSERT r3 ts=T1            → INSERT r5 ts=T2
       → all success                 → all success
     commitCheckpoint(1)          commitCheckpoint(2)
       → UPDATE aux table            → UPDATE aux table
         checkpoint_id=1                checkpoint_id=2
           │
           │  ╔═══════════════════╗
           │  ║ CRASH HERE!       ║
           │  ║                   ║
           │  ║ Recovery:         ║
           │  ║ Restore pending   ║
           │  ║ checkpoints       ║
           │  ║ from state        ║
           │  ║                   ║
           │  ║ cleanRestored():  ║
           │  ║  CP1 committed?   ║
           │  ║  YES → discard    ║
           │  ║  CP2 committed?   ║
           │  ║  NO → keep        ║
           │  ║                   ║
           │  ║ Next checkpoint:  ║
           │  ║  Replay CP2       ║
           │  ║  Same ts=T2       ║
           │  ║  → idempotent!    ║
           │  ╚═══════════════════╝
```

---

# Key File Reference

| File | Location | Purpose |
|------|----------|---------|
| `GenericWriteAheadSink.java` | `flink-streaming-java/.../operators/` | Core WAL framework: buffering, checkpoint, replay |
| `CheckpointCommitter.java` | `flink-streaming-java/.../operators/` | Interface for external commit tracking |
| `CassandraTupleWriteAheadSink.java` | `streaming/connectors/cassandra/` | WAL sink for Tuple types |
| `CassandraRowWriteAheadSink.java` | `streaming/connectors/cassandra/` | WAL sink for Row types |
| `CassandraCommitter.java` | `streaming/connectors/cassandra/` | Stores commit metadata in Cassandra auxiliary table |
| `CassandraSinkBase.java` | `streaming/connectors/cassandra/` | Base for non-WAL sinks (at-least-once) |
| `CassandraTupleSink.java` | `streaming/connectors/cassandra/` | Non-WAL tuple sink |
| `CassandraRowSink.java` | `streaming/connectors/cassandra/` | Non-WAL row sink |
| `CassandraPojoSink.java` | `streaming/connectors/cassandra/` | Non-WAL POJO sink (no WAL support) |
| `CassandraSink.java` | `streaming/connectors/cassandra/` | Unified builder factory |
| `AbstractCassandraTupleSink.java` | `streaming/connectors/cassandra/` | Base for tuple/row non-WAL sinks |
| `CassandraSource.java` | `connector/cassandra/source/` | Bounded source (no watermarks) |
