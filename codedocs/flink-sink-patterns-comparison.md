# Flink Sink Patterns: Delivery Guarantees, Buffering, and Memory Comparison

This document compares the major sink patterns available in Apache Flink for achieving at-least-once or exactly-once delivery, with special focus on memory/state usage, MySQL/JDBC recommendations, and when to use each pattern.

---

# Part 1: The 5 Major Sink Patterns

## Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           FLINK SINK PATTERN TAXONOMY                                   │
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐  │
│  │                        AT-LEAST-ONCE PATTERNS                                     │  │
│  │                                                                                   │  │
│  │  ┌─────────────────────┐  ┌─────────────────────┐  ┌──────────────────────────┐  │  │
│  │  │  Pattern A:          │  │  Pattern B:          │  │  Pattern E:              │  │  │
│  │  │  Buffer + Batch      │  │  WAL + Committer     │  │  Async Sink Base         │  │  │
│  │  │  Flush               │  │                      │  │  (FLIP-171)              │  │  │
│  │  │                      │  │  GenericWrite-        │  │                          │  │  │
│  │  │  JdbcSink.sink()     │  │  AheadSink +         │  │  AsyncSinkBase           │  │  │
│  │  │  CassandraSinkBase   │  │  CheckpointCommitter │  │  (Kinesis, DynamoDB,     │  │  │
│  │  │                      │  │  (Cassandra WAL)     │  │   Firehose, etc.)        │  │  │
│  │  │  + upsert =          │  │  + idempotent =      │  │                          │  │  │
│  │  │  effective E2E EOS   │  │  effective E2E EOS   │  │  at-least-once only      │  │  │
│  │  └─────────────────────┘  └─────────────────────┘  └──────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐  │
│  │                        EXACTLY-ONCE PATTERNS                                      │  │
│  │                                                                                   │  │
│  │  ┌─────────────────────────────────┐  ┌─────────────────────────────────────┐    │  │
│  │  │  Pattern C:                      │  │  Pattern D:                         │    │  │
│  │  │  2PC with XA Transactions        │  │  2PC with Native Transactions       │    │  │
│  │  │                                  │  │                                     │    │  │
│  │  │  JdbcXaSinkFunction              │  │  TwoPhaseCommittingSink             │    │  │
│  │  │  (MySQL, PostgreSQL via XA)      │  │  (Kafka transactions)               │    │  │
│  │  │                                  │  │                                     │    │  │
│  │  │  True exactly-once via           │  │  True exactly-once via              │    │  │
│  │  │  DB transaction isolation        │  │  Kafka transaction isolation        │    │  │
│  │  └─────────────────────────────────┘  └─────────────────────────────────────┘    │  │
│  └───────────────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Pattern A: Buffer + Batch Flush (JDBC Default, Most Common)

**Examples**: `JdbcSink.sink()`, `CassandraSinkBase`, StarRocks connector, many custom sinks
**Guarantee**: At-least-once (effectively exactly-once with upsert/idempotent operations)

### How It Works

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  PATTERN A: Buffer + Batch Flush                                                 │
│                                                                                  │
│  TIME ─────────────────────────────────────────────────────────────────────────► │
│                                                                                  │
│  Records:  r1  r2  r3  r4  r5  r6 │ r7  r8  r9  r10  r11  r12 │ r13 ...       │
│            │   │   │   │   │   │  │  │   │   │    │    │    │  │               │
│            ▼   ▼   ▼   ▼   ▼   ▼  │  ▼   ▼   ▼    ▼    ▼    ▼  │               │
│           ┌────────────────────┐  │ ┌────────────────────────┐  │               │
│           │  In-Memory Buffer  │  │ │  In-Memory Buffer      │  │               │
│           │  [r1,r2,r3,r4,r5] │  │ │  [r7,r8,r9,r10,r11]   │  │               │
│           └────────┬───────────┘  │ └────────┬───────────────┘  │               │
│                    │              │          │                   │               │
│        batch_size=5 reached      │  checkpoint barrier          │               │
│                    │              │          │                   │               │
│                    ▼              │          ▼                   │               │
│           executeBatch()          │  flush() → executeBatch()    │               │
│           [r1..r5] → DB ✓        │  [r7..r11] → DB ✓           │               │
│                                  │          │                   │               │
│               r6 still in buffer─┤  snapshotState() completes   │               │
│                    │              │          │                   │               │
│        checkpoint barrier ────────┤  checkpoint N+1 starts       │               │
│                    │              │                              │               │
│               flush() ────────────┤                              │               │
│               [r6] → DB ✓        │                              │               │
│                                  │                              │               │
│  ═══ IF CRASH AFTER r6 FLUSHED BUT BEFORE CHECKPOINT N ═══      │               │
│                                                                  │               │
│  Recovery: Replay from checkpoint N-1                            │               │
│  r6 sent AGAIN to DB → duplicate!                                │               │
│  With upsert (ON DUPLICATE KEY UPDATE): overwrite → no effect    │               │
│                                                                  │               │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Key Characteristics

```java
// Simplified JdbcSink at-least-once pattern (conceptual):
class JdbcBatchSink implements CheckpointedFunction {
    private List<Row> buffer = new ArrayList<>();  // In-memory, NOT in Flink state
    private int batchSize = 5000;

    void invoke(Row value) {
        buffer.add(value);
        if (buffer.size() >= batchSize) {
            flush();  // executeBatch() to DB
        }
    }

    void snapshotState(FunctionSnapshotContext ctx) {
        flush();  // Flush ALL remaining records before checkpoint completes
        // No Flink state written — buffer is now empty
    }

    void flush() {
        PreparedStatement ps = connection.prepareStatement(sql);
        for (Row r : buffer) {
            ps.setXxx(...);
            ps.addBatch();
        }
        ps.executeBatch();  // Single JDBC batch call
        buffer.clear();
    }
}
```

### Memory: Only the in-memory buffer (no Flink state)

```
Memory usage = buffer.size() × record_size
             = min(batch_size, records_since_last_flush) × record_size

Typical: batch_size=5000, record_size=200B → buffer ≈ 1MB per subtask
Flink state: 0 bytes (nothing checkpointed)
```

---

## Pattern B: Write-Ahead Log (WAL) + CheckpointCommitter

**Examples**: `CassandraTupleWriteAheadSink`, `CassandraRowWriteAheadSink`
**Guarantee**: At-least-once + idempotent writes (effective exactly-once for idempotent operations)

### How It Works

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  PATTERN B: WAL (Write-Ahead Log)                                                │
│                                                                                  │
│  TIME ─────────────────────────────────────────────────────────────────────────► │
│                                                                                  │
│  Records:  r1  r2  r3  r4  r5  r6 │ r7  r8  r9  r10 ...                        │
│            │   │   │   │   │   │  │  │   │   │                                  │
│            ▼   ▼   ▼   ▼   ▼   ▼  │  ▼   ▼   ▼                                  │
│           ┌─────────────────────┐ │ ┌───────────────┐                            │
│           │  State Backend      │ │ │ State Backend  │                            │
│           │  (serialized bytes) │ │ │ (new buffer)   │                            │
│           │  [r1,r2,r3,r4,r5,  │ │ │ [r7,r8,r9,r10]│                            │
│           │   r6]               │ │ │                │                            │
│           └────────┬────────────┘ │ └────────────────┘                            │
│                    │              │                                               │
│        checkpoint barrier ────────┤  Records NOT sent to DB yet!                  │
│                    │              │                                               │
│        snapshotState():           │                                               │
│          close buffer             │                                               │
│          PendingCheckpoint(N,     │                                               │
│            handle→[r1..r6])       │                                               │
│                    │              │                                               │
│        ... checkpoint persisted...│                                               │
│                    │              │                                               │
│        notifyCheckpointComplete(N)│                                               │
│                    │              │                                               │
│        sendValues([r1..r6]) ──────┤──► Cassandra (with timestamp=T)               │
│                    │              │                                               │
│        commitCheckpoint(N) ───────┤──► auxiliary table (sink_id, sub_id, cp_id=N) │
│                    │              │                                               │
│        Data finally visible! ─────┤                                               │
│                                   │                                               │
│  Latency: checkpoint_interval + write_time (seconds to minutes)                   │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Memory: ALL records stored in Flink state backend

```
State size = record_size × throughput × checkpoint_interval

Example: 200B records × 100K/sec × 60s checkpoint = 12 GB state per subtask!

This is the MOST expensive pattern for state/memory.
```

---

## Pattern C: 2PC with XA Transactions (JDBC XA)

**Examples**: `JdbcXaSinkFunction` (`JdbcSink.exactlyOnceSink()`)
**Guarantee**: True exactly-once (via database transaction isolation)

### How It Works

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  PATTERN C: 2PC with XA Transactions                                             │
│                                                                                  │
│  TIME ─────────────────────────────────────────────────────────────────────────► │
│                                                                                  │
│  Records:  r1  r2  r3  r4  r5  r6 │ r7  r8  r9  r10 ...                        │
│            │   │   │   │   │   │  │  │   │   │                                  │
│            ▼   ▼   ▼   ▼   ▼   ▼  │  ▼   ▼   ▼                                  │
│           ┌─────────────────────┐ │ ┌───────────────┐                            │
│           │  XA Transaction T1  │ │ │ XA Txn T2     │                            │
│           │  (on MySQL)         │ │ │ (new txn)     │                            │
│           │                     │ │ │               │                            │
│           │  INSERT r1 ✓        │ │ │ INSERT r7 ✓   │                            │
│           │  INSERT r2 ✓        │ │ │ INSERT r8 ✓   │                            │
│           │  INSERT r3 ✓        │ │ │ ...           │                            │
│           │  INSERT r4 ✓        │ │ │               │                            │
│           │  INSERT r5 ✓        │ │ │               │                            │
│           │  INSERT r6 ✓        │ │ │               │                            │
│           │                     │ │ │               │                            │
│           │  (data invisible    │ │ │               │                            │
│           │   to other readers  │ │ │               │                            │
│           │   — held by txn     │ │ │               │                            │
│           │   lock/MVCC)        │ │ │               │                            │
│           └────────┬────────────┘ │ └───────────────┘                            │
│                    │              │                                               │
│        checkpoint barrier ────────┤                                               │
│                    │              │                                               │
│        XA PREPARE (precommit)     │  ← DB acknowledges transaction is             │
│                    │              │    prepared and WILL succeed on commit          │
│                    │              │                                               │
│        ... checkpoint persisted...│  (only Flink state: xid, not record data)     │
│                    │              │                                               │
│        notifyCheckpointComplete(N)│                                               │
│                    │              │                                               │
│        XA COMMIT ─────────────────┤──► Data becomes visible to readers!            │
│                    │              │                                               │
│        No duplicate possible ─────┤  (DB guarantees atomicity)                    │
│                                   │                                               │
│  ═══ IF CRASH AFTER PREPARE BUT BEFORE COMMIT ═══                                │
│                                                                                  │
│  Recovery: XA RECOVER lists prepared transactions                                 │
│            XA COMMIT on recovered transactions                                    │
│            → Data committed exactly once (DB holds the prepared state)             │
│                                                                                  │
│  ═══ IF CRASH BEFORE PREPARE ═══                                                  │
│                                                                                  │
│  Recovery: XA ROLLBACK (transaction was never prepared)                            │
│            Flink replays from last checkpoint → fresh XA transaction               │
│            → Data written exactly once                                             │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Memory: Minimal Flink state (transaction metadata only)

```
State size = XA transaction ID (xid) + metadata ≈ a few hundred bytes per subtask

Record data is in the DATABASE's transaction log (undo/redo log), NOT in Flink state.
This is the MOST efficient pattern for Flink state/memory.
```

### MySQL/PostgreSQL Configuration

```sql
-- MySQL: Grant XA recovery permission
GRANT XA_RECOVER_ADMIN ON *.* TO 'flink_user'@'%';

-- PostgreSQL: Enable prepared transactions
ALTER SYSTEM SET max_prepared_transactions = 100;
```

---

## Pattern D: Kafka-Style 2PC (TwoPhaseCommittingSink)

**Examples**: `KafkaSink` with `DeliveryGuarantee.EXACTLY_ONCE`
**Guarantee**: True exactly-once (via Kafka producer transactions)

### How It Works

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  PATTERN D: Kafka 2PC (TwoPhaseCommittingSink)                                   │
│                                                                                  │
│  Records:  r1  r2  r3  r4  r5  r6 │ r7  r8  r9 ...                             │
│            │   │   │   │   │   │  │  │   │   │                                  │
│            ▼   ▼   ▼   ▼   ▼   ▼  │  ▼   ▼   ▼                                  │
│           ┌─────────────────────┐ │ ┌───────────────┐                            │
│           │  Kafka Transaction  │ │ │ New Kafka Txn │                            │
│           │  (producerId+epoch) │ │ │               │                            │
│           │  send(r1..r6)       │ │ │ send(r7..r9)  │                            │
│           │  (invisible with    │ │ │               │                            │
│           │   read_committed)   │ │ │               │                            │
│           └────────┬────────────┘ │ └───────────────┘                            │
│                    │              │                                               │
│        checkpoint barrier ────────┤                                               │
│        prepareCommit():           │                                               │
│          KafkaCommittable(        │                                               │
│            producerId, epoch,     │                                               │
│            transactionalId)       │  ← only metadata in Flink state               │
│        precommitTransaction()     │                                               │
│                    │              │                                               │
│        notifyCheckpointComplete(N)│                                               │
│        KafkaCommitter.commit():   │                                               │
│          commitTransaction() ─────┤──► Data visible to read_committed consumers    │
│        backchannel → recycle      │                                               │
│                                   │                                               │
│  Recovery: abortTransaction() for uncommitted txns                                │
│            producerId+epoch fencing prevents zombie producers                      │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Memory: Transaction metadata only

```
State size = KafkaCommittable(producerId, epoch, txnId) + KafkaWriterState
           ≈ a few hundred bytes per subtask

Record data is in Kafka's transaction log, NOT in Flink state.
```

---

## Pattern E: Async Sink Base (FLIP-171)

**Examples**: AWS Kinesis Data Streams, DynamoDB, Firehose, custom API sinks
**Guarantee**: At-least-once

### How It Works

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  PATTERN E: Async Sink Base (FLIP-171)                                           │
│                                                                                  │
│  Records:  r1  r2  r3  r4  r5 │ r6  r7  r8  r9  r10 │ ...                      │
│            │   │   │   │   │  │  │   │   │   │    │  │                          │
│            ▼   ▼   ▼   ▼   ▼  │  ▼   ▼   ▼   ▼    ▼  │                          │
│           ┌────────────────┐  │ ┌──────────────────┐  │                          │
│           │ In-Memory      │  │ │ In-Memory        │  │                          │
│           │ Buffer         │  │ │ Buffer           │  │                          │
│           │ (bounded by    │  │ │                  │  │                          │
│           │ maxBuffered    │  │ │                  │  │                          │
│           │ Requests)      │  │ │                  │  │                          │
│           └───────┬────────┘  │ └───────┬──────────┘  │                          │
│                   │           │         │             │                          │
│       maxBatchSize reached    │  maxTimeInBufferMS    │                          │
│                   │           │         │             │                          │
│                   ▼           │         ▼             │                          │
│       submitRequestEntries()  │  submitRequestEntries()                          │
│       (async HTTP/SDK call)   │  (async)             │                          │
│           │                   │     │                │                          │
│           ├─ success → done   │     ├─ success       │                          │
│           └─ failure → retry  │     └─ failure→retry │                          │
│                               │                      │                          │
│       checkpoint barrier ─────┤                      │                          │
│           │                   │                      │                          │
│       1. Wait for in-flight   │                      │                          │
│          requests to complete │                      │                          │
│       2. Snapshot remaining   │                      │                          │
│          buffer to Flink state│                      │                          │
│       3. Checkpoint completes │                      │                          │
│                               │                      │                          │
│  Recovery: Restore buffer from state → re-submit     │                          │
│            → Records may be sent twice (at-least-once)                           │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Memory: Bounded buffer + state for unsent records

```
Memory = min(maxBufferedRequests × record_size, throughput × flush_interval × record_size)
State  = remaining_buffer_at_checkpoint × record_size (persisted on snapshot)

Typical: maxBufferedRequests=10000, record_size=200B → buffer ≈ 2MB
State: depends on how much is unflushed at checkpoint time
```

---

# Part 2: Memory and State Usage Comparison

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    MEMORY / STATE USAGE COMPARISON                                   │
│                                                                                     │
│  Assumptions: 200B records, 100K records/sec, 60s checkpoint interval               │
│               = 6M records per checkpoint = 1.2 GB raw data                         │
│                                                                                     │
│  Pattern A (Buffer+Batch):                                                          │
│    Memory:  batch_size × 200B = 5000 × 200B = 1 MB                                │
│    State:   0 bytes                                                                 │
│    Total:   ~1 MB ██                                                                │
│                                                                                     │
│  Pattern B (WAL):                                                                   │
│    Memory:  ~0 (serialized to state stream)                                         │
│    State:   100K/sec × 60s × 200B = 1.2 GB                                        │
│    Total:   ~1.2 GB ██████████████████████████████████████████████████████████████  │
│                                                                                     │
│  Pattern C (XA 2PC):                                                                │
│    Memory:  ~0 (records in DB transaction log)                                      │
│    State:   XA transaction ID ≈ 200 bytes                                           │
│    Total:   ~200 B █                                                                │
│                                                                                     │
│  Pattern D (Kafka 2PC):                                                             │
│    Memory:  Kafka producer buffer (configurable, default 32MB)                      │
│    State:   KafkaCommittable ≈ 200 bytes                                            │
│    Total:   ~200 B state + 32 MB producer buffer █                                  │
│                                                                                     │
│  Pattern E (Async Sink):                                                            │
│    Memory:  maxBufferedRequests × 200B = 10000 × 200B = 2 MB                       │
│    State:   remaining buffer at checkpoint ≈ 0-2 MB                                 │
│    Total:   ~2-4 MB ████                                                            │
│                                                                                     │
│  ═══════════════════════════════════════════════════════════════════════             │
│  Winner (lowest state):  Pattern C (XA) or Pattern D (Kafka 2PC)                    │
│  Worst (highest state):  Pattern B (WAL) — by orders of magnitude                   │
│  Best balance:           Pattern A (Buffer+Batch) or Pattern E (Async)              │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

# Part 3: JDBC / MySQL Specific Analysis

## Available Options for MySQL

MySQL supports both XA transactions and `INSERT ... ON DUPLICATE KEY UPDATE`, giving you two viable paths:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         MYSQL SINK DECISION TREE                                    │
│                                                                                     │
│  Do you need TRUE exactly-once (no duplicates even transiently)?                    │
│                                                                                     │
│    YES ──► Use JdbcSink.exactlyOnceSink() (Pattern C: XA)                          │
│            │                                                                        │
│            ├─ Pros: True exactly-once, DB-level isolation                           │
│            ├─ Cons: XA overhead, requires XA_RECOVER_ADMIN grant,                   │
│            │        max 1 XA txn per connection (MySQL limitation),                 │
│            │        long-running prepared txns hold locks,                           │
│            │        maxRetries MUST be 0                                             │
│            └─ When: Financial transactions, billing, audit logs                      │
│                                                                                     │
│    NO ───► Can you use upsert (table has primary key)?                              │
│                                                                                     │
│      YES ──► Use JdbcSink.sink() with upsert (Pattern A: Buffer+Batch)             │
│              │         ★ RECOMMENDED FOR MOST USE CASES ★                           │
│              │                                                                      │
│              ├─ Pros: Simple, fast (batched), no XA overhead,                       │
│              │        effective exactly-once via idempotent upsert,                  │
│              │        no extra permissions needed,                                   │
│              │        lower latency (flush at batch interval too)                    │
│              ├─ Cons: Transient duplicates visible between crash and recovery,       │
│              │        requires primary key for upsert                                │
│              └─ When: Analytics, metrics, dimension tables, most OLTP                │
│                                                                                     │
│      NO ───► Accept at-least-once with possible duplicates                          │
│              Or: Add a deduplication key to your schema                              │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Why Buffer+Batch with Upsert is Preferred for MySQL

| Factor | XA 2PC (exactlyOnceSink) | Buffer+Batch + Upsert (sink) |
|--------|--------------------------|------------------------------|
| **Complexity** | High (XA protocol, connection management) | Low (standard JDBC batch) |
| **Performance** | Slower (XA overhead, prepared txn locks) | Faster (native batch insert) |
| **DB permissions** | Requires `XA_RECOVER_ADMIN` | Standard INSERT/UPDATE |
| **Connection usage** | 1 XA txn per connection (MySQL limit) | Reusable connections |
| **Lock contention** | Prepared txns hold row locks until commit | No long-held locks |
| **Recovery** | Clean (XA COMMIT/ROLLBACK) | Replay + overwrite (upsert) |
| **Duplicate visibility** | Never (transaction isolation) | Transient (between crash and recovery) |
| **Configuration** | `maxRetries` must be 0 | Flexible retry settings |
| **State overhead** | ~200 bytes | 0 bytes |
| **Recommendation** | Financial/audit systems only | General purpose (most use cases) |

## JDBC Sink API

```java
// Pattern A: Buffer + Batch + Upsert (RECOMMENDED for MySQL)
JdbcSink.sink(
    "INSERT INTO orders (id, amount, status) VALUES (?, ?, ?) "
    + "ON DUPLICATE KEY UPDATE amount = VALUES(amount), status = VALUES(status)",
    (ps, order) -> {
        ps.setString(1, order.id);
        ps.setBigDecimal(2, order.amount);
        ps.setString(3, order.status);
    },
    JdbcExecutionOptions.builder()
        .withBatchSize(5000)               // Flush every 5000 records
        .withBatchIntervalMs(1000)         // Or every 1 second
        .withMaxRetries(3)                 // Retry on transient failures
        .build(),
    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://host:3306/db")
        .withDriverName("com.mysql.cj.jdbc.Driver")
        .withUsername("user")
        .withPassword("pass")
        .build()
);

// Pattern C: XA 2PC (for strict exactly-once)
JdbcSink.exactlyOnceSink(
    "INSERT INTO orders (id, amount, status) VALUES (?, ?, ?)",
    (ps, order) -> {
        ps.setString(1, order.id);
        ps.setBigDecimal(2, order.amount);
        ps.setString(3, order.status);
    },
    JdbcExecutionOptions.builder()
        .withMaxRetries(0)                 // MUST be 0 for exactly-once!
        .build(),
    JdbcExactlyOnceOptions.builder()
        .withTransactionPerConnection(true)  // Required for MySQL
        .build(),
    () -> DriverManager.getConnection("jdbc:mysql://host:3306/db", "user", "pass")
);
```

---

# Part 4: Full Comparison Table

| Aspect | A: Buffer+Batch | B: WAL+Committer | C: XA 2PC | D: Kafka 2PC | E: Async Sink |
|--------|----------------|-------------------|-----------|-------------|---------------|
| **Guarantee** | At-least-once | At-least-once | Exactly-once | Exactly-once | At-least-once |
| **Effective EOS** | Yes (with upsert) | Yes (with idempotent) | Yes (native) | Yes (native) | No |
| **Flink state** | 0 bytes | ~GB (all records) | ~200 bytes | ~200 bytes | ~MB (buffer) |
| **Memory** | ~MB (batch buffer) | ~0 (in state) | ~0 | ~32MB (Kafka producer) | ~MB (buffer) |
| **Latency** | Low (batch interval) | High (checkpoint interval) | Medium (checkpoint) | Medium (checkpoint) | Low (batch interval) |
| **Partial writes visible** | Yes | Yes (after flush) | No (txn isolation) | No (read_committed) | Yes |
| **Requires idempotent ops** | Yes (for EOS) | Yes (for EOS) | No | No | N/A |
| **Requires DB transactions** | No | No | Yes (XA) | Yes (Kafka txn) | No |
| **External metadata** | No | Yes (committer table) | No | No | No |
| **Checkpoint pressure** | None | Very high | None | None | Low |
| **Implementation complexity** | Low | Medium | High | High (built-in) | Low |
| **Flink connector** | `JdbcSink.sink()` | `CassandraWriteAheadSink` | `JdbcSink.exactlyOnceSink()` | `KafkaSink` | `AsyncSinkBase` |
| **Best for** | JDBC/MySQL, most DBs | Cassandra (idempotent) | Financial systems | Kafka | Cloud APIs |

---

# Part 5: Decision Flowchart

```
                          ┌──────────────────────────┐
                          │  What is your sink system? │
                          └─────────┬────────────────┘
                                    │
              ┌─────────────────────┼────────────────────────┐
              │                     │                        │
              ▼                     ▼                        ▼
        ┌──────────┐         ┌──────────┐            ┌──────────────┐
        │  Kafka   │         │  RDBMS   │            │  NoSQL/API   │
        │          │         │ (MySQL,  │            │ (Cassandra,  │
        │          │         │  PG, etc)│            │  DynamoDB,   │
        │          │         │          │            │  HTTP, etc)  │
        └────┬─────┘         └────┬─────┘            └──────┬───────┘
             │                    │                         │
             ▼                    ▼                         ▼
      ┌────────────┐      Need true EOS?           Supports idempotent
      │ Pattern D: │        │       │              writes (upsert)?
      │ Kafka 2PC  │       YES      NO                │       │
      │ (built-in) │        │       │                YES      NO
      └────────────┘        │       │                 │       │
                            ▼       ▼                 ▼       ▼
                     ┌──────────┐ ┌──────────┐  ┌──────────┐ ┌──────────┐
                     │Pattern C:│ │Pattern A: │  │Pattern A: │ │Pattern E:│
                     │ XA 2PC   │ │Buffer+    │  │Buffer+    │ │Async Sink│
                     │          │ │Batch +    │  │Batch +    │ │(at-least │
                     │          │ │Upsert     │  │idempotent │ │ once)    │
                     └──────────┘ │(recommend)│  │ OR        │ └──────────┘
                                  └──────────┘  │Pattern B: │
                                                │WAL+commit │
                                                └──────────┘
```

---

# Key File Reference

| Pattern | Flink Component | Location |
|---------|----------------|----------|
| A: Buffer+Batch | `JdbcSink.sink()` | [flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc) |
| B: WAL | `GenericWriteAheadSink` | `flink-streaming-java/.../operators/` |
| B: WAL | `CassandraTupleWriteAheadSink` | `flink-connector-cassandra` |
| B: WAL | `CheckpointCommitter` / `CassandraCommitter` | `flink-streaming-java` / `flink-connector-cassandra` |
| C: XA 2PC | `JdbcXaSinkFunction` | [flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc) |
| D: Kafka 2PC | `TwoPhaseCommittingSink` / `KafkaSink` | `flink-connector-kafka` |
| E: Async | `AsyncSinkBase` | `flink-connector-base` |

Sources:
- [Flink JDBC Connector Docs](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/jdbc/)
- [FLIP-171: Async Sink](https://cwiki.apache.org/confluence/display/FLINK/FLIP-171:+Async+Sink)
- [The Generic Asynchronous Base Sink (Flink Blog)](https://flink.apache.org/2022/03/16/the-generic-asynchronous-base-sink/)
- [FLINK-15578: Implement exactly-once JDBC sink](https://issues.apache.org/jira/browse/FLINK-15578)
- [flink-connector-jdbc GitHub](https://github.com/apache/flink-connector-jdbc)
- [End-to-End Exactly-Once in Flink](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/)
- [Flink Fault Tolerance Guarantees](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/guarantees/)
