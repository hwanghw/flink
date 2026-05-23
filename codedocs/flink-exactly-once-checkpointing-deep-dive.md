# Flink Exactly-Once Semantics, Checkpoint Modes, and CheckpointedFunction Deep Dive

This document covers: (1) exactly-once vs end-to-end exactly-once, (2) how `AT_LEAST_ONCE` checkpoint mode helps low-latency, (3) the `CheckpointedFunction` interface and its deprecated predecessor.

---

# Part 1: Exactly-Once vs End-to-End Exactly-Once

## The Two Levels of "Exactly-Once"

Most people say "Flink supports exactly-once" — but there are actually **two distinct guarantees** that are often conflated:

| Guarantee | Scope | What It Means |
|-----------|-------|---------------|
| **Exactly-once (state)** | Inside Flink only | Each incoming event affects Flink's internal state exactly once, even during failures |
| **End-to-end exactly-once** | Source → Flink → Sink | No data loss AND no duplicates visible to external systems (Kafka, DB, etc.) |

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    EXACTLY-ONCE SCOPE COMPARISON                         │
│                                                                          │
│  ┌──────────┐     ┌─────────────────────────────────┐     ┌──────────┐  │
│  │          │     │         FLINK CLUSTER            │     │          │  │
│  │  SOURCE  │────▶│  ┌─────┐   ┌─────┐   ┌─────┐   │────▶│   SINK   │  │
│  │ (Kafka)  │     │  │ Op1 │──▶│ Op2 │──▶│ Op3 │   │     │ (Kafka)  │  │
│  │          │     │  └─────┘   └─────┘   └─────┘   │     │          │  │
│  └──────────┘     └─────────────────────────────────┘     └──────────┘  │
│                                                                          │
│  ◀──── Exactly-Once (State) ────▶                                        │
│       Flink guarantees each record                                       │
│       affects internal state once                                        │
│                                                                          │
│  ◀──────────── End-to-End Exactly-Once ─────────────▶                    │
│       No duplicates visible in the sink output                           │
│       Requires source replay + sink transactions                         │
└──────────────────────────────────────────────────────────────────────────┘
```

## How Flink Achieves Exactly-Once (State) — Checkpoint Barriers

Flink's core mechanism is **distributed snapshots** using checkpoint barriers:

```
                    Checkpoint Barrier (CB)
                          │
     Source Stream:       ▼
  ┌───┬───┬───┬───┬──║──┬───┬───┬───┬───┐
  │ E9│ E8│ E7│ E6│CB│ E5│ E4│ E3│ E2│ E1│  ──▶  time
  └───┴───┴───┴───┴──║──┴───┴───┴───┴───┘
                      │
           ┌──────────┴──────────┐
           │                     │
     Post-checkpoint         Pre-checkpoint
     epoch (E6-E9)           epoch (E1-E5)
     NOT in snapshot         IN snapshot
```

**Steps:**
1. **JobManager** triggers a checkpoint and injects barriers at sources
2. Barriers flow downstream with the data, dividing epochs
3. When an operator receives barriers from **all** input channels, it snapshots its state
4. Once **all** operators complete, the checkpoint is marked as successful
5. On failure, Flink restores from the latest successful checkpoint and replays from there

**Key insight**: This only guarantees internal state consistency. If a sink already wrote data to Kafka/DB before the failure, those writes are NOT rolled back — you get **duplicates in the external system**.

## How End-to-End Exactly-Once Works — Two-Phase Commit

To extend the guarantee to external systems, Flink uses **two-phase commit (2PC)** coordinated with checkpoints:

```
┌─────────────────────────────────────────────────────────────────────┐
│              TWO-PHASE COMMIT WITH CHECKPOINTS                       │
│                                                                      │
│  Phase 1: PRE-COMMIT (during checkpoint)                             │
│  ─────────────────────────────────────────                           │
│                                                                      │
│  JobManager                                                          │
│      │                                                               │
│      │ 1. inject barrier                                             │
│      ▼                                                               │
│  ┌────────┐  barrier  ┌────────┐  barrier  ┌────────────────────┐   │
│  │ Source  │─────────▶│Operator│─────────▶│  Transactional Sink │   │
│  │        │          │        │          │                      │   │
│  │snapshot│          │snapshot│          │ snapshot state       │   │
│  │ offset │          │ state  │          │ + pre-commit txn     │   │
│  └────────┘          └────────┘          │ (flush, don't       │   │
│                                          │  commit yet!)        │   │
│                                          └────────────────────┘   │
│                                                                      │
│  Phase 2: COMMIT (after checkpoint succeeds)                         │
│  ────────────────────────────────────────────                        │
│                                                                      │
│  JobManager ── notifyCheckpointComplete() ──▶  Sink                  │
│                                                  │                   │
│                                                  ▼                   │
│                                          ┌────────────────┐         │
│                                          │ COMMIT the txn │         │
│                                          │ to external    │         │
│                                          │ system (Kafka) │         │
│                                          └────────────────┘         │
│                                                                      │
│  If pre-commit fails → ABORT all → rollback to previous checkpoint   │
│  If commit fails → restart app → retry commit (must eventually       │
│                     succeed for correctness!)                         │
└─────────────────────────────────────────────────────────────────────┘
```

### TwoPhaseCommitSinkFunction (Legacy) / TwoPhaseCommittingSink (New API)

The legacy `TwoPhaseCommitSinkFunction` (pre-FLIP-143) required implementing 4 methods:

| Method | Purpose |
|--------|---------|
| `beginTransaction()` | Start a new transaction (e.g., Kafka producer `beginTransaction()`) |
| `preCommit()` | Flush buffered data, finalize state — but do NOT commit yet |
| `commit()` | Atomically commit the transaction to the external system |
| `abort()` | Roll back a failed transaction |

The new Sink API (FLIP-143, Flink 1.14+) uses `TwoPhaseCommittingSink` with a `Committer` abstraction:

```
┌────────────────────────────────────────────────────────────────────┐
│                 SINK API EVOLUTION                                   │
│                                                                     │
│  Legacy (Flink ≤ 1.13):                                            │
│    SinkFunction + TwoPhaseCommitSinkFunction                        │
│    └─ Extends SinkFunction, implements beginTxn/preCommit/commit    │
│                                                                     │
│  New (Flink 1.14+):                                                 │
│    Sink interface (FLIP-143)                                        │
│    ├─ SinkWriter          — writes data, buffers                    │
│    ├─ Committer           — commits pre-committed data              │
│    └─ TwoPhaseCommittingSink — marker interface combining both      │
│                                                                     │
│  KafkaSink uses TwoPhaseCommittingSink under the hood               │
└────────────────────────────────────────────────────────────────────┘
```

### Requirements for End-to-End Exactly-Once

```
┌───────────────────────────────────────────────────────────────────────┐
│              THREE PILLARS OF END-TO-END EXACTLY-ONCE                  │
│                                                                        │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────────┐  │
│  │     SOURCE        │  │     FLINK         │  │       SINK          │  │
│  │                   │  │                   │  │                     │  │
│  │  Must support     │  │  Checkpointing    │  │  Must support       │  │
│  │  REPLAY from a    │  │  with             │  │  TRANSACTIONS       │  │
│  │  given offset     │  │  EXACTLY_ONCE     │  │  (or idempotent     │  │
│  │                   │  │  mode enabled     │  │   writes)           │  │
│  │  ✓ Kafka          │  │                   │  │                     │  │
│  │  ✓ Kinesis        │  │  Barrier          │  │  ✓ Kafka (txn)      │  │
│  │  ✗ Raw socket     │  │  alignment        │  │  ✓ JDBC (XA)        │  │
│  │                   │  │  ensures state     │  │  ✓ Upsert sinks    │  │
│  │                   │  │  consistency       │  │  ✗ Plain file       │  │
│  └──────────────────┘  └──────────────────┘  └─────────────────────┘  │
└───────────────────────────────────────────────────────────────────────┘
```

---

# Part 2: AT_LEAST_ONCE Checkpoint Mode for Low Latency

## Checkpoint Alignment and Its Cost

With `EXACTLY_ONCE` mode, operators with multiple inputs must **align barriers** — they block fast channels while waiting for slow channels to deliver their barriers:

```
┌──────────────────────────────────────────────────────────────────────┐
│            EXACTLY_ONCE: BARRIER ALIGNMENT (BLOCKING)                │
│                                                                      │
│  Input Channel 1 (fast):                                             │
│  ┌───┬───┬──║──┬───┬───┐                                            │
│  │ E5│ E4│CB│ E3│ E2│ E1│  ──▶    barrier arrived early              │
│  └───┴───┴──║──┴───┴───┘                                            │
│                 │                                                     │
│                 ▼ BLOCKED!  (E4, E5 buffered, not processed)         │
│            ┌─────────┐                                               │
│            │         │                                               │
│            │ Operator│ ──▶  WAITING for Ch2 barrier                  │
│            │ (join)  │                                               │
│            │         │                                               │
│            └─────────┘                                               │
│                 ▲ still processing                                    │
│                 │                                                     │
│  Input Channel 2 (slow, backpressured):                              │
│  ┌───┬───┬───┬───┬──║──┬───┐                                        │
│  │E12│E11│E10│ E9│CB│ E8│ E7│  ──▶    barrier far behind             │
│  └───┴───┴───┴───┴──║──┴───┘                                        │
│                                                                      │
│  Result: Channel 1 is STALLED. Latency spikes until Ch2 catches up.  │
│  Alignment time can be seconds or even minutes under backpressure!   │
└──────────────────────────────────────────────────────────────────────┘
```

## AT_LEAST_ONCE: Skip Alignment

With `AT_LEAST_ONCE` mode, operators **do not block** channels during barrier alignment. They continue processing events from all channels regardless of barrier arrival:

```
┌──────────────────────────────────────────────────────────────────────┐
│            AT_LEAST_ONCE: NO BARRIER ALIGNMENT (NO BLOCKING)         │
│                                                                      │
│  Input Channel 1 (fast):                                             │
│  ┌───┬───┬──║──┬───┬───┐                                            │
│  │ E5│ E4│CB│ E3│ E2│ E1│  ──▶    barrier arrived                    │
│  └───┴───┴──║──┴───┴───┘                                            │
│                 │                                                     │
│                 ▼ NOT blocked! E4, E5 processed immediately          │
│            ┌─────────┐                                               │
│            │         │                                               │
│            │ Operator│ ──▶  Continues processing ALL channels        │
│            │ (join)  │                                               │
│            │         │                                               │
│            └─────────┘                                               │
│                 ▲ also processing                                     │
│                 │                                                     │
│  Input Channel 2 (slow):                                             │
│  ┌───┬───┬───┬───┬──║──┬───┐                                        │
│  │E12│E11│E10│ E9│CB│ E8│ E7│  ──▶    barrier still far behind       │
│  └───┴───┴───┴───┴──║──┴───┘                                        │
│                                                                      │
│  ⚠ E4, E5 from Ch1 are post-barrier but included in snapshot state.  │
│  On recovery, they will be replayed → DUPLICATES in state.           │
│  Benefit: ZERO alignment latency. Throughput never blocked.          │
└──────────────────────────────────────────────────────────────────────┘
```

### Trade-off Summary

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    CHECKPOINT MODE COMPARISON                             │
│                                                                          │
│  ┌──────────────────────────────┐  ┌──────────────────────────────────┐  │
│  │     EXACTLY_ONCE              │  │     AT_LEAST_ONCE                │  │
│  │                               │  │                                  │  │
│  │  ✓ No duplicate processing    │  │  ✗ May process records >1 time  │  │
│  │  ✗ Alignment blocks channels  │  │  ✓ No alignment blocking        │  │
│  │  ✗ Latency spikes during CP   │  │  ✓ Minimal latency impact       │  │
│  │  ✗ Backpressure slows CP      │  │  ✓ CP speed independent of BP   │  │
│  │                               │  │                                  │  │
│  │  Use when: correctness is     │  │  Use when: latency matters more │  │
│  │  critical, can tolerate       │  │  than duplicate processing,     │  │
│  │  occasional latency spikes    │  │  or sink is idempotent          │  │
│  └──────────────────────────────┘  └──────────────────────────────────┘  │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │     UNALIGNED CHECKPOINTS (Flink 1.11+)                          │    │
│  │                                                                   │    │
│  │  ✓ EXACTLY_ONCE without alignment blocking!                       │    │
│  │  ✓ Barriers overtake in-flight data in buffers                    │    │
│  │  ✗ Larger checkpoint size (includes in-flight data)               │    │
│  │  ✗ Only 1 concurrent checkpoint allowed                           │    │
│  │  ✗ Higher checkpoint storage cost                                 │    │
│  │                                                                   │    │
│  │  Best of both: exactly-once + low latency under backpressure      │    │
│  └──────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
```

### How Unaligned Checkpoints Work

```
┌──────────────────────────────────────────────────────────────────────┐
│           UNALIGNED CHECKPOINT: BARRIER OVERTAKES BUFFERS            │
│                                                                      │
│  Input Channel 1 (fast):                                             │
│  ┌───┬───┬──║──┬───┬───┐                                            │
│  │ E5│ E4│CB│ E3│ E2│ E1│  ──▶                                       │
│  └───┴───┴──║──┴───┴───┘                                            │
│                 │                                                     │
│                 ▼ barrier arrives first                               │
│            ┌─────────┐                                               │
│            │ Operator │                                               │
│            │          │                                               │
│            │ 1. Immediately snapshot state                            │
│            │ 2. Store in-flight data (E4,E5 from Ch1 buffer)         │
│            │    as PART of the checkpoint                             │
│            │ 3. Forward barrier immediately (don't wait for Ch2)     │
│            │ 4. Continue processing normally                          │
│            └─────────┘                                               │
│                 ▲                                                     │
│  Input Channel 2 (slow):                                             │
│  ┌───┬───┬───┬──║──┬───┐                                            │
│  │E11│E10│ E9│CB│ E8│ E7│  ──▶  when barrier arrives later,          │
│  └───┴───┴───┴──║──┴───┘       in-flight data (E9,E10,E11)          │
│                                  also stored in checkpoint            │
│                                                                      │
│  Checkpoint includes: operator state + ALL in-flight buffer data     │
│  On recovery: state restored + buffered data re-injected             │
│  Result: exactly-once guarantee WITHOUT alignment blocking!          │
└──────────────────────────────────────────────────────────────────────┘
```

### Configuration

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Option 1: AT_LEAST_ONCE — skip alignment entirely (fastest, allows duplicates)
env.enableCheckpointing(1000);
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

// Option 2: Unaligned checkpoints — exactly-once without alignment blocking
env.enableCheckpointing(1000);
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().enableUnalignedCheckpoints();

// Option 3: Standard aligned exactly-once (default, highest latency under backpressure)
env.enableCheckpointing(1000);
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
```

### When to Use AT_LEAST_ONCE

- Your sink is **idempotent** (upsert to DB with primary key, dedup downstream)
- You need **sub-millisecond latency** and cannot tolerate any checkpoint-induced stalls
- Your pipeline has **severe backpressure** and unaligned checkpoints are not an option (e.g., iterative jobs)
- You're doing **approximate analytics** (counts, aggregations where small overcounting is acceptable)

---

# Part 3: CheckpointedFunction — Managing Operator State

## Interface Evolution

Flink has gone through several iterations of its stateful function interfaces:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                 CHECKPOINT INTERFACE EVOLUTION                            │
│                                                                          │
│  Flink 1.0-1.1:  Checkpointed<T extends Serializable>     ❌ REMOVED    │
│                   └─ snapshotState() returns T                           │
│                   └─ restoreState(T state)                               │
│                                                                          │
│  Flink 1.2-1.3:  ListCheckpointed<T extends Serializable> ⚠ DEPRECATED │
│                   └─ snapshotState() returns List<T>                     │
│                   └─ restoreState(List<T> state)                         │
│                   └─ Uses Java Serialization (savepoint compat issues)   │
│                                                                          │
│  Flink 1.2+:     CheckpointedFunction                     ✅ CURRENT    │
│                   └─ snapshotState(FunctionSnapshotContext)               │
│                   └─ initializeState(FunctionInitializationContext)       │
│                   └─ Uses Flink's TypeSerializer (safe for savepoints)   │
│                                                                          │
│  For KEYED state only:  RichFunction + RuntimeContext      ✅ CURRENT    │
│                   └─ getRuntimeContext().getState(descriptor)             │
│                   └─ Simpler API when you only need keyed state          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Why ListCheckpointed Was Deprecated

`ListCheckpointed` (see [FLINK-6258](https://issues.apache.org/jira/browse/FLINK-6258)) was deprecated because:

1. **Java Serialization** — It uses Java's built-in serialization for state objects, which breaks savepoint compatibility when classes change
2. **Limited control** — No way to specify custom serializers or have multiple named states
3. **Rigid API** — Only supports list-style redistribution with `snapshotState()` → `List<T>` and `restoreState(List<T>)`

`CheckpointedFunction` replaces it by giving direct access to `OperatorStateStore`, which supports custom `TypeSerializer`, multiple named states, and both union and list redistribution strategies.

## CheckpointedFunction Interface

```java
// From: flink-runtime/.../checkpoint/CheckpointedFunction.java
@Public
public interface CheckpointedFunction {

    /**
     * Called whenever a checkpoint takes a state snapshot.
     * Ensure checkpointed data structures are up to date.
     */
    void snapshotState(FunctionSnapshotContext context) throws Exception;

    /**
     * Called when the parallel function instance is created.
     * Set up state storing data structures here.
     * Also called during recovery — use context.isRestored() to check.
     */
    void initializeState(FunctionInitializationContext context) throws Exception;
}
```

## Complete Code Example: Buffered Sink with Operator State

This example shows a sink that buffers elements in memory and flushes to an external system in batches. The buffer is checkpointed so that no data is lost on failure.

```java
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * A sink that buffers elements and flushes them in batches.
 * Uses CheckpointedFunction to persist the buffer across checkpoints,
 * ensuring no data loss on failure/recovery.
 */
public class BufferingSink
        implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

    private final int batchSize;

    // In-memory buffer (NOT managed by Flink)
    private transient List<Tuple2<String, Integer>> bufferedElements;

    // Flink-managed operator state (persisted in checkpoints)
    private transient ListState<Tuple2<String, Integer>> checkpointedState;

    public BufferingSink(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        // Buffer incoming elements
        bufferedElements.add(value);

        // Flush when batch is full
        if (bufferedElements.size() >= batchSize) {
            flush();
        }
    }

    /**
     * Called during checkpoint — copy the in-memory buffer into Flink's
     * managed ListState so it gets persisted to the checkpoint.
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Clear previously checkpointed state
        checkpointedState.clear();

        // Copy current buffer into the managed state
        for (Tuple2<String, Integer> element : bufferedElements) {
            checkpointedState.add(element);
        }

        // At this point, Flink will persist checkpointedState to the state backend
    }

    /**
     * Called on startup AND on recovery from a checkpoint.
     * Initialize or restore the buffer from Flink's managed state.
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // Define the state descriptor with proper TypeInformation (not Java Serialization!)
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ListStateDescriptor<>(
                        "buffered-elements",
                        TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<
                                Tuple2<String, Integer>>() {}));

        // Get or create the operator state
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        // Restore: rebuild in-memory buffer from checkpointed state
        bufferedElements = new ArrayList<>();
        if (context.isRestored()) {
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }

    private void flush() throws Exception {
        // Send buffered data to external system (DB, API, file, etc.)
        for (Tuple2<String, Integer> element : bufferedElements) {
            // ... write to external system ...
        }
        bufferedElements.clear();
    }
}
```

### How It Works During Checkpoint and Recovery

```
┌──────────────────────────────────────────────────────────────────────┐
│          CHECKPOINTED BUFFERING SINK LIFECYCLE                       │
│                                                                      │
│  NORMAL OPERATION:                                                   │
│  ─────────────────                                                   │
│  invoke(E1) → buffer=[E1]                                            │
│  invoke(E2) → buffer=[E1,E2]                                        │
│  invoke(E3) → buffer=[E1,E2,E3]  (batchSize=5, don't flush yet)     │
│                                                                      │
│  CHECKPOINT TRIGGERED (snapshotState):                               │
│  ─────────────────────────────────────                               │
│  checkpointedState.clear()                                           │
│  checkpointedState.add(E1)                                           │
│  checkpointedState.add(E2)                                           │
│  checkpointedState.add(E3)                                           │
│  → Flink persists [E1,E2,E3] to state backend (RocksDB/FS)          │
│                                                                      │
│  CONTINUE PROCESSING:                                                │
│  ─────────────────────                                               │
│  invoke(E4) → buffer=[E1,E2,E3,E4]                                  │
│  invoke(E5) → buffer=[E1,E2,E3,E4,E5]  → flush()! buffer=[]         │
│  invoke(E6) → buffer=[E6]                                            │
│                                                                      │
│  💥 FAILURE! (E5 was flushed to external system, E6 in buffer)       │
│                                                                      │
│  RECOVERY (initializeState with isRestored=true):                    │
│  ────────────────────────────────────────────────                    │
│  Restore from checkpoint: buffer=[E1,E2,E3]                         │
│  Replay from source offset at checkpoint time                        │
│  invoke(E4) → buffer=[E1,E2,E3,E4]                                  │
│  invoke(E5) → buffer=[E1,E2,E3,E4,E5]  → flush()!                   │
│  invoke(E6) → buffer=[E6]                                            │
│                                                                      │
│  ⚠ E5 is flushed AGAIN → at-least-once to external system!          │
│  (For exactly-once: combine with transactions or idempotent writes)  │
└──────────────────────────────────────────────────────────────────────┘
```

## Example 2: Keyed + Operator State Together

The `CheckpointedFunction` can manage both keyed state and operator state simultaneously. This is from the official Javadoc in the Flink source code:

```java
public class MyFunction<T> implements MapFunction<T, T>, CheckpointedFunction {

    private ReducingState<Long> countPerKey;       // KEYED state — one per key
    private ListState<Long> countPerPartition;     // OPERATOR state — one per parallel instance

    private long localCount;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // Keyed state: per-key counter (only available after keyBy())
        countPerKey = context.getKeyedStateStore().getReducingState(
                new ReducingStateDescriptor<>("perKeyCount", new AddFunction<>(), Long.class));

        // Operator state: per-partition counter (redistributed on rescale)
        countPerPartition = context.getOperatorStateStore().getOperatorState(
                new ListStateDescriptor<>("perPartitionCount", Long.class));

        // Rebuild local variable from operator state (handles rescaling)
        for (Long l : countPerPartition.get()) {
            localCount += l;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Keyed state is automatically managed — no action needed
        // Operator state: sync local variable → managed state
        countPerPartition.clear();
        countPerPartition.add(localCount);
    }

    @Override
    public T map(T value) throws Exception {
        countPerKey.add(1L);
        localCount++;
        return value;
    }
}
```

### Operator State Redistribution on Rescale

```
┌─────────────────────────────────────────────────────────────────────┐
│       OPERATOR STATE REDISTRIBUTION (ListState)                      │
│                                                                      │
│  Before rescale (parallelism = 3):                                   │
│                                                                      │
│    func_1           func_2           func_3                          │
│  ┌────┬────┐     ┌────┬────┐      ┌────┐                            │
│  │ S1 │ S2 │     │ S3 │ S4 │      │ S5 │                            │
│  └────┴────┘     └────┴────┘      └────┘                            │
│                                                                      │
│  Checkpoint: [S1,S2] ++ [S3,S4] ++ [S5] = [S1,S2,S3,S4,S5]         │
│                                                                      │
│  After rescale to parallelism = 5 (even split):                      │
│                                                                      │
│  func_1   func_2   func_3   func_4   func_5                         │
│  ┌────┐   ┌────┐   ┌────┐   ┌────┐   ┌────┐                        │
│  │ S1 │   │ S2 │   │ S3 │   │ S4 │   │ S5 │                        │
│  └────┘   └────┘   └────┘   └────┘   └────┘                        │
│                                                                      │
│  After rescale to parallelism = 2 (merged):                          │
│                                                                      │
│       func_1              func_2                                     │
│  ┌────┬────┬────┐    ┌────┬────┐                                    │
│  │ S1 │ S2 │ S3 │    │ S4 │ S5 │                                    │
│  └────┴────┴────┘    └────┴────┘                                    │
│                                                                      │
│  initializeState() iterates over all assigned sub-states             │
│  and aggregates them (e.g., sum of counts for a counter).            │
└─────────────────────────────────────────────────────────────────────┘
```

---

# Part 4: Quick Reference

## Decision Flowchart

```
                    Which checkpoint mode should I use?
                                │
                                ▼
                ┌───────────────────────────────┐
                │  Do you need exactly-once      │
                │  in the external sink?          │
                └───────────────┬───────────────┘
                       YES      │       NO
                 ┌──────────────┴──────────────┐
                 ▼                              ▼
    ┌─────────────────────┐        ┌──────────────────────────┐
    │ Use EXACTLY_ONCE +   │        │ Is your sink idempotent?  │
    │ transactional sink   │        └────────────┬─────────────┘
    │ (Kafka txn, JDBC XA) │              YES    │    NO
    │                      │         ┌───────────┴───────────┐
    │ If backpressure is   │         ▼                       ▼
    │ an issue, enable     │  ┌──────────────┐    ┌──────────────────┐
    │ unaligned CPs        │  │ AT_LEAST_ONCE│    │ EXACTLY_ONCE     │
    └─────────────────────┘  │ (fastest,     │    │ (safe default)   │
                              │  dedup at     │    │                  │
                              │  sink level)  │    │ + unaligned CPs  │
                              └──────────────┘    │  if needed        │
                                                  └──────────────────┘
```

## Configuration Cheat Sheet

| Config Key | Default | Description |
|---|---|---|
| `execution.checkpointing.mode` | `EXACTLY_ONCE` | Guarantee level |
| `execution.checkpointing.interval` | (none) | Checkpoint interval in ms |
| `execution.checkpointing.timeout` | 10 min | Max checkpoint duration |
| `execution.checkpointing.min-pause` | 0 | Min gap between checkpoints |
| `execution.checkpointing.max-concurrent-checkpoints` | 1 | Concurrent checkpoints |
| `execution.checkpointing.unaligned.enabled` | false | Enable unaligned checkpoints |
| `execution.checkpointing.tolerable-failed-checkpoints` | 0 | Allowed consecutive failures |

---

## References

- [An Overview of End-to-End Exactly-Once Processing in Apache Flink](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/)
- [Mastering Exactly-Once Processing in Apache Flink](https://risingwave.com/blog/mastering-exactly-once-processing-in-apache-flink/)
- [From Aligned to Unaligned Checkpoints](https://flink.apache.org/2020/10/15/from-aligned-to-unaligned-checkpoints-part-1-checkpoints-alignment-and-backpressure/)
- [Flink Checkpointing Configuration](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/)
- [Getting into Low-Latency Gears with Apache Flink](https://flink.apache.org/2022/05/23/getting-into-low-latency-gears-with-apache-flink-part-two/)
- [FLINK-6258: Deprecate ListCheckpointed](https://issues.apache.org/jira/browse/FLINK-6258)
- [Checkpointing Under Backpressure](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/checkpointing_under_backpressure/)
