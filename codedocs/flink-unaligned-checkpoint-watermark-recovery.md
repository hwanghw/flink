# Unaligned Checkpoints & Watermark Recovery in Flink

## Problem Statement

From the [Flink docs on checkpointing under backpressure](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/state/checkpointing_under_backpressure/):

> Unaligned checkpoints break with an implicit guarantee in respect to watermarks during recovery.
> Currently, Flink generates the watermark as the first step of recovery instead of storing the latest
> watermark in the operators to ease rescaling. In unaligned checkpoints, that means on recovery, Flink
> generates watermarks after it restores in-flight data. If your pipeline uses an operator that applies
> the latest watermark on each record will produce different results than for aligned checkpoints.

This document explains exactly what this means, traces the relevant Flink source code, and provides a concrete workaround with a code example.

---

## 1. Background: Aligned vs Unaligned Checkpoints

### Aligned Checkpoints (Default)

When a checkpoint barrier arrives at an operator with multiple inputs, Flink **blocks** the channel
that received the barrier first and waits until barriers arrive on **all** input channels. Only then
does it snapshot the state and unblock all channels.

**Source:** `AbstractAlignedBarrierHandlerState.java`
```java
// flink-runtime/.../io/checkpointing/AbstractAlignedBarrierHandlerState.java:52-70
public final BarrierHandlerState barrierReceived(
        Controller controller,
        InputChannelInfo channelInfo,
        CheckpointBarrier checkpointBarrier,
        boolean markChannelBlocked)
        throws IOException, CheckpointException {
    checkState(!checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint());

    if (markChannelBlocked) {
        state.blockChannel(channelInfo);   // ← blocks the channel
    }

    if (controller.allBarriersReceived()) {
        return triggerGlobalCheckpoint(controller, checkpointBarrier);  // ← waits for ALL barriers
    }

    return convertAfterBarrierReceived(state);
}
```

This blocking creates a **clean cut**: everything before the barrier is "pre-checkpoint," everything
after is "post-checkpoint." No in-flight data crosses the boundary.

### Unaligned Checkpoints

Unaligned checkpoints do **not** block channels. When the first barrier arrives, the checkpoint
triggers **immediately**, and the barrier overtakes any buffered data. The in-flight data (records
sitting in network buffers) is captured and saved as part of the checkpoint state.

**Source:** `AlternatingWaitingForFirstBarrierUnaligned.java`
```java
// flink-runtime/.../io/checkpointing/AlternatingWaitingForFirstBarrierUnaligned.java:58-86
public BarrierHandlerState barrierReceived(
        Controller controller,
        InputChannelInfo channelInfo,
        CheckpointBarrier checkpointBarrier,
        boolean markChannelBlocked)
        throws CheckpointException, IOException {

    // Only block if it's actually an out-of-order aligned barrier
    if (markChannelBlocked
            && !checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint()) {
        channelState.blockChannel(channelInfo);
    }

    CheckpointBarrier unalignedBarrier = checkpointBarrier.asUnaligned();
    controller.initInputsCheckpoint(unalignedBarrier);
    for (CheckpointableInput input : channelState.getInputs()) {
        input.checkpointStarted(unalignedBarrier);   // ← start capturing in-flight buffers
    }
    controller.triggerGlobalCheckpoint(unalignedBarrier);  // ← trigger immediately, no waiting
    // ...
}
```

This is why unaligned checkpoints are fast under backpressure — they don't wait for barrier alignment.

---

## 2. The Watermark Problem During Recovery

### Flink Does NOT Store Watermarks in Checkpoint State

The `StatusWatermarkValve` (which tracks the current watermark per input channel) is **not**
checkpointed. On recovery, every subpartition's watermark resets to `Long.MIN_VALUE`:

```java
// flink-runtime/.../watermarkstatus/StatusWatermarkValve.java:125-137
SubpartitionStatus subpartitionStatus = new SubpartitionStatus();
subpartitionStatus.watermark = Long.MIN_VALUE;   // ← reset to minimum on construction
subpartitionStatus.watermarkStatus = WatermarkStatus.ACTIVE;
// ...
this.lastOutputWatermark = Long.MIN_VALUE;
```

The watermark must be **regenerated** from the data stream after recovery.

### Recovery Sequence: Where the Ordering Breaks

During recovery, Flink processes data in this order:

1. **First**: Restore in-flight data (the buffered records captured by unaligned checkpoint)
2. **Then**: Process `EndOfOutputChannelStateEvent` to signal recovery is complete
3. **Then**: Switch to normal input processing
4. **Then**: Start receiving live data from upstream, which carries fresh watermarks

**Source:** `AbstractStreamTaskNetworkInput.java`
```java
// flink-runtime/.../io/AbstractStreamTaskNetworkInput.java:280-283
} else if (event.getClass() == EndOfOutputChannelStateEvent.class) {
    if (checkpointedInputGate.allChannelsRecovered()) {
        return DataInputStatus.END_OF_RECOVERY;
    }
}
```

**Source:** `StreamOneInputProcessor.java`
```java
// flink-streaming-java/.../io/StreamOneInputProcessor.java:70-74
} else if (status == DataInputStatus.END_OF_RECOVERY) {
    if (input instanceof RecoverableStreamTaskInput) {
        input = ((RecoverableStreamTaskInput<IN>) input).finishRecovery();
    }
    return DataInputStatus.MORE_AVAILABLE;
}
```

### The Ordering Inversion Visualized

```
ALIGNED CHECKPOINT & RECOVERY:

  Checkpoint boundary (clean cut)
  ────────|──────────────────────
  ... R5  |  ← nothing in flight
          |
  Recovery:
  [restore state] → [W=100 from upstream] → [R6, R7, R8...]
                     ^ watermark BEFORE records ✓


UNALIGNED CHECKPOINT & RECOVERY:

  Checkpoint boundary (barrier overtakes buffers)
  ──────|──R3──R4──R5──────────
        |  ↑ in-flight, saved in checkpoint
        |
  Recovery:
  [restore state] → [replay R3,R4,R5] → [W=100 from upstream] → [R6, R7, R8...]
                     ^ watermark = Long.MIN_VALUE here!   ^ watermark arrives late
```

### Why This Matters

If your operator's logic depends on the **current watermark value** when processing each record,
it will behave differently after unaligned checkpoint recovery:

- A window operator that checks `watermark >= window.end` to decide if a record is late
- A process function that reads `timerService.currentWatermark()` and uses it in business logic
- Any operator that applies the latest watermark to each record

During aligned checkpoint recovery, the watermark arrives before records, so behavior matches
normal processing. During unaligned checkpoint recovery, records arrive **before** the watermark
catches up, so `currentWatermark()` returns `Long.MIN_VALUE` during replay of in-flight data.

This can cause:
- Records incorrectly treated as "on time" when they should be "late"
- Different output compared to aligned checkpoint recovery
- Non-deterministic behavior depending on checkpoint mode

---

## 3. Watermark Alignment in StatusWatermarkValve

The `StatusWatermarkValve` distinguishes between **aligned** and **unaligned** subpartitions when
computing the output watermark:

```java
// flink-runtime/.../watermarkstatus/StatusWatermarkValve.java:165-186
if (lastOutputWatermarkStatus.isActive() && subpartitionStatus.watermarkStatus.isActive()) {
    long watermarkMillis = watermark.getTimestamp();

    if (watermarkMillis > subpartitionStatus.watermark) {
        subpartitionStatus.watermark = watermarkMillis;

        if (subpartitionStatus.isWatermarkAligned) {
            adjustAlignedSubpartitionStatuses(subpartitionStatus);
        } else if (watermarkMillis >= lastOutputWatermark) {
            // previously unaligned subpartitions are now aligned if watermark has caught up
            markWatermarkAligned(subpartitionStatus);
        }

        // now, attempt to find a new min watermark across all aligned subpartitions
        findAndOutputNewMinWatermarkAcrossAlignedSubpartitions(output);
    }
}
```

Only **aligned** subpartitions contribute to the minimum watermark output. A subpartition transitions
from unaligned to aligned when its watermark catches up to `lastOutputWatermark`.

---

## 4. The Workaround: Store Watermarks in Union State

### Why Union State?

The docs recommend storing the watermark in **union state** (`getUnionListState`):

- **Regular list state**: On rescaling (e.g., 4→8 subtasks), each new subtask sees only a fraction
  of the old state entries. Watermarks could be lost.
- **Union state**: On rescaling, **every** new subtask receives **all** entries from all old subtasks.
  Each subtask can then pick the minimum watermark and correctly initialize itself.

### Why Per Key Group?

Different key groups may have different event-time progress. Storing per key group ensures that
after rescaling, each new subtask can restore the correct watermark for the keys it is now
responsible for.

---

## 5. Real-World Example from Flink Codebase: WindowAggOperator

The `WindowAggOperator` in `flink-table-runtime` uses exactly this pattern:

```java
// flink-table/flink-table-runtime/.../window/tvf/common/WindowAggOperator.java:122, 183-206

/** The operator state to store watermark. */
private transient ListState<Long> watermarkState;

@Override
public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    ListStateDescriptor<Long> watermarkStateDesc =
            new ListStateDescriptor<>("watermark", LongSerializer.INSTANCE);
    this.watermarkState = context.getOperatorStateStore().getUnionListState(watermarkStateDesc);
    if (context.isRestored()) {
        Iterable<Long> watermarks = watermarkState.get();
        if (watermarks != null) {
            long minWatermark = Long.MAX_VALUE;
            for (Long watermark : watermarks) {
                minWatermark = Math.min(watermark, minWatermark);
            }
            if (minWatermark != Long.MAX_VALUE) {
                this.currentWatermark = minWatermark;
            }
        }
    }
}

@Override
public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    this.watermarkState.update(Collections.singletonList(currentWatermark));
}
```

The key points:
1. **`getUnionListState`** ensures all subtasks see all watermarks on rescale
2. **`Math.min`** across all restored watermarks picks the safe lower bound
3. **`snapshotState`** saves the current watermark as a single-element list
4. **`initializeWatermark`** (in `open()`) pushes the restored watermark into the timer service

---

## 6. Runnable Code Example: WatermarkAwareDeduplicator

A complete, self-contained runnable example is at:

**`flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/WatermarkAwareDeduplicator.java`**

This example demonstrates the full pattern in a DataStream API `KeyedProcessFunction`:

- **`WatermarkAwareDeduplicateFunction`** — implements `CheckpointedFunction` with union state watermark persistence
- **`DelayedEventsSource`** — emits two batches with a sleep in between (same pattern as `TwoPhaseCountDeduplicatedEventTimeV2`)
- Unaligned checkpoints are enabled in `main()`
- Late events are dropped using `Math.max(restoredWatermark, timerService.currentWatermark())`
- Cleanup timers clear keyed state after a TTL

### The core three-step pattern:

```java
// 1. initializeState: declare union state, restore min watermark
ListStateDescriptor<Long> watermarkDesc =
        new ListStateDescriptor<>("watermark", LongSerializer.INSTANCE);
this.watermarkState = context.getOperatorStateStore().getUnionListState(watermarkDesc);
if (context.isRestored()) {
    long minWatermark = Long.MAX_VALUE;
    for (Long wm : watermarkState.get()) {
        minWatermark = Math.min(wm, minWatermark);
    }
    if (minWatermark != Long.MAX_VALUE) {
        this.currentWatermark = minWatermark;
    }
}

// 2. snapshotState: save current watermark
watermarkState.update(Collections.singletonList(currentWatermark));

// 3. processElement: use effective watermark
long effectiveWatermark = Math.max(currentWatermark, ctx.timerService().currentWatermark());
```

---

## 7. Summary Table

| Aspect | Aligned Checkpoints | Unaligned Checkpoints |
|--------|--------------------|-----------------------|
| **Channel blocking** | Blocks on barrier | No blocking |
| **In-flight data** | Not captured | Captured via ChannelStateWriter |
| **Checkpoint speed** | Slow under backpressure | Fast (independent of backpressure) |
| **Recovery: watermark order** | Watermark arrives BEFORE records | Records replayed BEFORE watermark |
| **Watermark state** | Not stored (regenerated naturally) | Must be stored manually for correctness |
| **Workaround needed?** | No | Yes, if operator depends on current watermark per record |

## Key Source Files

| File | Purpose |
|------|---------|
| `flink-runtime/.../watermarkstatus/StatusWatermarkValve.java` | Watermark tracking per subpartition, alignment logic |
| `flink-runtime/.../io/checkpointing/AbstractAlignedBarrierHandlerState.java` | Aligned checkpoint barrier handling (blocks channels) |
| `flink-runtime/.../io/checkpointing/AlternatingWaitingForFirstBarrierUnaligned.java` | Unaligned checkpoint barrier handling (immediate trigger) |
| `flink-runtime/.../io/AbstractStreamTaskNetworkInput.java` | Recovery event processing (END_OF_RECOVERY) |
| `flink-runtime/.../io/StreamOneInputProcessor.java` | Switches from recovery input to normal input |
| `flink-table/.../window/tvf/common/WindowAggOperator.java` | Real-world union state watermark pattern |
| `flink-end-to-end-tests/.../SequenceGeneratorSource.java` | Another union state watermark example |
