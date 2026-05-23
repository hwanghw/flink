/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * V3: Two-phase count with NO-SHUFFLE local pre-aggregation.
 *
 * <p>Compared to V2 which uses keyBy(key + subtaskIndex) for the local phase
 * (causing a network shuffle), V3 uses a non-keyed ProcessFunction that
 * buffers counts in JVM heap per subtask — zero network cost for the local phase.
 *
 * <pre>
 * V2 data flow (TWO shuffles):
 *   Source → map(add subtaskIdx) → keyBy(key+subtaskIdx) [SHUFFLE] → window → aggregate
 *          → keyBy(key) [SHUFFLE] → window → global merge
 *
 * V3 data flow (ONE shuffle):
 *   Source → LocalPreAggregator (no keyBy, no shuffle) → keyBy(key) [SHUFFLE] → window → global merge
 * </pre>
 *
 * <p>Trade-offs:
 * <ul>
 *   <li>V3 eliminates one network shuffle entirely</li>
 *   <li>V3 uses operator ListState (CheckpointedFunction) to checkpoint local buffers</li>
 *   <li>V3 requires manual watermark tracking and window flushing</li>
 *   <li>V3's local buffer is lost on failure → global phase still correct
 *       because Kafka replays from checkpoint and re-aggregates</li>
 * </ul>
 */
public class TwoPhaseCountDeduplicatedEventTimeV3 {

    private static final OutputTag<Tuple4<String, Integer, Long, Long>> GLOBAL_LATE_TAG =
            new OutputTag<Tuple4<String, Integer, Long, Long>>("global-late-data") {};

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);

        // Reuse the same source from V2
        DataStream<Tuple3<String, String, Long>> rawInput = env
                .addSource(
                        new TwoPhaseCountDeduplicatedEventTimeV2.DelayedEventsSource(),
                        TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {}));

        // Assign event-time timestamps and watermarks
        DataStream<Tuple3<String, String, Long>> input = rawInput
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.f2)
                                .withIdleness(Duration.ofMinutes(1))
                );

        // ============================================================
        // Step 1: LOCAL pre-aggregation — NO keyBy, NO shuffle
        //
        //   Each subtask maintains a JVM HashMap:
        //     Map<windowStart, Map<groupKey, count>>
        //
        //   On each element: increment local count.
        //   When watermark passes window end: flush all local counts
        //   for that window as (key, subtaskIndex, count, windowStart).
        //
        //   Output: Tuple4<groupKey, subtaskIndex, localCount, windowStart>
        //           (windowStart is needed to reassign event timestamps)
        // ============================================================
        SingleOutputStreamOperator<Tuple4<String, Integer, Long, Long>> localCounts = input
                .process(new LocalPreAggregator(
                        Duration.ofMinutes(1).toMillis(),  // window size
                        Duration.ofSeconds(30).toMillis()  // allowed lateness
                ))
                .returns(TypeInformation.of(new TypeHint<Tuple4<String, Integer, Long, Long>>() {}))
                .uid("local-pre-aggregator");

        // Reassign timestamps from the windowStart field so the global
        // window assigns these records to the correct time window.
        DataStream<Tuple4<String, Integer, Long, Long>> timestamped = localCounts
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<String, Integer, Long, Long>>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(5))
                                .withTimestampAssigner((t, ts) -> t.f3) // windowStart as timestamp
                                .withIdleness(Duration.ofMinutes(1))
                );

        // ============================================================
        // Step 2: GLOBAL deduplicated count
        //
        //   Same dedup logic as V2: Map<subtaskIndex, latestCount>
        //   overwrites stale counts from initial fires with updated
        //   counts from re-fires.
        // ============================================================
        SingleOutputStreamOperator<Tuple4<String, Long, Long, Long>> globalCounts = timestamped
                .keyBy(t -> t.f0)  // keyBy groupKey — the ONLY shuffle
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(Duration.ofSeconds(30))
                .sideOutputLateData(GLOBAL_LATE_TAG)
                .process(new GlobalDeduplicatedCountFunction());

        DataStream<Tuple4<String, Integer, Long, Long>> globalLateEvents =
                globalCounts.getSideOutput(GLOBAL_LATE_TAG);

        // Output
        globalCounts.print("RESULT");
        globalLateEvents.print("GLOBAL-LATE");

        env.execute("Two-Phase Count Deduplicated V3 (No-Shuffle Local Pre-Agg)");
    }

    // ================================================================
    // LOCAL PRE-AGGREGATOR — non-keyed, no shuffle
    //
    // Maintains JVM-local buffers per (windowStart, groupKey).
    // Flushes completed windows when watermark advances.
    //
    // This is NOT checkpointed Flink state — it's plain JVM heap.
    // On failure, the buffer is lost, but that's OK: Flink restores
    // from checkpoint, Kafka replays events, and counts rebuild.
    //
    // Input:  Tuple3<groupKey, value, eventTimestamp>
    // Output: Tuple4<groupKey, subtaskIndex, localCount, windowStart>
    //
    // ── TWO-MAP LIFECYCLE ──────────────────────────────────────────
    //
    // Each window goes through three phases, managed by two maps:
    //
    //   windowBuffers (ACCUMULATING phase)
    //     A window lives here while it is still open (watermark has
    //     NOT yet passed windowEnd). Every incoming event for this
    //     window increments the count in this map. No output is
    //     emitted yet — we are purely buffering.
    //
    //     Example state:
    //       windowBuffers = {
    //         0     → {"group1": 5, "group2": 3},   // window [0, 60000)
    //         60000 → {"group1": 2}                  // window [60000, 120000)
    //       }
    //
    //   flushedWindows (LATE-DATA phase)
    //     When the watermark crosses windowEnd, the window is
    //     "flushed": all (key, count) pairs are emitted downstream
    //     and the entry MOVES from windowBuffers → flushedWindows.
    //
    //     The window stays in flushedWindows for up to
    //     allowedLatenessMs after windowEnd. During this period,
    //     late events can still arrive and update the count here.
    //     Each late event triggers an IMMEDIATE re-emission of the
    //     UPDATED TOTAL count (not a delta), so the global phase
    //     can overwrite the stale value via its dedup logic.
    //
    //     Example after flush + 1 late event for group1:
    //       flushedWindows = {
    //         0 → {"group1": 6, "group2": 3}
    //              // was 5 at flush time, late event bumped to 6
    //              // re-emitted (group1, subtask0, 6, 0) downstream
    //       }
    //
    //   (EVICTED)
    //     Once watermark >= windowEnd + allowedLatenessMs, the entry
    //     is removed from flushedWindows entirely. Any subsequent
    //     event for this window is dropped (too late).
    //
    // ── TIMELINE FOR A SINGLE WINDOW [0, 60000) ───────────────────
    //
    //   watermark < 60000:
    //     Event arrives → windowBuffers[0]["group1"]++
    //     No output emitted.
    //
    //   watermark crosses 60000:
    //     flushCompletedWindows() fires:
    //       emit (group1, subtask0, 5, 0) → downstream
    //       emit (group2, subtask0, 3, 0) → downstream
    //       MOVE: windowBuffers[0] → flushedWindows[0]
    //       DELETE: windowBuffers[0]
    //
    //   watermark ∈ [60000, 90000):  (within allowed lateness 30s)
    //     Late event for group1 arrives:
    //       flushedWindows[0]["group1"]++ → now 6
    //       IMMEDIATELY emit (group1, subtask0, 6, 0) → downstream
    //       Global phase receives both (5) and (6), dedup keeps (6).
    //
    //   watermark >= 90000:  (past allowed lateness)
    //     flushedWindows[0] removed.
    //     Any event for window [0, 60000) is now DROPPED.
    //
    // ── WHY RE-EMIT THE TOTAL, NOT A DELTA? ───────────────────────
    //
    //   The global phase uses ACCUMULATING windows with a dedup map:
    //     Map<subtaskIndex, latestCount>
    //   When it receives (group1, subtask0, 6, 0), it overwrites
    //   the previous (group1, subtask0, 5, 0) via map.put(0, 6).
    //   If we emitted a delta (+1 instead of 6), the global phase
    //   would need to SUM, but in accumulating mode it sees ALL
    //   historical records — leading to double-counting.
    //   Emitting the TOTAL makes the global dedup simple: last wins.
    //
    // ── FAILURE RECOVERY ──────────────────────────────────────────
    //
    //   Both maps are checkpointed via operator ListState
    //   (CheckpointedFunction — snapshotState / initializeState).
    //
    //   Checkpoints are NOT aligned to window boundaries — they can
    //   fire at any point mid-window. Without checkpointing the maps,
    //   a restore would replay Kafka only from the checkpoint offset,
    //   losing all counts accumulated between the window start and
    //   the checkpoint, leading to undercounting.
    //
    //   On task failure:
    //     1. Flink restores from latest checkpoint
    //     2. windowBuffers and flushedWindows are restored from
    //        operator ListState (counts up to checkpoint are intact)
    //     3. Kafka source replays from checkpoint offsets
    //     4. Replayed events (after checkpoint) rebuild the remaining
    //        counts on top of the restored maps
    //     5. Total count = (restored pre-checkpoint counts)
    //                    + (replayed post-checkpoint counts) ✓
    //
    // ================================================================
    public static class LocalPreAggregator
            extends ProcessFunction<
            Tuple3<String, String, Long>,
            Tuple4<String, Integer, Long, Long>>
            implements CheckpointedFunction {

        private final long windowSizeMs;
        private final long allowedLatenessMs;

        // ── windowBuffers: OPEN windows still accumulating events ──
        //
        // Structure: Map<windowStart, Map<groupKey, count>>
        //
        // An entry exists here from the first event for a window until
        // the watermark crosses windowEnd. During this time, every event
        // increments the count, but NOTHING is emitted downstream.
        //
        // Checkpointed via checkpointedWindowBuffers (operator ListState).
        // On restore, this map is rebuilt from that state so mid-window
        // counts are not lost when a checkpoint was taken before the
        // window closed.
        private transient Map<Long, Map<String, Long>> windowBuffers;

        // ── flushedWindows: CLOSED windows accepting late data ────
        //
        // Structure: Map<windowStart, Map<groupKey, count>>
        //
        // When a window is flushed (watermark >= windowEnd), its entry
        // moves HERE from windowBuffers. The counts at flush time have
        // already been emitted downstream.
        //
        // Purpose: if a late event arrives for a window that was already
        // flushed, we need to know the PREVIOUS total count so we can
        // increment it and re-emit the UPDATED total (not a delta).
        //
        // Without this map, we'd lose track of what was already emitted
        // and couldn't produce correct updated counts for the global
        // dedup phase.
        //
        // Entries are evicted once watermark >= windowEnd + allowedLateness.
        // After eviction, events for that window are dropped.
        //
        // Checkpointed via checkpointedFlushedWindows (operator ListState).
        private transient Map<Long, Map<String, Long>> flushedWindows;

        // ── Operator state (checkpointed) ──────────────────────────
        //
        // Non-keyed ProcessFunction uses operator ListState, not keyed state.
        // On checkpoint: maps are serialized into these ListState holders.
        // On restore: maps are rebuilt from these holders so mid-window
        // counts survive failure + replay correctly.
        //
        // Why ListState and not ValueState?
        //   Non-keyed operators use OperatorStateStore, which only offers
        //   ListState/UnionListState — there is no ValueState for non-keyed ops.
        //   We store a single element (the whole map) in the list.
        private transient ListState<Map<Long, Map<String, Long>>> checkpointedWindowBuffers;
        private transient ListState<Map<Long, Map<String, Long>>> checkpointedFlushedWindows;

        private transient int subtaskIndex;

        // Track last seen watermark to avoid redundant flush scans.
        // We only scan windowBuffers for flushable windows when the
        // watermark actually advances (not on every element).
        private transient long lastWatermark;

        public LocalPreAggregator(long windowSizeMs, long allowedLatenessMs) {
            this.windowSizeMs = windowSizeMs;
            this.allowedLatenessMs = allowedLatenessMs;
        }

        @Override
        public void open(OpenContext openContext) {
            // windowBuffers and flushedWindows are initialized in initializeState(),
            // which runs before open() on both fresh start and restore.
            subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            lastWatermark = Long.MIN_VALUE;
        }

        @Override
        public void processElement(
                Tuple3<String, String, Long> event,
                Context ctx,
                Collector<Tuple4<String, Integer, Long, Long>> out) {

            long eventTime = event.f2;
            String groupKey = event.f0;
            long windowStart = eventTime - (eventTime % windowSizeMs);
            long windowEnd = windowStart + windowSizeMs;
            long currentWatermark = ctx.timerService().currentWatermark();

            // ── GATE 1: Drop hopelessly late events ──
            // If watermark has passed windowEnd + allowedLateness, this event
            // is too late for both the local buffer AND the global window.
            // The global window would drop it anyway, so discard early.
            if (currentWatermark >= windowEnd + allowedLatenessMs) {
                System.out.printf("[subtask-%d] Dropping late event: key=%s, time=%d, "
                                + "window=[%d,%d), watermark=%d%n",
                        subtaskIndex, groupKey, eventTime,
                        windowStart, windowEnd, currentWatermark);
                return;
            }

            // ── GATE 2: Route to the correct map ──
            // Check if this window was already flushed. This determines
            // whether the event goes to windowBuffers (normal) or
            // flushedWindows (late data path).
            boolean isLateForFlushedWindow = flushedWindows.containsKey(windowStart);

            if (isLateForFlushedWindow) {
                // ── LATE DATA PATH ──
                // Window was already flushed — the initial counts were emitted.
                // Increment the count in flushedWindows and IMMEDIATELY re-emit
                // the UPDATED TOTAL so the global phase can overwrite the stale value.
                //
                // Example: window was flushed with count=5 for group1.
                // Late event arrives → count becomes 6.
                // Emit (group1, subtask0, 6, windowStart) — the global phase
                // sees both (5) and (6) in its accumulating window, dedup picks (6).
                flushedWindows.get(windowStart).merge(groupKey, 1L, Long::sum);
                long updatedCount = flushedWindows.get(windowStart).get(groupKey);
                out.collect(Tuple4.of(groupKey, subtaskIndex, updatedCount, windowStart));
            } else {
                // ── NORMAL PATH ──
                // Window is still open. Just increment the count.
                // No output yet — will be emitted when window is flushed.
                windowBuffers
                        .computeIfAbsent(windowStart, k -> new HashMap<>())
                        .merge(groupKey, 1L, Long::sum);
            }

            // ── FLUSH CHECK ──
            // Only scan for flushable windows when watermark has actually advanced.
            // This avoids O(num_windows) scan on every single element.
            if (currentWatermark > lastWatermark) {
                lastWatermark = currentWatermark;
                flushCompletedWindows(currentWatermark, out);
            }
        }

        /**
         * Scan windowBuffers and flush any window whose end time the watermark
         * has passed. "Flush" means:
         *   1. Emit all (key, count) pairs for the window
         *   2. Move the entry from windowBuffers → flushedWindows
         *   3. Remove from windowBuffers
         *
         * Also evicts entries from flushedWindows that are past allowed lateness.
         */
        private void flushCompletedWindows(
                long watermark,
                Collector<Tuple4<String, Integer, Long, Long>> out) {

            Iterator<Map.Entry<Long, Map<String, Long>>> it =
                    windowBuffers.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<Long, Map<String, Long>> entry = it.next();
                long windowStart = entry.getKey();
                long windowEnd = windowStart + windowSizeMs;

                if (watermark >= windowEnd) {
                    // ── FLUSH: emit all local counts for this window ──
                    Map<String, Long> keyCounts = entry.getValue();
                    for (Map.Entry<String, Long> kc : keyCounts.entrySet()) {
                        out.collect(Tuple4.of(
                                kc.getKey(), subtaskIndex, kc.getValue(), windowStart));
                    }

                    System.out.printf("[subtask-%d] Flushed window [%d, %d): %s%n",
                            subtaskIndex, windowStart, windowEnd, keyCounts);

                    // ── MOVE to flushedWindows ──
                    // Keep the counts around so late events can increment them
                    // and re-emit updated totals. The map reference is moved
                    // (not copied) for efficiency.
                    flushedWindows.put(windowStart, keyCounts);
                    it.remove();
                }
            }

            // ── EVICT expired flushedWindows ──
            // Windows whose allowed lateness has expired can never receive
            // more late data. Remove them to bound memory usage.
            // Without this, flushedWindows would grow unbounded over time.
            flushedWindows.entrySet().removeIf(entry -> {
                long windowEnd = entry.getKey() + windowSizeMs;
                return watermark >= windowEnd + allowedLatenessMs;
            });
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedWindowBuffers.clear();
            checkpointedWindowBuffers.add(new HashMap<>(windowBuffers));
            checkpointedFlushedWindows.clear();
            checkpointedFlushedWindows.add(new HashMap<>(flushedWindows));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            TypeInformation<Map<Long, Map<String, Long>>> mapType =
                    TypeInformation.of(new TypeHint<Map<Long, Map<String, Long>>>() {});

            checkpointedWindowBuffers = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("window-buffers", mapType));
            checkpointedFlushedWindows = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("flushed-windows", mapType));

            windowBuffers = new HashMap<>();
            flushedWindows = new HashMap<>();

            if (context.isRestored()) {
                for (Map<Long, Map<String, Long>> restored : checkpointedWindowBuffers.get()) {
                    windowBuffers.putAll(restored);
                }
                for (Map<Long, Map<String, Long>> restored : checkpointedFlushedWindows.get()) {
                    flushedWindows.putAll(restored);
                }
            }
        }

        @Override
        public void close() throws Exception {
            // On shutdown, log any remaining unflushed windows.
            // In practice, Flink sends a MAX_WATERMARK at end-of-input which
            // triggers flushCompletedWindows() via the last processElement() call.
            // Any remaining buffers here indicate events that arrived but whose
            // window wasn't yet closed when the source finished.
            if (windowBuffers != null && !windowBuffers.isEmpty()) {
                System.out.printf("[subtask-%d] close(): %d unflushed windows remaining%n",
                        subtaskIndex, windowBuffers.size());
            }
        }
    }

    // ================================================================
    // GLOBAL ProcessWindowFunction — same dedup logic as V2
    //
    // Input:  Tuple4<groupKey, subtaskIndex, localCount, windowStart>
    // Output: Tuple4<groupKey, windowStart, windowEnd, totalCount>
    // ================================================================
    public static class GlobalDeduplicatedCountFunction
            extends ProcessWindowFunction<
            Tuple4<String, Integer, Long, Long>,
            Tuple4<String, Long, Long, Long>,
            String,
            TimeWindow> {

        @Override
        public void process(
                String key,
                Context context,
                Iterable<Tuple4<String, Integer, Long, Long>> elements,
                Collector<Tuple4<String, Long, Long, Long>> out) {

            TimeWindow window = context.window();
            long windowStart = window.getStart();
            long windowEnd = window.getEnd();

            // Deduplicate: Map<subtaskIndex, latestCount>
            // In ACCUMULATING mode, elements contains ALL records ever added.
            // For a given subtask, the last entry wins (overwrites stale counts).
            Map<Integer, Long> subtaskCounts = new HashMap<>();

            for (Tuple4<String, Integer, Long, Long> element : elements) {
                int subtaskIndex = element.f1;
                long count = element.f2;
                subtaskCounts.put(subtaskIndex, count);
            }

            long totalCount = 0L;
            for (long c : subtaskCounts.values()) {
                totalCount += c;
            }

            out.collect(Tuple4.of(key, windowStart, windowEnd, totalCount));

            System.out.printf(
                    "[%tT.%<tL] Window [%d, %d) | Key: %s | Subtasks: %s | Total: %d%n",
                    System.currentTimeMillis(),
                    windowStart, windowEnd, key, subtaskCounts, totalCount);
        }
    }
}
