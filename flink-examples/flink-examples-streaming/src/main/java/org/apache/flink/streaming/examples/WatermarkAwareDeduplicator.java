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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;

/**
 * Demonstrates how to persist watermarks in union state to make a {@link KeyedProcessFunction}
 * safe for use with unaligned checkpoints.
 *
 * <h2>Problem</h2>
 *
 * <p>Unaligned checkpoints capture in-flight records as part of the checkpoint. On recovery,
 * these in-flight records are replayed <em>before</em> any new watermarks arrive from upstream.
 * This means {@code ctx.timerService().currentWatermark()} returns {@code Long.MIN_VALUE} during
 * replay, causing operators that depend on watermarks (e.g., for late-event detection) to produce
 * different results than with aligned checkpoints.
 *
 * <h2>Workaround</h2>
 *
 * <p>Store the current watermark in <strong>union list state</strong>. On recovery, restore the
 * minimum watermark from all subtask entries and use it as the effective watermark until the
 * timer service's watermark catches up. Union state ensures correct behavior during rescaling
 * because all entries are broadcast to every new subtask.
 *
 * <p>See: <a href="https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/state/checkpointing_under_backpressure/">
 * Checkpointing under Backpressure</a>
 *
 * <h2>What this example does</h2>
 *
 * <ol>
 *   <li>Emits events in two batches with a sleep in between (simulating late data)</li>
 *   <li>Deduplicates events by key, emitting only the first occurrence</li>
 *   <li>Drops late events using the persisted watermark (safe for unaligned checkpoints)</li>
 *   <li>Registers cleanup timers to clear keyed state after a TTL</li>
 * </ol>
 */
public class WatermarkAwareDeduplicator {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("taskmanager.memory.network.min", "256mb");
        configuration.setString("taskmanager.memory.network.max", "256mb");
        configuration.setString("taskmanager.memory.network.fraction", "0.2");

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);

        // ============================================================
        // Enable unaligned checkpoints -- the scenario this example
        // is designed to handle correctly.
        // ============================================================
        env.enableCheckpointing(10_000); // every 10 seconds
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        // ============================================================
        // Source: two batches of events with a sleep in between
        // ============================================================
        DataStream<Event> events =
                env.addSource(new DelayedEventsSource())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(5))
                                        .withTimestampAssigner(
                                                (event, ts) -> event.getTimestamp())
                                        .withIdleness(Duration.ofMinutes(1)));

        // ============================================================
        // Deduplicate by key, dropping late events using the
        // persisted watermark (safe across unaligned checkpoint
        // recovery).
        // ============================================================
        DataStream<Event> deduplicated =
                events.keyBy(Event::getKey)
                        .process(new WatermarkAwareDeduplicateFunction());

        deduplicated.print("DEDUP");

        env.execute("Watermark-Aware Deduplicator (Unaligned Checkpoint Safe)");
    }

    // ================================================================
    // KeyedProcessFunction that persists watermarks in union state
    //
    // This is the core pattern. On recovery from an unaligned
    // checkpoint, in-flight records are replayed before any new
    // watermark arrives. Without persisting the watermark, the
    // function would see currentWatermark() == Long.MIN_VALUE
    // during replay, incorrectly treating late events as on-time.
    //
    // Pattern (from WindowAggOperator in flink-table-runtime):
    //   - initializeState: getUnionListState + restore min watermark
    //   - snapshotState:   save currentWatermark as single entry
    //   - processElement:  use max(restored, timerService) watermark
    // ================================================================
    public static class WatermarkAwareDeduplicateFunction
            extends KeyedProcessFunction<String, Event, Event>
            implements CheckpointedFunction {

        private static final long TTL_MILLIS = 60_000L; // 1 minute

        /** Keyed state: whether we've already emitted for this key. */
        private transient ValueState<Boolean> seenState;

        /**
         * Operator state (union): persists the latest watermark across checkpoints. Union
         * redistribution broadcasts all entries to all subtasks on rescale, so each subtask
         * can pick the safe minimum.
         */
        private transient ListState<Long> watermarkState;

        /** The restored/tracked watermark, available immediately after recovery. */
        private long currentWatermark = Long.MIN_VALUE;

        // ─── CheckpointedFunction ─────────────────────────────────────

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // Declare watermark as UNION list state so that on rescale,
            // every new subtask receives ALL entries from ALL old subtasks.
            ListStateDescriptor<Long> watermarkDesc =
                    new ListStateDescriptor<>("watermark", LongSerializer.INSTANCE);
            this.watermarkState =
                    context.getOperatorStateStore().getUnionListState(watermarkDesc);

            // On restore: take the MINIMUM across all subtasks' watermarks.
            // This is the safe lower bound -- no event with a timestamp below
            // this value could have been in-flight at checkpoint time.
            if (context.isRestored()) {
                long minWatermark = Long.MAX_VALUE;
                for (Long wm : watermarkState.get()) {
                    minWatermark = Math.min(wm, minWatermark);
                }
                if (minWatermark != Long.MAX_VALUE) {
                    this.currentWatermark = minWatermark;
                }
                System.out.printf(
                        "[RESTORE] Restored watermark = %d from union state%n",
                        currentWatermark);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // Save the current watermark as a single entry.
            // On rescale, union redistribution broadcasts this to all new subtasks.
            watermarkState.update(Collections.singletonList(currentWatermark));
        }

        // ─── KeyedProcessFunction ─────────────────────────────────────

        @Override
        public void open(OpenContext openContext) throws Exception {
            seenState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("seen", Boolean.class));
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<Event> out)
                throws Exception {

            // Use the MAX of the restored watermark and the timer service's
            // watermark. During normal operation, the timer service is
            // authoritative. During unaligned checkpoint recovery, the timer
            // service returns Long.MIN_VALUE while our restored value is correct.
            long effectiveWatermark =
                    Math.max(currentWatermark, ctx.timerService().currentWatermark());

            // Drop late events
            if (event.getTimestamp() < effectiveWatermark) {
                System.out.printf(
                        "[LATE] Dropping event: key=%s ts=%d (watermark=%d)%n",
                        event.getKey(), event.getTimestamp(), effectiveWatermark);
                return;
            }

            // Deduplicate: emit only the first occurrence per key
            if (seenState.value() == null) {
                seenState.update(true);
                out.collect(event);

                // Register a cleanup timer to clear state after TTL
                ctx.timerService()
                        .registerEventTimeTimer(event.getTimestamp() + TTL_MILLIS);
            } else {
                System.out.printf(
                        "[DUP] Skipping duplicate: key=%s ts=%d%n",
                        event.getKey(), event.getTimestamp());
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Event> out)
                throws Exception {
            // Timer fires when watermark passes (event.timestamp + TTL).
            // Update our tracked watermark and clear the keyed state.
            currentWatermark = Math.max(currentWatermark, timestamp);
            seenState.clear();
            System.out.printf(
                    "[TIMER] Cleared state for key=%s at watermark=%d%n",
                    ctx.getCurrentKey(), timestamp);
        }
    }

    // ================================================================
    // Event POJO
    // ================================================================
    public static class Event {

        private String key;
        private long timestamp;
        private String payload;

        public Event() {}

        public Event(String key, long timestamp, String payload) {
            this.key = key;
            this.timestamp = timestamp;
            this.payload = payload;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public String getPayload() {
            return payload;
        }

        public void setPayload(String payload) {
            this.payload = payload;
        }

        @Override
        public String toString() {
            return String.format("Event{key='%s', ts=%d, payload='%s'}", key, timestamp, payload);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Event event = (Event) o;
            return timestamp == event.timestamp
                    && Objects.equals(key, event.key)
                    && Objects.equals(payload, event.payload);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, timestamp, payload);
        }
    }

    // ================================================================
    // Custom SourceFunction that emits events in two batches with a
    // sleep in between, ensuring the watermark advances before late
    // events arrive. This mirrors the pattern in
    // TwoPhaseCountDeduplicatedEventTimeV2.DelayedEventsSource.
    //
    // Batch 1: Normal events in window [0, 60000)
    //   + watermark-advancing events at t=70000
    //   → watermark = 70000 - 5000 = 65000
    //
    // --- Thread.sleep(5 seconds) ---
    //
    // Batch 2: Late events (ts < 65000, some already seen keys)
    //   + duplicate keys to test deduplication
    // ================================================================
    public static class DelayedEventsSource implements SourceFunction<Event> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // Batch 1: Normal events
            ctx.collect(new Event("user-1", 1000L, "click"));
            ctx.collect(new Event("user-2", 2000L, "click"));
            ctx.collect(new Event("user-3", 3000L, "click"));
            ctx.collect(new Event("user-1", 4000L, "scroll")); // duplicate key, different ts
            ctx.collect(new Event("user-4", 5000L, "click"));
            ctx.collect(new Event("user-5", 10000L, "click"));

            // Push watermark past 60000: watermark = 70000 - 5000 = 65000
            ctx.collect(new Event("user-1", 70000L, "pageview"));
            ctx.collect(new Event("user-2", 70000L, "pageview"));

            System.out.println("[SOURCE] Emitted batch 1 (normal events + watermark advance)");

            // Sleep to let watermark propagate and timers fire
            Thread.sleep(5000);
            if (!isRunning) {
                return;
            }

            // Batch 2: Late events (ts < watermark 65000)
            // These should be dropped by the watermark-aware function
            ctx.collect(new Event("user-6", 500L, "late-click"));
            ctx.collect(new Event("user-7", 2500L, "late-click"));

            // Duplicate key that was already emitted (user-3)
            // Even if not late, should be deduplicated
            ctx.collect(new Event("user-3", 68000L, "click-again"));

            System.out.println("[SOURCE] Emitted batch 2 (late + duplicate events)");

            // Let the pipeline process before source finishes
            Thread.sleep(3000);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
