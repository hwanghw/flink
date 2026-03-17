package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.flink.configuration.Configuration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class TwoPhaseCountDeduplicatedEventTimeV2 {

    // ================================================================
    // Side output tags for late data at each phase
    // ================================================================
    private static final OutputTag<Tuple3<String, Integer, String>> LOCAL_LATE_TAG =
            new OutputTag<Tuple3<String, Integer, String>>("local-late-data") {};

    private static final OutputTag<Tuple3<String, Integer, Long>> GLOBAL_LATE_TAG =
            new OutputTag<Tuple3<String, Integer, Long>>("global-late-data") {};

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("taskmanager.memory.network.min", "256mb");
        configuration.setString("taskmanager.memory.network.max", "256mb");
        configuration.setString("taskmanager.memory.network.fraction", "0.2");

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);

        // Example input: (key, value, eventTimestamp)
        // Custom source with a sleep between normal and late events
        // to ensure the window fires before the late events arrive
        DataStream<Tuple3<String, String, Long>> rawInput = env
                .addSource(new DelayedEventsSource(),
                        TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {}));

        // ============================================================
        // Assign event-time timestamps and watermarks
        // ============================================================
        DataStream<Tuple3<String, String, Long>> input = rawInput
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.f2)
                                .withIdleness(Duration.ofMinutes(1))
                );

        // ============================================================
        // Step 1: LOCAL count (pre-aggregation per subtask)
        //
        //   Output: Tuple3<groupKey, subtaskIndex, localCount>
        //
        //   The subtaskIndex is preserved in the output so the global
        //   phase can track which subtask produced each partial count
        //   and deduplicate on re-fires.
        // ============================================================
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> localCounts = input
                .map(new RichMapFunction<
                        Tuple3<String, String, Long>,
                        Tuple3<String, Integer, String>>() {
                    @Override
                    public Tuple3<String, Integer, String> map(
                            Tuple3<String, String, Long> value) {
                        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
                        return Tuple3.of(value.f0, subtaskIndex, value.f1);
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(Duration.ofSeconds(30))
                .sideOutputLateData(LOCAL_LATE_TAG)
                .aggregate(new LocalCountAggregate());

        // Capture events too late even for local allowedLateness
        DataStream<Tuple3<String, Integer, String>> localLateEvents =
                localCounts.getSideOutput(LOCAL_LATE_TAG);

        // ============================================================
        // Step 2: GLOBAL deduplicated count with window info
        //
        //   Uses a ProcessWindowFunction that:
        //   - Accesses window boundaries via context.window()
        //   - Maintains Map<subtaskIndex, latestCount> to deduplicate
        //   - Emits Tuple4<groupKey, windowStart, windowEnd, count>
        // ============================================================
        SingleOutputStreamOperator<Tuple4<String, Long, Long, Long>> globalCounts = localCounts
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(Duration.ofSeconds(30))
                .sideOutputLateData(GLOBAL_LATE_TAG)
                .process(new GlobalDeduplicatedCountFunction());

        // Capture partial counts too late for the global window
        DataStream<Tuple3<String, Integer, Long>> globalLateEvents =
                globalCounts.getSideOutput(GLOBAL_LATE_TAG);

        // ============================================================
        // Output streams
        // ============================================================
        globalCounts.print("RESULT");
        localLateEvents.print("LOCAL-LATE");
        globalLateEvents.print("GLOBAL-LATE");

        env.execute("Two-Phase Count Deduplicated V2 (Event Time)");
    }

    // ================================================================
    // LOCAL aggregate
    //
    // Input:  Tuple3<groupKey, subtaskIndex, value>
    // Acc:    Tuple3<groupKey, subtaskIndex, count>
    // Output: Tuple3<groupKey, subtaskIndex, localCount>
    // ================================================================
    public static class LocalCountAggregate
            implements AggregateFunction<
            Tuple3<String, Integer, String>,
            Tuple3<String, Integer, Long>,
            Tuple3<String, Integer, Long>> {

        @Override
        public Tuple3<String, Integer, Long> createAccumulator() {
            return Tuple3.of(null, -1, 0L);
        }

        @Override
        public Tuple3<String, Integer, Long> add(
                Tuple3<String, Integer, String> value,
                Tuple3<String, Integer, Long> accumulator) {
            accumulator.f0 = value.f0;       // groupKey
            accumulator.f1 = value.f1;       // subtaskIndex
            accumulator.f2 += 1L;            // increment count
            return accumulator;
        }

        @Override
        public Tuple3<String, Integer, Long> getResult(
                Tuple3<String, Integer, Long> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple3<String, Integer, Long> merge(
                Tuple3<String, Integer, Long> a,
                Tuple3<String, Integer, Long> b) {
            return Tuple3.of(a.f0, a.f1, a.f2 + b.f2);
        }
    }

    // ================================================================
    // GLOBAL ProcessWindowFunction with deduplication + window info
    //
    // Input:  Tuple3<groupKey, subtaskIndex, localCount>
    // Output: Tuple4<groupKey, windowStart, windowEnd, totalCount>
    //
    // Deduplication logic:
    //   - Iterates all elements in the window (ACCUMULATING mode
    //     means ALL elements ever added are present on every fire)
    //   - Builds Map<subtaskIndex, latestCount>
    //   - Map.put() overwrites stale counts from initial fires
    //     with updated counts from re-fires
    //   - Final count = sum of all map values
    //
    // Window info:
    //   - context.window() provides the TimeWindow for this
    //     invocation, guaranteeing elements are scoped to
    //     exactly one window
    //   - Window start/end are included in the output
    // ================================================================
    public static class GlobalDeduplicatedCountFunction
            extends ProcessWindowFunction<
            Tuple3<String, Integer, Long>,
            Tuple4<String, Long, Long, Long>,
            String,
            TimeWindow> {

        @Override
        public void process(
                String key,
                Context context,
                Iterable<Tuple3<String, Integer, Long>> elements,
                Collector<Tuple4<String, Long, Long, Long>> out) {

            // Access the window this invocation belongs to
            TimeWindow window = context.window();
            long windowStart = window.getStart();
            long windowEnd = window.getEnd();

            // Deduplicate: Map<subtaskIndex, latestCount>
            // In ACCUMULATING mode, elements contains ALL records
            // ever added to this window. For a given subtask:
            //   - Initial fire added ("group1", 0, 3)
            //   - Re-fire added    ("group1", 0, 4)
            //   - Both are present in elements
            //   - Iterating in order, put(0, 3) then put(0, 4)
            //   - Map ends up with {0: 4} — correct latest value
            Map<Integer, Long> subtaskCounts = new HashMap<>();

            for (Tuple3<String, Integer, Long> element : elements) {
                int subtaskIndex = element.f1;
                long count = element.f2;
                subtaskCounts.put(subtaskIndex, count);
            }

            // Sum the deduplicated counts from all subtasks
            long totalCount = 0L;
            for (long c : subtaskCounts.values()) {
                totalCount += c;
            }

            out.collect(Tuple4.of(key, windowStart, windowEnd, totalCount));

            // Optional: log for debugging / monitoring
            System.out.printf(
                    "[%tT.%<tL] Window [%d, %d) | Key: %s | Subtasks: %s | Total: %d%n",
                    System.currentTimeMillis(),
                    windowStart, windowEnd, key, subtaskCounts, totalCount);
        }
    }

    // ================================================================
    // Custom SourceFunction that emits events in two batches with a
    // sleep in between, ensuring the window fires (initial fire)
    // before the late events arrive (triggering a re-fire).
    //
    // Batch 1: Normal events + watermark-advancing events
    // --- Thread.sleep(5 seconds) ---
    // Batch 2: Late events (within allowed lateness)
    // ================================================================
    public static class DelayedEventsSource
            implements SourceFunction<Tuple3<String, String, Long>> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
            // Batch 1: Normal events in window [0, 60000)
            ctx.collect(Tuple3.of("group1", "event", 1000L));
            ctx.collect(Tuple3.of("group1", "event", 2000L));
            ctx.collect(Tuple3.of("group1", "event", 3000L));
            ctx.collect(Tuple3.of("group1", "event", 4000L));
            ctx.collect(Tuple3.of("group1", "event", 5000L));
            ctx.collect(Tuple3.of("group2", "event", 1000L));
            ctx.collect(Tuple3.of("group2", "event", 2000L));
            ctx.collect(Tuple3.of("group2", "event", 3000L));

            // Events that push the watermark past 60000
            // watermark = 70000 - 5000 = 65000 > 60000 → window fires
            ctx.collect(Tuple3.of("group1", "event", 70000L));
            ctx.collect(Tuple3.of("group2", "event", 70000L));

            System.out.println("Emitted batch 1 events");
            // Sleep to ensure the watermark propagates and the
            // window [0, 60000) fires with the initial counts
            // before the late events arrive
            Thread.sleep(5000);

            if (!isRunning) {
                return;
            }

            // Batch 2: Late events — still within allowedLateness
            // (watermark ~65000 < 60000 + 30000 = 90000)
            ctx.collect(Tuple3.of("group1", "event", 500L));
            ctx.collect(Tuple3.of("group2", "event", 200L));

            // Sleep a bit to let re-fire complete before source finishes
            Thread.sleep(2000);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
