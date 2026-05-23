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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Compares two approaches for computing count/sum/avg on skewed data (hot keys):
 *
 * <h2>Approach 1: Two-Phase keyBy (Recommended)</h2>
 *
 * <pre>
 *   Phase 1: keyBy(url + "#" + subtaskId)  → window → local aggregate
 *     Subtask 0: google.com#0  → (count=25, sum=2500)
 *     Subtask 1: google.com#1  → (count=30, sum=3100)
 *     Subtask 2: google.com#2  → (count=28, sum=2800)
 *     Subtask 3: google.com#3  → (count=22, sum=2200)
 *
 *   Phase 2: keyBy(url) → window → merge partials
 *     google.com → count=105, sum=10600, avg=100.95
 * </pre>
 *
 * <h2>Approach 2: Custom Partitioner</h2>
 *
 * <pre>
 *   partitionCustom(hotKeyRouter)  → local accumulation (no KeyedState)
 *     Hot keys (google.com) → round-robin across subtasks
 *     Other keys → hash-partition
 *
 *   Then: keyBy(url) → window → merge partials (same as above)
 * </pre>
 *
 * <h2>Trade-off Comparison</h2>
 *
 * <pre>
 *   | Aspect                  | Two-Phase keyBy        | Custom Partitioner      |
 *   |-------------------------|------------------------|-------------------------|
 *   | KeyedState in local agg | Yes (automatic)        | No (manual HashMap)     |
 *   | Checkpointed?           | Yes (automatic)        | No (lost on failure!)   |
 *   | State TTL               | Yes                    | Must manage manually    |
 *   | Rescaling               | Automatic              | Must handle manually    |
 *   | Knows hot keys?         | No (spreads all keys)  | Yes (must know or guess)|
 *   | Code complexity         | Simple                 | More complex            |
 *   | Performance             | Same                   | Same                    |
 *   | Verdict                 | USE THIS               | Only if you need to     |
 *   |                         |                        | control routing logic   |
 * </pre>
 *
 * <p><b>Bottom line:</b> Two-phase keyBy is almost always better because you get KeyedState,
 * automatic checkpointing, TTL, and rescaling for free. Custom partitioner is only useful when
 * you need explicit control over which subtask processes which data (e.g., routing to subtasks
 * with GPUs, co-locating related keys, or respecting external partition assignments).
 */
public class SkewedAggregation {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);

        DataStream<PageViewEvent> events =
                env.addSource(new SkewedPageViewSource())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<PageViewEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(5))
                                        .withTimestampAssigner(
                                                (event, ts) -> event.getTimestamp())
                                        .withIdleness(Duration.ofMinutes(1)));

        // ============================================================
        // Toggle between approaches by uncommenting one:
        // ============================================================

        // Approach 1: Two-Phase keyBy (recommended)
        DataStream<Tuple6<String, Long, Long, Long, Long, Double>> results =
                twoPhaseKeyBy(events);

        // Approach 2: Custom Partitioner
        // DataStream<Tuple6<String, Long, Long, Long, Long, Double>> results =
        //         customPartitioner(events);

        results.print("RESULT");
        env.execute("Skewed Aggregation (count/sum/avg)");
    }

    // ================================================================
    //  APPROACH 1: Two-Phase keyBy  (RECOMMENDED)
    //
    //  Phase 1: keyBy(url + "#" + subtaskId) → window → local aggregate
    //    - Uses AggregateFunction (incremental, memory-efficient)
    //    - KeyedState is automatic → checkpointed, rescalable
    //    - Each subtask computes partial count/sum for its shard
    //
    //  Phase 2: keyBy(url) → window → merge
    //    - Only N partial results per URL per window (lightweight)
    //    - Dedup by subtaskId for correctness on re-fires
    //    - Compute total count, sum, avg
    // ================================================================
    public static DataStream<Tuple6<String, Long, Long, Long, Long, Double>> twoPhaseKeyBy(
            DataStream<PageViewEvent> events) {

        // Phase 1: local aggregation per (url, subtaskId)
        SingleOutputStreamOperator<Tuple4<String, Integer, Long, Long>> localAgg =
                events.map(
                                new RichMapFunction<PageViewEvent, PageViewEvent>() {
                                    private int subtaskId;

                                    @Override
                                    public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
                                        subtaskId =
                                                getRuntimeContext()
                                                        .getTaskInfo()
                                                        .getIndexOfThisSubtask();
                                    }

                                    @Override
                                    public PageViewEvent map(PageViewEvent event) {
                                        // Tag with subtask ID for Phase 2 dedup
                                        event.setSubtaskId(subtaskId);
                                        return event;
                                    }
                                })
                        .keyBy(
                                event ->
                                        event.getUrl()
                                                + "#"
                                                + event.getSubtaskId())
                        .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                        .aggregate(new LocalAggregateFunction());

        // Phase 2: merge partials from all subtasks
        return localAgg
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .process(new MergePartialsFunction());
    }

    /** Phase 1 aggregate: computes partial (url, subtaskId, count, sum). */
    public static class LocalAggregateFunction
            implements AggregateFunction<
                    PageViewEvent, LocalAggregateFunction.Acc, Tuple4<String, Integer, Long, Long>> {

        public static class Acc {
            String url;
            int subtaskId = -1;
            long count;
            long sum;
        }

        @Override
        public Acc createAccumulator() {
            return new Acc();
        }

        @Override
        public Acc add(PageViewEvent event, Acc acc) {
            acc.url = event.getUrl();
            acc.subtaskId = event.getSubtaskId();
            acc.count++;
            acc.sum += event.getResponseTimeMs();
            return acc;
        }

        @Override
        public Tuple4<String, Integer, Long, Long> getResult(Acc acc) {
            return Tuple4.of(acc.url, acc.subtaskId, acc.count, acc.sum);
        }

        @Override
        public Acc merge(Acc a, Acc b) {
            a.count += b.count;
            a.sum += b.sum;
            return a;
        }
    }

    /**
     * Phase 2: merge partial (count, sum) from all subtask shards.
     *
     * <p>Same dedup-by-subtaskId pattern as {@link
     * TwoPhaseCountDeduplicatedEventTimeV2.GlobalDeduplicatedCountFunction}.
     */
    public static class MergePartialsFunction
            extends ProcessWindowFunction<
                    Tuple4<String, Integer, Long, Long>,
                    Tuple6<String, Long, Long, Long, Long, Double>,
                    String,
                    TimeWindow> {

        @Override
        public void process(
                String url,
                Context context,
                Iterable<Tuple4<String, Integer, Long, Long>> elements,
                Collector<Tuple6<String, Long, Long, Long, Long, Double>> out) {

            TimeWindow window = context.window();

            // Dedup by subtaskId: keep latest partial per subtask
            // (handles re-fires from allowed lateness)
            Map<Integer, long[]> shardPartials = new HashMap<>();
            for (Tuple4<String, Integer, Long, Long> el : elements) {
                shardPartials.put(el.f1, new long[] {el.f2, el.f3}); // [count, sum]
            }

            long totalCount = 0;
            long totalSum = 0;
            for (long[] partial : shardPartials.values()) {
                totalCount += partial[0];
                totalSum += partial[1];
            }
            double avg = totalCount > 0 ? (double) totalSum / totalCount : 0.0;

            out.collect(
                    Tuple6.of(
                            url,
                            window.getStart(),
                            window.getEnd(),
                            totalCount,
                            totalSum,
                            avg));

            System.out.printf(
                    "[TWO-PHASE] Window [%d, %d) | %s | shards=%d | count=%d sum=%d avg=%.2f%n",
                    window.getStart(),
                    window.getEnd(),
                    url,
                    shardPartials.size(),
                    totalCount,
                    totalSum,
                    avg);
        }
    }

    // ================================================================
    //  APPROACH 2: Custom Partitioner
    //
    //  Step 1: partitionCustom → routes hot keys round-robin,
    //          other keys hash-partitioned normally.
    //
    //  Step 2: RichFlatMapFunction → local accumulation in a
    //          transient HashMap (NOT KeyedState — no keyBy here).
    //          Flushes partials in close() or when buffer is large.
    //
    //  Step 3: keyBy(url) → window → merge (same as Approach 1)
    //
    //  DRAWBACKS:
    //    - Local HashMap is NOT checkpointed — lost on failure!
    //    - No state TTL — must manage cleanup manually.
    //    - No automatic rescaling.
    //    - Must know hot keys ahead of time (or detect dynamically).
    //    - More code for the same result.
    // ================================================================
    public static DataStream<Tuple6<String, Long, Long, Long, Long, Double>> customPartitioner(
            DataStream<PageViewEvent> events) {

        // Step 1 + 2: custom partition → local accumulation
        DataStream<Tuple4<String, Integer, Long, Long>> localAgg =
                events.partitionCustom(
                                new HotKeyPartitioner(Set.of("google.com")),
                                PageViewEvent::getUrl)
                        .flatMap(new LocalAccumulatorFunction())
                        .returns(
                                TypeInformation.of(
                                        new TypeHint<Tuple4<String, Integer, Long, Long>>() {}));

        // Step 3: merge (reuse same merge function as Approach 1)
        return localAgg
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .process(new MergePartialsFunction());
    }

    /**
     * Routes known hot keys round-robin across subtasks. Other keys use hash partitioning.
     *
     * <p>This is where custom partitioner adds value: you control exactly which subtask gets
     * which data. But you must know (or dynamically detect) which keys are hot.
     */
    public static class HotKeyPartitioner implements Partitioner<String> {

        private final Set<String> hotKeys;
        private final AtomicInteger counter = new AtomicInteger(0);

        public HotKeyPartitioner(Set<String> hotKeys) {
            this.hotKeys = new HashSet<>(hotKeys);
        }

        @Override
        public int partition(String key, int numPartitions) {
            if (hotKeys.contains(key)) {
                // Round-robin: spreads hot key evenly across all subtasks
                return Math.abs(counter.getAndIncrement() % numPartitions);
            }
            // Normal hash partitioning for non-hot keys
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    /**
     * Accumulates (count, sum) per URL in a local HashMap.
     *
     * <p><b>WARNING:</b> This state is NOT checkpointed. On failure, accumulated data since the
     * last flush is lost. This is the key disadvantage of the custom partitioner approach — you
     * give up Flink's state management guarantees.
     *
     * <p>Flushes all accumulated data when the window boundary is crossed (approximated by
     * checking timestamps), or when the buffer exceeds a threshold.
     */
    public static class LocalAccumulatorFunction
            extends RichFlatMapFunction<PageViewEvent, Tuple4<String, Integer, Long, Long>> {

        /** NOT checkpointed — lost on failure! */
        private transient Map<String, long[]> localBuffer; // url → [count, sum]

        private transient int subtaskId;
        private transient long currentWindowEnd;
        private static final int FLUSH_THRESHOLD = 50;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
            localBuffer = new HashMap<>();
            subtaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            currentWindowEnd = -1;
        }

        @Override
        public void flatMap(
                PageViewEvent event, Collector<Tuple4<String, Integer, Long, Long>> out) {

            // Check if we crossed a window boundary → flush
            long eventWindowEnd = (event.getTimestamp() / 60_000 + 1) * 60_000;
            if (currentWindowEnd > 0 && eventWindowEnd > currentWindowEnd) {
                flush(out);
            }
            currentWindowEnd = eventWindowEnd;

            // Accumulate locally
            long[] acc =
                    localBuffer.computeIfAbsent(event.getUrl(), k -> new long[] {0, 0});
            acc[0]++;
            acc[1] += event.getResponseTimeMs();

            // Flush if buffer is large (prevents unbounded memory growth)
            if (localBuffer.size() > FLUSH_THRESHOLD) {
                flush(out);
            }
        }

        private void flush(Collector<Tuple4<String, Integer, Long, Long>> out) {
            for (Map.Entry<String, long[]> entry : localBuffer.entrySet()) {
                long[] acc = entry.getValue();
                if (acc[0] > 0) {
                    out.collect(Tuple4.of(entry.getKey(), subtaskId, acc[0], acc[1]));
                }
            }
            localBuffer.clear();
        }

        @Override
        public void close() {
            // Note: cannot flush here — no Collector available in close().
            // Any data remaining in localBuffer is LOST.
            // This is a fundamental limitation of this approach.
            if (!localBuffer.isEmpty()) {
                System.out.printf(
                        "[WARN] Subtask %d: %d URLs with unflushed data lost in close()%n",
                        subtaskId, localBuffer.size());
            }
        }
    }

    // ================================================================
    // Shared: PageViewEvent POJO
    // ================================================================
    public static class PageViewEvent {

        private String url;
        private long responseTimeMs;
        private long timestamp;
        private int subtaskId; // set by Phase 1 map, unused in Approach 2

        public PageViewEvent() {}

        public PageViewEvent(String url, long responseTimeMs, long timestamp) {
            this.url = url;
            this.responseTimeMs = responseTimeMs;
            this.timestamp = timestamp;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public long getResponseTimeMs() {
            return responseTimeMs;
        }

        public void setResponseTimeMs(long responseTimeMs) {
            this.responseTimeMs = responseTimeMs;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public int getSubtaskId() {
            return subtaskId;
        }

        public void setSubtaskId(int subtaskId) {
            this.subtaskId = subtaskId;
        }

        @Override
        public String toString() {
            return String.format("PageView{url='%s', rt=%d, ts=%d}", url, responseTimeMs, timestamp);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PageViewEvent that = (PageViewEvent) o;
            return responseTimeMs == that.responseTimeMs
                    && timestamp == that.timestamp
                    && Objects.equals(url, that.url);
        }

        @Override
        public int hashCode() {
            return Objects.hash(url, responseTimeMs, timestamp);
        }
    }

    // ================================================================
    // Shared: Skewed Page View Source
    //
    // google.com:  60% of traffic (hot key)
    // github.com:  25%
    // example.com: 10%
    // long-tail:    5%
    //
    // Response times: 50-200ms (random)
    // ================================================================
    public static class SkewedPageViewSource implements SourceFunction<PageViewEvent> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<PageViewEvent> ctx) throws Exception {
            Random random = new Random(42);

            // Batch 1: 200 events in window [0, 60000)
            for (int i = 0; i < 200; i++) {
                String url = pickUrl(random);
                long responseTime = 50 + random.nextInt(150); // 50-200ms
                long ts = random.nextInt(55_000) + 1000L;
                ctx.collect(new PageViewEvent(url, responseTime, ts));
            }

            // Push watermark past window end
            ctx.collect(new PageViewEvent("google.com", 100, 70_000L));
            ctx.collect(new PageViewEvent("github.com", 80, 70_000L));

            System.out.println("[SOURCE] Emitted batch 1 (200 events + watermark advance)");

            Thread.sleep(5000);
            if (!isRunning) {
                return;
            }

            // Batch 2: late events
            ctx.collect(new PageViewEvent("google.com", 150, 500L));
            ctx.collect(new PageViewEvent("google.com", 120, 2000L));
            ctx.collect(new PageViewEvent("github.com", 90, 3000L));

            System.out.println("[SOURCE] Emitted batch 2 (late events)");
            Thread.sleep(3000);
        }

        private static String pickUrl(Random random) {
            int r = random.nextInt(100);
            if (r < 60) {
                return "google.com";
            } else if (r < 85) {
                return "github.com";
            } else if (r < 95) {
                return "example.com";
            } else {
                return "site-" + random.nextInt(10) + ".com";
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
