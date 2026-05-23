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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * Computes the top 10 most-clicked ads in each 3-minute tumbling event-time window.
 *
 * <h2>Pipeline (Two-Phase)</h2>
 *
 * <pre>
 *  Ad Click Stream
 *  [adId, userId, timestamp]
 *          │
 *          ▼
 *    assignTimestampsAndWatermarks
 *          │
 *          ▼
 *    keyBy(adId)
 *          │
 *          ▼
 *    TumblingEventTimeWindow(3 min)
 *    AggregateFunction  ← incremental count per ad per window
 *          │
 *          ▼
 *    AdClickCount(adId, count, windowEnd)    ← one record per (ad, window)
 *          │
 *    keyBy(windowEnd)  ← group all ads from the same window together
 *          │
 *          ▼
 *    KeyedProcessFunction
 *    ┌──────────────────────────────────────────────────────┐
 *    │  ListState: buffer AdClickCounts for this windowEnd  │
 *    │  EventTimeTimer at windowEnd + 100ms grace period    │
 *    │  onTimer: sort by count desc → emit top 10           │
 *    └──────────────────────────────────────────────────────┘
 *          │
 *          ▼
 *    Top10Result [rank, adId, clickCount, windowStart, windowEnd]
 *          │
 *          ▼
 *         print
 * </pre>
 *
 * <h2>Why Two Phases?</h2>
 *
 * <p>Phase 1 is parallelized by adId — each subtask independently counts clicks for its assigned
 * ads. This scales horizontally with the number of unique ads.
 *
 * <p>Phase 2 gathers all per-ad counts for the same window onto a single subtask (keyed by
 * windowEnd), sorts them, and emits the top 10. The fan-in here is bounded: at most one record per
 * unique ad per window arrives in Phase 2, so the state size is O(numAds), not O(numClicks).
 *
 * <h2>Data</h2>
 *
 * <pre>
 *   20 ads with skewed click rates:
 *     ad-001 to ad-005 → "popular" ads  (~25% of clicks each)
 *     ad-006 to ad-010 → "medium"       (~3% of clicks each)
 *     ad-011 to ad-020 → "long-tail"    (~0.5% of clicks each)
 *
 *   Two windows of data are generated:
 *     Window 1: ts in [0, 180000)    → watermark advance to 200000
 *     Window 2: ts in [180000, 360000) → watermark advance to 400000
 * </pre>
 */
public class Top10ClickedAds {

    private static final int TOP_N = 10;

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);

        DataStream<AdClickEvent> clicks =
                env.addSource(new AdClickSource())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(5))
                                        .withTimestampAssigner((e, ts) -> e.timestamp)
                                        .withIdleness(Duration.ofMinutes(1)));

        // Phase 1: count clicks per ad per 3-min window
        DataStream<AdClickCount> perAdCounts =
                clicks.keyBy(e -> e.adId)
                        .window(TumblingEventTimeWindows.of(Duration.ofMinutes(3)))
                        .aggregate(
                                new ClickCountAggregator(),
                                new AttachWindowMetaFunction());

        // Phase 2: gather all ads from the same window, sort, emit top 10
        DataStream<Top10Result> top10 =
                perAdCounts
                        .keyBy(c -> c.windowEnd)
                        .process(new Top10KeyedProcessFunction(TOP_N));

        top10.print("TOP-10");
        env.execute("Top 10 Clicked Ads per 3 Minutes");
    }

    // ================================================================
    //  Phase 1: Aggregate — count clicks per ad
    //
    //  AggregateFunction is incremental: accumulator is updated
    //  for each record as it arrives, so memory usage is O(1)
    //  per key per window (just the count), not O(records).
    // ================================================================

    /** Accumulates click count for one ad in one window. */
    public static class ClickCountAggregator
            implements AggregateFunction<AdClickEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent event, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * ProcessWindowFunction that wraps the aggregate result with window metadata.
     *
     * <p>When combined with {@link AggregateFunction} via the two-argument {@code aggregate()}
     * overload, Flink calls AggregateFunction incrementally and only calls this function once at
     * window close — passing the final aggregate value. This gives you both incremental efficiency
     * AND access to window start/end times.
     */
    public static class AttachWindowMetaFunction
            extends ProcessWindowFunction<Long, AdClickCount, String, TimeWindow> {

        @Override
        public void process(
                String adId,
                Context context,
                Iterable<Long> counts,
                Collector<AdClickCount> out) {

            long clickCount = counts.iterator().next(); // single value from AggregateFunction
            TimeWindow window = context.window();
            out.collect(new AdClickCount(adId, clickCount, window.getStart(), window.getEnd()));
        }
    }

    // ================================================================
    //  Phase 2: Top-N via KeyedProcessFunction
    //
    //  All AdClickCounts for the same windowEnd land on one subtask
    //  (keyed by windowEnd). We buffer them in ListState and register
    //  an event-time timer at windowEnd + grace. When the timer fires
    //  we know all Phase-1 results for that window have arrived
    //  (watermark has passed windowEnd), so we sort and emit top N.
    //
    //  State layout (per windowEnd key):
    //    ListState<AdClickCount>  ← buffered per-ad counts
    //    Timer at windowEnd + GRACE_MS ← fires once watermark passes
    // ================================================================
    public static class Top10KeyedProcessFunction
            extends KeyedProcessFunction<Long, AdClickCount, Top10Result> {

        private static final long GRACE_MS = 100L; // small grace to let stragglers arrive

        private final int topN;

        // One list per windowEnd key, cleared after the timer fires
        private ListState<AdClickCount> bufferedCounts;

        public Top10KeyedProcessFunction(int topN) {
            this.topN = topN;
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
            bufferedCounts =
                    getRuntimeContext()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "buffered-ad-counts",
                                            TypeInformation.of(
                                                    new TypeHint<AdClickCount>() {})));
        }

        @Override
        public void processElement(
                AdClickCount count,
                Context ctx,
                Collector<Top10Result> out) throws Exception {

            bufferedCounts.add(count);

            // Register timer once per window (idempotent — Flink deduplicates same timestamp)
            ctx.timerService().registerEventTimeTimer(count.windowEnd + GRACE_MS);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Top10Result> out) throws Exception {

            long windowEnd = timestamp - GRACE_MS;
            long windowStart = windowEnd - Duration.ofMinutes(3).toMillis();

            // Collect, sort descending by click count, take top N
            List<AdClickCount> all = new ArrayList<>();
            for (AdClickCount c : bufferedCounts.get()) {
                all.add(c);
            }
            all.sort(Comparator.comparingLong((AdClickCount c) -> c.clickCount).reversed());

            int rank = 1;
            for (AdClickCount c : all) {
                if (rank > topN) {
                    break;
                }
                out.collect(new Top10Result(rank, c.adId, c.clickCount, windowStart, windowEnd));
                rank++;
            }

            // Clear state — this window is done
            bufferedCounts.clear();
        }
    }

    // ================================================================
    //  POJOs
    // ================================================================

    /** Raw ad click event emitted by the source. */
    public static class AdClickEvent {
        public String adId;
        public String userId;
        public long timestamp;

        public AdClickEvent() {}

        public AdClickEvent(String adId, String userId, long timestamp) {
            this.adId = adId;
            this.userId = userId;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("AdClick{adId='%s', userId='%s', ts=%d}", adId, userId, timestamp);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AdClickEvent that = (AdClickEvent) o;
            return timestamp == that.timestamp
                    && Objects.equals(adId, that.adId)
                    && Objects.equals(userId, that.userId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(adId, userId, timestamp);
        }
    }

    /** Per-ad click count for one window, output of Phase 1. */
    public static class AdClickCount {
        public String adId;
        public long clickCount;
        public long windowStart;
        public long windowEnd;

        public AdClickCount() {}

        public AdClickCount(String adId, long clickCount, long windowStart, long windowEnd) {
            this.adId = adId;
            this.clickCount = clickCount;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return String.format(
                    "AdClickCount{adId='%s', count=%d, window=[%d,%d)}",
                    adId, clickCount, windowStart, windowEnd);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AdClickCount that = (AdClickCount) o;
            return clickCount == that.clickCount
                    && windowStart == that.windowStart
                    && windowEnd == that.windowEnd
                    && Objects.equals(adId, that.adId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(adId, clickCount, windowStart, windowEnd);
        }
    }

    /** Final output: one record per ranked ad per window. */
    public static class Top10Result {
        public int rank;
        public String adId;
        public long clickCount;
        public long windowStart;
        public long windowEnd;

        public Top10Result() {}

        public Top10Result(
                int rank, String adId, long clickCount, long windowStart, long windowEnd) {
            this.rank = rank;
            this.adId = adId;
            this.clickCount = clickCount;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return String.format(
                    "Rank #%02d | %-8s | clicks=%4d | window=[%d, %d)",
                    rank, adId, clickCount, windowStart, windowEnd);
        }
    }

    // ================================================================
    //  Source: generates skewed ad click data for two 3-min windows
    //
    //  Ad click rate distribution:
    //    ad-001 to ad-005  → "hot" ads    ~25% of all clicks each
    //    ad-006 to ad-010  → "medium" ads  ~3% of all clicks each
    //    ad-011 to ad-020  → "long-tail"  ~0.5% of all clicks each
    //
    //  Window 1: timestamps in [0, 180_000)  — 1000 events
    //  Window 2: timestamps in [180_000, 360_000) — 1000 events
    //  Then watermark advances past both windows to flush results.
    // ================================================================
    public static class AdClickSource implements SourceFunction<AdClickEvent> {

        private volatile boolean isRunning = true;

        // 20 ads with skewed weights
        private static final String[] ADS = {
            "ad-001", "ad-002", "ad-003", "ad-004", "ad-005", // hot
            "ad-006", "ad-007", "ad-008", "ad-009", "ad-010", // medium
            "ad-011", "ad-012", "ad-013", "ad-014", "ad-015", // long-tail
            "ad-016", "ad-017", "ad-018", "ad-019", "ad-020"  // long-tail
        };

        // Cumulative weights (out of 1000): hot=250 each, medium=30 each, long-tail=5 each
        private static final int[] CUMULATIVE_WEIGHTS = buildCumulativeWeights();

        private static int[] buildCumulativeWeights() {
            int[] weights = new int[ADS.length];
            int cumulative = 0;
            for (int i = 0; i < ADS.length; i++) {
                if (i < 5) cumulative += 250;        // hot ads
                else if (i < 10) cumulative += 30;   // medium ads
                else cumulative += 5;                 // long-tail ads
            }
            // Normalize to cumulative
            int[] cum = new int[ADS.length];
            int c = 0;
            for (int i = 0; i < ADS.length; i++) {
                if (i < 5) c += 250;
                else if (i < 10) c += 30;
                else c += 5;
                cum[i] = c;
            }
            return cum;
        }

        @Override
        public void run(SourceContext<AdClickEvent> ctx) throws Exception {
            Random random = new Random(42);
            int totalWeight = CUMULATIVE_WEIGHTS[CUMULATIVE_WEIGHTS.length - 1];

            // Window 1: [0, 180_000)
            emitWindow(ctx, random, totalWeight, 0L, 170_000L, 1000, "Window 1");

            // Advance watermark past window 1 boundary (180_000)
            ctx.collect(new AdClickEvent("ad-001", "user-watermark", 200_000L));

            System.out.println("[SOURCE] Watermark advanced past window 1 (180s)");
            Thread.sleep(3000);
            if (!isRunning) return;

            // Window 2: [180_000, 360_000)
            emitWindow(ctx, random, totalWeight, 181_000L, 355_000L, 1000, "Window 2");

            // Advance watermark past window 2 boundary (360_000)
            ctx.collect(new AdClickEvent("ad-001", "user-watermark", 400_000L));

            System.out.println("[SOURCE] Watermark advanced past window 2 (360s)");
            Thread.sleep(3000);
        }

        private void emitWindow(
                SourceContext<AdClickEvent> ctx,
                Random random,
                int totalWeight,
                long tsMin,
                long tsMax,
                int count,
                String label) {

            for (int i = 0; i < count; i++) {
                String adId = pickAd(random, totalWeight);
                String userId = "user-" + random.nextInt(10_000);
                long ts = tsMin + (long) (random.nextDouble() * (tsMax - tsMin));
                ctx.collect(new AdClickEvent(adId, userId, ts));
            }
            System.out.printf("[SOURCE] Emitted %d events for %s%n", count, label);
        }

        private static String pickAd(Random random, int totalWeight) {
            int roll = random.nextInt(totalWeight);
            for (int i = 0; i < CUMULATIVE_WEIGHTS.length; i++) {
                if (roll < CUMULATIVE_WEIGHTS[i]) {
                    return ADS[i];
                }
            }
            return ADS[ADS.length - 1];
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
