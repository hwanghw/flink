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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * Demonstrates how to use <strong>salting</strong> to deduplicate URLs with skewed traffic,
 * preventing OOM on hot keys.
 *
 * <h2>Business Problem</h2>
 *
 * <p><strong>Count unique visitors per URL in real-time</strong> from a stream of page-visit
 * events (e.g., a clickstream from a web analytics platform). The output is a per-URL,
 * per-minute count of distinct {@code visitorId}s — the metric behind "unique visitors"
 * on a live dashboard.
 *
 * <p>The challenge: traffic is <em>heavily skewed</em>. A handful of popular URLs
 * (google.com, youtube.com) receive 100-1000x more visits than the long tail. Deduplication
 * requires per-URL state that tracks which visitors have already been seen — and for hot
 * URLs this state can hold tens of millions of entries, far exceeding what a single Flink
 * subtask can handle in memory.
 *
 * <p>Concretely, the requirements are:
 * <ol>
 *   <li>Exactly-correct unique counts per URL per 1-minute tumbling window</li>
 *   <li>Handle 100K+ events/sec with 60% going to a single URL</li>
 *   <li>Bounded memory: no OOM even if a single URL has 100M unique visitors</li>
 *   <li>Support late data (events arriving after the window closes)</li>
 * </ol>
 *
 * <h2>The Technical Problem: keyBy Skew → OOM</h2>
 *
 * <p>Naive deduplication: {@code keyBy(url)} puts ALL visits to a popular URL (e.g., google.com)
 * on a single subtask. If google.com has 100M unique visitors, that subtask's state holds 100M
 * entries while others sit idle → OOM.
 *
 * <pre>
 *   keyBy(url)
 *
 *   Subtask 0: google.com → 100M visitors (OOM!)
 *   Subtask 1: github.com → 1M visitors
 *   Subtask 2: example.com → 100K visitors
 *   Subtask 3: (idle)
 * </pre>
 *
 * <h2>The Solution: Two-Phase Salted Deduplication</h2>
 *
 * <pre>
 *   Phase 1: keyBy(url + "#" + salt)     — salt = hash(visitorId) % N
 *
 *   Subtask 0: google.com#0 → 25M visitors  (manageable!)
 *   Subtask 1: google.com#1 → 25M visitors
 *   Subtask 2: google.com#2 → 25M visitors
 *   Subtask 3: google.com#3 → 25M visitors
 *   (other URLs also distributed)
 *
 *   Each shard deduplicates its visitors independently using MapState + TTL.
 *   Emits (url, salt, uniqueCount) per window.
 *
 *   Phase 2: keyBy(url)
 *
 *   Receives only N numbers per URL per window (not millions of records).
 *   Sums unique counts from all salt shards.
 *   Emits (url, windowStart, windowEnd, totalUniqueVisitors).
 * </pre>
 *
 * <p>Key properties:
 * <ul>
 *   <li>Salt is deterministic: {@code hash(visitorId) % N} ensures the same visitor always
 *       goes to the same shard → dedup is correct per-shard, no double counting.</li>
 *   <li>MapState with TTL auto-clears old entries → prevents unbounded state growth (the OOM fix).</li>
 *   <li>Phase 2 is lightweight: only N entries per URL per window, not millions.</li>
 * </ul>
 *
 * <h2>Why Not {@code partitionCustom}?</h2>
 *
 * <p>A custom partitioner ({@code .partitionCustom(partitioner, keySelector)}) does NOT help
 * for deduplication:
 * <ul>
 *   <li>{@code partitionCustom → keyBy(url) → process}: the {@code keyBy} overrides the custom
 *       partitioner — google.com still goes to one subtask → OOM.</li>
 *   <li>{@code partitionCustom → keyBy(url+salt) → process}: the {@code keyBy} overrides again.
 *       This is just salting with an unnecessary extra step.</li>
 *   <li>{@code partitionCustom → process (no keyBy)}: without {@code keyBy}, there is no
 *       {@code KeyedState} (MapState/ValueState). You'd have to manage a raw HashMap in
 *       operator state — no automatic key-group rescaling, no state TTL, and you're back
 *       to OOM with extra complexity.</li>
 * </ul>
 *
 * <p>Custom partitioner IS useful for other skew scenarios: stateless operations (filter/map)
 * where you want to route hot keys to dedicated subtasks, or associative operations (count/sum)
 * where you can pre-aggregate locally. But for dedup, the state itself is the OOM risk, and
 * only {@code keyBy} with salting correctly distributes it.
 *
 * <h2>RocksDB TTL Compact Filter: How {@code cleanupInRocksdbCompactFilter()} Works</h2>
 *
 * <p>This is the most efficient TTL cleanup strategy for the RocksDB state backend.
 * During RocksDB compaction (when SST files are merged and rewritten), a custom
 * {@code FlinkCompactionFilter} inspects each state entry's TTL timestamp. Expired
 * entries are dropped from the output SST file — cleanup piggybacks on compaction
 * with no extra I/O.
 *
 * <h3>{@code queryTimeAfterNumEntries} and JNI Performance</h3>
 *
 * <p>The compact filter runs in <strong>native C++ code</strong> (RocksDB compaction thread).
 * To check expiration it needs the current timestamp from the JVM. This requires a
 * <strong>JNI callback</strong> into Java — specifically into
 * {@code RocksDbTtlCompactFiltersManager.TimeProviderWrapper.currentTimestamp()},
 * which delegates to {@code TtlTimeProvider.DEFAULT} (= {@code System.currentTimeMillis()}).
 *
 * <p>JNI calls are expensive in a tight native loop processing millions of entries,
 * so the filter does NOT call back on every entry. Instead, {@code queryTimeAfterNumEntries}
 * controls batching: the filter queries the JVM timestamp once every N entries and
 * caches the result for the next N entries. For example, with {@code queryTimeAfterNumEntries = 1000}:
 *
 * <pre>
 *   Native compaction loop:
 *     entry 0:    JNI call → currentTimestamp = 1711100000000  (cache)
 *     entry 1:    use cached timestamp
 *     entry 2:    use cached timestamp
 *     ...
 *     entry 999:  use cached timestamp
 *     entry 1000: JNI call → currentTimestamp = 1711100000050  (refresh cache)
 *     entry 1001: use cached timestamp
 *     ...
 * </pre>
 *
 * <p>Performance trade-off:
 * <ul>
 *   <li>Lower value (e.g. 100): more accurate expiry, but more JNI overhead → slower compaction</li>
 *   <li>Higher value (e.g. 10000): faster compaction, but some entries may live slightly past TTL</li>
 *   <li>1000 is a reasonable default — at typical compaction speeds the cached timestamp
 *       is stale by only a few milliseconds, negligible vs. a 1-hour TTL.</li>
 * </ul>
 *
 * <h3>Why Not Other Cleanup Strategies?</h3>
 *
 * <ul>
 *   <li>{@code cleanupFullSnapshot()}: only removes expired entries during checkpoint/savepoint.
 *       State grows unbounded between snapshots — bad for high-volume dedup.</li>
 *   <li>{@code cleanupIncrementally(N, skip)}: scans N entries on each state access.
 *       Adds latency to every {@code processElement()} call. Designed for the heap backend;
 *       on RocksDB it conflicts with the native iterator model.</li>
 *   <li>No cleanup strategy: expired entries are logically filtered on read
 *       ({@code NeverReturnExpired}), but never physically deleted — unbounded disk growth.</li>
 * </ul>
 *
 * <h3>Source Code References</h3>
 *
 * <ul>
 *   <li>Compact filter setup and JNI bridge:
 *       {@code flink-state-backends/flink-statebackend-rocksdb/.../ttl/RocksDbTtlCompactFiltersManager.java}</li>
 *   <li>{@code TimeProviderWrapper} (JNI callback for timestamp):
 *       same file, inner class {@code TimeProviderWrapper.currentTimestamp()}</li>
 *   <li>Native compact filter factory:
 *       {@code org.rocksdb.FlinkCompactionFilter} / {@code FlinkCompactionFilterFactory}
 *       (in the frocksdbjni library — Ververica's RocksDB fork)</li>
 *   <li>TTL expiration check:
 *       {@code flink-runtime/.../ttl/TtlUtils.expired(lastAccessTimestamp, ttl, currentTimestamp)}</li>
 *   <li>Default time provider:
 *       {@code TtlTimeProvider.DEFAULT = System::currentTimeMillis}</li>
 * </ul>
 *
 * @see TwoPhaseCountDeduplicatedEventTimeV2 for the pre-aggregation approach to skew handling
 */
public class SaltedUrlDeduplication {

    /** Number of salt buckets. Each URL is spread across this many shards. */
    private static final int NUM_SALT_BUCKETS = 4;

    private static final OutputTag<Tuple3<String, Integer, Long>> PHASE2_LATE_TAG =
            new OutputTag<Tuple3<String, Integer, Long>>("phase2-late-data") {};

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
//        configuration.setString("taskmanager.memory.network.min", "256mb");
//        configuration.setString("taskmanager.memory.network.max", "256mb");
//        configuration.setString("taskmanager.memory.network.fraction", "0.2");

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);

        // ============================================================
        // Source: skewed URL visit events
        // ============================================================
        DataStream<UrlVisitEvent> events =
                env.addSource(new SkewedUrlSource())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<UrlVisitEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(5))
                                        .withTimestampAssigner(
                                                (event, ts) -> event.getTimestamp())
                                        .withIdleness(Duration.ofMinutes(1)));

        // ============================================================
        // Phase 1: Salted deduplication
        //
        //   keyBy(url + "#" + salt) spreads hot URLs across N subtasks.
        //   Each shard deduplicates its slice of visitors using
        //   MapState<visitorId, Boolean> with TTL.
        //   Emits (url, salt, uniqueCount) at window boundaries.
        // ============================================================
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> shardCounts =
                events.keyBy(
                                event -> {
                                    int salt =
                                            Math.abs(event.getVisitorId().hashCode())
                                                    % NUM_SALT_BUCKETS;
                                    return event.getUrl() + "#" + salt;
                                })
                        .process(new SaltedDeduplicateFunction(NUM_SALT_BUCKETS));

        // ============================================================
        // Phase 2: Merge unique counts from all salt shards
        //
        //   keyBy(url) collects the N shard counts per URL.
        //   ProcessWindowFunction sums them, deduplicating by salt
        //   (same pattern as GlobalDeduplicatedCountFunction in
        //   TwoPhaseCountDeduplicatedEventTimeV2).
        //
        //   This is lightweight: only N entries per URL per window.
        // ============================================================
        SingleOutputStreamOperator<Tuple4<String, Long, Long, Long>> results =
                shardCounts
                        .keyBy(t -> t.f0)
                        .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                        .allowedLateness(Duration.ofSeconds(30))
                        .sideOutputLateData(PHASE2_LATE_TAG)
                        .process(new MergeUniqueCountsFunction());

        results.print("RESULT");
        results.getSideOutput(PHASE2_LATE_TAG).print("LATE");

        env.execute("Salted URL Deduplication (Skew-Resistant)");
    }

    // ================================================================
    // Phase 1: Salted deduplication per shard
    //
    // Keyed by: url + "#" + salt
    //
    // State:
    //   - MapState<visitorId, Boolean> with TTL — tracks seen visitors
    //   - ValueState<Long> — running unique count for current window
    //
    // On each event:
    //   if visitorId not in MapState → new unique visitor, count++
    //   Register timer at window end to emit the count.
    //
    // On timer:
    //   Emit (url, salt, uniqueCount) and reset count for next window.
    //
    // TTL on MapState prevents unbounded state growth.
    // Without TTL, 100M unique visitors to google.com = 100M entries
    // per shard = OOM. With 1-hour TTL, only recent visitors are kept.
    // ================================================================
    public static class SaltedDeduplicateFunction
            extends KeyedProcessFunction<String, UrlVisitEvent, Tuple3<String, Integer, Long>> {

        private final int numSaltBuckets;

        /** Per-shard visitor dedup state with TTL. This is the key to preventing OOM. */
        private transient MapState<String, Boolean> visitorsState;

        /** Running unique count for the current window. */
        private transient ValueState<Long> uniqueCountState;

        public SaltedDeduplicateFunction(int numSaltBuckets) {
            this.numSaltBuckets = numSaltBuckets;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            // MapState with TTL: entries expire after 1 hour of inactivity.
            // This is the critical OOM prevention mechanism — without it,
            // state grows unbounded for hot URLs.
            StateTtlConfig ttlConfig =
                    StateTtlConfig.newBuilder(Duration.ofHours(1))
                            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                            .setStateVisibility(
                                    StateTtlConfig.StateVisibility.NeverReturnExpired)
                            // Physically removes expired entries during RocksDB compaction.
                            // 1000 = JNI timestamp refresh interval (see class javadoc for details).
                            .cleanupInRocksdbCompactFilter(1000)
                            .build();

            MapStateDescriptor<String, Boolean> visitorsDesc =
                    new MapStateDescriptor<>("visitors", String.class, Boolean.class);
            visitorsDesc.enableTimeToLive(ttlConfig);
            visitorsState = getRuntimeContext().getMapState(visitorsDesc);

            uniqueCountState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("unique-count", Long.class));
        }

        @Override
        public void processElement(
                UrlVisitEvent event,
                Context ctx,
                Collector<Tuple3<String, Integer, Long>> out)
                throws Exception {

            // Check if this visitor has been seen in this shard
            if (!Boolean.TRUE.equals(visitorsState.get(event.getVisitorId()))) {
                // New unique visitor for this shard
                visitorsState.put(event.getVisitorId(), Boolean.TRUE);
                Long count = uniqueCountState.value();
                uniqueCountState.update(count == null ? 1L : count + 1L);
            }

            // Register timer at the end of the 1-minute tumbling window
            long windowEnd = (event.getTimestamp() / 60_000 + 1) * 60_000;
            ctx.timerService().registerEventTimeTimer(windowEnd);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple3<String, Integer, Long>> out)
                throws Exception {

            // Timer fires at window end. Emit the unique count for this shard.
            String saltedKey = ctx.getCurrentKey();
            int hashIdx = saltedKey.lastIndexOf('#');
            String url = saltedKey.substring(0, hashIdx);
            int salt = Integer.parseInt(saltedKey.substring(hashIdx + 1));

            Long count = uniqueCountState.value();
            if (count != null && count > 0) {
                out.collect(Tuple3.of(url, salt, count));

                System.out.printf(
                        "[PHASE1] Window end=%d | %s shard#%d | unique=%d%n",
                        timestamp, url, salt, count);

                // Reset count for the next window
                uniqueCountState.clear();
            }
            // Note: we do NOT clear visitorsState here — TTL handles cleanup.
            // Visitors seen in window N are still "seen" in window N+1,
            // providing cross-window deduplication within the TTL period.
        }
    }

    // ================================================================
    // Phase 2: Merge unique counts from all salt shards
    //
    // Input:  Tuple3<url, salt, uniqueCount>   (from each shard)
    // Output: Tuple4<url, windowStart, windowEnd, totalUniqueVisitors>
    //
    // Same deduplication pattern as GlobalDeduplicatedCountFunction:
    //   Map<salt, latestCount> → sum all values.
    //
    // This is lightweight — at most NUM_SALT_BUCKETS entries per URL.
    // ================================================================
    public static class MergeUniqueCountsFunction
            extends ProcessWindowFunction<
                    Tuple3<String, Integer, Long>,
                    Tuple4<String, Long, Long, Long>,
                    String,
                    TimeWindow> {

        @Override
        public void process(
                String url,
                Context context,
                Iterable<Tuple3<String, Integer, Long>> elements,
                Collector<Tuple4<String, Long, Long, Long>> out) {

            TimeWindow window = context.window();

            // Deduplicate by salt: keep latest count per shard
            // (handles re-fires from allowed lateness)
            Map<Integer, Long> shardCounts = new HashMap<>();
            for (Tuple3<String, Integer, Long> element : elements) {
                shardCounts.put(element.f1, element.f2);
            }

            // Sum unique counts from all shards
            long totalUnique = 0L;
            for (long c : shardCounts.values()) {
                totalUnique += c;
            }

            out.collect(Tuple4.of(url, window.getStart(), window.getEnd(), totalUnique));

            System.out.printf(
                    "[RESULT] Window [%d, %d) | %s | shards=%s | total_unique=%d%n",
                    window.getStart(),
                    window.getEnd(),
                    url,
                    shardCounts,
                    totalUnique);
        }
    }

    // ================================================================
    // URL Visit Event POJO
    // ================================================================
    public static class UrlVisitEvent {

        private String url;
        private String visitorId;
        private long timestamp;

        public UrlVisitEvent() {}

        public UrlVisitEvent(String url, String visitorId, long timestamp) {
            this.url = url;
            this.visitorId = visitorId;
            this.timestamp = timestamp;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getVisitorId() {
            return visitorId;
        }

        public void setVisitorId(String visitorId) {
            this.visitorId = visitorId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format(
                    "Visit{url='%s', visitor='%s', ts=%d}", url, visitorId, timestamp);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UrlVisitEvent that = (UrlVisitEvent) o;
            return timestamp == that.timestamp
                    && Objects.equals(url, that.url)
                    && Objects.equals(visitorId, that.visitorId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(url, visitorId, timestamp);
        }
    }

    // ================================================================
    // Skewed URL Source
    //
    // Generates URL visit events with realistic skew:
    //   google.com:  60% of traffic (hot URL — the OOM risk)
    //   github.com:  25% of traffic
    //   example.com: 10% of traffic
    //   long-tail:    5% of traffic (random URLs)
    //
    // Batch 1: 100 events in window [0, 60000) with various visitors
    //   + watermark-advancing events at t=70000
    //
    // --- Thread.sleep(5 seconds) ---
    //
    // Batch 2: Late events (within allowed lateness)
    //   + duplicate visitors (should not increase unique count)
    // ================================================================
    public static class SkewedUrlSource implements SourceFunction<UrlVisitEvent> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<UrlVisitEvent> ctx) throws Exception {
            Random random = new Random(42); // deterministic for reproducibility

            // Batch 1: 100 events in window [0, 60000)
            for (int i = 0; i < 100; i++) {
                String url = pickUrl(random);
                String visitorId = "visitor-" + random.nextInt(50); // 50 possible visitors
                long ts = random.nextInt(55_000) + 1000L; // timestamps 1000-56000

                ctx.collect(new UrlVisitEvent(url, visitorId, ts));
            }

            // Push watermark past window end: watermark = 70000 - 5000 = 65000
            ctx.collect(new UrlVisitEvent("google.com", "wm-push-1", 70_000L));
            ctx.collect(new UrlVisitEvent("github.com", "wm-push-2", 70_000L));

            System.out.println("[SOURCE] Emitted batch 1 (100 events + watermark advance)");

            // Sleep to let window fire before late events arrive
            Thread.sleep(5000);
            if (!isRunning) {
                return;
            }

            // Batch 2: Late events (ts < watermark 65000)
            // These include DUPLICATE visitors that were already counted
            ctx.collect(new UrlVisitEvent("google.com", "visitor-0", 500L));
            ctx.collect(new UrlVisitEvent("google.com", "visitor-1", 1500L));
            ctx.collect(new UrlVisitEvent("google.com", "visitor-99", 2000L)); // new visitor
            ctx.collect(new UrlVisitEvent("github.com", "visitor-0", 3000L));

            System.out.println("[SOURCE] Emitted batch 2 (late + duplicate events)");
            Thread.sleep(3000);
        }

        /** Picks a URL with skewed distribution. */
        private static String pickUrl(Random random) {
            int r = random.nextInt(100);
            if (r < 60) {
                return "google.com"; // 60% — hot URL
            } else if (r < 85) {
                return "github.com"; // 25%
            } else if (r < 95) {
                return "example.com"; // 10%
            } else {
                return "site-" + random.nextInt(20) + ".com"; // 5% long tail
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
