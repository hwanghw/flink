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

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

/**
 * Demonstrates how to use <strong>broadcast state</strong> to filter a data stream against
 * a periodically-updated blacklist.
 *
 * <h2>Topology</h2>
 *
 * <pre>
 *   ┌───────────────────┐
 *   │  BlacklistSource   │  parallelism = 1
 *   │  (polls external   │  emits BlacklistUpdate every N seconds
 *   │   store hourly)    │
 *   └────────┬──────────┘
 *            │
 *            ▼ .broadcast(descriptor)
 *     ┌──────┼──────┬──────┐
 *     ▼      ▼      ▼      ▼
 *   Sub0   Sub1   Sub2   Sub3    ← BlacklistFilterFunction
 *     │      │      │      │       (BroadcastProcessFunction)
 *     ▼      ▼      ▼      ▼
 *   filtered output (non-blacklisted events only)
 *
 *   ┌───────────────────┐
 *   │  EventSource       │  parallelism = default
 *   │  (generates random │
 *   │   events)          │──────► connects to broadcast stream above
 *   └───────────────────┘
 * </pre>
 *
 * <h2>Key Design Points</h2>
 *
 * <ul>
 *   <li>The blacklist source runs at parallelism 1 — only one task polls the external store.</li>
 *   <li>Broadcast state is heap-only (no RocksDB). Each subtask holds a full copy.</li>
 *   <li>Checkpoint size = blacklist_size × parallelism. Keep the blacklist small (&lt; 10M IDs).</li>
 *   <li>Before the first broadcast arrives, the state is empty → fail-open (all records pass).
 *       The source emits immediately on startup to minimize this window.</li>
 *   <li>The blacklist is stored as a single MapState entry (key="bl", value=Set&lt;String&gt;)
 *       rather than one entry per ID, for simpler checkpointing and atomic updates.</li>
 * </ul>
 *
 * <h2>Production Notes</h2>
 *
 * <p>This example simulates the external store with random data. In production, replace
 * {@link BlacklistSource} internals with real Redis/DB calls (see comments in the code).
 *
 * <p>For blacklists larger than ~10M IDs, consider the TaskManager-level cache approach
 * (1 copy per TM, not per subtask) or a Bloom filter + Redis fallback.
 * See {@code codedocs/flink-blacklist-filtering-solutions.md} for a full comparison.
 */
public class BlacklistFilterBroadcastState {

    /** Key used in the broadcast MapState. Single entry holding the full blacklist set. */
    private static final String BLACKLIST_STATE_KEY = "bl";

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);

        // State descriptor for broadcast state
        final MapStateDescriptor<String, Set<String>> blacklistDescriptor =
                new MapStateDescriptor<>(
                        "blacklist-state",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        TypeInformation.of(new TypeHint<Set<String>>() {}));

        // ============================================================
        // Blacklist source: polls external store periodically
        // Parallelism = 1 so only one task hits the external store.
        // ============================================================
        DataStream<BlacklistUpdate> blacklistStream =
                env.addSource(new BlacklistSource(10_000L)) // 10s for demo; use 3_600_000 in prod
                        .setParallelism(1)
                        .name("blacklist-source");

        BroadcastStream<BlacklistUpdate> broadcastBlacklist =
                blacklistStream.broadcast(blacklistDescriptor);

        // ============================================================
        // Event source: generates random events with IDs
        // ============================================================
        DataStream<Event> events =
                env.addSource(new EventSource()).name("event-source");

        // ============================================================
        // Connect data stream with broadcast stream and filter
        // ============================================================
        DataStream<Event> filtered =
                events.connect(broadcastBlacklist)
                        .process(new BlacklistFilterFunction(blacklistDescriptor))
                        .name("blacklist-filter");

        filtered.print("PASSED");

        env.execute("Blacklist Filter (Broadcast State)");
    }

    // ================================================================
    // Broadcast Process Function
    //
    // processBroadcastElement: receives blacklist updates, writes to
    //   broadcast state (read-write access).
    //
    // processElement: checks each event against broadcast state
    //   (read-only access). Passes non-blacklisted events.
    // ================================================================
    public static class BlacklistFilterFunction
            extends BroadcastProcessFunction<Event, BlacklistUpdate, Event> {

        private final MapStateDescriptor<String, Set<String>> descriptor;

        public BlacklistFilterFunction(MapStateDescriptor<String, Set<String>> descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        public void processElement(Event event, ReadOnlyContext ctx, Collector<Event> out)
                throws Exception {
            // Read-only access to broadcast state
            ReadOnlyBroadcastState<String, Set<String>> broadcastState =
                    ctx.getBroadcastState(descriptor);

            Set<String> blacklist = broadcastState.get(BLACKLIST_STATE_KEY);

            // Fail-open: if blacklist hasn't arrived yet, let all records through
            if (blacklist == null || !blacklist.contains(event.getId())) {
                out.collect(event);
            }
        }

        @Override
        public void processBroadcastElement(
                BlacklistUpdate update, Context ctx, Collector<Event> out) throws Exception {
            // Read-write access to broadcast state
            BroadcastState<String, Set<String>> broadcastState =
                    ctx.getBroadcastState(descriptor);

            // Atomic replacement of the full blacklist
            broadcastState.put(BLACKLIST_STATE_KEY, update.getBlacklistedIds());

            System.out.printf(
                    "[BROADCAST] Updated blacklist: %d IDs on subtask %d%n",
                    update.getBlacklistedIds().size(),
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        }
    }

    // ================================================================
    // Blacklist Update POJO
    //
    // Contains the full replacement set of blacklisted IDs.
    // Emitted by BlacklistSource, received by processBroadcastElement.
    // ================================================================
    public static class BlacklistUpdate implements java.io.Serializable {

        private Set<String> blacklistedIds;

        public BlacklistUpdate() {}

        public BlacklistUpdate(Set<String> blacklistedIds) {
            this.blacklistedIds = blacklistedIds;
        }

        public Set<String> getBlacklistedIds() {
            return blacklistedIds;
        }

        public void setBlacklistedIds(Set<String> blacklistedIds) {
            this.blacklistedIds = blacklistedIds;
        }

        @Override
        public String toString() {
            return "BlacklistUpdate{size=" + (blacklistedIds != null ? blacklistedIds.size() : 0) + "}";
        }
    }

    // ================================================================
    // Blacklist Source
    //
    // Simulates polling an external store (Redis/DB) periodically.
    // Emits the full blacklist as a BlacklistUpdate.
    //
    // In production, replace the random ID generation with:
    //   RedisClient client = RedisClient.create("redis://localhost:6379");
    //   StatefulRedisConnection<String, String> conn = client.connect();
    //   Set<String> ids = new HashSet<>();
    //   ScanCursor cursor = ScanCursor.INITIAL;
    //   do {
    //       ValueScanCursor<String> result = conn.sync().sscan("blacklist", cursor);
    //       ids.addAll(result.getValues());
    //       cursor = result;
    //   } while (!cursor.isFinished());
    //   conn.close();
    //   client.shutdown();
    // ================================================================
    public static class BlacklistSource implements SourceFunction<BlacklistUpdate> {

        private final long pollIntervalMs;
        private volatile boolean isRunning = true;

        public BlacklistSource(long pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
        }

        @Override
        public void run(SourceContext<BlacklistUpdate> ctx) throws Exception {
            Random random = new Random(42);
            int generation = 0;

            while (isRunning) {
                // Simulate loading blacklist from external store
                Set<String> blacklistedIds = new HashSet<>();
                int size = 50 + random.nextInt(50); // 50-100 IDs for demo
                for (int i = 0; i < size; i++) {
                    blacklistedIds.add("user-" + random.nextInt(200));
                }

                // Emit under checkpoint lock to ensure consistency
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(new BlacklistUpdate(blacklistedIds));
                }

                generation++;
                System.out.printf(
                        "[SOURCE] Emitted blacklist generation %d: %d IDs%n",
                        generation, blacklistedIds.size());

                // Sleep until next poll
                long slept = 0;
                while (isRunning && slept < pollIntervalMs) {
                    long toSleep = Math.min(1000, pollIntervalMs - slept);
                    Thread.sleep(toSleep);
                    slept += toSleep;
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // ================================================================
    // Event POJO
    // ================================================================
    public static class Event implements java.io.Serializable {

        private String id;
        private String payload;
        private long timestamp;

        public Event() {}

        public Event(String id, String payload, long timestamp) {
            this.id = id;
            this.payload = payload;
            this.timestamp = timestamp;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getPayload() {
            return payload;
        }

        public void setPayload(String payload) {
            this.payload = payload;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("Event{id='%s', payload='%s', ts=%d}", id, payload, timestamp);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Event event = (Event) o;
            return timestamp == event.timestamp
                    && Objects.equals(id, event.id)
                    && Objects.equals(payload, event.payload);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, payload, timestamp);
        }
    }

    // ================================================================
    // Event Source
    //
    // Generates random events at a steady rate for demonstration.
    // IDs overlap with the blacklist (user-0 through user-199)
    // so some events will be filtered.
    // ================================================================
    public static class EventSource implements SourceFunction<Event> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            Random random = new Random(123);
            long eventCount = 0;

            while (isRunning) {
                String id = "user-" + random.nextInt(200);
                String payload = "data-" + eventCount;
                long ts = System.currentTimeMillis();

                ctx.collect(new Event(id, payload, ts));
                eventCount++;

                // ~100 events/sec for demo
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
