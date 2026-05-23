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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Map;

/**
 * Joins bid requests and win events using {@link KeyedCoProcessFunction} with
 * {@code connect()}, providing full control over side outputs for late data.
 *
 * <h2>Why KeyedCoProcessFunction instead of .join()?</h2>
 *
 * <p>Flink's {@code .join().window()} API does NOT support {@code sideOutputLateData()}.
 * {@code KeyedCoProcessFunction} gives us:
 * <ul>
 *   <li>{@code ctx.output(tag, value)} — side output for late data</li>
 *   <li>Full keyed state — buffer requests and wins independently</li>
 *   <li>Timers — fire at window boundaries to emit join results</li>
 *   <li>Custom join semantics — left outer join (unmatched bids emitted too)</li>
 * </ul>
 *
 * <h2>How it works</h2>
 *
 * <pre>
 *   Requests (bid=true)                    Wins
 *       │                                   │
 *       └──── keyBy(reqId+"_"+vendorId) ────┘
 *                       │
 *                    connect
 *                       │
 *                       ▼
 *            KeyedCoProcessFunction:
 *
 *   processElement1 (request arrives):
 *     if (watermark > windowEnd + allowedLateness):
 *       → ctx.output(LATE_REQUEST_TAG, request)     ← side output!
 *     else:
 *       → store in MapState&lt;windowEnd, request&gt;
 *       → register timer at windowEnd
 *
 *   processElement2 (win arrives):
 *     if (watermark > windowEnd + allowedLateness):
 *       → ctx.output(LATE_WIN_TAG, win)             ← side output!
 *     else:
 *       → store in MapState&lt;windowEnd, win&gt;
 *       → register timer at windowEnd
 *
 *   onTimer (window boundary):
 *     → look up requests and wins for this window
 *     → join: matched bids + unmatched bids
 *     → emit results
 *     → clean up state
 * </pre>
 *
 * <h2>Trade-off: connect() + CoProcess vs union() + Window</h2>
 *
 * <pre>
 *   | Aspect                    | union + Window (BidWinJoin) | connect + CoProcess (this)      |
 *   |---------------------------|-----------------------------|---------------------------------|
 *   | Side output late data     | Yes (via WindowedStream)    | Yes (via ctx.output)            |
 *   | No wrapper type needed    | No (needs TaggedEvent)      | Yes (two separate streams)      |
 *   | Window logic              | Flink handles it            | Manual (compute boundary,       |
 *   |                           |                             |  register timers, cleanup)      |
 *   | State management          | Flink window state          | Manual MapState per window      |
 *   | Code volume               | Less                        | More                            |
 *   | Flexibility               | Bound to window semantics   | Full control (custom join,      |
 *   |                           |                             |  per-stream lateness, etc.)     |
 * </pre>
 *
 * <p>Both approaches work. The {@code connect()} approach is more code but avoids the
 * {@code TaggedEvent} wrapper and gives full control over join semantics per stream.
 * The {@code union()} approach ({@link BidWinJoin}) is simpler when standard window
 * semantics suffice.
 *
 * <h2>State Buffering: Why CoProcess Wins for Joins</h2>
 *
 * <p>{@code ProcessWindowFunction} (used in the union approach) is a <b>full window</b>
 * operator — it buffers ALL incoming elements in window state before calling
 * {@code process()} at window end. For a join keyed by {@code (requestId, vendorId)},
 * each key has at most 1 request + 1 win, so this is harmless:
 *
 * <pre>
 *   keyBy(requestId + vendorId) + ProcessWindowFunction:
 *     Per key per window: stores 1-2 TaggedEvent objects → trivial
 *
 *   keyBy(requestId + vendorId) + KeyedCoProcessFunction:
 *     Per key per window: stores 1 BidRequest + 1 WinEvent → same amount
 * </pre>
 *
 * <p>But if the key were coarser (e.g., {@code keyBy(vendorId)} with thousands of
 * requests per vendor per window), the difference becomes significant:
 *
 * <pre>
 *   keyBy(vendorId) + ProcessWindowFunction:
 *     vendor-1 window: [req-001, req-002, ..., req-50000, win-001, win-003, ...]
 *     ALL 50K+ records buffered in state before process() is called!
 *     Then must iterate all to separate requests from wins and match them.
 *     Memory: O(all records in window)
 *
 *   keyBy(vendorId) + KeyedCoProcessFunction:
 *     processElement1: requestState.put(reqId, request)   → 1 state write per record
 *     processElement2: winState.put(reqId, win)            → 1 state write per record
 *     onTimer: iterate requestState, look up matching win  → same total work
 *     BUT records are already organized by reqId in state — no bulk buffering.
 * </pre>
 *
 * <h2>Why keyBy(requestId + vendorId) Is the Right Key</h2>
 *
 * <p>The join key should match the join predicate. Since we join ON
 * {@code (requestId, vendorId)}, that should be the {@code keyBy}:
 *
 * <pre>
 *   keyBy(requestId + vendorId):
 *     ✓ Each key has exactly 1 request + 0-1 win
 *     ✓ State is tiny per key (O(1))
 *     ✓ Timer fires → read 1 req + 1 win → done
 *
 *   keyBy(vendorId):         ← WRONG for this join
 *     ✗ Each key has 1000s of requests + hundreds of wins
 *     ✗ Must iterate and match them at window fire
 *     ✗ Basically reimplementing a hash join inside the operator
 *     ✗ State design becomes awkward (composite MapState keys)
 *     ✗ Only makes sense if you also need per-vendor aggregation
 * </pre>
 *
 * <p>Reuses the same {@link BidWinJoin.BidRequest}, {@link BidWinJoin.WinEvent},
 * {@link BidWinJoin.JoinedResult} POJOs and source functions from {@link BidWinJoin}.
 */
public class BidWinCoProcessJoin {

    private static final long WINDOW_SIZE_MS = 5 * 60 * 1000L; // 5 minutes
    private static final long ALLOWED_LATENESS_MS = 1 * 60 * 1000L; // 1 minute

    static final OutputTag<BidWinJoin.BidRequest> LATE_REQUEST_TAG =
            new OutputTag<BidWinJoin.BidRequest>("late-request") {};

    static final OutputTag<BidWinJoin.WinEvent> LATE_WIN_TAG =
            new OutputTag<BidWinJoin.WinEvent>("late-win") {};

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setParallelism(1);

        // ============================================================
        // Requests stream: filter to bid=true only
        // ============================================================
        DataStream<BidWinJoin.BidRequest> bidRequests =
                env.addSource(new BidWinJoin.BidRequestSource())
                        .filter(BidWinJoin.BidRequest::isBid)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<BidWinJoin.BidRequest>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(30))
                                        .withTimestampAssigner(
                                                (r, ts) -> r.getRequestTimestamp()));

        // ============================================================
        // Wins stream: with idleness
        // ============================================================
        DataStream<BidWinJoin.WinEvent> wins =
                env.addSource(new BidWinJoin.WinSource())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<BidWinJoin.WinEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(30))
                                        .withTimestampAssigner(
                                                (w, ts) -> w.getWinTimestamp())
                                        .withIdleness(Duration.ofMinutes(10)));

        // ============================================================
        // Connect + KeyedCoProcessFunction
        //
        // Both streams keyed by (requestId + "_" + vendorId).
        // The CoProcessFunction handles:
        //   - Buffering requests and wins in state per window
        //   - Firing at window end to emit join results
        //   - Side-outputting late data (what .join() can't do!)
        // ============================================================
        SingleOutputStreamOperator<BidWinJoin.JoinedResult> results =
                bidRequests
                        .connect(wins)
                        .keyBy(
                                r -> r.getRequestId() + "_" + r.getVendorId(),
                                w -> w.getRequestId() + "_" + w.getVendorId())
                        .process(new BidWinCoProcessJoinFunction());

        results.print("JOINED");
        results.getSideOutput(LATE_REQUEST_TAG).print("LATE-REQ");
        results.getSideOutput(LATE_WIN_TAG).print("LATE-WIN");

        env.execute("Bid-Win Join via KeyedCoProcessFunction");
    }

    // ================================================================
    // KeyedCoProcessFunction: manual windowed join with side outputs
    //
    // State layout (per key = requestId + vendorId):
    //   - requestState:  MapState<windowEnd, BidRequest>
    //   - winState:      MapState<windowEnd, WinEvent>
    //   - firedWindows:  MapState<windowEnd, Boolean>
    //
    // On each element:
    //   1. Compute which window it belongs to
    //   2. If watermark has passed window + lateness → side output
    //   3. Otherwise → store in state, register timer at windowEnd
    //
    // On timer (window boundary):
    //   1. Look up request and win for this window
    //   2. Join them (left outer: unmatched bids emitted too)
    //   3. Register cleanup timer at windowEnd + lateness
    //
    // On cleanup timer:
    //   1. Remove state for this window
    // ================================================================
    public static class BidWinCoProcessJoinFunction
            extends KeyedCoProcessFunction<
                    String, BidWinJoin.BidRequest, BidWinJoin.WinEvent, BidWinJoin.JoinedResult> {

        /**
         * Stores the request per window. Key = windowEnd. For this join key
         * (requestId+vendorId), there's typically one request per window.
         */
        private transient MapState<Long, BidWinJoin.BidRequest> requestState;

        /** Stores the win per window. Key = windowEnd. */
        private transient MapState<Long, BidWinJoin.WinEvent> winState;

        /** Tracks which windows have already fired (for re-fire on late data). */
        private transient MapState<Long, Boolean> firedWindows;

        @Override
        public void open(OpenContext openContext) throws Exception {
            requestState =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "requests",
                                            Long.class,
                                            BidWinJoin.BidRequest.class));
            winState =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "wins",
                                            Long.class,
                                            BidWinJoin.WinEvent.class));
            firedWindows =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "fired", Long.class, Boolean.class));
        }

        @Override
        public void processElement1(
                BidWinJoin.BidRequest request,
                Context ctx,
                Collector<BidWinJoin.JoinedResult> out)
                throws Exception {

            long windowEnd = getWindowEnd(request.getRequestTimestamp());
            long watermark = ctx.timerService().currentWatermark();

            // Late data check: if watermark has passed window + allowed lateness
            if (watermark >= windowEnd + ALLOWED_LATENESS_MS) {
                // Side output! This is what .join() API cannot do.
                ctx.output(LATE_REQUEST_TAG, request);
                System.out.printf(
                        "[LATE-REQ] req=%s watermark=%d > windowEnd=%d + lateness=%d%n",
                        request.getRequestId(), watermark, windowEnd, ALLOWED_LATENESS_MS);
                return;
            }

            // Store in state
            requestState.put(windowEnd, request);

            // Register timer at window end (deduplicated by Flink — same timer = no-op)
            ctx.timerService().registerEventTimeTimer(windowEnd);

            // If window already fired (late data within lateness), re-fire immediately
            if (Boolean.TRUE.equals(firedWindows.get(windowEnd))) {
                emitJoinResult(windowEnd, out);
            }
        }

        @Override
        public void processElement2(
                BidWinJoin.WinEvent win,
                Context ctx,
                Collector<BidWinJoin.JoinedResult> out)
                throws Exception {

            long windowEnd = getWindowEnd(win.getWinTimestamp());
            long watermark = ctx.timerService().currentWatermark();

            // Late data check
            if (watermark >= windowEnd + ALLOWED_LATENESS_MS) {
                ctx.output(LATE_WIN_TAG, win);
                System.out.printf(
                        "[LATE-REQ] req=%s watermark=%d > windowEnd=%d + lateness=%d%n",
                        win.getRequestId(), watermark, windowEnd, ALLOWED_LATENESS_MS);
                return;
            }

            // Store in state (keep highest price if duplicate wins)
            BidWinJoin.WinEvent existing = winState.get(windowEnd);
            if (existing == null || win.getWinPrice() > existing.getWinPrice()) {
                winState.put(windowEnd, win);
            }

            ctx.timerService().registerEventTimeTimer(windowEnd);

            // Re-fire if window already fired
            if (Boolean.TRUE.equals(firedWindows.get(windowEnd))) {
                emitJoinResult(windowEnd, out);
            }
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<BidWinJoin.JoinedResult> out)
                throws Exception {

            long windowEnd = timestamp;

            // Check if this is a cleanup timer (windowEnd + lateness)
            // We use the convention: cleanup timers fire at windowEnd + lateness + 1
            long cleanupTime = windowEnd + ALLOWED_LATENESS_MS + 1;
            if (timestamp == cleanupTime) {
                // Cleanup: remove state for this window
                requestState.remove(windowEnd - ALLOWED_LATENESS_MS - 1);
                winState.remove(windowEnd - ALLOWED_LATENESS_MS - 1);
                firedWindows.remove(windowEnd - ALLOWED_LATENESS_MS - 1);
                return;
            }

            // Window fire: emit join result
            emitJoinResult(windowEnd, out);
            firedWindows.put(windowEnd, true);

            // Register cleanup timer
            ctx.timerService()
                    .registerEventTimeTimer(windowEnd + ALLOWED_LATENESS_MS + 1);
        }

        private void emitJoinResult(long windowEnd, Collector<BidWinJoin.JoinedResult> out)
                throws Exception {

            long windowStart = windowEnd - WINDOW_SIZE_MS;
            BidWinJoin.BidRequest request = requestState.get(windowEnd);
            BidWinJoin.WinEvent win = winState.get(windowEnd);

            if (request == null) {
                // Win without request (unjoined) — log but don't emit
                if (win != null) {
                    System.out.printf(
                            "[UNJOINED-WIN] Window [%d, %d) | req=%s%n",
                            windowStart, windowEnd, win.getRequestId());
                }
                return;
            }

            // Build result
            BidWinJoin.JoinedResult result = new BidWinJoin.JoinedResult();
            result.setRequestId(request.getRequestId());
            result.setVendorId(request.getVendorId());
            result.setUrl(request.getUrl());
            result.setCampaignId(request.getCampaignId());
            result.setCreativeId(request.getCreativeId());
            result.setRequestTimestamp(request.getRequestTimestamp());
            result.setWindowStart(windowStart);
            result.setWindowEnd(windowEnd);

            if (win != null) {
                result.setMatched(true);
                result.setWinTimestamp(win.getWinTimestamp());
                result.setWinPrice(win.getWinPrice());
            } else {
                result.setMatched(false);
            }

            out.collect(result);

            System.out.printf(
                    "[COPROCESS] Window [%d, %d) | %s | %s%n",
                    windowStart,
                    windowEnd,
                    request.getRequestId(),
                    win != null
                            ? String.format("WIN price=%.2f", win.getWinPrice())
                            : "NO WIN");
        }

        /** Compute the end of the 5-minute tumbling window containing this timestamp. */
        private static long getWindowEnd(long timestamp) {
            return (timestamp / WINDOW_SIZE_MS + 1) * WINDOW_SIZE_MS;
        }
    }
}
