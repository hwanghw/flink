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

/**
 * Full interval join using {@link KeyedCoProcessFunction} with support for:
 * <ul>
 *   <li>Matched pairs (request + win within 5-minute interval) → main output</li>
 *   <li>Late data (beyond interval + 1-minute allowed lateness) → side output</li>
 *   <li><b>Unmatched requests</b> (bid with no win) → side output</li>
 *   <li><b>Unmatched wins</b> (win with no request) → side output</li>
 * </ul>
 *
 * <h2>Why not Flink's built-in interval join?</h2>
 *
 * <p>The built-in {@code intervalJoin} API ({@link BidWinBuiltinIntervalJoin}) is an
 * <b>inner join</b> — it only emits matched pairs. Unmatched records are silently dropped.
 * In ad-tech, ~60-80% of bids don't win, so losing those records means losing most of
 * your data for analytics, debugging, and billing reconciliation.
 *
 * <h2>How it works</h2>
 *
 * <pre>
 *   bidRequests ──┐
 *                  ├── connect ──► keyBy(reqId + vendorId) ──► KeyedCoProcessFunction
 *   wins ─────────┘
 *
 *   Join condition: win.ts ∈ [request.ts, request.ts + INTERVAL]
 *
 *   Allowed lateness: events within INTERVAL + 1min of watermark are still accepted.
 *   Beyond that → LATE side output.
 *
 *   Wins stream: withIdleness(10min) — if wins go quiet for 10 minutes,
 *   Flink stops waiting for its watermark, letting requests drive time forward.
 *
 *   processElement1 (request arrives):
 *     if watermark > request.ts + INTERVAL + LATENESS → LATE, side output
 *     else → store request in ValueState
 *            check if a buffered win matches → join immediately if so
 *            register timer at request.ts + INTERVAL (unmatched timeout)
 *
 *   processElement2 (win arrives):
 *     if watermark > win.ts + INTERVAL + LATENESS → LATE, side output
 *     else → look up request in ValueState
 *            if found AND win.ts within interval → join, emit result
 *            if not found → buffer win, register timeout timer
 *
 *   onTimer (request timeout):
 *     if request still unmatched → side output as UNMATCHED_REQUEST
 *     cleanup state
 *
 *   onTimer (win timeout):
 *     if win still unmatched → side output as UNMATCHED_WIN
 *     cleanup state
 * </pre>
 *
 * <h2>Comparison with built-in interval join</h2>
 *
 * <pre>
 *   | Feature                  | Built-in intervalJoin       | This (KeyedCoProcessFunction)  |
 *   |--------------------------|-----------------------------|--------------------------------|
 *   | Matched pairs            | ✓                           | ✓                              |
 *   | Late data side output    | ✓ sideOutputLeft/RightLate  | ✓ ctx.output(LATE_TAG)         |
 *   | Unmatched request output | ✗ SILENTLY DROPPED          | ✓ ctx.output(UNMATCHED_REQ)    |
 *   | Unmatched win output     | ✗ SILENTLY DROPPED          | ✓ ctx.output(UNMATCHED_WIN)    |
 *   | Code complexity          | Simple (~20 lines)          | More (~150 lines)              |
 *   | State management         | Flink handles it            | Manual ValueState + timers     |
 * </pre>
 *
 * @see BidWinBuiltinIntervalJoin for the simpler built-in API (inner join only)
 * @see BidWinJoin for the tumbling window approach
 * @see IntervalJoinSource for the shared source demonstrating cross-window-boundary events
 */
public class BidWinIntervalJoin {

    /** Join interval: win must arrive within 5 minutes of the request. */
    private static final long JOIN_INTERVAL_MS = 5 * 60 * 1000L;

    /** Allowed lateness: events arriving within this grace period after their join window
     *  closes are still processed. Beyond this → LATE side output. */
    private static final long ALLOWED_LATENESS_MS = 1 * 60 * 1000L;

    static final OutputTag<BidWinJoin.BidRequest> LATE_REQUEST_TAG =
            new OutputTag<BidWinJoin.BidRequest>("late-request") {};

    static final OutputTag<BidWinJoin.WinEvent> LATE_WIN_TAG =
            new OutputTag<BidWinJoin.WinEvent>("late-win") {};

    static final OutputTag<BidWinJoin.BidRequest> UNMATCHED_REQUEST_TAG =
            new OutputTag<BidWinJoin.BidRequest>("unmatched-request") {};

    static final OutputTag<BidWinJoin.WinEvent> UNMATCHED_WIN_TAG =
            new OutputTag<BidWinJoin.WinEvent>("unmatched-win") {};

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);

        // Parallelism = 1 for this demo. With higher parallelism, the
        // SourceFunction.run() executes on ALL subtasks, emitting duplicate
        // data. This also causes unpredictable watermark timing because
        // multiple wins subtasks go idle/reactivate at different wall-clock
        // times, creating races in the min-watermark calculation.
        // In production, Kafka/Kinesis sources handle partitioning correctly.
        env.setParallelism(1);

        DataStream<BidWinJoin.BidRequest> bidRequests =
                env.addSource(new IntervalJoinSource.BidRequestSource())
                        .filter(BidWinJoin.BidRequest::isBid)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<BidWinJoin.BidRequest>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(30))
                                        .withTimestampAssigner(
                                                (r, ts) -> r.getRequestTimestamp()));

        // ============================================================
        // Wins stream: withIdleness(10 min)
        //
        // HOW withIdleness WORKS INTERNALLY:
        //
        // 1. WatermarkStrategy.withIdleness() wraps the generator in
        //    WatermarksWithIdleness (flink-core eventtime package).
        //
        // 2. Inside WatermarksWithIdleness, an IdlenessTimer tracks
        //    activity using a WALL-CLOCK timer (not event-time):
        //      - onEvent():        idlenessTimer.activity() — resets counter
        //      - onPeriodicEmit(): idlenessTimer.checkIfIdle()
        //        → if no events for > idleTimeout → output.markIdle()
        //
        // 3. markIdle() emits WatermarkStatus.IDLE downstream.
        //
        // 4. StatusWatermarkValve (in the downstream operator) receives
        //    IDLE status and REMOVES this subpartition from the
        //    min-watermark calculation:
        //      - Before idle: globalWM = min(requests_wm, wins_wm)
        //                     = min(570s, 320s) = 320s  ← wins holds back!
        //      - After idle:  globalWM = requests_wm = 570s  ← wins excluded
        //
        // 5. When wins becomes active again (new event arrives):
        //      - onEvent() sets isIdleNow=false, resets counter
        //      - Next onPeriodicEmit() calls markActive()
        //      - Emits WatermarkStatus.ACTIVE
        //      - StatusWatermarkValve re-adds wins to min-watermark calc
        //      - If wins' watermark is behind, it's marked "unaligned"
        //        until it catches up to lastOutputWatermark
        //
        // 6. The timer uses PausableRelativeClock which subtracts
        //    backpressure time — so backpressure does NOT trigger
        //    false idle detection.
        //
        // WHY THIS MATTERS HERE:
        //   Wins stream is sparse (only ~30-40% of bids win).
        //   Between win bursts, wins goes quiet. Without idleness,
        //   the global watermark would be stuck at the last win's
        //   timestamp, preventing timers from firing and late
        //   detection from working on the requests side.
        //
        // NOTE: We use 5 seconds here so the demo runs quickly.
        // In production, use a longer timeout (e.g., 10 minutes)
        // that reflects how long the wins stream can realistically
        // go silent before you want watermarks to advance without it.
        // ============================================================
        DataStream<BidWinJoin.WinEvent> wins =
                env.addSource(new IntervalJoinSource.WinSource())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<BidWinJoin.WinEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(30))
                                        .withTimestampAssigner(
                                                (w, ts) -> w.getWinTimestamp())
                                        .withIdleness(Duration.ofSeconds(5)));  // 5s for demo; 10min in prod

        SingleOutputStreamOperator<BidWinJoin.JoinedResult> results =
                bidRequests
                        .connect(wins)
                        .keyBy(
                                r -> r.getRequestId() + "_" + r.getVendorId(),
                                w -> w.getRequestId() + "_" + w.getVendorId())
                        .process(new IntervalJoinFunction());

        results.print("JOINED");
        results.getSideOutput(LATE_REQUEST_TAG).print("LATE-REQ");
        results.getSideOutput(LATE_WIN_TAG).print("LATE-WIN");
        results.getSideOutput(UNMATCHED_REQUEST_TAG).print("UNMATCHED-REQ");
        results.getSideOutput(UNMATCHED_WIN_TAG).print("UNMATCHED-WIN");

        env.execute("Bid-Win Interval Join (Full: matched + late + unmatched)");
    }

    // ================================================================
    // Interval Join via KeyedCoProcessFunction
    //
    // State (per key = requestId + vendorId):
    //   requestState:  the bid request (at most 1)
    //   winState:      the win event (at most 1)
    //   matchedState:  whether this key has been joined
    //
    // Timer conventions (to distinguish request vs win timeouts):
    //   request timeout fires at: request.ts + INTERVAL
    //   win timeout fires at:     win.ts + INTERVAL + 1  (offset by 1ms)
    //   cleanup fires at:         max(request timeout, win timeout) + 1
    // ================================================================
    public static class IntervalJoinFunction
            extends KeyedCoProcessFunction<
                    String, BidWinJoin.BidRequest, BidWinJoin.WinEvent, BidWinJoin.JoinedResult> {

        private transient ValueState<BidWinJoin.BidRequest> requestState;
        private transient ValueState<BidWinJoin.WinEvent> winState;
        private transient ValueState<Boolean> matchedState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            requestState =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "request", BidWinJoin.BidRequest.class));
            winState =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>("win", BidWinJoin.WinEvent.class));
            matchedState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("matched", Boolean.class));
        }

        @Override
        public void processElement1(
                BidWinJoin.BidRequest request,
                Context ctx,
                Collector<BidWinJoin.JoinedResult> out)
                throws Exception {

            long watermark = ctx.timerService().currentWatermark();

            System.out.printf(
                    "[PROCESS-REQ] %s ts=%d watermark=%d%n",
                    request.getRequestId(), request.getRequestTimestamp(), watermark);

            // Late check: if watermark is past request's join window + allowed lateness
            // i.e., even with the grace period, this event is too late to join
            if (watermark > request.getRequestTimestamp() + JOIN_INTERVAL_MS + ALLOWED_LATENESS_MS) {
                ctx.output(LATE_REQUEST_TAG, request);
                System.out.printf(
                        "[LATE-REQ] %s ts=%d watermark=%d > ts+interval+lateness=%d%n",
                        request.getRequestId(),
                        request.getRequestTimestamp(),
                        watermark,
                        request.getRequestTimestamp() + JOIN_INTERVAL_MS + ALLOWED_LATENESS_MS);
                return;
            }

            // Store request
            requestState.update(request);

            // Check if a win already arrived and is within interval
            BidWinJoin.WinEvent bufferedWin = winState.value();
            if (bufferedWin != null) {
                long gap = bufferedWin.getWinTimestamp() - request.getRequestTimestamp();
                if (gap >= 0 && gap <= JOIN_INTERVAL_MS) {
                    // Match! Emit joined result
                    emitJoined(request, bufferedWin, out);
                    matchedState.update(true);
                }
            }

            // Register timeout: if no win arrives within INTERVAL, emit as unmatched
            ctx.timerService()
                    .registerEventTimeTimer(request.getRequestTimestamp() + JOIN_INTERVAL_MS);
        }

        @Override
        public void processElement2(
                BidWinJoin.WinEvent win,
                Context ctx,
                Collector<BidWinJoin.JoinedResult> out)
                throws Exception {

            long watermark = ctx.timerService().currentWatermark();

            System.out.printf(
                    "[PROCESS-WIN] %s ts=%d watermark=%d%n",
                    win.getRequestId(), win.getWinTimestamp(), watermark);

            // Late check: watermark past win's join window + allowed lateness
            if (watermark > win.getWinTimestamp() + JOIN_INTERVAL_MS + ALLOWED_LATENESS_MS) {
                ctx.output(LATE_WIN_TAG, win);
                System.out.printf(
                        "[LATE-WIN] %s ts=%d watermark=%d > ts+interval+lateness=%d%n",
                        win.getRequestId(),
                        win.getWinTimestamp(),
                        watermark,
                        win.getWinTimestamp() + JOIN_INTERVAL_MS + ALLOWED_LATENESS_MS);
                return;
            }

            // Check if request already arrived
            BidWinJoin.BidRequest bufferedRequest = requestState.value();
            if (bufferedRequest != null) {
                long gap = win.getWinTimestamp() - bufferedRequest.getRequestTimestamp();
                if (gap >= 0 && gap <= JOIN_INTERVAL_MS) {
                    // Match!
                    emitJoined(bufferedRequest, win, out);
                    // Store win but don't clear state yet. The request timer
                    // (registered in processElement1) will still fire — we can't
                    // cancel it. While clearing state here would be safe in the
                    // happy path (onTimer's null checks would make it a no-op),
                    // keeping matchedState around guards against duplicate/retried
                    // events: if a second win arrives for the same key before the
                    // timer fires, matchedState prevents re-processing. Cleanup
                    // is centralized in onTimer once all timers have fired.
                    matchedState.update(true);
                    winState.update(win);  // store for cleanup
                    return;
                }
            }

            // No matching request yet — buffer the win and wait
            winState.update(win);

            // Register win timeout (+1ms offset to distinguish from request timer)
            ctx.timerService()
                    .registerEventTimeTimer(win.getWinTimestamp() + JOIN_INTERVAL_MS + 1);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<BidWinJoin.JoinedResult> out)
                throws Exception {

            boolean matched = Boolean.TRUE.equals(matchedState.value());

            BidWinJoin.BidRequest request = requestState.value();
            BidWinJoin.WinEvent win = winState.value();

            if (!matched) {
                // Check if this is a request timeout or win timeout
                if (request != null
                        && timestamp == request.getRequestTimestamp() + JOIN_INTERVAL_MS) {
                    // Request timeout: no win arrived within interval
                    ctx.output(UNMATCHED_REQUEST_TAG, request);
                    System.out.printf(
                            "[UNMATCHED-REQ] %s — no win within %dms%n",
                            request.getRequestId(), JOIN_INTERVAL_MS);
                }

                if (win != null
                        && timestamp == win.getWinTimestamp() + JOIN_INTERVAL_MS + 1) {
                    // Win timeout: no matching request found
                    if (request == null) {
                        ctx.output(UNMATCHED_WIN_TAG, win);
                        System.out.printf(
                                "[UNMATCHED-WIN] %s — no matching request%n",
                                win.getRequestId());
                    }
                }
            }

            // Cleanup: if both timers have fired, clear all state
            boolean requestTimerDone =
                    request == null
                            || timestamp >= request.getRequestTimestamp() + JOIN_INTERVAL_MS;
            boolean winTimerDone =
                    win == null || timestamp >= win.getWinTimestamp() + JOIN_INTERVAL_MS + 1;

            if (requestTimerDone && winTimerDone) {
                requestState.clear();
                winState.clear();
                matchedState.clear();
            }
        }

        private void emitJoined(
                BidWinJoin.BidRequest request,
                BidWinJoin.WinEvent win,
                Collector<BidWinJoin.JoinedResult> out) {

            BidWinJoin.JoinedResult result = new BidWinJoin.JoinedResult();
            result.setRequestId(request.getRequestId());
            result.setVendorId(request.getVendorId());
            result.setUrl(request.getUrl());
            result.setCampaignId(request.getCampaignId());
            result.setCreativeId(request.getCreativeId());
            result.setRequestTimestamp(request.getRequestTimestamp());
            result.setWinTimestamp(win.getWinTimestamp());
            result.setWinPrice(win.getWinPrice());
            result.setMatched(true);

            out.collect(result);

            System.out.printf(
                    "[JOINED] %s | reqTs=%d winTs=%d gap=%dms price=%.2f%n",
                    request.getRequestId(),
                    request.getRequestTimestamp(),
                    win.getWinTimestamp(),
                    win.getWinTimestamp() - request.getRequestTimestamp(),
                    win.getWinPrice());
        }
    }
}
