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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Joins bid requests and win events using Flink's <b>built-in interval join API</b>.
 *
 * <h2>Why interval join instead of tumbling window join?</h2>
 *
 * <p>Tumbling windows can miss valid joins when events straddle a window boundary:
 *
 * <pre>
 *   Tumbling window [00:00, 00:05)          [00:05, 00:10)
 *     request at 00:03:30                      win at 00:05:30
 *     ← different windows! NEVER JOINED even though gap = 2 min →
 *
 *   Interval join: win.ts ∈ [request.ts, request.ts + 5min]
 *     request at 00:03:30 → accepts wins from 00:03:30 to 00:08:30
 *     win at 00:05:30     → ✓ within interval → JOINED!
 * </pre>
 *
 * <h2>Built-in API features</h2>
 *
 * <ul>
 *   <li>{@code sideOutputLeftLateData(tag)} — captures late requests ✓</li>
 *   <li>{@code sideOutputRightLateData(tag)} — captures late wins ✓</li>
 *   <li>{@code ProcessJoinFunction} — processes matched pairs ✓</li>
 * </ul>
 *
 * <h2 style="color:red">⚠ CRITICAL LIMITATION: Inner Join Only — Unmatched Records Are DROPPED</h2>
 *
 * <p><b>The built-in interval join is an INNER JOIN.</b> This means:
 *
 * <pre>
 *   ✓ Request with matching win  → emitted via ProcessJoinFunction
 *   ✓ Late request/win           → emitted to side output
 *
 *   ✗ Request WITHOUT a win      → SILENTLY DROPPED (no output anywhere!)
 *   ✗ Win WITHOUT a request      → SILENTLY DROPPED
 * </pre>
 *
 * <p>In ad-tech, most bids do NOT win (~60-80% unmatched). With this API, those unmatched
 * bids simply vanish. If you need unmatched records (e.g., for win-rate analytics, debugging,
 * or billing reconciliation), use {@link BidWinIntervalJoin} which implements a full
 * outer interval join via {@code KeyedCoProcessFunction}.
 *
 * @see BidWinIntervalJoin for the full interval join with unmatched record side outputs
 * @see BidWinJoin for the tumbling window approach (union + ProcessWindowFunction)
 * @see BidWinCoProcessJoin for the tumbling window approach (connect + KeyedCoProcessFunction)
 */
public class BidWinBuiltinIntervalJoin {

    private static final OutputTag<BidWinJoin.BidRequest> LATE_REQUEST_TAG =
            new OutputTag<BidWinJoin.BidRequest>("late-request") {};

    private static final OutputTag<BidWinJoin.WinEvent> LATE_WIN_TAG =
            new OutputTag<BidWinJoin.WinEvent>("late-win") {};

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);

        // Parallelism = 1 for this demo to avoid duplicate data from SourceFunction
        // and unpredictable watermark timing across multiple source subtasks.
        env.setParallelism(1);

        // Requests: filter to bid=true
        DataStream<BidWinJoin.BidRequest> bidRequests =
                env.addSource(new IntervalJoinSource.BidRequestSource())
                        .filter(BidWinJoin.BidRequest::isBid)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<BidWinJoin.BidRequest>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(30))
                                        .withTimestampAssigner(
                                                (r, ts) -> r.getRequestTimestamp()));

        // Wins: with idleness for sparse stream
        // 5 seconds for demo; in production use 10 minutes or appropriate for your win latency
        DataStream<BidWinJoin.WinEvent> wins =
                env.addSource(new IntervalJoinSource.WinSource())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<BidWinJoin.WinEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(30))
                                        .withTimestampAssigner(
                                                (w, ts) -> w.getWinTimestamp())
                                        .withIdleness(Duration.ofSeconds(5)));

        // ============================================================
        // Built-in interval join:
        //   Join condition: request.ts <= win.ts <= request.ts + 5min
        //   i.e., win arrives within 5 minutes AFTER the request.
        //
        //   Late data → side output
        //   Unmatched records → DROPPED (inner join only!)
        // ============================================================
        SingleOutputStreamOperator<BidWinJoin.JoinedResult> results =
                bidRequests
                        .keyBy(r -> r.getRequestId() + "_" + r.getVendorId())
                        .intervalJoin(
                                wins.keyBy(
                                        w -> w.getRequestId() + "_" + w.getVendorId()))
                        .between(Duration.ZERO, Duration.ofMinutes(5))
                        .sideOutputLeftLateData(LATE_REQUEST_TAG)
                        .sideOutputRightLateData(LATE_WIN_TAG)
                        .process(new BidWinProcessJoinFunction());

        results.print("JOINED");
        results.getSideOutput(LATE_REQUEST_TAG).print("LATE-REQ");
        results.getSideOutput(LATE_WIN_TAG).print("LATE-WIN");

        // NOTE: there is NO output for unmatched requests or wins!
        // They are silently dropped by the inner join.

        env.execute("Bid-Win Built-in Interval Join (Inner Join Only)");
    }

    /** Processes matched (request, win) pairs. Only called for successful joins. */
    public static class BidWinProcessJoinFunction
            extends ProcessJoinFunction<
                    BidWinJoin.BidRequest, BidWinJoin.WinEvent, BidWinJoin.JoinedResult> {

        @Override
        public void processElement(
                BidWinJoin.BidRequest request,
                BidWinJoin.WinEvent win,
                Context ctx,
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
                    "[INTERVAL-JOIN] req=%s vendor=%s | reqTs=%d winTs=%d gap=%dms price=%.2f%n",
                    request.getRequestId(),
                    request.getVendorId(),
                    request.getRequestTimestamp(),
                    win.getWinTimestamp(),
                    win.getWinTimestamp() - request.getRequestTimestamp(),
                    win.getWinPrice());
        }
    }
}
