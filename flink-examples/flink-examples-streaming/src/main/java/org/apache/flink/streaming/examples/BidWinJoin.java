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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Joins two ad-tech streams — bid requests and win notifications — in a tumbling event-time
 * window, with late data handling via side outputs.
 *
 * <h2>Streams</h2>
 *
 * <pre>
 *   Requests stream:                         Wins stream:
 *   ┌─────────────────────────────────┐      ┌──────────────────────────┐
 *   │ requestId, vendorId,            │      │ requestId, vendorId,     │
 *   │ requestTimestamp, url,          │      │ winTimestamp, winPrice    │
 *   │ creativeId, campaignId, bid     │      │                          │
 *   └─────────────────────────────────┘      └──────────────────────────┘
 *          │                                         │
 *          │ filter(bid == true)                      │ withIdleness(10 min)
 *          ▼                                         ▼
 *   tag as TaggedEvent(REQUEST)              tag as TaggedEvent(WIN)
 *          │                                         │
 *          └──────────── union ──────────────────────┘
 *                          │
 *                          ▼
 *              keyBy(requestId + "_" + vendorId)
 *                          │
 *                          ▼
 *              TumblingEventTimeWindows(5 min)
 *              allowedLateness(1 min)
 *              sideOutputLateData(LATE_TAG)
 *                          │
 *                          ▼
 *              ProcessWindowFunction:
 *                separate requests from wins,
 *                join by requestId+vendorId,
 *                emit matched + unmatched bids
 * </pre>
 *
 * <h2>Why union-then-window instead of coGroup/join API?</h2>
 *
 * <p>Flink's {@code coGroup()} and {@code join()} APIs create a
 * {@code CoGroupedStreams.WithWindow} / {@code JoinedStreams.WithWindow} that supports
 * {@code allowedLateness()} but does <b>NOT</b> support {@code sideOutputLateData()}.
 * The union approach gives us full {@link ProcessWindowFunction} with window context
 * and side outputs.
 *
 * <h2>Idleness on the wins stream</h2>
 *
 * <p>The wins stream is sparse — not every bid wins, and wins may arrive in bursts.
 * Without {@code withIdleness(10 min)}, a quiet wins stream would hold back the global
 * watermark indefinitely, preventing windows from firing even when the requests stream
 * has advanced. Idleness tells Flink: "if no events arrive for 10 minutes, stop waiting
 * for this stream's watermark and let other streams drive progress."
 */
public class BidWinJoin {

    /** Late events that arrive after the 1-minute allowed lateness. */
    private static final OutputTag<TaggedEvent> LATE_TAG =
            new OutputTag<TaggedEvent>("late-data") {};

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setParallelism(1);

        // ============================================================
        // Requests stream: filter to bid=true only
        // ============================================================
        DataStream<TaggedEvent> bidRequests =
                env.addSource(new BidRequestSource())
                        .filter(BidRequest::isBid)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<BidRequest>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(30))
                                        .withTimestampAssigner(
                                                (r, ts) -> r.getRequestTimestamp()))
                        .map(r -> TaggedEvent.request(r))
                        .returns(TypeInformation.of(new TypeHint<TaggedEvent>() {}));

        // ============================================================
        // Wins stream: with idleness to prevent watermark stalling
        //
        // withIdleness(10 min): if the wins stream has no events for
        // 10 minutes, Flink marks it as idle and stops using its
        // watermark for global watermark computation. This prevents
        // a quiet wins stream from holding back window firing.
        // ============================================================
        DataStream<TaggedEvent> wins =
                env.addSource(new WinSource())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<WinEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(30))
                                        .withTimestampAssigner(
                                                (w, ts) -> w.getWinTimestamp())
                                        .withIdleness(Duration.ofMinutes(10)))
                        .map(w -> TaggedEvent.win(w))
                        .returns(TypeInformation.of(new TypeHint<TaggedEvent>() {}));

        // ============================================================
        // Union → KeyBy → Window → Join
        //
        // Both streams are tagged and unioned into one stream, then
        // keyed by (requestId, vendorId) and windowed. The
        // ProcessWindowFunction separates requests from wins and
        // joins them, emitting both matched and unmatched bids.
        // ============================================================
        SingleOutputStreamOperator<JoinedResult> results =
                bidRequests
                        .union(wins)
                        .keyBy(TaggedEvent::getJoinKey)
                        .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
                        .allowedLateness(Duration.ofMinutes(1))
                        .sideOutputLateData(LATE_TAG)
                        .process(new BidWinJoinFunction());

        results.print("JOINED");
        results.getSideOutput(LATE_TAG).print("LATE");

        env.execute("Ad-Tech Bid-Win Join");
    }

    // ================================================================
    // Join Logic: ProcessWindowFunction
    //
    // Receives all tagged events (requests + wins) for one
    // (requestId, vendorId) key in one 5-minute window.
    //
    // Separates them, then:
    //   - If a request has a matching win → emit joined result
    //   - If a request has no win → emit unmatched result
    //   - If a win has no request → log unjoined win (can occur
    //     with late data or data inconsistency)
    // ================================================================
    public static class BidWinJoinFunction
            extends ProcessWindowFunction<TaggedEvent, JoinedResult, String, TimeWindow> {

        @Override
        public void process(
                String key,
                Context context,
                Iterable<TaggedEvent> elements,
                Collector<JoinedResult> out) {

            TimeWindow window = context.window();

            // Separate requests and wins
            // Key is (requestId + "_" + vendorId), so normally there's 1 request
            // and 0 or 1 win. But with late data re-fires, there could be multiples.
            List<BidRequest> requests = new ArrayList<>();
            List<WinEvent> winEvents = new ArrayList<>();

            for (TaggedEvent event : elements) {
                if (event.isRequest()) {
                    requests.add(event.getRequest());
                } else {
                    winEvents.add(event.getWin());
                }
            }

            // Build win lookup: requestId+vendorId → WinEvent
            // (in this example, key already is requestId+vendorId,
            //  so there's typically 0 or 1 win, but we handle multiples)
            Map<String, WinEvent> winMap = new HashMap<>();
            for (WinEvent w : winEvents) {
                String winKey = w.getRequestId() + "_" + w.getVendorId();
                // Keep the win with the highest price if duplicates
                winMap.merge(
                        winKey,
                        w,
                        (existing, incoming) ->
                                incoming.getWinPrice() > existing.getWinPrice()
                                        ? incoming
                                        : existing);
            }

            // Join: for each request, find matching win
            for (BidRequest req : requests) {
                String reqKey = req.getRequestId() + "_" + req.getVendorId();
                WinEvent matchedWin = winMap.remove(reqKey);

                JoinedResult result = new JoinedResult();
                result.setRequestId(req.getRequestId());
                result.setVendorId(req.getVendorId());
                result.setUrl(req.getUrl());
                result.setCampaignId(req.getCampaignId());
                result.setCreativeId(req.getCreativeId());
                result.setRequestTimestamp(req.getRequestTimestamp());
                result.setWindowStart(window.getStart());
                result.setWindowEnd(window.getEnd());

                if (matchedWin != null) {
                    result.setMatched(true);
                    result.setWinTimestamp(matchedWin.getWinTimestamp());
                    result.setWinPrice(matchedWin.getWinPrice());
                } else {
                    result.setMatched(false);
                }

                out.collect(result);
            }

            // Log unjoined wins (win without a matching request in this window)
            for (WinEvent unjoined : winMap.values()) {
                System.out.printf(
                        "[unjoined-WIN] Window [%d, %d) | req=%s vendor=%s price=%.2f%n",
                        window.getStart(),
                        window.getEnd(),
                        unjoined.getRequestId(),
                        unjoined.getVendorId(),
                        unjoined.getWinPrice());
            }
        }
    }

    // ================================================================
    // TaggedEvent: wrapper to union requests and wins into one stream
    // ================================================================
    public static class TaggedEvent {

        public enum Type {
            REQUEST,
            WIN
        }

        private Type type;
        private BidRequest request;
        private WinEvent win;

        public TaggedEvent() {}

        public static TaggedEvent request(BidRequest r) {
            TaggedEvent e = new TaggedEvent();
            e.type = Type.REQUEST;
            e.request = r;
            return e;
        }

        public static TaggedEvent win(WinEvent w) {
            TaggedEvent e = new TaggedEvent();
            e.type = Type.WIN;
            e.win = w;
            return e;
        }

        public boolean isRequest() {
            return type == Type.REQUEST;
        }

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public BidRequest getRequest() {
            return request;
        }

        public void setRequest(BidRequest request) {
            this.request = request;
        }

        public WinEvent getWin() {
            return win;
        }

        public void setWin(WinEvent win) {
            this.win = win;
        }

        public String getJoinKey() {
            if (type == Type.REQUEST) {
                return request.getRequestId() + "_" + request.getVendorId();
            } else {
                return win.getRequestId() + "_" + win.getVendorId();
            }
        }

        @Override
        public String toString() {
            return type == Type.REQUEST ? "Tagged[" + request + "]" : "Tagged[" + win + "]";
        }
    }

    // ================================================================
    // BidRequest POJO
    // ================================================================
    public static class BidRequest {

        private String requestId;
        private String vendorId;
        private long requestTimestamp;
        private String url;
        private String creativeId;
        private String campaignId;
        private boolean bid; // true = we placed a bid

        public BidRequest() {}

        public BidRequest(
                String requestId,
                String vendorId,
                long requestTimestamp,
                String url,
                String creativeId,
                String campaignId,
                boolean bid) {
            this.requestId = requestId;
            this.vendorId = vendorId;
            this.requestTimestamp = requestTimestamp;
            this.url = url;
            this.creativeId = creativeId;
            this.campaignId = campaignId;
            this.bid = bid;
        }

        public String getRequestId() {
            return requestId;
        }

        public void setRequestId(String requestId) {
            this.requestId = requestId;
        }

        public String getVendorId() {
            return vendorId;
        }

        public void setVendorId(String vendorId) {
            this.vendorId = vendorId;
        }

        public long getRequestTimestamp() {
            return requestTimestamp;
        }

        public void setRequestTimestamp(long requestTimestamp) {
            this.requestTimestamp = requestTimestamp;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getCreativeId() {
            return creativeId;
        }

        public void setCreativeId(String creativeId) {
            this.creativeId = creativeId;
        }

        public String getCampaignId() {
            return campaignId;
        }

        public void setCampaignId(String campaignId) {
            this.campaignId = campaignId;
        }

        public boolean isBid() {
            return bid;
        }

        public void setBid(boolean bid) {
            this.bid = bid;
        }

        @Override
        public String toString() {
            return String.format(
                    "Bid{req=%s, vendor=%s, url=%s, campaign=%s, creative=%s, bid=%s, ts=%d}",
                    requestId, vendorId, url, campaignId, creativeId, bid, requestTimestamp);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BidRequest that = (BidRequest) o;
            return requestTimestamp == that.requestTimestamp
                    && bid == that.bid
                    && Objects.equals(requestId, that.requestId)
                    && Objects.equals(vendorId, that.vendorId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(requestId, vendorId, requestTimestamp, bid);
        }
    }

    // ================================================================
    // WinEvent POJO
    // ================================================================
    public static class WinEvent {

        private String requestId;
        private String vendorId;
        private long winTimestamp;
        private double winPrice;

        public WinEvent() {}

        public WinEvent(String requestId, String vendorId, long winTimestamp, double winPrice) {
            this.requestId = requestId;
            this.vendorId = vendorId;
            this.winTimestamp = winTimestamp;
            this.winPrice = winPrice;
        }

        public String getRequestId() {
            return requestId;
        }

        public void setRequestId(String requestId) {
            this.requestId = requestId;
        }

        public String getVendorId() {
            return vendorId;
        }

        public void setVendorId(String vendorId) {
            this.vendorId = vendorId;
        }

        public long getWinTimestamp() {
            return winTimestamp;
        }

        public void setWinTimestamp(long winTimestamp) {
            this.winTimestamp = winTimestamp;
        }

        public double getWinPrice() {
            return winPrice;
        }

        public void setWinPrice(double winPrice) {
            this.winPrice = winPrice;
        }

        @Override
        public String toString() {
            return String.format(
                    "Win{req=%s, vendor=%s, price=%.2f, ts=%d}",
                    requestId, vendorId, winPrice, winTimestamp);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WinEvent that = (WinEvent) o;
            return winTimestamp == that.winTimestamp
                    && Double.compare(winPrice, that.winPrice) == 0
                    && Objects.equals(requestId, that.requestId)
                    && Objects.equals(vendorId, that.vendorId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(requestId, vendorId, winTimestamp, winPrice);
        }
    }

    // ================================================================
    // JoinedResult POJO
    // ================================================================
    public static class JoinedResult {

        private String requestId;
        private String vendorId;
        private String url;
        private String campaignId;
        private String creativeId;
        private long requestTimestamp;
        private long winTimestamp;
        private double winPrice;
        private boolean matched; // true = bid won
        private long windowStart;
        private long windowEnd;

        public JoinedResult() {}

        public String getRequestId() {
            return requestId;
        }

        public void setRequestId(String requestId) {
            this.requestId = requestId;
        }

        public String getVendorId() {
            return vendorId;
        }

        public void setVendorId(String vendorId) {
            this.vendorId = vendorId;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getCampaignId() {
            return campaignId;
        }

        public void setCampaignId(String campaignId) {
            this.campaignId = campaignId;
        }

        public String getCreativeId() {
            return creativeId;
        }

        public void setCreativeId(String creativeId) {
            this.creativeId = creativeId;
        }

        public long getRequestTimestamp() {
            return requestTimestamp;
        }

        public void setRequestTimestamp(long requestTimestamp) {
            this.requestTimestamp = requestTimestamp;
        }

        public long getWinTimestamp() {
            return winTimestamp;
        }

        public void setWinTimestamp(long winTimestamp) {
            this.winTimestamp = winTimestamp;
        }

        public double getWinPrice() {
            return winPrice;
        }

        public void setWinPrice(double winPrice) {
            this.winPrice = winPrice;
        }

        public boolean isMatched() {
            return matched;
        }

        public void setMatched(boolean matched) {
            this.matched = matched;
        }

        public long getWindowStart() {
            return windowStart;
        }

        public void setWindowStart(long windowStart) {
            this.windowStart = windowStart;
        }

        public long getWindowEnd() {
            return windowEnd;
        }

        public void setWindowEnd(long windowEnd) {
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            if (matched) {
                return String.format(
                        "Joined{req=%s, vendor=%s, url=%s, campaign=%s, "
                                + "reqTs=%d, winTs=%d, price=%.2f, window=[%d,%d)}",
                        requestId,
                        vendorId,
                        url,
                        campaignId,
                        requestTimestamp,
                        winTimestamp,
                        winPrice,
                        windowStart,
                        windowEnd);
            } else {
                return String.format(
                        "Unmatched{req=%s, vendor=%s, url=%s, campaign=%s, "
                                + "reqTs=%d, NO WIN, window=[%d,%d)}",
                        requestId,
                        vendorId,
                        url,
                        campaignId,
                        requestTimestamp,
                        windowStart,
                        windowEnd);
            }
        }
    }

    // ================================================================
    // Bid Request Source
    //
    // Generates bid requests in two 5-minute windows:
    //   Window 1: [0, 300000)   — 10 requests, 7 with bid=true
    //   Window 2: [300000, 600000) — for watermark advancement
    //
    // Some requests will have matching wins, others won't.
    // bid=false requests are filtered out before the join.
    // ================================================================
    public static class BidRequestSource implements SourceFunction<BidRequest> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<BidRequest> ctx) throws Exception {
            String[] urls = {"news.com", "sports.com", "tech.com", "shop.com"};
            String[] campaigns = {"camp-A", "camp-B", "camp-C"};
            String[] creatives = {"banner-1", "video-2", "native-3"};

            // Window 1: [0, 300_000) — 5 minutes
            // Request IDs: req-001 through req-010
            for (int i = 1; i <= 10; i++) {
                String reqId = String.format("req-%03d", i);
                boolean bid = i <= 7; // first 7 bid, last 3 don't
                long ts = (i - 1) * 25_000L + 10_000L; // 10s, 35s, 60s, ...

                ctx.collect(
                        new BidRequest(
                                reqId,
                                "vendor-1",
                                ts,
                                urls[i % urls.length],
                                creatives[i % creatives.length],
                                campaigns[i % campaigns.length],
                                bid));
            }

            // Push watermark into window 2: ts=350_000 → watermark ~320_000
            ctx.collect(
                    new BidRequest(
                            "req-020",
                            "vendor-1",
                            350_000L,
                            "news.com",
                            "banner-1",
                            "camp-A",
                            true));

            System.out.println("[REQUESTS] Emitted 11 bid requests (7 with bid=true in window 1)");

            // Sleep to let window 1 fire before late events
            Thread.sleep(5000);
            if (!isRunning) {
                return;
            }

            // Late request: within 1 min allowed lateness (watermark ~320_000, window end 300_000)
            ctx.collect(
                    new BidRequest(
                            "req-011",
                            "vendor-1",
                            280_000L, // in window 1 but arrives late
                            "late-site.com",
                            "banner-1",
                            "camp-B",
                            true));

            System.out.println("[REQUESTS] Emitted 1 late request (within allowed lateness)");

            Thread.sleep(2000);

            // Very late request: beyond allowed lateness → should go to side output
            // Push watermark further first
            ctx.collect(
                    new BidRequest(
                            "req-030",
                            "vendor-1",
                            700_000L,
                            "future.com",
                            "video-2",
                            "camp-C",
                            true));

            Thread.sleep(2000);

            // This is now too late: window 1 ended at 300_000, lateness=60_000,
            // watermark now ~670_000 >> 300_000 + 60_000
            ctx.collect(
                    new BidRequest(
                            "req-012",
                            "vendor-1",
                            200_000L, // way too late
                            "very-late.com",
                            "native-3",
                            "camp-A",
                            true));

            System.out.println("[REQUESTS] Emitted 1 very late request (should be side-output)");
            Thread.sleep(2000);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // ================================================================
    // Win Source
    //
    // Generates win notifications for a SUBSET of bid requests.
    // Win notifications arrive AFTER the request (realistically,
    // auction results come 100ms-2s after the bid).
    //
    // Only ~40% of bids win (typical in ad-tech).
    //
    // Uses withIdleness(10 min) on the watermark strategy to prevent
    // the sparse wins stream from holding back the global watermark.
    // ================================================================
    public static class WinSource implements SourceFunction<WinEvent> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<WinEvent> ctx) throws Exception {
            // Small delay to let some requests arrive first
            Thread.sleep(1000);

            // Wins for requests: req-001, req-003, req-005 (3 out of 7 bids)
            // Win arrives slightly after request (realistic: auction delay)
            ctx.collect(new WinEvent("req-001", "vendor-1", 10_500L, 2.50));
            ctx.collect(new WinEvent("req-003", "vendor-1", 60_800L, 1.75));
            ctx.collect(new WinEvent("req-005", "vendor-1", 110_300L, 3.20));

            System.out.println("[WINS] Emitted 3 win events (for req-001, req-003, req-005)");

            // Sleep — wins stream goes quiet.
            // Without withIdleness(10min), this silence would hold back the watermark.
            Thread.sleep(8000);
            if (!isRunning) {
                return;
            }

            // Late win: for req-011 which was a late request
            ctx.collect(new WinEvent("req-011", "vendor-1", 281_000L, 1.90));

            System.out.println("[WINS] Emitted 1 late win (for req-011)");
            Thread.sleep(3000);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
