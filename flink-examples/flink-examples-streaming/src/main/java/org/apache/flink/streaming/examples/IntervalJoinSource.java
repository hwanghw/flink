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

import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

/**
 * Shared sources for interval join examples. Generates bid requests and wins that exercise
 * ALL code paths:
 *
 * <pre>
 *   ════════════════════════════════════════════════════════════════════════════════
 *   Test case                    | Request ts | Win ts  | Gap    | Expected output
 *   ════════════════════════════════════════════════════════════════════════════════
 *   1. Normal join (same window) | 30s        | 31s     | 1s     | JOINED
 *   2. Normal join (same window) | 60s        | 62s     | 2s     | JOINED
 *   3. Cross-window join         | 200s       | 310s    | 110s   | JOINED (tumbling would miss!)
 *   4. Cross-window join         | 250s       | 320s    | 70s    | JOINED (tumbling would miss!)
 *   5. Unmatched request         | 100s       | (none)  | —      | UNMATCHED-REQ
 *   6. Unjoined win              | (none)     | 350s    | —      | UNMATCHED-WIN
 *   7. Filtered out (bid=false)  | 150s       | —       | —      | (filtered before join)
 *   ─────────────────────────────┼────────────┼─────────┼────────┼──────────────────
 *   === Watermark pushed to ~570s by req-wm @ 600s ===
 *   === Sleep 5s to let watermark propagate ===
 *   ─────────────────────────────┼────────────┼─────────┼────────┼──────────────────
 *   8. Late request within       | 280s       | —       | —      | JOINED or allowed
 *      allowed lateness (1min)   |            |         |        | (watermark ~570 &lt; 280+300+60)
 *   ─────────────────────────────┼────────────┼─────────┼────────┼──────────────────
 *   === Watermark pushed to ~1170s by req-wm2 @ 1200s ===
 *   === Sleep 3s ===
 *   ─────────────────────────────┼────────────┼─────────┼────────┼──────────────────
 *   9. Very late request         | 50s        | —       | —      | LATE-REQ side output
 *      (watermark ~1170 &gt; 50+300+60)                            | (beyond interval+lateness)
 *   10. Very late win            | —          | 80s     | —      | LATE-WIN side output
 *       (watermark ~1170 &gt; 80+300+60)                           | (beyond interval+lateness)
 *   ─────────────────────────────┼────────────┼─────────┼────────┼──────────────────
 *   === Wins stream goes quiet ===
 *   === After 5s (demo) / 10min (prod) idleness, Flink stops waiting for wins watermark ===
 *   === Requests stream pushes watermark alone ===
 * </pre>
 */
public class IntervalJoinSource {

    /** Bid requests source — generates test data for all code paths. */
    public static class BidRequestSource implements SourceFunction<BidWinJoin.BidRequest> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<BidWinJoin.BidRequest> ctx) throws Exception {
            // ── Normal requests (will be joined) ──

            // req-001: win arrives quickly (same 5-min window)
            ctx.collect(new BidWinJoin.BidRequest(
                    "req-001", "vendor-1", 30_000L,
                    "news.com", "banner-1", "camp-A", true));

            // req-002: win arrives 2s later (same 5-min window)
            ctx.collect(new BidWinJoin.BidRequest(
                    "req-002", "vendor-1", 60_000L,
                    "sports.com", "video-2", "camp-B", true));

            // req-003: request at 200s, win at 310s — CROSSES 5-min window boundary
            ctx.collect(new BidWinJoin.BidRequest(
                    "req-003", "vendor-1", 200_000L,
                    "tech.com", "native-3", "camp-C", true));

            // req-004: request at 250s, win at 320s — CROSSES 5-min window boundary
            ctx.collect(new BidWinJoin.BidRequest(
                    "req-004", "vendor-1", 250_000L,
                    "shop.com", "banner-1", "camp-A", true));

            // ── Unmatched request (no win will arrive) ──

            // req-005: no win — will be side-output as UNMATCHED-REQ
            ctx.collect(new BidWinJoin.BidRequest(
                    "req-005", "vendor-1", 100_000L,
                    "blog.com", "video-2", "camp-B", true));

            // ── Filtered out (bid=false) ──

            ctx.collect(new BidWinJoin.BidRequest(
                    "req-006", "vendor-1", 150_000L,
                    "ads.com", "native-3", "camp-C", false));

            System.out.println("[REQUESTS] Batch 1: emitted 6 requests (5 bid=true, 1 filtered)");

            // ── Push watermark forward to ~570s ──
            // (600s - 30s out-of-orderness = 570s)
            ctx.collect(new BidWinJoin.BidRequest(
                    "req-wm", "vendor-1", 600_000L,
                    "wm.com", "banner-1", "camp-A", true));

            System.out.println("[REQUESTS] Watermark push: req-wm @ 600s → watermark ~570s");

            // Sleep to let watermark propagate and timers fire
            Thread.sleep(5000);
            if (!isRunning) {
                return;
            }

            // ── Late request WITHIN allowed lateness ──
            // watermark ~570s, this request ts=280s
            // late check: watermark (570) > ts (280) + interval (300) + lateness (60) = 640? NO
            // So this is still allowed (within lateness)
            ctx.collect(new BidWinJoin.BidRequest(
                    "req-late-ok", "vendor-1", 280_000L,
                    "late-ok.com", "banner-1", "camp-B", true));

            System.out.println("[REQUESTS] Batch 2: emitted 1 late request (within allowed lateness)");

            // Wait for wins stream to become IDLE.
            // withIdleness is set to 5s for demo purposes.
            // Wins stream's last event was ~8s ago (wall-clock), so idle timeout
            // should have fired by now. After idle, global watermark = requests_wm only.
            Thread.sleep(6000);
            if (!isRunning) {
                return;
            }

            // ── Push watermark much further to ~1170s ──
            // (1200s - 30s = 1170s)
            // NOW wins is idle → global watermark follows requests_wm = 1170s
            // (without idleness, it would be stuck at min(1170, 320) = 320s)
            ctx.collect(new BidWinJoin.BidRequest(
                    "req-wm2", "vendor-1", 1_200_000L,
                    "wm2.com", "banner-1", "camp-A", true));

            System.out.println(
                    "[REQUESTS] Watermark push: req-wm2 @ 1200s → watermark ~1170s "
                            + "(wins idle, so global WM follows requests only)");

            Thread.sleep(3000);
            if (!isRunning) {
                return;
            }

            // ── Very late request — BEYOND allowed lateness → LATE-REQ side output ──
            // watermark ~1170s, ts=50s
            // late check: watermark (1170) > ts (50) + interval (300) + lateness (60) = 410? YES!
            ctx.collect(new BidWinJoin.BidRequest(
                    "req-very-late", "vendor-1", 50_000L,
                    "very-late.com", "native-3", "camp-A", true));

            System.out.println(
                    "[REQUESTS] Batch 3: emitted 1 very late request → should be LATE-REQ side output");

            Thread.sleep(2000);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * Win events source — some cross window boundaries, some are late.
     *
     * <p>The wins stream goes quiet after the initial batch. Without
     * {@code withIdleness(5s (demo) / 10min (prod))}, its watermark would hold back the global watermark
     * and prevent the requests stream from advancing time alone.
     */
    public static class WinSource implements SourceFunction<BidWinJoin.WinEvent> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<BidWinJoin.WinEvent> ctx) throws Exception {
            Thread.sleep(1000); // let requests arrive first

            // ── Normal wins ──

            // win for req-001: arrives quickly, same 5-min window
            ctx.collect(new BidWinJoin.WinEvent("req-001", "vendor-1", 31_000L, 2.50));

            // win for req-002: arrives 2s later, same 5-min window
            ctx.collect(new BidWinJoin.WinEvent("req-002", "vendor-1", 62_000L, 1.75));

            // win for req-003: arrives at 310s — DIFFERENT 5-min window (gap = 110s < 5min)
            ctx.collect(new BidWinJoin.WinEvent("req-003", "vendor-1", 310_000L, 3.20));

            // win for req-004: arrives at 320s — DIFFERENT 5-min window (gap = 70s < 5min)
            ctx.collect(new BidWinJoin.WinEvent("req-004", "vendor-1", 320_000L, 4.10));

            // ── Unjoined win (no matching request) → UNMATCHED-WIN ──
            ctx.collect(new BidWinJoin.WinEvent("req-999", "vendor-1", 350_000L, 0.50));

            // (no win for req-005 → it will be UNMATCHED-REQ)

            System.out.println(
                    "[WINS] Batch 1: emitted 5 wins (2 same-window, 2 cross-window, 1 unjoined)");

            // ══════════════════════════════════════════════════════════
            // Wins stream goes QUIET here. This demonstrates withIdleness:
            //
            // WHAT HAPPENS INTERNALLY (wall-clock timeline):
            //
            //  t=0s:    Last win event emitted (ts=350s). IdlenessTimer
            //           counter incremented. isIdleNow=false.
            //
            //  t=0-5s:  onPeriodicEmit() called every 200ms (default).
            //  (demo)   IdlenessTimer.checkIfIdle():
            //             counter == lastCounter (no new events)
            //             startOfInactivityNanos set on first check
            //             clock.relativeTimeNanos() - start < idleTimeout
            //             → returns false, normal watermark emitted
            //
            //  t=5s:    IdlenessTimer.checkIfIdle():
            //  (demo)     clock.relativeTimeNanos() - start > idleTimeout
            //             → returns true!
            //           WatermarksWithIdleness calls output.markIdle()
            //           → WatermarkStatus.IDLE sent downstream
            //
            //  Downstream StatusWatermarkValve:
            //           Removes wins subpartition from min-watermark calc
            //           Global watermark = requests_wm only = 570s
            //           (was stuck at min(570, 320) = 320s before!)
            //           → timers fire, late detection works correctly
            //
            // Without withIdleness(5s (demo) / 10min (prod)):
            //   Wins watermark stays at ~320s forever
            //   Global watermark = min(requests_wm, wins_wm) = 320s
            //   → requests stream can't advance time past 320s!
            //   → no timers fire, no late data detected
            //
            // NOTE: PausableRelativeClock subtracts backpressure time,
            // so network backpressure does NOT trigger false idle detection.
            // ══════════════════════════════════════════════════════════
            System.out.println(
                    "[WINS] Stream going quiet — withIdleness(5s (demo) / 10min (prod)) will let requests "
                            + "stream drive the watermark alone");
            System.out.println(
                    "[WINS] Internally: WatermarksWithIdleness.IdlenessTimer starts wall-clock "
                            + "countdown. After 5s (demo) / 10min (prod) of no events → output.markIdle() → "
                            + "StatusWatermarkValve excludes wins from min-watermark calc");

            // Wait long enough for the requests stream to push watermarks
            Thread.sleep(12_000);
            if (!isRunning) {
                return;
            }

            // ── Wins stream REACTIVATES ──
            // When this event arrives, WatermarksWithIdleness.onEvent():
            //   1. idlenessTimer.activity() → increments counter
            //   2. isIdleNow = false
            //   3. Next onPeriodicEmit() → checkIfIdle() returns false (counter changed)
            //   4. output.markActive() → emits WatermarkStatus.ACTIVE
            //   5. StatusWatermarkValve re-adds wins to min-watermark calculation
            //   6. If wins' watermark is behind global watermark, it's marked
            //      "unaligned" until it catches up to lastOutputWatermark

            // ── Very late win — BEYOND allowed lateness → LATE-WIN side output ──
            // By now watermark should be ~1170s (from req-wm2)
            // late check: watermark (1170) > ts (80) + interval (300) + lateness (60) = 440? YES!
            ctx.collect(new BidWinJoin.WinEvent("req-very-late-win", "vendor-1", 80_000L, 1.00));

            System.out.println(
                    "[WINS] Batch 2: wins stream REACTIVATES — WatermarksWithIdleness "
                            + "calls markActive() → StatusWatermarkValve re-adds wins "
                            + "to min-watermark calc");
            System.out.println(
                    "[WINS] Emitted 1 very late win (ts=80s, watermark ~1170s) "
                            + "→ should be LATE-WIN side output");

            Thread.sleep(2000);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
