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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * V5: KEYED global aggregator. Mirrors the Table API's {@code WindowAggOperator}.
 *
 * <p>Receives per-({@code key}, {@code windowStart}) partials from all local subtasks, MERGES
 * them into a per-({@code key}, {@code windowStart}) global accumulator stored in keyed {@link
 * MapState}, and on {@code windowEnd} fires the final result as a changelog row.
 *
 * <h2>Output is changelog (RowKind-tagged)</h2>
 *
 * <ul>
 *   <li>First fire → {@code +I} (INSERT)
 *   <li>Re-fire on late event within allow-lateness → {@code -U} (UPDATE_BEFORE) + {@code +U}
 *       (UPDATE_AFTER)
 * </ul>
 *
 * <p>This is the only stage that touches changelog semantics; the local stage is pure
 * insert-only.
 *
 * <h2>All late-data semantics live in this stage</h2>
 *
 * <ul>
 *   <li>A partial that arrives for a window whose end has been crossed but {@code
 *       windowEnd + allow-lateness} hasn't elapsed → re-fire ({@code -U}/{@code +U}).
 *   <li>A partial that arrives past {@code windowEnd + allow-lateness} → silently dropped.
 *       (Real {@code WindowAggOperator} increments a {@code numLateRecordsDropped} metric
 *       here.)
 * </ul>
 *
 * <h2>Key correctness property: windowStart is preserved end-to-end</h2>
 *
 * <p>The local stage tags each emitted partial with the {@code windowStart} it computed from
 * the event's timestamp. This stage reads it back via {@code partial.getLong(1)} and uses it
 * to index into the per-window state ({@link #accState}, {@link #firedState}, {@link
 * #lastEmittedState} are all keyed by {@code windowStart}).
 *
 * <p>A late event for window {@code [0, 60000)} therefore <b>always</b> updates {@code
 * accState[0]}, never {@code accState[60000]}. The output row carries {@code windowStart=0},
 * not the current window's start.
 *
 * <h2>Worked example: late event triggers re-fire</h2>
 *
 * <p>Suppose window size is 60s and allow-lateness is 30s. Two events arrive for key {@code A}
 * in window {@code [0, 60000)}, then watermark advances past windowEnd, then a third late
 * event arrives for the same window.
 *
 * <pre>
 *   t=1  event(A, ts=10s)  ─ local buffer[A][0].count = 1
 *   t=2  event(A, ts=20s)  ─ local buffer[A][0].count = 2
 *   t=3  watermark→65s
 *          local: emit partial(A, 0, 2); drop buffer[A][0]
 *          global: accState[0] = 2, register timer @ 60s
 *          global: watermark crosses 60s → fire timer
 *                  emit +I[A, ws=0, count=2]
 *                  firedState[0] = true
 *                  register cleanup timer @ 90s
 *   t=4  event(A, ts=15s)  ─ LATE (event time in window [0, 60))
 *          local: windowStart = 15 - 15%60000 = 0 (original window, NOT 60000!)
 *                  buffer[A][0] is gone → create new, count = 1
 *   t=5  watermark→75s
 *          local: emit partial(A, 0, 1); drop buffer[A][0]
 *          global: 75s &lt; 90s → don't drop
 *                  accState[0] = 2 + 1 = 3
 *                  firedState[0] is true → re-fire
 *                  emit -U[A, ws=0, count=2]
 *                  emit +U[A, ws=0, count=3]
 *   t=6  watermark→95s
 *          global: cleanup timer at 90s fires → accState.remove(0), firedState.remove(0)
 *   t=7  event(A, ts=5s)   ─ TOO LATE
 *          local: windowStart = 0 → buffer[A][0].count = 1
 *   t=8  watermark→100s
 *          local: emit partial(A, 0, 1)
 *          global: 100s &gt;= 90s → DROP (past allow-lateness)
 * </pre>
 *
 * <p>Final output stream for key {@code A}:
 *
 * <pre>
 *   +I[A, windowStart=0, count=2]      ← initial fire
 *   -U[A, windowStart=0, count=2]      ┐ late event triggers
 *   +U[A, windowStart=0, count=3]      ┘ retraction + new value
 * </pre>
 */
public final class GlobalWindowAggregateFunctionV5
        extends KeyedProcessFunction<String, RowData, RowData> {
    private static final long serialVersionUID = 1L;

    /** Window duration. Must match {@link LocalWindowAggregateOperatorV5}. */
    public static final long WINDOW_SIZE_MS = Duration.ofMinutes(1).toMillis();

    /** Allowed-lateness, matching V4's {@code emit.allow-lateness = '30 s'}. */
    public static final long ALLOWED_LATENESS_MS = Duration.ofSeconds(30).toMillis();

    /** windowStart -> global accumulator (merged across all local partials). */
    private transient MapState<Long, GenericRowData> accState;

    /** windowStart -> TRUE if the windowEnd timer has already fired. */
    private transient MapState<Long, Boolean> firedState;

    /** windowStart -> the last emitted aggregate value (for emitting -U on re-fire). */
    private transient MapState<Long, Long> lastEmittedState;

    private transient AggsHandleFunction handler;

    @Override
    public void open(OpenContext openContext) throws Exception {
        accState =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        "global-acc",
                                        TypeInformation.of(Long.class),
                                        TypeInformation.of(GenericRowData.class)));
        firedState =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        "global-fired",
                                        TypeInformation.of(Long.class),
                                        TypeInformation.of(Boolean.class)));
        lastEmittedState =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        "global-last-emitted",
                                        TypeInformation.of(Long.class),
                                        TypeInformation.of(Long.class)));
        handler = new CountStarAggsHandleFunctionV5();
        handler.open(null);
    }

    @Override
    public void processElement(RowData partial, Context ctx, Collector<RowData> out)
            throws Exception {
        // Partial layout: [key:STRING, windowStart:BIGINT, partialCount:BIGINT]
        // The windowStart is the ORIGINAL window the event belonged to — set by the local
        // stage based on the event's timestamp. We use it verbatim to index into accState,
        // so a late event always updates its original window's accumulator.
        long windowStart = partial.getLong(1);
        long partialCount = partial.getLong(2);
        long windowEnd = windowStart + WINDOW_SIZE_MS;
        long cleanupTime = windowEnd + ALLOWED_LATENESS_MS;
        long currentWatermark = ctx.timerService().currentWatermark();

        // ── DROP gate (past allow-lateness) ──────────────────────────────────
        // If the watermark has already advanced past windowEnd + allowedLateness, the window
        // is dead — its cleanup timer has fired (or is about to) and the state for this
        // windowStart has been removed. Drop silently. Real WindowAggOperator increments
        // numLateRecordsDropped here.
        if (currentWatermark >= cleanupTime) {
            return;
        }

        // ── MERGE into the correct window's accumulator ──────────────────────
        // Build a one-field accumulator from the incoming partialCount (same shape
        // CountStarAggsHandleFunctionV5 uses internally), then merge() into the stored
        // global accumulator at accState[windowStart].
        GenericRowData incoming = new GenericRowData(1);
        incoming.setField(0, partialCount);

        GenericRowData globalAcc = accState.get(windowStart);
        if (globalAcc == null) {
            globalAcc = (GenericRowData) handler.createAccumulators();
        }
        handler.setAccumulators(globalAcc);
        handler.merge(incoming);
        accState.put(windowStart, (GenericRowData) handler.getAccumulators());

        // ── LATE-FIRE: emit retraction + new value ───────────────────────────
        // If the window has already fired (its initial windowEnd timer fired earlier), any
        // subsequent merge is by definition a "late update" and must be surfaced as a -U/+U
        // pair so downstream can apply the correction.
        if (Boolean.TRUE.equals(firedState.get(windowStart))) {
            emitChangelog(out, ctx.getCurrentKey(), windowStart, /* isReFire */ true);
            return;
        }

        // Not fired yet — just register the windowEnd timer. Re-registering the same
        // event-time timestamp is a no-op, so this is idempotent across multiple input
        // events arriving before the window fires.
        ctx.timerService().registerEventTimeTimer(windowEnd);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out)
            throws Exception {
        // Timers fire at TWO timestamps per window:
        //   • windowEnd                       → "fire" timer, emit +I
        //   • windowEnd + ALLOWED_LATENESS_MS → "cleanup" timer, clear state
        //
        // We disambiguate by trying each interpretation against state.

        // ── Try: this is a FIRE timer ────────────────────────────────────────
        long fireCandidate = timestamp - WINDOW_SIZE_MS;
        if (fireCandidate % WINDOW_SIZE_MS == 0
                && accState.contains(fireCandidate)
                && !Boolean.TRUE.equals(firedState.get(fireCandidate))) {
            emitChangelog(out, ctx.getCurrentKey(), fireCandidate, /* isReFire */ false);
            firedState.put(fireCandidate, Boolean.TRUE);
            ctx.timerService()
                    .registerEventTimeTimer(fireCandidate + WINDOW_SIZE_MS + ALLOWED_LATENESS_MS);
            return;
        }

        // ── Try: this is a CLEANUP timer ─────────────────────────────────────
        long cleanupCandidate = timestamp - WINDOW_SIZE_MS - ALLOWED_LATENESS_MS;
        if (cleanupCandidate % WINDOW_SIZE_MS == 0
                && Boolean.TRUE.equals(firedState.get(cleanupCandidate))) {
            accState.remove(cleanupCandidate);
            firedState.remove(cleanupCandidate);
            lastEmittedState.remove(cleanupCandidate);
        }
    }

    /**
     * Emit one row in changelog form.
     *
     * <ul>
     *   <li>{@code isReFire = false} → emit +I with the current accumulator value.
     *   <li>{@code isReFire = true} → emit -U(previous_value), then +U(current).
     * </ul>
     */
    private void emitChangelog(
            Collector<RowData> out, String currentKey, long windowStart, boolean isReFire)
            throws Exception {
        GenericRowData acc = accState.get(windowStart);
        handler.setAccumulators(acc);
        long currentValue = handler.getValue().getLong(0);

        if (isReFire) {
            Long previous = lastEmittedState.get(windowStart);
            if (previous != null && previous == currentValue) {
                // No-op update — real Table API skips this via a generated equaliser.
                return;
            }
            if (previous != null) {
                out.collect(buildRow(currentKey, windowStart, previous, RowKind.UPDATE_BEFORE));
            }
            out.collect(buildRow(currentKey, windowStart, currentValue, RowKind.UPDATE_AFTER));
        } else {
            out.collect(buildRow(currentKey, windowStart, currentValue, RowKind.INSERT));
        }
        lastEmittedState.put(windowStart, currentValue);
    }

    private static RowData buildRow(String key, long windowStart, long count, RowKind rowKind) {
        GenericRowData row = new GenericRowData(3);
        row.setField(0, StringData.fromString(key));
        row.setField(1, windowStart);
        row.setField(2, count);
        row.setRowKind(rowKind);
        return row;
    }
}
