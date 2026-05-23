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

package org.apache.flink.streaming.test.examples.aggregation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.examples.CountStarAggsHandleFunctionV5;
import org.apache.flink.streaming.examples.EventToRowDataMapV5;
import org.apache.flink.streaming.examples.GlobalWindowAggregateFunctionV5;
import org.apache.flink.streaming.examples.KeyExtractorV5;
import org.apache.flink.streaming.examples.LocalWindowAggregateOperatorV5;
import org.apache.flink.streaming.examples.TwoPhaseCountDeduplicatedEventTimeV5;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.CloseableIterator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the V5 local-global pipeline, focused on the late-data path.
 *
 * <p>The tests use a deterministic source that emits events <i>and</i> explicit watermarks
 * (no sleeps, no real time). With {@code parallelism=1} the source emissions are processed
 * by downstream operators in strict order, so the output sequence is fully deterministic.
 *
 * <p>Each test exercises one branch of the late-data state machine:
 *
 * <ul>
 *   <li>{@link #testLateEventTriggersRefire} — late event within allow-lateness produces a
 *       {@code -U} / {@code +U} retraction pair.
 *   <li>{@link #testEventPastAllowedLatenessIsDropped} — event past {@code windowEnd +
 *       allow-lateness} is silently dropped; no extra output rows.
 *   <li>{@link #testHandlerArithmetic} — sanity check for {@link
 *       CountStarAggsHandleFunctionV5} (the codegen-equivalent class).
 * </ul>
 */
public class TwoPhaseCountDeduplicatedEventTimeV5Test {

    private static final long WIN = TwoPhaseCountDeduplicatedEventTimeV5.WINDOW_SIZE_MS; // 60_000

    /**
     * Late event for an already-fired window should trigger a re-fire (UPDATE_BEFORE +
     * UPDATE_AFTER) carrying the ORIGINAL windowStart.
     *
     * <pre>
     *   1) emit 3 events in window [0, 60000) for key A           → buffered locally
     *   2) emit watermark 70000  (past windowEnd=60000)            → local flushes, global fires +I
     *   3) emit late event (ts=1500, also in [0, 60000))           → buffered locally for windowStart=0
     *   4) emit watermark 80000  (within allow-lateness = 90000)   → local flushes, global re-fires
     *
     *   Expected output:
     *     +I[A, windowStart=0, count=3]      ← initial fire
     *     -U[A, windowStart=0, count=3]      ┐ late event triggers
     *     +U[A, windowStart=0, count=4]      ┘ retraction + new value
     * </pre>
     */
    @Test
    public void testLateEventTriggersRefire() throws Exception {
        List<String> collected =
                runPipeline(
                        new EmitSourceBuilder()
                                .event("A", 1000L)
                                .event("A", 2000L)
                                .event("A", 3000L)
                                .watermark(WIN + 10_000L) // 70000 — past windowEnd, triggers fire
                                .event("A", 1500L) // LATE — still in window [0, 60000)
                                .watermark(WIN + 20_000L) // 80000 — within allow-lateness=90000
                                .watermark(Long.MAX_VALUE)
                                .build());

        assertEquals(
                Arrays.asList(
                        "+I[A, windowStart=0, count=3]",
                        "-U[A, windowStart=0, count=3]",
                        "+U[A, windowStart=0, count=4]"),
                collected);
    }

    /**
     * An event past {@code windowEnd + allow-lateness} should be silently dropped — no
     * additional output rows after the initial fire.
     *
     * <pre>
     *   1) emit 3 events in window [0, 60000) for key A
     *   2) emit watermark 70000  → fire +I
     *   3) emit watermark 95000  (past windowEnd+lateness = 90000) → cleanup timer fires
     *   4) emit a "too late" event (ts=500, still nominally in [0, 60000))
     *      → global stage drops it; no output
     *
     *   Expected output: ONLY the initial fire.
     * </pre>
     */
    @Test
    public void testEventPastAllowedLatenessIsDropped() throws Exception {
        List<String> collected =
                runPipeline(
                        new EmitSourceBuilder()
                                .event("A", 1000L)
                                .event("A", 2000L)
                                .event("A", 3000L)
                                .watermark(WIN + 10_000L) // 70000 — fire
                                .watermark(WIN + 35_000L) // 95000 — past cleanup time 90000
                                .event("A", 500L) // event for now-cleaned-up window → dropped
                                .watermark(Long.MAX_VALUE)
                                .build());

        assertEquals(
                Arrays.asList("+I[A, windowStart=0, count=3]"),
                collected);
    }

    /**
     * Pure unit test for the hand-written aggregate handler — verifies the codegen-equivalent
     * arithmetic without going through any operators.
     */
    @Test
    public void testHandlerArithmetic() throws Exception {
        CountStarAggsHandleFunctionV5 handler = new CountStarAggsHandleFunctionV5();
        handler.open(null);

        // Accumulate 3 records (input contents are ignored by COUNT(*)).
        GenericRowData acc = (GenericRowData) handler.createAccumulators();
        handler.setAccumulators(acc);
        handler.accumulate(makeRow("x"));
        handler.accumulate(makeRow("y"));
        handler.accumulate(makeRow("z"));
        assertEquals(3L, handler.getValue().getLong(0));

        // Retract one — count decrements.
        handler.retract(makeRow("x"));
        assertEquals(2L, handler.getValue().getLong(0));

        // Merge another accumulator carrying count=5.
        GenericRowData other = new GenericRowData(1);
        other.setField(0, 5L);
        handler.merge(other);
        assertEquals(7L, handler.getValue().getLong(0));

        // Reset returns to zero.
        handler.resetAccumulators();
        assertEquals(0L, handler.getValue().getLong(0));

        handler.close();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static RowData makeRow(String key) {
        GenericRowData row = new GenericRowData(1);
        row.setField(0, StringData.fromString(key));
        return row;
    }

    /**
     * Build the V5 pipeline against a finite test source and collect its formatted output.
     */
    private static List<String> runPipeline(List<Object> sourceEmissions) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        DataStream<Tuple3<String, String, Long>> input =
                env.addSource(new ScriptedSource(sourceEmissions));

        DataStream<RowData> rowStream =
                input.map(new EventToRowDataMapV5()).returns(TypeInformation.of(RowData.class));

        DataStream<RowData> localPartials =
                rowStream.transform(
                        "local-window-aggregate",
                        TypeInformation.of(RowData.class),
                        new LocalWindowAggregateOperatorV5(new KeyExtractorV5(), WIN));

        DataStream<RowData> finalResults =
                localPartials
                        .keyBy(new KeyExtractorV5())
                        .process(new GlobalWindowAggregateFunctionV5())
                        .returns(TypeInformation.of(RowData.class));

        DataStream<String> formatted =
                finalResults.map(
                        (MapFunction<RowData, String>)
                                TwoPhaseCountDeduplicatedEventTimeV5::formatRow);

        List<String> collected = new ArrayList<>();
        try (CloseableIterator<String> it = formatted.executeAndCollect()) {
            while (it.hasNext()) {
                collected.add(it.next());
            }
        }
        return collected;
    }

    /**
     * Builder for a deterministic source program — interleaves {@code event} and {@code
     * watermark} commands which the {@link ScriptedSource} replays in order.
     */
    private static final class EmitSourceBuilder {
        private final List<Object> commands = new ArrayList<>();

        EmitSourceBuilder event(String key, long ts) {
            commands.add(new EventCmd(key, ts));
            return this;
        }

        EmitSourceBuilder watermark(long ts) {
            commands.add(new WatermarkCmd(ts));
            return this;
        }

        List<Object> build() {
            return commands;
        }
    }

    /** Marker for "emit a watermark with this timestamp". */
    private static final class WatermarkCmd implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        final long ts;

        WatermarkCmd(long ts) {
            this.ts = ts;
        }
    }

    /** Marker for "emit an event with this key and timestamp". */
    private static final class EventCmd implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        final String key;
        final long ts;

        EventCmd(String key, long ts) {
            this.key = key;
            this.ts = ts;
        }
    }

    /**
     * Source that replays a pre-recorded sequence of events and watermarks. With {@code
     * parallelism=1}, downstream operators receive the commands in the recorded order, so the
     * output is fully deterministic.
     */
    private static final class ScriptedSource
            implements SourceFunction<Tuple3<String, String, Long>> {
        private static final long serialVersionUID = 1L;
        private final List<Object> commands;
        private volatile boolean running = true;

        ScriptedSource(List<Object> commands) {
            this.commands = commands;
        }

        @Override
        public void run(SourceContext<Tuple3<String, String, Long>> ctx) {
            for (Object cmd : commands) {
                if (!running) {
                    return;
                }
                if (cmd instanceof EventCmd) {
                    EventCmd e = (EventCmd) cmd;
                    ctx.collectWithTimestamp(Tuple3.of(e.key, "x", e.ts), e.ts);
                } else if (cmd instanceof WatermarkCmd) {
                    ctx.emitWatermark(new Watermark(((WatermarkCmd) cmd).ts));
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
