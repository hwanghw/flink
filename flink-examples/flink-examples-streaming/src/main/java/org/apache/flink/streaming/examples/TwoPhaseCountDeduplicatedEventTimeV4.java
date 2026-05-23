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

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * V4: Table-API equivalent of {@link TwoPhaseCountDeduplicatedEventTimeV2}.
 *
 * <p>Where V2 hand-rolls a two-phase windowed pipeline (LOCAL count per
 * (key, subtask) with a salted keyBy, then GLOBAL window with a Map-based
 * dedup of partial counts), and {@link TwoPhaseCountDeduplicatedEventTimeV3}
 * skips the local-phase shuffle with a non-keyed {@code ProcessFunction},
 * V4 expresses the same task with the Table API Java DSL and lets the
 * planner generate the two-phase pipeline automatically.
 *
 * <h2>How V4 differs from V2 / V3</h2>
 *
 * <pre>
 * V2 (DataStream, two shuffles, dedup-by-subtask):
 *   src → map(addSubtaskIdx) → keyBy(key+subtaskIdx) [SHUFFLE]
 *       → window(1m, allowedLateness=30s) → aggregate(LocalCountAggregate)
 *       → keyBy(key) [SHUFFLE]
 *       → window(1m, allowedLateness=30s) → process(GlobalDeduplicatedCountFunction)
 *   Output: insert-only rows, dedup relies on accumulating-mode + ListState FIFO order
 *
 * V3 (DataStream, no local shuffle, checkpointed operator state):
 *   src → LocalPreAggregator (non-keyed ProcessFunction + ListState)
 *       → keyBy(key) [SHUFFLE]
 *       → window(1m, allowedLateness=30s) → process(GlobalDeduplicatedCountFunction)
 *   Output: insert-only rows, same dedup-by-subtask pattern as V2
 *
 * V4 (Table API, planner-driven):
 *   src → tEnv.fromDataStream(...)                                ← bridge to Table
 *       → window(Tumble 1m).groupBy(key, w).select(...)           ← single declarative spec
 *   Output: CHANGELOG (+I / -U / +U / -D), no manual dedup, late events emit retractions
 * </pre>
 *
 * <h2>What the planner inserts under the hood</h2>
 *
 * <p>With {@code table.optimizer.agg-phase-strategy = TWO_PHASE} the planner
 * splits the windowed aggregate into:
 *
 * <pre>
 *   StreamPhysicalGlobalWindowAggregate
 *     +- StreamPhysicalExchange (hash by url)
 *        +- StreamPhysicalLocalWindowAggregate
 *           +- (source bridge)
 * </pre>
 *
 * <p>That's the windowed Local-Global rewrite (see
 * {@code TwoStageOptimizedWindowAggregateRule}). The local stage emits one
 * row per (key, slice_end) — slices are non-overlapping, so no dedup is
 * needed on the global side. Each slice's partial accumulator is merged
 * algebraically into the global window's keyed {@code ValueState<RowData>}.
 *
 * <h2>Late-data semantics</h2>
 *
 * <ul>
 *   <li>{@code table.exec.emit.allow-lateness = '30 s'} mirrors V2's
 *       {@code allowedLateness(Duration.ofSeconds(30))}.
 *   <li>{@code table.exec.emit.late-fire.enabled = true} makes a late event
 *       (within allow-lateness) trigger a re-fire that emits a -U/+U pair —
 *       proper changelog, not "emit a new absolute count" as in V2.
 *   <li>{@code sideOutputLateData(tag)} has no Table-API equivalent: events
 *       past {@code allow-lateness} are dropped silently and only counted
 *       via the {@code numLateRecordsDropped} metric.
 * </ul>
 *
 * <h2>What you give up</h2>
 *
 * <ul>
 *   <li>Side output for too-late events (V2's {@code LOCAL_LATE_TAG} /
 *       {@code GLOBAL_LATE_TAG}). Past {@code allow-lateness} the events
 *       are dropped silently.
 *   <li>Append-only output. Downstream sinks must understand the changelog
 *       (or use an upsert sink); otherwise the late-fire {@code -U} rows
 *       are dropped on the floor.
 *   <li>Explicit control over the local-stage shuffle. V3 eliminates that
 *       shuffle entirely with a non-keyed local pre-aggregator; the planner
 *       still inserts a hash exchange between local and global.
 * </ul>
 */
public class TwoPhaseCountDeduplicatedEventTimeV4 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // ────────────────────────────────────────────────────────────────────
        // Enable Local-Global + windowed late-data semantics on the Table env.
        //
        // The MiniBatch knobs are NOT required for windowed Local-Global
        // (slicing is the batching). They are kept here so the same config
        // block also covers unbounded GROUP BY aggregates in the same job.
        // ────────────────────────────────────────────────────────────────────
        tEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
        tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "5 s");
        tEnv.getConfig().set("table.exec.mini-batch.size", "5000");
        tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");

        // Windowed late-data — mirrors V2's allowedLateness(30s)
        tEnv.getConfig().set("table.exec.emit.allow-lateness", "30 s");
        tEnv.getConfig().set("table.exec.emit.late-fire.enabled", "true");
        tEnv.getConfig().set("table.exec.emit.late-fire.delay", "0 ms");

        // ────────────────────────────────────────────────────────────────────
        // Source — reuse the same delayed-events source as V2/V3.
        //
        // Tuple3<key, value, eventTimestamp> where eventTimestamp is millis.
        // Batch 1 fires the window normally; Batch 2 emits late events to
        // exercise the late-fire path.
        // ────────────────────────────────────────────────────────────────────
        DataStream<Tuple3<String, String, Long>> rawInput =
                env.addSource(
                        new TwoPhaseCountDeduplicatedEventTimeV2.DelayedEventsSource(),
                        TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {}));

        // ────────────────────────────────────────────────────────────────────
        // Bridge DataStream → Table.
        //
        // event_time is computed from the f2 (epoch-millis) column; the
        // watermark is held back 5s behind event_time to match V2's
        // forBoundedOutOfOrderness(Duration.ofSeconds(5)).
        // ────────────────────────────────────────────────────────────────────
        Table inputTable = tEnv.fromDataStream(
                rawInput,
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())     // key
                        .column("f1", DataTypes.STRING())     // value
                        .column("f2", DataTypes.BIGINT())     // epoch millis
                        .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(f2, 3)")
                        .watermark("event_time", "event_time - INTERVAL '5' SECOND")
                        .build());

        // ────────────────────────────────────────────────────────────────────
        // Windowed two-phase aggregate, declared in one expression.
        //
        // The planner rewrites this as:
        //   StreamPhysicalGlobalWindowAggregate
        //     +- Exchange (hash by f0)
        //        +- StreamPhysicalLocalWindowAggregate
        //           +- inputTable
        //
        // Each input record contributes exactly once at the local stage;
        // each slice's partial accumulator is merged into the global window
        // state exactly once. No dedup needed.
        // ────────────────────────────────────────────────────────────────────
        Table result = inputTable
                .window(Tumble.over(lit(1).minutes()).on($("event_time")).as("w"))
                .groupBy($("f0"), $("w"))
                .select(
                        $("f0").as("key"),
                        $("w").start().as("window_start"),
                        $("w").end().as("window_end"),
                        $("f0").count().as("cnt"));

        // ────────────────────────────────────────────────────────────────────
        // Surface the changelog so late-fire retractions are visible.
        //
        // For an initial window fire followed by a late-data re-fire,
        // expect output like:
        //   +I[group1, 1970-01-01T00:00, 1970-01-01T00:01, 5]
        //   -U[group1, 1970-01-01T00:00, 1970-01-01T00:01, 5]
        //   +U[group1, 1970-01-01T00:00, 1970-01-01T00:01, 6]
        // ────────────────────────────────────────────────────────────────────
        DataStream<Row> resultStream = tEnv.toChangelogStream(result);
        resultStream.print("RESULT");

        env.execute("Two-Phase Count Deduplicated V4 (Table API)");
    }
}
