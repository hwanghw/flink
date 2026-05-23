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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;

/**
 * V5: hand-written equivalent of what the Table API codegen produces for a single {@code
 * COUNT(*)} aggregate.
 *
 * <h2>What this class stands in for</h2>
 *
 * <p>In the real Table API, the SQL {@code COUNT(*)} is parsed into a Calcite {@code
 * AggregateCall}, passed to {@code AggsHandlerCodeGenerator}, which assembles a Java source
 * string. Janino compiles that source at task {@code open()} time. The generated class
 * implements {@link AggsHandleFunction}. The body is essentially what's below — modulo
 * variable naming, defensive null checks, and the closure references that flow in via the
 * generated class's {@code references} array.
 *
 * <h2>Aggregation arithmetic</h2>
 *
 * <p>For {@code COUNT(*)} the arithmetic is trivial:
 *
 * <ul>
 *   <li>accumulator = single-field {@link GenericRowData} containing a {@code Long} counter
 *   <li>{@code accumulate(any row)} → {@code counter++} (ignores the input — COUNT(*) doesn't
 *       inspect column values)
 *   <li>{@code retract(any row)} → {@code counter--}
 *   <li>{@code merge(other acc row)} → {@code counter += other.counter}
 *   <li>{@code getValue()} → single-field row containing the counter
 * </ul>
 *
 * <p>Different SQL aggregates (SUM, AVG, COUNT(DISTINCT col), user UDAFs) produce different
 * {@link AggsHandleFunction} implementations but the V5 outer operators ({@link
 * LocalWindowAggregateOperatorV5} / {@link GlobalWindowAggregateFunctionV5}) stay the same.
 * That separation is the educational point of V5.
 *
 * <h2>What we don't need</h2>
 *
 * <ul>
 *   <li>No DataViews — {@code COUNT(*)} has no per-aggregate state views (those are used for
 *       things like {@code COUNT(DISTINCT col)} which holds a {@code MapState} of seen
 *       values). So {@link #open(StateDataViewStore)} is a no-op.
 *   <li>No input inspection — {@code COUNT(*)} ignores the contents of each input row.
 *   <li>No window-size setup — that's only relevant for batch-style window functions like
 *       {@code PERCENT_RANK}.
 * </ul>
 */
public final class CountStarAggsHandleFunctionV5 implements AggsHandleFunction {
    private static final long serialVersionUID = 1L;

    /** The current accumulator. Single field: count : BIGINT. */
    private transient GenericRowData accumulator;

    @Override
    public void open(StateDataViewStore store) {
        // COUNT(*) needs no per-aggregate state views — nothing to register.
    }

    @Override
    public RowData createAccumulators() {
        GenericRowData acc = new GenericRowData(1);
        acc.setField(0, 0L);
        return acc;
    }

    @Override
    public void setAccumulators(RowData accumulators) {
        this.accumulator = (GenericRowData) accumulators;
    }

    @Override
    public RowData getAccumulators() {
        return accumulator;
    }

    @Override
    public void accumulate(RowData input) {
        long current = accumulator.getLong(0);
        accumulator.setField(0, current + 1L);
    }

    @Override
    public void retract(RowData input) {
        long current = accumulator.getLong(0);
        accumulator.setField(0, current - 1L);
    }

    @Override
    public void merge(RowData otherAccumulators) {
        long current = accumulator.getLong(0);
        long incoming = otherAccumulators.getLong(0);
        accumulator.setField(0, current + incoming);
    }

    @Override
    public void resetAccumulators() {
        accumulator.setField(0, 0L);
    }

    @Override
    public RowData getValue() {
        // For COUNT(*), the "value" is simply the count.
        GenericRowData out = new GenericRowData(1);
        out.setField(0, accumulator.getLong(0));
        return out;
    }

    @Override
    public void setWindowSize(int windowSize) {
        // Unused — only relevant for batch-style window functions like PERCENT_RANK.
    }

    @Override
    public void cleanup() {
        // Nothing to clean up — accumulator is recreated per (key, window).
    }

    @Override
    public void close() {
        // Nothing to close.
    }
}
