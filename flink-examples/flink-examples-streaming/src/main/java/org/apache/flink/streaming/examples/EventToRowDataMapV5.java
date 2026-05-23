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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/**
 * V5 plumbing: convert the {@code Tuple3<key, value, eventTimeMillis>} input format used by
 * V2/V3/V4 into the {@link RowData} format that the Table runtime uses internally.
 *
 * <p>The Table runtime speaks {@link RowData} (a binary, columnar internal row representation)
 * everywhere — operators, state, serializers — so V5 lifts the input into that format at the
 * pipeline entrance. We project down to a single STRING column because {@code COUNT(*)} only
 * cares about the key for grouping; the value and event-time are unused once watermarks have
 * been attached upstream.
 *
 * <p>RowData layout: {@code [ STRING key ]}.
 *
 * <p>This is the V5 equivalent of what the planner generates as a {@code Calc} ExecNode above
 * a {@code TableSourceScan}: project the SQL row down to just the columns the aggregate needs.
 */
public final class EventToRowDataMapV5
        implements MapFunction<Tuple3<String, String, Long>, RowData> {
    private static final long serialVersionUID = 1L;

    @Override
    public RowData map(Tuple3<String, String, Long> in) {
        GenericRowData row = new GenericRowData(1);
        row.setField(0, StringData.fromString(in.f0));
        return row;
    }
}
