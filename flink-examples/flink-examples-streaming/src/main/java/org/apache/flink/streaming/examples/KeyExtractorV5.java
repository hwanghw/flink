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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;

/**
 * V5 plumbing: extract the {@code key} column (field 0) from a {@link RowData}.
 *
 * <p>The Table runtime uses {@code RowDataKeySelector} for this — it projects the {@code GROUP
 * BY} columns out of a row and builds a binary key. V5 uses a trivial one that returns the
 * STRING value of field 0 since the V5 schema has the key as the first (only) grouping field.
 *
 * <p>The same selector is used for two purposes:
 *
 * <ul>
 *   <li>Inside {@link LocalWindowAggregateOperatorV5} to compute the bundle key for each
 *       incoming row. The local stage is non-keyed (no preceding {@code keyBy}) so it must
 *       extract the key in-operator.
 *   <li>For the {@code keyBy()} between the local and global stages — the single hash exchange
 *       in the V5 pipeline.
 * </ul>
 *
 * <p>Field 0 holds the key in BOTH:
 *
 * <ul>
 *   <li>input rows (layout: {@code [key]}), and
 *   <li>local-partial rows (layout: {@code [key, windowStart, partialCount]}).
 * </ul>
 *
 * <p>So one selector works for both call sites.
 */
public final class KeyExtractorV5 implements KeySelector<RowData, String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String getKey(RowData row) {
        return row.getString(0).toString();
    }
}
