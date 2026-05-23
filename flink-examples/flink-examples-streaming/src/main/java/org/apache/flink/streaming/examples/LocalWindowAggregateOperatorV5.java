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
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.types.RowKind;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * V5: NON-KEYED local pre-aggregator. Mirrors the Table API's {@code
 * LocalSlicingWindowAggOperator}.
 *
 * <h2>Why this stage is non-keyed</h2>
 *
 * <p>The point of a "local" pre-aggregation is to reduce records <b>before</b> any shuffle. If
 * this stage were keyed, all records for a given key would already have been hash-shuffled to a
 * single subtask — there would be no "local" amortization, only "global" amortization slightly
 * earlier.
 *
 * <p>The real Table API's {@code LocalSlicingWindowAggOperator} is therefore a raw {@link
 * AbstractStreamOperator} that runs at upstream parallelism with no preceding {@code keyBy}.
 * The bundle key is extracted by a {@code RowDataKeySelector} <i>inside</i> the operator.
 * Per-(key, slice) accumulators live in a plain in-memory buffer (no Flink state); the buffer
 * is flushed on watermark advance and forcibly flushed at every checkpoint barrier via {@code
 * prepareSnapshotPreBarrier}, which means there is no checkpointed state to lose.
 *
 * <h2>Buffer lifecycle</h2>
 *
 * <pre>
 *   processElement(row):
 *     bundleKey = keySelector.getKey(row)
 *     windowStart = ts - ts % windowSize        ← assigned from EVENT time
 *     handler.accumulate(row) into buffer[bundleKey][windowStart]
 *
 *   processWatermark(wm):
 *     forward the watermark downstream (so the global stage's timers fire),
 *     then for every (bundleKey, windowStart) whose windowEnd ≤ wm,
 *     emit (bundleKey, windowStart, getValue) and drop from buffer.
 *
 *   prepareSnapshotPreBarrier(ckpId):
 *     flush ALL — keeps the buffer empty at the barrier point so there is no
 *     state to checkpoint. On recovery, the upstream source replays from the
 *     checkpoint offset, the operator re-receives the same events, and the
 *     buffer rebuilds identically.
 * </pre>
 *
 * <h2>No late-data handling here</h2>
 *
 * <p>The local stage does NOT check {@code allow-lateness} or drop late events. If a late
 * event arrives whose {@code windowEnd} has already passed (so the partial for that window was
 * emitted earlier and the entry was dropped from the buffer), the event creates a FRESH
 * accumulator in the buffer for that {@code windowStart} and gets emitted on the next
 * watermark advance as another partial. The downstream {@link GlobalWindowAggregateFunctionV5}
 * decides what to do with it (re-fire or drop).
 *
 * <p>This is the same design as the real {@code LocalSlicingWindowAggOperator}.
 *
 * <p>The key correctness property: {@code windowStart} is computed from the event's
 * timestamp, not from the current watermark or clock — so a late event always lands in its
 * <b>original</b> window, never bleeds into the next one.
 */
public final class LocalWindowAggregateOperatorV5 extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {
    private static final long serialVersionUID = 1L;

    private final KeySelector<RowData, String> keySelector;
    private final long windowSizeMs;

    /**
     * In-memory buffer: bundleKey → (windowStart → accumulator). Pure heap — no Flink state.
     * The buffer is flushed before every checkpoint barrier, so it never participates in a
     * snapshot.
     */
    private transient Map<String, Map<Long, GenericRowData>> windowBuffer;

    /** The codegen-equivalent aggregation logic. */
    private transient AggsHandleFunction handler;

    /** Track the current watermark so we know when to flush. */
    private transient long currentWatermark;

    public LocalWindowAggregateOperatorV5(KeySelector<RowData, String> keySelector, long windowSizeMs) {
        this.keySelector = keySelector;
        this.windowSizeMs = windowSizeMs;
    }

    @Override
    public void open() throws Exception {
        super.open();
        windowBuffer = new HashMap<>();
        handler = new CountStarAggsHandleFunctionV5();
        handler.open(null);
        currentWatermark = Long.MIN_VALUE;
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();

        // Extract the bundle key from the row (mirroring how the real Table API's
        // RowDataKeySelector projects the GROUP BY columns).
        String bundleKey = keySelector.getKey(input);

        // Bucket by tumbling window. Real Table API uses a SliceAssigner; for tumbling-only
        // the slice end is just `windowStart + windowSize`.
        long eventTime = element.getTimestamp();
        long windowStart = eventTime - (eventTime % windowSizeMs);

        // Load-or-create the per-(bundleKey, windowStart) accumulator and run it through the
        // codegen-equivalent handler.
        Map<Long, GenericRowData> windowsForKey =
                windowBuffer.computeIfAbsent(bundleKey, k -> new HashMap<>());
        GenericRowData acc = windowsForKey.get(windowStart);
        if (acc == null) {
            acc = (GenericRowData) handler.createAccumulators();
        }
        handler.setAccumulators(acc);
        handler.accumulate(input);
        windowsForKey.put(windowStart, (GenericRowData) handler.getAccumulators());
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (mark.getTimestamp() > currentWatermark) {
            currentWatermark = mark.getTimestamp();
            flushCompletedWindows(currentWatermark);
        }
        // Forward the watermark downstream so the global stage's timers fire.
        super.processWatermark(mark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        // Flush EVERYTHING in the buffer before the checkpoint barrier. This guarantees the
        // buffer is empty at barrier time, so the operator has no state to checkpoint — and
        // on restore, replay from the source rebuilds the buffer identically.
        flushAllWindows();
    }

    /** Emit partials for every (bundleKey, windowStart) whose windowEnd has passed. */
    private void flushCompletedWindows(long watermark) throws Exception {
        for (Map.Entry<String, Map<Long, GenericRowData>> keyEntry : windowBuffer.entrySet()) {
            String bundleKey = keyEntry.getKey();
            Iterator<Map.Entry<Long, GenericRowData>> it =
                    keyEntry.getValue().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, GenericRowData> entry = it.next();
                long windowStart = entry.getKey();
                long windowEnd = windowStart + windowSizeMs;
                if (watermark >= windowEnd) {
                    emitPartial(bundleKey, windowStart, entry.getValue());
                    it.remove(); // partial is sent — drop the accumulator
                }
            }
        }
    }

    /** Flush the entire buffer (used before checkpoint barriers). */
    private void flushAllWindows() throws Exception {
        for (Map.Entry<String, Map<Long, GenericRowData>> keyEntry : windowBuffer.entrySet()) {
            String bundleKey = keyEntry.getKey();
            for (Map.Entry<Long, GenericRowData> entry : keyEntry.getValue().entrySet()) {
                emitPartial(bundleKey, entry.getKey(), entry.getValue());
            }
        }
        windowBuffer.clear();
    }

    /** Emit one partial row (insert-only — changelog semantics are global-stage only). */
    private void emitPartial(String bundleKey, long windowStart, GenericRowData acc)
            throws Exception {
        handler.setAccumulators(acc);
        long partial = handler.getValue().getLong(0);

        GenericRowData out = new GenericRowData(3);
        out.setField(0, StringData.fromString(bundleKey));
        out.setField(1, windowStart);
        out.setField(2, partial);
        out.setRowKind(RowKind.INSERT);
        output.collect(new StreamRecord<>(out));
    }

    @Override
    public void close() throws Exception {
        if (handler != null) {
            handler.close();
        }
        super.close();
    }
}
