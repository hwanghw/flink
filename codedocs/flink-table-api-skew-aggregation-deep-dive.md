# Flink Table API: How Aggregations Handle Data Skew (Local–Global + Split-Distinct)

A deep dive into how Flink SQL / Table API protects aggregations against data skew. Three optimizations work together:

1. **MiniBatch** — buffer records to reduce state access (foundation for the two below)
2. **Local-Global aggregation** — two-phase aggregate split (combine + reduce) to fan-in skewed group keys
3. **Split-Distinct aggregation** — hash-bucket rewrite to break up hot-spots in `COUNT(DISTINCT …)`

The user-facing question that motivates this doc:

> *"What happens when I run `SELECT COUNT(1) FROM table1` against a skewed source? Does Flink break the aggregate into local + global stages?"*

Short answer: **`SELECT COUNT(1) FROM table1`** is a *scalar* aggregate with no `GROUP BY`. It has no group key to skew on, but its downstream **single subtask** is still a fan-in stress point. Local-Global still applies — each upstream parallel task pre-aggregates into a single row per mini-batch before the final singleton sink. The "skew" case people usually mean is `GROUP BY skewed_key` (where Local-Global helps) or `COUNT(DISTINCT col)` (where Split-Distinct helps). All three scenarios are covered below.

---

## Architecture Overview

```
                     ┌─────────────────────────────────────────────────────┐
                     │  Three layers of Table-API aggregate optimization   │
                     └─────────────────────────────────────────────────────┘

        Logical SQL                  Logical plan                    Physical plan
   ┌──────────────────┐         ┌────────────────────┐         ┌────────────────────────┐
   │  user SQL with   │  parse  │ FlinkLogical-      │  rules  │ StreamPhysicalGroup-   │
   │  GROUP BY /      │ ──────▶ │ Aggregate          │ ──────▶ │ Aggregate              │
   │  DISTINCT        │         │                    │         │ (default: ONE stage)   │
   └──────────────────┘         └────────────────────┘         └────────────────────────┘
                                          │                              │
                                          │  SplitAggregateRule          │ TwoStageOptimized-
                                          │  (logical)                   │ AggregateRule
                                          │  rewrites DISTINCT into      │ (physical)
                                          ▼  bucket-hashed two-level     ▼ splits 1-stage
                                  ┌─────────────────────┐           into local+global
                                  │ FlinkLogical-       │
                                  │ Aggregate (final)   │       ┌──────────────────────────┐
                                  │  +- Aggregate       │       │ StreamPhysicalGlobal-    │
                                  │     (partial)       │       │ GroupAggregate           │
                                  │     +- Expand       │       │  +- Exchange (hash key)  │
                                  │        +- Calc      │       │     +- StreamPhysical-   │
                                  │           (hash%N)  │       │        LocalGroupAggregate│
                                  └─────────────────────┘       └──────────────────────────┘
                                                                          │ translateToPlan
                                                                          ▼
                                  ┌──────────────────────────────────────────────────────┐
                                  │ runtime operators (one TM-side, one downstream):     │
                                  │  • MiniBatchLocalGroupAggFunction                    │
                                  │    (bundle-only, no state, MapBundleFunction)        │
                                  │  • MiniBatchGlobalGroupAggFunction                   │
                                  │    (bundle + keyed ValueState<RowData>)              │
                                  └──────────────────────────────────────────────────────┘
```

There are **two transformations** at different levels of the planner:

- **`SplitAggregateRule`** is a **logical** rule. It looks for `DISTINCT` calls and rewrites the logical aggregate into a two-level structure with a synthetic `hash_code(distinct_key) % bucket_num` group key.
- **`TwoStageOptimizedAggregateRule`** is a **physical** rule. It matches a `StreamPhysicalGroupAggregate` over an `Exchange` and splits it into a local–global pair, dropping the local agg on the input side of the exchange so it runs chained with the upstream operator.

They are complementary — and on a query with `COUNT(DISTINCT …) GROUP BY …` both fire, layering one on top of the other.

---

## 1. MiniBatch — the prerequisite

MiniBatch isn't a skew optimization per se; it's a state-access optimization that **enables** Local-Global. Without MiniBatch, the Local-Global rule will not match.

### What it does

Buffer N records (or wait T duration), then aggregate the batch in one shot. Instead of `state.get → update → state.put` per record, you do it once per (key, batch). Reduces RocksDB roundtrips dramatically when many records share a key.

The runtime side is a `MapBundleOperator` that wraps a `MapBundleFunction`. Both Local and Global aggregate functions extend `MapBundleFunction<RowData, RowData, RowData, RowData>`. See:

- `MiniBatchLocalGroupAggFunction.java:37-101`
- `MiniBatchGlobalGroupAggFunction.java:45-242`

### Config

| Key | Type | Default | Notes |
|---|---|---|---|
| `table.exec.mini-batch.enabled` | boolean | `false` | Master switch. Source: `ExecutionConfigOptions.java:664-674` |
| `table.exec.mini-batch.allow-latency` | duration | `0 ms` | Must be `> 0` when enabled. Source: `ExecutionConfigOptions.java:676-687` |
| `table.exec.mini-batch.size` | long | `-1` | Must be `> 0` when enabled. Source: `ExecutionConfigOptions.java:689-700` |

Minimal enabling boilerplate:

```sql
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5 s';
SET 'table.exec.mini-batch.size' = '5000';
```

---

## 2. Local-Global for unbounded Group Aggregate (no windowing)

This section covers the **unbounded** path: `SELECT key, COUNT(*) FROM t GROUP BY key` with no `WINDOW` clause. Records arrive forever, the state is updated forever, results are emitted as a changelog (`+I` / `-U` / `+U` / `-D`). There is no concept of "late" data here — every record is just an update. The windowed analogue is covered in §3.

### When it applies

A `StreamPhysicalGroupAggregate` directly over an `Exchange` (i.e., a hash-shuffled aggregate). The optimizer wants to split this into a local pre-aggregate (no shuffle) followed by an exchange and a global aggregate.

### The rule precondition logic

`TwoStageOptimizedAggregateRule.matches(...)` (`TwoStageOptimizedAggregateRule.java:86-94`):

```java
boolean isMiniBatchEnabled =
        tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
boolean isTwoPhaseEnabled =
        getAggPhaseStrategy(tableConfig) != AggregatePhaseStrategy.ONE_PHASE;

return isMiniBatchEnabled && isTwoPhaseEnabled && matchesTwoStage(call.rel(0), call.rel(2));
```

`matchesTwoStage(...)` (lines 96-117) additionally requires:

- All aggregates are **partial-merge-capable** — `AggregateUtil.doAllSupportPartialMerge(aggInfoList.aggInfos())`. `SUM`, `COUNT`, `MIN`, `MAX`, `AVG` all qualify; user-defined `AggregateFunction`s only qualify if they implement `merge()`.
- The input does **not already satisfy** the required hash distribution — i.e., there genuinely is a shuffle to break (otherwise local agg buys nothing).

### What the rule rewrites

From the rule's class JavaDoc (`TwoStageOptimizedAggregateRule.java:65-72`), `onMatch` transforms

```
StreamPhysicalGroupAggregate
  +- StreamPhysicalExchange
     +- input
```

into

```
StreamPhysicalGlobalGroupAggregate
  +- StreamPhysicalExchange
     +- StreamPhysicalLocalGroupAggregate
        +- input
```

`onMatch` (lines 137-193) builds the two new nodes:

```java
StreamPhysicalLocalGroupAggregate localHashAgg =
        new StreamPhysicalLocalGroupAggregate(
                originalAgg.getCluster(),
                localAggTraitSet,
                realInput,
                originalAgg.grouping(),         // same grouping keys
                originalAgg.aggCalls(),
                aggCallNeedRetractions,
                needRetraction,
                originalAgg.partialFinalType());

int[] globalGrouping =
        IntStream.range(0, originalAgg.grouping().length).toArray();
FlinkRelDistribution globalDistribution = createDistribution(globalGrouping);
RelNode newInput =
        FlinkExpandConversionRule.satisfyDistribution(
                FlinkConventions.STREAM_PHYSICAL(), localHashAgg, globalDistribution);

StreamPhysicalGlobalGroupAggregate globalAgg =
        new StreamPhysicalGlobalGroupAggregate(
                originalAgg.getCluster(),
                globalAggProvidedTraitSet,
                newInput,                       // already wrapped in Exchange by satisfyDistribution
                originalAgg.getRowType(),
                globalGrouping,
                originalAgg.aggCalls(),
                aggCallNeedRetractions,
                realInput.getRowType(),
                needRetraction,
                originalAgg.partialFinalType(),
                Option.empty(),
                originalAgg.hints());
```

`createDistribution(keys)` returns **`SINGLETON`** when `keys.length == 0` — this is the key insight for `SELECT COUNT(1) FROM t` (see §5).

### Why this fixes skew

Without local agg:

```
   subtask 0: 1,000,000 rows   ─► shuffle by key ─► one subtask gets 99% of them
   subtask 1:        10 rows                          (hot key)
   subtask 2:        10 rows
   subtask 3:        10 rows
```

With local agg:

```
   subtask 0: 1,000,000 rows ─► local agg ─► 5 rows (one per local key) ─► shuffle ─► global agg
   subtask 1:        10 rows ─► local agg ─► 5 rows                       ─► shuffle ─► global agg
   subtask 2:        10 rows ─► local agg ─► 5 rows                       ─► shuffle ─► global agg
   subtask 3:        10 rows ─► local agg ─► 5 rows                       ─► shuffle ─► global agg
```

The hot key still goes to one global subtask, but it now sees ~4 rows from upstream instead of 1,000,000+. The CPU/state work is shifted from the global side (which can't be parallelized for that key) to the local side (which is fully parallel).

### Config

| Key | Type | Default | Notes |
|---|---|---|---|
| `table.optimizer.agg-phase-strategy` | enum | `AUTO` | `AUTO` lets the cost model decide; `TWO_PHASE` forces the split when possible; `ONE_PHASE` disables the rule. Source: `OptimizerConfigOptions.java:44-55`, `AggregatePhaseStrategy.java:27-43` |

`AUTO` means: the rule still fires when the precondition logic passes — the "auto" is about preferring local-global when it's available, not turning the rule on/off based on data statistics. The hard switch is `agg-phase-strategy: ONE_PHASE`, which disables the rule entirely (`TwoStageOptimizedAggregateRule.java:90-91`).

---

## 3. Local-Global for windowed Aggregate (with late-data semantics)

The unbounded path in §2 has no concept of "late" — every record updates the running state. The **windowed** path is different: events have an event-time, the operator advances a watermark, and a record can arrive *after* the watermark for its window. This section covers what happens then.

### Two-stage rule for windowed aggregates

The unbounded `TwoStageOptimizedAggregateRule` does **not** match `StreamPhysicalWindowAggregate` — there's a separate rule, `TwoStageOptimizedWindowAggregateRule` (`flink-table-planner/.../rules/physical/stream/TwoStageOptimizedWindowAggregateRule.java:66-160`). It does the same split shape — `WindowAggregate → Exchange → input` becomes `GlobalWindowAggregate → Exchange → LocalWindowAggregate → input` — but with extra preconditions:

```java
// TwoStageOptimizedWindowAggregateRule.matches  (lines 80-108)
if (getAggPhaseStrategy(tableConfig) == AggregatePhaseStrategy.ONE_PHASE) return false;

// processing time windows can't be split — proctime must materialize on one node
if (!windowing.isRowtime()) return false;

// session windows produce different assignments on each side — skip
if (windowing.getWindow() instanceof SessionWindowSpec) return false;

// every aggregate must implement merge()
if (!AggregateUtil.doAllSupportPartialMerge(windowAgg.aggInfoList().aggInfos())) return false;

return !isInputSatisfyRequiredDistribution(realInput, windowAgg.grouping());
```

Notably absent: **the `mini-batch.enabled` check.** Windowed two-stage doesn't depend on the mini-batch operator — windowing itself is the batching mechanism. Slice-level pre-aggregation happens regardless.

### Slice-based pre-aggregation

Window TVF aggregates (the modern path: `SELECT … FROM TABLE(TUMBLE(…)) GROUP BY window_start, …`) compute on **slices**, not whole windows. A slice is the smallest non-overlapping time unit; a window is composed of one or more slices.

- Tumbling: slice = window.
- Hopping: slice = `gcd(size, slide)`. A 60s/20s hopping window has 20s slices; each window contains 3 slices.
- Cumulative: slice = `step`. Each new firing extends the window by one slice.

The local stage accumulates per-`(key, slice)`. The global stage merges per-`(key, window)` by combining the relevant slices. This is *also* a form of "local-global", layered on top of the parallelism-based one — slicing splits *time*, local-global splits *parallelism*.

```
                       ┌───────────────────────────────────────────────────────┐
                       │  windowed Local-Global runtime layout                 │
                       └───────────────────────────────────────────────────────┘

   parallel input ─►  LocalWindowAggregate  ─►  Exchange (hash by key)  ─►  GlobalWindowAggregate
                       (per-task, per-slice)                                  (keyed state per (key, window)
                       no shuffle yet                                          stored in WindowAggOperator
                                                                               via InternalTimerService)
```

Both stages are deployed via `StreamExecLocalWindowAggregate` / `StreamExecGlobalWindowAggregate` ExecNodes. The runtime operator on the global side is `WindowAggOperator` (`flink-table-runtime/.../operators/window/tvf/common/WindowAggOperator.java`).

### Late data handling in `WindowAggOperator`

The operator's hot-path is `processElement` (`WindowAggOperator.java:215-224`):

```java
@Override
public void processElement(StreamRecord<RowData> element) throws Exception {
    RowData inputRow = element.getValue();
    RowData currentKey = (RowData) getCurrentKey();
    boolean isElementDropped = windowProcessor.processElement(currentKey, inputRow);
    if (isElementDropped) {
        lateRecordsDroppedRate.markEvent();   // numLateRecordsDropped counter
    }
}
```

The `windowProcessor.processElement(...)` returns `true` when the record's event-time falls behind the operator's progress watermark by more than the configured allowed-lateness — i.e. the record is **late and dropped**. Otherwise the record is folded into the slice accumulator (in heap state for the in-flight slice, then merged into RocksDB-backed window state when the slice closes).

When the watermark advances past `window.maxTimestamp() + allowedLateness`, the operator fires the window (via `onEventTime`, lines 241-243 → `onTimer`, lines 258-265):

```java
private void onTimer(InternalTimer<K, W> timer) throws Exception {
    setCurrentKey(timer.getKey());
    W window = timer.getNamespace();
    windowProcessor.fireWindow(timer.getTimestamp(), window);
    windowProcessor.clearWindow(timer.getTimestamp(), window);
}
```

### What "late but not dropped" looks like

If `table.exec.emit.allow-lateness` is configured to allow some lateness, a late event that *would* be inside the window can:

- **Trigger a re-fire of the window** if `table.exec.emit.late-fire.enabled = true`. The operator re-emits the updated result. Downstream sees a retract (`-U`) of the previous window result followed by `+U` with the new value.
- **Be silently absorbed** into the still-open window state if the cleanup timer hasn't fired yet (the watermark is past window-end but the cleanup timer for `window.maxTimestamp() + allowedLateness` hasn't arrived). The window will fire normally later, with the late record included.
- **Be dropped** if past the cleanup boundary. `numLateRecordsDropped` increments.

This is the same pattern as the DataStream `WindowOperator`'s `isWindowLate` / `isElementLate` checks documented in `flink-window-state-rocksdb-checkpoint-deep-dive.md` (the operators differ but the semantics line up).

### How a late event flows through Local + Global

```
   ┌──────────────────────────────────────────────────────────────────────────────┐
   │  Late event arrives at Local-Window-Aggregate (upstream of shuffle)          │
   └──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
   ① Local stage check: is this event behind the local progress watermark
      by more than allowedLateness?
        yes → drop, increment local numLateRecordsDropped
        no  → fold into the local slice accumulator (in-memory per task)
                                       │
                                       ▼
   ② Slice closes (watermark crosses slice-end). Local stage emits the slice's
      (key, slice_end, partial_acc) downstream.
                                       │  hash-shuffle by key
                                       ▼
   ③ Global stage receives (key, slice_end, partial_acc).
        setCurrentKey(key)
        looks up window-namespaced state for (key, window_containing_slice)
        merges partial_acc into the window accumulator
                                       │
                                       ▼
   ④ When the global stage's cleanup timer fires (window.maxTimestamp +
      allowedLateness), it emits the window result and clears the window state.

   ⑤ If table.exec.emit.late-fire.enabled, a late event that lands inside an
      already-fired-but-not-yet-cleared window triggers a re-fire:
        -U  (key, window_start, window_end, previous_result)   ← retract
        +U  (key, window_start, window_end, updated_result)    ← new
```

The local stage's late-drop is *independent* of the global stage's. A late event might survive the local watermark check but be dropped by the global stage, or vice versa, depending on each stage's progress watermark and configured allowed-lateness. In practice they advance together because watermarks propagate through the exchange.

### Config (windowed-specific)

From `WindowEmitStrategy.scala:198-264`:

| Key | Type | Default | Notes |
|---|---|---|---|
| `table.exec.emit.early-fire.enabled` | boolean | `false` | **Experimental.** Emit partial window results before the watermark passes window-end. Combine with `early-fire.delay` for the firing interval. |
| `table.exec.emit.early-fire.delay` | duration | — | Required when `early-fire.enabled` is true. `0` = fire per record. |
| `table.exec.emit.late-fire.enabled` | boolean | `false` | **Experimental.** Re-fire the window when late events arrive after window-end (within `allow-lateness`). |
| `table.exec.emit.late-fire.delay` | duration | — | Required when `late-fire.enabled` is true. |
| `table.exec.emit.allow-lateness` | duration | falls back to `IDLE_STATE_RETENTION` | Max event-time lateness tolerated. Past this, events are dropped and `numLateRecordsDropped` is incremented. |

All five are still tagged `@Experimental` (FLINK-29692 tracks their stabilization). They apply only to windowed queries.

### Windowed aggregates and Split-Distinct

`SplitAggregateRule.matches` (covered in §4) excludes **proctime** and **session** windowed aggregates explicitly:

```java
return agg.partialFinalType() == PartialFinalType.NONE
        && agg.containsDistinctCall()
        && splitDistinctAggEnabled
        && isAllAggSplittable
        && !isProctimeWindowAgg
        && !isTableAgg
        && !isSessionWindowAgg;
```

Rowtime tumbling/hopping/cumulative window TVF aggregates DO support split-distinct.

---

## 4. Split-Distinct Aggregation — break up hot-key `COUNT(DISTINCT …)`

`COUNT(DISTINCT user_id)` has a worst-case design: all rows hash to the same group key on the global side, and the operator must maintain a `Set<user_id>` (or equivalent `MapState`) per group. With one group, that's one subtask holding the entire universe.

The fix is to introduce an *additional* synthetic group key — the **hash bucket** — that's only used in the first level of aggregation.

### The rewrite (from `SplitAggregateRule` JavaDoc)

For

```sql
SELECT SUM(DISTINCT b), COUNT(DISTINCT c), AVG(b) FROM MyTable GROUP BY a
```

the logical plan becomes (`SplitAggregateRule.java:100-117`):

```
FlinkLogicalCalc(select=[$f1 AS EXPR$0, $f2 AS EXPR$1, CAST(... / $f4) AS EXPR$2])
+- FlinkLogicalAggregate(group=[{0}], agg#0=[SUM($3)], agg#1=[$SUM0($4)],
                        agg#2=[$SUM0($5)], agg#3=[$SUM0($6)])
   +- FlinkLogicalAggregate(group=[{0, 3, 4}],
                            agg#0=[SUM(DISTINCT $1) FILTER $5],
                            agg#1=[COUNT(DISTINCT $2) FILTER $6],
                            agg#2=[$SUM0($1) FILTER $7],
                            agg#3=[COUNT($1) FILTER $7])
      +- FlinkLogicalCalc(select=[a, b, c, $f3, $f4,
                                   =($e, 1) AS $g_1, =($e, 2) AS $g_2, =($e, 3) AS $g_3])
         +- FlinkLogicalExpand(projects=[a, b, c, $f3, $f4, $e])
            +- FlinkLogicalCalc(select=[a, b, c,
                                         MOD(HASH_CODE(b), 1024) AS $f3,
                                         MOD(HASH_CODE(c), 1024) AS $f4])
               +- FlinkLogicalTableSourceScan(table=[[MyTable]], fields=[a, b, c])
```

Key transformations, bottom-up:

1. **Hash projection** (`SplitAggregateRule.java:212-233`): for each distinct-arg column, add `MOD(HASH_CODE(col), BUCKET_NUM)` as a new projected field.
2. **Expand** when more than one distinct-arg exists (`needExpand = newGroupSetsNum > 1`, line 295): replicate each input row N times (one per distinct call), with an `$e` group-id column distinguishing which expand-copy this row belongs to. This avoids cross-product blow-up.
3. **Partial aggregate** (line 235+): group by `(orig_keys, hash_mod)` and run the partial agg-functions (`SUM(DISTINCT col)` etc.) with row-level filter columns derived from `$e`.
4. **Final aggregate**: group by `orig_keys` and merge the partial results (`SUM` or `$SUM0` of partials, depending on null-handling semantics).

### Bucket key — how the hash works

`SplitAggregateRule.java:208-227`:

```java
int buckets =
        tableConfig.get(
                OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_BUCKET_NUM);
...
RexNode node =
        relBuilder.call(
                SqlStdOperatorTable.MOD,
                relBuilder.call(FlinkSqlOperatorTable.HASH_CODE, hashField),
                relBuilder.literal(buckets));
```

The same `distinct_key` always lands in the same bucket, so no `DISTINCT` element is double-counted. The bucket fans the work across up to `BUCKET_NUM` subtasks on the partial side, while the final side merges per-bucket partial results back into one row per original group.

### Match preconditions

`SplitAggregateRule.matches(...)` (lines 150-181):

```java
return agg.partialFinalType() == PartialFinalType.NONE   // not already split
        && agg.containsDistinctCall()                    // has DISTINCT
        && splitDistinctAggEnabled                       // config on
        && isAllAggSplittable                            // every agg supports split
        && !isProctimeWindowAgg                          // proctime windows excluded
        && !isTableAgg                                   // FLINK-21923
        && !isSessionWindowAgg;                          // window-assigner correctness
```

`isAllAggSplittable` is checked by `AggregateUtil.doAllAggSupportSplit(…)` — for built-ins, AVG / SUM / COUNT / MAX / MIN are all splittable. User-defined `AggregateFunction`s are not.

### Config

| Key | Type | Default | Notes |
|---|---|---|---|
| `table.optimizer.distinct-agg.split.enabled` | boolean | `false` | Source: `OptimizerConfigOptions.java:66-77` |
| `table.optimizer.distinct-agg.split.bucket-num` | int | `1024` | Source: `OptimizerConfigOptions.java:79-87` |

---

## 5. Concrete walkthrough: `SELECT COUNT(1) FROM table1`

This is your originally-asked query. Let's trace what happens to it.

### Without optimization (default config)

```
                          parallel input
                  ┌─────────┬─────────┬─────────┬─────────┐
                  │ src-0   │ src-1   │ src-2   │ src-3   │
                  │ 1M rows │ 1M rows │ 1M rows │ 1M rows │  ◄── 4M rows, each emitted
                  └────┬────┴────┬────┴────┬────┴────┬────┘     individually
                       │         │         │         │
                       └──────┐  ▼  ┌──────┘         │
                              └──┐│┌──┐              │
                                 ▼▼▼  ▼
                            (SINGLETON shuffle — all 4M rows go to one subtask)
                                  │
                                  ▼
                       ┌────────────────────────┐
                       │ StreamPhysicalGroup-   │ ◄── one subtask
                       │ Aggregate              │     handles all 4M
                       │ (count++ per row)      │     row events
                       └────────────────────────┘
```

The downstream subtask sees the full input rate. State is one `ValueState<Long>` (or similar) updated 4M times. The shuffle network carries 4M rows.

### With `mini-batch.enabled=true` + `agg-phase-strategy=TWO_PHASE`

`TwoStageOptimizedAggregateRule` fires. `createDistribution([])` returns `FlinkRelDistribution.SINGLETON()` (line 132), so the exchange is still a singleton:

```
                          parallel input
                  ┌─────────┬─────────┬─────────┬─────────┐
                  │ src-0   │ src-1   │ src-2   │ src-3   │
                  │ 1M rows │ 1M rows │ 1M rows │ 1M rows │
                  └────┬────┴────┬────┴────┬────┴────┬────┘
                       ▼         ▼         ▼         ▼
              ┌──────────┐┌──────────┐┌──────────┐┌──────────┐
              │ Local-   ││ Local-   ││ Local-   ││ Local-   │ ◄── chained with source
              │ GroupAgg ││ GroupAgg ││ GroupAgg ││ GroupAgg │     no shuffle
              │ buffers  ││ buffers  ││ buffers  ││ buffers  │
              │ batch    ││ batch    ││ batch    ││ batch    │
              │ emits    ││ emits    ││ emits    ││ emits    │
              │  ONE     ││  ONE     ││  ONE     ││  ONE     │ ◄── one partial-count
              │  row per ││  row per ││  row per ││  row per │     row per mini-batch
              │  batch   ││  batch   ││  batch   ││  batch   │
              └────┬─────┘└────┬─────┘└────┬─────┘└────┬─────┘
                   │           │           │           │
                   └───────────┴───────┬───┴───────────┘
                                       ▼
                          (SINGLETON shuffle ── carries a
                           handful of rows/sec, not 4M/sec)
                                       │
                                       ▼
                            ┌────────────────────────┐
                            │ Global-                │ ◄── one subtask, but
                            │ GroupAggregate         │     receiving way less
                            │ (sums up partial cnts) │     traffic
                            └────────────────────────┘
```

Same final answer, but:

- The 4M-row firehose into the global subtask becomes a small trickle of pre-aggregated rows.
- The 1M `state.get/put` operations per upstream task that the global agg used to do are replaced by ~one bundle-merge per mini-batch.

Whether this is technically "skew elimination" depends on definitions. For a no-`GROUP BY` aggregate, the global subtask is always the bottleneck (`SINGLETON` distribution); local-global just shifts the *work* upstream so the bottleneck doesn't drown.

### With `GROUP BY` (the "real" skew case)

```sql
SELECT user_id, COUNT(*) FROM events GROUP BY user_id;
```

Here `createDistribution(grouping)` returns a hash distribution. Each upstream subtask runs its own LocalGroupAggregate (pre-aggregating per `user_id` within the task), then the exchange routes per-key partials to the GlobalGroupAggregate. The skew on a hot `user_id` is reduced from "one global subtask eats 99% of all events" to "one global subtask eats 99% of *the partial-aggregated trickle*."

### With `COUNT(DISTINCT)` (the split-distinct case)

```sql
SELECT day, COUNT(DISTINCT user_id) FROM events GROUP BY day;
```

With `distinct-agg.split.enabled = true`, `SplitAggregateRule` first rewrites the logical aggregate into a two-level one with a synthetic bucket key:

```sql
-- conceptually
SELECT day, SUM(cnt) FROM (
    SELECT day, COUNT(DISTINCT user_id) AS cnt
    FROM events
    GROUP BY day, MOD(HASH_CODE(user_id), 1024)
) GROUP BY day;
```

Then `TwoStageOptimizedAggregateRule` runs on each of those two physical aggregates, giving up to **four** runtime aggregates in the chain. (In practice the inner aggregate's local stage is sometimes elided.)

---

## 6. How the key drives state updates in the global aggregate

This is the question of "which slot in RocksDB does a particular record update?" — the answer is the same machinery as everywhere else in Flink keyed state, just packaged behind the SQL planner.

### The key flows from SQL → planner → operator

```
   SQL                                     Planner                                      Runtime
   ───                                     ───────                                      ───────

   GROUP BY col_a, col_b      ──► grouping = [0, 1]  (field indices)   ──► RowDataKeySelector
                                                                            ├─ project (col_a, col_b)
                                                                            └─ build a binary RowData
                                                                                 = the "key"
                                                                            │
                                                                            ▼
                                                                       keyBy in DataStream layer
                                                                            │
                                                                            ▼
                                                                       KeyedStream uses this RowData
                                                                       as the keying context for
                                                                       state access
```

`StreamExecLocalGroupAggregate.translateToPlanInternal(...)` (`flink-table-planner/.../stream/StreamExecLocalGroupAggregate.java:168-176`) is where the `RowDataKeySelector` is constructed:

```java
final RowDataKeySelector selector =
        KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(),
                grouping,                                                  // SQL GROUP BY indices
                (InternalTypeInfo<RowData>) inputTransform.getOutputType());

final MapBundleOperator<RowData, RowData, RowData, RowData> operator =
        new MapBundleOperator<>(
                aggFunction, MinibatchUtil.createMiniBatchTrigger(config), selector);
```

The same selector is what the downstream Exchange uses to hash-partition rows, and what the GlobalAggregate uses for keyed state lookups.

### Inside the global stage's `finishBundle`

When the mini-batch fires, the global stage walks the in-memory bundle and **explicitly sets the current key per entry** before touching state. From `MiniBatchGlobalGroupAggFunction.finishBundle` (lines 161-230):

```java
for (Map.Entry<RowData, RowData> entry : buffer.entrySet()) {
    RowData currentKey = entry.getKey();    // the projected GROUP BY columns
    RowData bufferAcc  = entry.getValue();  // the bundle's partial accumulator

    ctx.setCurrentKey(currentKey);          // ◄── pivots the keyed state backend

    RowData stateAcc = accState.value();    // RocksDB GET keyed by currentKey
    ...
    globalAgg.merge(bufferAcc);             // fold the bundle into the keyed state
    ...
    accState.update(stateAcc);              // RocksDB PUT keyed by currentKey
}
```

`ctx.setCurrentKey(currentKey)` is the linchpin — it tells the `KeyedStateBackend` *which* RocksDB slot to address for the next `accState.value()` / `accState.update()`. Without this call, the previous bundle entry's key would still be active and you'd corrupt the wrong slot.

### What's in the composite key bytes (RocksDB layer)

This is the same composite-key layout documented in [flink-window-state-rocksdb-checkpoint-deep-dive.md §1](flink-window-state-rocksdb-checkpoint-deep-dive.md):

```
┌────────────────────┬──────────────────────┬───────────────────────────────────┐
│  key-group prefix  │    serialized key    │   serialized namespace            │
│   1 or 2 bytes     │       M bytes        │         N bytes                   │
└────────────────────┴──────────────────────┴───────────────────────────────────┘
        ▲                       ▲                        ▲
   derived from           the RowData that was      different for unbounded
   maxParallelism +       set via setCurrentKey     vs windowed aggregates
   currentKey
```

| Aggregate kind | What lives in `[serialized namespace]` |
|---|---|
| **Unbounded GroupAggregate** | `VoidNamespace.INSTANCE` — a zero-byte namespace. One state slot per key, ever. |
| **Windowed Aggregate** (Window TVF) | The `TimeWindow{start, end}` — one slot per `(key, window)`. Same layout as the DataStream `WindowOperator`. |

So the difference between the unbounded and windowed paths is **purely the namespace bytes**. The key-extraction logic (`RowDataKeySelector` building the binary key from the SQL `GROUP BY` columns) is identical.

### The `SELECT COUNT(1) FROM table1` corner case

`COUNT(1)` has **no `GROUP BY`**, so `grouping = []`. The `RowDataKeySelector` becomes the trivial "always returns the empty RowData" selector. Every record maps to the same key, and the keyed state has exactly one slot — but it's still a *keyed* state, not a per-operator one. That's why the downstream global agg has parallelism = 1 (`SINGLETON` distribution, see §2's `createDistribution([])` → `SINGLETON()`): only one subtask owns the only key.

```
   Source records (any number)
              │
              ▼  RowDataKeySelector with grouping=[]
              ─ every record produces the same empty RowData key
              │
              ▼
   Local stage:   in-memory bundle keyed by empty RowData → ONE entry per batch,
                  accumulator = count of records in this batch
              │
              ▼  exchange (SINGLETON; all routes converge to one subtask)
              │
              ▼
   Global stage:  ctx.setCurrentKey(EMPTY_ROW)
                  stateAcc = accState.value()       ← ONE state slot, ever
                  globalAgg.merge(bundleAcc)
                  accState.update(stateAcc)
```

### Per-key state lifetime (TTL)

The global stage's `accState` is wired with `StateTtlConfig` from `stateRetentionTime` (`MiniBatchGlobalGroupAggFunction.java:117-136`):

```java
StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
...
ValueStateDescriptor<RowData> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
if (ttlConfig.isEnabled()) {
    accDesc.enableTimeToLive(ttlConfig);
}
accState = ctx.getRuntimeContext().getState(accDesc);
```

The TTL value comes from `table.exec.state.ttl`. A key whose state hasn't been touched within the TTL is eligible for compaction-time cleanup — meaning a brand-new event for an old key starts a fresh accumulator. This is how "remove inactive keys" works without an explicit `DELETE`. The local stage has no state, so no TTL applies there.

For windowed aggregates, TTL is unused — the operator clears the window's state explicitly via `windowProcessor.clearWindow(...)` (`WindowAggOperator.java:262`) when the cleanup timer fires.

---

## 7. Runtime operators

### `MiniBatchLocalGroupAggFunction` — pure in-memory bundling

`MiniBatchLocalGroupAggFunction.java:37-101`. Extends `MapBundleFunction<RowData, RowData, RowData, RowData>`. **Has no state.** The bundle buffer (an in-memory `Map<RowData, RowData>` of key→accumulator) is the only storage.

```java
@Override
public RowData addInput(@Nullable RowData previousAcc, RowData input) throws Exception {
    RowData currentAcc = (previousAcc == null) ? function.createAccumulators() : previousAcc;
    function.setAccumulators(currentAcc);
    if (isAccumulateMsg(input)) {
        function.accumulate(input);
    } else {
        function.retract(input);
    }
    return function.getAccumulators();
}

@Override
public void finishBundle(Map<RowData, RowData> buffer, Collector<RowData> out) {
    for (Map.Entry<RowData, RowData> entry : buffer.entrySet()) {
        resultRow.replace(entry.getKey(), entry.getValue());
        out.collect(resultRow);
    }
    buffer.clear();
}
```

Why state-free: this runs **upstream of the shuffle**, where keys aren't guaranteed to land in the same subtask across batches. State here would be useless — a key seen in batch A might not return in batch B if the upstream re-partitions. So the local agg only deduplicates *within* a mini-batch.

`StreamExecLocalGroupAggregate.translateToPlanInternal(...)` (`StreamExecLocalGroupAggregate.java:132-185`) is what wires the runtime function into a `MapBundleOperator`:

```java
final AggsHandlerCodeGenerator generator = ...;
generator.needAccumulate().needMerge(0, true, null);
if (needRetraction) generator.needRetract();

final GeneratedAggsHandleFunction aggsHandler =
        generator.generateAggsHandler("GroupAggsHandler", aggInfoList);
final MiniBatchLocalGroupAggFunction aggFunction =
        new MiniBatchLocalGroupAggFunction(aggsHandler);
...
final MapBundleOperator<RowData, RowData, RowData, RowData> operator =
        new MapBundleOperator<>(
                aggFunction, MinibatchUtil.createMiniBatchTrigger(config), selector);
```

### `MiniBatchGlobalGroupAggFunction` — bundle + keyed `ValueState`

`MiniBatchGlobalGroupAggFunction.java:45-242`. Same `MapBundleFunction` base, but **with keyed state**:

```java
// from constructor:
private transient ValueState<RowData> accState = null;
// ...
private final GeneratedAggsHandleFunction genLocalAggsHandler;   // re-merges the bundle
private final GeneratedAggsHandleFunction genGlobalAggsHandler;  // merges bundle result into state
```

Inside a mini-batch the bundle is folded by the local handler first (just like the upstream local stage). Then on `finishBundle`, for each key the global handler merges that bundle's accumulator into the keyed state. Retraction is supported via a `RecordCounter` tied to a `COUNT(*)` index — needed when the source emits `UPDATE_BEFORE` / `DELETE`.

This is also where the **state TTL** is wired up — `createTtlConfig(stateRetentionTime)` is applied to the `ValueStateDescriptor`, so `table.exec.state.ttl` propagates here.

---

## 8. Retraction flow (and why "late" doesn't apply to unbounded GroupAggregate)

For windowed aggregates "late" has a precise meaning (event-time past the watermark, §3). For **unbounded** aggregates there's no such concept — but there *is* a related mechanism: **retraction**. When the upstream operator (a CDC source, a join, a deduplicate, an update-by-row) emits `UPDATE_BEFORE` or `DELETE` rows, the unbounded global aggregate must undo a previously-accumulated contribution. This section traces how that flows through Local + Global.

### `needRetraction` is decided at plan time

In `TwoStageOptimizedAggregateRule.matchesTwoStage` (`TwoStageOptimizedAggregateRule.java:96-117`):

```java
boolean needRetraction = !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) realInput);
boolean[] needRetractionArray =
        AggregateUtil.deriveAggCallNeedRetractions(
                agg.grouping().length, agg.aggCalls(), needRetraction, monotonicity);
```

If the upstream is **insert-only**, both stages are generated without retract handlers and the codegen produces leaner accumulators. If the upstream is a changelog, `needRetract()` is enabled on the `AggsHandlerCodeGenerator` and both stages produce `retract()` methods alongside `accumulate()`.

### Local stage — bundle-local accumulate vs retract

`MiniBatchLocalGroupAggFunction.addInput` (`MiniBatchLocalGroupAggFunction.java:65-81`):

```java
public RowData addInput(@Nullable RowData previousAcc, RowData input) throws Exception {
    RowData currentAcc = (previousAcc == null) ? function.createAccumulators() : previousAcc;
    function.setAccumulators(currentAcc);
    if (isAccumulateMsg(input)) {
        function.accumulate(input);   // +I / +U
    } else {
        function.retract(input);      // -U / -D
    }
    return function.getAccumulators();
}
```

The bundle's in-memory accumulator can absorb both +/-, so within one mini-batch a `+I` followed by a `-D` of the same row contributes nothing downstream. This is "free deduplication" — the more retraction churn the upstream produces, the more the bundle absorbs before sending anything to the global stage.

### Global stage — `RecordCounter` and the `DELETE` path

`MiniBatchGlobalGroupAggFunction.finishBundle` (`MiniBatchGlobalGroupAggFunction.java:161-231`) is the interesting code:

```java
for (Map.Entry<RowData, RowData> entry : buffer.entrySet()) {
    RowData currentKey = entry.getKey();
    RowData bufferAcc  = entry.getValue();

    ctx.setCurrentKey(currentKey);

    RowData stateAcc = accState.value();
    boolean firstRow = (stateAcc == null);
    if (firstRow) stateAcc = globalAgg.createAccumulators();
    globalAgg.setAccumulators(stateAcc);

    RowData prevAggValue = globalAgg.getValue();
    globalAgg.merge(bufferAcc);
    RowData newAggValue  = globalAgg.getValue();
    stateAcc             = globalAgg.getAccumulators();

    if (!recordCounter.recordCountIsZero(stateAcc)) {
        // Still have contributing rows → UPDATE
        accState.update(stateAcc);
        if (!firstRow && !equaliser.equals(prevAggValue, newAggValue)) {
            if (generateUpdateBefore) emit (-U, prevAggValue);
            emit (+U, newAggValue);
        } else if (firstRow) {
            emit (+I, newAggValue);
        }
        // if !firstRow && equal, suppress no-op
    } else {
        // Every contributing row has been retracted → DELETE + clear state
        if (!firstRow) emit (-D, prevAggValue);
        accState.clear();
        globalAgg.cleanup();
    }
}
```

Four mechanisms wired together:

1. **`RecordCounter`** (the operator constructor argument `indexOfCountStar`). When `needRetraction` is true, the planner injects a hidden `COUNT(*)` aggregate so the operator can detect "this key has had every contributing row retracted." When `recordCountIsZero(stateAcc)` returns true, the operator emits a `DELETE` and wipes the keyed state.
2. **`equaliser`** — suppresses no-op updates. If retracting one row didn't change the answer (e.g., retracting a non-max from a `MAX`), no `-U`/`+U` is emitted.
3. **`generateUpdateBefore`** — chosen at plan time based on the downstream sink's `ChangelogMode`. Upserts can request `+U` only; full changelog sinks get both `-U` and `+U`.
4. **`accState.clear()` + `globalAgg.cleanup()`** — when the last contributing row is retracted, both the keyed state and the per-key DataView state (for distinct values, e.g.) are erased. This is what stops a long-tail of dead keys from growing the RocksDB footprint indefinitely.

### Worked example

Suppose the unbounded query is `SELECT user_id, SUM(amount) FROM payments GROUP BY user_id`, and the upstream is a CDC source.

```
Time  Upstream row                          Local stage          Global stage
────  ─────────────                         ───────────          ────────────
t1    +I (user=A, amount=10)                bundle: A→acc(10)    (still bundling)
t2    +I (user=A, amount=5)                 bundle: A→acc(15)
t3    -U (user=A, amount=10)  ─ retract     bundle: A→acc(5)     (one input row +
t3    +U (user=A, amount=7)   ─ accumulate  bundle: A→acc(12)     one update = 12)

      [mini-batch fires]
                                            emit (A, acc(12)) ──► merge into accState
                                                                  prev: NULL → first
                                                                  new : 12
                                                                  recordCount > 0
                                                                  emit +I (A, 12)

t4    -D (user=A, amount=12)  ─ retract     bundle: A→acc(-12)   (subtract 12)

      [mini-batch fires]
                                            emit (A, acc(-12)) ──► merge into accState
                                                                  prev: 12
                                                                  new : 0
                                                                  recordCount == 0
                                                                  emit -D (A, 12)
                                                                  accState.clear()
```

The `-D` emitted at `t4` carries the **previous** aggregated value (`12`), not the retracting record's value. This is critical for downstream sinks that maintain a materialized view — they need to know "the row that was previously `(A, 12)` is now gone," not just "subtract 12."

### Late-arriving DELETE (a subtle case)

If a `-D` for `user=A` arrives long after `accState.clear()` has run for `A` (e.g., upstream lag, retry, reprocessing), the global stage will see `prevAggValue = NULL` (because state is empty) — but the retract path adds a negative contribution. With `state.ttl` enabled, the operator treats this as a brand-new key. The result can be a "negative count" key sitting in state until next compaction.

This is a **correctness hazard** when:
- TTL is too short relative to upstream retraction lag, and
- The source can emit a `-D` for a row whose corresponding `+I` was already TTL'd out.

The mitigation is to size `table.exec.state.ttl` larger than the maximum possible upstream retraction delay. The Flink docs call this out explicitly for retraction-aware sources.

---

## 9. Config matrix — what you can turn on, what depends on what

| Optimization | Enable | Default | Dependencies | Helps when… |
|---|---|---|---|---|
| **MiniBatch** | `table.exec.mini-batch.enabled = true` <br/> `table.exec.mini-batch.allow-latency = …` <br/> `table.exec.mini-batch.size = …` | OFF | — | Any high-throughput keyed aggregate. Prerequisite for Local-Global. |
| **Local-Global** | `table.optimizer.agg-phase-strategy = TWO_PHASE` (or `AUTO`) | `AUTO` | **MiniBatch must be enabled.** All aggs must be partial-merge-capable. | Skewed `GROUP BY key`, scalar aggregates (no GROUP BY), any aggregate where upstream parallelism > downstream parallelism. |
| **Split-Distinct** | `table.optimizer.distinct-agg.split.enabled = true` <br/> `table.optimizer.distinct-agg.split.bucket-num = 1024` | OFF | All distinct-args must be splittable built-ins. Not for proctime or session windows. Not for TableAggregate. | `COUNT(DISTINCT col)` / `SUM(DISTINCT col)` with skewed distinct keys, especially when one group key holds most distinct values. |
| **Windowed Local-Global** | `table.optimizer.agg-phase-strategy = TWO_PHASE` (or `AUTO`) | `AUTO` | **Rowtime only** (no proctime). Not for session windows. All aggs must support `merge()`. **Does NOT require mini-batch** — slicing is the batching. | Any rowtime tumbling/hopping/cumulative window TVF aggregate over a hash-shuffled exchange. |
| **Window emit (late-fire)** | `table.exec.emit.late-fire.enabled = true` <br/> `table.exec.emit.late-fire.delay = …` | OFF (Experimental) | Windowed queries only. Source: `WindowEmitStrategy.scala:227-248` | When you want late events within `allow-lateness` to trigger a window re-fire instead of being absorbed silently. |
| **Window emit (allow-lateness)** | `table.exec.emit.allow-lateness = …` | falls back to `IDLE_STATE_RETENTION` | Windowed queries only. Source: `WindowEmitStrategy.scala:254-264` | Tolerate event-time lateness up to this duration; events past this are dropped (`numLateRecordsDropped`). |
| **Window emit (early-fire)** | `table.exec.emit.early-fire.enabled = true` <br/> `table.exec.emit.early-fire.delay = …` | OFF (Experimental) | Windowed queries only. Source: `WindowEmitStrategy.scala:199-220` | When you want incremental results before the window closes (low-latency dashboards, etc.). Each early fire emits `-U` / `+U` pairs. |

A typical "give me everything" config block:

```sql
-- MiniBatch (prerequisite)
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5 s';
SET 'table.exec.mini-batch.size' = '5000';

-- Local-Global
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';

-- Split-Distinct (only if you have COUNT(DISTINCT ...) etc.)
SET 'table.optimizer.distinct-agg.split.enabled' = 'true';
-- 'table.optimizer.distinct-agg.split.bucket-num' defaults to 1024
```

---

## 10. End-to-end planning flow (textual sequence)

For a query like `SELECT key, COUNT(DISTINCT v) FROM t GROUP BY key` with all three optimizations enabled:

```
SQL string
   │ Calcite parse
   ▼
SqlNode (parse tree)
   │ Calcite validate + relational algebra conversion
   ▼
RelNode tree with LogicalAggregate
   │
   │  ─── SplitAggregateRule fires (logical) ────────────────────
   │      Adds a hash-mod projection, an Expand if needed,
   │      a partial Aggregate (group by key + hash%N),
   │      and a final Aggregate (group by key) on top.
   ▼
two stacked FlinkLogicalAggregates  (PartialFinalType = PARTIAL / FINAL)
   │ logical → physical conversion
   ▼
two stacked StreamPhysicalGroupAggregates
   │
   │  ─── TwoStageOptimizedAggregateRule fires (physical) ───────
   │      EACH StreamPhysicalGroupAggregate that sits over an
   │      Exchange gets split into LocalGroupAggregate +
   │      Exchange + GlobalGroupAggregate.
   ▼
StreamPhysicalGlobalGroupAggregate                  ◄── outer/final pair
  +- StreamPhysicalExchange (hash by key)
     +- StreamPhysicalLocalGroupAggregate
        +- StreamPhysicalGlobalGroupAggregate       ◄── inner/partial pair
           +- StreamPhysicalExchange (hash by key, hash%N)
              +- StreamPhysicalLocalGroupAggregate
                 +- (source + hash projection + optional Expand)
   │ each ExecNode's translateToPlanInternal runs
   ▼
MapBundleOperator wrapping
   MiniBatchLocalGroupAggFunction  (upstream of each exchange)
MapBundleOperator wrapping
   MiniBatchGlobalGroupAggFunction (downstream of each exchange)
```

That's the full path from SQL string to deployed `MapBundleOperator`s. Each layer's responsibility:

| Layer | Class / file | What it does |
|---|---|---|
| Logical rule (distinct split) | `SplitAggregateRule.java` | Rewrites `DISTINCT` into a two-level aggregate with a hash-bucket key. |
| Physical rule (phase split) | `TwoStageOptimizedAggregateRule.java` | Splits each `StreamPhysicalGroupAggregate` over an `Exchange` into Local + Global. |
| Local ExecNode | `StreamExecLocalGroupAggregate.java` | Generates an `AggsHandleFunction` with `needMerge(0, true, null)` and wires `MiniBatchLocalGroupAggFunction` into a `MapBundleOperator`. |
| Global ExecNode | `StreamExecGlobalGroupAggregate.java` | Generates two `AggsHandleFunction`s (local re-merge + global) and wires `MiniBatchGlobalGroupAggFunction` (which holds the keyed `ValueState<RowData>`). |
| Local runtime | `MiniBatchLocalGroupAggFunction.java:37-101` | In-memory bundle. No state. Emits one row per (key) per batch. |
| Global runtime | `MiniBatchGlobalGroupAggFunction.java:45-242` | Bundle + keyed `ValueState<RowData>`. Supports retraction via `RecordCounter`. |

---

## 11. Compared to a hand-written DataStream version: `SkewedAggregation.java`

This repo ships an example that hand-writes a two-phase skew-handling pipeline at the **DataStream** level: `flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/SkewedAggregation.java`. It's instructive to compare it side-by-side with Table-API Local-Global because it exposes exactly which machinery the planner is automating.

The example offers two approaches, both for a *windowed* count/sum/avg over a hot URL (`google.com` = 60% of traffic):

| Approach | What it does |
|---|---|
| **Approach 1: Two-phase `keyBy`** | Salts the key with `subtaskId` for Phase 1 (`keyBy(url + "#" + subtaskId)`), aggregates per salted key in a tumbling window, then `keyBy(url)` + window + merge for Phase 2. |
| **Approach 2: Custom partitioner** | `partitionCustom(HotKeyPartitioner, url)` — routes hot keys round-robin, others by hash. Accumulates in a transient `HashMap` (not checkpointed), flushes to Phase 2 manually. |

### Mapping to Table API concepts

```
   SkewedAggregation.java                              Table API equivalent
   ─────────────────────                              ────────────────────

   Approach 1 Phase 1:                                Local stage of windowed Local-Global
     keyBy(url + "#" + subtaskId)                       (TwoStageOptimizedWindowAggregateRule
     .window(Tumble 1 min)                              splits StreamPhysicalWindowAggregate
     .aggregate(LocalAggregateFunction)                 into StreamPhysicalLocalWindowAggregate
                                                        + StreamPhysicalGlobalWindowAggregate)
   Approach 1 Phase 2:
     keyBy(url)                                       Global stage of windowed Local-Global
     .window(Tumble 1 min)                              (WindowAggOperator)
     .process(MergePartialsFunction)

   Approach 2:                                        ≈ Local stage of unbounded Local-Global
     partitionCustom(HotKeyPartitioner, url)            (MiniBatchLocalGroupAggFunction's
     .flatMap(LocalAccumulatorFunction)                  in-memory bundle has the same
     // accumulates in a transient HashMap               "no keyed state, no checkpoint"
                                                         property — but it's safe because
                                                         the bundle is flushed before every
                                                         checkpoint barrier)
```

Approach 1 corresponds closely to Table API's **windowed** Local-Global (the §3 path). Approach 2 corresponds — at first glance — to the **unbounded** local stage's in-memory bundle (the §2 path), but the safety properties are very different (see gap #9 below).

### What's missing in `SkewedAggregation.java` compared to Table-API Local-Global

| # | Gap | Where it shows up in the example | What Table API does instead |
|---|---|---|---|
| 1 | **No mini-batch bundling.** Phase 1 reads/writes RocksDB state for every record because it's a windowed `aggregate(...)`. | `LocalAggregateFunction.add(...)` runs per record; the `Acc` is stored as keyed window state. | The unbounded local stage uses an **in-memory bundle** (`MapBundleFunction`) with no keyed state at all — only emits one row per `(key, batch)`. |
| 2 | **Salting is parallelism-coupled.** The `subtaskId` salt ties the partition count to the runtime parallelism; rescaling shifts which subtask holds which "shard." | Phase 1 key = `url + "#" + subtaskId` in `RichMapFunction.open`. | For non-distinct local-global, **Table API doesn't salt at all** — the local stage just bundles whatever records the upstream operator chain delivers. For `COUNT(DISTINCT)`, Split-Distinct salts with `hash_code(col) % bucket_num` (configurable, decoupled from parallelism). |
| 3 | **Phase 1 holds state proportional to `\|keys\| × parallelism`.** Each (url, subtaskId) pair owns a keyed window accumulator. | `LocalAggregateFunction` is a windowed `AggregateFunction` — Flink stores one `Acc` per salted key per window. | Unbounded local stage holds zero keyed state. Windowed local stage stores per-slice partial accumulators only until the slice closes. |
| 4 | **No retraction support.** Both phases assume insert-only input. | `LocalAggregateFunction.add` just increments; no `retract` path. `MergePartialsFunction` only sums positives. | `MiniBatchGlobalGroupAggFunction.finishBundle` handles `accumulate` + `retract` with a hidden `RecordCounter`; emits `-U`/`+U`/`-D` (§8). |
| 5 | **No no-op suppression on the global side.** Every window fire emits a row, even if the result didn't change. | `MergePartialsFunction.process` always calls `out.collect(...)`. | `MiniBatchGlobalGroupAggFunction` uses an `equaliser` to skip emissions when `newAggValue.equals(prevAggValue)` (§8). |
| 6 | **`ProcessWindowFunction` in Phase 2 buffers all elements via `Iterable`.** | `MergePartialsFunction extends ProcessWindowFunction` — receives `Iterable<Tuple4>`. Manageable here because the iterable is bounded by parallelism, but heavier than incremental aggregation. | Table API's Global stage is **incremental**: each input is merged into the keyed accumulator one at a time via `globalAgg.merge(input)`. No intermediate list. |
| 7 | **Manual dedup-by-subtaskId.** Phase 2 uses a `HashMap<Integer, long[]>` to dedupe partials by subtask for the re-fire case (allowed-lateness). | `MergePartialsFunction.process` builds `Map<Integer, long[]> shardPartials`. | Not needed in Table API: the global stage merges idempotently because each local emission is keyed by `(key, slice_end)` and slices are non-overlapping. |
| 8 | **Hardcoded tumbling windowing.** Cannot do unbounded `GROUP BY` without rewriting. | Both phases use `TumblingEventTimeWindows.of(Duration.ofMinutes(1))`. | Table API supports unbounded `GROUP BY` (§2) and all three windowing modes (tumble/hop/cumulate) via the rule (§3). |
| 9 | **Approach 2's HashMap is *not* checkpointed.** A failure between window-boundary flushes loses accumulated records. The example's own JavaDoc calls this out as a known limitation. | `LocalAccumulatorFunction.localBuffer` is `transient Map<String, long[]>`. `close()` cannot flush (no `Collector` available). | The unbounded local stage's bundle IS safe across failure: `MapBundleOperator` flushes via `prepareSnapshotPreBarrier` before every checkpoint, so the bundle is always empty at the barrier point — there's nothing to lose. |
| 10 | **No automatic enablement.** Skew handling requires manually choosing one of two approaches and wiring it. | `main` toggles between `twoPhaseKeyBy(events)` and `customPartitioner(events)`. | Set three configs (`mini-batch.enabled`, `mini-batch.allow-latency`, `agg-phase-strategy=TWO_PHASE`) and the planner does the rewrite automatically across every aggregate in every SQL query. |
| 11 | **No state TTL.** The example relies on window cleanup. For an unbounded variant, the user would have to roll their own. | Window state is cleared at window end. No alternative for unbounded. | `MiniBatchGlobalGroupAggFunction` wires `StateTtlConfig` from `table.exec.state.ttl` onto the accState `ValueStateDescriptor` (§6). |
| 12 | **Approach 2 must know hot keys in advance.** The partitioner's hot-key set is a constructor argument. | `HotKeyPartitioner(Set.of("google.com"))`. | The planner's optimizations are key-agnostic — they spread *all* keys, not just known-hot ones. |

### What `SkewedAggregation.java` does well

It's not a strawman — the example demonstrates a real-world DataStream pattern with explicit trade-offs:

- The dedup-by-subtaskId pattern in Phase 2 is *correct* and necessary when allowed-lateness causes window re-fires. The example explicitly mentions this and links to `TwoPhaseCountDeduplicatedEventTimeV2`.
- The `LocalAggregateFunction` is an incremental `AggregateFunction` (not a `ProcessWindowFunction`) so memory is bounded by `O(salted_keys)` not `O(records)`.
- Approach 2's caveats are documented in its own JavaDoc, including the unchecked-pointed HashMap.

The gap list above is about **automation and safety**, not correctness — both approaches in the example produce correct results when run as-is.

### The shortest possible "this is the Table API equivalent"

If the same workload were written with Table API and the configs from §9 were enabled, the entire pipeline is:

```sql
CREATE TEMPORARY VIEW page_views AS
  SELECT
    url,
    response_time_ms,
    event_time
  FROM source_table;

-- skew handling: just turn on the configs
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5 s';
SET 'table.exec.mini-batch.size' = '5000';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';

SELECT
    url,
    window_start,
    window_end,
    COUNT(*)               AS cnt,
    SUM(response_time_ms)  AS total_rt,
    AVG(response_time_ms)  AS avg_rt
FROM TABLE(
    TUMBLE(TABLE page_views, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY url, window_start, window_end;
```

That's ~10 lines of SQL replacing ~300 lines of hand-written DataStream code, with retraction support, no-op suppression, state TTL, automatic rescaling, and a checkpoint-safe local bundle — none of which the example has.

The trade-off is *flexibility*: the example's Approach 2 (custom partitioner) is the one place where DataStream beats SQL, because the partitioner can encode workload-specific routing (GPU subtasks, co-location, external partition assignment) that the planner can't infer. For pure skew distribution, the planner wins.

---

## 12. Compared to a hand-written event-time two-phase dedup: `TwoPhaseCountDeduplicatedEventTimeV2.java`

A second DataStream example in this repo focuses on something `SkewedAggregation.java` doesn't: **how to correctly handle late-data re-fires** when doing two-phase aggregation. File: `flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/TwoPhaseCountDeduplicatedEventTimeV2.java`.

Where `SkewedAggregation.java` shows *parallelism splitting* by hand, V2 focuses on *event-time correctness*: how a windowed two-phase pipeline copes with late events that arrive *after the window has already fired its initial result*.

### What V2 does

The pipeline is two windowed phases connected by a key-by:

```
   ┌───────────────────────────────────────────────────────────────────────────┐
   │ Phase 1: LOCAL count (pre-aggregation per (key, subtask))                 │
   └───────────────────────────────────────────────────────────────────────────┘
       input
         │ map: tag each event with subtaskIdx
         ▼
       Tuple3<key, subtaskIdx, value>
         │ keyBy(key + "_" + subtaskIdx)   ◄── SALT by subtaskIdx
         ▼
       Tumbling event-time window (1 min)
         + allowedLateness(30s)
         + sideOutputLateData(LOCAL_LATE_TAG)   ◄── side-output for too-late events
         │
         │ aggregate(LocalCountAggregate)  ◄── incremental AggregateFunction
         ▼
       Tuple3<key, subtaskIdx, localCount>

   ┌───────────────────────────────────────────────────────────────────────────┐
   │ Phase 2: GLOBAL dedup + total                                             │
   └───────────────────────────────────────────────────────────────────────────┘
         │ keyBy(key)
         ▼
       Tumbling event-time window (1 min)
         + allowedLateness(30s)
         + sideOutputLateData(GLOBAL_LATE_TAG)
         │
         │ process(GlobalDeduplicatedCountFunction)  ◄── ProcessWindowFunction
         ▼
       Tuple4<key, windowStart, windowEnd, totalCount>
```

The critical detail is the **dedup inside `GlobalDeduplicatedCountFunction`** (`TwoPhaseCountDeduplicatedEventTimeV2.java:210-294`):

```java
Map<Integer, Long> subtaskCounts = new HashMap<>();
for (Tuple3<String, Integer, Long> element : elements) {
    subtaskCounts.put(element.f1, element.f2);   // overwrite stale count
}
long totalCount = subtaskCounts.values().stream().mapToLong(Long::longValue).sum();
```

This works *only* because the global window operator uses default **ACCUMULATING** semantics (no `PurgingTrigger`): every re-fire's `Iterable<...>` still contains **all** prior partial counts plus the new one. The `Map.put(subtaskIdx, count)` then overwrites the earlier partial with the later one — assuming FIFO iteration order of the underlying `ListState`.

The example's own JavaDoc flags this dependency as a risk: *"FIFO ordering of `ListState.get()` is an implementation detail, not a documented API contract."* It also suggests a safer alternative: `subtaskCounts.merge(idx, count, Math::max)` — which is iteration-order-independent because re-fires only ever increase counts.

### Why V2 needs the dedup at all

This is the heart of the comparison. The dedup exists because V2 chose to use **windowed `aggregate(...)` + `ProcessWindowFunction`**, with ACCUMULATING semantics, and to identify each contribution by `subtaskIdx`. The pattern is:

1. Phase 1 fires for window `[0, 60000)` at watermark = 65000 → emits `(group1, subtask0, 3)`.
2. Phase 2 receives that and adds it to its own window state for `[0, 60000)`.
3. Phase 2 fires → `elements = [(group1, subtask0, 3)]` → total = 3.
4. Late event arrives for window `[0, 60000)`.
5. Phase 1's `[0, 60000)` window re-fires with the updated count → emits `(group1, subtask0, 4)` (NOT `(group1, subtask0, +1)` — it's an absolute count, not a delta).
6. Phase 2's `[0, 60000)` window now has **both** `(subtask0, 3)` and `(subtask0, 4)` in its `ListState` — that's why dedup is required.

In short: V2's local stage emits **absolute counts**, not deltas. Re-fires emit superseding values, not increments. Without dedup, the global stage would sum `3 + 4 = 7` for what should have been `4`.

### Table API's windowed Local-Global avoids this by construction

Now compare to the §3 path (the windowed two-stage rule):

| V2's mechanism | Table API equivalent |
|---|---|
| `keyBy(key + "_" + subtaskIdx)` to spread the work | **No salting at all.** The local stage just bundles records that naturally land in the same task. Skew within a single subtask is reduced by the bundle's `merge()` calls; cross-subtask skew is handled by the global stage receiving fewer, smaller updates. |
| `LocalCountAggregate` is invoked once per record; stored in keyed window state per `(key+subtaskId, window)` | Pre-aggregation is done **per slice** (`slice = gcd(size, slide)` — for tumbling, slice = window). The local stage emits one row per `(key, slice_end)` — never the same slice twice. |
| Phase-2 must dedup using `Map<subtaskIdx, latestCount>` | **No dedup.** Each local emission is for a distinct slice; the global stage merges by adding the slice's partial accumulator to the (key, window) state. |
| ACCUMULATING + ListState FIFO order is a hidden contract | The global stage uses `globalAgg.merge(bundleAcc)` — an explicit algebraic merge defined by the aggregate function. Order independent. |
| Late event triggers Phase-1 re-fire → emits **superseding absolute count** | Late event triggers re-emission as `-U` / `+U` (retraction + new value) — **proper changelog semantics**, not absolute-value supersede. |

The key insight: V2 reinvents two-phase aggregation by storing partials in window state and recomputing the total on every fire. Table API does it by **never storing partials** — each slice is consumed by the global stage exactly once, and the global stage holds the running window accumulator directly.

### How late data is supported in windowed Table API

V2 uses two DataStream primitives that Table API expresses differently:

| DataStream (V2) | Table API equivalent | Behavior |
|---|---|---|
| `.allowedLateness(Duration.ofSeconds(30))` | `table.exec.emit.allow-lateness = '30 s'` | Events arriving within 30s of watermark passing window-end are absorbed into the (still-open) window state. The window cleanup timer is `window.maxTimestamp() + allowedLateness`. |
| (default — no late re-fire) | `table.exec.emit.late-fire.enabled = true` <br/> `table.exec.emit.late-fire.delay = '0 ms'` | Each late event (within allow-lateness) immediately triggers a re-fire that emits a `-U` / `+U` retraction pair for the previous window result. Delay > 0 batches re-fires. |
| `.sideOutputLateData(LOCAL_LATE_TAG)` | **Not exposed in Table API.** Late events past `allow-lateness` are dropped silently; only the metric `numLateRecordsDropped` reflects them. | The drop happens in `WindowAggOperator.processElement` (lines 215-224) — the `windowProcessor.processElement(...)` returns `true` for too-late records, which then increments `lateRecordsDroppedRate`. |

So Table API offers:
- **Absorb late within `allow-lateness`** — yes, identical to V2's `allowedLateness`.
- **Trigger re-fire on late within `allow-lateness`** — yes, via `late-fire.enabled`, but emitted as proper changelog (`-U`/`+U`) instead of "emit a new absolute count and trust downstream to dedup."
- **Side-output for events past `allow-lateness`** — **no SQL/Table-API exposure.** This is the one capability lost in the migration. If you need it, you fall back to DataStream.

The retraction model has an important downstream implication: downstream sinks must support upsert semantics (or be able to apply `-U` retractions). V2's pattern produces only inserts (the global ProcessWindowFunction emits one row per fire), which is easier on append-only sinks but trades correctness for naïve consumers.

### V4 — implementing the same task with the Table API

> **Note:** The "V3" slot is taken by `TwoPhaseCountDeduplicatedEventTimeV3.java`, which is a *DataStream-level* optimization (a non-keyed `LocalPreAggregator` `ProcessFunction` with operator `ListState` that eliminates the local-phase shuffle entirely). V3 keeps the same V2-style dedup map; it does not use the Table API. The Table-API equivalent is implemented as **`TwoPhaseCountDeduplicatedEventTimeV4`** in the same package — that's the file the code block below refers to.

Here's what a Table-API equivalent of V2 looks like (programmatic Java DSL, not SQL string). Note that V3 doesn't need the `subtaskIdx` salting or the `Map`-based dedup — the planner takes care of two-phase aggregation, slice-based pre-aggregation, and changelog-based late-fire automatically.

```java
package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Table-API equivalent of {@link TwoPhaseCountDeduplicatedEventTimeV2}.
 *
 * <p>Demonstrates how the planner replaces the hand-written two-phase + dedup
 * pipeline with declarative configuration:
 * <ul>
 *   <li>Local-Global is enabled via {@code agg-phase-strategy=TWO_PHASE} —
 *       the planner inserts a {@code LocalWindowAggregate} + exchange +
 *       {@code GlobalWindowAggregate} pair automatically.
 *   <li>Late data is handled via {@code emit.allow-lateness} and (optionally)
 *       {@code emit.late-fire.enabled} — produces proper {@code -U}/{@code +U}
 *       changelog instead of dedup-on-the-consumer-side.
 *   <li>No salting / no Map-based dedup needed. Each slice is emitted exactly
 *       once and merged algebraically into the global window accumulator.
 * </ul>
 */
public class TwoPhaseCountDeduplicatedEventTimeV4 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // ────────────────────────────────────────────────────────────────────
        // Enable Local-Global + windowed late-data semantics on the Table env
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
        // Source — reuse V2's DelayedEventsSource shape
        //   Tuple3<key, value, eventTimestamp>
        // ────────────────────────────────────────────────────────────────────
        DataStream<Tuple3<String, String, Long>> rawInput =
                env.addSource(
                        new TwoPhaseCountDeduplicatedEventTimeV2.DelayedEventsSource(),
                        TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {}));

        DataStream<Tuple3<String, String, Long>> input =
                rawInput.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.f2)
                                .withIdleness(Duration.ofMinutes(1)));

        // ────────────────────────────────────────────────────────────────────
        // Bridge DataStream → Table with an event-time column + watermark
        // ────────────────────────────────────────────────────────────────────
        Table inputTable = tEnv.fromDataStream(
                input,
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())     // key
                        .column("f1", DataTypes.STRING())     // value
                        .column("f2", DataTypes.BIGINT())     // raw timestamp millis
                        .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(f2, 3)")
                        .watermark("event_time", "event_time - INTERVAL '5' SECOND")
                        .build());

        // ────────────────────────────────────────────────────────────────────
        // The whole pipeline: window + group-by + count.
        //
        //   Planner output:
        //     StreamPhysicalGlobalWindowAggregate
        //       +- Exchange (hash by f0)
        //          +- StreamPhysicalLocalWindowAggregate
        //             +- source
        //
        //   No salting, no dedup, no manual subtaskIdx tagging.
        // ────────────────────────────────────────────────────────────────────
        Table result = inputTable
                .window(Tumble.over(lit(1).minutes()).on($("event_time")).as("w"))
                .groupBy($("f0"), $("w"))
                .select(
                        $("f0").as("key"),
                        $("w").start().as("window_start"),
                        $("w").end().as("window_end"),
                        $("f0").count().as("count"));

        // ────────────────────────────────────────────────────────────────────
        // Result stream contains a changelog (insert + retract on late-fire).
        // toChangelogStream surfaces the -U/+U pairs that the planner emits.
        // ────────────────────────────────────────────────────────────────────
        DataStream<Row> resultStream = tEnv.toChangelogStream(result);
        resultStream.print("RESULT");

        env.execute("Two-Phase Count Deduplicated V3 (Table API)");
    }
}
```

### What V4 demonstrates vs V2

| Concern | V2 (DataStream, hand-written) | V4 (Table API DSL) |
|---|---|---|
| Lines of code | ~350 | ~80 |
| Salting key with subtaskIdx | Required (`keyBy(key + "_" + subtaskIdx)`) | Not needed — planner handles parallelism via the exchange |
| Local-stage operator | `aggregate(new LocalCountAggregate())` on a windowed keyed stream → stores acc in keyed window state | `StreamPhysicalLocalWindowAggregate` → emits one row per `(key, slice_end)`; in-memory slice accumulator |
| Global-stage operator | `process(new GlobalDeduplicatedCountFunction())` with `Map<Integer, Long>` dedup | `StreamPhysicalGlobalWindowAggregate` (`WindowAggOperator`) — incremental algebraic merge, no dedup |
| Late-data handling | `.allowedLateness(30s)` + `.sideOutputLateData(tag)` | `table.exec.emit.allow-lateness=30s` + (optional) `table.exec.emit.late-fire.enabled=true` |
| Late-fire emission style | Re-fire emits **absolute count** — consumer must dedup | Re-fire emits `-U` (previous result) + `+U` (new result) — consumer applies retract |
| Side output for past-lateness | `LOCAL_LATE_TAG`, `GLOBAL_LATE_TAG` (two outputs) | **Not exposed.** Dropped silently; counted by `numLateRecordsDropped` metric |
| Retraction-aware downstream sink? | Not required (only inserts) | Required (or upsert sink) |
| ListState-FIFO-order dependency | Yes (the `Map.put` overwrite pattern) | No |
| Code clarity | Has to reason about ACCUMULATING vs PURGING, FIFO order, re-fire semantics | All handled by the planner |

### Could you wire `MiniBatchLocalGroupAggFunction` directly?

A natural follow-up question: *"can V4 manually wire the same runtime classes the planner uses (`MiniBatchLocalGroupAggFunction`, `MiniBatchGlobalGroupAggFunction`, `MapBundleOperator`) as DataStream operators, bypassing SQL/Table-API entirely?"*

Technically yes, but it requires **codegen** — these runtime functions take a `GeneratedAggsHandleFunction`, which is produced by `AggsHandlerCodeGenerator`. That generator is parameterized by an `AggregateInfoList` derived from logical `AggregateCall` objects, which is what the planner produces from SQL. To use the runtime classes from raw user code, you'd have to:

1. Construct a logical `AggregateCall` (or manually write the equivalent generated Java class).
2. Set up an `AggsHandlerCodeGenerator` with the right `CodeGeneratorContext` and `RelBuilder`.
3. Invoke `generator.needAccumulate().needMerge(0, true, null)` to mark which methods to generate.
4. Build a `MapBundleOperator` with the generated function + `RowDataKeySelector`.
5. Manage `RowData` conversions on the input/output edges.

The result is essentially "implement `StreamExecLocalGroupAggregate.translateToPlanInternal` by hand" — about 100 lines of boilerplate per stage. Useful as a learning exercise; not how you'd ship a feature. The Table API DSL version above is the practical answer.

---

## 13. Inside the operators: `TwoPhaseCountDeduplicatedEventTimeV5`

V4 shows the Table API doing windowed Local-Global *automatically*. V5 (`flink-examples-streaming/.../TwoPhaseCountDeduplicatedEventTimeV5.java`) implements the same task using only the DataStream API plus the Table runtime's `AggsHandleFunction` interface — letting you read the two operators side-by-side with the codegen-equivalent aggregate logic.

The implementation is split across six files in the same package, all suffixed `V5` for clarity:

| File | Role | Mirrors |
|---|---|---|
| `TwoPhaseCountDeduplicatedEventTimeV5` | `main()` + pipeline wiring | — |
| `EventToRowDataMapV5` | `Tuple3` → `RowData[key]` | What the planner's `Calc` ExecNode does above a `TableSourceScan` |
| `KeyExtractorV5` | `RowData → String` key | `RowDataKeySelector` |
| `CountStarAggsHandleFunctionV5` | Aggregation arithmetic for `COUNT(*)` | What `AggsHandlerCodeGenerator` emits + Janino compiles for this SQL |
| `LocalWindowAggregateOperatorV5` | Non-keyed, in-memory pre-aggregator | `LocalSlicingWindowAggOperator` |
| `GlobalWindowAggregateFunctionV5` | Keyed final aggregator with changelog output | `WindowAggOperator` |

### The structural property — `windowStart` is preserved end-to-end

The thing that makes late-data handling correct is that **the local stage tags each emitted partial with the `windowStart` it computed from the event's original timestamp**, and the global stage uses that exact value as its `MapState` key. A late event for window `[0, 60000)` therefore always updates `accState[0]`, never bleeds into the next window.

```
   Local stage (LocalWindowAggregateOperatorV5)
   ──────────────────────────────────────────
   processElement(row):
     bundleKey   = keySelector.getKey(row)
     windowStart = ts - ts % windowSize        ◄── computed from EVENT time
     buffer[bundleKey][windowStart] ← accumulate(row)

   emitPartial(bundleKey, windowStart, acc):
     row = (bundleKey, windowStart, getValue)  ◄── windowStart carried forward
     output.collect(row)


   Global stage (GlobalWindowAggregateFunctionV5)
   ──────────────────────────────────────────────
   processElement(partial):
     windowStart = partial.getLong(1)           ◄── read back from the partial
     accState[windowStart] ← merge(partial)     ◄── per-window state
```

### Late-data path — worked trace

Window size = 60s, allow-lateness = 30s, key = `"A"`.

| Step | What happens | Local-stage state | Global-stage state | Output |
|---|---|---|---|---|
| 1 | event `(A, ts=10s)` | `buffer[A][0].count = 1` | — | — |
| 2 | event `(A, ts=20s)` | `buffer[A][0].count = 2` | — | — |
| 3 | event `(A, ts=70s)` | `buffer[A][0].count = 2`, `buffer[A][60000].count = 1` | — | — |
| 4 | watermark → 65s | local: `flushCompletedWindows(65s)` — `windowEnd=60000 ≤ 65000` for `windowStart=0` → emit partial(A, 0, 2), drop `buffer[A][0]` | accState[0] = 2, register timer @ 60000 | — |
| 5 | (watermark forwarded) | — | wm crosses 60000 → fire timer → emit `+I[A, ws=0, count=2]`, `firedState[0] = true`, register cleanup @ 90000 | `+I[A, ws=0, count=2]` |
| 6 | event `(A, ts=15s)` ◄ LATE | `windowStart = 15000 - 15000%60000 = 0` (original window!). `buffer[A][0]` was dropped at step 4 → create new acc, count=1 | — | — |
| 7 | watermark → 75s | local: flush `buffer[A][0]` (end=60000 ≤ 75000) → emit partial(A, 0, 1) | merge → accState[0] = 3. `firedState[0]` is true → re-fire → emit `-U[A, ws=0, count=2]`, `+U[A, ws=0, count=3]` | `-U[A, ws=0, count=2]`<br>`+U[A, ws=0, count=3]` |
| 8 | watermark → 95s | local: nothing to flush | wm crosses 90000 → cleanup timer fires for `windowStart=0` → `accState.remove(0)`, `firedState.remove(0)`, `lastEmittedState.remove(0)` | — |
| 9 | event `(A, ts=5s)` ◄ TOO LATE | `windowStart = 0`, `buffer[A][0].count = 1` (fresh) | — | — |
| 10 | watermark → 100s | local: emit partial(A, 0, 1) | `currentWatermark=100000 ≥ cleanupTime=90000` → DROP. No output. | — |

Final output stream:

```
+I[A, windowStart=0, count=2]
-U[A, windowStart=0, count=2]
+U[A, windowStart=0, count=3]
```

The late event at step 6 was correctly attributed to `windowStart=0` (not `windowStart=60000`), and the global stage emitted a proper retraction pair so downstream upsert sinks can reconcile.

### Where each piece of the logic lives

| Concern | Lives in | Mechanism |
|---|---|---|
| Compute `windowStart` from event timestamp | `LocalWindowAggregateOperatorV5.processElement` | `ts - ts % windowSizeMs` |
| Buffer per (bundleKey, windowStart) accumulator | `LocalWindowAggregateOperatorV5.windowBuffer` | `Map<String, Map<Long, GenericRowData>>` — heap only |
| Flush on watermark advance | `LocalWindowAggregateOperatorV5.processWatermark` | iterate buffer, emit + drop entries whose windowEnd ≤ wm |
| Checkpoint safety | `LocalWindowAggregateOperatorV5.prepareSnapshotPreBarrier` | flush ALL — buffer is empty at barrier, no state to snapshot |
| Drop past-lateness events | `GlobalWindowAggregateFunctionV5.processElement` | `if (currentWatermark >= cleanupTime) return;` |
| Merge late partial into the correct window | `GlobalWindowAggregateFunctionV5.processElement` | `accState.get(windowStart).merge(...)` — windowStart from the partial row |
| Initial fire on `windowEnd` | `GlobalWindowAggregateFunctionV5.onTimer` | fire timer at `windowEnd`, emit `+I`, set `firedState[windowStart] = true` |
| Late re-fire | `GlobalWindowAggregateFunctionV5.processElement` | `if firedState[windowStart] == TRUE → emit -U/+U` |
| `-U` carries previous value | `GlobalWindowAggregateFunctionV5.lastEmittedState` | `MapState<windowStart, Long>` tracks last-emitted value |
| Cleanup at `windowEnd + lateness` | `GlobalWindowAggregateFunctionV5.onTimer` | cleanup timer fires → clear all three MapStates for `windowStart` |

### Verification

`TwoPhaseCountDeduplicatedEventTimeV5Test` covers three scenarios with a deterministic scripted source:

| Test | Asserts |
|---|---|
| `testLateEventTriggersRefire` | After initial fire, a late event within allow-lateness produces `-U` / `+U` carrying the **original** `windowStart=0` |
| `testEventPastAllowedLatenessIsDropped` | Event arriving after `windowEnd + allow-lateness` produces no extra output; cleanup has run |
| `testHandlerArithmetic` | `CountStarAggsHandleFunctionV5` correctly counts on `accumulate` / `retract` / `merge` / `resetAccumulators` |

The tests run on a single-parallelism `LocalEnvironment` so the order of `+I` / `-U` / `+U` is fully deterministic — no `Thread.sleep`, no timing.

### PURGING vs ACCUMULATING — why V5 (and the Table API) chose PURGING

V5's local stage drops the accumulator the moment the watermark crosses `windowEnd`. Late events for that window create a **fresh** accumulator counting only the new events. Each emission is therefore a **delta** (events since the last flush). This is the same design `LocalSlicingWindowAggOperator` uses.

V3's `LocalPreAggregator` makes the opposite choice: it MOVES the accumulator from `windowBuffers` into a second map `flushedWindows` on flush, and keeps mutating it in place until the allowed-lateness window expires. Each V3 emission is the **absolute running total**.

#### Side-by-side code

```java
// V5 — PURGING. Mirrors LocalSlicingWindowAggOperator.
// flushCompletedWindows:
if (watermark >= windowEnd) {
    emitPartial(bundleKey, windowStart, entry.getValue());
    it.remove();                                  // ◄── drop the accumulator
}

// processElement (after flush):
GenericRowData acc = windowsForKey.get(windowStart);
if (acc == null) {
    acc = (GenericRowData) handler.createAccumulators();  // ◄── FRESH for late events
}
handler.accumulate(input);                        // counts only the new event
```

```java
// V3 — ACCUMULATING.
// flushCompletedWindows:
if (watermark >= windowEnd) {
    for (Map.Entry<String, Long> kc : keyCounts.entrySet()) {
        out.collect(Tuple4.of(kc.getKey(), subtaskIndex, kc.getValue(), windowStart));
    }
    flushedWindows.put(windowStart, keyCounts);   // ◄── MOVE to flushedWindows, KEEP
    it.remove();                                   // remove only from windowBuffers
}

// processElement (late path):
if (flushedWindows.containsKey(windowStart)) {
    flushedWindows.get(windowStart).merge(groupKey, 1L, Long::sum);  // ◄── INCREMENT in place
    long updatedCount = flushedWindows.get(windowStart).get(groupKey);
    out.collect(Tuple4.of(groupKey, subtaskIndex, updatedCount, windowStart));  // absolute
}
```

#### What gets emitted

Same scenario as the §13 trace: 3 events in window `[0, 60000)`, then 1 late event in the same window.

```
   Watermark crosses windowEnd ────────────────────────► Late event arrives
                                                                  │
   V5 emissions (deltas):                                          │
     partial(A, ws=0, count=3)     ◄── delta = 3                   │
                                      ────────────────────────────► partial(A, ws=0, count=1)
                                                                          ◄── delta = 1
                                                                              (NOT 4)
   V3 emissions (absolute totals):
     (A, subtask0, count=3, ws=0)  ◄── absolute = 3
                                      ────────────────────────────► (A, subtask0, count=4, ws=0)
                                                                          ◄── absolute = 4
                                                                              (includes the prior 3)
```

#### How each global stage consumes them

```
V5 GLOBAL — algebraic merge, no dedup
─────────────────────────────────────
  accState[0] ← merge(partial(3))    → accState[0] = 3
  accState[0] ← merge(partial(1))    → accState[0] = 4   ◄── just sum the deltas


V3 GLOBAL — overwrite by subtaskIdx, then sum
─────────────────────────────────────────────
  subtaskCounts[subtask0] = 3        → total = 3
  subtaskCounts[subtask0] = 4        → total = 4         ◄── overwrite earlier value
  (relies on accumulating ListState semantics +
   FIFO iteration order, both of which V3's JavaDoc
   flags as undocumented contracts)
```

#### Comparison

| Property | V5 / Table API (PURGING) | V3 (ACCUMULATING) |
|---|---|---|
| Local state size | bounded by **currently-open windows** only | open windows **plus** flushed-within-lateness windows |
| What each partial carries | **delta** since last flush — self-contained | **absolute total** since window start — must override any earlier emission |
| Global merge logic | `handler.merge(incoming)` — algebraic, works for any decomposable aggregate | overwrite by subtaskIdx, then sum — works for COUNT/SUM-like; awkward for AVG, COUNT(DISTINCT), user UDAFs |
| Subtask-id tagging | not needed — partials are subtask-agnostic | required — the dedup map is keyed by subtaskIdx |
| Hidden contract | none | ACCUMULATING window semantics + FIFO `ListState.get()` iteration order |
| Rescaling | safe — partials carry no subtask identity | breaks if subtask count changes (subtaskIdx → window-shard mapping shifts) |
| Output shape | proper changelog (`+I` / `-U` / `+U`) ready for upsert sinks | one absolute row per fire — needs application-level dedup to drive an upsert sink |

#### Why PURGING is the Table API's choice

The decisive reason is **generality**. The Table API's global operator only ever calls `handler.merge(incoming)` on the codegen-generated `AggsHandleFunction`. That single primitive must work uniformly across every SQL aggregate the planner might generate:

- `COUNT(*)` — merge means `+=`
- `SUM(col)` — same
- `AVG(col)` — `merge(other)` adds both `sum` and `count` fields
- `MAX(col)` — `merge(other)` keeps the larger value
- `COUNT(DISTINCT col)` — `merge(other)` unions the underlying state-backed `MapView<col, true>`
- user UDAFs — whatever the user defined

If the global stage instead did "overwrite by subtaskIdx" (V3's choice), it would need a different unification rule per aggregate type, and the codegen output would have to encode that rule. Worse, AVG can't naively use overwrite — averaging the latest reported AVG from each subtask is not the same as averaging the underlying values. The PURGING + algebraic-merge approach sidesteps this by making the global side aggregate-agnostic.

The secondary reason is **bounded state**. The local stage in V5 holds state only for windows that haven't fired yet; V3's local stage holds state for the full allowed-lateness duration after the fire too. For a query with allow-lateness = 1 hour and a high event rate, this can be a significant memory difference.

#### One subtle consequence visible in the V5 output

Because V5 emits deltas and the global stage uses algebraic merge, the re-fire `+U` value is **strictly greater than** the previous emitted value (for COUNT/SUM, since these only grow with positive deltas):

```
+I[A, ws=0, count=3]   ◄── after initial partial(3)
-U[A, ws=0, count=3]   ◄── retract previous
+U[A, ws=0, count=4]   ◄── after merging partial(1) — new total 3+1=4
```

V3's design wouldn't naturally produce this `-U` / `+U` pair — its global stage just emits the new absolute count per fire (`INSERT`-only), and downstream consumers must reconcile by themselves. That's why hooking V3 up to an upsert sink takes extra code; V5's output is already a proper changelog.

---

## 14. Pitfalls and gotchas

1. **Local-Global silently does nothing without MiniBatch.** The rule's first check is `isMiniBatchEnabled`. If MiniBatch is off, the rule's `matches(...)` returns `false` and no error/warning is emitted. Always verify with `EXPLAIN`.
2. **`agg-phase-strategy = AUTO` is the default, but Local-Global still needs MiniBatch.** "AUTO" controls whether the rule is preferred over one-phase **when applicable**, not whether the prerequisite features are turned on.
3. **Split-Distinct is OFF by default.** Even on Flink 1.20 / 2.x as of this writing.
4. **Split-Distinct has overhead.** State access for the partial aggregate plus an extra shuffle stage. Don't enable it for queries with no distinct-key skew, or for tiny input sets — the rule's docs explicitly warn against this.
5. **User-defined `AggregateFunction` blocks both optimizations.** Local-Global needs `merge()` to be implemented; Split-Distinct only supports built-ins.
6. **Session windows + Split-Distinct are excluded by design.** `SplitAggregateRule.matches(...)` short-circuits for `SessionWindowSpec` because the window assigner output would differ between the partial and final aggregate runs.
7. **Two operators, two states.** With Local-Global enabled, the *global* operator holds the keyed state for the aggregate; the local operator holds none. This shifts the state-size discussion away from "one big keyed state on the global side" toward "small bundle buffers on the local side, full state on the global side." For TTL tuning, only the global side matters.
8. **`SELECT COUNT(1) FROM t` is still bottlenecked by SINGLETON shuffle.** Local-Global reduces *traffic*, but not parallelism. If you need parallel global counting, you have to introduce an explicit `GROUP BY` (e.g., a bucket key) and sum at the application level.
9. **Windowed Local-Global doesn't need mini-batch.** Unbounded Local-Global silently no-ops without mini-batch (pitfall #1). The windowed version is independent — slicing is the batching mechanism. So `agg-phase-strategy = TWO_PHASE` alone enables it for windowed queries.
10. **State TTL shorter than upstream retraction lag is a correctness hazard for unbounded GroupAggregate.** If a `-D` for key `K` arrives after `K`'s state has been TTL'd out, the operator starts a fresh accumulator and the retract produces a "negative" partial that lives until next compaction. Size `table.exec.state.ttl` larger than the maximum upstream retraction delay. See §8 ("Late-arriving DELETE").
11. **`accState.value()` returning `null` is the "first row" signal** in the global aggregate. The operator distinguishes between (a) no state ever existed for this key → emit `+I`, and (b) state existed but was cleared by a prior `recordCount == 0` → also treated as first row. If your code reads `accState` and assumes non-null, you'll see wrong output during the first emission per key (after recovery from a savepoint, this is the default state for every key the savepoint didn't include).
12. **The hand-written DataStream pattern in `SkewedAggregation.java` is a good reference for *what the planner is doing under the hood***, but production workloads should use the Table-API path. See §11 for the gap analysis.

---

## Sources

### Apache Flink documentation
- [Performance Tuning — Apache Flink master docs](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/tuning/) — primary reference for MiniBatch + Local-Global + Split-Distinct
- [FLIP-44: Support Local Aggregation in Flink — Apache Confluence](https://cwiki.apache.org/confluence/display/FLINK/FLIP-44:+Support+Local+Aggregation+in+Flink) — the original design doc
- [Apache Flink 1.12 Streaming Aggregation tuning page](https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/table/tuning/streaming_aggregation_optimization.html) — older but still readable reference for the same three optimizations

### Industry write-ups
- [How to Write Fast Flink SQL — Ververica](https://www.ververica.com/blog/how-to-write-fast-flink-sql)
- [Joining Highly Skewed Streams in Flink SQL — Ververica](https://www.ververica.com/blog/joining-highly-skewed-streams-in-flink-sql)
- [Optimize Flink SQL — Alibaba Cloud (Realtime Compute)](https://www.alibabacloud.com/help/en/flink/optimize-flink-sql)
- [Flink SQL Performance Tuning, Part 2 — Chunting Wu / Medium](https://lazypro.medium.com/flink-sql-performance-tuning-part-2-c102177b1ce1)

### Source files (this repo)
- `flink-table/flink-table-api-java/src/main/java/org/apache/flink/table/api/config/AggregatePhaseStrategy.java`
- `flink-table/flink-table-api-java/src/main/java/org/apache/flink/table/api/config/OptimizerConfigOptions.java` (lines 44-87)
- `flink-table/flink-table-api-java/src/main/java/org/apache/flink/table/api/config/ExecutionConfigOptions.java` (lines 664-700)
- `flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/plan/rules/logical/SplitAggregateRule.java`
- `flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/plan/rules/physical/stream/TwoStageOptimizedAggregateRule.java`
- `flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/plan/nodes/exec/stream/StreamExecLocalGroupAggregate.java`
- `flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/plan/nodes/exec/stream/StreamExecGlobalGroupAggregate.java`
- `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/aggregate/MiniBatchLocalGroupAggFunction.java`
- `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/aggregate/MiniBatchGlobalGroupAggFunction.java`
- `flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/plan/rules/physical/stream/TwoStageOptimizedWindowAggregateRule.java`
- `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/tvf/common/WindowAggOperator.java`
- `flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/utils/WindowEmitStrategy.scala`
- `flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/SkewedAggregation.java` — hand-written DataStream-level analogue, used for the gap analysis in §11
- `flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/TwoPhaseCountDeduplicatedEventTimeV2.java` — hand-written event-time two-phase dedup, used for the §12 comparison
- `flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/TwoPhaseCountDeduplicatedEventTimeV3.java` — DataStream-level optimization of V2 that eliminates the local-phase shuffle via a non-keyed `LocalPreAggregator`; keeps the dedup-by-subtaskIdx pattern
- `flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/TwoPhaseCountDeduplicatedEventTimeV4.java` — Table API rewrite of V2 referenced from §12
- V5 (DataStream-only reimplementation of windowed Local-Global, referenced from §13):
  - `TwoPhaseCountDeduplicatedEventTimeV5.java` — `main()` + pipeline wiring
  - `EventToRowDataMapV5.java` — Tuple3 → RowData[key]
  - `KeyExtractorV5.java` — RowData → bundle key
  - `CountStarAggsHandleFunctionV5.java` — hand-written codegen output for COUNT(*)
  - `LocalWindowAggregateOperatorV5.java` — non-keyed pre-aggregator
  - `GlobalWindowAggregateFunctionV5.java` — keyed final aggregator with changelog
  - Plus `flink-examples/flink-examples-streaming/src/test/java/org/apache/flink/streaming/test/examples/aggregation/TwoPhaseCountDeduplicatedEventTimeV5Test.java` for the late-data verification
- Real Table API references that V5 mirrors:
  - `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/aggregate/window/LocalSlicingWindowAggOperator.java`
  - `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/tvf/common/WindowAggOperator.java`
  - `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/generated/AggsHandleFunction.java`
- Related: `codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md` for the RocksDB composite-key layout that backs §6
