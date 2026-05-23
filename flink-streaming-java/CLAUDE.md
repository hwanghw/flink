# `flink-streaming-java`

## Purpose

Historically the home of the **DataStream API** -- the low-level streaming API
for Flink (programs written against `StreamExecutionEnvironment` and `DataStream`
chains). In this fork the module has been **carved up**: the bulk of the
DataStream API surface (`StreamExecutionEnvironment`, `DataStream`,
`KeyedStream`, the `Transformation` planning tree,
`StreamGraph`/`StreamingJobGraphGenerator`, the operator base classes, the
`StreamTask` family, partitioners, watermark plumbing, the legacy
`SourceFunction`, and the standard window assigners/triggers/evictors) has been
**moved into `flink-runtime`**. What remains here is a focused slice that still
logically belongs to the user-facing streaming surface but was either too
domain-specific to pull into the engine, legacy-with-migration-tests, or
experimental. Do not look here for `DataStream.java` -- it is in `flink-runtime`.

## Where it fits

Sits between `flink-core` (types/config) and `flink-runtime` (execution). Because
most DataStream classes were promoted into `flink-runtime`, this module depends
on `flink-runtime`, not the other way around. Higher-level layers (Table API,
CEP, connectors, state-processor) depend on both.

## Maven coordinates

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java</artifactId>
  <version>2.3-SNAPSHOT</version>
</dependency>
```

Also publishes a `tests` classifier (test-jar) used by downstream modules for
streaming test harnesses; see "Tests" below.

## Dependencies

### Direct (from `pom.xml`)
- `flink-core` -- types, config, eventtime APIs (`WatermarkStrategy`, etc.)
- `flink-runtime` -- the engine, **and** the home of `DataStream`,
  `StreamExecutionEnvironment`, `Transformation`, `StreamTask`, partitioners,
  standard window assigners, the legacy `SourceFunction`, and the new `Source`
  operator infrastructure.
- `flink-file-sink-common` -- shared rolling-policy / bucket-writer machinery for
  the `StreamingFileSink` (filesystem legacy sink) that lives here.
- `flink-shaded-guava` -- internal collections.
- `commons-math3` -- numerics used by the delta-window helpers.
- Test scope: `flink-test-utils-junit`, `flink-core` test-jar, `flink-runtime`
  test-jar, `flink-migration-test-utils`.

### Used by (selected, non-exhaustive)
`flink-clients`, `flink-table-*`, `flink-connectors/*`, `flink-formats/*`,
`flink-libraries/flink-cep`, `flink-libraries/flink-state-processing-api`,
`flink-state-backends/*` (rocksdb/forst/changelog), `flink-datastream` (new V2
API), `flink-python`, `flink-dist`, `flink-tests`, `flink-container`,
`flink-yarn-tests`, the `flink-test-utils-parent/*` modules, `flink-dstl-dfs`,
`flink-walkthroughs/*`, `flink-examples-streaming`, and every
`flink-end-to-end-tests/*` module. That breadth is why nearly any change here
can ripple through the build.

## Source layout

Roughly 96 production classes under `org.apache.flink.streaming.`:

- `api.datastream/` -- `AsyncDataStream`, `DataStreamUtils`,
  `MultipleConnectedStreams` only (rest is in flink-runtime).
- `api.functions.async/` -- `AsyncFunction`, `RichAsyncFunction`, `ResultFuture`,
  `AsyncRetryStrategy/Predicate`, `CollectionSupplier`.
- `api.functions.co/` -- `RichCoMapFunction`, `RichCoFlatMapFunction`.
- `api.functions.sink/` -- `PrintSink` (v2), `SinkContextUtil`;
  `filesystem/` (`Bucket`, `Buckets`, `BucketState[Serializer]`,
  `BucketFactory`, life-cycle listeners, `StreamingFileSinkHelper`);
  `filesystem/legacy/StreamingFileSink`; `legacy/` (`DiscardingSink`,
  `TwoPhaseCommitSinkFunction`); `v2/DiscardingSink`.
- `api.functions.source/` -- `SerializedCheckpointData`; `datagen/`
  (`DataGenerator`, `DataGeneratorSource`, `Random/SequenceGenerator`).
- `api.functions.timestamps/` -- legacy `AscendingTimestampExtractor`,
  `BoundedOutOfOrdernessTimestampExtractor`.
- `api.functions.windowing/` -- `RichWindowFunction`, `RichAllWindowFunction`,
  the `delta/` + `delta/extractor/` family.
- `api.legacy.io/` -- `CollectionInputFormat`, `TextInput/OutputFormat`.
- `api.lineage/` -- `DefaultLineageVertex`,
  `DatasetConfig/Schema/TypeFacet*`, `LineageUtils`.
- `api.operators/` -- `AbstractInput`, `OnWatermarkCallback`;
  `async/AsyncWaitOperator(Factory)`; `async/queue/` (`Ordered/Unordered
  StreamElementQueue`, `Stream(Record)?QueueEntry`, `WatermarkQueueEntry`).
- `api.windowing/` -- the **specialized** assigners/triggers/evictors only
  (standard ones are in flink-runtime): session assigners and
  `Dynamic*SessionWindows` + `SessionWindowTimeGapExtractor`; `DeltaEvictor`,
  `TimeEvictor`; `AsyncProcessingTimeoutTrigger`,
  `Continuous{EventTime,ProcessingTime}Trigger`, `DeltaTrigger`,
  `ProcessingTimeoutTrigger`.
- `experimental/` -- `CollectSink`, `SocketStreamIterator` (not @Public).
- `runtime.execution/` -- `JobCreatedEvent` + default impl (lineage event).
- `runtime.operators/` -- `GenericWriteAheadSink`, `CheckpointCommitter`;
  `windowing/KeyMap`.
- `util.retryable/` -- `AsyncRetryStrategies`, `RetryPredicates`.
- `util.serialize/` -- `FlinkChillPackageRegistrar`,
  `InetSocketAddressSerializer`, `PriorityQueueSerializer`.

## Where the rest of the DataStream API now lives

All of the following sit under `flink-runtime/src/main/java/`, in the same
`org.apache.flink.streaming.*` package hierarchy they always used:

- `api.environment.*` -- `StreamExecutionEnvironment`,
  `Local/RemoteStreamEnvironment`, `CheckpointConfig`,
  `ExecutionCheckpointingOptions`.
- `api.datastream.*` -- `DataStream`, `KeyedStream`, `Windowed/AllWindowedStream`,
  `SingleOutputStreamOperator`, `DataStreamSource/Sink`, `ConnectedStreams`,
  `BroadcastStream(/Connected)`, `Joined/CoGroupedStreams`,
  `SideOutputDataStream`, `CachedDataStream`, `QueryableStateStream`.
- `api.transformations.*` -- the planning tree (`OneInput/TwoInput/Source/
  Sink/Partition` transformations, `Multiple*InputTransformation`,
  `LegacySource/SinkTransformation`, `TimestampsAndWatermarksTransformation`,
  ...).
- `api.graph.*` -- `StreamGraph`, `StreamingJobGraphGenerator`, `StreamConfig`,
  `StreamNode`, `StreamEdge`, `StreamGraphGenerator`, adaptive-graph machinery.
- `api.operators.*` -- `StreamOperator` -> `AbstractStreamOperator` ->
  `AbstractUdfStreamOperator`, plus `KeyedProcessOperator`, `ProcessOperator`,
  `StreamMap/Filter/FlatMap`, `SourceOperator`, the V2 line.
- `api.windowing.{assigners,triggers,windows}` and `runtime.operators.windowing`
  -- standard `Tumbling*`/`Sliding*`/`GlobalWindows`, `EventTime/Processing
  Time/Count/Purging` triggers, the `WindowOperator` family, `MergingWindowSet`.
- `runtime.tasks.*` -- `StreamTask`,
  `OneInput/TwoInput/MultipleInputStreamTask`, `SourceStreamTask`,
  `SourceOperatorStreamTask`, `OperatorChain`, mailbox executor.
- `runtime.{streamrecord,partitioner,watermark,watermarkstatus}` -- record
  carriers and stream partitioners.
- `api.functions.source.legacy.*` -- **legacy `SourceFunction`** and its
  subclasses (`RichSourceFunction`, `RichParallelSourceFunction`,
  `FromElementsFunction`, `SocketTextStreamFunction`,
  `ContinuousFileMonitoringFunction`, `StatefulSequenceSource`, ...).
- `runtime.operators.sink.*` -- Sink v2 runtime (committer operator, global
  committer, state writers).

### New `Source` / `SourceReader` API
Interfaces (`Source`, `SourceReader`, `SplitEnumerator`, `SourceSplit`,
`SourceEvent`, `Boundedness`, `ReaderOutput`, `SourceOutput`, `SourceReaderContext`,
`SplitEnumeratorContext`) live in `flink-core` under
`org.apache.flink.api.connector.source`. Runtime side (`SourceOperator`,
`SourceCoordinator`) is in `flink-runtime`. This is the **preferred** API for
new connectors; built-in connectors (Kafka, files, JDBC, ...) and CEP/Table all
target it via `env.fromSource(...)`.

## Architecture & key concepts

### Programming surface
Semantics of `StreamExecutionEnvironment`, `DataStream<T>`, `KeyedStream<T,K>`,
and the fluent builder pattern are unchanged -- they just compile against
`flink-runtime`. The artifact users add to their POM (`flink-streaming-java`)
continues to be the right one because it transitively pulls `flink-runtime`.

### Source/Sink APIs

- **Modern `Source`** (FLIP-27): an enumerator on the JM produces `SourceSplit`s,
  per-subtask `SourceReader`s consume them. Interfaces in `flink-core`
  (`org.apache.flink.api.connector.source`); runtime (`SourceOperator`,
  `SourceOperatorStreamTask`, `SourceCoordinator`) in `flink-runtime`.
- **Modern `Sink`** (FLIP-143 / Sink v2): `Sink`, `SinkWriter`, optional
  `Committer`/`GlobalCommitter`. `PrintSink` (this module) and `sink/v2/
  DiscardingSink` are the smallest examples; committer-operator wiring lives in
  `flink-runtime`.
- **Legacy `SourceFunction`** (old run/cancel loop) has moved to `flink-runtime`
  package `streaming.api.functions.source.legacy`. The old import path
  (`streaming.api.functions.source.SourceFunction`) no longer resolves. Strongly
  prefer the new `Source` API.
- **Legacy `SinkFunction`** -- `DiscardingSink` and the
  `TwoPhaseCommitSinkFunction` template -- still lives here under
  `api.functions.sink.legacy/`. `StreamingFileSink` (under `filesystem/legacy/`)
  is the canonical legacy example; new code should use Sink v2 +
  `flink-connector-files` `FileSink`.

### Operator hierarchy
`StreamOperator` -> `AbstractStreamOperator` -> `AbstractUdfStreamOperator` ->
concrete operators (`StreamMap`, `StreamFilter`, `StreamFlatMap`,
`KeyedProcessOperator`, `ProcessOperator`, ...). Those base classes and most
concrete operators are in `flink-runtime`. What this module adds:
`AbstractInput` (base for `AbstractStreamOperatorV2` inputs);
`OnWatermarkCallback` (async-of-element watermark hook); `AsyncWaitOperator(
Factory)` and the `StreamElementQueue` family that powers `AsyncDataStream`;
`GenericWriteAheadSink` + `CheckpointCommitter` for a WAL-sink pattern.
Chaining, factories, lifecycle, state init, and keyed-state plumbing live in
`flink-runtime`.

### Window operator (the big one)
`WindowOperator`/`EvictingWindowOperator`, the merging-window bookkeeping
(`MergingWindowSet`), the `WindowAssigner`/`Trigger`/`Evictor` contracts, and the
standard time/count assigners are all in `flink-runtime`. This module ships the
**specialized** window machinery only: session-window assigners (including the
`Dynamic*` variants with a `SessionWindowTimeGapExtractor`); delta-based windows
(`DeltaTrigger`, `DeltaEvictor`, the `DeltaFunction` /
`ExtractionAwareDeltaFunction` + Euclidean/Cosine distance + `extractor/`
helpers); continuous and timeout triggers; `TimeEvictor`;
`RichWindowFunction`/`RichAllWindowFunction`; and the internal `KeyMap` used by
older window operators.

See `codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md` and
`codedocs/flink-window-event-assignment-and-state-lifecycle.md`.

### Watermarks
The user-facing watermark API (`WatermarkStrategy`, `WatermarkGenerator`,
`TimestampAssigner`, `BoundedOutOfOrdernessWatermarks`,
`WatermarkStrategyWithIdleness`, ...) lives in `flink-core` under
`org.apache.flink.api.common.eventtime`. Runtime plumbing
(`TimestampsAndWatermarksOperator`, watermark valves, `WatermarkStatus`
multiplexing, idleness detection) is in `flink-runtime`. This module contributes
only `AscendingTimestampExtractor` and `BoundedOutOfOrdernessTimestampExtractor`
(legacy migration adapters) and the operator-side `OnWatermarkCallback` hook.

### Transformations & the planning tree
`DataStream.map(...)` appends a `Transformation` to the env; at
`env.execute(...)`, `StreamGraphGenerator` walks the transformation DAG into a
`StreamGraph`, then `StreamingJobGraphGenerator` lowers that to a `JobGraph`.
**Every class in that pipeline now lives in `flink-runtime`** -- this module
ships none of it, but pins behavior with tests (`StreamGraphGeneratorTest`,
`StreamingJobGraphGeneratorTest`, `AdaptiveGraphManagerTest`, ...) under
`src/test`.

### Async I/O
`AsyncDataStream` is the entry point; `AsyncFunction`/`RichAsyncFunction` is the
user contract; `ResultFuture`, `CollectionSupplier`, `AsyncRetryStrategy`/
`AsyncRetryPredicate` round it out. The operator is
`AsyncWaitOperator(Factory)` with the queue infrastructure
(`Ordered/UnorderedStreamElementQueue`, `StreamElementQueueEntry`,
`WatermarkQueueEntry`). Ordered preserves input order at the cost of head-of-
line blocking; unordered emits as results complete (watermark fences keep
event-time correctness).

### Lineage
`api.lineage` exposes the public-evolving lineage facets
(`DatasetConfigFacet`, `DatasetSchemaFacet`, `DatasetSchemaField`,
`TypeDatasetFacet`/`Provider`) and default vertex implementations
(`DefaultLineageVertex`, `DefaultSourceLineageVertex`, `LineageUtils`).
Surfaced to listeners via `runtime.execution.JobCreatedEvent` -- a
`JobStatusChangedEvent` carrying the `LineageGraph` and the
`RuntimeExecutionMode`.

### Iterations
No first-class iteration API. The historical `IterativeStream` is gone;
`StreamIterationHead`/`Tail` remain in `flink-runtime`'s
`streaming.runtime.tasks` for back-compat plumbing but are not exposed.

## Important public APIs (at a glance)

Module-resident classes worth knowing:

- `AsyncDataStream`, `DataStreamUtils`, `MultipleConnectedStreams` -- thin
  builder/helper layer over `DataStream` (which lives in flink-runtime).
- `AsyncFunction`, `RichAsyncFunction`, `ResultFuture`, `AsyncRetryStrategy`,
  `AsyncRetryPredicate`, `AsyncWaitOperator(Factory)` -- async-I/O contract and
  runtime operator.
- `RichCoMapFunction`, `RichCoFlatMapFunction` -- two-input UDF bases.
- `PrintSink` (Sink v2), `DiscardingSink` (legacy and v2 flavors),
  `TwoPhaseCommitSinkFunction` (canonical exactly-once SinkFunction template).
- `StreamingFileSink` (legacy) plus `Bucket`, `Buckets`, `BucketAssigner`,
  `BucketState`, `BucketStateSerializer`, `StreamingFileSinkHelper` -- bucketed
  filesystem output.
- `DataGeneratorSource`, `DataGenerator`, `RandomGenerator`, `SequenceGenerator`
  -- legacy synthetic-data source. (New `Source`-based generator is in
  `flink-connector-datagen`.)
- `AscendingTimestampExtractor`, `BoundedOutOfOrdernessTimestampExtractor` --
  legacy timestamp extractors. Prefer `WatermarkStrategy`.
- `EventTimeSessionWindows`, `ProcessingTimeSessionWindows`,
  `DynamicEventTimeSessionWindows`, `DynamicProcessingTimeSessionWindows`,
  `SessionWindowTimeGapExtractor` -- session windowing.
- `DeltaTrigger`, `DeltaEvictor`, `TimeEvictor`, `ContinuousEventTimeTrigger`,
  `ContinuousProcessingTimeTrigger`, `ProcessingTimeoutTrigger`,
  `AsyncProcessingTimeoutTrigger`, `RichWindowFunction`, `RichAllWindowFunction`
  -- specialized window pieces.
- `GenericWriteAheadSink`, `CheckpointCommitter` -- WAL-sink pattern.
- `JobCreatedEvent`, `DefaultJobCreatedEvent`, the `lineage.*` facets --
  lineage event surface.
- `AsyncRetryStrategies`, `RetryPredicates` -- pre-built retry policies.
- `FlinkChillPackageRegistrar`, `InetSocketAddressSerializer`,
  `PriorityQueueSerializer` -- Kryo defaults Flink registers automatically.

## Internal flows

- **User code -> JobGraph** (planning, classes in `flink-runtime`):
  `DataStream.foo(...)` appends a `Transformation` to the env; on
  `env.execute()`, `StreamGraphGenerator` walks the DAG and emits a
  `StreamGraph`; `StreamingJobGraphGenerator` collapses chainable operators
  into `JobVertex`es and emits a `JobGraph`; the `JobGraph` is submitted via
  `ClusterClient`. Tests that pin this pipeline live in this module's
  `src/test` (`StreamGraphGeneratorTest`, `StreamingJobGraphGeneratorTest`,
  `StreamingJobGraphGeneratorNodeHashTest`,
  `SinkV2TransformationTranslatorITCase`, `AdaptiveGraphManagerTest`).
- **Async I/O dispatch**: `AsyncDataStream.orderedWait` ->
  `AsyncWaitOperatorFactory` creates an `AsyncWaitOperator` -> `processElement`
  enqueues a `StreamRecordQueueEntry` into an `Ordered`/`UnorderedStreamElement
  Queue` and triggers the user `AsyncFunction` -> on completion the operator
  emits via the mailbox executor (in order, or as completed). Watermarks ride
  as `WatermarkQueueEntry`s so event-time semantics are preserved.
- **Session-window assignment**: `EventTimeSessionWindows.assignWindows` opens
  a one-element window per record; the `WindowOperator` (in `flink-runtime`)
  uses `MergingWindowSet` to coalesce overlapping windows, calling
  `mergeWindows`. `Dynamic*` variants ask `SessionWindowTimeGapExtractor` per
  record.
- **Bucketed file sink**: `StreamingFileSink.invoke` -> `Buckets.onElement` ->
  per-bucket `Bucket.write` to a `BucketWriter` (from `flink-file-sink-common`).
  On checkpoint, `BucketStateSerializer` writes in-progress part-file handles
  into operator state for exactly-once recovery.

## Tests

Publishes a **test-jar** consumed by downstream modules. It carries the
task-level harnesses (`StreamTaskTestHarness`, `OneInputStreamTaskTestHarness`,
`TwoInputStreamTaskTestHarness`, `StreamTaskMailboxTestHarness` + builder),
`StreamPartitionerTestUtils`, `CollectingSink` (under `streaming.util.testing`),
`SinkTestUtil`, and filesystem-sink `TestUtils`. **The canonical
`*StreamOperatorTestHarness` classes (`AbstractStreamOperatorTestHarness`,
`KeyedOneInputStreamOperatorTestHarness`, etc.) live in flink-runtime's
test-jar, not here.**

Notable tests in `src/test`: `DataStreamTest`, `StreamExecutionEnvironmentTest`,
the full `streaming.api.graph.*` suite (graph-generation correctness),
`StreamTaskTest`, `OneInputStreamTaskTest`, `SourceOperatorStreamTaskTest`,
`LocalStateForwardingTest`, `SinkV2TransformationTranslatorITCase`, plus the
state-migration fixtures under `src/test/resources/bucket-state-migration-test/`
and `two-phase-commit-sink-state-serializer-1.{11..20}`.

## Pitfalls & gotchas

- **Legacy `SourceFunction` is in `flink-runtime`, not here.** The new import
  path is `org.apache.flink.streaming.api.functions.source.legacy.SourceFunction`
  (and the same for `RichParallelSourceFunction`, `FromElementsFunction`, ...).
  Code written against the old path will not compile. Prefer the new `Source` API.
- **`DataStream`, `KeyedStream`, `StreamExecutionEnvironment` are in
  `flink-runtime`.** When searching for a method on `DataStream`, look in
  `flink-runtime` first. The `flink-streaming-java` artifact transitively pulls
  them, so user POMs are unchanged.
- **Two `DiscardingSink`s exist** -- legacy (`sink/legacy/`, `SinkFunction`) and
  v2 (`sink/v2/`, `Sink`). Pick the one matching your sink path.
- **`StreamingFileSink` is the legacy sink.** New code should use `FileSink` in
  `flink-connector-files`; `StreamingFileSink` is retained for state migration.
- **Watermark idleness** is enforced at the watermark-status level, not by
  individual extractors. Replace legacy `AscendingTimestampExtractor` /
  `BoundedOutOfOrdernessTimestampExtractor` with
  `WatermarkStrategy.forBoundedOutOfOrderness(...).withIdleness(...)`.
- **Async I/O ordered vs unordered.** Ordered mode head-of-line-blocks on slow
  futures; unordered mode emits as results complete but only preserves order
  within a watermark fence.
- **Object reuse / chained-operator copying.** Whether `StreamRecord`s are
  copied between chained operators depends on `ExecutionConfig.objectReuse` and
  `OperatorChain`'s copy/no-copy outputs (in `flink-runtime`). Stateful mutation
  of records emitted downstream is unsafe unless object reuse is disabled.
- **`TwoPhaseCommitSinkFunction`** keeps a `LinkedHashMap` of pending
  transactions in operator state; state-migration fixtures under
  `src/test/resources/two-phase-commit-sink-state-serializer-1.{11..20}` lock
  the binary format -- be careful changing field layout.
- **`KeyMap`** under `runtime.operators.windowing` is an internal flat-hash
  structure used by older window operators; not a public state-API map.
- **`experimental/`** classes are explicitly not `@Public`. `CollectSink` and
  `SocketStreamIterator` back `DataStreamUtils.collect()`.

## Related modules / codedocs

- `codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md` -- `WindowOperator`,
  window state, and checkpoint interactions end-to-end.
- `codedocs/flink-window-event-assignment-and-state-lifecycle.md` -- how an
  element flows through assigner -> trigger -> per-window state.
- `codedocs/flink-accumulating-vs-purging-and-allowed-lateness.md` --
  `PurgingTrigger`/`allowedLateness` semantics.
- `codedocs/flink-table-api-skew-aggregation-deep-dive.md` -- V5 plan expansion
  that emits DataStream operators from this module + runtime.
- `codedocs/flink-unaligned-checkpoint-watermark-recovery.md` -- watermark and
  checkpoint-barrier coordination in `StreamTask`.
- `codedocs/flink-serialization-deep-dive.md` -- ties into the Kryo registrar
  and serializers under `util/serialize/`.
- `codedocs/flink-network-credit-flow-control-tuning.md`,
  `codedocs/flink-network-direct-memory-netty.md` -- network layer feeding
  `StreamTask` input gates.
- `codedocs/flink-exactly-once-checkpointing-deep-dive.md` and
  `codedocs/flink-sink-patterns-comparison.md` -- context for
  `TwoPhaseCommitSinkFunction`, Sink v2 committers, and picking a sink type.
- Sibling modules: `flink-runtime/CLAUDE.md` (real home of DataStream API
  classes), `flink-core/CLAUDE.md` (types, eventtime, `Source` interfaces),
  `flink-datastream-api/CLAUDE.md` and `flink-datastream/CLAUDE.md` (new V2
  ProcessFunction-based DataStream API).
