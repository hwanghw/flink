# `flink-datastream`

## Purpose

The **runtime / implementation side of the DataStream V2 API**. Where
`flink-datastream-api` defines the user-facing interfaces, this module
provides the concrete stream types, operators, contexts, and the
`ExecutionEnvironment` that turns a V2 program into a Flink `StreamGraph`
and submits it for execution.

Maven name: *"Flink : DataStream : Impl"* (vs. `flink-datastream-api` =
*"Flink : DataStream : API"*).

## Where it fits — relationship to `flink-streaming-java`

This module sits *on top of* `flink-streaming-java`. It does **not**
replace it — it builds an alternative user-facing API surface that lowers
into the same `StreamGraph` / `Transformation` / `StreamOperator`
infrastructure already in `flink-streaming-java`.

```
       User code
            │
            ▼
   flink-datastream-api   (interfaces only, FLIP-409 surface)
            │
            ▼
   flink-datastream       (THIS MODULE: ExecutionEnvironmentImpl,
            │              ProcessOperator, *StreamImpl, contexts,
            │              window/join/eventtime extensions)
            ▼
   flink-streaming-java   (StreamGraph, Transformation, StreamOperator,
            │              AbstractStreamOperator, KeyedStateBackend...)
            ▼
   flink-runtime          (task execution, network, checkpointing)
```

`flink-streaming-java`'s legacy `DataStream` API is a *sibling* layer at the
same level as this module: both produce `Transformation`s and ultimately
hand off to `StreamGraphGenerator`. They are not interoperable from the
user's perspective, but they share the lower runtime.

One notable side effect: this module adds files into the
`org.apache.flink.streaming.api.transformations` and
`org.apache.flink.streaming.runtime.translators` packages (see Source
layout) so it can plug new `Transformation` / translator types into
`StreamGraphGenerator` without modifying `flink-streaming-java` itself.

## Maven coordinates

```xml
<groupId>org.apache.flink</groupId>
<artifactId>flink-datastream</artifactId>
<version>2.3-SNAPSHOT</version>
```

`<packaging>jar</packaging>`. Parent: `flink-parent`.

JDK17+ note: pom sets `--add-opens=java.base/java.util=ALL-UNNAMED` for
Surefire because sink transformation translator registration uses
reflection on a `final` map (see `DataStreamV2SinkTransformationTranslator.registerSinkTransformationTranslator()`).

## Dependencies

### Direct (main)

- `flink-datastream-api` — the public interfaces this module implements.
- `flink-streaming-java` — `StreamGraph`, `StreamGraphGenerator`,
  `Transformation` hierarchy, `StreamOperator` base classes,
  `AbstractAsyncStateUdfStreamOperator`, watermark plumbing, etc.

That's it — transitively it pulls in `flink-runtime`, `flink-core`,
`flink-core-api`. Test-scope deps add `flink-test-utils-junit`,
`flink-streaming-java` test-jar, and `flink-runtime` test-jar.

### Used by

End-user jobs and downstream Flink connector/extension modules that target
DataStream V2. Putting **only** `flink-datastream` on the user classpath is
sufficient — it transitively brings `flink-datastream-api`.

## Source layout

`src/main/java`: 67 Java files.
`src/test/java`: 43 Java files.

```
org/apache/flink/datastream/impl/
├── ExecutionEnvironmentImpl.java          looked up reflectively from API's ExecutionEnvironment.getInstance()
├── ExecutionEnvironmentFactory.java
├── ExecutionContextEnvironment.java
├── stream/                                concrete stream classes (one per API interface)
│   ├── AbstractDataStream.java            base; holds the Transformation
│   ├── NonKeyedPartitionStreamImpl, KeyedPartitionStreamImpl
│   ├── GlobalStreamImpl, BroadcastStreamImpl
│   ├── ProcessConfigurableAnd{Global,Keyed,NonKeyed,TwoNonKeyed,TwoKeyed}PartitionStream*Impl
│   └── ProcessConfigureHandle.java         setName/setParallelism/setUid plumbing
├── operators/                             StreamOperator wrappers around user ProcessFunctions
│   ├── ProcessOperator                    OneInput -> AbstractAsyncStateUdfStreamOperator
│   ├── KeyedProcessOperator, BaseKeyedProcessOperator
│   ├── TwoInputBroadcastProcessOperator,  KeyedTwoInputBroadcastProcessOperator
│   ├── TwoInputNonBroadcastProcessOperator, KeyedTwoInputNonBroadcastProcessOperator, BaseKeyedTwoInputNonBroadcastProcessOperator
│   └── TwoOutputProcessOperator, KeyedTwoOutputProcessOperator, BaseKeyedTwoOutputProcessOperator
├── context/                               Default* implementations of the API context interfaces
│   ├── DefaultRuntimeContext, DefaultJobInfo, DefaultTaskInfo
│   ├── DefaultPartitionedContext / DefaultTwoOutputPartitionedContext
│   ├── DefaultNonPartitionedContext / DefaultTwoOutputNonPartitionedContext
│   ├── DefaultStateManager                drives the declarative state contract
│   ├── DefaultProcessingTimeManager, UnsupportedProcessingTimeManager
│   └── AbstractPartitionedContext
├── common/
│   ├── OutputCollector, TimestampCollector
│   └── KeyCheckedOutputCollector          enforces keyBy invariants
├── watermark/DefaultWatermarkManager.java
├── attribute/AttributeParser.java         reads @NoOutputUntilEndOfInput etc.
├── builtin/
│   ├── BuiltinJoinFuncs                   reflectively loaded by API's BuiltinFuncs.join(...)
│   └── BuiltinWindowFuncs                 reflectively loaded by API's BuiltinFuncs.window(...)
├── extension/
│   ├── eventtime/                         EventTimeExtensionImpl (loaded by API's EventTimeExtension)
│   │   ├── functions/EventTimeWrapped{OneInput,TwoInputBroadcast,TwoInputNonBroadcast,TwoOutput}StreamProcessFunction
│   │   ├── functions/ExtractEventTimeProcessFunction
│   │   └── timer/DefaultEventTimeManager
│   ├── window/                            window operators + context + utils
│   │   ├── operators/{OneInput,TwoInputNonBroadcast,TwoOutput}WindowProcessOperator
│   │   ├── operators/MergingWindowSet
│   │   ├── context/DefaultOneInputWindowContext, DefaultTwoInputWindowContext, WindowStateStore, WindowTriggerContext
│   │   ├── function/Internal*WindowStreamProcessFunction (bridges window strategy + user function)
│   │   └── utils/WindowUtils
│   └── join/operators/TwoInputNonBroadcastJoinProcessFunction + TwoInputNonBroadcastJoinProcessOperator
└── utils/StreamUtils.java                 helpers for building Transformations

org/apache/flink/streaming/                # intentionally placed under streaming-java's package
├── api/transformations/DataStreamV2SinkTransformation.java
└── runtime/translators/DataStreamV2SinkTransformationTranslator.java
```

## Architecture & key concepts

- **One stream class per API interface, all extend `AbstractDataStream`.**
  `AbstractDataStream` carries the underlying
  `Transformation<T>`; `.process(...)` builds a new operator + transformation
  and wraps the result in the appropriate `ProcessConfigurableAnd*` impl so
  the user can chain `setName(...)`, `setParallelism(...)`, etc.
- **`ProcessOperator` family.** Each `*ProcessOperator` extends
  `AbstractAsyncStateUdfStreamOperator` (async state path) or the appropriate
  two-input base. They translate `processElement(StreamRecord)` →
  `userFunction.processRecord(...)` and `processWatermark` →
  `userFunction.onWatermark(...)`, threading
  `DefaultPartitionedContext` / `DefaultRuntimeContext` /
  `DefaultProcessingTimeManager` in.
- **`KeyCheckedOutputCollector`** wraps emission from `KeyedPartitionStream`
  operators that promised key-preservation; at runtime it verifies the
  emitted record's key still maps to the partition before forwarding.
- **Watermark extension lowering.** When the user calls
  `EventTimeExtension.wrapProcessFunction(udf)` (in the API module), the
  result is an `EventTimeWrapped*StreamProcessFunction` from
  `impl.extension.eventtime.functions`. That class implements the
  V2 `OneInputStreamProcessFunction` (etc.) and delegates to the user's
  `EventTimeProcessFunction`, while owning a `DefaultEventTimeManager` for
  event timers and bridging `Watermark`-as-data to
  `EventTimeWatermarkHandler` in `flink-streaming-java`.
- **Window extension lowering.** `BuiltinFuncs.window(strategy, udf)` wraps
  the user `WindowProcessFunction` in an `InternalOneInputWindowStreamProcessFunction`
  (etc.), which the `ProcessOperator` translates to
  `OneInputWindowProcessOperator`. The operator owns timers + window state
  (`WindowStateStore`, `WindowTriggerContext`) and, for session windows,
  drives a `MergingWindowSet`.
- **Sink path.** `stream.toSink(sink)` produces a `DataStreamV2SinkTransformation`,
  registered with `StreamGraphGenerator` via
  `DataStreamV2SinkTransformationTranslator.registerSinkTransformationTranslator()`.
  Registration uses reflection to insert into a final map field — hence the
  `--add-opens` Surefire config in pom.xml.

## Important public APIs

This module is **not** intended as a direct compile-time API surface. User
code should import from `org.apache.flink.datastream.api.*`. The classes
here exist to be:

1. Loaded by reflection from the API module
   (`ExecutionEnvironmentImpl#newInstance`, `EventTimeExtensionImpl`,
   `BuiltinJoinFuncs`, `BuiltinWindowFuncs`).
2. Returned downcast as the corresponding API interface from those
   reflection calls.

If you must reference an impl class directly, treat it as `@Internal` even
though the source doesn't always annotate it.

## Internal flows

1. **Job construction.** `ExecutionEnvironment.getInstance()` →
   `ExecutionEnvironmentImpl` (static block registers the sink translator).
   `env.fromSource(source, name)` builds a `SourceTransformation` and wraps
   it in a `NonKeyedPartitionStreamImpl`.
2. **Operator chaining.** Each `.process(...)` call:
   - Picks the operator class (e.g., `ProcessOperator`,
     `KeyedTwoInputBroadcastProcessOperator`).
   - Builds a `OneInputTransformation` / `TwoInputTransformation` referencing
     the upstream `Transformation` and the new operator.
   - Returns a `ProcessConfigurableAndNonKeyedPartitionStreamImpl` (or the
     keyed variant) wrapping the new transformation.
3. **Submission.** `env.execute(name)` calls
   `StreamGraphGenerator.generate(transformations)` →
   `PipelineExecutor.execute(...)` (the standard flink-runtime path).
4. **Watermark dispatch in operators.** `ProcessOperator#processWatermark`
   resolves the incoming `WatermarkEvent` against the per-operator
   `watermarkDeclarationMap` (assembled from `userFunction.declareWatermarks()`),
   invokes `userFunction.onWatermark(...)`, and respects the returned
   `WatermarkHandlingResult` (PEEK / POLL).

## Tests

`src/test/java` mirrors the main package tree. Coverage focuses on:

- `impl/operators/` — `KeyedProcessOperatorTest`,
  `KeyedTwoInputBroadcastProcessOperatorTest`,
  `KeyedTwoInputNonBroadcastProcessOperatorTest`,
  `KeyedTwoOutputProcessOperatorTest` (use `Mock*ProcessFunction` helpers in
  the same package).
- `impl/context/` — round-trips for `DefaultStateManager`, `DefaultJobInfo`,
  `DefaultTaskInfo`, `DefaultProcessingTimeManager`,
  `DefaultNonPartitionedContext`, `DefaultTwoOutputNonPartitionedContext`.
- `impl/common/` — `OutputCollectorTest`, `KeyCheckedOutputCollectorTest`,
  type-descriptor tests.
- `impl/attribute/` — `StreamingJobGraphGeneratorWithAttributeTest` for the
  attribute parser (covers `@NoOutputUntilEndOfInput`).
- `impl/stream/`, `impl/utils/` — stream-construction smoke tests.
- `impl/extension/eventtime/` — `ExtractEventTimeProcessFunctionTest`.
- `impl/ExecutionEnvironmentImplTest` — environment lifecycle.

Helpers: `TestingExecutionEnvironmentFactory`, `TestingTransformation`,
`ContextTestUtils`, `TestingTimestampCollector`.

## Pitfalls & gotchas

- **This is a separate API from `flink-streaming-java`'s `DataStream`. Do
  not mix them.** They share `Transformation`/`StreamGraph` plumbing but
  have disjoint user-facing types and `ExecutionEnvironment`s. A job picks
  one stack.
- **Everything in `api.*` is `@Experimental`.** Even though the impl
  package does not always carry an annotation, treat impl classes as
  fully internal and subject to change.
- **Reflection-loaded class names are part of the contract.** Renaming or
  repackaging `ExecutionEnvironmentImpl`, `EventTimeExtensionImpl`,
  `BuiltinJoinFuncs`, or `BuiltinWindowFuncs` breaks the API module's
  reflective loaders. `ExecutionEnvironmentImpl` has an explicit Javadoc
  warning to this effect.
- **JDK17+ requires `--add-opens=java.base/java.util=ALL-UNNAMED`** when
  the sink transformation translator registers itself. Tests do this via
  Surefire `argLine`; downstream runners must too.
- **Key-preserving `.process(udf, newKeySelector)` on `KeyedPartitionStream`**
  is enforced at *runtime* by `KeyCheckedOutputCollector` — a buggy key
  selector throws at first non-conforming record, not at compile time.
- **Async state operator base.** `*ProcessOperator` extends
  `AbstractAsyncStateUdfStreamOperator`. Implementing additional V2
  operators outside this module means reckoning with async state semantics.
- **The `o.a.f.streaming.api.transformations` and `.runtime.translators`
  packages are intentionally shared with `flink-streaming-java`.** Don't
  treat the package split as a layer boundary; the V2 sink classes live
  there so they can register translators into a `flink-streaming-java`
  internal map.

## Related modules / docs

- `flink-datastream-api` — the public interfaces this module implements.
- `flink-streaming-java` — provides the operator/transformation/graph
  infrastructure this module builds on; also hosts the *legacy*
  `DataStream` API as a sibling stack.
- `flink-runtime` — execution, checkpointing, state backends.
- `flink-core` / `flink-core-api` — `dsv2.Source`/`Sink` markers and
  wrapping utilities (`DataStreamV2SourceUtils`, `WrappedSource`,
  `DataStreamV2SinkUtils`, `WrappedSink`) that adapt FLIP-27
  sources/`Sink<T>` to the V2 markers.
- FLIP-409 — "DataStream V2".
