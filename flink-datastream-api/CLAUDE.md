# `flink-datastream-api`

## Purpose

The **public API surface of the DataStream V2 API**: a redesigned, experimental
successor to the legacy `DataStream` API in `flink-streaming-java`. This module
contains *only interfaces, abstract classes, and lightweight value types* — no
runtime, no operators, no graph generation. Job authors write code against
these types; `flink-datastream` (the impl module) wires them to the actual
stream operators at runtime via reflection.

All types are marked `@Experimental`. The API was introduced as part of the
DataStream V2 effort (referenced in the codebase as "DataStream V2" /
"dsv2"; see also `flink-core-api`'s `org.apache.flink.api.connector.dsv2`
package, whose `Source` Javadoc notes "Source interface for DataStream api
v2"). Per the user's notes this corresponds to **FLIP-409** ("DataStream V2").

## Where it fits — relationship to `flink-streaming-java`

Flink ships **two** DataStream APIs in parallel:

| | Legacy `DataStream` | DataStream V2 (this module + impl) |
|---|---|---|
| Job-author entry point | `StreamExecutionEnvironment` (flink-streaming-java) | `ExecutionEnvironment` (this module) |
| Top stream type | `DataStream<T>` (concrete, in `o.a.f.streaming.api.datastream`) | `DataStream` marker interface + partition-specific subtypes (`NonKeyedPartitionStream`, `KeyedPartitionStream`, `GlobalStream`, `BroadcastStream`) |
| Per-record UDF entry | `ProcessFunction` / `KeyedProcessFunction` (flink-streaming-java) | `OneInputStreamProcessFunction`, `TwoInputBroadcastStreamProcessFunction`, `TwoOutputStreamProcessFunction`, etc. |
| Watermarks | Control-plane events (`WatermarkStrategy`, separate from records) | **Watermark-as-data**: generic `Watermark` records flow through `onWatermark(...)`; declared up-front via `ProcessFunction.declareWatermarks()` |
| Event time | Built into the runtime | Opt-in **extension** under `extension/eventtime` (see `EventTimeExtension`) |
| Windows | Native API methods on `KeyedStream`/`WindowedStream` | Opt-in extension via `WindowStrategy` + `BuiltinFuncs.window(...)` |
| Joins | API on the stream | Opt-in extension via `BuiltinFuncs.join(...)` |

The two stacks are **not source-compatible**. A job uses one or the other. The
new design favors a smaller core surface and pushes time/window/join semantics
into pluggable extensions.

## Maven coordinates

```xml
<groupId>org.apache.flink</groupId>
<artifactId>flink-datastream-api</artifactId>
<version>2.3-SNAPSHOT</version>
```

`<packaging>jar</packaging>`. Parent: `flink-parent`.

## Dependencies

### Direct

- `flink-core-api` — only dependency. Provides `Function`, `KeySelector`,
  `Watermark`, `WatermarkDeclaration`/`WatermarkDeclarations`,
  `WatermarkHandlingResult`, the `dsv2.Source`/`Sink` markers, etc.

That's it. No `flink-core`, no `flink-runtime`, no `flink-streaming-java`. The
module is deliberately a thin contract.

### Used by

- `flink-datastream` (the impl module) — implements every interface here and is
  loaded at runtime via reflection from `ExecutionEnvironment.getInstance()`,
  `BuiltinFuncs`, and `EventTimeExtension`. Users typically only put
  `flink-datastream` on the classpath; this module is pulled in transitively.

## Source layout

`org.apache.flink.datastream.api` — 51 Java files, all `@Experimental`.

```
api/
├── ExecutionEnvironment.java         entry point; gets impl by reflection
├── common/Collector.java             record emission (replaces Collector<T> from flink-core for V2)
├── attribute/NoOutputUntilEndOfInput.java
├── context/                          runtime contexts handed to UDFs
│   ├── RuntimeContext, JobInfo, TaskInfo
│   ├── PartitionedContext, NonPartitionedContext
│   ├── TwoOutputPartitionedContext, TwoOutputNonPartitionedContext
│   ├── StateManager                  declarative state access
│   └── ProcessingTimeManager
├── function/                         the ProcessFunction family
│   ├── ProcessFunction               base; declares states & watermarks
│   ├── OneInputStreamProcessFunction
│   ├── TwoInputBroadcastStreamProcessFunction
│   ├── TwoInputNonBroadcastStreamProcessFunction
│   ├── TwoOutputStreamProcessFunction
│   ├── ApplyPartitionFunction, TwoOutputApplyPartitionFunction
├── stream/                           the stream-type hierarchy
│   ├── DataStream                    topmost marker
│   ├── NonKeyedPartitionStream, KeyedPartitionStream
│   ├── GlobalStream, BroadcastStream
│   └── ProcessConfigurable           setName/setParallelism/setUid
├── builtin/BuiltinFuncs.java         static factories for join/window helpers
└── extension/
    ├── eventtime/                    EventTimeExtension, EventTimeManager, EventTimeWatermarkStrategy/Builder
    ├── window/                       WindowStrategy (tumbling/sliding/session/global), WindowProcessFunction family
    └── join/                         JoinFunction, JoinType
```

Notice: **no implementation classes**. Reflection lookups in `ExecutionEnvironment`,
`BuiltinFuncs`, and `EventTimeExtension` resolve to `org.apache.flink.datastream.impl.*`
in `flink-datastream`.

## Architecture & key concepts

- **Partition-aware stream typing.** Unlike the legacy single `DataStream<T>`,
  V2 separates *unkeyed* (`NonKeyedPartitionStream`), *keyed*
  (`KeyedPartitionStream<K,T>`), *global* (`GlobalStream`, parallelism 1), and
  *broadcast* (`BroadcastStream`) into distinct interfaces. Transitions
  between them (`keyBy`, `global`, `broadcast`, `shuffle`) are explicit.
- **Process-only.** There is no `map`/`filter`/`flatMap`/`reduce` at this
  layer. Everything is `.process(OneInputStreamProcessFunction)`. The intent
  is "one primitive, composable extensions".
- **Watermarks are first-class records.** Functions implement
  `WatermarkHandlingResult onWatermark(Watermark, Collector, ctx)`. They are
  *generic* (`LongWatermarkDeclaration`, `BoolWatermarkDeclaration`) — event
  time is no longer privileged; it's just the `BUILTIN_API_EVENT_TIME`
  declaration defined in `EventTimeExtension`.
- **Declarative state & watermark contracts.** `ProcessFunction.usesStates()`
  and `declareWatermarks()` force the user to enumerate what they touch up
  front, so the runtime can validate and reserve.
- **Extensions are opt-in static wrappers.** Event time, windows, and joins
  are not on the stream interface. You wrap a `ProcessFunction` via
  `EventTimeExtension.wrapProcessFunction(...)` or `BuiltinFuncs.window(...)`
  / `BuiltinFuncs.join(...)`, then pass the result to `stream.process(...)`.

## Important public APIs

- `ExecutionEnvironment` — `getInstance()` (reflection → impl),
  `fromSource(Source, name)`, `execute(jobName)`, `setExecutionMode`.
- `NonKeyedPartitionStream<T>` / `KeyedPartitionStream<K,T>` — `.process(...)`,
  `.connectAndProcess(...)`, `.keyBy`, `.toSink`, etc. Their nested
  `ProcessConfigurableAnd*Stream` types layer in `setName`/`setParallelism` via
  `ProcessConfigurable`.
- `OneInputStreamProcessFunction<IN,OUT>` — `processRecord`, `endInput`,
  `onProcessingTimer`, `onWatermark`.
- `TwoOutputStreamProcessFunction<IN,OUT1,OUT2>` — single input, side-output-
  style fan-out built in.
- `BuiltinFuncs.join(...)`, `BuiltinFuncs.window(...)` — the only ways to do
  joins/windows; both return wrapped `ProcessFunction`s.
- `EventTimeExtension` — exposes
  `EVENT_TIME_WATERMARK_DECLARATION` / `IDLE_STATUS_WATERMARK_DECLARATION`
  (the canonical event-time watermarks), `newWatermarkGeneratorBuilder(...)`
  for periodic/punctuated watermark generation, `wrapProcessFunction(...)`
  to inject an `EventTimeManager` (event timers) into a user
  `EventTimeProcessFunction`.
- `WindowStrategy` — `global()`, `tumbling(Duration[, TimeType[, lateness]])`,
  `sliding(...)`, `session(Duration[, TimeType])`. `TimeType.EVENT` /
  `TimeType.PROCESSING`.
- `Collector<T>` — `collect(T)` and `collectAndOverwriteTimestamp(T, long)`.

## Internal flows

This module is API-only; flow happens in `flink-datastream`. The handoff is
always through reflection:

1. `ExecutionEnvironment.getInstance()` → loads
   `org.apache.flink.datastream.impl.ExecutionEnvironmentImpl` via
   `Class.forName(...).getMethod("newInstance").invoke(null)`.
2. `BuiltinFuncs.join(...)` / `BuiltinFuncs.window(...)` → loads
   `BuiltinJoinFuncs` / `BuiltinWindowFuncs` from impl.
3. `EventTimeExtension.wrapProcessFunction(...)` → loads
   `EventTimeExtensionImpl` from impl.

If the impl module is missing from the classpath, all three error with
`"Please ensure that flink-datastream in your class path"`.

## Tests

None in this module — it's an API jar. Conformance tests live in
`flink-datastream/src/test`.

## Pitfalls & gotchas

- **Do not mix with `flink-streaming-java`'s `DataStream` API.** They share
  some core types (`KeySelector`, `Sink`/`Source` connectors) but their
  stream, function, and environment types are disjoint and *not*
  interoperable. Pick one stack per job.
- **Everything is `@Experimental`.** Signatures, package paths, and semantics
  may change between minor releases. Do not build long-term contracts on it.
- **Reflection class names are load-bearing.** `ExecutionEnvironmentImpl#newInstance`,
  `EventTimeExtensionImpl`, `BuiltinJoinFuncs`, `BuiltinWindowFuncs`
  must keep their fully-qualified names and method signatures, or this
  module breaks. There is an explicit comment to that effect in
  `ExecutionEnvironmentImpl`.
- **Watermarks are records, not control events.** A `ProcessFunction` that
  forgets to override `onWatermark` will silently propagate watermarks via
  the default `PEEK` strategy declared on the watermark itself. If you need
  to *stop* a watermark, return `WatermarkHandlingResult.POLL`.
- **Event-time timers only work on `KeyedPartitionStream`.** Every
  `EventTimeExtension.wrapProcessFunction(...)` Javadoc repeats this.
- **`tumbling`/`sliding`/`session` on a `GlobalStream`** silently keys by
  zero and runs as a keyed window — fine for correctness, but parallelism
  collapses to 1 for the window operator.

## Related modules / docs

- `flink-datastream` — the runtime/impl side; required at runtime.
- `flink-core-api` — sole compile dep; defines `Watermark`,
  `WatermarkDeclaration`, `Function`, `KeySelector`, `Sink`/`Source` markers.
- `flink-streaming-java` — the **legacy** DataStream API; orthogonal stack.
- `flink-core`'s `org.apache.flink.api.connector.dsv2` package
  (`DataStreamV2SourceUtils`, `WrappedSource`, etc.) — V2 source/sink
  adapter utilities that wrap FLIP-27 sources/`Sink<T>` into the V2
  `Source`/`Sink` markers.
- FLIP-409 — "DataStream V2" (the design proposal driving this module).
