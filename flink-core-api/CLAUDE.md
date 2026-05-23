# `flink-core-api`

## Purpose

`flink-core-api` is the **minimal, near-zero-dependency public-API surface**
introduced in the Flink 2.x line. It carries the abstract user-facing
types — `Function`, `KeySelector`, the `Tuple1`..`Tuple25` family, state
declarations, watermarks, `RuntimeExecutionMode`, `MemorySize`, and the
DataStream-V2 `Source` / `Sink` placeholders — without dragging in
`flink-core`'s heavyweight `TypeInformation`, Kryo, serialization, and
configuration infrastructure.

It exists so that thin clients (DataStream V2 user programs, lightweight
connector authors, Table API) can compile against the user-visible
interfaces without pulling the entire core jar. `flink-core` itself
depends on `flink-core-api`, not the other way around, so the API jar is
the leaf of the user-facing API graph.

## Where it fits in the Flink stack

```
                +-------------------+
                | flink-annotations |   (zero-dep)
                +---------+---------+
                          ^
                          |
                +---------+----------+        +--------------------+
                |  flink-metrics-core|        |   flink-test-utils-junit (test) |
                +---------+----------+        +--------------------+
                          ^
                          |
                +---------+----------+
                |   flink-core-api   |   <-- THIS MODULE
                +---------+----------+
                          ^
            +-------------+----------------+
            |                              |
   +--------+--------+         +-----------+----------+
   |   flink-core    |         |  flink-datastream-api|
   +--------+--------+         +-----------+----------+
            ^                              ^
            |                              |
   (the rest of Flink: streaming, table, runtime, …)
```

- **Consumed by:** `flink-core` (compile) and `flink-datastream-api`
  (compile). Two direct consumers, but transitively every Flink user job
  has these classes on its classpath.
- **Depends on:** `flink-annotations` and `flink-metrics-core` only (plus
  `flink-test-utils-junit` for tests). No third-party libs at runtime.

## How this differs from `flink-core`

| Concern              | `flink-core-api`                              | `flink-core`                                    |
| -------------------- | --------------------------------------------- | ----------------------------------------------- |
| Type system          | `TypeDescriptor` (abstract handle)            | `TypeInformation`, `TypeSerializer`, Kryo glue  |
| Configuration        | `MemorySize` only                             | `Configuration`, `ConfigOption`, parsing, …    |
| Functions            | `Function`, `ReduceFunction`, `AggregateFunction`, `KeySelector` (lambdas only) | Same interfaces + `RichFunction` machinery, accumulators |
| State                | `*StateDeclaration` + v1 / v2 `State` interfaces | `StateBackend`, `KeyedStateBackend`, serializers |
| Tuples               | Yes, `Tuple1`..`Tuple25`                      | No (moved here in 2.x)                          |
| Watermarks           | Generalized `Watermark` interface + `Long`/`Bool` impls (DSv2) | Legacy `Watermark` long-timestamp class       |
| Source / Sink        | `Source<T>` / `Sink<T>` placeholders (DSv2)   | FLIP-27 Source, Sink V2                         |
| Third-party deps     | None                                          | Many (Kryo, Apache Commons, etc.)              |

In other words: `flink-core-api` is the *contract*, `flink-core` is the
*implementation* and the legacy V1 surface. The duplication of
`SlotSharingGroup` (this jar has its own copy; see the class JavaDoc) is
explicit and acknowledged — the `flink-core` version exposes internal
types, the `flink-core-api` version is clean for DataStream V2 users. The
legacy copy will go away once V1 is removed.

## Maven coordinates

- **Group:** `org.apache.flink`
- **Artifact:** `flink-core-api`
- **Packaging:** `jar`
- **Version:** inherits `2.3-SNAPSHOT` from `flink-parent`
- **Sub-modules:** none

## Dependencies

### Direct (from `pom.xml`)

| Dependency                          | Scope   | Why                                                      |
| ----------------------------------- | ------- | -------------------------------------------------------- |
| `org.apache.flink:flink-annotations`| compile | API stability annotations on every type                  |
| `org.apache.flink:flink-metrics-core`| compile | Allows API types to expose `MetricGroup` etc.            |
| `org.apache.flink:flink-test-utils-junit` | test | JUnit 5 base for the handful of tests in this jar    |

That is the complete dependency list. No Jackson, no Kryo, no Guava.

### Used by

- `flink-core/pom.xml` (compile) — the legacy core jar depends on the new
  API jar.
- `flink-datastream-api/pom.xml` (compile) — DataStream V2's user-facing
  API jar depends on this one directly.

Everything else picks the classes up transitively via `flink-core`.

## Source layout

```
src/main/java/org/apache/flink/
├── api/
│   ├── common/
│   │   ├── RuntimeExecutionMode.java         # STREAMING / BATCH / AUTOMATIC enum
│   │   ├── SlotSharingGroup.java             # DSv2 clean copy (vs flink-core's)
│   │   ├── functions/
│   │   │   ├── Function.java                 # empty SAM-friendly marker
│   │   │   ├── ReduceFunction.java
│   │   │   └── AggregateFunction.java
│   │   ├── state/                            # v1 state declarations
│   │   │   ├── State, ValueState, ListState, MapState, BroadcastState,
│   │   │   ├── AggregatingState, ReducingState, MergingState, AppendingState,
│   │   │   ├── ReadOnlyBroadcastState, IllegalRedistributionModeException
│   │   │   ├── StateDeclaration (+ RedistributionMode enum)
│   │   │   ├── ValueStateDeclaration, ListStateDeclaration, MapStateDeclaration,
│   │   │   ├── ReducingStateDeclaration, AggregatingStateDeclaration,
│   │   │   ├── BroadcastStateDeclaration
│   │   │   ├── StateDeclarations.java        # static builders / factory
│   │   │   └── v2/                           # async state API (V2)
│   │   │       └── State, ValueState, ListState, MapState, AggregatingState,
│   │   │            ReducingState, MergingState, AppendingState,
│   │   │            StateFuture, StateIterator
│   │   ├── typeinfo/
│   │   │   ├── TypeDescriptor.java           # abstract handle, no serializer
│   │   │   ├── TypeDescriptors.java          # static factories
│   │   │   └── utils/TypeUtils.java
│   │   └── watermark/                        # DSv2 watermarks
│   │       ├── Watermark.java                # generalized interface
│   │       ├── BoolWatermark, LongWatermark  # concrete impls
│   │       ├── *Declaration*, WatermarkCombinationFunction/Policy,
│   │       └── WatermarkManager, WatermarkHandlingResult/Strategy
│   ├── connector/dsv2/                       # DSv2 placeholder Source/Sink
│   │   ├── Source.java
│   │   └── Sink.java
│   └── java/
│       ├── functions/KeySelector.java        # the canonical SAM key extractor
│       └── tuple/                            # Tuple0 .. Tuple25 + Tuple<N>Builder
│           ├── Tuple.java
│           ├── Tuple0.java ... Tuple25.java
│           └── builder/Tuple0Builder.java ... Tuple25Builder.java
├── configuration/
│   └── MemorySize.java                       # "256mb" style size parser
├── types/
│   └── NullFieldException.java
└── util/
    ├── TaggedUnion.java                      # internal, used by co-group / two-input window
    └── function/                             # SerializableX / *WithException helpers
        ├── BiConsumerWithException, BiFunctionWithException,
        ├── CheckedSupplier, FunctionWithException, LongFunctionWithException,
        ├── QuadConsumer, QuadFunction, RunnableWithException,
        ├── SerializableFunction, SerializableSupplier, SerializableSupplierWithException,
        ├── SupplierWithException, ThrowingConsumer, ThrowingExceptionUtils,
        ├── ThrowingRunnable, TriConsumer, TriConsumerWithException,
        └── TriFunction, TriFunctionWithException

src/test/java/.../
├── SlotSharingGroupTest, WatermarkDeclarationsTest, Tuple2Test,
└── MemorySize{,PrettyPrinting}Test
```

≈ 130 main source files. Most files are tiny interfaces; the bulk by line
count is the generated `Tuple<N>` / `Tuple<N>Builder` family.

## Architecture & key concepts

### Functions (the user-facing SAM interfaces)

`Function` is *empty*; this is deliberate so that subinterfaces like
`ReduceFunction`, `AggregateFunction`, and `KeySelector` can be Java-8
functional interfaces (SAM), implementable as lambdas. Every function
extends `java.io.Serializable` because Flink ships closures from the
client to TaskManagers.

`AggregateFunction<IN, ACC, OUT>` is the four-method aggregate contract
(`createAccumulator`, `add`, `getResult`, `merge`). The "Rich" variants
live in `flink-core`, not here — staying out of this jar keeps lambda
usage cheap.

### State declarations (v1)

The `state/` package gives users a *declarative* way to describe state
without naming a backend. A `StateDeclaration` carries:

- the state name,
- the redistribution mode (`NONE` / `REDISTRIBUTABLE` / `IDENTICAL`),
- type info (via `TypeDescriptor`, *not* `TypeInformation`).

`StateDeclarations` is the static façade — `StateDeclarations
.valueStateBuilder(name, typeDescriptor).build()` etc. The concrete
backend wiring happens in `flink-core` / `flink-runtime`.

### State v2 (async)

The `state/v2/` package mirrors the v1 hierarchy but every operation
returns a `StateFuture<T>`. This is the async state API introduced for the
ForSt state backend and DataStream V2 — the idea is that state reads
should not block the operator thread. `StateIterator` is the async
analogue of `Iterable` for `MapState.entries()`.

### Watermarks (generalized)

The `watermark/` package is a redesign of watermarks for DataStream V2.
Instead of "a watermark is a long timestamp", a `Watermark` is anything
with a `String getIdentifier()`. Concrete impls (`LongWatermark`,
`BoolWatermark`) carry a value plus identifier. Users declare what
watermarks an operator emits via `WatermarkDeclaration`, and the runtime
combines watermarks from multiple inputs via `WatermarkCombinationPolicy`
+ `WatermarkCombinationFunction`. `WatermarkManager` is the user-side
handle for emitting them.

### Tuples

Strongly-typed `Tuple1` .. `Tuple25` plus a `Tuple0`. Each arity has a
companion `Tuple<N>Builder` for fluent construction. Used by Table API
collectors, by DataStream operations like `join`/`coGroup` result types,
and as a default rich type that the Flink type system supports natively
without Pojo registration. `Tuple.MAX_ARITY = 25`.

### `TypeDescriptor`

A thin handle that knows only `Class<T> getTypeClass()`. The point is
that this jar **does not depend on the heavyweight `TypeInformation` /
serializer machinery from `flink-core`**. `flink-core` provides factory
methods that turn a `TypeDescriptor` into a fully-fledged `TypeInformation`
under the hood.

### `MemorySize`

The "256mb", "2gb", "1024" parser/formatter. Moved here so DataStream V2
resource declarations (`SlotSharingGroup.taskHeapMemory(...)`) can use it
without the user needing `flink-core`.

### `util/function/`

A grab-bag of `WithException` / `Throwing*` / `Serializable*` functional
interfaces used throughout the Flink codebase. They live here to avoid
forcing internal modules to depend on `flink-core` for trivial helpers.

## Important public APIs

- **`org.apache.flink.api.common.functions.Function`** — root marker for
  every user-defined function. Empty by design.
- **`org.apache.flink.api.java.functions.KeySelector<IN, KEY>`** — the
  canonical `(IN) -> KEY` extractor used by `keyBy(...)`. Implementations
  must be deterministic and stable.
- **`org.apache.flink.api.common.RuntimeExecutionMode`** — `STREAMING`,
  `BATCH`, or `AUTOMATIC`. Controls scheduling, shuffles, watermark
  semantics. `AUTOMATIC` picks `BATCH` if all sources are bounded.
- **`org.apache.flink.api.common.SlotSharingGroup`** — describes resource
  requirements (`cpuCores`, `taskHeapMemory`, `taskOffHeapMemory`,
  `managedMemory`, `externalResources`) for a slot-sharing group. The
  clean DSv2 copy (read the class JavaDoc for the history).
- **`org.apache.flink.api.common.state.StateDeclarations`** — entry point
  for declaring `ValueState` / `ListState` / `MapState` / etc.
- **`org.apache.flink.api.common.watermark.WatermarkDeclarations`** —
  entry point for declaring `LongWatermark` / `BoolWatermark` user-defined
  watermarks (DSv2).
- **`org.apache.flink.api.common.typeinfo.TypeDescriptor`** — handle for
  expressing types in user code without depending on `flink-core`.
- **`org.apache.flink.configuration.MemorySize`** — parser for human-
  readable byte sizes.
- **`org.apache.flink.api.connector.dsv2.Source` / `Sink`** —
  placeholder marker interfaces for the DataStream-V2 connector contract
  (not yet finalised; see JavaDoc).
- **Tuples** — `Tuple1`..`Tuple25` for ad-hoc product types in user code.

## Internal flows (if applicable)

There is essentially no runtime "flow" in this module — it is almost
entirely interfaces and data classes. The two interesting compile-time
flows are:

1. **State declaration → backend wiring.** User code calls
   `StateDeclarations.valueStateBuilder(...)`; this returns a builder
   that produces a `ValueStateDeclaration`. `flink-core` /
   `flink-runtime` later inspects the declaration's `RedistributionMode`
   and `TypeDescriptor` and chooses the right `KeyedStateBackend`
   primitive. Nothing in this jar reaches that runtime — it just records
   intent.
2. **Watermark V2 path.** Operators declare watermarks via
   `WatermarkDeclaration`; the runtime (in `flink-streaming-java`) reads
   the declaration, instantiates a `WatermarkManager`, and at runtime
   combines incoming watermarks across inputs using the declared
   `WatermarkCombinationPolicy` (one of `ANY`, `ALL`, or a custom
   `WatermarkCombinationFunction`).

## Tests

Five tests, all small:

- `SlotSharingGroupTest` — builder validation, resource equality.
- `WatermarkDeclarationsTest` — round-trips of declarations.
- `Tuple2Test` — sanity check that tuple equality / hashCode / copy work.
- `MemorySizeTest` + `MemorySizePrettyPrintingTest` — parser and
  formatter round-trips ("1024" ↔ "1 kb", "1.5g", etc.).

JUnit 5, no Flink mini-cluster needed. Runs in milliseconds.

## Pitfalls & gotchas

- **Don't add heavyweight deps here.** The whole point of the module is
  to be loadable without Kryo / Jackson / Commons. Adding any of them
  bloats every Flink user job.
- **Two `SlotSharingGroup` classes coexist** — one in this jar
  (`org.apache.flink.api.common.SlotSharingGroup`) and one in
  `flink-core` with the same FQN. The duplicate is intentional during
  DataStream V1 → V2 migration; IDE auto-import can pick the wrong one.
  The class JavaDoc explicitly calls this out.
- **`Function` is empty on purpose.** Don't add abstract methods to it —
  doing so would make every subinterface non-SAM and break lambda usage
  everywhere.
- **`TypeDescriptor` is a deliberately weak handle.** It is *not*
  `TypeInformation`. Code that needs serializers must go through
  `flink-core` to upgrade a descriptor to a full type info.
- **State v1 vs v2 packages are not interchangeable.** `v2.State` is
  async (`StateFuture`-returning), `state.State` is sync. Operators that
  pick one cannot reuse the other's declarations.
- **`@Experimental` covers most of the new surface** — state declarations,
  watermark declarations, `TypeDescriptor`, DSv2 `Source`/`Sink`. Expect
  breaking changes between minor releases. The legacy bits
  (`Function`, `KeySelector`, tuples, `MemorySize`) are `@Public` or
  `@PublicEvolving`.
- **Tuple arity ceiling is 25.** `Tuple.MAX_ARITY = 25`; anything wider
  needs a POJO or row type.
- **`util/TaggedUnion`** is `@Internal` — it lives here only because
  `flink-streaming-java` co-group / two-input window operators need it
  without pulling in flink-core's util.

## Related modules / docs

- `flink-annotations/CLAUDE.md` — every type here is annotated with one
  of the stability annotations defined there.
- `flink-architecture-tests/CLAUDE.md` — `ApiAnnotationRules` enforces
  that every class in `org.apache.flink..api..` (i.e. essentially this
  whole jar) is properly annotated and that public methods don't leak
  internal types.
- `flink-core` (no CLAUDE.md yet) — the bigger sibling that implements
  the type / serializer / configuration machinery these interfaces are
  designed to plug into.
- `flink-datastream-api` (no CLAUDE.md yet) — DataStream V2's user-facing
  API jar, the most direct consumer of the DSv2 placeholders here
  (`Source`, `Sink`, watermark V2, state V2).
- `codedocs/flink-serialization-deep-dive.md` — explains how
  `TypeDescriptor` here relates to `TypeInformation` / `TypeSerializer`
  in flink-core.
- `codedocs/flink-state-ttl-architecture.md` — uses the state declarations
  defined here.
