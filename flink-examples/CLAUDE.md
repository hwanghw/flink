# `flink-examples`

## Purpose

Aggregator (Maven `packaging=pom`) for the user-facing example programs that
ship with the Flink distribution. Each sub-module produces one or more
self-contained example JARs (one per `<main-class>`) which `flink-dist`
copies into `build-target/examples/{streaming,table}` and into the binary
release tarball under `examples/`. The examples double as IT-case fixtures
and as the canonical "hello world" code that the Flink docs link to.

The parent pom is light: it only pulls `flink-core`, log4j and
`flink-test-utils-junit` for inherited test setup, then sets
`japicmp.skip=true` (no public API guarantees on example classes) and skips
dependency-convergence enforcement.

## Where it fits

- Built late in the reactor — depends on most of `flink-streaming-java`,
  `flink-table-*`, `flink-connector-*`, all state backends, plus
  `flink-clients` so users can run example JARs directly.
- Consumed by `flink-dist` (binary tarball + `build-target/examples/`) and
  by `flink-end-to-end-tests` which exercise the example JARs as part of
  the release validation.
- Not consumed by any production runtime module — purely outbound.

## Maven coordinates

Parent: `org.apache.flink:flink-examples:2.3-SNAPSHOT` (pom).

Sub-modules declared in `pom.xml`:

```xml
<modules>
    <module>flink-examples-streaming</module>
    <module>flink-examples-table</module>
    <module>flink-examples-build-helper</module>
</modules>
```

- **`flink-examples-streaming`** — DataStream API examples. The bulk of the
  example surface lives here, including the V2-V5 two-phase aggregation
  series described below.
- **`flink-examples-table`** — Table API / SQL examples. Artifact is
  `flink-examples-table_${scala.binary.version}` (Scala-suffixed because the
  pom pulls `flink-table-planner_${scala.binary.version}` directly to
  sidestep an IntelliJ classloader bug — see comment in the pom).
- **`flink-examples-build-helper`** — Re-packaging helper. Only contains the
  sub-module `flink-examples-streaming-state-machine`, which shades the
  `statemachine` package + Kafka client into a single fat JAR
  (`StateMachineExample.jar`) for the binary tarball.

## Dependencies

### Direct (parent pom)

- `flink-core` (compile)
- `log4j-slf4j-impl`, `log4j-api`, `log4j-core` (compile — so examples log
  out of the box from the IDE)
- `flink-test-utils-junit` (inherited test scope)

### Used by

- `flink-dist` — copies the produced JARs into `examples/` of the tarball.
- `flink-end-to-end-tests` — submits these JARs against a cluster.

## Source layout

Per sub-module, see below. Pom-level conventions: every example
main-class gets its own `<execution>` in `maven-jar-plugin` (or
`maven-shade-plugin` for fat-JAR examples that need the datagen
connector / GPU bindings); a `maven-antrun-plugin` `rename` execution
copies the classifier'd JAR to the short name used in the tarball
(`WordCount.jar` etc.). The streaming pom skips checkstyle/spotless/RAT
and adds `--add-opens=java.base/java.util=ALL-UNNAMED` to surefire (for
DataStream V2 reflection on JDK 17).

## Sub-module deep-dives

### `flink-examples-streaming` — DataStream API examples

Top-level files under
`src/main/java/org/apache/flink/streaming/examples/`:

| File / package | What it shows |
| -------------- | ------------- |
| `wordcount/WordCount.java` (+ `util/CLI`, `util/WordCountData`) | Canonical batch + streaming word count over `--input` files or built-in data. Has its own JAR execution. |
| `socket/SocketWindowWordCount.java` | Word count over a `socketTextStream` with a tumbling processing-time window. |
| `windowing/TopSpeedWindowing.java` (+ `util/CarGeneratorFunction`) | Continuous over-speed report — event-time tumbling window driven by `datagen`. Fat-shaded JAR. |
| `windowing/SessionWindowing.java` | Session windows on a small finite source. Fat-shaded JAR. |
| `windowing/WindowWordCount.java`, `GroupedProcessingTimeWindowExample.java` | Smaller window demos. |
| `join/WindowJoin.java`, `WindowJoinSampleData.java` | Tumbling window join of two finite streams. |
| `async/AsyncIOExample.java`, `AsyncClient.java` | `AsyncDataStream` with a fake async client. Fat-shaded JAR. |
| `sideoutput/SideOutputExample.java` | Splitting late events via `OutputTag`. |
| `statemachine/` (root `StateMachineExample.java` + `dfa/` + `event/` + `generator/` + `kafka/`) | DFA-based fraud detection; consumed by `flink-examples-build-helper` to produce `StateMachineExample.jar`. |
| `datagen/DataGenerator*.java` | Helpers showing the FLIP-27 `DataGeneratorSource` API. |
| `gpu/MatrixVectorMul.java` | JCuda-based matrix multiply; needs `jcuda.version` aligned with the host CUDA toolkit. Excluded from default fat-shade. |
| `dsv2/` (sub-packages `wordcount`, `windowing`, `join`, `eventtime`, `watermark`) | New DataStream **V2** API examples — uses `flink-datastream-api`. |
| `utils/ThrottledIterator.java` | Helper to throttle source rates in the demos. |

**Custom-aggregation / interview-style examples (recent additions)** —
sit at the package root (not under any sub-dir):

| File | What it shows |
| ---- | ------------- |
| `SkewedAggregation.java` | Salted keyBy + two-phase windowed count to mitigate hot-key skew. |
| `SaltedUrlDeduplication.java` | URL dedup with salted key + per-subtask local bloom-like buffer. |
| `WatermarkAwareDeduplicator.java` | Dedup that respects watermarks and emits on window close. |
| `BlacklistFilterBroadcastState.java` | Broadcast-state filter pattern. |
| `Top10ClickedAds.java` | Top-N over tumbling event-time windows. |
| `BidWinJoin.java`, `BidWinCoProcessJoin.java`, `BidWinIntervalJoin.java`, `BidWinBuiltinIntervalJoin.java`, `IntervalJoinSource.java` | Four side-by-side ways to implement the same bid/win RTB join: hand-rolled window join, `CoProcessFunction`, hand-rolled interval join, and the built-in `KeyedStream.intervalJoin`. Shares one source class. |

**Two-phase count series (V2 → V5)** — five progressively-different
implementations of the *same* logical job ("dedup-by-subtask count per key
per tumbling event-time window with allowed lateness"):

| File | API used | Notes |
| ---- | -------- | ----- |
| `TwoPhaseCountDeduplicatedEventTimeV2.java` | DataStream, two shuffles | Hand-rolled. `keyBy(key+subtaskIdx)` for the local phase, then `keyBy(key)` for global. Dedup via accumulating-mode + `ListState` FIFO. Reference implementation. |
| `TwoPhaseCountDeduplicatedEventTimeV3.java` | DataStream, one shuffle | Non-keyed `ProcessFunction` + operator `ListState` (`CheckpointedFunction`) for the local phase — no local shuffle. |
| `TwoPhaseCountDeduplicatedEventTimeV4.java` | **Table API** | Same job expressed declaratively. `table.optimizer.agg-phase-strategy=TWO_PHASE` causes the planner to insert `StreamPhysicalLocalWindowAggregate` + `StreamPhysicalGlobalWindowAggregate`. Output is changelog (+I / -U / +U / -D). |
| `TwoPhaseCountDeduplicatedEventTimeV5.java` (+ `CountStarAggsHandleFunctionV5`, `EventToRowDataMapV5`, `GlobalWindowAggregateFunctionV5`, `KeyExtractorV5`, `LocalWindowAggregateOperatorV5`) | DataStream, mirrors V4 internals | Hand-written equivalent of what Table API codegen produces: an `AggsHandleFunction` for `COUNT(*)` and a non-keyed local pre-aggregator + keyed global aggregator with retraction-style refire on late events. **Pedagogical anchor for "what does the planner actually do?".** |

V4 and V5 are the reason the streaming pom adds Table API deps
(`flink-table-api-java-bridge`, `flink-table-planner-loader`,
`flink-table-runtime`). V2/V3 are the reason the pom explicitly pulls
`flink-runtime` transitively (legacy `SourceFunction`).

### `flink-examples-table` — Table API / SQL examples

Source under
`src/main/java/org/apache/flink/table/examples/java/`:

| Package / class | What it shows |
| --------------- | ------------- |
| `basics/GettingStartedExample` | Minimal `TableEnvironment` setup — print a generated table. |
| `basics/WordCountSQLExample` | Word count via pure SQL. |
| `basics/StreamSQLExample` | Streaming `INSERT INTO … SELECT` over a `DataStream` bridge. |
| `basics/StreamWindowSQLExample` | Streaming windowed SQL (TVF window). |
| `basics/UpdatingTopCityExample` | Updating Top-N with retraction stream output. |
| `basics/TemporalJoinSQLExample` | Temporal table join (no JAR execution — IT case only). |
| `connectors/SocketDynamicTableFactory` + `SocketSource` + `SocketDynamicTableSource` + `ChangelogCsvFormat*` + `ChangelogCsvDeserializer` + `ChangelogSocketExample` | End-to-end example of writing a custom **dynamic table factory** + **changelog format** discovered via SPI (`META-INF/services`). |
| `functions/AdvancedFunctionsExample` + `LastDatedValueFunction` + `InternalRowMergerFunction` | User-defined aggregate / scalar functions using `RowData`. |

The pom pulls `flink-table-planner_${scala.binary.version}` (not the
loader) for IDE compatibility; it sets `failOnWarning=true` on the
compile plugin so deprecated Table API examples fail the build (keeps the
examples up-to-date).

### `flink-examples-build-helper` — fat-JAR re-packaging

Aggregator with one child:

- **`flink-examples-streaming-state-machine`** — depends on
  `flink-examples-streaming` and uses `maven-shade-plugin` to bundle just
  the `statemachine/**` classes + `flink-connector-kafka` +
  `flink-connector-datagen` into `StateMachineExample.jar` for the
  release tarball.

## Architecture & key concepts

- **One execution per example**: A single source tree produces many tiny
  JARs by classifier; `maven-jar-plugin` `<includes>` scope each JAR to
  a slice of classes; `<manifestEntries>` sets `program-class` so
  `flink run example.jar` works without `-c`.
- **Self-contained data generation**: Examples that don't read from a
  user-provided file use `flink-connector-datagen`'s
  `DataGeneratorSource` (FLIP-27) or a legacy `SourceFunction` — they
  run end-to-end with no external dependencies.
- **Tests are IT cases**: `StreamingExamplesITCase`, `DSv2ExamplesITCase`,
  `SocketWindowWordCountITCase`, `TopSpeedWindowingExampleITCase` plus
  the V5 unit-style test
  `examples/aggregation/TwoPhaseCountDeduplicatedEventTimeV5Test` all
  run `main(…)` on a `MiniCluster` and assert output.

## How to run

From the repo root:

```bash
# Build all examples (skip tests for speed)
mvn -pl flink-examples -am package -DskipTests

# Resulting short-named JARs land under each module's target/
ls flink-examples/flink-examples-streaming/target/*.jar
ls flink-examples/flink-examples-table/target/*.jar

# Run one against a local cluster
./bin/flink run flink-examples/flink-examples-streaming/target/WordCount.jar
```

From an IDE: just run the `main(…)` method. The streaming pom pulls
`flink-clients` so `StreamExecutionEnvironment.getExecutionEnvironment()`
returns a `LocalStreamEnvironment` automatically.

## Tests

- `flink-examples-streaming/src/test/java/...StreamingExamplesITCase`
  runs the suite of streaming examples on a `MiniCluster`.
- `flink-examples-streaming/src/test/java/...DSv2ExamplesITCase` covers
  the DataStream V2 examples; reference CSVs live under
  `src/test/resources/datas/dsv2/`.
- `flink-examples-streaming/src/test/java/...aggregation/TwoPhaseCountDeduplicatedEventTimeV5Test`
  drives the V5 pipeline with a deterministic source + explicit
  watermarks (`parallelism=1`) and asserts the late-data refire pattern.
- `flink-examples-table/src/test/java/...` has an IT case per Table
  example (`StreamSQLExampleITCase`, `WordCountSQLExampleITCase`, etc.)
  plus the shared `utils/ExampleOutputTestBase`.

## Pitfalls & gotchas

- **`flink-examples-table` artifactId is Scala-suffixed**
  (`flink-examples-table_2.12`) but the directory is plain. The pom
  pulls `flink-table-planner_…` rather than the loader (IntelliJ
  classloader bug IDEA-93855, see comment in pom).
- **JAR-per-example shading is fragile**: Adding a new main class
  requires a `maven-jar-plugin`/`maven-shade-plugin` `<execution>` *and*
  a `maven-antrun-plugin` `<copy>` line. The `<includes>` must cover
  every referenced helper class — easy to silently miss one.
- **GPU example is opt-in**: `MatrixVectorMul` excludes JCuda natives;
  end-users rebuild with their own `jcuda.version`.
- **State-machine fat JAR is built in `flink-examples-build-helper`**,
  not in `flink-examples-streaming`. Easy to look in the wrong place
  for `StateMachineExample.jar`.
- **Streaming pom skips style checks**: New files won't be caught by
  checkstyle/spotless/RAT in CI; keep license headers and formatting
  consistent manually.

## Related modules / docs

- **Upstream**: `flink-streaming-java`, `flink-table-api-java-bridge`,
  `flink-table-planner-loader`, `flink-table-runtime`,
  `flink-connector-datagen`, `flink-connector-kafka`,
  `flink-connector-files`, `flink-csv`, `flink-statebackend-rocksdb`,
  `flink-statebackend-forst`.
- **Downstream**: `flink-dist` (tarball), `flink-end-to-end-tests`.
- **Walkthroughs**: `flink-walkthroughs` — guided archetype-style projects
  that grow into the same shape as these examples but ship as Maven
  archetypes rather than ready-built JARs.
- **Codedocs**:
  - `codedocs/flink-table-api-skew-aggregation-deep-dive.md` — companion
    write-up for the V4/V5 two-phase Table API examples.
  - `codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md` —
    relevant when running the V2/V3/V5 examples on RocksDB.
  - `codedocs/flink-accumulating-vs-purging-and-allowed-lateness.md` —
    background for the allowed-lateness behaviour of V2-V5 and the
    `BidWin*` join examples.
  - `codedocs/flink-blacklist-filtering-solutions.md` — companion to
    `BlacklistFilterBroadcastState`.
  - `codedocs/flink-interview-click-analytics.md` /
    `flink-interview-fraud-detection.md` — narrative behind
    `Top10ClickedAds`, `SaltedUrlDeduplication`, and the `statemachine`
    example.
