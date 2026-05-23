# `flink-table`

## Purpose

The Table API & SQL stack for Flink — declarative streaming and batch queries expressed in SQL or a fluent Java/Scala DSL, compiled by a Calcite-based planner into Flink ExecNode graphs and executed on top of the DataStream / DataSet runtime. This is the **recommended high-level interface** in Flink 2.x; nearly everything new (lookups, joins, async I/O on tables, materialized tables, ML predict, vector search) lands here first.

## Where it fits

```
            ┌──────────────────────────────────────────────────────────────┐
            │  USER FACE                                                   │
            │  SQL string, Table DSL (Java/Scala), CompiledPlan JSON       │
            └──────────────────────────────────────────────────────────────┘
                            │
                            ▼
            ┌──────────────────────────────────────────────────────────────┐
            │  flink-table-api-*  (this module's "front")                  │
            │  TableEnvironment / StreamTableEnvironment / Expressions     │
            └──────────────────────────────────────────────────────────────┘
                            │
                            ▼
            ┌──────────────────────────────────────────────────────────────┐
            │  flink-table-planner  (Calcite optimizer + ExecNode graph)   │
            │  • SqlNode (parser, flink-sql-parser)                        │
            │  • RelNode (logical plan)                                    │
            │  • StreamPhysicalRel (physical plan)                         │
            │  • StreamExecNode (serializable plan, this is the "JSON")    │
            └──────────────────────────────────────────────────────────────┘
                            │
                            ▼
            ┌──────────────────────────────────────────────────────────────┐
            │  flink-table-runtime  (operators)                            │
            │  MapBundleOperator, WindowAggOperator, codegen'd AggsHandle  │
            └──────────────────────────────────────────────────────────────┘
                            │
                            ▼
            ┌──────────────────────────────────────────────────────────────┐
            │  flink-streaming-java + flink-runtime  (StreamGraph→JobGraph)│
            └──────────────────────────────────────────────────────────────┘
```

The Table API sits **on top of** flink-streaming-java; user-level SQL/Table queries are eventually compiled into ordinary `Transformation`s that build a `StreamGraph` like any DataStream program. The bridge layer (`flink-table-api-java-bridge`) is the seam — call `StreamTableEnvironment.fromDataStream(...)` / `toDataStream(...)` to cross it.

## Maven coordinates

Aggregator (`packaging=pom`). Calcite 1.36.0, Janino 3.1.10, Guava 33.4.0-jre, Quartz 2.3.2 are pinned at this level (`pom.xml:80-88`).

### Sub-modules

#### API sub-modules

- **`flink-table-common`** — the *zero-Calcite* foundation. Defines the type system (`DataType`, `LogicalType`, `RowType`, …), the catalog SPIs (`Catalog`, `CatalogTable`, `CatalogPartition`), the connector SPIs (`DynamicTableSource`, `DynamicTableSink`, `LookupTableSource`, `ScanTableSource`), the factory framework (`Factory`, `DynamicTableFactory`, `FormatFactory`), and runtime-visible user-facing types (`Row`, `RowKind`, `TableException`). **No planner dependency** — connectors compile against this only. Packages: `org.apache.flink.table.{catalog,connector,data,factories,functions,module,types}`.

- **`flink-table-api-java`** — Java DSL frontends and configuration. `TableEnvironment` (the unified Stream+Batch entrypoint), `Table` (the relation), `Expressions` (the column DSL), `EnvironmentSettings`, `TableConfig`, and the *config option classes* (`ExecutionConfigOptions`, `OptimizerConfigOptions`, `TableConfigOptions`, `MaterializedTableConfigOptions`). Also hosts the planner-loader plumbing (`PlannerModule.java` lives here so it can be reached without depending on the planner) and the `CompiledPlan` JSON entrypoint.

- **`flink-table-api-bridge-base`** — shared base between Java and Scala bridge implementations. Holds `AbstractStreamTableEnvironmentImpl`, the `DataStreamQueryOperation`, and `StreamExecutorFactory`. Pure inheritance helper module; do not depend on it directly.

- **`flink-table-api-java-bridge`** — the DataStream ↔ Table bridge for Java. `StreamTableEnvironment.fromDataStream` / `toDataStream` / `toChangelogStream` lives here. This is what users in DataStream code import to wrap a stream into a Table.

- **`flink-table-api-java-uber`** — pre-shaded fat-jar bundling `flink-table-common`, `flink-table-api-java`, `flink-table-api-java-bridge` (plus selected externals). Use as the only flink-table dependency in user projects; intended to insulate user code from internal layering churn.

- **`flink-table-api-scala`** + **`flink-table-api-scala-bridge`** — Scala equivalents of the Java DSL and bridge. Largely a thin Scala-implicit veneer.

#### Planner sub-modules

- **`flink-table-planner`** — the Calcite-based optimizer, **the big one** (~hundreds of thousands of lines). Holds:
  - `o.a.f.table.planner.delegation.{PlannerBase, StreamPlanner, BatchPlanner}` — the orchestration entrypoints (Scala)
  - `o.a.f.table.planner.plan.optimize.program.FlinkStreamProgram` — the rule-program assembly for streaming
  - `o.a.f.table.planner.plan.rules.{logical,physical}` — the Calcite rules (Java and Scala)
  - `o.a.f.table.planner.plan.nodes.exec.stream.StreamExec*` — the ExecNodes (Java; serializable, version-pinned via `@ExecNodeMetadata`)
  - `o.a.f.table.planner.plan.nodes.physical.stream.StreamPhysical*` — the physical RelNodes (Scala)
  - `o.a.f.table.planner.codegen.*` — operator codegen (Scala; produces `Generated*Function` instances)
  - `o.a.f.table.planner.calcite.*` — Flink-side Calcite integration (FlinkPlannerImpl, custom type factories)

- **`flink-table-planner-loader`** — three tiny `Delegate*Factory` classes (`BaseDelegateFactory`, `DelegatePlannerFactory`, `DelegateExecutorFactory`) that forward `Factory.create(...)` calls into a planner instance loaded inside a `PlannerComponentClassLoader` (a `ComponentClassLoader` with overridden URL-add behavior — see `PlannerModule.java` in `flink-table-api-java`, lines ~97-180). This is the classloader-isolation seam: user code, connectors, and Flink core only see `flink-table-api-java` types; Calcite, the planner internals, and their shaded transitive deps live in a *separate* classloader that can never leak. **Always depend on `flink-table-planner-loader`, never on `flink-table-planner` directly.**

- **`flink-table-planner-loader-bundle`** — bundling pom that packages the planner-loader plus the shaded planner JAR for distribution layout. Empty `src/main/resources`.

- **`flink-table-calcite-bridge`** — public-API surface for components that *do* need to reach Calcite from outside the planner classloader. Two files: `CalciteContext` (an SPI for embedding alternate Calcite contexts) and `PlannerExternalQueryOperation`. Used by Hive, materialized tables, and a few legacy bridges.

- **`flink-sql-parser`** — the Calcite-generated parser, with Flink customizations sitting in `org.apache.calcite.sql.*` (re-exported customizations like `SqlMapTypeNameSpec`, `SqlExplicitModelCall`, `SqlJsonQueryFunction`) and `org.apache.flink.sql.parser.*` (Flink-only DDL, validators, parse helpers). Codegen lives under `src/main/codegen/` — the `.jj` template is fed through `JavaCC` to produce `FlinkSqlParserImpl`. Tests assert grammar coverage in `FlinkSqlParserImplTest`.

#### Runtime sub-modules

- **`flink-table-runtime`** — the operators and supporting infrastructure that actually run at task-execution time. Highlights:
  - `o.a.f.table.runtime.operators.bundle.{MapBundleOperator, KeyedMapBundleOperator, AbstractMapBundleOperator, MapBundleFunction}` — the mini-batch substrate; buffer N records, apply once.
  - `o.a.f.table.runtime.operators.aggregate.{GroupAggFunction, MiniBatchGroupAggFunction, MiniBatchLocalGroupAggFunction, MiniBatchGlobalGroupAggFunction, MiniBatchIncrementalGroupAggFunction, GroupTableAggFunction}` — the unbounded-aggregate "business logic" plugged into `MapBundleOperator`.
  - `o.a.f.table.runtime.operators.aggregate.window.{LocalSlicingWindowAggOperator, WindowAggOperatorBuilder, processors.*, combines.{AggCombiner,LocalAggCombiner,GlobalAggCombiner}}` — windowed aggregation, the "slicing" path.
  - `o.a.f.table.runtime.operators.window.tvf.{common.WindowAggOperator, slicing.{SliceAssigner,SliceAssigners,SlicingWindowTimerServiceImpl}, unslicing.*, state.*, asyncprocessing.*}` — the Window TVF runtime (the modern path: `TABLE(TUMBLE(...))`, `TABLE(HOP(...))`, `TABLE(CUMULATE(...))`).
  - `o.a.f.table.runtime.operators.{join,deduplicate,rank,over,sort,match,correlate,calc,sink,source,values,wmassigners,multipleinput,fusion,process,ml,search,runtimefilter,dynamicfiltering,misc}` — everything else (regular join, interval join, lookup join, async lookup, dedup, top-N, OVER, CEP MATCH_RECOGNIZE, vector search, ML predict).
  - `o.a.f.table.runtime.generated.{Generated*, *HandleFunction, CompileUtils}` — the **codegen surface**. `AggsHandleFunction`, `NamespaceAggsHandleFunction`, `TableAggsHandleFunction`, `JoinCondition`, `WatermarkGenerator`, `RecordEqualiser`, `RecordComparator`, `NormalizedKeyComputer` are all interfaces implemented by Janino-compiled classes.
  - `o.a.f.table.data.{binary,util,*}` — runtime-side row implementations (`BinaryRowData`, `BoxedWrapperRowData`, `UpdatableRowData`).

- **`flink-table-code-splitter`** — rewrites very large Janino-generated source files (which Janino can't compile due to JVM 64KB method-size limits) into smaller pieces. JavaCC-driven; the `src/test/resources/{declaration,member,...}` folders ship sample inputs and expected outputs.

#### SQL tooling sub-modules

- **`flink-sql-client`** — interactive REPL (`sql-client.sh`). Entry point: `o.a.f.table.client.SqlClient` (CLI bootstrap). Sub-packages: `cli` (terminal UI, JLine), `config`, `gateway` (embedded gateway mode), `resource`, `util`. Speaks to the gateway over its REST API in remote mode or boots an embedded gateway in local mode.

- **`flink-sql-gateway-api`** — the stable SPI for SQL gateway implementations. Sub-packages: `endpoint` (`SqlGatewayEndpoint`, `SqlGatewayEndpointFactory`), `session`, `operation`, `results`, `config`, `utils`. Third-party endpoints (e.g. a Hive-server compatible front, REST clients) implement these.

- **`flink-sql-gateway`** — the reference implementation. `SqlGateway` (entry-point), `SqlGatewayServiceImpl` (the central service), `rest.SqlGatewayRestEndpoint` + `SqlGatewayRestEndpointFactory` (HTTP layer), plus `workflow` (materialized-table refresh scheduling, Quartz-backed).

- **`flink-sql-jdbc-driver`** — a JDBC type-4 driver that talks to the gateway's REST API. `FlinkDriver` registers under `jdbc:flink://...`. The `Base*` classes (`BaseConnection`, `BaseStatement`, `BaseResultSet`, `BaseDatabaseMetaData`) supply UnsupportedOperationException defaults that the `Flink*` concrete classes selectively override.

- **`flink-sql-jdbc-driver-bundle`** — fat-jar shading for the JDBC driver. No source (`src/` absent); `pom.xml` only.

#### Test support

- **`flink-table-test-utils`** — assertion helpers (`TableAssertions`) and test fixtures. Used in tests across the stack.

## Dependencies (aggregator-level)

This is a `pom`-only module; it has no direct code dependencies. It pins external versions (Calcite 1.36.0, Janino 3.1.10, Guava 33.4.0, Quartz 2.3.2) in `<dependencyManagement>` for the children.

**Who depends on flink-table from outside this tree**:
- `flink-connectors/*` — every connector implements `flink-table-common` SPIs (`DynamicTableFactory`, `ScanTableSource`, `LookupTableSource`, `Format*`).
- `flink-formats/*` — `EncodingFormat` / `DecodingFormat` SPIs from `flink-table-common`.
- `flink-python` — Python UDFs translate through the planner.
- `flink-libraries/flink-state-processing-api` — uses Table API for catalog/savepoint inspection.
- `flink-end-to-end-tests`, `flink-yarn-tests`, `flink-tests` — SQL/Table end-to-end coverage.

## Architecture overview

### The compile pipeline (streaming)

```
SQL string ──► SqlNode tree                            (flink-sql-parser, JavaCC)
              │
              │  SqlToRelConverter + FlinkPlannerImpl
              ▼
           RelNode (FlinkLogical*)                     (flink-table-planner, Calcite)
              │
              │  FlinkStreamProgram (volcano + hep)
              │   • subquery removal, decorrelation
              │   • projection / filter pushdown
              │   • SplitAggregateRule  (logical, distinct-bucket rewrite)
              │   • watermark assigner, miniBatch interval propagation
              ▼
           StreamPhysical* RelNode                     (flink-table-planner)
              │
              │  Trait-based physical optimization:
              │   • TwoStageOptimizedAggregateRule       (unbounded)
              │   • TwoStageOptimizedWindowAggregateRule (windowed)
              │   • IncrementalAggregate rewrites
              │   • exchange-elimination, change-tracking
              ▼
           StreamExecNode graph                        (StreamExec*GroupAggregate, …)
              │       ↑
              │       └── serializable via @ExecNodeMetadata; this is the JSON plan
              │
              │  translateToPlanInternal(...)  +  codegen
              ▼
           Transformation graph                        (flink-streaming-java)
              │  StreamGraphGenerator
              ▼
           StreamGraph → JobGraph → ExecutionGraph     (flink-runtime)
              │
              ▼
           runtime operators                           (flink-table-runtime)
           e.g. MapBundleOperator wrapping
                MiniBatchGlobalGroupAggFunction
                holding a codegen'd AggsHandleFunction
```

### Key optimization rules

Most rule files live under `flink-table-planner/src/main/{java,scala}/org/apache/flink/table/planner/plan/rules/`. The ones you will hit most often:

| Rule | Layer | What it does |
| ---- | ----- | ------------ |
| `SplitAggregateRule` | logical | Rewrites `COUNT(DISTINCT x) GROUP BY g` into a two-level aggregate keyed by `(g, hash(x) % N)`, breaking up distinct hot-spots. (`rules/logical/SplitAggregateRule.java`, 604 LOC) |
| `TwoStageOptimizedAggregateRule` | physical (stream) | Splits a `StreamPhysicalGroupAggregate` over an `Exchange` into Local+Global. Requires mini-batch. (`rules/physical/stream/TwoStageOptimizedAggregateRule.java`, 223 LOC) |
| `TwoStageOptimizedWindowAggregateRule` | physical (stream) | Same shape as above, but for windowed aggregates. Requires rowtime, rejects session windows, requires all aggs to implement `merge()`. **Does not require mini-batch** — slicing is its own batching. |
| `IncrementalAggregateRule` | physical (stream) | Pushes partial aggregation across an exchange when the global side itself feeds another aggregate (e.g. `COUNT(DISTINCT …)` inside Split-Aggregate). |
| `StreamPhysicalGroupAggregateRule` | physical conversion | Lowers `FlinkLogicalAggregate` to its physical counterpart. |
| `StreamLogicalWindowAggregateRule` | logical | Recognizes `GROUP BY TUMBLE(...) / HOP(...) / SESSION(...)` and produces a windowed logical aggregate. (Legacy window path; the Window TVF path goes through dedicated rules.) |
| `FlinkSubQueryRemoveRule`, `JoinConditionEqualityTransferRule`, `ConvertToNotInOrInRule` | logical | Calcite-style pattern cleanups. |
| `PushPartitionIntoLegacyTableSourceScanRule`, `PushProjectInto…Rule` | logical | Source-side pushdown (legacy + new connector both have variants). |

The full rule sets are wired by `FlinkStreamRuleSets.scala` and assembled into a sequenced program by `FlinkStreamProgram.scala`.

### Codegen

The planner generates Janino source for hot-path code rather than reflecting on it. The main entry point for aggregates is **`AggsHandlerCodeGenerator`** (Scala, `flink-table-planner/.../codegen/agg/AggsHandlerCodeGenerator.scala`). It emits one of:

- `GeneratedAggsHandleFunction` → unbounded (no namespace)
- `GeneratedNamespaceAggsHandleFunction` → windowed (window is the namespace)
- `GeneratedTableAggsHandleFunction` / `GeneratedNamespaceTableAggsHandleFunction` → table-valued aggregates

Each `Generated*` carries the source as a string. At task `open()` time the runtime calls `CompileUtils.compile(...)` which delegates to Janino, returning a `Class<? extends AggsHandleFunction>`. The compiled class is held by the operator and called from the bundle/window hot path.

When the generated source exceeds JVM method-size limits, **`flink-table-code-splitter`** rewrites it (splitting long methods, large literal arrays, …) before Janino sees it.

### Mini-batch, late-fire, allow-lateness

The runtime knobs that change *when* operators emit are all `table.exec.*` config options on `ExecutionConfigOptions`:

- `table.exec.mini-batch.enabled` + `.allow-latency` + `.size` — buffer-then-aggregate; the prerequisite for unbounded Local-Global.
- `table.exec.emit.early-fire.enabled` + `.delay` — emit window result before close.
- `table.exec.emit.late-fire.enabled` + `.delay` — re-fire a window on late records still within allowed lateness.
- `table.exec.emit.allow-lateness` — how far past `window-end` to keep state alive.

**See `codedocs/flink-table-api-skew-aggregation-deep-dive.md` for an end-to-end walk-through of how mini-batch, Local-Global, and Split-Distinct interact, including line-precise references into the rule + runtime classes.**

## Important public APIs

| Class | Module | Use |
| ----- | ------ | --- |
| `TableEnvironment` | `flink-table-api-java` | Pure Table API entry — execute SQL, register catalogs/functions, build `Table`s. |
| `StreamTableEnvironment` | `flink-table-api-java-bridge` | DataStream interop — `fromDataStream`, `toDataStream`, `toChangelogStream`. |
| `Table`, `Expressions` | `flink-table-api-java` | The fluent DSL. |
| `TableConfig` | `flink-table-api-java` | Per-environment config; wraps a `Configuration` and exposes type-safe getters/setters. |
| `ExecutionConfigOptions` | `flink-table-api-java/api/config` | Runtime knobs (mini-batch, emit, state TTL, async lookup, sink upsert materialize). |
| `OptimizerConfigOptions` | `flink-table-api-java/api/config` | Planner knobs (`agg-phase-strategy`, `join-reorder.enabled`, `distinct-agg.split.enabled`, source predicate pushdown). |
| `TableConfigOptions` | `flink-table-api-java/api/config` | Catalog / dialect / plan-compilation knobs. |
| `AggregatePhaseStrategy` | `flink-table-api-java/api/config` | `AUTO` / `TWO_PHASE` / `ONE_PHASE` — controls Local-Global. |
| `EnvironmentSettings` | `flink-table-api-java` | Builder selecting Stream vs Batch mode, catalog name, etc. |
| `CompiledPlan` | `flink-table-api-java` | Serialize a Job plan to JSON; reload across versions per `@ExecNodeMetadata`. |
| `DynamicTableFactory` + family | `flink-table-common` | The SPI every connector implements. |

## Internal flows

### "SQL string → JobGraph"

1. `TableEnvironmentImpl.executeSql(str)` → `Planner.parse(str)` → `SqlNode` (via `flink-sql-parser`).
2. `Planner.translate(...)` validates against the catalog, produces `RelNode`s.
3. `FlinkStreamProgram` runs the rule program (logical → physical) — Calcite Volcano + Hep stages.
4. Physical RelNodes are converted to `ExecNode`s; the planner now holds an `ExecNodeGraph` (serializable, version-pinned).
5. `ExecNode.translateToPlanInternal(planner)` walks the graph in topological order, calling per-node `translateToPlanInternal` that produces a `Transformation<RowData>` and chains operators via `flink-streaming-java`.
6. Codegen happens inline during translation: e.g. `StreamExecGlobalGroupAggregate.translateToPlanInternal` calls `AggsHandlerCodeGenerator` and embeds the `GeneratedAggsHandleFunction` into the operator factory.
7. The `Transformation` list is handed back to `StreamGraphGenerator` (flink-streaming-java).
8. From there, the path is identical to a DataStream program: `StreamGraph → JobGraph → ExecutionGraph`.

### "Mini-batch local-global rewrite" (unbounded)

`SELECT k, COUNT(*) FROM t GROUP BY k` with `table.exec.mini-batch.enabled=true` and `table.optimizer.agg-phase-strategy != ONE_PHASE`:

1. **Logical**: `FlinkLogicalAggregate(k, COUNT(*))` over `FlinkLogicalTableSourceScan(t)`.
2. **Physical**: `StreamPhysicalGroupAggregate` over `StreamPhysicalExchange[hash by k]` over scan.
3. **TwoStageOptimizedAggregateRule** matches the `Aggregate→Exchange` pattern and rewrites to:
   ```
   StreamPhysicalGlobalGroupAggregate
     +- StreamPhysicalExchange[hash by k]
        +- StreamPhysicalLocalGroupAggregate
           +- scan
   ```
4. **ExecNode**: `StreamExecGlobalGroupAggregate` and `StreamExecLocalGroupAggregate`.
5. **Runtime**:
   - Local side: `MapBundleOperator` wrapping `MiniBatchLocalGroupAggFunction`. No keyed state — pure in-memory bundle pre-aggregate. Chained with the upstream source (no shuffle).
   - Network exchange (hash by `k`).
   - Global side: `KeyedMapBundleOperator` wrapping `MiniBatchGlobalGroupAggFunction`. Holds `ValueState<RowData>` keyed by `k`; merges per-batch into state, emits the changelog.

The full walkthrough — including the SplitAggregateRule rewrite for `COUNT(DISTINCT)` and the slice-based windowed path — is in **`codedocs/flink-table-api-skew-aggregation-deep-dive.md`**.

## Tests

- **Planner tests**: `flink-table-planner/src/test/{java,scala}/.../plan/{batch,stream}/sql/...` — golden-file plan tests (`@TestTemplate` + `*.xml` golden files). Per-rule tests verify the rewrite matches. Operator harness tests (`HarnessTestBase`) exercise the runtime classes.
- **Runtime tests**: `flink-table-runtime/src/test/java/...` — operator-level (e.g. `WindowAggOperatorTest`, `MapBundleOperatorTest`).
- **SQL parser**: `flink-sql-parser/src/test/.../FlinkSqlParserImplTest.java` — every supported DDL/DML/query shape.
- **End-to-end**: `flink-end-to-end-tests/flink-sql-*-test/*` — full job-submission paths through the gateway and SQL client.
- **Test utilities**: `flink-table-test-utils` exposes `TableAssertions`. Many planner tests use `BatchTestBase` / `StreamingTestBase` (set up `tEnv`, register sources).

## Pitfalls & gotchas

- **Never depend on `flink-table-planner` directly** in connector or library code. Use `flink-table-planner-loader`. Direct dependency punches a hole in the classloader isolation (Calcite + Guava + Janino get mixed into the user classloader), and your code will silently break when users update the planner.
- **Mini-batch must be enabled for unbounded Local-Global.** `TwoStageOptimizedAggregateRule` short-circuits if `table.exec.mini-batch.enabled=false`. The *windowed* rule does not — slicing is its own batching mechanism.
- **`table.optimizer.agg-phase-strategy=AUTO` does not gate on data statistics.** It prefers Local-Global *when applicable*. The hard off-switch is `ONE_PHASE` (line ~90 of `TwoStageOptimizedAggregateRule.java`).
- **`table.exec.emit.*` is experimental.** The emit configs change behavior of windowed aggregates (early-fire, late-fire). They are documented as experimental — expect API churn and read-the-code semantics. See the deep-dive doc for current behavior.
- **Don't mix Table API and DataStream V2.** DataStream V2 (`flink-datastream`) is a separate, parallel stack. The Table API integrates with the *V1* DataStream API via `flink-table-api-java-bridge`. Crossing into V2 territory is not supported.
- **Session windows can't be Local-Global'd.** `TwoStageOptimizedWindowAggregateRule` explicitly rejects `SessionWindowSpec` because slice boundaries depend on data.
- **Processing-time windows can't be Local-Global'd.** Same rule — requires `windowing.isRowtime()`.
- **Custom UDAFs need `merge()`** to participate in Local-Global (windowed or otherwise). `AggregateUtil.doAllSupportPartialMerge(...)` is the gate.
- **ExecNode JSON plans are version-pinned.** Bumping a `@ExecNodeMetadata` version requires keeping the previous version's deserializer for backward compat. Don't change ExecNode fields without going through that machinery.
- **Codegen failures often look like Janino exceptions at job startup.** If a query produces a massive aggregate over many distinct columns, you may hit the 64KB method limit — `flink-table-code-splitter` is supposed to handle this but it is rule-based and has edge cases. Look for `JaninoCompileException` in TM logs.
- **`flink-table-runtime` is shipped to all TMs but the planner is not.** Anything you reference from a runtime operator must be in `flink-table-runtime` or `flink-table-common`, never `flink-table-planner`. The codegen-emitted code only imports runtime-side packages.
- **The SQL gateway's session state is in-memory.** If the gateway process dies, sessions vanish. For HA, run multiple gateways behind a load balancer and pin client sessions or use the JDBC driver's connection-per-statement style.

## Related modules / codedocs

- **`codedocs/flink-table-api-skew-aggregation-deep-dive.md`** — the canonical deep-dive on mini-batch, Local-Global (unbounded *and* windowed), Split-Distinct, late-data handling in `WindowAggOperator`, line-precise references into the rule + runtime sources. **Read this first if you touch aggregation rules or window operators.**
- **`codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md`** — DataStream `WindowOperator` internals; mirror semantics for the table-side `WindowAggOperator` (`isWindowLate` / `isElementLate` in particular).
- **`codedocs/flink-accumulating-vs-purging-and-allowed-lateness.md`** — emit-mode semantics that surface through `table.exec.emit.*`.
- **`codedocs/flink-state-ttl-architecture.md`** — state TTL is set per-operator from `table.exec.state.ttl`; understand the underlying state-backend mechanics here.
- **`codedocs/flink-serialization-deep-dive.md`** — `RowData` serializers and their interaction with the `TypeSerializer` framework.
- `flink-streaming-java/CLAUDE.md` (if present) — the layer immediately below; understand `Transformation` / `StreamGraph` to debug planner translation.
- `flink-connectors/CLAUDE.md` — implementations of `flink-table-common` SPIs.
