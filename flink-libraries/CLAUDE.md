# `flink-libraries`

## Purpose

Aggregator (Maven `packaging=pom`) for optional library modules that build on
top of `flink-core` / `flink-streaming-java` / `flink-table-*`. Each
sub-module adds a focused capability and is opt-in for users; the parent pom
ships no code of its own beyond a few shared dependency overrides (slf4j,
jsr305 marked `provided`, plus `flink-test-utils-junit` for inherited tests).

In Flink 2.x the aggregator has been trimmed down significantly compared with
1.x: `flink-gelly`, `flink-gelly-scala`, `flink-cep-scala`, `flink-python`,
`flink-streaming-python` and the ML library have all been removed. Today only
two sub-modules remain.

## Where it fits

- Sits above `flink-streaming-java` / `flink-table-runtime`.
- Consumed almost exclusively by **user code** — no internal Flink module
  depends on `flink-cep` or `flink-state-processor-api`.
- Not bundled into the default lean `flink-dist` jar for user jobs; users
  add the library jar (e.g. `flink-cep`) as a dependency or drop it into
  `lib/` on the cluster.

## Maven coordinates

Parent: `org.apache.flink:flink-libraries:2.3-SNAPSHOT` (pom).

Sub-modules declared in `pom.xml`:

```xml
<modules>
    <module>flink-cep</module>
    <module>flink-state-processing-api</module>
</modules>
```

Note the directory `flink-state-processing-api/` produces the artifact
`flink-state-processor-api` (artifactId differs from directory name — easy
gotcha when grepping pom dependencies).

## Sub-module deep-dives

### flink-cep — Complex Event Processing

Pattern-matching DSL over `DataStream`s built on a serialised NFA.

**Source layout** (`src/main/java/org/apache/flink/cep`):

| Package | Responsibility |
| ------- | -------------- |
| (root) `cep` | User-facing entry points: `CEP`, `PatternStream`, `PatternStreamBuilder`, plus the `PatternSelectFunction` / `PatternFlatSelectFunction` / `PatternTimeoutFunction` interfaces and their rich variants. |
| `cep.pattern` | `Pattern` DSL builder (`begin`, `next`, `followedBy`, `where`, `or`, `times`, `within`), `Quantifier`, `GroupPattern`, `WithinType`, `MalformedPatternException`. |
| `cep.pattern.conditions` | `IterativeCondition` base, `SimpleCondition`, `SubtypeCondition`, `BooleanConditions`, plus rich composite conditions (`RichAndCondition`, `RichOrCondition`, `RichNotCondition`). |
| `cep.nfa` | NFA engine: `NFA`, `NFAState`, `State`, `StateTransition`, `ComputationState`, `DeweyNumber` (versioning of partial matches), serializers + snapshot classes. |
| `cep.nfa.compiler` | `NFACompiler` lowers a `Pattern` graph into an `NFA`. `NFAStateNameHandler` makes state names unique. |
| `cep.nfa.sharedbuffer` | The on-state `SharedBuffer` that stores in-flight partial matches keyed by `EventId` / `NodeId` with reference counting via `Lockable`. Backed by Flink keyed `MapState`. |
| `cep.nfa.aftermatch` | `AfterMatchSkipStrategy` implementations (`NoSkip`, `SkipPastLast`, `SkipToNext`, `SkipToFirst`, `SkipToLast`, `SkipRelativeToWholeMatch`). |
| `cep.operator` | `CepOperator` — the keyed `AbstractUdfStreamOperator` that runs the NFA, plus `CepRuntimeContext` and `StreamRecordComparator` for ordering. |
| `cep.functions` | `PatternProcessFunction` (newer process-function-style API) and `TimedOutPartialMatchHandler`. `functions.adaptors` bridges the older `PatternSelectFunction` API. |
| `cep.time` | `TimerService` abstraction used by the operator. |
| `cep.configuration` | `CEPCacheOptions` and `SharedBufferCacheConfig` (per-key LRU caches in front of state). |

**Key public APIs**:

- `CEP.pattern(DataStream<T>, Pattern<T,?>)` — entry point.
- `Pattern.<T>begin("name")` builder.
- `PatternStream#process(PatternProcessFunction)` / `#select` / `#flatSelect`.
- `Pattern#within(Duration, WithinType)` for time constraints (event or
  processing time, set on the operator via `PatternStream.inEventTime()` /
  `inProcessingTime()`).
- `PatternStream#sideOutputLateData(OutputTag)` for late events.

**Important flows**:

1. User builds a `Pattern` chain.
2. `NFACompiler` walks the chain and emits an `NFA` plus its serializer.
3. `CepOperator` is added to the keyed stream (`KeyedStream.transform` under
   the hood via `PatternStreamBuilder`). It keeps a `NFAState` and a
   `SharedBuffer` in Flink keyed state.
4. For every event the operator advances the NFA, evicts expired partial
   matches based on `within`, and emits completed matches to the user
   `PatternProcessFunction` (and optionally timed-out partial matches via
   side output).

**Pitfalls / gotchas**:

- State size grows with the cardinality of in-flight partial matches. Always
  set a `within(...)` window unless you really know matches are short.
- Migrations: `NFAStateSerializerSnapshot` and
  `SharedBufferNodeSerializerSnapshotV2` exist exactly because the NFA state
  layout has changed between versions; do not break their binary format
  without a migration test (see `flink-migration-test-utils` and the
  `generate-migration-test-data` profile in the pom).
- `IterativeCondition` runs once per event per state — putting heavy logic
  there scales linearly with the NFA branching factor.
- `AfterMatchSkipStrategy` is global to the pattern; choose carefully or you
  will emit overlapping matches (default is `NoSkip`).
- `SharedBufferCacheConfig` controls in-memory caching of `SharedBuffer`
  entries; tune via `CEPCacheOptions` when on RocksDB.

### flink-state-processor-api — read/write Flink savepoints out-of-band

Batch (well, bounded-stream) API for reading state out of an existing
savepoint and for bootstrapping new savepoints from arbitrary data. Used for
state migrations, debugging, testing, and bulk transformations of an existing
application's state without running the application.

**Source layout** (`src/main/java/org/apache/flink/state/api`):

| Package | Responsibility |
| ------- | -------------- |
| (root) `state.api` | Public entry points: `SavepointReader`, `SavepointWriter`, `WindowSavepointReader`, `EvictingWindowSavepointReader`, `OperatorIdentifier`, `OperatorTransformation`, `StateBootstrapTransformation`, `KeyedStateTransformation`, `OneInputStateTransformation`, `WindowedStateTransformation`, `SavepointWriterOperatorFactory`. |
| `state.api.functions` | UDF interfaces the user implements: `KeyedStateBootstrapFunction`, `StateBootstrapFunction`, `BroadcastStateBootstrapFunction`, `KeyedStateReaderFunction`, `WindowReaderFunction`, `Timestamper`. |
| `state.api.input` | Bounded sources that read savepoint state: `KeyedStateInputFormat`, `BroadcastStateInputFormat`, `ListStateInputFormat`, `UnionStateInputFormat`, `OperatorStateInputFormat`, plus `SourceBuilder`, `MultiStateKeyIterator`, `BufferingCollector`, `StreamOperatorContextBuilder`. |
| `state.api.input.operator` | Adapter operators used by the input formats: `KeyedStateReaderOperator`, `StateReaderOperator`, `WindowReaderOperator`. |
| `state.api.input.operator.window` | Window-state readers: `EvictingWindowReaderFunction`, `AggregateEvictingWindowReaderFunction`, `ReduceEvictingWindowReaderFunction`, `ProcessEvictingWindowReader`, `PassThroughReader`, `WindowContents`. |
| `state.api.input.splits` | `InputSplit` types describing chunks of a savepoint. |
| `state.api.output` | Writes state back out: `SavepointOutputFormat`, `MergeOperatorStates`, `FileCopyFunction`, `StatePathExtractor`, `OperatorSubtaskStateReducer`, `SnapshotUtils`, `TaggedOperatorSubtaskState`, `BootstrapStreamTask`, `BootstrapStreamTaskRunner`, `EndOfDataMarker`. |
| `state.api.output.operators` | Bootstrap operators: `KeyedStateBootstrapOperator`, `BroadcastStateBootstrapOperator`, `StateBootstrapOperator`, `StateBootstrapWrapperOperator(Factory)`, `GroupReduceOperator`, `LazyTimerService`. |
| `state.api.runtime` | Embedded mini-runtime used to drive a real operator without a real task: `SavepointEnvironment`, `SavepointLoader`, `SavepointRuntimeContext`, `SavepointTaskManagerRuntimeInfo`, `SavepointTaskStateManager`, `NeverFireProcessingTimeService`, `VoidTriggerable`, `OperatorIDGenerator`, `MutableConfig`, `StateBootstrapTransformationWithID`. |
| `state.api.runtime.metadata` | `SavepointMetadataV2`, `OperatorStateSpecV2` — in-memory model of `_metadata`. |
| `state.table` | Table/SQL bridge: `SavepointDynamicTableSource(Factory)`, `SavepointConnectorOptions(Util)`, `SavepointMetadataTableFunction`, `KeyedStateReader`, `SavepointTypeInformationFactory`, `SavepointDataStreamScanProvider`, `StateValueColumnConfiguration`, plus an SQL module under `state.table.module`. Lets you `SELECT` from a savepoint via Table API. |

**Key public APIs**:

- `SavepointReader.read(env, path)` (optionally with explicit `StateBackend`)
  → expose state as `DataStream`s via `readKeyedState`, `readListState`,
  `readUnionState`, `readBroadcastState`, plus window variants.
- `SavepointWriter.newSavepoint(...)` / `fromExistingSavepoint(env, path)`
  → attach `StateBootstrapTransformation`s per operator UID, then `write(path)`.
- `OperatorTransformation.bootstrapWith(DataStream)` is the fluent builder
  that turns a bounded stream into per-operator state.
- `OperatorIdentifier` (UID or UID-hash) — keys transformations to existing
  operators; mismatched UIDs are the #1 source of "state not restored".

**Important flows**:

1. **Reading**: `SavepointReader` loads `CheckpointMetadata` via
   `SavepointLoader`, derives `maxParallelism`, and for each requested state
   builds a `*StateInputFormat` whose splits cover key-group ranges. The
   format spins up a real keyed state backend inside `SavepointEnvironment`,
   replays the snapshot, and iterates entries through a
   `KeyedStateReaderFunction` (or window-reader equivalent).
2. **Writing / bootstrapping**: Each `StateBootstrapTransformation` is
   compiled into a bootstrap operator (`KeyedStateBootstrapOperator` etc.)
   wrapped by `StateBootstrapWrapperOperator`, executed inside
   `BootstrapStreamTask`. `SnapshotUtils.snapshot` triggers a synthetic
   checkpoint; `TaggedOperatorSubtaskState` rides through the job, then
   `MergeOperatorStates` + `OperatorSubtaskStateReducer` produce a single
   `OperatorState` per operator, written by `SavepointOutputFormat`.
   `FileCopyFunction` / `StatePathExtractor` handle shared/private state
   file relocation for incremental snapshots.
3. **Table integration** (`state.table`): registers a dynamic table source
   pointing at a savepoint path so SQL queries can scan state values.

**Pitfalls / gotchas**:

- **Savepoint format compatibility**: This module only reads what the active
  Flink version's checkpoint metadata format supports. Cross-version reads
  (1.x → 2.x) require the source job to have used a state backend whose
  serializer snapshots are still understood.
- **State backend must match for raw RocksDB state**: For RocksDB savepoints
  use `fromExistingSavepoint(env, path, new EmbeddedRocksDBStateBackend())`.
  Heap-backed (`HashMapStateBackend`) savepoints are usually transparent.
- **UID stability**: Bootstrapping a state for an operator that the live job
  cannot match by UID/UID-hash silently produces orphan state.
- **`@PublicEvolving`**: All entry points are `@PublicEvolving`, not
  `@Public` — minor API breaks across releases are allowed.
- **Bounded job needed**: Bootstrap requires a finite input. Mixing
  `SavepointWriter#write` with an unbounded source will hang.
- **Avro test sources**: The pom wires `avro-maven-plugin` for test-only
  generated classes under `target/generated-test-sources/avro` — don't be
  surprised by Avro-generated types showing up in test classpaths.

## Tests

Per sub-module — both sit alongside the main sources.

- `flink-cep/src/test/java/...`: ~38 files. Pattern tests, NFA tests, IT
  cases per operator behaviour (`CEPITCase`, `NFAITCase`, `GreedyITCase`,
  `NotPatternITCase`, `AfterMatchSkipITCase`, `UntilConditionITCase`),
  plus migration tests (`NFASerializerUpgradeTest`).
- `flink-state-processing-api/src/test/java/...`: ~45 files. End-to-end IT
  cases for reader/writer (`SavepointReaderITCase`, `SavepointWriterITCase`,
  `SavepointWindowReaderITCase`, `SavepointReaderUidHashITCase`),
  per-state-backend variants (`EmbeddedRocksDBStateBackendReaderKeyedStateITCase`,
  `HashMapStateBackendWindowITCase`), plus `SavepointDeepCopyTest` and
  `SavepointDynamicTableSourceTest`.

Both poms publish a `test-jar` so downstream modules can reuse the helpers
(`maven-jar-plugin` `test-jar` execution).

## Pitfalls & gotchas (cross-module)

- The directory `flink-state-processing-api/` produces artifact
  `flink-state-processor-api` — see `dependency-reduced-pom.xml` if shading
  surprises you.
- Both poms set surefire `--add-opens` flags for `java.base/java.util` (chill
  / Kryo); inheriting test setups that rely on additional opens must be
  redeclared.
- `flink-cep` and `flink-state-processor-api` both list core deps as
  `provided` — uber-jars produced by users must explicitly include them.
- Migration test data lives under `src/test/resources/` per version and is
  regenerated via the `generate-migration-test-data` Maven profile (CEP
  only). Don't hand-edit binary snapshots.
- The aggregator pulls `flink-test-utils-junit` into every sub-module's
  `compile` scope (not `test`) — this is intentional so utility classes can
  be shared, but it means library users transitively see JUnit symbols.

## Related modules / docs

- Upstream: `flink-streaming-java`, `flink-table-common`, `flink-table-runtime`,
  `flink-runtime` (for `CheckpointMetadata` and keyed state backends).
- State backends: `flink-state-backends/flink-statebackend-rocksdb` is used
  in tests of both sub-modules.
- Migration test infrastructure: `flink-migration-test-utils`.
- Codedocs covering adjacent topics:
  - `codedocs/flink-exactly-once-checkpointing-deep-dive.md` — checkpoint
    metadata format the state processor API consumes.
  - `codedocs/flink-state-ttl-architecture.md` — TTL semantics that interact
    with both CEP state and bootstrapped state.
  - `codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md` — relevant
    when reading windowed state via `WindowSavepointReader`.
  - `codedocs/flink-rocksdb-state-backend-tuning.md` — applies to CEP's
    `SharedBuffer` when running on RocksDB.
  - `codedocs/flink-serialization-deep-dive.md` — context for the many
    `*SerializerSnapshot` classes in `flink-cep`.
  - `codedocs/flink-cassandra-connector-wal-exactly-once-architecture.md`
    discusses related state-and-checkpoint concerns.
