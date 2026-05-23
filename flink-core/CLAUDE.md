# `flink-core`

## Purpose

`flink-core` is Flink's foundational module. It hosts the **type system**,
**serialization stack**, **configuration framework**, **memory abstractions**,
**filesystem abstractions**, the **common Function API**, the **Source / Sink v2
connector contracts**, and a large library of general-purpose utilities. It is
the lowest layer that contributors writing connectors, operators, formats,
state backends, or table runtime code transitively pull in. Almost every Flink
artifact depends on it.

It deliberately excludes the runtime engine (scheduling, RPC, network shuffle,
state backends, checkpoint coordination — those live in `flink-runtime` and
sibling modules).

## Where it fits

```
flink-annotations                  (Public / Internal / Experimental markers)
flink-core-api                     (minimal API surface: TypeInfo interface,
                                    state descriptors, function interfaces,
                                    java tuples, watermarks)
        ^
        |
flink-core   <-- this module       (implementation: TypeExtractor, Kryo,
                                    Configuration, FileSystem, MemorySegment,
                                    PojoSerializer, TypeSerializerSnapshot ...)
        ^
        |
flink-runtime, flink-streaming-java, flink-table-common, flink-clients,
flink-state-backends/*, flink-formats/*, flink-connectors/*, flink-metrics/*,
flink-filesystems/*, flink-libraries/*, flink-python, flink-cep, ...
```

`flink-core-api` was carved out as a *minimal* API surface (only public
interfaces, no implementation; see `/home/wangh/src/myflink/flink-core-api/`).
`flink-core` adds the heavy implementation — anything that needs Kryo, reflection,
the FileSystem stack, MemorySegment, etc. Connectors that want to compile
against the smallest possible surface depend on `flink-core-api`; everything
else depends on `flink-core`.

## Maven coordinates

- Group / Artifact / Version: `org.apache.flink:flink-core:2.3-SNAPSHOT`
- Packaging: `jar` (also publishes a `test-jar` with reusable test bases)
- Java module config: requires `--add-opens` for `java.base/java.util`,
  `java.lang`, `java.io`, `java.net`, etc. (see `surefire.module.config` in
  `pom.xml`). Off-heap memory access via `sun.misc.Unsafe`.

## Dependencies

### Direct (from `pom.xml`)
- `flink-core-api` — public API interfaces this module implements
- `flink-annotations` — `@Public`, `@PublicEvolving`, `@Internal`, `@Experimental`
- `flink-shaded-asm-9` — bytecode analysis for `TypeExtractor` and lambda type
  inference
- `flink-shaded-jackson` — JSON for configuration descriptions and various
  serialization helpers
- `flink-shaded-guava` — used by ratelimiting and collection utilities
- `commons-lang3`, `commons-text`, `commons-collections`, `commons-compress`
- `snakeyaml-engine` — YAML parsing for `flink-conf.yaml` / `config.yaml`
- `com.esotericsoftware:kryo` — fallback generic serializer
- (test) `flink-architecture-tests-test`, `flink-test-utils-junit`,
  `flink-migration-test-utils`, `joda-time`, `lombok`, `zstd-jni`

### Used by (major direct consumers)
`flink-runtime`, `flink-streaming-java`, `flink-clients`, `flink-table-common`,
`flink-table-api-*`, `flink-rpc-core`, `flink-rpc-akka`, `flink-cep`,
`flink-python`, `flink-state-backends/*`, `flink-statebackend-changelog`,
`flink-dstl-dfs`, `flink-queryable-state-*`, `flink-metrics/*`,
`flink-formats/*` (json, avro, csv, parquet, protobuf, sequence-file,
compress, hadoop-bulk, avro-confluent-registry), `flink-filesystems/*` (s3,
azure, gs, oss, hadoop), `flink-test-utils-parent/*`,
`flink-external-resources/*`, `flink-models/*`. Effectively: everything.

## Source layout

```
org.apache.flink
├── api
│   ├── common/                Common API: ExecutionConfig, JobID, TaskInfo,
│   │   ├── accumulators/      Counters/min/max/histogram accumulators
│   │   ├── aggregators/       (legacy DataSet aggregators)
│   │   ├── cache/             DistributedCache
│   │   ├── eventtime/         Watermarks, WatermarkStrategy, timestamp
│   │   │                       assigners, idleness, alignment, multiplexer
│   │   ├── externalresource/  ExternalResourceInfo
│   │   ├── functions/         MapFunction, FlatMapFunction, RichFunction,
│   │   │                       RuntimeContext, OpenContext, Partitioner ...
│   │   ├── io/                InputFormat / OutputFormat hierarchy,
│   │   │                       compression, ratelimiting, statistics
│   │   ├── operators/         (legacy operator base classes + helpers used
│   │   │                       still by runtime: MailboxExecutor,
│   │   │                       ProcessingTimeService, SlotSharingGroup,
│   │   │                       ResourceSpec, Keys, Ordering)
│   │   ├── resources/         CPUResource, MemoryResource, GPUResource ...
│   │   ├── serialization/     DeserializationSchema, SerializationSchema,
│   │   │                       Encoder, SerializerConfig
│   │   ├── state/             StateDescriptor + variants (Value/List/Map/
│   │   │   │                   Reducing/Aggregating), StateTtlConfig
│   │   │   └── v2/            Next-gen async-friendly state descriptors
│   │   ├── time/              Deadline, Time helpers
│   │   ├── typeinfo/          TypeInformation, BasicTypeInfo, Types,
│   │   │   │                   TypeHint, TypeInfoFactory, @TypeInfo
│   │   │   └── descriptor/    Type descriptor impls (Basic/List/Map/Value)
│   │   └── typeutils/         TypeSerializer, TypeSerializerSnapshot,
│   │       │                   CompositeType, CompositeSerializer,
│   │       │                   CompositeTypeSerializerSnapshot
│   │       └── base/          Primitive serializers + comparators
│   │                           (Int/Long/Double/String/BigDec/Instant/
│   │                            LocalDate/List/Map/Either/Enum/Array ...)
│   ├── connector/
│   │   ├── source/            FLIP-27 Source, SourceReader, SplitEnumerator,
│   │   │                       SourceReaderContext, Boundedness
│   │   ├── sink2/             FLIP-191/143 Sink v2 (Sink, SinkWriter,
│   │   │                       Committer, StatefulSinkWriter, InitContext)
│   │   └── dsv2/              DataStream v2 connector glue
│   ├── dag/                   Pipeline + Transformation abstractions
│   └── java
│       ├── functions/         KeySelector helpers
│       └── typeutils/         TypeExtractor (the big one, 2.5k LOC),
│           │                   PojoTypeInfo, TupleTypeInfo, RowTypeInfo,
│           │                   EnumTypeInfo, GenericTypeInfo, MapTypeInfo,
│           │                   ListTypeInfo, EitherTypeInfo, MissingTypeInfo
│           └── runtime/       PojoSerializer, TupleSerializer, RowSerializer,
│               │               PojoComparator, NullableSerializer,
│               │               PojoSerializerSnapshot, FieldSerializer
│               └── kryo/      KryoSerializer + KryoSerializerSnapshot,
│                               ChillSerializerRegistrar, Serializers
├── configuration/             Configuration, ConfigOption, ConfigOptions
│                               builder, ReadableConfig, WritableConfig,
│                               DelegatingConfiguration, GlobalConfiguration,
│                               and ~30 *Options classes (CoreOptions,
│                               PipelineOptions, ExecutionOptions,
│                               JobManagerOptions, TaskManagerOptions,
│                               CheckpointingOptions, MetricOptions,
│                               RpcOptions, RestOptions, SecurityOptions,
│                               HighAvailabilityOptions, ...)
├── core/
│   ├── asyncprocessing/       AsyncFuture, AsyncFutureImpl (used by async
│   │                           state, FLIP-425/426)
│   ├── classloading/          ComponentClassLoader, SubmoduleClassLoader
│   ├── execution/             JobClient, PipelineExecutor*, CheckpointType,
│   │                           CheckpointingMode, RecoveryClaimMode,
│   │                           SavepointFormatType, JobListener,
│   │                           JobStatusChangedListener
│   ├── failure/               FailureEnricher SPI (FLIP-304)
│   ├── fs/                    FileSystem, Path, FSDataInputStream,
│   │                           FSDataOutputStream, RecoverableWriter,
│   │                           SafetyNetCloseableRegistry, EntropyInjector,
│   │                           LimitedConnectionsFileSystem, local/*
│   ├── io/                    IOReadableWritable, InputSplit, InputStatus,
│   │                           Versioned, SimpleVersionedSerializer
│   ├── memory/                MemorySegment (1671 LOC), MemorySegmentFactory,
│   │                           DataInputView / DataOutputView (+ wrappers),
│   │                           DataInputDeserializer, DataOutputSerializer,
│   │                           ManagedMemoryUseCase
│   ├── plugin/                PluginManager, PluginLoader,
│   │                           DirectoryBasedPluginFinder
│   ├── security/              FlinkSecurityManager, UserSystemExitException,
│   │                           token/ (delegation token managers)
│   └── state/                 InternalStateIterator, StateFutureUtils
├── management/jmx/            JMXService, JMXServer
├── streaming/api/operators/   OutputTypeConfigurable (only file here — kept
│                               for cross-module access)
├── types/                     Value types (IntValue/LongValue/StringValue/
│                               NullValue/...), Row, RowKind, Either,
│                               Nothing, Record, NormalizableKey, Key,
│                               parser/ (field parsers for CSV-style),
│                               variant/ (binary Variant type, SQL VARIANT)
└── util/                      89 files of utilities: ExceptionUtils,
                                Preconditions, FlinkUserCodeClassLoader,
                                InstantiationUtil, NetUtils, IOUtils,
                                FileUtils, AutoCloseableRegistry,
                                CloseableIterator, MutableObjectIterator,
                                concurrent/ (FutureUtils, ScheduledExecutor),
                                function/ (FunctionWithException variants),
                                clock/, jackson/
```

859 production `.java` files; 485 tests; 882 test directories (many are
versioned migration test snapshots — see Tests section).

## Architecture & key concepts

### Configuration system

Built around three pieces:
- **`ConfigOption<T>`** (`configuration/ConfigOption.java`) — immutable
  description of a config key with type, default, fallback keys, deprecation
  list, and human-readable description.
- **`ConfigOptions`** builder DSL — `ConfigOptions.key("x.y").intType().defaultValue(42).withDescription(...)`.
  Supports `booleanType`, `intType`, `longType`, `floatType`, `doubleType`,
  `stringType`, `enumType`, `durationType`, `memorySize`, `mapType`, and
  `.asList()` for list-of-T variants.
- **`Configuration`** (`configuration/Configuration.java`) — concrete
  key/value store implementing `ReadableConfig` + `WritableConfig` +
  `IOReadableWritable` (so it can be shipped across the wire) +
  `ExecutionConfig.GlobalJobParameters`. Internally a `HashMap<String, Object>`
  with type-tagged byte codes for binary serialization.

Per-area `*Options` classes (`CoreOptions`, `PipelineOptions`,
`ExecutionOptions`, `CheckpointingOptions`, `MetricOptions`, ~30 total) are
the canonical source of every Flink config key. The `flink-docs` module
reflects over these classes to generate the documentation tables.

Resolution order when reading: current key -> fallback keys (in declared
order) -> deprecated keys (logs a deprecation warning) -> default value.
See `FallbackKey` and `ConfigOption.withFallbackKeys` / `withDeprecatedKeys`.

`GlobalConfiguration.loadConfiguration(...)` parses `config.yaml` (or legacy
`flink-conf.yaml`) using `snakeyaml-engine`. Flat keys (`x.y.z: v`) and
nested YAML maps both work.

### Type system

`TypeInformation<T>` (`api/common/typeinfo/TypeInformation.java`) is the apex.
Every value flowing through Flink has a `TypeInformation` that knows:
- arity / total field count (for flat schema mapping),
- key types (for keyed operations),
- how to build a `TypeSerializer<T>` (via `createSerializer(SerializerConfig)`).

Concrete `TypeInformation` subclasses (selection):
- `BasicTypeInfo` — primitives + their boxed types, `String`, `Date`, `Void`
- `PrimitiveArrayTypeInfo`, `BasicArrayTypeInfo`, `ObjectArrayTypeInfo`
- `TupleTypeInfo` — `Tuple0..Tuple25` from `flink-core-api`
- `PojoTypeInfo` — fields-with-getters/setters or all-public POJOs
- `RowTypeInfo` — `Row` (dynamic schema, used heavily by Table API)
- `EnumTypeInfo`, `EitherTypeInfo`, `MapTypeInfo`, `ListTypeInfo`,
  `MultisetTypeInfo`, `NullableListTypeInfo`, `NullableMapTypeInfo`
- `GenericTypeInfo` — fallback: instructs runtime to use Kryo
- `MissingTypeInfo` — sentinel used during type extraction failures
- `VariantTypeInfo` — for the binary `Variant` type

`TypeSerializer<T>` (`api/common/typeutils/TypeSerializer.java`) is the
runtime serialization contract: `serialize`, `deserialize`,
`copy(DataInputView, DataOutputView)` (for state migration), `getLength`,
`duplicate()` (stateful serializers must be cloned per thread), and
`snapshotConfiguration()` returning a `TypeSerializerSnapshot<T>`.

`TypeSerializerSnapshot<T>` is the **schema evolution** anchor: it's what
gets written into checkpoints/savepoints (not the serializer itself). On
restore, the new serializer's snapshot calls
`resolveSchemaCompatibility(oldSnapshot)` returning a
`TypeSerializerSchemaCompatibility` (compatible / compatible-after-migration /
compatible-with-reconfigured-serializer / incompatible). `CompositeTypeSerializerSnapshot`
is a base class for serializers composed of nested serializers (Tuple, Row,
Pojo, Map, List, Either ...).

### TypeExtractor

`api/java/typeutils/TypeExtractor.java` (2,509 LOC) — the reflection-driven
mechanism that converts Java `Class`/`Type` instances and lambda generics
into `TypeInformation`. Entry points:

- `TypeExtractor.createTypeInfo(Class<T>)` / `(Type)`
- `TypeExtractor.createTypeInfo(baseClass, clazz, inputType1Idx, inputType2)`
  for `Function`-style inference from a `MapFunction<IN, OUT>` etc.
- `TypeExtractor.getForClass(Class)` / `getForObject(value)`
- `TypeHint<T>` — anonymous-subclass trick for capturing generic parameters
  (`new TypeHint<List<Long>>(){}`)
- `Types` — fluent constructors (`Types.STRING`, `Types.TUPLE(...)`,
  `Types.POJO(Foo.class)`)

Custom type info via `@TypeInfo(MyFactory.class)` (`TypeInfoFactory`) on
the class, or `ResultTypeQueryable` on the function.

### Functions / common API

`api/common/functions/`:
- `Function` (marker; in `flink-core-api`)
- `MapFunction`, `FlatMapFunction`, `FilterFunction`, `ReduceFunction`,
  `AggregateFunction`, `JoinFunction`, `CoGroupFunction`, `CrossFunction`,
  `GroupReduceFunction`, `GroupCombineFunction`, `MapPartitionFunction`,
  `CombineFunction`, `Partitioner`
- `RichFunction` adds lifecycle: `open(OpenContext)`, `close()`,
  `getRuntimeContext()`. `AbstractRichFunction` is the default base.
- `RuntimeContext` (`api/common/functions/RuntimeContext.java`) — what user
  functions see at runtime: parallelism / subtask index, metrics groups,
  accumulators, distributed cache, broadcast variables, keyed state access
  (`getState(ValueStateDescriptor)`, etc.), `JobInfo`, `TaskInfo`.
- `OpenContext` (FLIP-344) replaces the old `open(Configuration)` signature
  and exposes only what `open` truly needs.

### Memory management

`core/memory/MemorySegment.java` (1,671 LOC) — sealed-feel `final` class that
unifies on-heap, off-heap direct, and off-heap unsafe memory under one API.
Uses `sun.misc.Unsafe` for absolute-address access, supports both
big-endian and little-endian explicit getters/setters, plus binary `compare`,
`swap`, `copyTo`. Allocated via `MemorySegmentFactory` (heap /
allocateUnpooledOffHeapMemory / allocateOffHeapUnsafeMemory). Underpins
network buffers, sorting/joining managed memory, state backend serialization,
and the table runtime's binary row formats.

`DataInputView` / `DataOutputView` (`core/memory/`) are the IO abstractions
serializers use — both `DataInputDeserializer` (over a byte[]) and views
backed by `MemorySegment` arrays plug in cleanly. Stream wrappers exist
(`DataInputViewStreamWrapper` etc.) for bridging to Java IO.

### Filesystem (`core/fs/`)

`FileSystem` is the abstract Flink filesystem (HDFS, S3, GCS, local, etc.).
Implementations live in `flink-filesystems/*` and are loaded as plugins.
`Path` is the Flink-internal path object. `RecoverableWriter` /
`RecoverableFsDataOutputStream` are the FLIP-115 atomic-write contract that
powers exactly-once file sinks. `SafetyNetCloseableRegistry` /
`FileSystemSafetyNet` close leaked streams when a user function exits.
`EntropyInjector` adds random prefixes to paths for S3 partitioning.

### IO

- `core/io/`: low-level `IOReadableWritable` (Flink's own `Writable`
  analogue), `InputSplit` and `InputSplitAssigner` (used by both legacy
  `InputFormat` and FLIP-27), `SimpleVersionedSerializer<T>` (for sink
  committables and source split state).
- `api/common/io/`: the legacy `InputFormat` / `OutputFormat` hierarchy
  (`FileInputFormat`, `DelimitedInputFormat`, `BinaryInputFormat`,
  `GenericInputFormat`, `RichInputFormat`...). Still alive for batch and
  for compatibility, even though FLIP-27 `Source` + Sink v2 are preferred
  for new connectors.

### Connectors (Source v2 / Sink v2)

- `api/connector/source/` — `Source`, `SourceReader`, `SplitEnumerator`,
  `SplitsAssignment`, `SourceSplit`, `Boundedness`, `SourceReaderContext`,
  `SplitEnumeratorContext`. This is the FLIP-27 unified source API.
- `api/connector/sink2/` — `Sink`, `SinkWriter`, `StatefulSinkWriter`,
  `CommittingSinkWriter`, `Committer`, `SupportsCommitter`,
  `SupportsWriterState`, `WriterInitContext`, `CommitterInitContext`. This
  is the Sink v2 / FLIP-143/191 API.
- `api/connector/dsv2/` — glue for the DataStream V2 API (in `flink-datastream-api`).

### Utilities (`util/`)

89 source files. Most-touched:
- `Preconditions` — `checkNotNull`, `checkArgument`, `checkState`
- `ExceptionUtils` — `findThrowable`, `rethrowIfFatalError`, `stripCompletionException`
- `InstantiationUtil` — reflective `new`, serialize-to-bytes, classloader-safe
  Java deserialization
- `FlinkUserCodeClassLoader` / `ChildFirstClassLoader` / `MutableURLClassLoader`
- `FileUtils`, `IOUtils`, `NetUtils`, `CollectionUtil`, `MathUtils`
- `AutoCloseableRegistry`, `CloseableIterator`, `CloseableIterable`
- `concurrent/FutureUtils` — heavy `CompletableFuture` plumbing used by all
  async runtime code; retry strategies live here too
- `function/` — `*WithException` variants (Function, BiFunction, Supplier,
  Consumer ...) for code that can throw checked exceptions
- `MdcUtils` + `MdcAware*` — SLF4J MDC propagation across thread-pool
  boundaries (job-id / task-name tagging in logs)

## Important public APIs

The top classes contributors typically end up reading or extending:

| Class | Why it matters |
|---|---|
| `Configuration` | The job-wide config object. Pass it everywhere. |
| `ConfigOption` / `ConfigOptions` | Declare a new configuration key. |
| `ReadableConfig` / `WritableConfig` | Interfaces preferred over raw `Configuration` in signatures. |
| `ExecutionConfig` | Per-job execution settings (parallelism, restart strategy, registered Kryo serializers, object reuse). |
| `TypeInformation` | The "type descriptor" object — required for every stream type. |
| `TypeSerializer` | Implement this to add custom serialization. |
| `TypeSerializerSnapshot` | Implement this in tandem to support schema evolution. |
| `TypeExtractor` / `TypeHint` / `Types` | How to obtain `TypeInformation` for a Java type or generic. |
| `Function` / `RichFunction` / `AbstractRichFunction` | Base of all user UDFs. |
| `RuntimeContext` / `OpenContext` | What user functions see at runtime. |
| `StateDescriptor` (Value/List/Map/Reducing/Aggregating) | Declare keyed state. v2 variants under `state/v2/` for async state. |
| `WatermarkStrategy` / `TimestampAssigner` / `WatermarkGenerator` | Event-time plumbing. |
| `Source` (FLIP-27) / `SourceReader` / `SplitEnumerator` | New-style sources. |
| `Sink` / `SinkWriter` / `Committer` (sink2) | New-style sinks. |
| `MemorySegment` / `DataInputView` / `DataOutputView` | Low-level serialization plumbing. |
| `FileSystem` / `Path` / `RecoverableWriter` | Filesystem abstraction. |
| `Row` / `RowKind` | Dynamic row type, central to Table API. |
| `JobID` / `TaskInfo` / `JobInfo` | Identity carriers. |
| `SimpleVersionedSerializer` | Serialize sink committables / source split state with a version byte. |
| `Preconditions` / `ExceptionUtils` / `InstantiationUtil` | Used in approximately every Flink file. |

## Internal flows

### Type extraction: `TypeInformation.of(...)`
1. `Types.of(...)` / `TypeExtractor.createTypeInfo(...)` enters
   `TypeExtractor`.
2. If the class is annotated with `@TypeInfo`, the declared
   `TypeInfoFactory` wins.
3. Otherwise: check `BasicTypeInfo` table -> `Tuple` subclass ->
   array / collection -> `Either` / `Row` / `Value` -> POJO analysis
   (`analyzePojo` — needs default ctor + getter/setter or all-public fields,
   and *no* generic parents that can't be resolved).
4. If POJO analysis fails, fall back to `GenericTypeInfo` (Kryo) and emit
   a warning if `disableGenericTypes` is set.
5. For lambdas, ASM (`flink-shaded-asm-9`) reads the synthesized
   `writeReplace`/`SerializedLambda` info to recover generic argument types.

### Serializer compatibility (state schema evolution)
1. On snapshot: `TypeSerializer.snapshotConfiguration()` returns a
   `TypeSerializerSnapshot` which is written to the checkpoint metadata
   (via `TypeSerializerSnapshotSerializationUtil`).
2. On restore: the snapshot is read first, then `restoreSerializer()` builds
   a serializer compatible with the persisted bytes.
3. The *new* serializer (built from current code) calls
   `snapshotConfiguration().resolveSchemaCompatibility(oldSnapshot)`.
4. Four outcomes: `compatibleAsIs`, `compatibleAfterMigration` (re-read with
   restore serializer, re-write with new), `compatibleWithReconfiguredSerializer`,
   `incompatible` (job fails). `CompositeTypeSerializerSnapshot` automates
   this for nested serializers (Tuple, Row, Pojo, etc.).

### Configuration resolution
`Configuration.get(ConfigOption<T>)`:
1. Look up `option.key()` in the map. If present, convert and return.
2. Iterate `fallbackKeys` in declared order; deprecated keys log a warning.
3. Return `option.defaultValue()` if nothing found (which may be `null`).

`DelegatingConfiguration` wraps a parent with a prefix — useful for
sub-configurations (e.g. `state.backend.rocksdb.*` settings stripped of
the `state.backend.` prefix).

## Tests

- Production tests live under `src/test/java/org/apache/flink/{api,configuration,core,types,util}/...` (485 files).
- `src/test/java/org/apache/flink/architecture/TestCodeArchitectureTest.java`
  runs ArchUnit rules from `flink-architecture-tests`. Violations exempted
  per-module are listed in `archunit-violations/`.
- `src/test/java/org/apache/flink/testutils/` is published via the
  `test-jar` artifact for downstream modules. Notable utilities:
  `DeeplyEqualsChecker`, `CustomEqualityMatcher`,
  `ArtificialCNFExceptionThrowingClassLoader`, `TestFileSystem`,
  `EntropyInjectingTestFileSystem`, `TestingUtils`,
  `testutils/serialization/`, `testutils/migration/`, `testutils/runtime/`.
- `src/test/resources/*-{version}/` directories hold **migration snapshots**
  — pre-generated serializer snapshots from past Flink releases (1.7
  through 2.2). The serializer compatibility tests reload these to ensure
  `TypeSerializerSnapshot.resolveSchemaCompatibility` keeps working across
  upgrades.
- The `generate-migration-test-data` Maven profile re-generates those
  snapshots from `MigrationTestsSnapshotGenerator` (in
  `flink-migration-test-utils`).

## Pitfalls & gotchas

- **POJO requirements are strict.** A class must have a public no-arg
  constructor, all fields must be either `public` or have public
  getter/setter pairs (Java-bean style), and any generic type parameters
  must be resolvable. If any check fails, `TypeExtractor` silently falls
  back to `GenericTypeInfo` (Kryo) — which kills schema evolution and
  performance. To detect this, set
  `pipeline.generic-types: false` (`PipelineOptions.GENERIC_TYPES`); the
  job will fail loudly instead of falling back.
- **Lambdas + generics are fragile.** `MapFunction<String, Long>` as a
  lambda needs ASM to recover the type. Sometimes it can't — supply a
  `TypeHint` or `.returns(TypeInformation)` to disambiguate.
- **Stateful serializers must `duplicate()`.** A single `TypeSerializer`
  instance is shared across operators; if it has mutable state it must
  return a fresh copy from `duplicate()`. Forgetting this causes
  intermittent corruption under concurrent access.
- **`ConfigOption` deprecation.** Adding a fallback/deprecated key isn't
  free — `Configuration.get` logs each deprecated lookup at WARN, which
  can flood logs. Prefer `withFallbackKeys` over `withDeprecatedKeys` if
  the rename isn't a hard deprecation.
- **Kryo Chill registrations live in `kryo/ChillSerializerRegistrar` and
  the Twitter chill jar.** When upgrading Kryo or Chill, the registered
  IDs change and savepoints break. Migration tests under
  `src/test/resources/kryo-*` exercise this.
- **`MemorySegment` is `final` for a reason.** Subclassing was tried
  historically and JIT couldn't devirtualize the abstract methods; the
  current implementation manually dispatches on a `byte[]` (heap) vs.
  `null` (off-heap) check. Don't try to "improve" this with inheritance.
- **`FileSystem.get(URI)` is plugin-loaded.** If the right
  `FileSystemFactory` isn't on the classpath / plugin dir, you'll get
  `UnsupportedFileSystemSchemeException`. Plugin loading uses
  `core/plugin/PluginManager`.
- **`TypeInformation.equals` matters.** Two `TypeInformation` instances
  for the "same" type must `equals` each other or operator chaining /
  state restore breaks subtly. Implement carefully in custom types.
- **`Configuration` is `Serializable` but treats `byte[]` specially.**
  Putting a `byte[]` round-trips via base64 in string form; mixing this
  with binary YAML is fragile.
- **`@Public` vs `@PublicEvolving` vs `@Internal`.** flink-core enforces
  these via ArchUnit. Don't tag a new class `@Public` unless you're sure
  — it locks the signature.

## Related modules / docs

- `flink-core-api` (`/home/wangh/src/myflink/flink-core-api/`) — minimal API
  surface; flink-core implements it. New public interfaces should usually
  go there, not here.
- `flink-annotations` — `@Public`, `@PublicEvolving`, `@Experimental`,
  `@Internal`, `@VisibleForTesting`.
- `flink-runtime` — operator runtime, network stack, scheduling,
  checkpoint coordination. Depends on flink-core for everything in this
  module.
- `flink-streaming-java` — `DataStream` API, stream operators built on top
  of flink-core types and functions.
- `flink-table-common` — Table API common types; depends on flink-core for
  type system, `Row`, `Variant`, configuration.
- `flink-state-backends/*` — implementations of state that consume
  `StateDescriptor`s declared here.
- Codedocs (under `/home/wangh/src/myflink/openspec/` and any
  per-package CLAUDE.md files) — design notes for ongoing changes.
