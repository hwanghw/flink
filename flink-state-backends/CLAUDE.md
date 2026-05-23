# `flink-state-backends`

Parent Maven aggregator pom for Flink's pluggable on-disk and disaggregated state
backend implementations. Heap (in-memory) state lives in `flink-runtime` and is
the framework default; this module bundles the disk-spilling and disaggregated
alternatives plus the changelog wrapper.

## Purpose

- Aggregator-only (`<packaging>pom</packaging>`) — produces no jar of its own.
- Houses every state backend implementation that is NOT the default heap backend.
- Each backend is plugged in at runtime via `state.backend.type = rocksdb | forst | hashmap`
  and is consumed by `flink-runtime`'s keyed state framework.

## Where it fits

```
User config (state.backend.type = rocksdb)
        |
        v
flink-runtime / StateBackendLoader
        |
        v   (plugin)
flink-state-backends/flink-statebackend-{rocksdb,forst,changelog,...}
        |
        v
JNI (frocksdbjni, forstjni) -> on-disk LSM tree
        |
        v
Snapshots -> checkpoint storage (S3/HDFS/local)
```

## Maven coordinates

- Group: `org.apache.flink`
- Artifact: `flink-state-backends`
- Version: `2.3-SNAPSHOT`
- Parent: `flink-parent`
- Packaging: `pom`

### Sub-modules

| Sub-module | Purpose |
| ---------- | ------- |
| `flink-statebackend-rocksdb` | Embedded RocksDB-based keyed state backend (the main on-disk backend used in production). |
| `flink-statebackend-forst` | ForSt-based disaggregated/async state backend (a Flink-friendly RocksDB fork tuned for object storage and async access). |
| `flink-statebackend-changelog` | Changelog wrapper backend that delegates working state to an underlying backend (rocksdb/heap) while logging every change to DSTL for incremental checkpoints. |
| `flink-statebackend-common` | Shared infrastructure for backends that need periodic materialization (used by changelog). |
| `flink-statebackend-heap-spillable` | Experimental skip-list–based spillable heap state map (off-heap memory allocator + copy-on-write snapshot). |

The default `HashMapStateBackend` does NOT live here — it ships inside
`flink-runtime`.

## Dependencies

### Direct (parent pom)

None at the aggregator level beyond inheriting from `flink-parent`. Each
sub-module declares its own deps; common patterns:

- `flink-streaming-java`, `flink-runtime`, `flink-core` — `<scope>provided</scope>`
- `com.ververica:frocksdbjni:8.10.0-ververica-1.0` — RocksDB JNI
- `com.ververica:forstjni:0.1.8` — ForSt JNI
- `flink-shaded-guava`
- `flink-test-utils-junit`, `flink-runtime` test-jar — for tests

### Used by

- `flink-dist` — bundles the rocksdb/forst/changelog jars into the distribution.
- `flink-tests`, `flink-end-to-end-tests` — integration tests.
- `flink-state-backends-tests`-style state harnesses in other modules.

## Sub-module architectures

### flink-statebackend-rocksdb

**Purpose.** The workhorse on-disk state backend. Stores keyed state in an
embedded RocksDB LSM tree under the TaskManager's local disk; snapshots SST
files to durable checkpoint storage incrementally.

**Key classes** (`org.apache.flink.state.rocksdb`):

- `EmbeddedRocksDBStateBackend` (+ `EmbeddedRocksDBStateBackendFactory`) —
  `PublicEvolving` user-facing backend; discovery factory loaded reflectively
  by `StateBackendLoader`.
- `RocksDBKeyedStateBackend` (+ `RocksDBKeyedStateBackendBuilder`) — runtime
  backend; owns the `RocksDB` handle, column family registry, snapshot
  strategy, ttl filters, resource container.
- `AbstractRocksDBState` + `RocksDB{Value,List,Map,Reducing,Aggregating}State`
  and `AbstractRocksDBAppendingState` — per-descriptor state wrappers.
- `RocksDBPriorityQueueSetFactory`, `RocksDBCachingPriorityQueueSet`,
  `TreeOrderedSetCache` — RocksDB-backed timer/priority-queue support.
- `RocksDBResourceContainer`, `RocksDBSharedResources(Factory)`,
  `RocksDBMemoryControllerUtils`, `RocksDBMemoryConfiguration` — Options
  lifecycle plus the slot-shared block cache and write buffer manager.
- `RocksDBOperationUtils`, `RocksDBWriteBatchWrapper`, `RocksIteratorWrapper`
  — batched writes and iterator safety.
- `RocksDBNativeMetricMonitor`, `RocksDBNativeMetricOptions`, `RocksDBProperty`
  — RocksDB internal counters as Flink metrics.
- `RocksDBConfigurableOptions`, `RocksDBOptions`, `PredefinedOptions`,
  `ConfigurableRocksDBOptionsFactory` — config knobs.

**Snapshot strategies** (`.snapshot`): `RocksDBSnapshotStrategyBase` (shared
skeleton), `RocksIncrementalSnapshotStrategy` (default; ships SST deltas as
`IncrementalRemoteKeyedStateHandle`), `RocksNativeFullSnapshotStrategy` (full
upload for savepoints), `RocksDBFullSnapshotResources`, `RocksSnapshotUtil`,
and `RocksDBStateUploader` / `RocksDBStateDownloader` /
`RocksDBStateDataTransferHelper` for parallel SST transfer.

**Restore** (`.restore`): `RocksDBRestoreOperation` with
`RocksDBNoneRestoreOperation`, `RocksDBFullRestoreOperation`,
`RocksDBIncrementalRestoreOperation`, `RocksDBHeapTimersFullRestoreOperation`,
plus `RestoredDBInstance`, `RocksDBHandle`, `RocksDBRestoreResult`,
`DistributeStateHandlerHelper`. Rescaling uses `IngestExternalFile` and
`ClipDB`/`deleteFilesInRange`.

**SST manual compaction** (`.sstmerge`): `RocksDBManualCompactionManager(Impl)`,
`CompactionScheduler`, `CompactionTracker`, `CompactionTask(Producer)`,
`Compactor`, `ColumnFamilyLookup`, `RocksDBManualCompactionConfig/Options` —
background SST merging to bound incremental-checkpoint delta size.

**TTL** (`.ttl`): `RocksDbTtlCompactFiltersManager` registers compaction
filters that drop expired entries during background compactions.

**Iteration** (`.iterator`): `AbstractRocksStateKeysIterator`,
`RocksSingleStateIterator`, `RocksStatesPerKeyGroupMergeIterator` for full
scans (savepoint write, queryable state).

**Mechanics worth knowing.**

- **Composite key layout:** every RocksDB row key is encoded as
  `keyGroup | key | namespace` (with VLE-prefixed lengths). Map state appends
  the user map key. This keeps per-key-group state contiguous, enabling
  iterator-based snapshot scans.
- **Column family per state descriptor:** each registered keyed state gets its
  own RocksDB column family. CF options are derived from the predefined +
  user options factory.
- **Memory model:** under managed memory the block cache and write buffer
  manager are shared across all RocksDB instances in a slot via
  `RocksDBSharedResources` — see the deep-dive doc.
- **Incremental snapshots:** SST files are immutable. After a successful sync
  RocksDB checkpoint we hard-link SSTs into the snapshot directory; the async
  phase uploads only files not already known to the JM-side shared registry,
  yielding O(delta) bytes per checkpoint.

Link: see `codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md` for the
full sync/async snapshot walkthrough and SST shared-registry semantics.

### flink-statebackend-forst

**Purpose.** Async/disaggregated successor of the RocksDB backend, oriented at
object-storage-resident state. Lets the LSM tree live remotely (S3/HDFS) and
exposes asynchronous state operations through the AsyncKeyedStateBackend
contract.

**Key classes** (`org.apache.flink.state.forst`):

- `ForStStateBackend` / `ForStStateBackendFactory` — user-facing entry points.
- `ForStKeyedStateBackend` / `ForStKeyedStateBackendBuilder` — runtime backend.
- State wrappers: `ForSt{Value,List,Map,Reducing}State`, `ForStMapIterator`.
- Async request pipeline: `ForStStateExecutor`, `ForStStateRequestClassifier`,
  `ForStDBOperation` plus per-op requests
  (`ForStDB{Get,SingleGet,RawGet,Put,ListGet,Iter,Map{Key,Value,Entry}Iter,MultiRawMergePut}Request`),
  `ForStGeneralMultiGetOperation`, `ForStWriteBatchOperation`,
  `ForStIterateOperation` — coalesces/batches requests to amortize remote IO.
- Resources/config: `ForStResourceContainer`, `ForStSharedResources`,
  `ForStMemoryConfiguration`, `ForStMemoryControllerUtils`,
  `ForStOptionsFactory`, `ConfigurableForStOptionsFactory`,
  `ForStConfigurableOptions`, `ForStNativeMetricOptions`, `ForStProperty`.
- Checkpoint/TTL: `ForStIncrementalCheckpointUtils`, `StateHandleTransferSpec`,
  `ForStDBTtlCompactFiltersManager`, `ForStInnerTable`, `ContextKey`,
  `ListDelimitedSerializer`.

**How it differs from rocksdb.**

- LSM files can live in remote DFS rather than local disk; local disk is only
  a hot cache.
- All state access is request-based and async-friendly — designed for the
  async record processing path (`AsyncKeyedStateBackend` / async state API).
- Uses `forstjni` (a separate ForSt JNI; not frocksdbjni).

**When to choose it.** Very large state where local-disk pressure dominates,
deployments where you want stateless scale-out (TM disk independence), or
async-state-API workloads.

### flink-statebackend-changelog

**Purpose.** Doesn't store state itself — wraps another backend (`hashmap`,
`rocksdb`) and additionally appends every state change to the Durable Short
Term Log (DSTL). On checkpoint, the changelog is uploaded incrementally
(very fast, very small) and the heavyweight materialization to the underlying
backend happens periodically/asynchronously.

**Key classes** (`org.apache.flink.state.changelog`):

- `ChangelogStateBackend` (extends `AbstractChangelogStateBackend`) and
  `DeactivatedChangelogStateBackend` — backend entry points.
- `ChangelogKeyedStateBackend` — runtime wrapper delegating to the inner
  backend and the `StateChangelogStorage`.
- State wrappers: `AbstractChangelogState`, `Changelog{Value,List,Map,Reducing,Aggregating}State`,
  `ChangelogKeyGroupedPriorityQueue`, `ChangelogStateFactory`.
- Loggers: `KvStateChangeLogger(Impl)`, `AbstractStateChangeLogger`,
  `PriorityQueueStateChangeLoggerImpl`, `StateChangeLogger`,
  `StateChangeOperation`, `StateChangeLoggingIterator`,
  `ChangelogTruncateHelper`, `ChangelogStateBackendMetricGroup`.
- Restore (`.restore`): `ChangelogBackendRestoreOperation`,
  `ChangelogRestoreTarget`, `ChangelogMigrationRestoreTarget`,
  `ChangelogBackendLogApplier`, `ChangelogApplierFactory(Impl)` plus per-state
  `*StateChangeApplier` classes, `StateID`, `FunctionDelegationHelper`.

Periodic materialization is driven by `flink-statebackend-common`'s
`PeriodicMaterializationManager`.

### flink-statebackend-common

Tiny module — two classes: `PeriodicMaterializationManager` (schedules and
coordinates writing changelog state into the underlying backend's format) and
`ChangelogMaterializationMetricGroup` (cadence/duration/throughput metrics).
Pulled in by `flink-statebackend-changelog`.

### flink-statebackend-heap-spillable

Experimental heap state map backed by an off-heap allocator and a skip-list
index, with copy-on-write snapshots. NOT the production heap backend — that
one is `HashMapStateBackend` in `flink-runtime`. Key classes
(`org.apache.flink.runtime.state.heap`): `CopyOnWriteSkipListStateMap(Snapshot)`,
`SkipListKeySerializer`/`SkipListValueSerializer`/`SkipListKeyComparator`/`SkipListUtils`,
`LevelIndexHeader`/`OnHeapLevelIndexHeader`/`NodeStatus`, and the off-heap
chunk allocator under `space.` (`Allocator`, `Chunk`, `Constants`, `SpaceUtils`).

## Important public APIs

- `state.backend.type` — selects the backend. Values: `hashmap` (default,
  in `flink-runtime`), `rocksdb`, `forst`.
- `state.backend.changelog.enabled` — turns on the changelog wrapper.
- `state.backend.incremental` — enables incremental checkpoints (rocksdb/forst).
- `EmbeddedRocksDBStateBackend.setPredefinedOptions(PredefinedOptions)` and
  `setRocksDBOptions(RocksDBOptionsFactory)` — programmatic tuning.
- Factories (`*StateBackendFactory`) are loaded reflectively by
  `StateBackendLoader` in `flink-runtime`.

## Internal flows

- **RocksDB incremental snapshot (sync + async).** Sync phase: flush memtables,
  invoke RocksDB `Checkpoint::CreateCheckpoint`, hard-link SSTs into snapshot
  dir, gather meta. Async phase: `RocksDBStateUploader` uploads only the SSTs
  not registered in the prior `SharedStateRegistry`; produces
  `IncrementalRemoteKeyedStateHandle`. See
  `codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md`.
- **S3 upload of SST files with incremental reuse.** Each SST is a content-
  addressed shared-state object. The JM-side `SharedStateRegistry` ref-counts
  them so subsequent checkpoints only ship deltas.
- **Restore + rescale.** `RocksDBIncrementalRestoreOperation` uses
  `IngestExternalFile` (and optionally `ClipDB`/`deleteFilesInRange`) to merge
  multiple source key-group ranges into a target instance with minimal copy.
- **Changelog materialization.** `PeriodicMaterializationManager` triggers
  an async materialization on the wrapped backend; the changelog up to that
  watermark can then be truncated by `ChangelogTruncateHelper`.

## Tests

- **rocksdb:** `EmbeddedRocksDBStateBackendTest`,
  `RocksDBAsyncSnapshotTest`, `RocksIncrementalCheckpointRescalingTest`,
  `RocksDBRecoveryTest`, `RocksDBStateUploaderTest`,
  `RocksDBStateDownloaderTest`, `RocksDBMemoryControllerUtilsTest`,
  `RocksDBNativeMetricMonitorTest`, `RocksDBSharedResourcesFactoryTest`,
  `RocksDBStateBackendConfigTest`, `RocksDBStateBackendMigrationTest`,
  `HeapTimersSnapshottingTest`, `RocksDbMultiClassLoaderTest`,
  `RocksDBAutoCompactionIngestRestoreTest`. Test harness:
  `RocksDBTestUtils`, `RocksDBExtension`,
  `RocksDBKeyedStateBackendTestFactory`.
- The rocksdb module publishes a test-jar (see `pom.xml`) so other modules
  (changelog, e2e tests, pyflink) can reuse `RocksDBTestUtils`,
  `EmbeddedRocksDBStateBackendTest`, and `RocksDBStateBackendConfigTest`.
- Backend conformance lives in `flink-runtime`'s
  `StateBackendTestBase`/`StateBackendTestV2Base` — each backend extends it.

## Pitfalls & gotchas

- **Managed memory sizing.** RocksDB's block cache + write buffer manager are
  driven by `taskmanager.memory.managed.size` (or fraction). Under-sizing
  causes write stalls and frequent compactions; over-sizing starves network
  buffers. See `codedocs/flink-rocksdb-state-backend-tuning.md`.
- **TTL configuration.** `RocksDbTtlCompactFiltersManager` only drops TTL
  entries during background compactions. If compaction never runs (cold state
  or "manual compaction only" setups), expired data lingers and inflates
  checkpoints. See `codedocs/flink-state-ttl-architecture.md`.
- **State migration between versions.** Changing a state descriptor's
  serializer requires a compatible `TypeSerializerSnapshot`; otherwise restore
  fails. RocksDB column families are keyed by state name + serializer
  compatibility.
- **Sync vs async snapshot.** The sync phase blocks the task thread; very
  large state or slow disks turn this into checkpoint-alignment back-pressure.
  Prefer incremental checkpoints and watch `lastCheckpointSyncDuration`.
- **Column family explosion.** Dynamically creating state descriptors
  (different names per record) creates a column family per name. Each CF has
  its own memtable footprint — keep state descriptor names static.
- **Changelog isn't free.** Per-mutation logging doubles write amplification
  and requires DSTL storage. Use it when checkpoint latency matters more than
  steady-state throughput.
- **ForSt is async-API only for full benefit.** Synchronous state access
  through ForSt blocks on remote IO; mixing it with the legacy sync state API
  defeats the disaggregation gains.

## Related modules / codedocs

- `codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md` — the
  authoritative deep dive on the RocksDB snapshot pipeline (sync phase, async
  phase, SST sharing, restore, rescaling).
- `codedocs/flink-exactly-once-checkpointing-deep-dive.md` — how the backend
  fits into the broader exactly-once checkpoint protocol.
- `codedocs/flink-state-ttl-architecture.md` — TTL semantics and the
  RocksDB/ForSt compaction-filter integration.
- `codedocs/flink-rocksdb-state-backend-tuning.md` — managed memory, block
  cache, write buffer manager, predefined options.
- `codedocs/flink-unaligned-checkpoint-watermark-recovery.md` — restore-path
  interaction with unaligned checkpoints and in-flight buffers.
- Adjacent modules: `flink-runtime` (heap backend + `StateBackendLoader`),
  `flink-dstl/flink-dstl-dfs` (changelog storage), `flink-dist` (bundling).
