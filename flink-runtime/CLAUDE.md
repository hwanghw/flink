# `flink-runtime`

## Purpose

This is Flink's distributed execution engine. Every Flink job — regardless of which API it was written against (DataStream, Table/SQL, Python, CEP, State Processor) — is compiled into a `JobGraph` that is submitted to processes built from `flink-runtime`. The module owns:

- The **JobManager-side** components: `Dispatcher`, `JobMaster`, `ResourceManager`, `CheckpointCoordinator`, `ExecutionGraph`, the schedulers (default, adaptive, adaptive-batch), the slot manager.
- The **TaskManager-side** components: `TaskExecutor`, `Task`, the network shuffle service, `StreamTask` and the `OperatorChain`, the mailbox executor.
- The **shared infrastructure**: the state-backend SPI, the network stack (Netty + buffer pool + credit-based flow control), the BLOB service, leader election & HA, the REST server, the metric registry, RPC bindings on top of `flink-rpc`.
- The **legacy streaming runtime**: `StreamTask`, `AbstractStreamOperator`, the `SourceFunction` family, the window operators, watermark generation — all the classes that used to live in `flink-streaming-java` were moved here for Flink 2.x.

If a class is part of "what runs on the cluster" rather than "what the user writes," it lives in this module.

## Where it fits

```
flink-core / flink-rpc-core / flink-datastream-api
         ↑               (types, config, RPC contract)
    flink-runtime    ← THIS MODULE
         ↑
    flink-streaming-java / flink-clients / flink-table-runtime / flink-state-backends-* /
    flink-connectors / flink-kubernetes / flink-yarn / flink-runtime-web / flink-tests
```

`flink-runtime` consumes types and SPIs from `flink-core` and the `flink-rpc-*` modules and is consumed by *every* deployment, API, state-backend, and connector module that needs to plug into the engine.

## Maven coordinates

- groupId: `org.apache.flink`
- artifactId: `flink-runtime`
- version: `2.3-SNAPSHOT`
- packaging: `jar`

This module also publishes a `test-jar` (see `maven-jar-plugin/executions/test-jar` in `pom.xml:402-414`). The test-jar contains the operator test harnesses (`AbstractStreamOperatorTestHarness`, `OneInputStreamOperatorTestHarness`, `KeyedOneInputStreamOperatorTestHarness`, `TwoInputStreamOperatorTestHarness`, `BroadcastOperatorTestHarness`, `KeyedBroadcastOperatorTestHarness`, `SourceOperatorTestHarness`, `ProcessFunctionTestHarnesses`, plus their `asyncprocessing` variants) and is what every other module pulls in for `<type>test-jar</type><scope>test</scope>` operator testing.

## Dependencies

### Direct (production)

| Dependency | Why |
| --- | --- |
| `flink-core` | Types, configuration, `MemorySegment`, serializers, plugin loader. |
| `flink-datastream-api` | New (FLIP-409) DataStream API contract. |
| `flink-rpc-core` | `RpcGateway` / `RpcService` / `RpcEndpoint` interfaces (the actual definitions live in `flink-rpc-core`, package `org.apache.flink.runtime.rpc`). |
| `flink-rpc-akka-loader` | The pekko-based RPC implementation discovered at runtime. The IDE-fallback flavor is also pulled in for test scope. |
| `flink-queryable-state-client-java` | Public surface for queryable state. |
| `flink-hadoop-fs` | Hadoop FS plugin; Hadoop itself is `optional`. |
| `flink-connector-datagen` | Built-in datagen source (referenced from the streaming source examples and from tests). |
| `flink-shaded-netty` | Network stack. |
| `flink-shaded-guava` / `flink-shaded-jackson` / `flink-shaded-zookeeper-3` | Engine internals & HA. |
| `flink-shaded-swagger` | OpenAPI generation (optional, only `flink-docs` needs it at runtime). |
| `javassist` | Dynamic class generation for the RPC gateway proxies (see comment around `pom.xml:220`). |
| `org.xerial.snappy:snappy-java`, `org.lz4:lz4-java`, `io.airlift:aircompressor` | Network/blocking-shuffle compression. `aircompressor` is shaded into `org.apache.flink.shaded.io.airlift.compress` (see `maven-shade-plugin` in `pom.xml:417-445`). |
| `tools.profiler:async-profiler` 2.9 | The profiler hooks exposed via REST (`/jobs/:jobid/profiler`). |
| `commons-io`, `commons-lang3`, `commons-text`, `commons-cli` | Utility usage scattered through entrypoints and the REST layer. |
| `oshi-core` | Optional; only used for richer host info logging. |

### Used by

Effectively every non-trivial module. Notable consumers (discovered by scanning `<artifactId>flink-runtime</artifactId>` in pom files):

- API: `flink-streaming-java`, `flink-datastream`, `flink-clients`, `flink-table-*`, `flink-python`, `flink-libraries/flink-cep`, `flink-libraries/flink-state-processing-api`
- Deployment: `flink-kubernetes`, `flink-yarn`, `flink-container`, `flink-dist`
- State backends: `flink-state-backends/flink-statebackend-rocksdb`, `flink-statebackend-changelog`, `flink-statebackend-forst`, `flink-statebackend-common`, `flink-statebackend-heap-spillable`
- Tests/Infra: `flink-tests`, `flink-test-utils-parent/*`, `flink-architecture-tests`, `flink-runtime-web`, `flink-docs`
- Filesystem & format: `flink-filesystems/flink-s3-fs-*`, `flink-formats/flink-parquet`, `flink-dstl/flink-dstl-dfs`, `flink-connectors/flink-hadoop-compatibility`

The grep returned ~40 direct consumers; transitively *every* runtime artifact in the distribution depends on this module.

## Source layout

`src/main/java/org/apache/flink/runtime/`:

| Package | One-line purpose |
| --- | --- |
| `accumulators/` | User accumulator aggregation and persistence. |
| `application/` | Application-mode (per-job-cluster) bootstrapping. |
| `asyncprocessing/` | New async state access framework (FLIP-425). |
| `blob/` | The BLOB service — distributes JARs and large user artifacts. |
| `blocklist/` | Blocked-node tracking used by the scheduler. |
| `broadcast/` | Broadcast state backend storage. |
| `checkpoint/` | `CheckpointCoordinator`, checkpoint metadata, completed-checkpoint store, channel-state writer. |
| `client/` | The internal Flink "cluster client" RPC plumbing (not user-facing). |
| `clusterframework/` | Resource-manager-agnostic process container abstractions. |
| `concurrent/` | Runtime-specific executors (e.g. `FutureUtils` companions). |
| `deployment/` | `TaskDeploymentDescriptor` — what gets shipped to a TM. |
| `dispatcher/` | `Dispatcher` JobManager-side process entry, job persistence, REST surface wiring. |
| `entrypoint/` | `ClusterEntrypoint` and its session/standalone variants — the `main()`s that start a JobManager process. |
| `event/` | Runtime events (operator events, task events). |
| `execution/` | `Execution` / `ExecutionState` enums and listeners. |
| `executiongraph/` | `ExecutionGraph` — the physical, parallelized execution DAG. |
| `externalresource/` | GPU & external-resource integration. |
| `failure/` | Failure-enrichment (labelling) framework. |
| `filecache/` | Distributed cache (`-yt` / `registerCachedFile`). |
| `hadoop/` | Hadoop-specific config/security shims. |
| `heartbeat/` | The bidirectional heartbeat manager used by JM↔TM↔RM. |
| `highavailability/` | HA SPI plus ZK and KubernetesPodService-backed implementations. |
| `history/` | History server. |
| `instance/` | `HardwareDescription`, `InstanceID`, `SlotSharingGroup`. |
| `io/` | Local IO (`AsynchronousFileIOChannel`), disorderly-disk batch IO, **and** the network stack under `io/network/`. |
| `iterative/` | Legacy batch iteration runtime. |
| `jobgraph/` | The `JobGraph` (compiled, pre-parallelization) data model and `JobVertex`/`JobEdge`. |
| `jobmanager/` | `ExecutionPlanStore` (job persistence), HA factory, process spec. |
| `jobmaster/` | `JobMaster` — per-job leader logic — plus slot pool, leadership runner, deployment tracker. |
| `leaderelection/` / `leaderretrieval/` | Leader election & lookup SPI + ZK and standalone drivers. |
| `memory/` | Managed-memory bookkeeping. |
| `messages/` | RPC message envelopes (`Acknowledge`, checkpoint messages, FlinkJobNotFoundException, …). |
| `metrics/` | `MetricRegistry`, scope formatters, reporter setup. |
| `minicluster/` | `MiniCluster` — the in-JVM cluster used by IDE/test runs. |
| `net/` | Connection utilities, SSL setup. |
| `operators/` | Legacy batch operator drivers (Map/Join/CoGroup). |
| `persistence/` | State stores for jobs/checkpoints (filesystem, ZK, K8s). |
| `plugable/` | Pluggable serializer SPI. |
| `query/` | Queryable-state lookup plumbing. |
| `registration/` | Common retry-with-deadline registration handshake used by JM/TM/RM. |
| `resourcemanager/` | `ResourceManager`, slot allocation, `slotmanager/` (declarative + fine-grained variants), active integrations. |
| `rest/` | The REST API: `RestServerEndpoint`, handlers, message types. |
| `scheduler/` | `SchedulerNG` interface plus `DefaultScheduler`, `AdaptiveScheduler`, `AdaptiveBatchScheduler`, region/pipelined scheduling strategies. |
| `security/` | Hadoop/Kerberos security context, token providers. |
| `shuffle/` | Pluggable shuffle SPI; the built-in `NettyShuffleMaster` and descriptors. |
| `slots/` | Slot lifecycle, slot owner. |
| `source/` | Coordinator and event plumbing for the FLIP-27 `Source` API. |
| `state/` | The whole keyed/operator state-backend SPI: `AbstractKeyedStateBackend`, snapshot strategies, checkpoint storage, TTL framework under `state/ttl/`, heap backend under `state/heap/`. |
| `taskexecutor/` | `TaskExecutor` (TaskManager RPC endpoint), `JobLeaderService`, `JobTable`, slot table. |
| `taskmanager/` | `Task` (the per-subtask thread driver), `RuntimeEnvironment`, network config. |
| `throughput/` | Buffer-debloating logic. |
| `throwable/` | Throwable classifier (recoverable vs. non-recoverable). |
| `topology/` | Generic topology interfaces used by the scheduler. |
| `util/` | Runtime utilities. |
| `webmonitor/` | The web-monitor REST endpoint base (extended by `flink-runtime-web`) and stat/thread-info trackers. |
| `zookeeper/` | ZK utilities used by HA, leader election, the BLOB store, and the completed-checkpoint store. |

`src/main/java/org/apache/flink/streaming/` (the legacy streaming runtime relocated into this module for Flink 2.x):

| Package | Purpose |
| --- | --- |
| `api/operators/` | `AbstractStreamOperator`, `AbstractStreamOperatorV2`, `KeyedProcessOperator`, `InternalTimerService*`, `MailboxWatermarkProcessor`, factory hierarchy. |
| `api/functions/source/` | The "modern" wrappers and helpers; the actual `SourceFunction` interface and its built-ins live under `api/functions/source/legacy/` (see the pitfalls section). |
| `api/functions/sink/` | Legacy `SinkFunction`. |
| `api/functions/co/`, `api/functions/aggregation/`, `api/functions/query/` | Function interfaces. |
| `api/windowing/` | Window assigners, triggers, evictors, window types. |
| `api/transformations/` | The transformation tree the client side builds; consumed by `api/graph/`. |
| `api/graph/` | `StreamGraph` and the `StreamGraph → JobGraph` translation (`StreamingJobGraphGenerator`). |
| `api/datastream/`, `api/environment/`, `api/checkpoint/`, `api/connector/sink2/`, `api/lineage/`, `api/watermark/` | Public API surfaces that previously lived in `flink-streaming-java`. |
| `runtime/tasks/` | `StreamTask`, `OneInputStreamTask`, `MultipleInputStreamTask`, `SourceStreamTask`, `SourceOperatorStreamTask`, `OperatorChain`, the `AsyncCheckpointRunnable`. |
| `runtime/io/` | The streaming-specific input/output processors (`StreamTaskNetworkInput`, `RecordWriterOutput`). |
| `runtime/streamrecord/`, `runtime/watermark/`, `runtime/watermarkstatus/` | `StreamRecord` and the watermark + idleness machinery. |
| `runtime/operators/` | Pre-built streaming operators (windowing, async I/O, joins). |
| `runtime/partitioner/` | `KeyGroupStreamPartitioner`, `ForwardPartitioner`, `RescalePartitioner`, etc. |
| `runtime/translators/` | Transformation-to-operator translators. |
| `runtime/metrics/`, `runtime/tasks/`, `util/` | Streaming-specific metric groups and the test harnesses (test scope). |

## Architecture & key concepts

### Job lifecycle: from `submit` to a running subtask

1. **Process bootstrap.** A deployment (standalone, YARN, K8s) starts a JVM whose `main()` is a `ClusterEntrypoint` subclass (`runtime/entrypoint/ClusterEntrypoint.java:112`). The entrypoint wires up: configuration loading, `HighAvailabilityServices`, `BlobServer`, `HeartbeatServices`, `MetricRegistry`, `RpcSystem` (via `flink-rpc`), and a `DispatcherResourceManagerComponent`.
2. **Leadership and dispatcher start.** The `Dispatcher` (`runtime/dispatcher/Dispatcher.java`, ~2k lines) participates in leader election; the winner exposes the cluster REST API via `DispatcherRestEndpoint` and accepts `JobGraph` submissions.
3. **Per-job leadership.** For each submitted job, `Dispatcher` constructs a `JobManagerRunner` (via `JobMasterServiceLeadershipRunnerFactory`) that performs leader election scoped to that job and, on winning, instantiates a `JobMaster` (`runtime/jobmaster/JobMaster.java`, ~1.8k lines).
4. **Scheduler & ExecutionGraph.** `JobMaster` builds an `ExecutionGraph` (`runtime/executiongraph/DefaultExecutionGraph.java`) from the `JobGraph`, then hands it to a `SchedulerNG` implementation (`DefaultScheduler`, `AdaptiveScheduler`, or `AdaptiveBatchScheduler`). The scheduler computes a `JobSchedulingPlan`, requests slots from the `SlotPool`, which in turn requests them from `ResourceManager`.
5. **Slot allocation.** `ResourceManager` (`runtime/resourcemanager/ResourceManager.java`) holds the cluster's `SlotManager` (declarative or `FineGrainedSlotManager`). It may ask its active driver (YARN / K8s / standalone) to start new TMs.
6. **Deployment.** Once slots are available, the scheduler builds a `TaskDeploymentDescriptor` per subtask and calls `TaskExecutor.submitTask` over RPC.
7. **Task execution.** `TaskExecutor` (`runtime/taskexecutor/TaskExecutor.java`, ~2.9k lines) instantiates a `Task` (`runtime/taskmanager/Task.java`, ~1.9k lines), which is a `Runnable`. The `Task` thread loads the user code and invokes its `AbstractInvokable` — for streaming jobs this is a `StreamTask` (`streaming/runtime/tasks/StreamTask.java`), which constructs the `OperatorChain` and runs the mailbox loop.

### Checkpointing

`CheckpointCoordinator` (`runtime/checkpoint/CheckpointCoordinator.java`, line 101, ~2.4k lines) runs on the JobMaster main thread executor. It periodically issues `triggerCheckpoint` to source executions; barriers flow through the topology, and each operator's `snapshotState` is invoked. Snapshot work is split into a **synchronous** phase (capture state on the task thread under the checkpoint lock) and an **asynchronous** phase (write to `CheckpointStorage` from `AsyncCheckpointRunnable`). Tasks `acknowledgeCheckpoint` back via the `CheckpointCoordinatorGateway`; once all subtasks (plus master hooks) ack, the coordinator materialises a `CompletedCheckpoint` into the `CompletedCheckpointStore` (ZK, K8s ConfigMap, or in-memory).

Sub-areas to know:
- `runtime/checkpoint/channel/` — `ChannelStateWriter` and `SequentialChannelStateReader` implement unaligned-checkpoint in-flight buffer persistence.
- `runtime/checkpoint/hooks/` — `MasterHook`s allow source-level coordination (Kafka, Pulsar).
- `runtime/checkpoint/filemerging/` — file-merging snapshot strategy (FLIP-306).
- `EmbeddedCompletedCheckpointStore` for `MiniCluster` runs; `DefaultCompletedCheckpointStore` for production HA.

See:
- [`codedocs/flink-exactly-once-checkpointing-deep-dive.md`](../codedocs/flink-exactly-once-checkpointing-deep-dive.md)
- [`codedocs/flink-unaligned-checkpoint-watermark-recovery.md`](../codedocs/flink-unaligned-checkpoint-watermark-recovery.md)
- [`codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md`](../codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md)

### State backends infrastructure

This module owns the *abstract framework* for state backends. Concrete on-disk implementations live in `flink-state-backends/`. Key pieces in `runtime/state/`:
- `StateBackend` SPI — `AbstractStateBackend`, `ConfigurableStateBackend`.
- `CheckpointableKeyedStateBackend`, `AbstractKeyedStateBackend`, `AbstractKeyedStateBackendBuilder` — the abstract class every backend (heap, RocksDB, ForSt, changelog) extends.
- `OperatorStateBackend` — non-keyed operator state.
- `KeyGroupRange`, `KeyGroupRangeAssignment` — the partition unit for rescaling.
- `SnapshotStrategy` framework and `SnapshotStrategyRunner` — implements the sync/async snapshot split.
- `CheckpointStorage` / `CheckpointStorageLoader` / `CheckpointStorageAccess` — pluggable persistence target.
- `runtime/state/heap/` — the built-in `HeapKeyedStateBackend` plus `CopyOnWriteStateTable`/`CopyOnWriteStateMap` for the copy-on-write snapshot semantics.
- `runtime/state/ttl/` — the `TtlStateFactory` framework that wraps any backend state with TTL bookkeeping.
- `runtime/state/changelog/` — generic-changelog SPI used by `flink-statebackend-changelog`.

### Network stack

The default shuffle service is `NettyShuffleService` (`runtime/io/network/NettyShuffleEnvironment.java` + `runtime/shuffle/NettyShuffleMaster.java`). The flow:

- Output side: `ResultPartition` (`runtime/io/network/partition/`) → `ResultSubpartition` views → `PartitionRequestQueue`. `BufferWritingResultPartition` is the streaming path; `BoundedBlockingResultPartition` is the batch / hybrid path.
- Input side: `InputGate` (`runtime/io/network/partition/consumer/`) hosts `LocalInputChannel` or `RemoteInputChannel`. `CreditBasedPartitionRequestClientHandler` (`runtime/io/network/netty/`) implements credit-based flow control.
- Buffer management: `NetworkBufferPool` (global) feeds per-gate `LocalBufferPool`s; segments are 32 KB by default `MemorySegment`s. `BufferCompressor`/`BufferDecompressor` plug in optional Snappy/LZ4/zstd.
- Buffer debloating lives in `runtime/throughput/` and adjusts in-flight buffer size dynamically.

See:
- [`codedocs/flink-network-credit-flow-control-tuning.md`](../codedocs/flink-network-credit-flow-control-tuning.md)
- [`codedocs/flink-network-direct-memory-netty.md`](../codedocs/flink-network-direct-memory-netty.md)

### RPC

The `RpcGateway` / `RpcEndpoint` / `RpcService` interfaces live in **`flink-rpc-core`** under package `org.apache.flink.runtime.rpc` (so the package name suggests they live here, but they don't — verified with `find`). `flink-runtime` *uses* them. The pekko-based implementation is `flink-rpc-akka` (loaded via `flink-rpc-akka-loader`). Every JM/RM/TM endpoint (`Dispatcher`, `JobMaster`, `ResourceManager`, `TaskExecutor`) extends `FencedRpcEndpoint` from `flink-rpc-core`.

### REST + Web UI

`runtime/rest/RestServerEndpoint.java` is the abstract HTTP base; `DispatcherRestEndpoint` (in `runtime/dispatcher/`) and `WebMonitorEndpoint` (in `runtime/webmonitor/`) extend it. Handlers live under `runtime/rest/handler/` (job, cluster, async-operation, JAR upload, savepoint, profiler, async profiler). The static UI assets are served by `flink-runtime-web`, a separate module that depends on this one.

### Resource management

- `ResourceManager` is an `RpcEndpoint` and the contact point for `TaskExecutor`s and `JobMaster`s alike.
- Two `SlotManager` implementations (`runtime/resourcemanager/slotmanager/`): the older declarative one and the **`FineGrainedSlotManager`** (default in 2.x) which allows per-slot CPU/memory profiles.
- The "active" RM variants (YARN, K8s) live in `flink-yarn` and `flink-kubernetes` and subclass `ActiveResourceManager` here.
- `StandaloneResourceManager` is the no-driver variant used by sessions and `MiniCluster`.

### Scheduling

`runtime/scheduler/` contains:
- `DefaultScheduler` — eager + pipelined-region restart (the streaming default).
- `AdaptiveScheduler` (`scheduler/adaptive/`) — for reactive mode: an FSM (`Created → CreatingExecutionGraph → Executing → Restarting/Canceling → Finished/Failed`) that adjusts parallelism to available resources.
- `AdaptiveBatchScheduler` (`scheduler/adaptivebatch/`) — speculative execution + dynamic parallelism inference for batch jobs.
- Region failover is implemented by `PipelinedRegionSchedulingStrategy` together with `FailoverStrategy` (only "region" is supported in 2.x).
- `runtime/scheduler/slowtaskdetector/` powers speculative execution; `stopwithsavepoint/` implements the `stop-with-savepoint` choreography.

### The "legacy" streaming sources

The `SourceFunction` interface and its concrete subclasses live at
`flink-runtime/src/main/java/org/apache/flink/streaming/api/functions/source/legacy/`:

- `SourceFunction`, `ParallelSourceFunction`, `RichSourceFunction`, `RichParallelSourceFunction`
- Built-ins: `FromElementsFunction`, `FromIteratorFunction`, `FromSplittableIteratorFunction`, `StatefulSequenceSource`, `SocketTextStreamFunction`, `ContinuousFileMonitoringFunction`, `FileMonitoringFunction`, `FileReadFunction`, `InputFormatSourceFunction`.

This is why **examples that use `SourceFunction` need `flink-runtime` on the classpath**, even though they look like a pure `flink-streaming-java` API. The modern FLIP-27 `Source` interface (in `flink-core` + `flink-datastream-api`) is preferred; `SourceFunction` is retained for backward compatibility.

## Important public APIs

Engine classes (Flink-internal but you'll see them all the time):

- **`ClusterEntrypoint`** (`runtime/entrypoint/ClusterEntrypoint.java:112`) — abstract `main()` framework for every JobManager process; subclassed for session, application, YARN, K8s.
- **`Dispatcher`** (`runtime/dispatcher/Dispatcher.java`) — receives `JobGraph` submissions, persists them, spawns `JobMaster`s, owns the REST cluster API.
- **`JobMaster`** (`runtime/jobmaster/JobMaster.java`) — per-job leader; talks to `ResourceManager`, holds the scheduler, talks to TMs.
- **`ResourceManager`** (`runtime/resourcemanager/ResourceManager.java`) — cluster-wide; owns the `SlotManager` and (in active mode) starts/stops TMs.
- **`TaskExecutor`** (`runtime/taskexecutor/TaskExecutor.java`) — TM-side RPC endpoint; manages slots and `Task`s.
- **`Task`** (`runtime/taskmanager/Task.java`) — the `Runnable` that runs one subtask attempt on a TM thread.
- **`ExecutionGraph` / `ExecutionJobVertex` / `ExecutionVertex` / `Execution`** (`runtime/executiongraph/`) — the parallelized execution model.
- **`JobGraph` / `JobVertex` / `JobEdge`** (`runtime/jobgraph/`) — the compiled, pre-parallelization job representation.
- **`SchedulerNG`** + `DefaultScheduler` / `AdaptiveScheduler` / `AdaptiveBatchScheduler` — the scheduler family.
- **`CheckpointCoordinator`** (`runtime/checkpoint/CheckpointCoordinator.java`) — orchestrates checkpoints/savepoints.
- **`CompletedCheckpointStore`** + `DefaultCompletedCheckpointStore` — persists confirmed checkpoint metadata for recovery.
- **`HighAvailabilityServices`** — leader election, leader retrieval, checkpoint/jobgraph stores, BLOB store.
- **`BlobServer` / `BlobCacheService`** — JAR/blob distribution.
- **`RestServerEndpoint`** — REST base; subclassed by `DispatcherRestEndpoint`.

State & I/O:

- **`AbstractKeyedStateBackend` / `CheckpointableKeyedStateBackend`** — the contract every keyed backend implements.
- **`OperatorStateBackend`** — non-keyed operator state.
- **`CheckpointStorage` / `CheckpointStorageAccess`** — durable target for checkpoint data.
- **`InternalTimerService` / `InternalTimerServiceImpl`** — keyed event-time/processing-time timers (`streaming/api/operators/`).
- **`ChannelStateWriter`** — unaligned-checkpoint channel-state persistence.
- **`NettyShuffleEnvironment`** — TaskManager-side shuffle environment; the lifeline for buffers and channels.
- **`ResultPartition` / `InputGate`** — output/input plumbing for each subtask.

Streaming:

- **`StreamTask`** (`streaming/runtime/tasks/StreamTask.java`) — the mailbox-driven invokable that runs every streaming subtask.
- **`OperatorChain`** — the chain of `StreamOperator`s in a single subtask; manages chained `Output`s and the head broadcast.
- **`AbstractStreamOperator` / `AbstractStreamOperatorV2` / `AbstractUdfStreamOperator`** — operator base classes.
- **`StreamGraph` / `StreamingJobGraphGenerator`** (`streaming/api/graph/`) — translation pipeline `Transformation → StreamGraph → JobGraph`.
- **Watermark/Idleness**: `streaming/runtime/watermark/`, `streaming/runtime/watermarkstatus/`, `streaming/api/watermark/Watermark`.
- **`SourceFunction`** + family (legacy) under `streaming/api/functions/source/legacy/`.

## Internal flows

### 1. Job submission → first subtask running

```
flink run …
  → RestClusterClient POSTs JobGraph to /jobs
  → DispatcherRestEndpoint → Dispatcher.submitJob
  → ExecutionPlanStore.put(jobGraph)            (HA / ZK / K8s)
  → Dispatcher creates JobManagerRunnerImpl
       └─ JobMasterServiceLeadershipRunner contends for /leader/jobs/<jid>
            └─ on win: instantiates JobMaster
  → JobMaster.start
       ├─ Build DefaultExecutionGraph from JobGraph
       ├─ Register with ResourceManager
       └─ Hand graph to SchedulerNG
  → DefaultScheduler.startScheduling
       └─ requests N slots via SlotPool.requestNewAllocatedSlots
            └─ DeclarativeSlotPool → JobMaster → RM.declareRequiredResources
                 └─ FineGrainedSlotManager allocates from registered TMs (or asks active driver to launch one)
                      └─ TM offers slot back via TaskExecutor.offerSlots
  → DefaultExecutionDeployer builds TaskDeploymentDescriptor per subtask
  → TaskExecutor.submitTask(TDD)
       └─ new Task(...) on a TaskManager thread
            └─ task.run() → loadInvokable() → StreamTask.invoke()
                 └─ StreamTask.beforeInvoke (operator chain init, restore state)
                 └─ StreamTask.runMailboxLoop()
```

### 2. Checkpoint trigger → JobManager-acknowledged

```
CheckpointCoordinator.triggerCheckpoint (scheduled, main-thread executor)
  ├─ Master-hook triggers (Kafka source, etc.)
  └─ Execution.triggerCheckpointHelper for every source subtask
       → TaskExecutor → Task → StreamTask.triggerCheckpointAsync
            ├─ inject CheckpointBarrier into output buffers
            └─ run synchronous state capture under the checkpoint lock
                 → SnapshotStrategy.snapshot returns a RunnableFuture
                      → schedule on the AsyncOperationsThreadPool as AsyncCheckpointRunnable
                           → write to CheckpointStorage; build SubtaskState
                                → TaskStateManager.reportTaskStateSnapshots
                                     → CheckpointResponder.acknowledgeCheckpoint (RPC)
CheckpointCoordinator.receiveAcknowledgeMessage
  ├─ once all acks + master-hook acks present: build CompletedCheckpoint
  ├─ persist via CompletedCheckpointStore (e.g. ZK)
  ├─ notify all execs (`notifyCheckpointComplete`)
  └─ subsume older checkpoints; trigger CheckpointsCleaner
```

### 3. State recovery on failover

```
Task throws → Task.failExternally → JobMaster.notifyExecutionStateChange
  → DefaultScheduler.handleTaskFailure
       └─ PipelinedRegionFailoverStrategy computes failover region
            → cancel all execs in region, allocate fresh slots, redeploy
  → on (re)deployment, TaskDeploymentDescriptor contains JobManagerTaskRestore
       (built by CheckpointCoordinator.restoreLatestCheckpointedStateToAll)
  → StreamTask.beforeInvoke
       ├─ OperatorChain.initializeStateAndOpenOperators
       │    └─ each AbstractStreamOperator.initializeState
       │         └─ StateInitializationContext exposes keyed/operator state handles
       │              └─ KeyedStateBackend.restore from KeyedStateHandle list
       └─ if unaligned: SequentialChannelStateReader replays in-flight buffers
  → mailbox loop resumes
```

## Tests

The module publishes a **test-jar** containing operator/process-function test harnesses. Pull it into another module like this:

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-runtime</artifactId>
  <version>${flink.version}</version>
  <type>test-jar</type>
  <scope>test</scope>
</dependency>
```

Key harnesses (under `src/test/java/org/apache/flink/streaming/util/`):

- `org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness` — base class.
- `org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness`
- `org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness`
- `org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness`
- `org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness`
- `org.apache.flink.streaming.util.MultiInputStreamOperatorTestHarness`, `KeyedMultiInputStreamOperatorTestHarness`
- `org.apache.flink.streaming.util.BroadcastOperatorTestHarness`, `KeyedBroadcastOperatorTestHarness`
- `org.apache.flink.streaming.util.SourceOperatorTestHarness`
- `org.apache.flink.streaming.util.ProcessFunctionTestHarnesses` — convenience factory for testing `ProcessFunction`/`KeyedProcessFunction`/`KeyedCoProcessFunction`/`BroadcastProcessFunction` without writing operator-level test code.
- `org.apache.flink.streaming.util.asyncprocessing.AsyncKeyedOneInputStreamOperatorTestHarness` etc. — async-state-access variants for FLIP-425 testing.
- `org.apache.flink.streaming.runtime.operators.windowing.TriggerTestHarness` / `SimpleTriggerTestHarness` / `AsyncTriggerTestHarness` — for window trigger logic.

For full clusters in unit tests prefer `MiniCluster` (`runtime/minicluster/MiniCluster`) or the `MiniClusterWithClientResource` extension in `flink-test-utils`.

## Add-opens / module access on JDK 17+

`pom.xml:43-55` declares `<surefire.module.config>` flags required to run this module's tests under JDK 17+:

```
--add-opens=java.base/java.util=ALL-UNNAMED            # JobManagerProcessUtilsTest
--add-opens=java.base/java.lang=ALL-UNNAMED            # ConnectionUtilsTest
--add-opens=java.base/java.net=ALL-UNNAMED             # ConnectionUtilsTest
--add-opens=java.base/java.io=ALL-UNNAMED              # OperatorStateBackendTest
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED # AsynchronousFileIOChannelTest
-Djunit.platform.reflection.search.useLegacySemantics=true
```

On **JDK 21+** an additional flag is appended via the `java21` profile (`pom.xml:490-501`):

```
-Djava.security.manager=allow                          # AbstractFineGrainedSlotManagerITCase
```

(`SecurityManager` is being removed; this is the transitional flag.) JDK 8/11 must *not* set the `java.security.manager=allow` flag — the comment in the pom explains that doing so hangs surefire there. Any downstream module that runs `flink-runtime` code in its test classloader needs the same flags.

## Pitfalls & gotchas

- **`SourceFunction` is in this module, not `flink-streaming-java`.** Look under `streaming/api/functions/source/legacy/`. Examples (e.g. branch `TwoPhaseCountDeduplicatedEventTimeV2`) using `SourceFunction` must list `flink-runtime` as a `compile` dependency. The same applies to `RichSourceFunction`, `ContinuousFileMonitoringFunction`, etc.
- **Test-jar requirement for operator harnesses.** The `OneInputStreamOperatorTestHarness` and friends are only available via the `test-jar` artifact of this module — adding `flink-runtime` alone won't pull them in.
- **JDK 17+ module access.** Anything that reflectively pokes `java.base/java.lang`, `java.io`, `java.net`, `java.util`, or `java.util.concurrent` (the runtime does, particularly the memory & process utilities) needs the `--add-opens` listed above. Failing tests often manifest as `InaccessibleObjectException` or hung surefire forks.
- **`CheckpointCoordinator` runs on the main-thread executor.** Anything submitted to it must be non-blocking; long-running work must be punted to `IOExecutor`/`AsyncOperationsThreadPool`. The class is ~2.4k lines and not thread-safe by design — *only* call its methods from the JobMaster main thread.
- **State TTL & checkpoint retention.** TTL state under `runtime/state/ttl/` is keyed-only and applies lazily on read or via incremental cleanup — it does not shrink RocksDB files until compaction. See [`codedocs/flink-state-ttl-architecture.md`](../codedocs/flink-state-ttl-architecture.md). Retained externalized checkpoints (`CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION`) are *not* cleaned up by `CheckpointsCleaner` — the user owns deletion.
- **`flink-rpc` package surprise.** `RpcGateway`, `RpcEndpoint`, `RpcService` live in `flink-rpc-core` even though their Java package is `org.apache.flink.runtime.rpc`. Do not grep for the interface in `flink-runtime/src/main/java/.../runtime/rpc/` — it isn't there.
- **`StreamTask` mailbox.** All operator callbacks must run on the task thread (the mailbox). Calling state-backend or output APIs off-thread is undefined behavior. The mailbox executor and `MailboxProcessor` enforce this. See `streaming/runtime/tasks/StreamTaskActionExecutor.java`.
- **Heartbeat timeouts vs. RPC timeouts** are configured separately; misalignment yields confusing "Task is not registered" messages on slow networks. See `runtime/heartbeat/`.
- **Adaptive scheduler is an FSM.** Changing scheduler state from outside the FSM (e.g. tests) requires going through the public transitions in `scheduler/adaptive/AdaptiveScheduler.java`.

## Related modules / codedocs

Deep-dives that explain runtime internals:

- [`codedocs/flink-exactly-once-checkpointing-deep-dive.md`](../codedocs/flink-exactly-once-checkpointing-deep-dive.md) — barriers, alignment, two-phase commit.
- [`codedocs/flink-unaligned-checkpoint-watermark-recovery.md`](../codedocs/flink-unaligned-checkpoint-watermark-recovery.md) — `ChannelStateWriter`, in-flight buffer recovery, watermark behavior.
- [`codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md`](../codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md) — window state shape, timers, incremental checkpoints.
- [`codedocs/flink-window-event-assignment-and-state-lifecycle.md`](../codedocs/flink-window-event-assignment-and-state-lifecycle.md) — window assigner / state cleanup.
- [`codedocs/flink-accumulating-vs-purging-and-allowed-lateness.md`](../codedocs/flink-accumulating-vs-purging-and-allowed-lateness.md).
- [`codedocs/flink-network-credit-flow-control-tuning.md`](../codedocs/flink-network-credit-flow-control-tuning.md) — credit flow, buffer sizing.
- [`codedocs/flink-network-direct-memory-netty.md`](../codedocs/flink-network-direct-memory-netty.md) — Netty direct-buffer accounting.
- [`codedocs/flink-memory-configuration.md`](../codedocs/flink-memory-configuration.md) — JM/TM memory model.
- [`codedocs/flink-serialization-deep-dive.md`](../codedocs/flink-serialization-deep-dive.md) — `TypeSerializer` lifecycle, snapshot migration.
- [`codedocs/flink-state-ttl-architecture.md`](../codedocs/flink-state-ttl-architecture.md).
- [`codedocs/flink-rocksdb-state-backend-tuning.md`](../codedocs/flink-rocksdb-state-backend-tuning.md).
- [`codedocs/flink-kubernetes-ha-deep-dive.md`](../codedocs/flink-kubernetes-ha-deep-dive.md) — K8s `HighAvailabilityServices`, leader election.
- [`codedocs/flink-autoscaler-deep-dive.md`](../codedocs/flink-autoscaler-deep-dive.md) — `AdaptiveScheduler` interactions with the autoscaler.

Adjacent modules:

- `flink-rpc-core` / `flink-rpc-akka` — RPC contract & implementation.
- `flink-state-backends/flink-statebackend-rocksdb` — plugs into the state-backend SPI in this module.
- `flink-statebackend-changelog`, `flink-statebackend-forst` — additional backends.
- `flink-runtime-web` — UI assets served via the REST endpoint defined here.
- `flink-clients` — depends on this module for `JobGraph`, REST submission helpers.
- `flink-streaming-java` — historically owned the streaming runtime; now mostly a thin compatibility/API shim on top of `flink-runtime`.
