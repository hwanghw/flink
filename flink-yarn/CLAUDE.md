# `flink-yarn`

## Purpose

`flink-yarn` is the **YARN deployment backend**. It implements every piece
needed to run Flink on Hadoop YARN: from `flink run -t yarn-…` on the client
through application-master bootstrap, container resource requests, TaskExecutor
launch, log aggregation, Kerberos / delegation-token handling, and shutdown.

It plugs into `flink-clients`' SPIs at three levels:

- **Client side**: `YarnClusterDescriptor` (the deployer),
  `YarnClusterClientFactory` (SPI: `ClusterClientFactory`),
  `YarnSessionClusterExecutorFactory` (SPI: `PipelineExecutorFactory`),
  `FlinkYarnSessionCli` (SPI: `CustomCommandLine` — adds `-yid`, `-ynm`,
  `-yqu`, etc. to `bin/flink`).
- **JobManager / ApplicationMaster side**: `YarnSessionClusterEntrypoint` and
  `YarnApplicationClusterEntryPoint` — the AM `main()` classes.
- **ResourceManager side**:
  `YarnResourceManagerDriver` (extends `AbstractResourceManagerDriver` from
  `flink-runtime`'s active resource manager framework) which talks to the
  YARN ResourceManager via `AMRMClientAsync` and to NodeManagers via
  `NMClientAsync` to launch/destroy `YarnWorkerNode`s on demand.

Per-job mode is **removed for YARN since Flink 1.15**; only **session** and
**application** modes remain. The values are `yarn-session` and
`yarn-application` (see `YarnDeploymentTarget`).

## Where it fits

```
                       flink run -t yarn-application <jar>           bin/yarn-session.sh
                                       │                                       │
                                       ▼                                       ▼
                            YarnClusterDescriptor.deployApplicationCluster   .deploySessionCluster
                                       │
                              (upload jars, build ContainerLaunchContext)
                                       │
                                       ▼
                        YARN ResourceManager allocates AM container
                                       │
                                       ▼
                YarnApplicationClusterEntryPoint  /  YarnSessionClusterEntrypoint
                                       │
                                       ▼
              Dispatcher + YarnResourceManagerDriver (the AM)
                                       │
                       ┌───────────────┴───────────────┐
                       ▼                               ▼
              AMRMClientAsync                   NMClientAsync
              (request containers)              (launch TM in container)
                       │                               │
                       ▼                               ▼
              YARN RM allocates                 YarnTaskExecutorRunner
              containers                        starts in TM container
```

## Maven coordinates

- Group / Artifact / Version: `org.apache.flink:flink-yarn:2.3-SNAPSHOT`
- Packaging: `jar`
- Surefire opens `java.base/java.util` (env-var setting via `CommonTestUtils#setEnv`).

## Dependencies

### Direct

- `flink-runtime` — `ResourceManagerDriver`, `ClusterEntrypoint`,
  `Dispatcher`, `BootstrapTools`, `ContaineredTaskManagerParameters`,
  delegation-token framework.
- `flink-clients` — `ClusterDescriptor`, `ClusterClient`,
  `ApplicationClusterEntryPoint`, `CustomCommandLine`, `RestClusterClient`,
  `PackagedProgram`.
- `org.apache.hadoop:hadoop-common`, `hadoop-hdfs`, `hadoop-yarn-common`,
  `hadoop-yarn-client`, `hadoop-mapreduce-client-core` — the actual YARN
  client APIs (`YarnClient`, `AMRMClientAsync`, `NMClientAsync`, `Path`,
  `FileSystem` for HDFS jar staging).
- (test) `flink-test-utils-junit`, `flink-runtime` test-jar,
  `hadoop-hdfs/common` test-jars, `hadoop-aws`, AWS SDK (for
  `YarnFileStageTestS3ITCase`).

### Used by

- `flink-yarn-tests` — mini-YARN integration tests.
- `flink-dist` — `opt/flink-yarn-*.jar`. The user moves it to `lib/` (or sets
  `HADOOP_CLASSPATH`) to enable YARN deployment.
- `flink-end-to-end-tests` (parts) — YARN-flavored E2E suites.

## Source layout

```
org.apache.flink.yarn
├── (root)
│   ├── YarnClusterDescriptor.java          The client-side deployer
│   ├── YarnClusterClientFactory.java       SPI impl: ClusterClientFactory
│   ├── YarnClientYarnClusterInformationRetriever.java
│   ├── YarnClusterInformationRetriever.java
│   ├── YarnResourceManagerDriver.java      AM-side: requests containers, launches TMs
│   ├── YarnResourceManagerClientFactory.java + Default impl    AMRMClientAsync factory
│   ├── YarnNodeManagerClientFactory.java + Default impl        NMClientAsync factory
│   ├── YarnTaskExecutorRunner.java         TaskManager main() launched in TM containers
│   ├── YarnApplicationFileUploader.java    Stage jars/files into HDFS
│   ├── YarnLocalResourceDescriptor.java    On-HDFS resource descriptor
│   ├── YarnWorkerNode.java                 Tracks one allocated container
│   ├── YarnConfigKeys.java                 Env-var key names passed AM → TM
│   ├── Utils.java                          Hadoop ↔ Flink config conversion, security helpers
│   ├── AMRMClientAsyncReflector.java       Reflective access to non-public Hadoop API
│   ├── ContainerRequestReflector.java      Same — supports older Hadoop versions
│   ├── RegisterApplicationMasterResponseReflector.java  Read fields not in public API
│   ├── ResourceInformationReflector.java   Set resource profiles (GPUs/FPGAs via YARN)
│   └── TaskExecutorProcessSpecContainerResourcePriorityAdapter.java
│                                            Maps Flink resource specs ↔ YARN Resource + Priority
├── cli/
│   ├── FlinkYarnSessionCli.java            CustomCommandLine: -yid, -ynm, -yqu, -yjm, -ytm, -ys, etc.
│   ├── AbstractYarnCli.java                Shared parent
│   ├── FallbackYarnSessionCli.java         Stub used when Hadoop classpath isn't found (-yid only, errors loudly)
│   └── YarnApplicationStatusMonitor.java   Polls YARN RM for AM state
├── configuration/
│   ├── YarnConfigOptions.java              ~50 yarn.* ConfigOptions
│   ├── YarnConfigOptionsInternal.java
│   ├── YarnDeploymentTarget.java           enum {SESSION, APPLICATION}
│   ├── YarnLogConfigUtil.java              Resolve log4j config for AM/TM containers
│   └── YarnResourceManagerDriverConfiguration.java   Snapshot of AM env vars
├── entrypoint/
│   ├── YarnSessionClusterEntrypoint.java       AM main() for session mode
│   ├── YarnApplicationClusterEntryPoint.java   AM main() for application mode
│   ├── YarnEntrypointUtils.java                Load Flink conf from env, configure logging
│   ├── YarnResourceManagerFactory.java         RM factory used by both entrypoints
│   └── YarnWorkerResourceSpecFactory.java      Builds WorkerResourceSpec from yarn settings
└── executors/
    ├── YarnSessionClusterExecutor.java         PipelineExecutor for `yarn-session`
    └── YarnSessionClusterExecutorFactory.java  SPI impl: PipelineExecutorFactory
```

37 production `.java` files; 31 unit-test files.

## Architecture & key concepts

### Two deployment targets

`YarnDeploymentTarget` recognises:

- `yarn-session` — long-lived multi-job cluster; client uses `bin/yarn-session.sh`
  or `flink run -t yarn-session -yid <appid> …`.
- `yarn-application` — one cluster per app; `flink run-application -t yarn-application`.

Per-job (`yarn-per-job`) was deprecated in 1.15 and removed; the code paths in
`YarnClusterDescriptor` for that mode are gone.

### Client side: `YarnClusterDescriptor`

The single biggest class in the module (~1700 LOC). Responsibilities:

1. Acquire a Hadoop `YarnClient`, validate the queue, find a suitable
   `ApplicationId`.
2. Validate `ClusterSpecification` (master memory, TM memory, slots) against
   YARN's maximum container resource.
3. Stage all required files into HDFS via `YarnApplicationFileUploader`:
   `flink-dist`, user jars, plugins, `flink-conf.yaml`, log4j config,
   `lib/`, `plugins/`, hadoop config, any `--ship-files`.
4. Build a `ContainerLaunchContext` with:
   - the AM command line (`java … YarnApplicationClusterEntryPoint`),
   - environment variables (`_FLINK_CLASSPATH`, `_FLINK_DIST_JAR`,
     `_APP_ID`, `_CLIENT_HOME_DIR`, `_CLIENT_SHIP_FILES`,
     `_HADOOP_USER_NAME`, plus Kerberos token bytes),
   - local resources (the staged HDFS files).
5. Acquire Hadoop delegation tokens via the
   `DefaultDelegationTokenManager` (HDFS / HBase / Hive / Kafka, etc.) and
   ship them in the container launch context.
6. Submit the application; poll for `RUNNING`; resolve the AM host/port and
   return a `RestClusterClient<ApplicationId>`.

For application mode it additionally writes the user jar URL into
`PipelineOptions.JARS` and configures `application.main-class` so the AM
runs the user main().

### AM side: `YarnResourceManagerDriver`

Lives inside the JobManager process. Implements
`ResourceManagerDriver<YarnWorkerNode>` from `flink-runtime`'s
`active` resource-manager framework:

- `requestResource(TaskExecutorProcessSpec)` → builds a YARN `ContainerRequest`
  with the right `Resource` (memory + vcores + external resources mapped via
  `ResourceInformationReflector`) and `Priority`. Priorities partition the
  outstanding requests by `TaskExecutorProcessSpec` so the
  `TaskExecutorProcessSpecContainerResourcePriorityAdapter` can demultiplex
  callbacks.
- AMRM callback handler (`AMRMCallbackHandler`):
  - `onContainersAllocated` → for each container, find a waiting request
    with matching priority, fulfill its future, build a `ContainerLaunchContext`
    for the TM (running `YarnTaskExecutorRunner`), and call
    `nodeManagerClient.startContainerAsync(...)`.
  - `onContainersCompleted` → free the slot in
    `WorkerResourceSpecToContainerResourceMap`, notify the framework so it
    can decide to re-request.
  - `onShutdownRequest` → trigger graceful AM termination.
  - `onNodesUpdated` / `onUpdatedNodes` → update blocked-nodes set (used by
    Flink's blacklist; see `flink-blacklist-filtering-solutions.md`).
- Recovers TMs from a previous attempt: on AM restart, YARN gives the AM the
  list of containers from the previous attempt
  (`registerApplicationMasterResponse.getContainersFromPreviousAttempts`).
  Those are wrapped as `YarnWorkerNode` and recovered, avoiding a full
  re-launch storm.
- Manages YARN heartbeat interval. Warns if the configured Flink heartbeat
  exceeds YARN's expiry interval (would get the AM killed).

### TaskManager side: `YarnTaskExecutorRunner`

`main()` in TM containers. Reads env vars (`_FLINK_CONTAINER_ID`,
`_FLINK_NODE_ID`, etc.) set by the AM, loads `flink-conf.yaml`, installs
Hadoop delegation tokens, then calls
`TaskManagerRunner.runTaskManagerProcessSecurely(...)`.

### Security & delegation tokens

The YARN AM acts as a Hadoop user. `YarnClusterDescriptor`
- Verifies Kerberos credentials via `KerberosLoginProvider`.
- Uses `DefaultDelegationTokenManager` to obtain HDFS/HBase/Hive/etc. tokens.
- Ships `DelegationTokenContainer` bytes through the AM's environment so the
  AM and every TM can authenticate to Hadoop services.
- Periodically refreshes tokens (the runtime's
  `DelegationTokenReceiverRepository` handles propagation to TMs).

### CLI integration

`FlinkYarnSessionCli` is loaded via the `CustomCommandLine` SPI from
`flink-clients` and activates when `HADOOP_CLASSPATH` is set or the `-m
yarn-cluster` / `-t yarn-…` flag is present. It adds:

- `-yid <appid>` — attach to an existing session.
- `-ynm` (name), `-yqu` (queue), `-yjm` / `-ytm` (memory), `-ys` (slots),
  `-yD<k>=<v>` (dynamic property), `-yz` (zookeeper namespace), `-ynl`
  (node-label), `-yt` (ship files), `-yat` (application-type),
  `-yD security.kerberos.…`, `-yh` (help).
- A `yarn-session.sh` wrapper that loops, printing the session's status until
  the user kills it.

## Important public APIs

| Class | Why it matters |
|---|---|
| `YarnClusterDescriptor` | Deploys YARN sessions / applications. |
| `YarnClusterClientFactory` | SPI for `flink-clients` — wraps a `YarnClient` and config. |
| `YarnResourceManagerDriver` | AM ↔ YARN bridge; container lifecycle. |
| `YarnTaskExecutorRunner` | TM JVM entry inside a YARN container. |
| `YarnApplicationClusterEntryPoint` / `YarnSessionClusterEntrypoint` | AM JVM entries. |
| `YarnConfigOptions` | All `yarn.*` configuration keys. |
| `YarnDeploymentTarget` | The deployment-target enum (`yarn-session`, `yarn-application`). |
| `FlinkYarnSessionCli` | `bin/flink` integration; user-facing flags. |
| `YarnApplicationFileUploader` | Jar/file staging into HDFS. |

## Internal flows

### `flink run-application -t yarn-application -c MyMain my.jar`
1. `CliFrontend` selects `GenericCLI` (or `FlinkYarnSessionCli` if HADOOP).
2. `YarnClusterClientFactory.createClusterDescriptor(conf)` → `YarnClusterDescriptor`.
3. `descriptor.deployApplicationCluster(spec, appConfig)`:
   - Pre-flight checks (queue, max memory, security).
   - `YarnApplicationFileUploader.uploadAll(...)` stages jars to
     `${yarn.staging-directory}/<appId>/`.
   - Builds AM `ContainerLaunchContext` with `YarnApplicationClusterEntryPoint`
     as the main class plus env vars for jar location, savepoint path,
     class loader resolve order, etc.
   - `yarnClient.submitApplication(appContext)`; poll `getApplicationReport`
     until `RUNNING`.
4. AM starts. `YarnApplicationClusterEntryPoint.main(args)` →
   `ApplicationClusterEntryPoint.startCluster(...)` →
   `Dispatcher` + `YarnResourceManagerDriver`.
5. `ApplicationDispatcherBootstrap` runs user `main()` in-process; user
   `env.execute()` → `EmbeddedExecutor` submits `JobGraph` to local
   Dispatcher.
6. `ResourceManager` requests containers via the YARN AMRM protocol;
   each granted container runs `YarnTaskExecutorRunner`, which registers
   back with the JM.

### Container failure recovery
1. NM kills the container → AMRM callback `onContainersCompleted` with exit
   status.
2. Driver logs the diagnostics, notifies the framework, decrements the slot
   manager's count.
3. SlotManager re-requests if there are still pending slot requests; driver
   creates a new `ContainerRequest` with the same priority.
4. If the AM itself dies, YARN restarts it (`yarn.application-attempts`).
   On second attempt, `getContainersFromPreviousAttempts` returns the
   surviving TMs; they re-register; the job recovers from the last
   checkpoint via HA.

## Tests

- Unit tests in `src/test/java/org/apache/flink/yarn/`:
  - `YarnResourceManagerDriverTest`, `YarnClusterDescriptorTest`,
    `YarnApplicationFileUploaderTest`,
    `TaskExecutorProcessSpecContainerResourcePriorityAdapterTest`,
    `YarnApplicationClusterEntryPointTest`, etc.
  - Heavy use of `TestingYarnAMRMClientAsync`, `TestingYarnNMClientAsync`
    mock factories and `TestingContainer` / `TestingContainerStatus`.
- `YarnFileStageTest` / `YarnFileStageTestS3ITCase` — file staging into
  HDFS / S3.
- Full mini-YARN-cluster end-to-end tests live in `flink-yarn-tests`
  (separate module); see its CLAUDE.md.

## Pitfalls & gotchas

- **`HADOOP_CLASSPATH` must be set** when running `flink run -t yarn-…`. The
  YARN jars are NOT bundled into `flink-dist`. If absent,
  `FallbackYarnSessionCli` activates and surfaces a clear error message.
- **Per-job mode is removed.** Use `yarn-application`. Old docs / scripts
  using `flink run -m yarn-cluster` still work (they map to per-job →
  application) but should be migrated.
- **YARN heartbeat vs YARN expiry.** If `yarn.heartbeat-delay` >=
  `yarn.am.liveness-monitor.expiry-interval-ms`, YARN will think the AM is
  dead and kill it. The driver constructor warns; fix by lowering Flink's
  heartbeat or raising YARN's expiry.
- **Resource priorities.** Each distinct `TaskExecutorProcessSpec` gets its
  own YARN `Priority`. Mixing many TM shapes (heterogeneous deployments) is
  supported but watch out: `Priority` values are recycled when specs are
  fully drained — debugging stale callbacks can be tricky.
- **External resources (GPU/FPGA) via YARN need
  `external-resource.<name>.yarn.config-key`** to map the Flink resource name
  to YARN's resource name. See `ResourceInformationReflector`.
- **Delegation tokens age out.** For long-running session clusters, configure
  `security.kerberos.token.renewal-interval` and ensure the principal has a
  keytab; the framework refreshes tokens automatically.
- **Reflectors guard against Hadoop version drift.** Methods like
  `RegisterApplicationMasterResponse.getContainersFromPreviousAttempts` were
  added in newer Hadoop versions — the reflectors transparently no-op on
  older releases.
- **`yarn.containers.vcores` defaults to TM slots.** If you set it explicitly
  you can request fractional CPUs (with Capacity Scheduler), but with the
  default it tracks slots, which can over-allocate.
- **Log aggregation requires `yarn.log-aggregation-enable: true` in
  yarn-site.xml.** Otherwise `yarn logs -applicationId …` returns nothing
  after the AM exits and you lose all diagnostics.
- **`/tmp` fills up.** Jar staging directory and Flink's local temp dirs can
  fill the NodeManager's local disks during long-running sessions; set
  `io.tmp.dirs` to a sized partition.

## Related modules / docs

- `flink-clients` — `ClusterDescriptor`, `ClusterClient`, `PipelineExecutor`
  SPIs that this module implements. `RestClusterClient` is the wire client.
- `flink-runtime` — `ClusterEntrypoint`, `AbstractResourceManagerDriver`,
  `Dispatcher`, `BootstrapTools`, delegation-token framework.
- `flink-yarn-tests` — mini-YARN-cluster integration tests.
- `flink-kubernetes` — analogous module for Kubernetes; `KubernetesResourceManagerDriver`
  mirrors `YarnResourceManagerDriver`.
- `codedocs/flink-memory-configuration.md` — JM / TM memory model; relevant
  for `-yjm` / `-ytm` and how `TaskExecutorProcessSpec` maps to YARN
  containers.
- `codedocs/flink-blacklist-filtering-solutions.md` — the
  blocked-nodes / updated-nodes logic that `YarnResourceManagerDriver`
  participates in.
