# `flink-clients`

## Purpose

`flink-clients` is the **job-submission layer**. It contains everything a user (or
a higher-level driver) needs to take a compiled Flink program and run it against
either a remote cluster (standalone / YARN / Kubernetes) or an embedded
`MiniCluster`. Concretely it owns:

- The **`bin/flink` CLI** entry point (`CliFrontend`) — `flink run`, `cancel`,
  `stop`, `savepoint`, `checkpoint`, `list`, `info`.
- The **`PipelineExecutor` SPI** implementations: `LocalExecutor`,
  `RemoteExecutor`, `EmbeddedExecutor`, `WebSubmissionExecutor`, plus the
  abstract `AbstractSessionClusterExecutor` extended by YARN/K8s.
- The **`ClusterClient` / `RestClusterClient`** REST plumbing used to talk to a
  running Dispatcher.
- The **`ClusterClientFactory` / `ClusterDescriptor`** abstractions that
  let session / application mode be plugged in per backend, and the standalone
  implementation (`StandaloneClientFactory`, `StandaloneClusterDescriptor`).
- The **Application-Mode** machinery (`ApplicationClusterEntryPoint`,
  `ApplicationDispatcherBootstrap`, `EmbeddedExecutor`, `ApplicationRunner`)
  that runs user `main()` *inside* the JobManager.
- The **`PackagedProgram`** model — wrapping a user jar + main class +
  arguments, with classloader isolation and manifest parsing.
- The **artifact fetcher** (`ArtifactFetchManager`, `FsArtifactFetcher`,
  `HttpArtifactFetcher`, `LocalArtifactFetcher`) used by container entrypoints
  to download the user jar before launching the JobManager.

It deliberately does **not** know about YARN or Kubernetes; those are
plugged in via `ClusterClientFactory` discovered through
`META-INF/services`.

## Where it fits

```
                          user program (DataStream / Table API)
                                       │
                                       ▼
                              StreamExecutionEnvironment.execute()
                                       │
                                       ▼
                       PipelineExecutor (chosen via DeploymentOptions.TARGET)
                                       │
              ┌─────────────┬──────────┼───────────┬──────────────┬───────────────────┐
              ▼             ▼          ▼           ▼              ▼                   ▼
        LocalExecutor  RemoteExecutor  Embedded    WebSubmission  YarnSessionExecutor  K8sSessionExecutor
        (MiniCluster)  (REST)          (in-JM)     (REST handler) (flink-yarn)         (flink-kubernetes)
```

`flink-clients` defines the contracts; `flink-yarn` and `flink-kubernetes`
implement them. `flink-container` reuses the application-mode entry point for
standalone Docker images.

## Maven coordinates

- Group / Artifact / Version: `org.apache.flink:flink-clients:2.3-SNAPSHOT`
- Packaging: `jar`
- Java module config: `--add-opens=java.base/java.util=ALL-UNNAMED` for chill's
  `ArraysAsListSerializer`.

## Dependencies

### Direct

- `flink-core` — `Configuration`, `ConfigOption`, `Pipeline`, `PipelineExecutor`
  SPI, `JobClient`.
- `flink-runtime` — `JobGraph`, `Dispatcher` / `DispatcherGateway`,
  `RestClient`, `BlobClient`, REST message types, HA services.
- `flink-streaming-java` — `StreamGraph`, `StreamExecutionEnvironment` (used
  by `StreamContextEnvironment` to capture user pipelines).
- `flink-datastream` — DataStream V2.
- `commons-cli` — option parsing for `CliFrontend`.

### Used by

- `flink-yarn`, `flink-kubernetes`, `flink-container` — plug their
  `ClusterDescriptor` / `PipelineExecutor` impls into the SPI.
- `flink-dist` — bundles this jar into `lib/` so `bin/flink` resolves.
- `flink-test-utils`, `flink-tests`, every end-to-end test — uses
  `MiniClusterClient`, `PerJobMiniClusterFactory`, `RestClusterClient`.
- `flink-python` (PyFlink) — invokes `CliFrontend` for `flink run`.
- `flink-table-planner` / SQL Client — uses `PackagedProgram` indirectly when
  submitting SQL jobs as application mode.

## Source layout

```
org.apache.flink.client
├── (root)
│   ├── ClientUtils.java                       Static helpers: jar URL resolution, classloader build
│   ├── FlinkPipelineTranslator.java           Pipeline → JobGraph translator SPI
│   ├── FlinkPipelineTranslationUtil.java      SPI loader + entry: getJobGraph(pipeline, conf, parallelism)
│   └── StreamGraphTranslator.java             Built-in translator for StreamGraph
├── cli/                                       Command-line frontend
│   ├── CliFrontend.java                       The `bin/flink` main class
│   ├── CliFrontendParser.java                 Options registry (-c, -p, -d, -s, --target, -D, ...)
│   ├── CustomCommandLine.java                 SPI: pluggable per-backend CLIs (yarn-session, etc.)
│   ├── AbstractCustomCommandLine.java         Base impl shared by Default/Generic/Yarn CLIs
│   ├── DefaultCLI.java                        Fallback CLI (standalone)
│   ├── GenericCLI.java                        Generic --target-based CLI used for k8s/yarn-application
│   ├── ProgramOptions.java                    Parsed `-c`, `-p`, `--jarfile`, etc.
│   ├── ExecutionConfigAccessor.java           Read/write execution config into Configuration
│   ├── *Options.java                          Per-subcommand option groups (Run/Cancel/Stop/Savepoint/...)
│   └── DynamicPropertiesUtil.java             -D parsing → Configuration
├── deployment/                                Cluster descriptors and factories
│   ├── ClusterClientFactory.java              SPI: discovered via META-INF/services
│   ├── ClusterClientServiceLoader.java        SPI loader
│   ├── ClusterDescriptor.java                 Deploy session / application / per-job (deprecated)
│   ├── ClusterSpecification.java              JM/TM memory + slots needed for a cluster
│   ├── AbstractContainerizedClusterClientFactory.java   Base for YARN/K8s factories
│   ├── StandaloneClient/ClusterDescriptor/ClusterId.java   Standalone impl
│   ├── application/                           Application-mode glue
│   │   ├── ApplicationClusterEntryPoint.java        JM entrypoint that runs user main()
│   │   ├── ApplicationConfiguration.java            jarMainClassName + programArgs
│   │   ├── ApplicationRunner / DetachedApplicationRunner
│   │   ├── ApplicationDispatcherGatewayServiceFactory
│   │   ├── ApplicationDispatcherLeaderProcessFactoryFactory
│   │   ├── PackagedProgramApplication.java          Captures the user app
│   │   ├── cli/ApplicationClusterDeployer.java      Client-side: deploy app cluster
│   │   └── executors/                               In-JM "embedded" PipelineExecutor
│   │       ├── EmbeddedExecutor.java                Submits JobGraph to local Dispatcher
│   │       ├── EmbeddedExecutorFactory.java
│   │       ├── EmbeddedJobClient.java               JobClient impl for embedded mode
│   │       ├── WebSubmissionExecutor.java           Used by /jars/:jarid/run REST handler
│   │       └── WebSubmissionJobClient.java
│   └── executors/                             Session-mode executors
│       ├── AbstractSessionClusterExecutor.java      Generic submit-to-cluster executor
│       ├── LocalExecutor.java + Factory             MiniCluster in-process
│       ├── RemoteExecutor.java + Factory            REST → existing standalone cluster
│       └── PipelineExecutorUtils.java               Pipeline → JobGraph + jar URLs
└── program/                                   Programs and client
    ├── ClusterClient.java                     Interface: submit/cancel/stop/savepoint/list
    ├── ClusterClientProvider.java             Lazy supplier
    ├── ClusterClientJobClientAdapter.java     ClusterClient → JobClient adapter
    ├── MiniClusterClient.java                 ClusterClient over an in-process MiniCluster
    ├── PerJobMiniClusterFactory.java          Spin a per-job MiniCluster (test/IDE use)
    ├── PackagedProgram.java                   User jar + main class + args (child-first CL)
    ├── PackagedProgramUtils.java              createJobGraph from a PackagedProgram
    ├── PackagedProgramRetriever.java          SPI: how to locate the user program
    ├── DefaultPackagedProgramRetriever.java   File-based default (used by container entrypoints)
    ├── StreamContextEnvironment.java          ExecutionEnvironment that captures execute() calls
    ├── StreamPlanEnvironment.java             ExecutionEnvironment that returns the plan w/o running
    ├── artifact/                              User-jar fetching (used by app-mode containers)
    │   ├── ArtifactFetchManager.java          Orchestrates Fs/Http/Local fetchers
    │   ├── FsArtifactFetcher.java             HDFS / S3 / GCS via Flink FileSystem
    │   ├── HttpArtifactFetcher.java           Plain HTTP(S)
    │   └── LocalArtifactFetcher.java          Local FS / file://
    └── rest/                                  REST-based ClusterClient
        ├── RestClusterClient.java             The ClusterClient used by `flink run` / Yarn / K8s
        ├── RestClusterClientConfiguration.java
        ├── UrlPrefixDecorator.java            For path-prefix proxies (e.g. K8s Ingress)
        └── retry/                             Exponential wait strategy for async REST ops
```

94 production `.java` files; 54 test files.

## Architecture & key concepts

### Deployment modes (the central distinction)

Flink supports three deployment modes; `flink-clients` defines the contracts
for all of them.

| Mode | Where user `main()` runs | JobManager lifetime | Typical use |
|---|---|---|---|
| **Session** | On the **client** | Pre-existing, long-lived; many jobs share it | Interactive, ad-hoc, SQL Client |
| **Application** | On the **JobManager** | One JM per application; dies when app finishes | Production deployments — recommended |
| **Per-job** (deprecated since 1.15) | On the client | One JM per job | YARN only; replaced by Application Mode |

Mode is selected by `execution.target` / `DeploymentOptions.TARGET`:
`local`, `remote`, `yarn-session`, `yarn-application`, `kubernetes-session`,
`kubernetes-application`, or `embedded` (in-JM). The `PipelineExecutorFactory`
SPI is keyed on this string.

### PipelineExecutor SPI

`PipelineExecutor` (in `flink-core`, `core/execution/`) is the contract that
`StreamExecutionEnvironment.execute()` calls. Each backend ships a factory
declared in
`META-INF/services/org.apache.flink.core.execution.PipelineExecutorFactory`:

```
flink-clients:        RemoteExecutorFactory, LocalExecutorFactory
flink-clients (app):  EmbeddedExecutorFactory, WebSubmissionExecutorFactory
flink-yarn:           YarnSessionClusterExecutorFactory  (yarn-session)
flink-kubernetes:     KubernetesSessionClusterExecutorFactory  (kubernetes-session)
```

Application-mode executors are *not* discovered via SPI for the user — the JM
itself loads `EmbeddedExecutor` when running an `ApplicationClusterEntryPoint`.

### ClusterDescriptor / ClusterClient

`ClusterDescriptor<ClusterID>` (one per backend) is the deploy-side handle:
`deploySessionCluster(spec)`, `deployApplicationCluster(spec, appConfig)`,
`retrieve(clusterId)` → `ClusterClient`. The corresponding
`ClusterClient<ClusterID>` is the *runtime* handle: submit / cancel / stop with
savepoint / trigger checkpoint / list jobs / send coordination requests.

`RestClusterClient` is the only non-test implementation; it talks to the
JobManager's REST endpoint via `flink-runtime`'s `RestClient`. It includes
`ClientHighAvailabilityServices` so it can re-resolve the leader on
JobManager failover (see `flink-kubernetes-ha-deep-dive.md`).

### Application Mode flow

1. Client side: `ApplicationClusterDeployer.run(config, appConfig)` calls
   `ClusterDescriptor.deployApplicationCluster(spec, appConfig)`.
2. Backend (yarn / k8s / standalone-container) launches a JM whose main class
   is `*ApplicationClusterEntryPoint` (subclass of
   `ApplicationClusterEntryPoint` in this module).
3. JM bootstraps a Dispatcher with `ApplicationDispatcherBootstrap` (in
   `flink-runtime`).
4. Bootstrap loads the user jar through `PackagedProgramRetriever`, invokes
   the user `main()` in a thread, and the user's `env.execute()` calls hit
   `EmbeddedExecutor.execute()`.
5. `EmbeddedExecutor` submits the `JobGraph` to the *local* Dispatcher (no
   network hop) and returns an `EmbeddedJobClient` that watches the job.
6. When the app `main()` returns and all jobs complete, the cluster shuts
   itself down (`execution.shutdown-on-application-finish: true`).

### CliFrontend lifecycle (`bin/flink run …`)

1. `flink-dist/bin/flink` launches `org.apache.flink.client.cli.CliFrontend`.
2. `CliFrontend.parseAndRun(args)` chooses the subcommand
   (`run` / `cancel` / `stop` / `savepoint` / `info` / `list`).
3. The first matching `CustomCommandLine` is selected — the order is
   `flink-yarn` (if HADOOP_CLASSPATH set) → `flink-kubernetes` → `GenericCLI`
   → `DefaultCLI`. Each parses backend-specific flags (`-yid`, `-yat`, …).
4. For `run`: build a `PackagedProgram` from the user jar, resolve the
   `PipelineExecutorFactory` via SPI, call `executeProgram(...)` which
   invokes user `main()`. `StreamContextEnvironment.execute()` then hands the
   `Pipeline` to the chosen `PipelineExecutor`.

### PackagedProgram + classloading

`PackagedProgram` builds a `FlinkUserCodeClassLoader` (child-first or
parent-first per `classloader.resolve-order`) containing the user jar and the
listed dependency jars (`-C`/`--classpath`). It parses the jar manifest for
`Main-Class` and `program-class`, supports zero-arg main and `Program`
implementations. The classloader is closed after job submission completes.

## Important public APIs

| Class | Why it matters |
|---|---|
| `CliFrontend` | `bin/flink` entry point. |
| `PackagedProgram` / `PackagedProgramUtils` | Wrap a user jar; build a `JobGraph` from it. |
| `ClusterClient` | Submit/cancel/stop/savepoint a job on a cluster. |
| `RestClusterClient` | The only non-test implementation; used everywhere. |
| `ClusterDescriptor` | Deploy a session / application cluster. |
| `ClusterClientFactory` | SPI for plugging in YARN / K8s / standalone. |
| `LocalExecutor` / `RemoteExecutor` | Default `PipelineExecutor`s for local + standalone. |
| `EmbeddedExecutor` / `ApplicationClusterEntryPoint` | Application-mode runtime. |
| `MiniClusterClient` / `PerJobMiniClusterFactory` | Run a job in-process (tests, IDE). |
| `ApplicationConfiguration` | jarMainClass + programArgs; carried into the JM. |
| `ClusterSpecification` | JM heap / TM heap / TM slots required. |

## Internal flows

### `flink run my-job.jar`
1. `CliFrontend.run(args)` → parses options, picks `CustomCommandLine`.
2. `PackagedProgram.newBuilder().setJarFile(...).setUserClassPaths(...).build()`.
3. `executeProgram(configuration, program)` → `program.invokeInteractiveModeForExecution()`
   which invokes user `main(args)`.
4. User `env.execute()` →
   `PipelineExecutorServiceLoader.getExecutorFactory(configuration)` →
   `executor.execute(pipeline, configuration, userCodeClassLoader)`.
5. Executor builds `JobGraph` via `FlinkPipelineTranslationUtil`, uploads
   user-jars via `BlobClient`, calls `Dispatcher.submitJob(jobGraph)` over REST.
6. `RestClusterClient` polls job status / accumulators / metrics.

### `flink savepoint <jobId> [-D ...]`
1. `CliFrontend.savepoint` parses options.
2. `RestClusterClient.triggerSavepoint(jobId, targetDirectory, formatType, ...)`
   POSTs to `/jobs/:jobid/savepoints` and polls the trigger ID until
   completion / failure.

### Application mode (YARN/K8s example)
1. Client: `flink run-application -t yarn-application my-job.jar`.
2. `ApplicationClusterDeployer` → `YarnClusterDescriptor.deployApplicationCluster`.
3. JM main: `YarnApplicationClusterEntryPoint.main(args)` →
   `ApplicationClusterEntryPoint.startCluster` (this module).
4. `ApplicationDispatcherBootstrap` (in `flink-runtime`) drives the user main.

## Tests

- `src/test/java/org/apache/flink/client/cli/CliFrontend*Test.java` —
  exhaustive subcommand tests (run/cancel/stop/list/savepoint/checkpoint).
  Often built on `CliFrontendTestBase` + `MockedCliFrontend`.
- `src/test/java/org/apache/flink/client/program/ClientTest.java` and
  `ClientHeartbeatTest.java` — `RestClusterClient` integration tests against
  a `MiniCluster`.
- `src/test/java/org/apache/flink/client/deployment/application/` —
  Application mode and `PackagedProgramApplication` tests.
- `src/test/java/org/apache/flink/client/program/artifact/` —
  `ArtifactFetchManagerTest` covers fs/http/local jar fetching.
- `src/test/java/org/apache/flink/client/testjar/` — tiny test jars
  (`TestJob`, `BlockingJob`, `FailingJob`) compiled into the
  `flink-clients-test-utils` artifact and loaded via custom classloaders.

## Pitfalls & gotchas

- **`StreamExecutionEnvironment.getExecutionEnvironment()` returns
  different subclasses in different modes.** Inside a packaged program it's
  `StreamContextEnvironment` (captures the pipeline); inside `flink info`
  it's `StreamPlanEnvironment` (returns the plan, never runs); inside the
  JM in application mode it's still `StreamContextEnvironment` but with
  `enforceSingleJobExecution=false` so multiple `execute()` calls work.
  Subtle bugs come from assuming you always get a `LocalStreamEnvironment`.
- **`PackagedProgram` closes its classloader.** Once it's closed, calls
  against returned `Class<?>` objects fail with `NoClassDefFoundError`.
  `RestClusterClient` keeps a reference to the user CL for as long as it
  needs to deserialize results.
- **`CustomCommandLine` order matters.** The first one whose `isActive` returns
  true wins. `FlinkYarnSessionCli` checks for `HADOOP_CLASSPATH`; if you set
  that env var but want kubernetes-application, you must pass `-e kubernetes-application`
  or `--target` explicitly.
- **Per-job mode is gone for YARN since 1.15.** Pre-existing per-job code
  paths remain in `ClusterDescriptor` for backward compatibility but use
  Application Mode instead.
- **`execution.shutdown-on-application-finish: true` is the default.** Application
  mode shuts the cluster down when the user `main()` returns. If your main()
  fires-and-forgets a detached job, the cluster will kill it. Set the flag to
  `false` (or `pipeline.jars` overrides) for SQL Gateway-style use cases.
- **Memory configuration on the client.** The CLI process itself runs the user
  `main()` in session/per-job mode; it inherits the env's heap. Building a huge
  `JobGraph` (1000s of operators, large broadcast state) can OOM the client
  even though the cluster is fine. See `flink-memory-configuration.md`.
- **`-yD`/`-D` dynamic properties replace, not merge, list values.** Passing
  `-Dpipeline.classpaths=foo` overrides any classpaths already in the
  configuration, including ones the CLI just discovered.
- **`bin/flink` adds `lib/*` to the classpath, not the user jar.** The user
  jar is loaded by `PackagedProgram`'s child-first classloader.

## Related modules / docs

- `flink-core` (`core/execution/PipelineExecutor*`, `DeploymentOptions`,
  `ExecutionOptions`) — the SPI contracts this module implements.
- `flink-runtime` (`Dispatcher`, `RestClient`, `BlobClient`,
  `ApplicationDispatcherBootstrap`) — the JM side that the
  `RestClusterClient` / `EmbeddedExecutor` talk to.
- `flink-yarn`, `flink-kubernetes`, `flink-container` — backend
  implementations of `ClusterDescriptor` / `ClusterClientFactory`.
- `flink-streaming-java` (`StreamGraph`, `StreamExecutionEnvironment`) — the
  Pipeline that `StreamGraphTranslator` converts to a `JobGraph`.
- `flink-dist/bin/flink` — the shell script that launches `CliFrontend`.
- `codedocs/flink-memory-configuration.md` — JobManager / TaskManager memory
  model; relevant when configuring `-yjm`, `-ytm`, or
  `jobmanager.memory.process.size` from the CLI.
