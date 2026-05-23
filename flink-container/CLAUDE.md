# `flink-container`

## Purpose

`flink-container` is a **tiny but pivotal module**: it provides the
`StandaloneApplicationClusterEntryPoint` — the JobManager `main()` class that
runs inside a generic container (Docker, Podman, plain `java -cp …`,
standalone-Kubernetes-without-native-integration, Nomad, etc.) in
**Application Mode**.

Concretely it contains exactly three classes:

- `StandaloneApplicationClusterEntryPoint` — the `main()` invoked by
  `bin/standalone-job.sh` (or by the official `flink:*` Docker image when
  `MODE=application`).
- `StandaloneApplicationClusterConfiguration` — value object holding the
  parsed CLI args (`--job-classname`, `--job-id`, `--jars`, `-D...`,
  `--fromSavepoint`).
- `StandaloneApplicationClusterConfigurationParserFactory` — `commons-cli`
  options definition + parser → `StandaloneApplicationClusterConfiguration`.

It does **not** implement the application-mode runtime — that lives in
`flink-clients` (`ApplicationClusterEntryPoint`) and `flink-runtime`
(`ApplicationDispatcherBootstrap`). `flink-container` just wires the standalone
deployment target into that runtime.

Despite the name, it is **not** the same module as `flink-kubernetes`. The
native-Kubernetes entrypoint (`KubernetesApplicationClusterEntrypoint`) lives in
`flink-kubernetes`. `flink-container` is the *generic-container* /
*standalone* form: the cluster does not know it's on Kubernetes, and there's
no native ConfigMap-based HA, no native pod-spawning ResourceManager. It is
typically combined with the `StandaloneResourceManagerFactory` (reactive
TaskManagers register themselves) and a separate ZooKeeper/K8s HA service if
HA is needed.

## Where it fits

```
        official flink Docker image  (apache/flink:<ver>)
                       │
                       ▼
            docker-entrypoint.sh   (modes: jobmanager, taskmanager,
                       │            standalone-job, history-server)
                       ▼
              bin/standalone-job.sh
                       │
                       ▼
   org.apache.flink.container.entrypoint.StandaloneApplicationClusterEntryPoint
                       │
                       ▼
           ApplicationClusterEntryPoint    ← flink-clients
                       │
                       ▼
           ApplicationDispatcherBootstrap  ← flink-runtime
                       │
                       ▼
                  user main()
```

The companion JobManager entrypoint for session mode
(`StandaloneSessionClusterEntrypoint`) lives in `flink-runtime`; the
TaskManager runner (`TaskManagerRunner`) likewise. This module only adds
*application mode* for standalone containers.

## Maven coordinates

- Group / Artifact / Version: `org.apache.flink:flink-container:2.3-SNAPSHOT`
- Packaging: `jar`
- All Flink dependencies are `<scope>provided</scope>` because the resulting
  jar is dropped into `lib/` of an existing `flink-dist` install — every
  Flink classpath already has `flink-runtime`, `flink-clients`, etc.

## Dependencies

### Direct

- `flink-runtime` (provided) — `ClusterEntrypoint`, `ClusterEntrypointUtils`,
  `EntrypointClusterConfiguration`, `StandaloneResourceManagerFactory`,
  `SavepointRestoreSettings`.
- `flink-clients` (provided) — `ApplicationClusterEntryPoint`,
  `PackagedProgram`, `DefaultPackagedProgramRetriever`, `ArtifactFetchManager`,
  `CliFrontendParser` (for savepoint flags).
- (test) `flink-streaming-java`, `flink-test-utils-junit`.

### Used by

- `flink-dist` — the resulting jar is bundled into the Flink distribution's
  `lib/` so `bin/standalone-job.sh` can resolve the main class.
- `flink-docker` (external repo `apache/flink-docker`) — Docker images set the
  `CMD` to invoke this entrypoint when run with `standalone-job` mode.
- E2E tests under `flink-end-to-end-tests` that bring up a standalone
  application cluster in Docker Compose use this entrypoint via
  `bin/standalone-job.sh`.

## Source layout

```
org.apache.flink.container.entrypoint
├── StandaloneApplicationClusterEntryPoint.java          The JM main() (159 LOC)
├── StandaloneApplicationClusterConfiguration.java       Value object
└── StandaloneApplicationClusterConfigurationParserFactory.java   commons-cli definitions
```

Three production classes, ~350 LOC total. Tests:
`src/test/java/org/apache/flink/container/entrypoint/StandaloneApplicationClusterConfigurationParserFactoryTest.java`.

## Architecture & key concepts

### Standalone Application Mode lifecycle

1. The container starts. `docker-entrypoint.sh` (in the `apache/flink-docker`
   repo) reads `MODE=standalone-job` and execs `bin/standalone-job.sh
   start-foreground --job-classname com.acme.MyJob ...`.
2. `standalone-job.sh` invokes
   `org.apache.flink.container.entrypoint.StandaloneApplicationClusterEntryPoint`
   with the user args.
3. `main()` parses CLI options via
   `StandaloneApplicationClusterConfigurationParserFactory`, then loads
   `flink-conf.yaml` / `config.yaml` from `--configDir` and overlays
   `-D` dynamic properties.
4. If `--jars` were supplied (remote URIs), `ArtifactFetchManager` downloads
   them (HDFS / S3 / HTTPS / local) before the JobManager comes up.
5. `DefaultPackagedProgramRetriever.create(userLibDir, jobJar, artifacts,
   jobClassName, args, conf)` builds a `PackagedProgram` from
   `/opt/flink/usrlib/` (the convention) plus any fetched jars.
6. `ApplicationClusterEntryPoint.configureExecution(conf, program)` writes the
   inferred `pipeline.jars`, `pipeline.classpaths`, and entry-class into the
   configuration.
7. `ClusterEntrypoint.runClusterEntrypoint(entrypoint)` starts the
   `DispatcherResourceManagerComponent` which uses
   `StandaloneResourceManagerFactory` (TM registers itself; no native
   pod-spawning). The Dispatcher then runs the user `main()` via
   `ApplicationDispatcherBootstrap` exactly as in YARN/K8s application mode.

### CLI options (`StandaloneApplicationClusterConfigurationParserFactory`)

| Option | Meaning |
|---|---|
| `--configDir`, `-c` | Flink conf directory (usually `/opt/flink/conf`). |
| `-D<key>=<value>` | Dynamic property; overlays YAML config. |
| `--job-classname`, `-j` | FQCN of the user main class. Optional if the manifest has one. |
| `--job-id`, `-jid` | Pin the `JobID` (useful for HA restart determinism). |
| `--jars` | Comma-separated remote URIs to fetch as user jars before launching. |
| `--fromSavepoint`, `-s` | Savepoint path to restore from. |
| `--allowNonRestoredState`, `-n` | Tolerate operators present in the savepoint that no longer exist. |

### Reactive mode

`supportsReactiveMode()` returns `true` (overridden from
`ApplicationClusterEntryPoint`). This means the standalone application cluster
honors `jobmanager.scheduler: adaptive` + reactive mode: it scales the job to
the *current* number of registered TaskManagers, so you can add/remove TMs
externally (e.g. via Kubernetes HPA on a StatefulSet) and the running job
re-parallelizes.

### Static JobID

If `--job-id <hex>` is provided, it's written to
`PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID` *before* the user main() runs.
This is what makes HA recovery work: when a JM dies and another container
restarts the same `flink-conf.yaml`, the recovered JobID matches the
checkpoint/savepoint metadata previously persisted.

## Important public APIs

| Class | Why it matters |
|---|---|
| `StandaloneApplicationClusterEntryPoint` | `main()` invoked by the container — the actual JVM entry. |
| `StandaloneApplicationClusterConfiguration` | Parsed CLI form; not user-facing but stable for tests. |

## Internal flows

### `docker run apache/flink:2.x standalone-job --job-classname com.acme.MyJob`
1. `docker-entrypoint.sh` → `bin/standalone-job.sh start-foreground …`.
2. `StandaloneApplicationClusterEntryPoint.main(args)` →
   `parseParametersOrExit(args, parserFactory, MainClass)`.
3. Filesystem plugins loaded (so `--jars hdfs://…` works) via
   `PluginUtils.createPluginManagerFromRootFolder(conf)` +
   `FileSystem.initialize(conf, pluginManager)`.
4. `SecurityContext.runSecured(() -> getPackagedProgram(...))` → builds the
   `PackagedProgram`, fetching remote `--jars` into a local temp dir.
5. `configureExecution(configuration, program)` (in `flink-clients`) updates
   `pipeline.jars`, `pipeline.classpaths`, `application.main-class`,
   `application.args`.
6. `new StandaloneApplicationClusterEntryPoint(conf, program)` constructed
   with `StandaloneResourceManagerFactory.getInstance()`.
7. `ClusterEntrypoint.runClusterEntrypoint(entrypoint)` bootstraps the JM:
   `Dispatcher` + standalone `ResourceManager` + REST endpoint + (if
   configured) ZooKeeper / Kubernetes HA services.
8. `ApplicationDispatcherBootstrap` runs `program.invokeInteractiveModeForExecution()`
   on a separate thread. The user's `env.execute()` calls hit
   `EmbeddedExecutor` and submit JobGraphs to the local Dispatcher.

## Tests

Only one test class:

- `StandaloneApplicationClusterConfigurationParserFactoryTest.java` —
  exhaustively covers CLI argument parsing: `--configDir`, dynamic properties,
  savepoint flags, job class name / id, jars list, error cases (missing
  required options, malformed JobID, invalid `--fromSavepoint` combinations).

There are no integration tests in this module — the actual
end-to-end behavior is covered by `flink-end-to-end-tests` (Docker Compose
based tests that bring up a real `apache/flink` image).

## Pitfalls & gotchas

- **`/opt/flink/usrlib/` is the conventional user lib location.** The Docker
  image's `standalone-job.sh` adds it to the classpath via
  `ClusterEntrypointUtils.tryFindUserLibDirectory()`. If your user jar is
  elsewhere you must pass `--jars` or set `FLINK_USR_LIB`.
- **Don't confuse with the K8s native entrypoint.** This is
  `org.apache.flink.container.entrypoint.StandaloneApplicationClusterEntryPoint`,
  not `org.apache.flink.kubernetes.entrypoint.KubernetesApplicationClusterEntrypoint`.
  The standalone version has no awareness of pods/configmaps — TMs come and
  go via the standalone `ResourceManager`.
- **HA still requires a separate config.** `high-availability: kubernetes`
  (ConfigMap-based) or `zookeeper` works, but you must configure it
  yourself; the standalone application cluster doesn't enable HA by default.
- **`--job-id` must be a 32-char hex string** (a `JobID`). The parser will
  reject anything else; useful for predictable HA restart but easy to
  mess up.
- **`SecurityContext.runSecured(...)` wraps the program load,** so Kerberos
  / OS-user setup applies. The user main() also runs under it.
- **`LEGACY` recovery claim mode logs a deprecation warning** — `CLAIM` or
  `NO_CLAIM` are preferred. See `RecoveryClaimMode` in `flink-core-api`.
- **All dependencies are `provided` in the pom.** Building this module in
  isolation produces a tiny jar (<20 KB). If you accidentally shade in
  `flink-clients`, you'll get classloader conflicts at runtime.
- **`flink-dist`'s `lib/` is where this jar lives.** A custom Flink image
  that strips lib jars (e.g., a slimming script) must keep this one or
  application mode breaks.

## Related modules / docs

- `flink-clients` (`client/deployment/application/ApplicationClusterEntryPoint`)
  — the base class. All the heavy lifting (artifact fetch, PackagedProgram,
  EmbeddedExecutor) is there.
- `flink-runtime` (`runtime/entrypoint/ClusterEntrypoint`,
  `StandaloneResourceManagerFactory`,
  `ApplicationDispatcherBootstrap`) — JM bootstrap and standalone RM.
- `flink-kubernetes` — the **native** Kubernetes equivalent
  (`KubernetesApplicationClusterEntrypoint`) when you want the JM itself to
  spawn TaskManager pods.
- `flink-yarn` — the YARN equivalent (`YarnApplicationClusterEntryPoint`).
- `flink-dist/src/main/flink-bin/bin/standalone-job.sh` — the shell wrapper.
- Apache Flink Docker repo (`apache/flink-docker`) — `docker-entrypoint.sh`
  and `Dockerfile` that consume this module's main class.
