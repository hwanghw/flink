# `flink-kubernetes`

## Purpose

`flink-kubernetes` is Flink's **native Kubernetes deployment backend**.
"Native" means the JobManager itself talks to the Kubernetes API server to
spawn TaskManager pods — there is no separate operator and no
externally-managed Deployment for TMs. The module provides:

- **Client side**: `KubernetesClusterDescriptor` (deploys session /
  application clusters), `KubernetesClusterClientFactory`,
  `KubernetesSessionClusterExecutorFactory`, `KubernetesSessionCli`
  (`bin/kubernetes-session.sh`).
- **JobManager / entrypoint side**: `KubernetesApplicationClusterEntrypoint`
  and `KubernetesSessionClusterEntrypoint`.
- **ResourceManager side**: `KubernetesResourceManagerDriver`
  (extends `AbstractResourceManagerDriver`), which uses the
  `FlinkKubeClient` to create/delete TaskManager pods.
- **High Availability**: ConfigMap-based leader election and metadata
  persistence (`KubernetesHaServicesFactory`,
  `KubernetesLeaderElectionDriver`, `KubernetesStateHandleStore`,
  `KubernetesCheckpointRecoveryFactory`) — see
  `codedocs/flink-kubernetes-ha-deep-dive.md` for the full design.
- **Pod construction via the Decorator pattern**: 12 step decorators
  (`InitJobManagerDecorator`, `CmdJobManagerDecorator`,
  `FlinkConfMountDecorator`, `HadoopConfMountDecorator`,
  `KerberosMountDecorator`, `PodTemplateMountDecorator`, etc.) compose a
  base pod from a user-supplied `kubernetes.pod-template-file` and Flink
  config into the final `Pod` spec.
- **Wrapper around the Fabric8 Kubernetes client** (`Fabric8FlinkKubeClient`)
  — Flink's API never references `io.fabric8.*` types outside this module,
  so the rest of Flink stays portable.

The `kubernetes-client.version` (Fabric8) is locked in the parent POM.
`kubernetes-server-mock` is used in tests.

## Where it fits

```
        flink run -t kubernetes-application <jar>             bin/kubernetes-session.sh
                              │                                            │
                              ▼                                            ▼
                  KubernetesClusterDescriptor.deployApplicationCluster   .deploySessionCluster
                              │
              (assemble JM pod via decorators; create Deployment/ConfigMap/Service)
                              │
                              ▼
                   Kubernetes API Server
                              │
                              ▼
                JM pod runs Kubernetes(Application|Session)ClusterEntrypoint
                              │
                              ▼
                  Dispatcher + KubernetesResourceManagerDriver
                              │
                              ▼
              FlinkKubeClient.createTaskManagerPod(pod)  ──▶  TM pods
                              │
                              ▼
              KubernetesTaskExecutorRunner.main()  (in TM pod)
```

For HA: a leader-election ConfigMap (`<clusterId>-<componentId>-leader`) plus
metadata-bearing ConfigMaps (`<clusterId>-<jobId>-jobmanager-leader`,
`<clusterId>-dispatcher-leader`, etc.). See the HA deep-dive.

## Maven coordinates

- Group / Artifact / Version: `org.apache.flink:flink-kubernetes:2.3-SNAPSHOT`
- Packaging: `jar`
- Shades-in: `io.fabric8.kubernetes-client` (and its `okhttp`-based
  HTTP client), `dk.brics.automaton` (referenced via Fabric8 model).

## Dependencies

### Direct

- `flink-runtime` — `AbstractResourceManagerDriver`, `ClusterEntrypoint`,
  HA primitives (`LeaderElectionDriver`, `LeaderRetrievalDriver`,
  `StateHandleStore`, `CheckpointRecoveryFactory`,
  `ExecutionPlanStore`).
- `flink-clients` — `ClusterDescriptor`, `ClusterClient`,
  `ApplicationClusterEntryPoint`, `RestClusterClient`.
- `flink-shaded-jackson` (provided) — pod-template / YAML parsing.
- `io.fabric8:kubernetes-client` + `kubernetes-httpclient-okhttp` — Fabric8
  K8s client.
- (test) `kubernetes-server-mock`, `flink-runtime` test-jar,
  `flink-test-utils`.

### Used by

- `flink-dist` — `opt/flink-kubernetes-*.jar`. Users move it to `lib/` to
  enable K8s deployment.
- `flink-end-to-end-tests` — E2E suites targeting real / minikube clusters.

## Source layout

```
org.apache.flink.kubernetes
├── (root)
│   ├── KubernetesClusterDescriptor.java         Client-side deployer (session + application)
│   ├── KubernetesClusterClientFactory.java      SPI: ClusterClientFactory
│   ├── KubernetesResourceManagerDriver.java     JM-side: create/delete TM pods, watch them
│   ├── KubernetesResourceManagerFactory.java
│   ├── KubernetesTaskExecutorRunner.java        TM main() inside a TM pod
│   ├── KubernetesWorkerNode.java                One pod handle, attemptId+podIndex
│   └── KubernetesWorkerResourceSpecFactory.java
├── artifact/                                    Optional jar uploader (e.g. PVC/HTTP/S3)
│   ├── KubernetesArtifactUploader.java          SPI
│   └── DefaultKubernetesArtifactUploader.java
├── cli/
│   └── KubernetesSessionCli.java                bin/kubernetes-session.sh entry; manages live session
├── configuration/
│   ├── KubernetesConfigOptions.java             ~60 kubernetes.* ConfigOptions
│   ├── KubernetesConfigOptionsInternal.java
│   ├── KubernetesDeploymentTarget.java          enum {SESSION, APPLICATION}
│   ├── KubernetesHighAvailabilityOptions.java   kubernetes.ha.* options
│   ├── KubernetesLeaderElectionConfiguration.java
│   └── KubernetesResourceManagerDriverConfiguration.java
├── entrypoint/
│   ├── KubernetesApplicationClusterEntrypoint.java   App-mode JM main()
│   ├── KubernetesSessionClusterEntrypoint.java       Session-mode JM main()
│   └── KubernetesEntrypointUtils.java
├── executors/
│   ├── KubernetesSessionClusterExecutor.java
│   └── KubernetesSessionClusterExecutorFactory.java  SPI: PipelineExecutorFactory
├── highavailability/                            ConfigMap-based HA
│   ├── KubernetesHaServicesFactory.java         SPI: HighAvailabilityServicesFactory
│   ├── KubernetesLeaderElectionHaServices.java
│   ├── KubernetesLeaderElectionDriver.java + Factory
│   ├── KubernetesLeaderRetrievalDriver.java + Factory
│   ├── KubernetesStateHandleStore.java          ConfigMap-backed StateHandleStore
│   ├── KubernetesCheckpointRecoveryFactory.java
│   ├── KubernetesCheckpointIDCounter.java       Atomic counter in ConfigMap
│   ├── KubernetesCheckpointStoreUtil.java
│   └── KubernetesExecutionPlanStoreUtil.java
├── kubeclient/                                  K8s client wrapper
│   ├── FlinkKubeClient.java                     Façade interface
│   ├── Fabric8FlinkKubeClient.java              Implementation
│   ├── FlinkKubeClientFactory.java              Build with config + thread pool
│   ├── FlinkPod.java                            Mutable pod+containerSpec accumulator
│   ├── KubernetesJobManagerSpecification.java   Deployment + ConfigMaps + Services bundle
│   ├── Endpoint.java                            Resolved JM REST endpoint
│   ├── KubernetesConfigMapSharedWatcher.java    Multi-tenant CM watch (used by HA)
│   ├── KubernetesSharedWatcher.java
│   ├── decorators/                              12 step decorators — see "Decorator pipeline"
│   │   ├── InitJobManagerDecorator.java         Mirror of InitTaskManagerDecorator
│   │   ├── InitTaskManagerDecorator.java        Basic resources, labels, env, image
│   │   ├── CmdJobManagerDecorator.java          Container command + args
│   │   ├── CmdTaskManagerDecorator.java
│   │   ├── ExternalServiceDecorator.java        ClusterIP/NodePort/LoadBalancer rest endpoint
│   │   ├── InternalServiceDecorator.java        Headless Service for JM RPC (TMs find JM)
│   │   ├── FlinkConfMountDecorator.java         ConfigMap with flink-conf.yaml + log4j
│   │   ├── HadoopConfMountDecorator.java        Hadoop config (HDFS/Kerberos)
│   │   ├── KerberosMountDecorator.java          Krb5 conf + keytab Secret
│   │   ├── EnvSecretsDecorator.java             envFrom: SecretRef
│   │   ├── MountSecretsDecorator.java           volume Secret mount
│   │   └── PodTemplateMountDecorator.java       User-provided pod-template injection
│   ├── factory/
│   │   ├── KubernetesJobManagerFactory.java     Runs all JM decorators; emits Spec
│   │   └── KubernetesTaskManagerFactory.java    Runs all TM decorators; emits Pod
│   ├── parameters/                              Per-component parameter holders
│   │   ├── KubernetesParameters.java            Interface
│   │   ├── AbstractKubernetesParameters.java
│   │   ├── KubernetesJobManagerParameters.java
│   │   └── KubernetesTaskManagerParameters.java
│   ├── resources/                               Wrappers over Fabric8 types
│   │   ├── KubernetesPod.java
│   │   ├── KubernetesConfigMap.java
│   │   ├── KubernetesService.java
│   │   ├── KubernetesLeaderElector.java         Wraps Fabric8 LeaderElector
│   │   ├── KubernetesPodsWatcher.java
│   │   ├── KubernetesSharedInformer.java
│   │   ├── KubernetesConfigMapSharedInformer.java
│   │   ├── KubernetesOwnerReference.java        OwnerRefs for cascading deletion
│   │   ├── KubernetesToleration.java
│   │   └── KubernetesSecretEnvVar.java
│   └── services/                                Service type strategies
│       ├── ServiceType.java                     enum
│       ├── ClusterIPService.java
│       ├── HeadlessClusterIPService.java
│       ├── NodePortService.java
│       └── LoadBalancerService.java
├── taskmanager/                                 (currently empty package, slot for future)
└── utils/
    ├── Constants.java                           Label keys, env-var names, mount paths
    └── KubernetesUtils.java                     Pod-template loader, OwnerReference helpers
```

84 production `.java` files; 70 test files.

## Architecture & key concepts

### Two deployment targets

`KubernetesDeploymentTarget`:
- `kubernetes-session` — long-lived session cluster. Created via
  `bin/kubernetes-session.sh` or `flink run -t kubernetes-session
  -Dkubernetes.cluster-id=<id>`.
- `kubernetes-application` — per-application cluster (recommended for
  production). User jar baked into the JM container image, or downloaded via
  `KubernetesArtifactUploader`.

There is **no per-job mode** for Kubernetes — it never existed.

### Pod assembly via the Decorator pipeline

Pods are built by composing a base `FlinkPod` through a sequence of
`KubernetesStepDecorator`s. Each decorator transforms a `FlinkPod` (which
wraps a `Pod` + main `Container`).

JM decorator order (`KubernetesJobManagerFactory`):

1. `InitJobManagerDecorator` — base labels, image, resource requests/limits.
2. `EnvSecretsDecorator` — `envFrom` references to Secrets.
3. `MountSecretsDecorator` — Secret volume mounts.
4. `CmdJobManagerDecorator` — container command + args (`kubernetes-entry.sh
   jobmanager` or `kubernetes-jobmanager.sh kubernetes-application`).
5. `InternalServiceDecorator` — headless Service for JM RPC (cluster-internal
   DNS so TMs find the JM).
6. `ExternalServiceDecorator` — Service for REST endpoint
   (ClusterIP / NodePort / LoadBalancer / Headless per
   `kubernetes.rest-service.exposed.type`).
7. `HadoopConfMountDecorator` — ConfigMap with `core-site.xml` /
   `hdfs-site.xml` if `HADOOP_CONF_DIR` is set on the client.
8. `KerberosMountDecorator` — `krb5.conf` + keytab Secret if Kerberos
   enabled.
9. `FlinkConfMountDecorator` — ConfigMap with the flattened Flink config +
   `log4j-console.properties`.
10. `PodTemplateMountDecorator` — overlays the user-provided
    `kubernetes.pod-template-file` (and per-component overrides).

The TM pipeline is parallel: `InitTaskManagerDecorator`,
`EnvSecretsDecorator`, `MountSecretsDecorator`, `CmdTaskManagerDecorator`,
`HadoopConfMountDecorator`, `KerberosMountDecorator`,
`FlinkConfMountDecorator`, `PodTemplateMountDecorator`.

### `FlinkKubeClient` facade

The only entry point for the rest of Flink into Fabric8. Notable methods:

- `createJobManagerComponent(KubernetesJobManagerSpecification)` — creates
  the JM Deployment + service(s) + flink-conf ConfigMap + hadoop ConfigMap
  + secrets.
- `createTaskManagerPod(KubernetesPod)` — returns a `CompletableFuture<Void>`
  because pod creation is async (it runs on the kube-client thread pool, not
  the RM's main thread).
- `stopPod(podName)` — delete the pod.
- `stopAndCleanupCluster(clusterId)` — `kubectl delete` everything with the
  `app=<clusterId>` label.
- `getService(name)`, `getRestEndpoint(clusterId)` — resolve REST endpoint
  for the client.
- `watchPodsAndDoCallback(labels, callbackHandler)` — registers a `Watch`
  (long-lived HTTP stream) for pod events.
- `createConfigMap`, `getConfigMap`, `checkAndUpdateConfigMap` — used by HA
  for ConfigMap-based leader election and state storage.

### ResourceManager (`KubernetesResourceManagerDriver`)

Lives in the JM. On `initializeInternal`:

1. Open a pod watch (label-selector `app=<clusterId>, component=taskmanager,
   type=flink-native-kubernetes`).
2. Load the TM pod template from
   `KubernetesUtils.getTaskManagerPodTemplateFileInPod()`
   (mounted via `PodTemplateMountDecorator`).
3. `recoverWorkerNodesFromPreviousAttempts()` — list existing TM pods
   (perhaps survivors of a JM failover); recover `currentMaxAttemptId` and
   `currentMaxPodId`.

`requestResource(TaskExecutorProcessSpec)`:

1. Build `KubernetesTaskManagerParameters` with the spec, blocked-node list,
   labels, env vars (cluster-id, REST address for registration).
2. Run the TM decorator pipeline → final pod spec.
3. `flinkKubeClient.createTaskManagerPod(pod)`; track the request in
   `requestResourceFutures` keyed by pod name (format:
   `<clusterId>-taskmanager-<attemptId>-<podIndex>`).
4. When the pod's main container actually starts and the TM registers, the
   future completes with a `KubernetesWorkerNode`.

`PodCallbackHandler`:

- `onPodAdded` — first time we see the pod alive.
- `onPodModified` — react to ready/scheduled status changes.
- `onPodDeleted` / `onPodTerminated` / `onError` — release the worker,
  notify framework which may re-request.

Pod naming includes the **attempt id**. When the JM dies and a new JM takes
over (HA), it bumps `currentMaxAttemptId` so its newly-requested pods don't
collide with possibly-still-alive zombies from the previous attempt.

### HA via ConfigMaps (FLIP-144)

The big idea: replace ZooKeeper. Each leader-electable component has a
ConfigMap whose annotations encode the current leader and lease expiry.
`KubernetesLeaderElector` (wrapper around Fabric8's `LeaderElector`) writes
to that ConfigMap with optimistic concurrency. Standby JMs watch the
ConfigMap; when the current leader's lease lapses, they race to update it.

Metadata (JobGraphs, completed checkpoint pointers, blob ref counts) is
stored in `KubernetesStateHandleStore`: each handle is one ConfigMap entry
(key = state-id, value = base64 of the serialized handle). Updates use
optimistic concurrency via `resourceVersion`; conflicts raise
`PossibleInconsistentStateException`.

See **`codedocs/flink-kubernetes-ha-deep-dive.md`** for the full picture —
ConfigMap layout, lease lifecycle, failover sequence, gotchas.

### Pod template support (FLIP-169)

Users can ship a `pod-template.yaml` with custom volumes, sidecars, init
containers, nodeAffinity, tolerations, etc. `PodTemplateMountDecorator`
merges the template *underneath* Flink's required fields: anything Flink
needs (the `flink-main-container`, mounts of the flink-conf ConfigMap, env
vars) is overlaid on top.

`KubernetesUtils.loadPodFromTemplateFile(client, file, mainContainerName)`
parses the template and locates the container named `flink-main-container`
(constant `Constants.MAIN_CONTAINER_NAME`). Side cars are preserved.

### Service exposure

`ServiceType` enum has implementations: `ClusterIPService`,
`HeadlessClusterIPService`, `NodePortService`, `LoadBalancerService`. Picked
via `kubernetes.rest-service.exposed.type`. The internal Service used by
TaskManagers to reach the JM is always headless ClusterIP — for stable DNS
names independent of pod restarts.

## Important public APIs

| Class | Why it matters |
|---|---|
| `KubernetesClusterDescriptor` | Deploys session/application clusters. |
| `KubernetesClusterClientFactory` | SPI for `flink-clients`. |
| `KubernetesResourceManagerDriver` | JM ↔ K8s API for TM pods. |
| `KubernetesApplicationClusterEntrypoint` / `KubernetesSessionClusterEntrypoint` | JM JVM entry. |
| `FlinkKubeClient` | The Fabric8 wrapper used everywhere. |
| `KubernetesConfigOptions` / `KubernetesHighAvailabilityOptions` | All `kubernetes.*` keys. |
| `KubernetesDeploymentTarget` | `kubernetes-session` / `kubernetes-application`. |
| `KubernetesStepDecorator` (+ 12 impls) | Pod composition. |
| `KubernetesHaServicesFactory` | ConfigMap-based HA. |
| `KubernetesPod` / `KubernetesConfigMap` / `KubernetesService` | Wrappers around Fabric8 objects. |

## Internal flows

### `flink run-application -t kubernetes-application <jar>`
1. `KubernetesClusterClientFactory.createClusterDescriptor(conf)` →
   `KubernetesClusterDescriptor` holding a `FlinkKubeClient`.
2. `deployApplicationCluster(spec, appConfig)`:
   - Build `KubernetesJobManagerParameters` from config.
   - Run JM decorator pipeline → `KubernetesJobManagerSpecification`
     (Deployment + Services + ConfigMaps).
   - Optional: `KubernetesArtifactUploader` uploads user jar(s) to a
     URI that the JM can pull from (HDFS/S3/HTTP) — useful when the
     image doesn't bundle the user code.
   - `flinkKubeClient.createJobManagerComponent(spec)` → POSTs everything.
3. Poll `flinkKubeClient.getRestEndpoint(clusterId)` until the Service has
   an address; build a `RestClusterClient` bound to that endpoint.
4. JM pod starts. `KubernetesApplicationClusterEntrypoint.main(...)` →
   `ApplicationClusterEntryPoint.startCluster(...)` →
   `Dispatcher` + `KubernetesResourceManagerDriver`.
5. `ApplicationDispatcherBootstrap` runs user `main()`; `env.execute()` →
   `EmbeddedExecutor` submits `JobGraph` to local Dispatcher.
6. RM requests TMs → driver creates pods → TMs register.

### JM failover (HA)
1. JM dies; its lease in the leader ConfigMap expires.
2. Standby JM (a second replica of the JM Deployment) wins re-election.
3. New leader reads job state from `KubernetesStateHandleStore`-backed
   ConfigMaps; restores `JobGraph`s and last completed checkpoint pointers.
4. `KubernetesResourceManagerDriver.recoverWorkerNodesFromPreviousAttempts()`
   inventories surviving TM pods and re-registers them.
5. Jobs resume from the last checkpoint. See HA deep-dive.

## Tests

- 70 test files under `src/test/java`. Heavily use
  `io.fabric8:kubernetes-server-mock` — an in-memory Kubernetes API server.
- Driver tests: `KubernetesResourceManagerDriverTest`,
  `KubernetesLeaderElectionDriverTest`,
  `KubernetesLeaderRetrievalDriverTest`,
  `KubernetesStateHandleStoreTest`.
- Decorator tests: one per decorator + base
  (`InitJobManagerDecoratorTest`, `CmdJobManagerDecoratorTest`,
  `FlinkConfMountDecoratorTest`, ...).
- Factory tests: `KubernetesJobManagerFactoryTest`,
  `KubernetesTaskManagerFactoryTest` exercise full decorator pipelines.
- `KubernetesClusterDescriptorTest` — full client-side deploy flow against
  the mock server.

E2E tests against real / minikube clusters live in `flink-end-to-end-tests`.

## Pitfalls & gotchas

- **`KUBECONFIG` / in-cluster config detection.** The Fabric8 client picks
  one automatically; if running `flink run -t kubernetes-application` from
  *inside* a pod that has a service account, it uses the
  `/var/run/secrets/kubernetes.io/serviceaccount/` token. Outside, it uses
  `~/.kube/config`. Set `kubernetes.kubeconfig.path` to override.
- **RBAC: the JM service account needs ConfigMap and Pod permissions.** Out
  of the box this means
  `get/list/watch/create/update/delete/patch` on `configmaps` and `pods`
  in its namespace. Without this, HA silently fails and pod creation 403s.
- **Pod-template precedence.** Fields Flink sets (image, command,
  env-from-flink) override the template. Mount paths Flink needs
  (`/opt/flink/conf`) are pre-claimed; reusing them in the template
  collides. Read `PodTemplateMountDecorator`.
- **Pod name length.** Format `<clusterId>-taskmanager-<attempt>-<podIdx>`
  must fit in 253 chars; long cluster IDs eat into the budget.
- **`checkAndUpdateConfigMap` lossy under K8s quirks.** If the API server
  returns a stale `resourceVersion` (rare but happens), the optimistic
  update sees the wrong state. The driver wraps these in
  `PossibleInconsistentStateException`; HA must handle them as
  potentially-applied. See `KubernetesStateHandleStore.replace` and the HA
  deep-dive's "PossibleInconsistentStateException" section.
- **Watch reconnects.** `KubernetesPodsWatcher` handles `Gone` (HTTP 410:
  resource version too old) by re-listing and re-watching. If a flood of
  pods churns and the watch falls behind, you can briefly see double
  events; the driver dedupes by pod name + UID.
- **`kubectl delete cluster` doesn't clean ConfigMaps for HA.** They're
  owned by the JM Deployment via `OwnerReference`s, so K8s GC takes care
  of it once the Deployment is gone — but only the JM ones. Standalone
  HA ConfigMaps survive if you didn't set ownership. Use
  `kubernetes.hadoop.conf.config-map.name` lifecycle carefully.
- **Image must match the Flink version in `lib/`.** A 1.18 JM image
  spawning 1.17 TM pods will fail RPC. `kubernetes.container.image` is
  expected to bundle the same `flink-dist` jar.
- **Service of type LoadBalancer costs money / is slow.** ClusterIP +
  port-forward or NodePort is fine for dev. The descriptor polls for the
  external IP for several minutes; that's normal.
- **The Fabric8 `kubernetes-httpclient-vertx` artifact is excluded** in
  favor of the `okhttp` one — Vert.x has a different thread model that
  conflicts with Flink's main-thread executor pattern.

## Related modules / docs

- `flink-clients` — `ClusterDescriptor`, `ClusterClient` SPIs implemented
  here.
- `flink-runtime` — `AbstractResourceManagerDriver`, `Dispatcher`,
  `ApplicationDispatcherBootstrap`, HA primitives.
- `flink-yarn` — analogous module for YARN.
- `flink-container` — the *non*-native standalone container entrypoint;
  used when running Flink on K8s *without* native integration (e.g.
  managed by an external operator).
- **`codedocs/flink-kubernetes-ha-deep-dive.md`** — the design and
  implementation of ConfigMap-based HA in this module. Read this if you
  touch anything under `highavailability/` or `KubernetesLeaderElector`.
- **`codedocs/flink-autoscaler-deep-dive.md`** — the
  flink-kubernetes-operator autoscaler (separate repo). The operator scales
  jobs running on this module's clusters by patching `parallelism.overrides`
  via Flink's REST API.
- `codedocs/flink-memory-configuration.md` — JM/TM memory model, mapped to
  K8s `requests`/`limits` in `InitJobManagerDecorator` /
  `InitTaskManagerDecorator`.
