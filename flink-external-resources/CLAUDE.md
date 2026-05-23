# `flink-external-resources`

## Purpose

`flink-external-resources` is the **parent of pluggable accelerator drivers**
for Flink — currently the GPU driver, but designed to host FPGAs and other
specialized hardware in the future. It does **not** define the SPI itself
(that lives in `flink-core`: `ExternalResourceDriver`,
`ExternalResourceDriverFactory`, `ExternalResourceInfo`) and does **not**
define the runtime that consumes drivers (that's in `flink-runtime`:
`ExternalResourceUtils`, `ExternalResourceInfoProvider`). It is purely a
**packaging umbrella** for officially-bundled driver implementations that
ship in `flink-dist/opt/external-resources/`.

This module is itself a Maven `<packaging>pom</packaging>` aggregator. It
has no `src/main`. Its single child module today is:

- **`flink-external-resource-gpu`** — NVIDIA GPU driver that discovers GPUs
  via a user-runnable shell script (default: `nvidia-smi`-based) and exposes
  them to user functions via `RuntimeContext.getExternalResourceInfos()`.

The whole external-resource framework lets users request, e.g.,
`external-resource.gpu.amount: 2` per TaskManager. Flink then asks the
container runtime (YARN / Kubernetes / standalone) to allocate the resource
*and* asks the driver to discover which specific device IDs are visible
inside the TM. User UDFs receive a `Set<ExternalResourceInfo>` they can use
to pin their work (e.g., `CUDA_VISIBLE_DEVICES`).

## Where it fits

```
flink-core (api/common/externalresource/):
   ExternalResourceDriver        ← interface (this module implements)
   ExternalResourceDriverFactory ← factory SPI
   ExternalResourceInfo          ← info object (one per discovered unit)

flink-runtime (externalresource/):
   ExternalResourceUtils         ← reads external-resource.* config, loads drivers as plugins
   ExternalResourceInfoProvider  ← TM-side accessor exposed via RuntimeContext

flink-yarn:
   YarnResourceManagerDriver     ← maps external-resource.<name>.yarn.config-key → YARN Resource
   ResourceInformationReflector

flink-kubernetes:
   KubernetesTaskManagerParameters ← maps external-resource.<name>.kubernetes.config-key → Pod resources

flink-external-resources/                      ← THIS MODULE
└── flink-external-resource-gpu/               ← THE GPU IMPLEMENTATION
    ├── GPUDriver        implements ExternalResourceDriver
    ├── GPUDriverFactory implements ExternalResourceDriverFactory
    ├── GPUInfo          implements ExternalResourceInfo
    └── nvidia-gpu-discovery.sh (default discovery script)

User code:
   getRuntimeContext().getExternalResourceInfos("gpu") → Set<GPUInfo>
   → use info.getProperty("index") to pin GPU work
```

## Maven coordinates

- Parent aggregator: `org.apache.flink:flink-external-resources:2.3-SNAPSHOT`
  — packaging: `pom`, no jar produced.
- Child: `org.apache.flink:flink-external-resource-gpu:2.3-SNAPSHOT`
  — packaging: `jar`.

The GPU jar is **plugin-loaded** at runtime: it's not on the main classpath.
The user copies it into `plugins/external-resource-gpu/` of their Flink
distribution; `core/plugin/PluginManager` then loads it through a child-first
classloader so the driver's dependencies stay isolated.

## Dependencies

### Direct

`flink-external-resources` (parent): no dependencies — aggregator only.

`flink-external-resource-gpu`:
- `flink-core` (provided) — `ExternalResourceDriver(Factory)`,
  `ExternalResourceInfo`, `Configuration`, `ConfigOption`.
- (test) `flink-test-utils-junit`.

### Used by

- `flink-dist` packages `flink-external-resource-gpu` plus its
  `nvidia-gpu-discovery.sh` + `gpu-discovery-common.sh` under
  `opt/external-resources/gpu/`. Users wire it in by moving it to
  `plugins/external-resource-gpu/`.
- The runtime (`ExternalResourceUtils` in `flink-runtime`) discovers driver
  factories by reading
  `META-INF/services/org.apache.flink.api.common.externalresource.ExternalResourceDriverFactory`
  inside each plugin jar.

## Source layout

```
flink-external-resources/                    POM aggregator only
└── pom.xml

flink-external-resources/flink-external-resource-gpu/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/org/apache/flink/externalresource/gpu/
    │   │   ├── GPUDriver.java                Discovers GPUs via shell script
    │   │   ├── GPUDriverFactory.java         SPI factory
    │   │   ├── GPUDriverOptions.java         ConfigOptions (discovery-script.path / args)
    │   │   └── GPUInfo.java                  ExternalResourceInfo with "index" property
    │   └── resources/
    │       ├── META-INF/services/
    │       │   └── org.apache.flink.api.common.externalresource.ExternalResourceDriverFactory
    │       │       (single line: org.apache.flink.externalresource.gpu.GPUDriverFactory)
    │       ├── nvidia-gpu-discovery.sh       Default discovery script (uses nvidia-smi)
    │       └── gpu-discovery-common.sh       Coordination-mode helper sourced by the above
    └── test/
        ├── java/.../GPUDriverTest.java       Unit tests with a stub script
        ├── java/.../GPUDiscoveryScriptTest.java
        └── resources/
            ├── testing-gpu-discovery.sh
            └── test-coordination-mode.sh
```

4 production `.java` files; 2 test files; 2 bundled shell scripts.

## Architecture & key concepts

### The driver SPI (`flink-core`)

`ExternalResourceDriver` (one method):

```java
Set<? extends ExternalResourceInfo> retrieveResourceInfo(long amount)
```

Called by the TaskManager when slot allocations bring resource needs;
returns one info per allocated unit. `ExternalResourceInfo` has
`getProperty(key)` and `getKeys()` — driver-specific. For GPUs, the only
property is `"index"`.

`ExternalResourceDriverFactory` (one method):

```java
ExternalResourceDriver createExternalResourceDriver(Configuration config)
```

Factories are discovered via `META-INF/services` from every plugin
classloader.

### Configuration namespace

All keys use the `external-resource.<resource_name>.…` pattern (from
`flink-core`'s `ExternalResourceOptions`):

| Key | Purpose |
|---|---|
| `external-resource.<name>.amount` | How many units per TM. |
| `external-resource.<name>.driver-factory.class` | FQCN of the factory (`GPUDriverFactory` for GPUs). |
| `external-resource.<name>.yarn.config-key` | YARN resource name (e.g. `yarn.io/gpu`). |
| `external-resource.<name>.kubernetes.config-key` | K8s resource name (e.g. `nvidia.com/gpu`). |
| `external-resource.<name>.param.discovery-script.path` | Driver-specific (GPU): script path. |
| `external-resource.<name>.param.discovery-script.args` | Driver-specific (GPU): script args. |

`<name>` is user-chosen (commonly `"gpu"`). The GPU driver's
`GPUDriverOptions` declares two suffix-options (`discovery-script.path` /
`discovery-script.args`) that live under
`external-resource.<name>.param.*`.

### `GPUDriver` discovery flow

1. Construction (`GPUDriver(config)`):
   - Reads `discovery-script.path` (default
     `${FLINK_PLUGINS_DIR}/external-resource-gpu/nvidia-gpu-discovery.sh`).
   - Resolves relative to `FLINK_HOME` if not absolute.
   - Validates the file exists and is executable.
   - Reads `discovery-script.args`.
2. `retrieveResourceInfo(amount)`:
   - Forks the script as `<scriptPath> <amount> <args>`.
   - 10-second timeout (`DISCOVERY_SCRIPT_TIMEOUT_MS`).
   - Captures stdout / stderr; non-zero exit → `FlinkException`.
   - Expects **one line of comma-separated indices** (e.g. `0,2,5`).
   - Returns `Set<GPUInfo>` with one `GPUInfo` per index.

If the script returns nothing (or `amount == 0`), the driver returns an
empty set — useful for "test deploys but don't really want GPUs" runs.

### Default `nvidia-gpu-discovery.sh`

The bundled bash script:

1. Parses `gpu-amount`, optional `--enable-coordination-mode`,
   `--coordination-file <path>`.
2. Runs `nvidia-smi --query-gpu=index --format=csv,noheader` to enumerate
   visible GPUs.
3. Sources `gpu-discovery-common.sh::gpu_discovery` which:
   - Without coordination mode: picks the first `amount` GPUs from the list
     `nvidia-smi` returned.
   - With coordination mode: takes a `flock` on the coordination file and
     records which indices this TM holds, preventing two TMs on the same
     host from getting overlapping device IDs even when `CUDA_VISIBLE_DEVICES`
     doesn't isolate them.

Coordination mode is critical on YARN/standalone where one host can run
multiple TMs sharing the same physical GPUs without container-level cgroup
isolation. On Kubernetes the `nvidia.com/gpu` device plugin already isolates
via `CUDA_VISIBLE_DEVICES`, so coordination mode is unnecessary.

### YARN / Kubernetes integration

External resources are resource-manager-aware:

- **YARN**: `YarnResourceManagerDriver` uses
  `external-resource.<name>.yarn.config-key` (e.g. `yarn.io/gpu`) to add the
  resource to the YARN `ContainerRequest`. `ResourceInformationReflector`
  reflects on the Hadoop `Resource` class to set the resource amount on
  older Hadoop versions that don't expose the setter.
- **Kubernetes**: `KubernetesTaskManagerParameters` sets pod resource
  requests/limits with `external-resource.<name>.kubernetes.config-key`
  (e.g. `nvidia.com/gpu`).
- **Standalone**: no scheduler-level allocation — discovery script is the
  only mechanism, hence coordination mode.

### User-facing API

Inside a UDF:

```java
public class MyMap extends RichMapFunction<…> {
    @Override
    public void open(OpenContext ctx) {
        Set<ExternalResourceInfo> gpus =
            getRuntimeContext().getExternalResourceInfos("gpu");
        for (ExternalResourceInfo info : gpus) {
            String index = info.getProperty("index").orElseThrow(...);
            // pin Cuda context to GPU `index`
        }
    }
}
```

## Important public APIs

| Class | Why it matters |
|---|---|
| `GPUDriver` | The driver implementation. Forks the discovery script. |
| `GPUDriverFactory` | SPI factory — entry point at the `META-INF/services` level. |
| `GPUInfo` | One device, one `index` property. |
| `GPUDriverOptions` | `discovery-script.path` + `discovery-script.args` config keys. |
| `nvidia-gpu-discovery.sh` | Default script. Users can substitute their own. |

(The interfaces — `ExternalResourceDriver`, `ExternalResourceDriverFactory`,
`ExternalResourceInfo` — live in `flink-core`, not here.)

## Internal flows

### TM startup with GPU discovery
1. TaskManager starts; `ExternalResourceUtils.createStaticExternalResourceInfoProvider(...)`
   inspects config, finds `external-resource.gpu.driver-factory.class:
   org.apache.flink.externalresource.gpu.GPUDriverFactory`.
2. `PluginManager` resolves the GPU plugin classloader; the factory is
   instantiated.
3. `GPUDriverFactory.createExternalResourceDriver(config)` builds a
   `GPUDriver`.
4. The driver's `retrieveResourceInfo(amount)` is invoked. Script runs:
   `nvidia-gpu-discovery.sh 2`. Output: `0,1`.
5. Result `Set.of(new GPUInfo("0"), new GPUInfo("1"))` is stored in the TM's
   `ExternalResourceInfoProvider`.
6. When the TM offers a slot, the `RuntimeContext` exposes the set to UDFs.

### Custom driver (FPGA example)
1. Implement `ExternalResourceDriver` + `ExternalResourceDriverFactory` in
   a new jar.
2. Declare the factory in
   `META-INF/services/org.apache.flink.api.common.externalresource.ExternalResourceDriverFactory`.
3. Drop the jar in `plugins/external-resource-fpga/`.
4. Configure `external-resource.fpga.driver-factory.class: ...`.

## Tests

- `GPUDriverTest.java` — Unit tests with `testing-gpu-discovery.sh` as a
  stub: verifies discovery-script invocation, timeout behaviour,
  non-zero-exit handling, multi-line output handling, executable-check
  errors.
- `GPUDiscoveryScriptTest.java` — Bash-level tests of the bundled scripts.
- `test-coordination-mode.sh` exercises the `flock`-based coordination
  branch.

## Pitfalls & gotchas

- **Plugin classloader is mandatory.** Don't put the GPU jar in `lib/` —
  it must go in `plugins/external-resource-gpu/`. In `lib/` the factory
  won't be discovered because `ExternalResourceUtils` only scans plugin
  classloaders.
- **Discovery script must be executable.** `chmod +x` is the most common
  setup error. The driver throws clearly but only at TM start.
- **The script's first stdout line is taken; extras are logged and
  discarded.** Avoid debug `echo` before the index list.
- **10-second timeout is hard-coded** (`DISCOVERY_SCRIPT_TIMEOUT_MS`).
  `nvidia-smi` is usually instant; on hosts with broken NVIDIA drivers it
  can hang minutes — your discovery times out and the TM fails to start.
- **Coordination mode requires a writable `/var/tmp` (or wherever
  `--coordination-file` points)** shared between TMs on the same host. In
  containerised setups, mount it explicitly.
- **External resources are TM-scoped, not slot-scoped.** A TM with 2 GPUs
  and 4 slots exposes both GPUs to every slot's UDFs — UDFs must pick which
  to use. Flink does not partition the set.
- **YARN/K8s resource names are not standardized.** `nvidia.com/gpu` is
  conventional on Kubernetes with the NVIDIA device plugin; YARN typically
  uses `yarn.io/gpu`. Misnaming = the scheduler doesn't allocate the
  resource and the TM ends up on a host without GPUs.
- **`GPUInfo.equals` is index-only.** Two GPUs with the same string index
  from different sources collide — make sure the discovery script returns
  unique indices.
- **`gpu-discovery-common.sh`'s coordination file is a long-lived bash
  state file.** Crashed TMs leak entries; pre-clean it on host reboot.
- **The driver is not currently compatible with non-NVIDIA accelerators.**
  AMD ROCm / Intel oneAPI users must write a custom driver. The framework
  is designed for that; only the bundled implementation is NVIDIA-specific.

## Related modules / docs

- `flink-core` (`api/common/externalresource/`) — interfaces
  `ExternalResourceDriver`, `ExternalResourceDriverFactory`,
  `ExternalResourceInfo`. New driver implementations must depend on this.
- `flink-runtime` (`runtime/externalresource/`) — `ExternalResourceUtils`,
  `ExternalResourceInfoProvider`. The consumer-side glue that loads
  drivers via `PluginManager` and exposes them through `RuntimeContext`.
- `flink-yarn` (`YarnResourceManagerDriver`,
  `ResourceInformationReflector`) — YARN side of the integration.
- `flink-kubernetes` (`KubernetesTaskManagerParameters`,
  `kubeclient/decorators/InitTaskManagerDecorator`) — Kubernetes side.
- Flink docs:
  [External Resources framework](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/advanced/external_resources/).
