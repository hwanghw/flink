# `flink-yarn-tests`

## Purpose

`flink-yarn-tests` runs **end-to-end YARN integration tests** for the `flink-yarn`
module. It boots an embedded Hadoop **`MiniYARNCluster`** (and a companion
`MiniDFSCluster`) inside the test JVM, then submits real Flink session /
application clusters against it using the actual `flink-dist` uber-jar.

Unlike the unit tests in `flink-yarn/src/test`, these tests exercise the full
client-side → AM → TaskManager → job-submit → recovery path with real Hadoop
YARN code. They are **slow** (typically several minutes each), so they live in
a separate module and are gated by Maven profiles in CI.

There is **no `src/main`** — this is a test-only module. All `.java` files
are under `src/test/java`.

## Where it fits

```
              flink-yarn (production: YarnClusterDescriptor, YarnResourceManagerDriver, ...)
                                      ▲
                                      │  tested by
                                      │
              flink-yarn-tests   ─────┘
              │
              ├── boots MiniYARNCluster (2 NMs) + MiniDFSCluster
              ├── consumes flink-dist's uber-jar as test scope
              └── submits jobs via the real bin/flink CLI behaviour
```

The module sits *downstream* of `flink-dist` in the build order (note the
comment in `YarnTestBase`: *"This class is located in a different package which
is built after flink-dist. This way, we can use the YARN uberjar of flink to
start a Flink YARN session."*).

## Maven coordinates

- Group / Artifact / Version: `org.apache.flink:flink-yarn-tests:2.3-SNAPSHOT`
- Packaging: `jar`
- No production classes — `src/main` doesn't exist.

## Dependencies

### Direct (all `<scope>test</scope>`)

- `flink-yarn` — the module under test.
- `flink-runtime`, `flink-streaming-java`, `flink-test-utils`,
  `flink-test-utils-junit` — base test infrastructure.
- `flink-dist_${scala.binary.version}` — the **uberjar** that gets shipped
  to the simulated YARN cluster. The test wires this up via
  `bin/flink`-equivalent paths.
- `flink-examples-streaming` — provides `WordCount` etc. as a known-good
  test workload.
- `org.apache.hadoop:hadoop-minicluster`, `hadoop-yarn-client`,
  `hadoop-yarn-api`, `hadoop-common`, `hadoop-minikdc` — Hadoop test
  infrastructure for the mini-YARN + mini-HDFS + mini-KDC clusters used in
  secured tests.
- `curator-test` — used for ZooKeeper-based HA tests
  (`YARNHighAvailabilityITCase`).

### Used by

- Nothing — it's a leaf module. CI invokes its tests directly.

## Source layout

```
org.apache.flink.yarn
├── YarnTestBase.java                   Abstract base: bootstrap mini-YARN cluster, run uberjar
├── YarnTestBaseTest.java               Smoke test for the test base itself
│
├── YARNApplicationITCase.java          Application Mode E2E tests
├── YARNSessionFIFOITCase.java          Session Mode w/ FIFO scheduler
├── YARNSessionCapacitySchedulerITCase.java    Session Mode w/ Capacity scheduler
├── YARNSessionFIFOSecuredITCase.java   Secured (mini-KDC + Kerberos) session
├── YARNHighAvailabilityITCase.java     ZooKeeper-based HA failover tests
├── YarnConfigurationITCase.java        End-to-end yarn.* config option coverage
├── YarnPrioritySchedulingITCase.java   Verifies priority assignment per spec
├── SqlYARNApplicationITCase.java       SQL Gateway → YARN Application Mode
├── UtilsTest.java                      Tests in this module that need the uberjar
│
├── testjob/                            Tiny user programs submitted by the tests
│   ├── YarnTestJob.java                Configurable test job (streaming, with checkpoints)
│   ├── YarnTestCacheJob.java           Uses DistributedCache
│   └── YarnTestArchiveJob.java         Verifies ZIP/JAR shipping
├── NoDataSource.java                   Source that emits nothing (for "test deploys cluster" tests)
└── util/
    ├── TestUtils.java                  Locate uberjar, build configurations
    ├── TestHadoopModuleFactory.java    Custom Hadoop Module SPI (security)
    └── TestHadoopSecurityContextFactory.java   SecurityContext for mini-KDC tests
```

18 test classes; ~3500 LOC. Tests run on JUnit 5.

## Architecture & key concepts

### MiniYARNCluster bootstrap

`YarnTestBase.startYARNWithConfig(yarnConfig, withDFS)`:

1. Creates a temp directory hierarchy for HDFS, YARN local-dirs, log-dirs,
   `flink-conf.yaml`.
2. Optionally starts a `MiniDFSCluster` (for staged jars).
3. Starts a `MiniYARNCluster` with `NUM_NODEMANAGERS = 2`.
4. Discovers and copies the `flink-dist-*.jar` uberjar into the test workspace
   (it must already be built — `mvn install` on `flink-dist`).
5. Writes a `flink-conf.yaml` that points at the mini-cluster's HDFS, plus
   any test-specific options.
6. Stores environment variables that the production `bin/flink` would set
   (`HADOOP_CONF_DIR`, `FLINK_CONF_DIR`, `FLINK_LIB_DIR`, etc.).

After each test:

- The cluster is **re-used** across tests in the same class (`@BeforeAll` /
  `@AfterAll`). Tests are **not** thread-safe — parallel execution is
  disabled.
- Each test calls `runTest(action)` which wraps the action in
  exception-and-log-aggregation analysis: scans NM logs for `Exception` /
  banned patterns and fails the test if any unexpected ones appear.

### Log-output assertions

`YarnTestBase` defines `PROHIBITED_STRINGS` (e.g. `"Exception"`,
`"Started SelectChannelConnector@0.0.0.0:8081"`) and a large
`WHITELISTED_STRINGS` regex list of *known-benign* patterns
(connection-refused during shutdown, Pekko transport noise, expected
`Stopping JobMaster` messages, the deliberate
`YarnResourceManagerDriver.ERROR_MESSAGE_ON_SHUTDOWN_REQUEST`, etc.). After
each test, all NM container logs are scanned: any prohibited substring not
covered by a whitelist regex fails the test. This catches accidentally-thrown
exceptions inside the AM/TM that would otherwise go unnoticed.

### `runTest(...)` boilerplate

Each test method body looks like:

```java
@Test
void testSomething() throws Exception {
    runTest(() -> {
        // ... interact with the cluster ...
        deployApplication(config);
        // assertions
    });
}
```

`runTest` adds:
- Stdout/stderr capture so the test can `assertThat(out).contains("…")`.
- Pre/post log-scanning.
- A timeout safety net.

### Secured tests

`YARNSessionFIFOSecuredITCase` brings up a `MiniKdc` and configures Hadoop
for Kerberos. The custom `TestHadoopSecurityContextFactory` (loaded via the
Flink `SecurityModuleFactory` SPI) installs the correct subject before
running. This is the only place delegation tokens, keytabs and principal
configuration are end-to-end-tested in CI.

### HA tests

`YARNHighAvailabilityITCase` uses `curator-test` to start an embedded
ZooKeeper, then crashes the AM and verifies a new attempt picks up the
job from the last checkpoint. Tests `yarn.application-attempts`,
`high-availability: zookeeper`, and the
`getContainersFromPreviousAttempts` recovery path in
`YarnResourceManagerDriver`.

## Important public APIs

This module has no public APIs — it's test infrastructure. The notable
abstract bases are:

| Class | Why it matters |
|---|---|
| `YarnTestBase` | Every IT case extends this. Boots mini-YARN, scans logs, owns `tmp` dirs. |
| `TestUtils` | Locate the uberjar / lib dir; build `Configuration`s. |
| `YarnTestJob` / `YarnTestCacheJob` / `YarnTestArchiveJob` | Reusable submittable workloads. |

## Internal flows

### Typical `YARNApplicationITCase.testApplicationCluster…()`
1. `@BeforeAll` → `YARN_CONFIGURATION` set, `startYARNWithConfig(...)`.
2. Test method → `runTest(() -> deployApplication(config))`.
3. `deployApplication(config)`:
   - Sets `DeploymentOptions.TARGET = "yarn-application"`,
     `pipeline.jars`, `application.main-class`, memory.
   - `YarnClusterClientFactory.createClusterDescriptor(config)` → real
     `YarnClusterDescriptor`.
   - `descriptor.deployApplicationCluster(spec, appConfig)` — talks to the
     **mini** YARN RM exactly as it would talk to a real one.
   - Polls `ClusterClient.requestJobStatus(...)` until the job finishes.
4. `runTest` post-step scans NM logs for any unexpected `Exception`.
5. `@AfterAll` → tear down YARN + DFS, delete temp dirs.

## Tests

This module is *itself* tests. Notable suites:

| Test | Verifies |
|---|---|
| `YARNApplicationITCase` | Application Mode happy paths + various `UserJarInclusion` modes + remote jars. |
| `YARNSessionFIFOITCase` | Session deploy + `bin/yarn-session.sh` interaction (FIFO scheduler). |
| `YARNSessionCapacitySchedulerITCase` | Session deploy with Capacity scheduler queues. |
| `YARNSessionFIFOSecuredITCase` | Kerberos / mini-KDC end-to-end. |
| `YARNHighAvailabilityITCase` | AM crash + recovery via ZooKeeper HA. |
| `YarnConfigurationITCase` | Tests `yarn.*` config options affect container launch. |
| `YarnPrioritySchedulingITCase` | Verifies per-TM-spec YARN `Priority` assignment. |
| `SqlYARNApplicationITCase` | SQL Gateway pushing SQL jobs into YARN Application Mode. |
| `YarnTestBaseTest` | Smoke-tests the test infrastructure itself. |
| `UtilsTest` | Standalone unit tests that require the full uberjar (so they live here, not in flink-yarn). |

## Pitfalls & gotchas

- **`flink-dist` must be installed first.** This module depends on the
  uberjar produced by `flink-dist`. `mvn test -pl flink-yarn-tests` from a
  fresh checkout fails; you need `mvn install -pl flink-dist -am` first.
- **Tests are not parallelizable.** `YarnTestBase` uses static fields for
  the mini-cluster and tmp dirs. The `surefire` config in `pom.xml` keeps
  fork count at 1.
- **Log scanning is greedy.** Adding a new log message containing
  `"Exception"` from production code can break many tests — either pick a
  different word or add a whitelist regex.
- **MiniYARNCluster is finicky on macOS/Windows.** YARN expects POSIX
  filesystem semantics; on case-insensitive filesystems some HDFS tests
  flake. CI runs Linux.
- **YARN's mini-cluster doesn't enforce resource isolation.** A TM that asks
  for 1 GiB and uses 10 GiB won't get killed — so over-allocation bugs are
  invisible here and only surface on real YARN with `cgroup`s.
- **`hadoop-minikdc` brings JDK security baggage.** Tests sometimes flake
  with `KrbException: Pre-authentication information was invalid` if the
  clock skews; the test base resets clocks at startup.
- **`curator-test` ZooKeeper port is dynamic.** When debugging, find the port
  via the test logs — don't try to attach to `2181`.
- **Mini-YARN test execution writes to `/tmp`.** Local-dirs + log-dirs can
  fill quickly when running the whole suite locally. Use `-Djava.io.tmpdir`
  pointing to a clean disk.
- **The whitelist of benign exceptions grows organically.** Every new
  whitelist entry should reference a Jira / PR explaining *why* it's benign;
  unbounded growth means real bugs hide there.

## Related modules / docs

- `flink-yarn` — the module under test. See its CLAUDE.md for production
  classes.
- `flink-test-utils` — `MiniClusterExtension`, `TestLogger`, base utilities
  used here.
- `flink-end-to-end-tests` — even higher-level E2E tests that drive real
  Flink distributions via shell scripts (no mini-cluster).
- Hadoop docs:
  [MiniYARNCluster](https://hadoop.apache.org/docs/r3.3.6/api/org/apache/hadoop/yarn/server/MiniYARNCluster.html)
  and [MiniDFSCluster](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/HdfsClusterSetup.html).
