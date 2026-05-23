# `flink-test-utils-parent`

## Purpose

Maven aggregator for **all of Flink's shared test infrastructure**. Almost
every other Flink module depends on at least one artifact from here at
`test` scope. If you have ever written `extends TestLogger`, used
`@ExtendWith(MiniClusterExtension.class)`, or registered a
`MiniClusterWithClientResource`, you are consuming this module.

The aggregator itself ships no code; it bundles seven sub-modules with
clearly separated concerns (JUnit-level helpers, MiniCluster bring-up,
classloading test jars, connector framework, migration tooling, table
filesystem catalog).

## Where it fits — when developers would touch this module

- Adding a JUnit 5 extension, an AssertJ assertion, or a shared latch /
  threading primitive that multiple test modules need.
- Adding a new `MiniClusterExtension` parameter resolver (e.g. injecting a
  new kind of client into tests).
- Adding a connector test-framework class (`ExternalContext`,
  `FlinkContainers`, `SourceTestSuiteBase`) used by external connector
  projects (Kafka, Pulsar, Elasticsearch repos pull from these).
- Adding helpers used by classloading / packaging tests — small user jars
  end up here so they can be reused.
- Migrating the table-filesystem catalog test factory.

## Maven coordinates

```
groupId    = org.apache.flink
artifactId = flink-test-utils-parent
packaging  = pom
```

### Sub-modules

| Sub-module | Purpose |
|---|---|
| `flink-test-utils-junit` | Pure-JUnit / Log4j2 / AssertJ-level helpers. No Flink-runtime deps. `TestLogger`, `TestLoggerExtension`, `LogLevelExtension`, `OneShotLatch`, `MultiShotLatch`, `BlockerSync`, `CheckedThread`, `RetryRule` / `RetryOnFailure`, `RetryOnException`, `SharedObjects`, `ContextClassLoaderExtension`, `ManuallyTriggeredScheduledExecutorService`, `FlinkAssertions`, `FlinkCompletableFutureAssert`, `LoggerAuditingExtension`, `FailsOnJava11` / `FailsOnJava17` / `FailsWithAdaptiveScheduler` / `FailsInGHAContainerWithRootUser` category tags, `DockerImageVersions`, `S3TestCredentials`, `OSSTestCredentials`, `Whitebox`. |
| `flink-test-utils` | The big one — Flink-runtime-aware test infra. `MiniClusterExtension` (JUnit 5), `MiniClusterWithClientResource` (JUnit 4), `TestStreamEnvironment`, `AbstractTestBase` / `AbstractTestBaseJUnit4`, `JavaProgramTestBase`, `MultipleProgramsTestBase`, `TestBaseUtils`, `TestUtils`, `SuccessException`, `FiniteTestSource`, `TestListResultSink`, `MetricListener` / `MetricAssertions`, `PojoTestUtils`, `PackagingTestUtils`, `SecureTestEnvironment` / `TestingSecurityContext`, `JobSubmission` / `SQLJobSubmission`, `MiniClusterPipelineExecutorServiceLoader`, `UpsertTestSink` (a tiny sink used as a fixture by Table sink ITCases), canonical `*Data` fixtures (`WordCountData`, `PageRankData`, `KMeansData`, …). |
| `flink-test-utils-connector` | Minimal Source v2 stubs for unit-level connector tests: `AbstractTestSource`, `SingleSplitEnumerator`, `TestSplitEnumerator`, `TestSplit`, `TestReaderOutput`, `TestSourceReader`, `VoidSerializer`. No external deps beyond `flink-core`. |
| `flink-connector-test-utils` | **Full connector test framework** for external connector repos. `org.apache.flink.connector.testframe.*`: `TestEnvironment` / `MiniClusterTestEnvironment` / `FlinkContainerTestEnvironment`, `FlinkContainers` + `FlinkImageBuilder` + `FlinkContainersSettings` + `TestcontainersSettings`, `SourceTestSuiteBase` / `SinkTestSuiteBase`, `ExternalContext` / `ExternalContextFactory` / `ExternalSystemSplitDataWriter` / `ExternalSystemDataReader` / `DefaultContainerizedExternalSystem`, `MetricQuerier`, `CollectIteratorAssertions`. Plus `formats.SchemaTestUtils` / `DummyInitializationContext`. |
| `flink-clients-test-utils` | Tiny module that *only* produces shaded test user jars used by `flink-clients` classloading tests: `TestUserClassLoaderJob`, `TestUserClassLoaderJobLib`, `TestUserClassLoaderAdditionalArtifact`. The pom builds three classifier-tagged jars (`job-jar`, `job-lib-jar`, `additional-artifact-jar`). |
| `flink-migration-test-utils` | State-migration test framework. `MigrationTest` interface (with `@SnapshotsGenerator` / `@ParameterizedSnapshotsGenerator`), `MigrationTestsSnapshotGenerator` CLI entry-point invoked by the `generate-migration-test-data` Maven profile, `SnapshotGeneratorUtils`, `PublishedVersionUtils`. See its own `README.md` for the snapshot-generation workflow. |
| `flink-table-filesystem-test-utils` | Test-only `TestFileSystemTableFactory` + `TestFileSystemCatalog` (+ `JsonSerdeUtil`) used by Table planner tests that need a filesystem-backed catalog without pulling in a real Hive/JDBC catalog. |

## Dependencies

### Direct (per sub-module — see each pom)

- `flink-test-utils-junit` — JUnit Jupiter + Vintage, AssertJ, Log4j2,
  Testcontainers. Zero Flink runtime deps so it can be safely depended on
  from the lowest layers.
- `flink-test-utils` — depends on `flink-runtime`, `flink-clients`,
  `flink-streaming-java`, `flink-table-common`, `flink-table-api-java`,
  `flink-statebackend-rocksdb` (runtime), `flink-rpc-akka-loader`
  (test-jar fallback for IDE runs), `flink-dstl-dfs` (runtime),
  `hadoop-minikdc` (`optional`), `curator-test`.
- `flink-connector-test-utils` — depends on `flink-test-utils`,
  `flink-test-utils-junit`, `flink-streaming-java`, `flink-table-common`,
  Testcontainers.
- `flink-table-filesystem-test-utils` — depends on `flink-table-common`,
  `flink-table-api-java-bridge`, `flink-connector-files` (all `provided`).
- `flink-migration-test-utils` — depends on `flink-annotations`,
  `flink-test-utils-junit`, `commons-cli`, JUnit Vintage.
- `flink-clients-test-utils` — depends on `flink-streaming-java`
  (`provided`); produces shaded user-jars.

### Used by

This is the most widely depended-upon test infrastructure in the repo.
A non-exhaustive list:

- `flink-tests`, `flink-tests-java17`, `flink-fs-tests`,
  `flink-end-to-end-tests` (every sub-module).
- `flink-runtime`, `flink-streaming-java`, `flink-clients`,
  `flink-table-*`, `flink-statebackend-*`, `flink-filesystems/*`,
  `flink-connectors/*`, `flink-formats/*`, `flink-yarn`,
  `flink-kubernetes`, `flink-metrics-*`, **and**
  `flink-examples-streaming` (which uses `MiniClusterWithClientResource`
  and `TestLogger` in its own tests).
- External (out-of-tree) connector projects (Kafka, Pulsar, ES, …)
  consume `flink-connector-test-utils` to plug into the `SourceTestSuite`
  / `SinkTestSuite` framework.

## Source layout

```
flink-test-utils-parent/
  pom.xml                                    # aggregator only
  flink-test-utils-junit/
    src/main/java/org/apache/flink/
      util/             # TestLogger, TestLoggerExtension, LogLevelExtension, DockerImageVersions, ...
      core/testutils/   # OneShotLatch, MultiShotLatch, BlockerSync, CheckedThread, FlinkAssertions, ...
      mock/             # Whitebox (reflection-based field access)
      testutils/junit/  # RetryRule, RetryOnFailure, RetryOnException, SharedObjects, FailsOnJavaXX, ...
      testutils/logging/# LoggerAuditingExtension, TestLoggerResource
      testutils/s3/     # S3TestCredentials
      testutils/oss/    # OSSTestCredentials
      testutils/executor/ # TestExecutorExtension, TestExecutorResource
  flink-test-utils/
    src/main/java/org/apache/flink/
      test/util/        # AbstractTestBase, MiniClusterWithClientResource, TestBaseUtils, JobSubmission, ...
      test/junit5/      # MiniClusterExtension, InjectClusterClient
      test/testdata/    # WordCountData, PageRankData, KMeansData, ConnectedComponentsData, ...
      test/parameters/  # ParameterProperty
      test/resources/   # ResourceTestUtils
      streaming/util/   # TestStreamEnvironment, FiniteTestSource
      test/streaming/runtime/util/ # TestListResultSink, TestListWrapper
      metrics/testutils/ # MetricListener, MetricAssertions
      packaging/        # PackagingTestUtils
      types/            # PojoTestUtils
      connector/upserttest/ # UpsertTest sink + Table factory (used by Table sink ITCases)
  flink-test-utils-connector/   # minimal Source v2 testing stubs
  flink-connector-test-utils/   # full external connector test framework
  flink-clients-test-utils/     # shaded user-code jars (test-user-classloader-*)
  flink-migration-test-utils/   # MigrationTest + snapshot generator
  flink-table-filesystem-test-utils/ # TestFileSystemCatalog + factory
```

## Architecture & key concepts

- **Layered by Flink-runtime dependency.**
  - `flink-test-utils-junit` is the foundation — pure JUnit / AssertJ /
    Log4j2 / Testcontainers, no Flink runtime types. It can be depended on
    by `flink-core` and `flink-annotations` tests without circular deps.
  - `flink-test-utils` layers on `flink-runtime` / `flink-streaming-java` /
    `flink-clients` and introduces MiniCluster lifecycle.
  - `flink-connector-test-utils` layers on `flink-test-utils` and adds
    Testcontainers-based real-cluster / external-system testing.
- **`MiniClusterExtension` is the recommended pattern.** It registers a
  `MiniCluster`, sets the `TestStreamEnvironment` thread-local so
  `StreamExecutionEnvironment.getExecutionEnvironment()` returns one
  bound to the MiniCluster, and exposes `@InjectClusterClient` for
  parameter resolution into `@Test` methods.
- **`MiniClusterWithClientResource` is the JUnit 4 equivalent**, still
  widely used by legacy tests. New tests should use the JUnit 5 extension.
- **`AbstractTestBase` (JUnit 5) / `AbstractTestBaseJUnit4`** wrap the
  above in a base class with a managed `@TempDir`, an `@AfterEach` that
  cancels straggling jobs, and a default 4-slot MiniCluster.
- **`TestLogger` / `TestLoggerExtension`** print which test is starting /
  finishing, with a `TestSignalHandler` registered to print stack traces
  on SIGTERM — invaluable for diagnosing CI hangs.
- **`MigrationTest` framework** — see
  `flink-migration-test-utils/README.md`. Implementers annotate
  snapshot-generator methods with `@SnapshotsGenerator` /
  `@ParameterizedSnapshotsGenerator`; the
  `generate-migration-test-data` Maven profile invokes
  `MigrationTestsSnapshotGenerator` to (re)produce the savepoints.
- **`FlinkContainers` framework** — `MiniClusterTestEnvironment` and
  `FlinkContainerTestEnvironment` are paired so connector test suites can
  run the same `SourceTestSuiteBase` / `SinkTestSuiteBase` against both
  in-JVM and real Dockerized Flink without code changes.
- **`upserttest` sink** (in `flink-test-utils`) is **not** a "test helper"
  in the JUnit sense — it's a registered Table sink connector
  (`org.apache.flink.connector.upserttest.table.UpsertTestDynamicTableSinkFactory`)
  whose only purpose is to capture rows for assertion in Table ITCases.
  Don't confuse it with production sinks.

## Important public APIs

This is what the rest of the codebase actually imports. The most
load-bearing classes:

- `org.apache.flink.test.junit5.MiniClusterExtension`
- `org.apache.flink.test.util.MiniClusterWithClientResource`
- `org.apache.flink.test.util.AbstractTestBase` /
  `AbstractTestBaseJUnit4`
- `org.apache.flink.streaming.util.TestStreamEnvironment`
- `org.apache.flink.streaming.util.FiniteTestSource`
- `org.apache.flink.util.TestLogger` / `TestLoggerExtension`
- `org.apache.flink.core.testutils.OneShotLatch` /
  `MultiShotLatch` / `CheckedThread` / `BlockerSync` /
  `ManuallyTriggeredScheduledExecutorService` / `FlinkAssertions`
- `org.apache.flink.testutils.junit.SharedObjects` / `RetryRule` /
  `RetryOnFailure` / `FailsOnJava11` / `FailsOnJava17`
- `org.apache.flink.metrics.testutils.MetricListener`
- `org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment`
- `org.apache.flink.connector.testframe.container.FlinkContainers`
- `org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase` /
  `SinkTestSuiteBase`
- `org.apache.flink.test.util.MigrationTest`

Note: `flink-examples-streaming` and many connector / format modules pull
in `flink-test-utils` solely for `MiniClusterWithClientResource` +
`TestLogger` + the `executeAndCollect()` plumbing in
`TestStreamEnvironment`.

## How to run the tests

```sh
# All sub-modules' own tests
mvn -pl flink-test-utils-parent -am test

# Single sub-module
mvn -pl flink-test-utils-parent/flink-test-utils test
```

Each sub-module has a small set of self-tests (`MiniClusterExtensionTest`,
`RetryOnFailureTest`, `OneShotLatchTest`, …) — these verify the test
infrastructure itself.

## Tests

The sub-modules are tested in-place:

- `flink-test-utils-junit/src/test/...` — `RetryOnFailureTest`,
  `RetryOnExceptionTest`, `OneShotLatchTest`, `FlinkAssertionsTest`,
  `TestLoggerResourceTest`, `ParameterizedTestExtensionTest`.
- `flink-test-utils/src/test/...` — `MiniClusterExtensionTest`,
  `MetricListenerTest`, `PojoTestUtilsTest`, `PackagingTestUtilsTest`,
  `RescalingBenchmarkTest`.
- `flink-table-filesystem-test-utils/src/test/...` —
  `TestFileSystemCatalogITCase`, `TestFileSystemCatalogFactoryTest`.
- `flink-test-utils-connector/src/test/...` —
  `SourcePatternExamplesTest`.

## Pitfalls & gotchas

- **`flink-test-utils-junit` is `compile`-scope intentionally.** Several
  modules want the latches and `TestLogger` available in `src/main`
  (for example so a connector's `*Builder` can expose a
  test-friendly hook). Don't mark its dependency as `test`-scope without
  understanding why each consumer needs it.
- **Do not create circular deps.** Many `flink-*` modules depend on
  `flink-test-utils` at `test` scope. Putting a `flink-tests` /
  `flink-streaming-java`-test-jar dependency back into `flink-test-utils`
  would form a cycle.
- **`MiniClusterExtension` vs `MiniClusterWithClientResource`.** They
  share `MiniClusterResource` under the hood but expose different
  lifecycles. Pick based on the JUnit version of the consuming test —
  do not register both in one test.
- **Test-jars matter.** `flink-test-utils` itself publishes a test-jar
  (see the `maven-jar-plugin` config in its pom). Some test code is in
  `src/test/...` and only reachable via that test-jar (e.g.
  `RescalingBenchmark`).
- **`hadoop-minikdc` is `<optional>true</optional>`.** Modules using
  `SecureTestEnvironment` must redeclare it themselves; the comment in
  the pom explains why.
- **`upserttest` factory** is registered via `META-INF/services` in
  `flink-test-utils`, so anything that transitively depends on
  `flink-test-utils` will see `upsert-test` as a valid connector. That's
  intentional but surprising — don't ship `flink-test-utils` on a
  production classpath.
- **External connector compatibility.** `flink-connector-test-utils`'s
  packages (`org.apache.flink.connector.testframe.*`) are consumed by
  out-of-tree connector repos. Treat its API surface as **public** —
  breaking changes need release-notes-grade coordination.

## Related modules / docs

- `flink-test-utils-parent/flink-migration-test-utils/README.md` — how
  to write a `MigrationTest` and (re)generate snapshot data.
- `flink-tests` — the headline consumer; cross-module ITCases.
- `flink-end-to-end-tests/flink-end-to-end-tests-common` — E2E-flavored
  test helpers (`FlinkResource`, `FlinkDistribution`,
  `AutoClosableProcess`) that complement the in-JVM helpers here.
- `flink-fs-tests`, `flink-tests-java17` — additional consumers in this
  repo.
- `flink-runtime/src/test/java/org/apache/flink/runtime/testutils/` —
  lower-level `InternalMiniClusterExtension` / `MiniClusterResource`
  that `MiniClusterExtension` wraps.
