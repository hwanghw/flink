# `flink-tests`

## Purpose

The main cross-module **integration test suite** for Flink. This is where ITCases
live that exercise more than one Flink module at once and therefore don't
"belong" to any single component module. Almost every test here spins up an
embedded `MiniCluster` via `MiniClusterExtension` / `MiniClusterWithClientResource`
and submits real `JobGraph`s end-to-end inside the JVM.

If a test needs `flink-streaming-java` + `flink-runtime` + `flink-clients` +
`flink-statebackend-rocksdb` together (e.g. checkpointing across rescaling +
state migration + classloader behavior), it lives here.

## Where it fits — when developers would touch this module

- Adding an integration test that crosses module boundaries (runtime + streaming
  + clients + state backend, etc.).
- Reproducing a user-reported bug as an ITCase before fixing it in the owning
  module.
- Investigating checkpoint / savepoint / rescaling / failover regressions —
  most of the canonical "did the rescaling story still work?" tests are here.
- Adding a state migration test from a previous Flink version (snapshots stored
  under `src/test/resources/`).
- Tightening completeness coverage (`TypeSerializerTestCoverageTest` etc. scan
  the classpath to make sure every `TypeSerializer` has a matching test).

## Maven coordinates

```
groupId    = org.apache.flink
artifactId = flink-tests
packaging  = jar
```

No sub-modules. This is a single Maven project whose entire purpose is the
`src/test/...` tree; it also publishes a `tests` test-jar so a handful of other
modules can pull in shared test fixtures (see `dependency-reduced-pom.xml`).

## Dependencies

### Direct (all `test` scope unless noted)

- `flink-core`, `flink-runtime`, `flink-streaming-java`, `flink-clients` — main +
  test-jars.
- `flink-table-test-utils`, `flink-table-runtime` (test-jar) — Table API ITCases.
- `flink-connector-files`, `flink-connector-datagen` (test-jar) — file / source
  ITCases.
- `flink-statebackend-rocksdb` (main + test-jar), `flink-dstl-dfs` — state
  backend / changelog ITCases.
- `flink-hadoop-compatibility` (main + test-jar) — HDFS-on-MiniCluster tests.
- `flink-examples-streaming` — used by classloading tests; the
  `flink-connector-kafka` transitive dep is **excluded** because
  `TypeSerializerTestCoverageTest` would otherwise demand a coverage test for
  Kafka internals.
- `flink-test-utils`, `flink-test-utils-junit`, `flink-test-utils-connector`,
  `flink-migration-test-utils` — base test infrastructure.
- `flink-avro`, `flink-json` — used by Table sink / format ITCases.
- `org.apache.curator:curator-test`, `org.apache.hadoop:hadoop-common`,
  `org.reflections:reflections`, `joda-time`, `oshi-core`, `scalatest`.

### Used by

- No production code depends on `flink-tests`. The published test-jar is
  consumed only inside the test harness; in this fork it is not pulled in by
  any other module via `flink-tests:test-jar`.

## Source layout

```
src/
  test/
    java/org/apache/flink/test/
      accumulators/           # accumulator + metric ITCases
      cancelling/             # job cancellation behavior
      checkpointing/          # SavepointITCase, RescalingITCase, ChangelogRecovery*, LocalRecoveryITCase, ...
      classloading/           # user-code classloader policy (uses the assembly-built jars below)
      classloading/jar/       # source for the user-jars that get assembled at process-test-classes
      completeness/           # TypeSerializerTestCoverageTest, TypeInfoTestCoverageTest (reflection-based audits)
      distributedcache/       # DistributedCache ITCases
      example/                # batch / streaming / failing example program ITCases
      execution/              # JobExecutionResult / async execution
      io/                     # input/output format ITCases
      junit5/                 # JUnit 5 wiring sanity test
      manual/                 # tests *not* run in CI — kept for hand-execution
      misc/                   # cross-cutting one-offs
      operators/              # DataStream operator ITCases
      plugin/                 # plugin loading (uses plugin-a / plugin-b assembled jars)
      recovery/               # TM/JM failover, leader election ITCases
      runtime/                # entrypoint, scheduler, leaderelection ITCases
      scheduling/             # scheduler ITCases
      state/                  # KeyedState / OperatorState ITCases
      streaming/              # DataStream API ITCases
      testfunctions/          # shared functions used by ITCases
      typeserializerupgrade/  # snapshot/restore tests across serializer versions
      util/                   # ITCase support: InfiniteIntegerSource, etc.
      windowing/              # window operator ITCases
    resources/
      avro/                                                 # schemas → generated Avro classes
      new-stateful-*-itcase-flink<X.Y>-...-savepoint/...    # canned migration savepoints for many Flink versions
      testdata/                                             # canonical input fixtures
    assembly/                                               # XMLs for the many shaded user-jars built in the pom
```

A `src/main/scala` / `src/test/scala` build is wired (so Scala / Java may be
mixed) but in this tree no Scala sources are present.

## Architecture & key concepts

- **MiniCluster everywhere.** Every ITCase runs against an in-JVM MiniCluster via
  `MiniClusterExtension` (JUnit 5) or `MiniClusterWithClientResource` (JUnit 4).
  No external cluster is involved — that's what `flink-end-to-end-tests` is for.
- **State migration tests.** Many tests implement `MigrationTest` (from
  `flink-migration-test-utils`). Past savepoints/checkpoints are committed under
  `src/test/resources/` keyed by Flink version + state backend; the
  `generate-migration-test-data` profile re-generates them via
  `MigrationTestsSnapshotGenerator`.
- **Test user-code jars.** The pom uses `maven-assembly-plugin` to build many
  small jars (`plugin-a`, `plugin-b`, `usercodetype`, `customsplit`,
  `streaming-customsplit`, `streamingclassloader`,
  `streaming-checkpointed-classloader`, `custom_kv_state`,
  `checkpointing_custom_kv_state`, `classloading_policy`). A subsequent
  `maven-clean-plugin` execution **deletes** the `**/classloading/jar/*.class`
  files from `target/test-classes` so the user-code is only loadable via the
  user classloader — this is what makes the classloading tests meaningful.
- **Completeness audits.** `TypeSerializerTestCoverageTest` and
  `TypeInfoTestCoverageTest` use Reflections to walk the classpath and fail the
  build if a public serializer / type-info is missing a matching test. This is
  why the pom excludes the Kafka connector transitive — those serializers do
  not live in this repo.

## Important public APIs

Tests live in `org.apache.flink.test.*` and are not intended to be consumed by
other modules. A few support classes that are reused via the test-jar:

- `org.apache.flink.test.util.InfiniteIntegerSource`,
  `InfiniteIntegerInputFormat`, `InfiniteIntegerTupleInputFormat`,
  `NumberSequenceSourceWithWaitForCheckpoint` — infinite / latch-coordinated
  inputs used by checkpoint / failover tests.

For test base classes consumed across modules (`AbstractTestBase`,
`MiniClusterExtension`, `MiniClusterWithClientResource`,
`TestStreamEnvironment`, `FiniteTestSource`, …) see
`flink-test-utils-parent/flink-test-utils`.

## How to run the tests

```sh
# Single ITCase
mvn -pl flink-tests -am test -Dtest=SavepointITCase

# Full suite (slow!)
mvn -pl flink-tests -am test

# Regenerate migration savepoints for a new Flink version
mvn -pl flink-tests package -Pgenerate-migration-test-data \
    -Dgenerate.version=2.3 -nsu -Dfast -DskipTests
```

Surefire excludes `**/*TestBase*.class` (those are intentionally abstract). The
pom adds `--add-opens=java.base/java.util=ALL-UNNAMED` and
`--add-opens=java.base/java.io=ALL-UNNAMED` for Kryo / Chill reflection.

## Tests

The module *is* the tests. There are no `src/main` Java sources — all code is
under `src/test/`. Test count is in the high hundreds; this is the single
slowest Maven module in the project and dominates CI time.

## Pitfalls & gotchas

- **Slow.** Each ITCase boots a MiniCluster (JM + TMs + RPC). Prefer running a
  single class with `-Dtest=...`.
- **`MiniClusterExtension` vs `MiniClusterWithClientResource`.** New tests use
  JUnit 5 + `MiniClusterExtension`; legacy JUnit 4 tests use
  `MiniClusterWithClientResource`. Don't mix the two in one test.
- **Migration snapshots.** Never delete files under
  `src/test/resources/new-stateful-*-...-savepoint/` — they are reference
  fixtures committed at the corresponding Flink release.
- **Classloading tests.** If you add a class under
  `src/test/java/org/apache/flink/test/classloading/jar/`, the
  `maven-clean-plugin` execution will strip its `.class` from the test-classes
  output, and the only way to load it at runtime is via the assembled user-jar.
  Make sure there's a matching assembly descriptor.
- **Completeness tests.** Adding a new `TypeSerializer` anywhere in the
  classpath visible from this module will fail the completeness test until a
  matching `TypeSerializerUpgradeTestBase` test exists.
- **`flink-examples-streaming` Kafka exclusion.** Don't remove it; the
  completeness test will start scanning Kafka serializers.

## Related modules / docs

- `flink-test-utils-parent/flink-test-utils` — `MiniClusterExtension`,
  `MiniClusterWithClientResource`, `AbstractTestBase`, `TestStreamEnvironment`.
- `flink-test-utils-parent/flink-migration-test-utils` —
  `MigrationTest`, `MigrationTestsSnapshotGenerator`.
- `flink-tests-java17` — JDK 17-only counterpart (records, sealed classes).
- `flink-end-to-end-tests` — for tests that must run against a deployed
  cluster, not a MiniCluster.
