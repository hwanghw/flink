# `flink-fs-tests`

## Purpose

Cross-cutting **FileSystem-layer tests** that don't fit inside any single
filesystem sub-module under `flink-filesystems/`. The tests in here exercise
the Flink `FileSystem` abstraction layer, the distributed cache, file-source
input formats, and HDFS-flavored filesystem behavior — using HDFS
(`MiniDFSCluster`) as the canonical "real" remote filesystem because every
Hadoop-compatible store is bytecode-tested against the same APIs.

It's deliberately a small, narrowly scoped module — the full filesystem
implementations and their unit tests live under
`flink-filesystems/flink-{hadoop,s3,gs,azure,oss}-fs-*/`. This module is for
tests that **cross** filesystems / sources / distributed-cache concerns.

## Where it fits — when developers would touch this module

- Adding an integration test that runs a streaming Flink job over a
  `MiniDFSCluster`.
- Validating that `ContinuousFileMonitoringFunction` /
  `ContinuousFileReaderOperator` behave correctly against a real DFS,
  including state restore across savepoints.
- Validating the `DistributedCache` against a DFS (vs the local FS, which is
  unit-tested in `flink-runtime`).
- Updating the canned migration snapshots used by
  `ContinuousFileProcessingMigrationTest` when a new Flink version ships.

For pure-FileSystem unit tests (e.g. `LocalFileSystemTest`,
`S3FileSystemBehaviorITCase`), use the relevant `flink-filesystems/...`
sub-module instead.

## Maven coordinates

```
groupId    = org.apache.flink
artifactId = flink-fs-tests
packaging  = jar
```

No sub-modules. This is a Hadoop-2/3 only module (the pom comments it as a
"Hadoop2 only flink module" but a `hadoop3-tests` profile adds the
`hadoop-hdfs-client` needed on Hadoop 3).

## Dependencies

### Direct (all `test` scope)

- `org.apache.hadoop:hadoop-common`, `hadoop-hdfs` (main + test-jar) —
  `MiniDFSCluster` and friends. `reload4j` / `slf4j-reload4j` are excluded
  everywhere because Flink standardizes on Log4j 2.
- `flink-streaming-java` (test-jar), `flink-runtime` (test-jar),
  `flink-core` (test-jar) — base test utilities + reusable abstract
  `*TestBase` classes.
- `flink-examples-streaming` — provides the small example jobs used as
  fixtures by the tests.
- `flink-avro` — used for Avro-format read/write over DFS.
- `flink-test-utils`, `flink-test-utils-junit` — `MiniClusterExtension`,
  `TestLogger`, etc.
- `flink-migration-test-utils` — `ContinuousFileProcessingMigrationTest`
  implements `MigrationTest` against canned savepoints.

### Profiles

- `hadoop3-tests` — adds `hadoop-hdfs-client` (needed on Hadoop 3, where the
  `hadoop-hdfs` jar no longer contains the client).
- `generate-migration-test-data` — re-generates the migration snapshots via
  `MigrationTestsSnapshotGenerator` (same mechanism used by `flink-tests`).

### Used by

Nothing. This module produces only test-classes.

## Source layout

```
src/test/
  java/org/apache/flink/hdfstests/
    HDFSTest.java                              # core FileSystem-over-MiniDFSCluster smoke tests
    ContinuousFileProcessingTest.java          # source-function level tests
    ContinuousFileProcessingITCase.java        # full job-graph ITCase over DFS
    ContinuousFileProcessingMigrationTest.java # MigrationTest with canned savepoints
    DistributedCacheDfsTest.java               # DistributedCache pull from DFS
    Utils.java                                 # shared MiniDFSCluster setup
  resources/                                   # canned savepoints + small input fixtures
```

There is no `src/main` — the entire module is `src/test/`.

## Architecture & key concepts

- **MiniDFSCluster as the substrate.** All tests boot an in-process HDFS
  cluster (`MiniDFSCluster`), get a `org.apache.hadoop.fs.FileSystem` from
  it, and exercise Flink against that. Flink's `FileSystem` is loaded via
  the `HadoopFileSystem` wrapper.
- **`MiniCluster + MiniDFSCluster` paired.** ITCases compose Flink's
  `MiniClusterExtension` with a JUnit lifecycle around `MiniDFSCluster`
  (see `Utils.java`). The two clusters live in the same JVM but use
  different ports.
- **Continuous file source coverage.** Three related tests deliberately
  layer over each other:
  - `ContinuousFileProcessingTest` — operator-level unit tests with
    `OneInputStreamOperatorTestHarness`.
  - `ContinuousFileProcessingITCase` — end-to-end DataStream job on
    MiniCluster + MiniDFSCluster.
  - `ContinuousFileProcessingMigrationTest` — restores savepoints saved by
    previous Flink versions, kept under `src/test/resources/`.
- **DistributedCache over DFS.** `DistributedCacheDfsTest` verifies that
  files registered via `env.registerCachedFile(dfsURI, ...)` are pulled to
  task managers correctly when the URI is a DFS URI (not a local path).

## Important public APIs

None — there is no `src/main`. The module exposes nothing to other modules.

## How to run the tests

```sh
# Default profile (Hadoop 2 baseline)
mvn -pl flink-fs-tests -am test

# Against Hadoop 3
mvn -pl flink-fs-tests -am test -Phadoop3-tests

# Regenerate migration snapshots (rare; do this on the target release branch)
mvn -pl flink-fs-tests package -Pgenerate-migration-test-data \
    -Dgenerate.version=2.3 -nsu -Dfast -DskipTests
```

The pom forcibly unsets `HADOOP_HOME` / `HADOOP_CONF_DIR` in the surefire
environment so a developer's host Hadoop install can't accidentally override
the `MiniDFSCluster`.

## Tests

Six Java test files (listed above) — that's the whole suite. Despite the
small count, each ITCase boots both a `MiniCluster` and a `MiniDFSCluster`,
so total runtime is on the order of a minute or two per class.

## Pitfalls & gotchas

- **Slow because of MiniDFSCluster.** HDFS startup is expensive; don't add
  per-test `@BeforeEach` boots — share the cluster via `@BeforeAll` /
  `@RegisterExtension` static fields.
- **Migration snapshots are append-only.** Files under `src/test/resources/`
  encode the on-wire state of `ContinuousFileMonitoringFunction` at past
  releases — never edit or delete existing ones.
- **Hadoop 2 vs Hadoop 3.** The default build covers Hadoop 2; CI also runs
  the `hadoop3-tests` profile. If you add a test that touches a HDFS API
  whose signature changed between versions, gate the assertion accordingly
  and exercise both profiles.
- **Logger exclusions matter.** The pom strips `reload4j` /
  `slf4j-reload4j` from every Hadoop dep. If you add a new Hadoop dep, mirror
  those exclusions or surefire will fail with multiple SLF4J bindings.
- **MiniCluster vs MiniClusterWithClientResource.** Tests in this module use
  JUnit 5 + `MiniClusterExtension` (preferred). Don't reach for the legacy
  JUnit 4 resource unless you're modifying a pre-existing JUnit 4 test.

## Related modules / docs

- `flink-filesystems/` — the actual `FileSystem` implementations (`hadoop`,
  `s3`, `gs`, `azure`, `oss`) and their own unit tests.
- `flink-connector-files` — `FileSource` / `FileSink` connectors; their
  in-JVM ITCases live in `flink-tests`.
- `flink-test-utils-parent/flink-test-utils` — `MiniClusterExtension`,
  `TestStreamEnvironment`.
- `flink-test-utils-parent/flink-migration-test-utils` — `MigrationTest`
  interface and `MigrationTestsSnapshotGenerator`.
- `flink-tests` — for cross-module ITCases that don't specifically need
  HDFS.
