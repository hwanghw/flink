# `flink-end-to-end-tests`

## Purpose

True **end-to-end (E2E) tests** that exercise Flink against a *real* deployed
cluster — not a MiniCluster. Tests in here launch an actual Flink
distribution (downloaded / built `flink-dist` tarball), submit jobs through
the CLI / REST / Kubernetes / YARN, and watch external systems (Kafka,
S3, HDFS, Confluent Schema Registry, …) for the expected effects.

These tests are slow, environment-heavy, and run by CI on every PR + nightly,
gated behind the `run-end-to-end-tests` Maven profile.

## Where it fits — when developers would touch this module

- Adding a new "deployment scenario" test: HA failover, savepoint upgrade,
  state migration, Kubernetes / YARN / standalone job-cluster lifecycle.
- Validating a connector against its real backing system (Kafka,
  Confluent Schema Registry, ES, …).
- Adding a regression test that requires the full Flink shell scripts
  (`bin/flink`, `bin/start-cluster.sh`, …) — these can't be exercised by a
  MiniCluster.
- Adding a SQL CLI / SQL gateway / JDBC driver test.

If a test only needs an in-JVM cluster, put it in `flink-tests` instead.

## Maven coordinates

```
groupId    = org.apache.flink
artifactId = flink-end-to-end-tests
packaging  = pom (aggregator)
```

### Sub-modules (33+)

Roughly grouped by what they test:

| Group | Sub-modules |
|---|---|
| **Common infrastructure** | `flink-end-to-end-tests-common`, `flink-end-to-end-tests-common-kafka` *(if present)* |
| **CLI / API surface** | `flink-cli-test`, `flink-end-to-end-tests-restclient`, `flink-end-to-end-tests-jdbc-driver`, `flink-sql-client-test`, `flink-sql-gateway-test` |
| **SQL / Table** | `flink-batch-sql-test`, `flink-stream-sql-test`, `flink-end-to-end-tests-sql`, `flink-end-to-end-tests-table-api`, `flink-tpch-test`, `flink-tpcds-test` |
| **DataStream / runtime** | `flink-datastream-allround-test`, `flink-distributed-cache-via-blob-test`, `flink-local-recovery-and-allocation-test`, `flink-heavy-deployment-stress-test`, `flink-netty-shuffle-memory-control-test` |
| **State / checkpointing** | `flink-stream-state-ttl-test`, `flink-stream-stateful-job-upgrade-test`, `flink-state-evolution-test`, `flink-rocksdb-state-memory-control-test`, `flink-queryable-state-test` |
| **File sink / connectors** | `flink-file-sink-test`, `flink-confluent-schema-registry` |
| **Classloading / packaging** | `flink-parent-child-classloading-test-program`, `flink-parent-child-classloading-test-lib-package`, `flink-quickstart-test`, `flink-quickstart-test-dummy-dependency`, `flink-plugins-test` |
| **Metrics / failures** | `flink-metrics-availability-test`, `flink-metrics-reporter-prometheus-test`, `flink-failure-enricher-test` |
| **Scala / Python** | `flink-end-to-end-tests-scala`, `flink-python-test` |

Each sub-module is either:
1. A **test program** — a small Flink job assembled into a fat-jar via
   `maven-shade-plugin`. The shell scripts in `test-scripts/` then submit it
   to the cluster.
2. A **Java ITCase** (`*ITCase`) that uses `flink-end-to-end-tests-common` to
   spin up a real Flink distribution via `FlinkContainers` /
   `LocalStandaloneFlinkResource`.

## Dependencies

### Direct

- `flink-yarn-tests` (`provided`, exclusions=`*:*`) — needed only to produce
  `target/yarn.classpath` which the shell scripts source. The exclusion
  prevents it from polluting test classpaths.
- Per-sub-module: each test program declares the exact connectors / formats /
  state backends it needs against the **bundled** `flink-dist`.

### Used by

Nothing. E2E artifacts are not depended on by other modules; they are
consumed only by the runner scripts in this module.

## Source layout

```
flink-end-to-end-tests/
  README.md                                 # bash vs Java tests, how to run
  pom.xml                                   # aggregator, profiles: run-end-to-end-tests / java11 / java17
  run-nightly-tests.sh                      # CI entry point (sources test-runner-common.sh)
  run-single-test.sh                        # local entry point
  test-scripts/                             # 60+ bash tests + common.sh helpers
    common.sh                               # core helpers (start_cluster, wait_job_running, ...)
    common_ha.sh, common_kubernetes.sh,
    common_s3.sh, common_yarn_docker.sh     # scenario-specific helpers
    test_<scenario>.sh                      # one per E2E test (test_resume_savepoint.sh, test_ha_datastream.sh, ...)
    test-runner-common.sh                   # exit-code & log-grep wrapper
    container-scripts/, docker-hadoop-secure-cluster/   # docker resources
    test-data/                              # canonical input fixtures
  flink-end-to-end-tests-common/            # Java helpers: FlinkResource, AutoClosableProcess, DownloadCache, ...
  flink-<scenario>-test/                    # one sub-module per scenario, see table above
```

## Architecture & key concepts

- **Two test styles.** Per the README, E2E tests come in two flavors:
  1. **Bash tests** under `test-scripts/test_*.sh`. They source `common.sh`,
     `start_cluster`, submit a job, grep logs, and exit non-zero on failure.
     Considered legacy — new tests should be Java.
  2. **Java ITCases** that drive a real distribution via
     `flink-end-to-end-tests-common`'s `FlinkResource` /
     `FlinkDistribution` / `FlinkContainers` abstractions, plus
     `AutoClosableProcess` for any external CLIs.
- **`flink-end-to-end-tests-common` is the foundation.** Key classes:
  - `org.apache.flink.tests.util.flink.FlinkResource` — abstract handle to a
    deployed Flink cluster; implementations include
    `LocalStandaloneFlinkResource`.
  - `FlinkDistribution`, `ClusterController`, `JobController`,
    `GatewayController` — façades over `flink-dist` shell scripts and REST.
  - `AutoClosableProcess`, `AutoClosablePath` — try-with-resources wrappers
    around external processes and on-disk artifacts (so the test can't leak
    PIDs or temp dirs on failure).
  - `DownloadCache` family — caches third-party tarballs (Hadoop, ZK, …)
    between CI runs.
  - `categories/PreCommit`, `categories/Dummy` — JUnit category markers used
    by surefire `<excludedGroups>`.
- **Profiles for the JVM matrix.** The aggregator pom exposes
  `excludeE2E`, set to `FailsOnJava11` / `FailsOnJava17` annotations
  depending on the active JDK. CI honors `-Prun-end-to-end-tests` to
  actually fork surefire in the `integration-test` phase.
- **Bash test contract** (from the README):
  - Exit code 0
  - No non-empty `*.out` files
  - No exceptions / errors in cluster logs (with a `check_logs_for_errors`
    whitelist).
- **Each test runs in its own forked surefire JVM** (`<forkCount>1</forkCount>`)
  because tests cannot share the single `flink-dist` directory concurrently.

## Important public APIs

Aside from the helpers in `flink-end-to-end-tests-common`, sub-modules
generally don't publish APIs — they ship a single shaded jar that the bash
scripts submit. The `flink-quickstart-test` sub-module is unusual: it builds
a quickstart-style archetype consumer to make sure the archetype still
produces a runnable job.

## How to run the tests

```sh
# Build the distribution first (E2E tests need a real flink-dist tarball)
mvn -pl flink-dist -am clean install -DskipTests

# Set FLINK_DIR to point at the built distribution
export FLINK_DIR=<repo>/flink-dist/target/flink-2.3-SNAPSHOT-bin/flink-2.3-SNAPSHOT

# Run the whole nightly suite (very slow, modifies host environment!)
./flink-end-to-end-tests/run-nightly-tests.sh

# Run a single bash test
./flink-end-to-end-tests/run-single-test.sh test_batch_wordcount.sh

# Run a single Java ITCase via Maven
mvn -pl flink-end-to-end-tests/flink-end-to-end-tests-common -am \
    verify -Prun-end-to-end-tests -Dtest=DynamicParameterITCase
```

## Tests

The bash tests are under `test-scripts/test_*.sh`; the Java tests are
`*ITCase` classes in each sub-module. CI runs both via `run-nightly-tests.sh`
and the `run-end-to-end-tests` Maven profile, respectively.

## Pitfalls & gotchas

- **They will modify your machine.** From the README:
  > We do not recommend executing the nightly test script on production or
  > personal desktop systems, as tests contained there might modify the
  > environment.
  Stick to disposable VMs / CI runners.
- **No concurrency on `flink-dist`.** Surefire is pinned to `forkCount=1`.
  Trying to parallelize defeats the point and will produce flaky failures.
- **`FLINK_DIR` is mandatory.** All scripts and Java tests assume an
  unpacked distribution. The build does not auto-resolve it.
- **Java version gates.** Tests annotated `@Category(FailsOnJava11.class)` /
  `@Category(FailsOnJava17.class)` are auto-excluded by the `java11` /
  `java17` profiles. If you add a test that legitimately can't run on a
  given JDK, annotate it — don't conditionally skip in code.
- **Log-grep whitelist.** A bash test "passes" only if logs contain *no*
  errors. Adding a noisy log line in the runtime can break unrelated E2E
  tests. The whitelist is in `test-scripts/common.sh::check_logs_for_errors`
  / `::check_logs_for_exceptions`.
- **Don't set the `PASS` global** — that legacy mechanism is no longer
  supported; tests must signal failure via exit codes only.
- **Don't run with `sh run-nightly-tests.sh`.** The scripts assume bash;
  invoking them through `sh` silently misbehaves.

## Related modules / docs

- `flink-end-to-end-tests/README.md` — how to write a bash / Java E2E test
  in this repo (canonical reference).
- `flink-test-utils-parent/flink-connector-test-utils` —
  `FlinkContainers` / `FlinkImageBuilder` / `MiniClusterTestEnvironment`
  used by the Java E2E tests.
- `flink-test-utils-parent/flink-test-utils-junit` — `FailsOnJava11` /
  `FailsOnJava17` JUnit category annotations.
- `flink-yarn-tests` — produces the `target/yarn.classpath` that the YARN
  E2E shell tests rely on.
- `flink-tests` — for in-JVM ITCases (faster, simpler, no `flink-dist`).
