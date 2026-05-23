# `flink-dist`

## Purpose

`flink-dist` produces the **Flink binary distribution** — the `flink-<version>-bin.tgz` tarball that
end users actually download and run. It is the assembly module: it pulls together the Flink runtime
fat jar, the optional jars under `opt/`, the pluggable jars under `plugins/`, the shell scripts
under `bin/`, the default configuration under `conf/`, and the bundled examples under `examples/`.

This is not a library module — nothing inside Flink "depends on" `flink-dist`. It is the
*sink* of the build: every important runtime jar funnels into here.

## Where it fits

- **Built last** (or near-last) in the reactor — after `flink-runtime`, `flink-runtime-web`,
  `flink-clients`, `flink-table-*`, `flink-connectors/flink-connector-files`, `flink-formats/*`,
  `flink-metrics/*`, `flink-state-backends/*`, `flink-filesystems/*`, `flink-libraries/flink-cep`,
  `flink-python`, `flink-examples/*`, `flink-dist-scala`, etc.
- The `symlink-build-target` profile (active on Unix) creates a `build-target` symlink at the repo
  root pointing at `flink-dist/target/flink-<version>-bin/flink-<version>` so other tests and tools
  can locate "the freshly built distribution."

## Maven coordinates

- groupId: `org.apache.flink`
- artifactId: `flink-dist_${scala.binary.version}` (the artifact is suffixed with `_2.12` for
  historical reasons even though the uber jar itself is Scala-free — the file is renamed to
  `flink-dist-<version>.jar` when copied into `lib/`)
- packaging: `jar` (but the *real* output is the directory assembly under `target/flink-<version>-bin/`)

## Dependencies

### Direct

The uber jar (`flink-dist-<version>.jar` in `lib/`) shades together (via `maven-shade-plugin`,
execution id `shade-dist`):

- `flink-core`, `flink-runtime`, `flink-runtime-web`, `flink-clients`, `flink-streaming-java`
- `flink-metrics-core`, `flink-container`, `flink-connector-base`, `flink-hadoop-fs`
- `flink-statebackend-rocksdb`, `flink-statebackend-forst`, `flink-statebackend-changelog`,
  `flink-dstl-dfs`
- `flink-kubernetes`, `flink-yarn` (with Hadoop excluded — users provide their own Hadoop)
- `slf4j-api`, `jsr305`, `objenesis`
- log4j 2 (`log4j-api`, `log4j-core`, `log4j-slf4j-impl`, `log4j-1.2-api`,
  `log4j-layout-template-json`) is intentionally **excluded** from the uber jar and placed under
  `lib/` as separate jars (so users can swap logging frameworks).

A second shade execution (`bash-utils`) produces `target/bash-java-utils.jar`, a tiny standalone
jar containing only `org.apache.flink.runtime.util.bash.BashJavaUtils` (used by the shell scripts
to compute memory parameters in JVM-equivalent units — see `codedocs/flink-memory-configuration.md`).

Provided-scope dependencies are *not* in the uber jar but are copied as individual jars into
`lib/`, `opt/`, `plugins/`, or `examples/` by the three assembly descriptors below.

### Used by

Internally: nothing — `flink-dist` is a terminal node.

Externally: every Apache Flink end user. The produced tarball is what is published to
[flink.apache.org/downloads](https://flink.apache.org/downloads/), what Docker images are built
from, and what Kubernetes/YARN operators ship.

## Layout of the produced artifact

The `maven-assembly-plugin` produces `target/flink-<version>-bin/flink-<version>/` with:

```
flink-<version>/
  bin/          start-cluster.sh, stop-cluster.sh, flink, jobmanager.sh, taskmanager.sh,
                historyserver.sh, flink-console.sh, flink-daemon.sh, config.sh,
                bash-java-utils.{sh,jar}, config-parser-utils.sh, find-flink-home.sh,
                migrate-config-file.sh, standalone-job.sh, sql-client.sh, sql-gateway.sh,
                pyflink-shell.sh, yarn-session.sh, kubernetes-{session,jobmanager,taskmanager}.sh,
                zookeeper.sh, start-zookeeper-quorum.sh, ...
  conf/         config.yaml, log4j.properties, log4j-cli.properties, log4j-console.properties,
                log4j-session.properties, logback*.xml, masters, workers, zoo.cfg
  lib/          flink-dist-<version>.jar (the uber jar), flink-scala_<scala>-<version>.jar,
                flink-table-api-java-uber, flink-table-runtime, flink-table-planner-loader,
                flink-cep, flink-connector-files, flink-csv, flink-json, log4j-* jars
  opt/          flink-table-planner_<scala>, flink-sql-client, flink-sql-gateway,
                flink-state-processor-api, flink-s3-fs-{hadoop,presto},
                flink-{azure,oss,gs}-fs-hadoop, flink-queryable-state-runtime, flink-python,
                opt/python/ (PyFlink shipping bits)
  plugins/      README.txt + one subdirectory per optional plugin:
                metrics-{jmx,graphite,influx,prometheus,statsd,datadog,slf4j,otel},
                external-resource-gpu/
  examples/     streaming/, table/, python/  (jar files + Python skeletons)
  log/          empty directory created by the assembly
  LICENSE, README.txt
```

The three assembly descriptors live under `src/main/assemblies/`:

- `bin.xml` — the main descriptor. Copies `lib/`, `conf/`, scripts, and examples.
- `opt.xml` — populates `opt/` (table planner with Scala, SQL client/gateway, FS connectors,
  queryable state, Python).
- `plugins.xml` — populates `plugins/` (each metric reporter is a separate plugin subdirectory; the
  GPU external-resource plugin ships with its discovery scripts).

## Architecture / what's bundled

Two distinct classloading regions:

1. **`lib/`** — loaded by the system classloader. Anything here is visible to user code and to all
   of Flink's own modules. Adding random jars here pollutes the classpath for every job.
2. **`plugins/<name>/`** — each subdirectory becomes an isolated `PluginClassLoader` (parent-last
   with respect to a small set of API packages). Metric reporters, GPU external resources, and most
   filesystem connectors are loaded this way so their transitive deps cannot conflict with user jobs.

Filesystem jars under `opt/flink-{s3,gs,oss,azure}-fs-*.jar` are **not** automatically active;
operators move/symlink them into a `plugins/<name>/` subdirectory at deploy time.

The two java11-exclusive dependencies (`jaxb-api`, `javax.activation-api`) are packed under
`META-INF/versions/11/` inside `flink-dist-<version>.jar` via the `maven-antrun-plugin` so they
are only on the classpath on Java 11+.

## Important configuration files

- **`conf/config.yaml`** — the canonical Flink configuration file as of Flink 2.x. Source:
  `src/main/resources/config.yaml`. Standard YAML format. Earlier versions used `flink-conf.yaml`
  with a custom flat-key syntax (e.g. `taskmanager.memory.process.size: 1728m`); 2.x switched to
  nested YAML (`taskmanager: { memory: { process: { size: 1728m } } }`). Most defaults relevant to
  memory tuning (process size, managed memory fraction, network buffers) live here — see
  `codedocs/flink-memory-configuration.md`.
- **`bin/migrate-config-file.sh`** — one-shot migrator that reads a legacy `flink-conf.yaml` and
  emits a `config.yaml` in standard YAML, via `BashJavaUtils#migrateLegacyFlinkConfigToStandardYaml`.
- **`bin/flink`** — the user-facing CLI entry point. Sources `config.sh`, which itself uses
  `bash-java-utils.sh` to invoke `bash-java-utils.jar` for memory/classpath computation.
- **`bin/start-cluster.sh` / `stop-cluster.sh`** — read `conf/masters` and `conf/workers` to start
  jobmanagers / taskmanagers via SSH.
- **`conf/log4j.properties`** — log4j 2 config for cluster daemons. Rolling file appender,
  100 MB rolls, `MAX_LOG_FILE_NUMBER` env var (default 10). `log4j-cli.properties`,
  `log4j-console.properties`, `log4j-session.properties` cover the CLI, foreground/container, and
  SQL client respectively. Logback variants (`logback*.xml`) exist for users who swap loggers.
- **`conf/zoo.cfg`** — default ZooKeeper config used by the bundled ZK quorum scripts.

## Pitfalls & gotchas

- **`config.yaml` vs `flink-conf.yaml`**: Flink 2.x switched format. Old scripts/operators that
  templated the flat-key file will produce a file Flink no longer parses correctly. Use
  `bin/migrate-config-file.sh`.
- **Plugin classloader isolation**: Drop a metric reporter or filesystem under `lib/` instead of
  `plugins/<name>/` and you will hit `NoClassDefFoundError` or shaded-dependency conflicts. The
  `plugins/` layout described in `plugins/README.txt` is required: `plugins/<plugin-id>/*.jar`.
- **Scala artifact suffix**: The Maven artifact is `flink-dist_2.12` but the jar inside the
  tarball is renamed to `flink-dist-<version>.jar` (no scala suffix) because the uber jar contains
  no Scala. The Scala bits live in a separate `flink-scala_2.12-<version>.jar` (sourced from
  `flink-dist-scala`).
- **Hadoop is not bundled**. The `flink-yarn` dependency excludes `org.apache.hadoop:*`; operators
  must set `HADOOP_CLASSPATH` or drop Hadoop jars into `lib/`.
- **`flink-table-planner_2.12` ships under `opt/`**, not `lib/`. The default `lib/` contains
  `flink-table-planner-loader-<version>.jar` (the Scala-free loader). Putting both on the
  classpath simultaneously breaks the planner-loader's isolated classloading.
- **`log4j-*` jars are *not* in the uber jar** — they are added separately to `lib/`. Stripping
  them out (e.g. when building a custom Docker image) leaves Flink without a working logging
  backend.
- **`flink.markBundledAsOptional`** property toggles whether shaded dependencies are marked
  `<optional>true</optional>` in the published POM. This is a build-engineering knob; do not flip
  it accidentally.
- **`shade-flink` execution id**: there is a now-empty execution kept around (phase=`none`)
  named `shade-flink` for historical compatibility. The active one is `shade-dist`.

## Related modules / docs

- `flink-dist-scala/` — the Scala-only sibling whose output jar is copied into `lib/` as
  `flink-scala_<scala>-<version>.jar`.
- `flink-quickstart/` — Maven archetypes consumed by end users; the archetype's generated POM
  targets `flink-streaming-java` + `flink-clients` (both of which are bundled here).
- `flink-table/flink-table-planner-loader/` — the Scala-free planner loader that ships in `lib/`.
- `flink-table/flink-table-planner/` — Scala-bearing planner that ships in `opt/` for users who
  need it.
- `flink-runtime/.../bash/BashJavaUtils` — the Java entry point bundled as `bash-java-utils.jar`.
- `codedocs/flink-memory-configuration.md` — explains how the defaults in `conf/config.yaml`
  (`jobmanager.memory.process.size`, `taskmanager.memory.process.size`, managed memory fractions)
  are interpreted at runtime.
