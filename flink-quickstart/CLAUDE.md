# `flink-quickstart`

## Purpose

`flink-quickstart` is a **container of Maven archetypes** that end users invoke to bootstrap a new
Flink application project. It does not ship any Flink runtime code; its only output is a set of
archetype jars that, when fed to `mvn archetype:generate`, scaffold a working Flink job project
(POM, skeleton main class, log4j config, etc.) targeting the current Flink version.

```
mvn archetype:generate                                       \
  -DarchetypeGroupId=org.apache.flink                        \
  -DarchetypeArtifactId=flink-quickstart-java                \
  -DarchetypeVersion=<flink-version>                         \
  -DgroupId=org.example -DartifactId=my-flink-job            \
  -Dversion=0.1 -Dpackage=org.example.myflinkjob
```

## Where it fits

- A pure-POM aggregator (`packaging=pom`) with archetype sub-modules.
- **No runtime dependency** on or from any other Flink module. It builds at the very end of the
  reactor purely so its version stays in lockstep with the rest of the release. The archetypes it
  produces reference `flink-streaming-java` and `flink-clients` *at the user side*, never here.

## Maven coordinates

- groupId: `org.apache.flink`
- artifactId: `flink-quickstart`
- packaging: `pom`
- Sub-modules (in this checkout):
  - `flink-quickstart-java` — `packaging=maven-archetype`, artifactId `flink-quickstart-java`,
    the Java DataStream archetype.

Historical note: older Flink versions (1.x) also shipped `flink-quickstart-scala`. In Flink 2.x,
following the broader deprecation of the Scala DataStream/DataSet APIs, the Scala archetype has
been **removed** from this module — `flink-quickstart-java` is the only sub-module declared in
`flink-quickstart/pom.xml`. Users wanting Scala can write the dependencies into a manually-created
POM but there is no first-class archetype any more.

## Dependencies

### Direct

The parent POM declares only build-time tooling:

- `org.apache.maven.archetype:archetype-packaging` (extension v2.2) — teaches Maven about the
  `maven-archetype` packaging type.
- `maven-archetype-plugin` v2.2 — wired with `<skip>true</skip>` so the archetype itself isn't
  re-installed on each build of the parent.
- `maven-shade-plugin` — *disabled* here (`<phase/>` empty) to prevent the parent's shade
  inheritance from running on archetype modules.
- `maven-resources-plugin` — configured with non-default delimiters
  (`<delimiter>@</delimiter>`, no defaults) so that the archetype templates can use the standard
  Maven archetype filter syntax `${groupId}`, `${package}`, etc. *without* Maven property
  resolution clobbering them. Build-time substitution uses `@property@` instead — that's how
  `@project.version@`, `@target.java.version@`, `@log4j.version@` make it into the generated
  POM (see `flink-quickstart-java/src/main/resources/archetype-resources/pom.xml`).

### Used by

Internally: nothing.

Externally: end users running `mvn archetype:generate`. Also referenced from Flink's getting-started
documentation, training material, and the Flink Operations Playground.

## Layout of the produced artifact

Each sub-module produces a single jar containing the archetype resources. For
`flink-quickstart-java`:

```
flink-quickstart-java-<version>.jar
├── META-INF/maven/archetype-metadata.xml         (declares filtered/packaged file sets)
└── archetype-resources/
    ├── pom.xml                                   (generated POM template)
    └── src/main/
        ├── java/DataStreamJob.java               (skeleton job class)
        └── resources/log4j2.properties           (log4j config)
```

`archetype-metadata.xml` declares:

- `src/main/java/**/*.java` — `filtered="true" packaged="true"` so velocity substitutes
  `${package}` and the file is placed under the user's chosen package path.
- `src/main/resources/**` — copied verbatim (not filtered).

`archetype.properties` under `src/test/resources/projects/testArtifact/` drives the
`maven-archetype-plugin`'s integration test (`mvn archetype:integration-test`).

## Important archetype templates

- **`archetype-resources/pom.xml`** — the POM the user gets. Notable bits:
  - `flink.version = @project.version@` — pinned to the Flink version that built the archetype.
  - `target.java.version = @target.java.version@` and `log4j.version = @log4j.version@`
    are also stamped from the build.
  - `flink-streaming-java` and `flink-clients` are declared at `provided` scope (they ship in
    Flink's `lib/`, so user fat jars must not duplicate them).
  - `log4j-api`, `log4j-core`, `log4j-slf4j-impl` at `runtime` scope (so they're available in the
    IDE but excluded from the assembled fat jar).
  - `maven-shade-plugin` 3.1.1 is preconfigured to build a fat jar with `mainClass = ${package}.DataStreamJob`,
    excluding `flink-shaded-force-shading`, `jsr305`, `org.slf4j:*`, and `org.apache.logging.log4j:*`
    from the user's shaded output.
  - `META-INF/*.SF`, `*.DSA`, `*.RSA` are filtered out of the shaded jar to avoid the classic
    "Invalid signature file digest for Manifest main attributes" SecurityException.
  - The Apache snapshots repository is enabled so users can pull `*-SNAPSHOT` Flink builds.

- **`archetype-resources/src/main/java/DataStreamJob.java`** — a minimal job that constructs a
  `StreamExecutionEnvironment` and calls `env.execute(...)`. Comments point at `env.fromSequence(1,
  10)` as a starter source and at the official tutorials.

- **`archetype-resources/src/main/resources/log4j2.properties`** — basic console appender suitable
  for IDE runs; mirrors the structure of the cluster `log4j.properties` in `flink-dist/conf/`.

## Pitfalls & gotchas

- **Two layers of substitution.** The archetype mechanism uses `${...}` placeholders, and the
  Flink build uses `@...@` placeholders (forced by the `maven-resources-plugin` config in the
  parent). Mixing them up — e.g. writing `${flink.version}` in a template expecting it to be
  expanded at build time — is the most common bug. `${flink.version}` is correct *inside the
  generated POM* because at archetype-generation time it expands to `@project.version@`'s value,
  which Maven Resources already substituted to the real version.

- **`flink-streaming-java` is at `provided` scope** in the generated POM. Users who try to run
  their job's fat jar standalone (i.e. not via `bin/flink run`) will get
  `NoClassDefFoundError`. This is by design — the Flink cluster provides those classes from
  `lib/flink-dist-<version>.jar`.

- **Connectors must be `compile` scope** in the user's POM. The template's comment is explicit
  about this; adding a Kafka connector at `provided` will compile fine and then fail at runtime
  in the cluster (connectors are not in `flink-dist`'s `lib/`).

- **`packaging=maven-archetype`** requires the `archetype-packaging` Maven extension declared in
  the parent POM. A user building from source with an old Maven that doesn't pick this up will
  see "Unknown packaging: maven-archetype."

- **Scala archetype is gone.** Don't add a Scala dependency back here without first revisiting
  the project-wide Scala-deprecation plan. The archetype produced would not be aligned with the
  Scala-free direction of Flink 2.x.

- **Java compiler version**: the template uses `target.java.version` interpolated at build
  time. If the root Flink build targets Java 11 but the user's machine only has Java 8, the
  generated project will fail with "invalid target release."

- **Integration test (`src/test/resources/projects/testArtifact/`)** generates a project with
  `groupId=org.apache.flink.archetypetest`, `artifactId=testArtifact`, `version=0.1`. Touching
  this without re-running `mvn verify` on the `flink-quickstart-java` module is risky — the
  archetype IT is one of the few signals that template substitution still works end-to-end.

## Related modules / docs

- `flink-dist/` — produces the distribution that the generated user job is meant to run inside.
  The archetype's `provided`-scope dependencies (`flink-streaming-java`, `flink-clients`) come
  from `flink-dist/lib/flink-dist-<version>.jar` at runtime.
- `flink-streaming-java/` — the API the generated `DataStreamJob.java` imports.
- `flink-clients/` — required for `env.execute(...)` to submit to a cluster from outside.
- Flink website's "Project Setup" / "DataStream Tutorial" pages, which are the primary
  documentation consumers of this module.
