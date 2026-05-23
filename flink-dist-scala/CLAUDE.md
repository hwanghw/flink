# `flink-dist-scala`

## Purpose

`flink-dist-scala` is the **Scala-runtime sidecar** for the Flink distribution. It produces a
single shaded jar containing the Scala standard library, the Scala reflection / compiler bits, the
Twitter `chill_<scala>` Kryo serializer, and any `org.apache.flink:*` classes that need to be
co-located with Scala — bundled as one artifact so that they can be dropped into the `lib/`
folder of the distribution as `flink-scala_<scala>-<version>.jar`.

The split exists because the main `flink-dist-<version>.jar` is deliberately **Scala-free**.
Flink's APIs have moved to Java; Scala support has been progressively de-emphasized across
1.15 → 2.x. By isolating Scala into a separate sidecar jar:

- The core uber jar has no Scala dependency and is Scala-version-agnostic.
- Users who do not need the Scala APIs (most users in 2.x) can simply delete one jar from `lib/`
  to drop Scala from their Flink installation.
- The build does not have to cross-publish the entire distribution per Scala binary version.

Note: this module is **not** the Scala REPL / Scala shell — that lived in `flink-scala-shell` in
older releases and has been removed. `flink-dist-scala` is now purely a "ship Scala stdlib +
chill" packaging concern.

## Where it fits

- Built before `flink-dist`. The `flink-dist/pom.xml` declares it at `provided` scope and
  `flink-dist/src/main/assemblies/bin.xml` copies
  `../flink-dist-scala/target/flink-dist-scala_<scala>-<version>.jar` into the tarball's `lib/`
  directory, renaming it to `flink-scala_<scala>-<version>.jar`.
- Its `dependency-reduced-pom.xml` (committed in the source tree alongside `pom.xml`) is the POM
  published to Maven Central with the shaded contents reflected.

## Maven coordinates

- groupId: `org.apache.flink`
- artifactId: `flink-dist-scala_${scala.binary.version}` (currently `_2.12` — the project's only
  supported binary Scala version, per the root POM's `scala.binary.version = 2.12` /
  `scala.version = 2.12.20`)
- packaging: `jar`
- `<japicmp.skip>true</japicmp.skip>` — no API-compatibility check; this is a packaging artifact,
  not an API surface.

## Dependencies

### Direct

- `org.scala-lang:scala-reflect` (bundled)
- `org.scala-lang:scala-library` (bundled)
- `org.scala-lang:scala-compiler` (bundled)
- `org.slf4j:slf4j-api` — *provided* (comes from `flink-dist`)
- `com.google.code.findbugs:jsr305` — *provided* (comes from `flink-dist`)

### Shade-plugin include list (artifactSet)

The `maven-shade-plugin` (execution `shade-flink`) is restricted to these include patterns — any
transitive dependency outside the list is **not** bundled:

```
org.apache.flink:*
org.scala-lang*:*
com.twitter:chill_${scala.binary.version}
```

The comment in the POM is explicit: "Transitive dependencies are bundled by either flink-dist(-scala),
and we need to make sure no dependency required for the Scala-free operation of Flink is bundled in
flink-dist-scala." In other words: the partition between `flink-dist` and `flink-dist-scala` must be
clean — duplicating, say, `slf4j-api` in both would lead to classloader headaches.

### Used by

- `flink-dist` (provided scope, then copied into the tarball as a plain file).
- No other internal module compiles against it; downstream Flink-Scala user code references the
  Scala library directly via their own POM, not this artifact.

## Layout of the produced artifact

A single jar at `target/flink-dist-scala_2.12-<version>.jar` containing roughly:

```
flink-dist-scala_2.12-<version>.jar
├── scala/                              (from scala-library)
├── scala/reflect/                      (from scala-reflect)
├── scala/tools/                        (from scala-compiler)
├── com/twitter/chill/                  (from chill_2.12, for Kryo + Scala serialization)
├── org/apache/flink/...                (any flink-* classes whose serialization paths need Scala)
└── META-INF/
    ├── NOTICE                          (from src/main/resources/META-INF/NOTICE)
    └── licenses/LICENSE.scala
```

The source tree is essentially empty of code — only `src/main/resources/META-INF/NOTICE` and
`src/main/resources/META-INF/licenses/LICENSE.scala`. The jar's contents come entirely from the
shade plugin's bundled dependencies.

## Pitfalls & gotchas

- **Don't bundle anything not on the include list.** The shade `<includes>` are an explicit
  allow-list. If a new Scala-flavored Flink dependency needs to ship in the Scala sidecar, add it
  to both the `<dependencies>` block *and* the shade `<includes>` block.
- **`dependency-reduced-pom.xml` is committed.** The shade plugin regenerates it during `package`.
  Diffs to that file in PRs are normal when dependency versions change.
- **Scala 2.13 is not built.** The root POM's `scala-2.12` profile is the only one available;
  attempts to build with `-Dscala-2.13` will fail because the build harness for it was removed.
- **The output is renamed.** `flink-dist-scala_2.12-<version>.jar` becomes
  `flink-scala_2.12-<version>.jar` once it lands in the distribution's `lib/`. When grepping a
  shipped Flink install for "where did the Scala lib come from?", the on-disk filename does not
  match the Maven artifactId.
- **Scala APIs are being deprecated.** The `flink-streaming-scala`, `flink-scala`, and Scala shell
  modules have been removed/deprecated in Flink 2.x. This module persists primarily to ship the
  Scala stdlib that the *Scala-flavored Table planner* (in `opt/flink-table-planner_2.12-*.jar`)
  and `chill` (used by Kryo serialization) still need at runtime. If/when those go fully
  Scala-free, this module can be deleted.
- **Removing this jar from `lib/`**: safe if user code does not use the Scala APIs and the
  Scala-flavored Table planner is not promoted from `opt/` to `lib/`. Otherwise you'll get
  `NoClassDefFoundError: scala/...` at job startup.

## Related modules / docs

- `flink-dist/` — the consumer; copies this jar into the tarball's `lib/` directory.
- `flink-table/flink-table-planner/` — the legacy Scala-bearing planner that *requires* this jar
  at runtime (shipped under `opt/`).
- `flink-table/flink-table-planner-loader/` — the modern Scala-free planner; users on
  planner-loader do not strictly need `flink-dist-scala` at runtime *unless* their user code uses
  Scala.
- Root `pom.xml` properties `scala.version` (`2.12.20`) and `scala.binary.version` (`2.12`)
  control which Scala lands here.
