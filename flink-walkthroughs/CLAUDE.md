# `flink-walkthroughs`

## Purpose

Aggregator (Maven `packaging=pom`) for the "guided walkthrough" projects
that the Flink documentation links to from its **Try Flink** tutorials.
Unlike `flink-examples` — which produces ready-to-run JARs in the binary
tarball — the walkthroughs are packaged as **Maven archetypes**: a user
runs `mvn archetype:generate -DarchetypeGroupId=org.apache.flink
-DarchetypeArtifactId=flink-walkthrough-datastream-java …` and gets a
fresh standalone project skeleton on disk that they can extend as they
follow the tutorial.

Two archetypes are published today: a DataStream-API skeleton (fraud
detection on credit-card transactions) and a Table-API skeleton (hourly
spend report on the same transactions). Both share a small library of
runtime helpers (`Transaction` entity, generated `TransactionSource`,
`AlertSink`) that the generated projects pull in as a regular runtime
dependency.

## Where it fits

- Sits next to `flink-examples` in the reactor; depends only on a small
  slice of the streaming + datagen API surface (mostly `provided`).
- Consumed by **end users via Maven Central** as archetype artifacts —
  no other Flink module depends on the walkthrough archetypes.
- The `flink-walkthrough-common` jar is the only deployable
  Flink-internal artifact and is what users' generated projects actually
  link against at runtime.

## Maven coordinates

Parent: `org.apache.flink:flink-walkthroughs:2.3-SNAPSHOT` (pom).

Sub-modules declared in `pom.xml`:

```xml
<modules>
    <module>flink-walkthrough-common</module>
    <module>flink-walkthrough-datastream-java</module>
    <module>flink-walkthrough-table-java</module>
</modules>
```

- **`flink-walkthrough-common`** — regular jar (`packaging=jar`).
  Shared runtime classes used by both archetypes: `Transaction` entity,
  `Alert` entity, `TransactionSource` (FLIP-27 `DataGeneratorSource` at
  10 records/sec cycling 50 fixed transactions across 5 account IDs),
  `AlertSink` (logger-backed), `LoggerOutputFormat`.
- **`flink-walkthrough-datastream-java`** — `packaging=maven-archetype`.
  Generates a project containing `FraudDetectionJob` (wires source →
  `keyBy(accountId)` → `KeyedProcessFunction` → `AlertSink`) and a
  skeleton `FraudDetector` that the user fills in.
- **`flink-walkthrough-table-java`** — `packaging=maven-archetype`.
  Generates a `SpendReport` Table-API job (datagen → tumbling hour
  aggregation by accountId) plus a `SpendReportTest` (batch-mode unit
  test) and a small `MyFloor` UDF stub.

## Dependencies

### Direct (parent pom)

The parent pom contributes **no dependencies**. It only wires:

- `maven-archetype-plugin` (registered via the `archetype-packaging`
  build extension), pluginManagement-only (the plugin itself is `skip`'d
  at the aggregator level — each archetype module enables it via its
  `<packaging>maven-archetype</packaging>`).
- `maven-shade-plugin` deactivated for archetypes (they ship sources,
  not fat JARs).
- `maven-resources-plugin` configured to use **`@…@` delimiters** for
  resource filtering (rather than the default `${…}`). This lets
  archetype templates keep literal `${groupId}` / `${package}` Velocity
  placeholders in their resource files while still letting the build
  substitute things like `@project.version@` and `@log4j.version@`
  into `archetype-resources/pom.xml`.

### Per sub-module

- `flink-walkthrough-common` → `flink-streaming-java` (`provided`),
  `flink-connector-datagen` (`provided`).
- `flink-walkthrough-{datastream,table}-java` → no Maven deps on their
  own (archetype packages). Their `archetype-resources/pom.xml` (the
  pom the *user's generated* project gets) pulls
  `flink-walkthrough-common`, `flink-streaming-java` (or the
  `flink-table-api-java` / `flink-table-planner-loader` /
  `flink-table-runtime` trio), `flink-clients`,
  `flink-connector-datagen`, and log4j.

### Used by

End users via `mvn archetype:generate …`; the Flink website's
**Try Flink** tutorials link directly to these archetypes.

## Source layout

```
flink-walkthroughs/
├── pom.xml                              ← aggregator (this dir)
├── flink-walkthrough-common/
│   └── src/main/java/org/apache/flink/walkthrough/common/
│       ├── entity/Transaction.java      ← POJO: accountId, timestamp, amount
│       ├── entity/Alert.java            ← POJO: id
│       ├── source/TransactionSource.java← FLIP-27 DataGeneratorSource @10 rps
│       ├── sink/AlertSink.java          ← Logger-backed SinkFunction
│       └── sink/LoggerOutputFormat.java
├── flink-walkthrough-datastream-java/
│   └── src/main/resources/
│       ├── META-INF/maven/archetype-metadata.xml   ← fileset descriptor
│       └── archetype-resources/                    ← TEMPLATE the user gets
│           ├── pom.xml                             ← shade fat-jar w/ FraudDetectionJob as main
│           └── src/main/java/{FraudDetectionJob,FraudDetector}.java
└── flink-walkthrough-table-java/
    └── src/main/resources/
        ├── META-INF/maven/archetype-metadata.xml
        └── archetype-resources/
            ├── pom.xml
            └── src/{main,test}/java/{SpendReport,MyFloor,SpendReportTest}.java
```

`archetype-metadata.xml` (per archetype) declares two filesets:
`src/main/java/**` is **filtered** + **packaged** (Velocity expands
`${package}`, `${groupId}`, `${artifactId}` into user-supplied values),
while `src/main/resources/**` is copied verbatim (so log4j2.properties
isn't touched).

## Architecture & key concepts

- **Archetype packaging**: Both archetype modules use
  `<packaging>maven-archetype</packaging>` (enabled by the
  `archetype-packaging` build extension in the parent pom). The
  `archetype-resources/` tree becomes an archetype jar that
  `maven-archetype-plugin` expands at `archetype:generate` time.
- **Two-layer poms — easy to edit the wrong one**: The pom the user
  *sees* (in `archetype-resources/pom.xml`) is different from the
  module-build pom. The build-side pom is essentially empty; the
  archetype-resources pom is a full project pom (shade plugin, log4j,
  Flink deps) that becomes the user's project.
- **`@…@` delimiters**: Resource filtering uses `@project.version@` /
  `@log4j.version@` instead of `${…}` so Velocity placeholders
  (`${groupId}`, `${artifactId}`, `${package}`, `${version}`) in
  `archetype-resources/` survive Maven and are expanded later by the
  archetype plugin.
- **Shared runtime helpers**: `flink-walkthrough-common` is the only
  *real* Flink module — it bundles the dummy `TransactionSource` (no
  Kafka required) and the SLF4J-backed `AlertSink`.
- **DataStream archetype = process-function tutorial**:
  `FraudDetectionJob` wires the pipeline; `FraudDetector` is a
  `KeyedProcessFunction<Long, Transaction, Alert>` skeleton the user
  fills in across tutorial steps (small→large flag, timer cleanup).
- **Table archetype = windowed aggregation tutorial**:
  `SpendReport.report(…)` is the unit-testable function (tumbling hour
  group-by). The generated project ships a JUnit 5 batch-mode test.

## Important public APIs / example programs

- `org.apache.flink.walkthrough.common.entity.Transaction` — fields
  `accountId` (`long`), `timestamp` (`long` epoch millis), `amount`
  (`double`). Plain POJO + standard constructor/getters/setters.
- `org.apache.flink.walkthrough.common.entity.Alert` — single `id` field.
- `org.apache.flink.walkthrough.common.source.TransactionSource.unbounded()`
  returns a configured `DataGeneratorSource<Transaction>` running at 10
  rps. The generator cycles 50 fixed transactions; one account (`3`)
  has a small-then-large pattern that triggers fraud alerts when the
  tutorial's logic is correct.
- `org.apache.flink.walkthrough.common.sink.AlertSink` — legacy
  `SinkFunction<Alert>` that logs via SLF4J at INFO level.
- The two `main(…)` entry points: `${package}.FraudDetectionJob` and
  `${package}.SpendReport` (where `${package}` is the user-supplied
  groupId / artifactId combo at archetype-generation time).

## How to run

```bash
# Build + install the archetypes locally
mvn -pl flink-walkthroughs -am install -DskipTests

# Generate a user project (DataStream archetype)
mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-java \
    -DarchetypeVersion=2.3-SNAPSHOT \
    -DgroupId=frauddetection -DartifactId=frauddetection \
    -Dversion=0.1 -Dpackage=spendreport -DinteractiveMode=false
```

Swap `flink-walkthrough-datastream-java` for
`flink-walkthrough-table-java` to generate the Table variant. The
generated project runs via `mvn package` (fat JAR) +
`flink run target/<artifact>-<version>.jar`, or directly in the IDE —
IntelliJ requires *Modify options > include dependencies with
"Provided" scope* (Javadoc in `FraudDetectionJob` / `SpendReport`
mentions this).

## Tests

- `flink-walkthrough-common` has **no tests** in this tree.
- The archetype modules have **no tests of the archetype itself** —
  they're shipped templates, not runnable code from the reactor's
  perspective. The only test under either archetype is the one *inside*
  `archetype-resources/`: `SpendReportTest` (JUnit 5, batch
  `TableEnvironment`, asserts `SpendReport.report(…)` against a small
  fixed input). It runs only after a user generates a project from the
  archetype.

If you need to validate an archetype change locally, generate a project
into a scratch directory (see "How to run") and `mvn verify` there.

## Pitfalls & gotchas

- **Two `pom.xml` per archetype** — changes to the *user's* generated
  project must go in `<module>/src/main/resources/archetype-resources/pom.xml`,
  not the top-level `<module>/pom.xml`.
- **Resource filter delimiters are `@…@`, not `${…}`.** Using
  `${project.version}` in an archetype-resources file at archetype
  build time silently fails; use `@project.version@`. The `${groupId}`,
  `${package}` etc. placeholders are Velocity, expanded by
  `archetype:generate`.
- **`archetype-metadata.xml` `packaged="true"`** moves files under the
  user's package and substitutes `${package}` into the `package`
  declaration. Resource files are typically *not* packaged.
- **`TransactionSource` is infinite** — `env.execute(...)` only returns
  on cancel/failure.
- **`AlertSink` uses the legacy `SinkFunction`** (not FLIP-143 `Sink`)
  — deliberate for tutorial simplicity; no exactly-once semantics
  demonstrated.
- **`flink-table-planner-loader` is `provided` in the generated Table
  project** — running `SpendReport.main` from IntelliJ without
  *Modify options > include provided* causes `NoClassDefFoundError` on
  a planner class (explicit Javadoc note in `SpendReport`).
- **Version bumps**: both `archetype-resources/pom.xml` files need
  `flink.version` (via `@project.version@`) and possibly
  `target.java.version` (`11`) updated when bumping Flink.

## Related modules / docs

- **Upstream**: `flink-streaming-java`, `flink-table-api-java`,
  `flink-table-planner-loader`, `flink-table-runtime`,
  `flink-clients`, `flink-connector-datagen`.
- **Sibling**: `flink-examples` — fully-built, ready-to-run example JARs
  (analogous content but a different distribution model — the JARs ship
  in the binary tarball rather than as archetype templates).
- **Documentation**: The Apache Flink docs site's **Try Flink** section
  walks step-by-step through `FraudDetectionJob` / `FraudDetector` and
  `SpendReport`. Changes to method signatures or method names in
  `archetype-resources/src/main/java/*.java` will break the tutorial
  prose unless the docs are updated in lockstep.
- **Codedocs**:
  - `codedocs/flink-interview-fraud-detection.md` — extended treatment
    of the same fraud-detection pattern that the DataStream walkthrough
    introduces.
