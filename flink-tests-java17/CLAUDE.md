# `flink-tests-java17`

## Purpose

A tiny, focused module that holds Flink tests which **require JDK 17 source-level
language features** to even compile — primarily Java `record` types (and, in
the future, sealed classes / pattern matching). It exists as a separate Maven
module because the rest of the codebase still compiles against an older Java
source level; isolating the JDK 17 sources here keeps the global
`source.java.version` low while still letting CI exercise records.

## Where it fits — when developers would touch this module

- Adding a (de)serialization / type-extraction test for a Java `record`.
- Verifying that `PojoSerializer` migration to / from a record-based POJO works
  across Flink versions (the resources tree holds canned savepoints for that).
- Adding a Table API `DataTypes` extraction test that needs records (see
  `DataTypeExtractorJava17Test`).

If your test does **not** need Java 17 syntax, put it in `flink-tests`
instead — this module is intentionally narrow.

## Maven coordinates

```
groupId    = org.apache.flink
artifactId = flink-tests-java17
packaging  = jar
```

No sub-modules. Key pom property:

```xml
<source.java.version>17</source.java.version>
```

The `maven-compiler-plugin` is configured with `<source>17</source>`, and the
checkstyle suppression file is the dedicated
`tools/maven/suppressions-tests-java17.xml`.

## Dependencies

### Direct (all `test` scope)

- `flink-core` (main + test-jar) — `SerializerTestBase`, `PojoSerializer`,
  Kryo/Java-record helpers.
- `flink-table-common` (main + test-jar) — `DataTypes`, type-extraction tests.
- `flink-migration-test-utils` — `MigrationTest` infrastructure for the
  record-based pojo-serializer migration tests.
- `flink-test-utils-junit` — `TestLogger`, JUnit extensions, AssertJ.

### Used by

Nothing. This module is a leaf in the dependency graph. Its outputs are
test-classes only; it is not depended on by any other module.

## Source layout

```
src/
  test/
    java/org/apache/flink/
      api/java/typeutils/runtime/
        PojoRecordSerializerTest.java                 # serializer behavior for record-shaped POJOs
        PojoRecordSerializerUpgradeTest.java          # MigrationTest for record POJOs across Flink versions
        PojoRecordSerializerUpgradeTestSpecifications.java
        RecordBuilderFactoryTest.java                 # PojoSerializer's record-builder
      table/types/extraction/
        DataTypeExtractorJava17Test.java              # Table API type extraction for records
    resources/
      pojo-serializer-record-migration-1.19/          # canned 1.19 snapshots: record → record
      pojo-serializer-to-record-1.19/                 # canned 1.19 snapshots: pre-record POJO → record
```

There is no `src/main` — the module produces only test-classes.

## Architecture & key concepts

- **Why a separate module?** Most of Flink's source still targets a lower Java
  version. Putting record-using sources in the main `flink-tests` module would
  force a JDK-17 source level on everything that depends on it. A dedicated
  module with its own compiler-plugin override and checkstyle suppression is
  the cleanest workaround.
- **`PojoRecordSerializerUpgradeTest`** is the marquee test: it implements the
  `MigrationTest` contract from `flink-migration-test-utils`, snapshots state
  using a record-shaped POJO at a given Flink version, and verifies that
  subsequent versions can restore it. The reference snapshots live under
  `src/test/resources/pojo-serializer-record-migration-1.19/` and
  `pojo-serializer-to-record-1.19/`.
- **`DataTypeExtractorJava17Test`** verifies that the Table API's reflection
  pipeline correctly maps record components to `DataTypes` fields, with the
  correct nullability and field ordering driven by the canonical record header.

## Important public APIs

None. There are no `src/main` sources; this module exposes no public API. All
classes are `@Test`-annotated.

## How to run the tests

```sh
# Only meaningful when building with JDK 17+ (the module is active in the
# default Maven layout when JAVA_HOME points at a JDK 17 or later install).
mvn -pl flink-tests-java17 -am test
```

The tests will be silently skipped or fail-to-compile if the build is run on a
JDK older than 17.

## Tests

The five test files listed under "Source layout" are the entire test suite —
no helpers, no support classes.

## Pitfalls & gotchas

- **Do not add non-record tests here.** Anything that compiles under the lower
  default Java source level belongs in `flink-tests`. Keeping this module
  narrow is what makes the build matrix work.
- **Migration resources are append-only.** When a new Flink release ships, a
  new snapshot directory should be added — never edit or delete an existing
  one.
- **Checkstyle override.** This module uses
  `tools/maven/suppressions-tests-java17.xml`, not the global suppressions
  file. If a new lint rule needs an exception specific to record source code,
  add it there.
- **Java release / target mismatch.** Only `<source>` is overridden in the pom;
  the target bytecode level comes from the parent. If the project ever moves
  to multi-release jars, that mismatch is the first thing to revisit.

## Related modules / docs

- `flink-tests` — the main integration test module; non-record ITCases.
- `flink-test-utils-parent/flink-migration-test-utils` — the `MigrationTest`
  framework that the upgrade test plugs into.
- `tools/maven/suppressions-tests-java17.xml` — checkstyle suppressions
  scoped to this module only.
