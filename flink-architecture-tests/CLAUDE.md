# `flink-architecture-tests`

## Purpose

Aggregator (Maven `packaging=pom`) for Flink's
[ArchUnit](https://www.archunit.org/)-based architectural test suite.
Architecture tests run as ordinary JUnit 5 tests but inspect the
compiled bytecode of Flink modules with ArchUnit and assert structural
invariants — for example *"every class under `org.apache.flink..api..`
must carry one of `@Public/@PublicEvolving/@Experimental/@Internal`"*,
*"connector packages may only depend on `@Public`/`@PublicEvolving`
types"*, or *"every `*ITCase` test must register a `MiniClusterExtension`."*

The whole thing exists because Flink has accumulated a large public-API
surface and a long-running JUnit 4 → 5 migration; without machine-checked
architectural rules these constraints decay. Existing violations are
*frozen* (recorded in per-module `archunit-violations/` stores) so that
new violations fail the build but legacy ones don't block CI.

## Where it fits in the Flink stack

```
+-----------------------------------+
|       flink-architecture-tests    |   (parent pom, packaging=pom)
+----+----------------+-------------+
     |                |             |
     v                v             v
 -base          -production       -test
 (ArchUnit       (production       (test-code rules:
  helpers,        bytecode          MiniCluster usage,
  Java-only       rules: API        no JUnit4, ITCase
  predicates,     annotations,      naming)
  import opts)    table API,
                  connectors,
                  checkpoint cfg)
                       ^                       ^
                       |                       |
                  compile-dep              test-dep
                       |                       |
                  +----+----+         +--------+--------+
                  | central |         | per-module test |
                  |  test   |         |  classes        |
                  |  in     |         | (e.g.           |
                  | -prod   |         | flink-core,     |
                  +---------+         | flink-csv, ...) |
                                       +-----------------+
```

- **Consumed by:** the parent declares deps in `dependencyManagement`, so
  it is "consumed" indirectly — sub-modules' jars are pulled into other
  modules as `<scope>test</scope>` deps. About **24** Flink modules in this
  branch depend on `flink-architecture-tests-test` (every module that runs
  its own test-code architecture test).
- **Production-side rules (`-production`)** live *centrally*: a single
  `ArchitectureTest` in the `-production` module analyzes the entire
  `org.apache.flink..` classpath. This is faster than running the rules
  per-module because the bytecode is loaded once.
- **Test-side rules (`-test`)** are run *per-module* — each consuming
  module writes its own `TestCodeArchitectureTest` that pulls in the
  shared rules via `ArchTests.in(TestCodeArchitectureTestBase.class)` and
  ships its own `archunit-violations/` store.

## Maven coordinates

- **Group:** `org.apache.flink`
- **Artifact:** `flink-architecture-tests`
- **Packaging:** `pom`
- **Version:** inherits `2.3-SNAPSHOT` from `flink-parent`
- **Sub-modules:**
  - `flink-architecture-tests-base` (jar) — shared ArchUnit helpers,
    Java-only predicates, import options. No Flink deps.
  - `flink-architecture-tests-production` (jar) — central production-code
    rules; compiles against the modules it tests (`flink-core`,
    `flink-runtime`, `flink-table-*`, `flink-sql-*`, `flink-connector-*`,
    `flink-model-openai`).
  - `flink-architecture-tests-test` (jar) — central test-code rule
    *definitions*; each consuming module supplies its own test class to
    actually execute them.

## Dependencies

### Parent `pom.xml`

No `<dependencies>`. The parent uses `<dependencyManagement>` to pin
versions of the Flink modules under test (so sub-modules don't have to
specify them):

- `flink-architecture-tests-base` (test scope, used by other -tests modules)
- `flink-annotations` (test scope)
- "Tested Flink modules" (all `scope=test`): `flink-core`, `flink-runtime`,
  `flink-table-common`, `flink-table-api-java`, `flink-table-api-java-bridge`,
  `flink-table-code-splitter`, `flink-table-runtime`,
  `flink-table-planner_${scala.binary.version}`, `flink-sql-client`,
  `flink-sql-gateway-api`, `flink-sql-gateway`, `flink-connector-base`,
  `flink-connector-files`, `flink-connector-datagen`, `flink-model-openai`,
  `flink-test-utils`.

### Sub-module direct deps

- **`-base`** — `com.tngtech.archunit:archunit` (compile). That's it.
- **`-production`** — `flink-architecture-tests-base`, `flink-annotations`,
  ArchUnit + `archunit-junit5`, `junit-jupiter`, plus *compile-scope* deps
  on every Flink module whose bytecode it inspects (`flink-core`,
  `flink-runtime`, `flink-table-*`, `flink-sql-*`, `flink-connector-base/
  -files/-datagen`, `flink-model-openai`).
- **`-test`** — `flink-architecture-tests-base`, ArchUnit +
  `archunit-junit5`, `junit-jupiter`, `junit-vintage-engine` (still needed
  because some downstream modules continue to run JUnit 4 tests during
  the migration).

### Used by

- `<parent>`-level: `pom.xml` (root) registers the aggregator.
- `flink-architecture-tests-test` is declared as `test`-scope dependency
  in ~24 sub-modules:
  - `flink-core`, `flink-connector-base`, `flink-connector-files`,
    `flink-connector-datagen-test`, `flink-hadoop-compatibility`,
    `flink-table-planner` and every `flink-formats/*` (avro, parquet,
    orc, json, csv, compress, sequence-file, hadoop-bulk,
    avro-confluent-registry), plus every `flink-filesystems/*`
    (s3-fs-hadoop/-presto/-base, gs-fs-hadoop, oss-fs-hadoop,
    azure-fs-hadoop, hadoop-fs).
- `flink-architecture-tests-production` is only consumed *internally* —
  its own `src/test/java/.../ArchitectureTest.java` runs all the rules.

## Source layout

```
flink-architecture-tests/
├── README.md                      # overarching docs (freeze, refreeze, Scala caveats)
├── pom.xml                        # aggregator
├── flink-architecture-tests-base/
│   ├── pom.xml
│   └── src/main/java/org/apache/flink/architecture/common/
│       ├── Conditions.java            # ArchCondition factories (fulfill, haveLeafTypes, ...)
│       ├── GivenJavaClasses.java      # classes()/noClasses() but excluding Scala
│       ├── ImportOptions.java         # MavenMainClassesOnly, ExcludeScala, ExcludeShaded
│       ├── JavaFieldPredicates.java   # JavaField-level helpers
│       ├── Predicates.java            # JavaClass-level helpers, annotation matchers, exactlyOneOf, ...
│       └── SourcePredicates.java      # isJavaClass / areJavaClasses (file-suffix sniff)
├── flink-architecture-tests-production/
│   ├── pom.xml
│   ├── README.md
│   ├── archunit-violations/           # frozen-rule violation stores (stored.rules + UUID files)
│   ├── src/main/java/org/apache/flink/architecture/
│   │   ├── ProductionCodeArchitectureBase.java       # the @ArchTest aggregator
│   │   └── rules/
│   │       ├── ApiAnnotationRules.java               # @Public/@PublicEvolving/@Internal correctness
│   │       ├── TableApiRules.java                    # ConfigOption locations, FactoryUtil, ...
│   │       ├── ConnectorRules.java                   # connector pkgs → only @Public deps
│   │       └── CheckpointingConfigurationAccessRules.java  # forbid raw cfg.get(CHECKPOINTING_*)
│   └── src/test/
│       ├── java/.../ArchitectureTest.java            # @AnalyzeClasses(packages="org.apache.flink")
│       └── resources/archunit.properties             # freeze.store.* knobs
└── flink-architecture-tests-test/
    ├── pom.xml
    ├── README.md
    └── src/main/java/org/apache/flink/architecture/
        ├── TestCodeArchitectureTestBase.java         # shared @ArchTest aggregator (currently ITCase only)
        └── rules/
            ├── ITCaseRules.java                      # ITCase naming + MiniCluster registration
            └── BanJunit4Rules.java                   # noClasses().dependOn("org.junit" or "junit")
```

## Architecture & key concepts

### ArchUnit, freeze, and violation stores

The build uses **`FreezingArchRule.freeze(rule)`** to wrap every
production rule. ArchUnit records the *current* violations in a store
file (e.g. `flink-core/archunit-violations/<uuid>` plus
`archunit-violations/stored.rules`) and only fails the build on *new*
violations. The key files:

- `archunit.properties` (per `src/test/resources/`):
  - `freeze.store.default.allowStoreUpdate=true` — allow the store to
    drop violations as they are fixed.
  - `freeze.store.default.allowStoreCreation=true` — *disabled by
    default*; turn on only when introducing a new rule, otherwise new
    rules' violations are silently ignored.
  - `freeze.refreeze=true` — *disabled by default*; turn on to overwrite
    the store with the current set of violations (use sparingly).
  - `freeze.store.default.path=archunit-violations`.
- `archunit-violations/stored.rules` is a `key=uuid` map from the rule's
  description to a UUID file under the same directory that lists the
  fully-qualified violator names. The seven currently-frozen
  production rules are (paraphrased):
  1. Classes in API packages should have at least one API visibility annotation.
  2. Public method return / argument types must be `@Public`.
  3. `@PublicEvolving` method return / argument types must be `@Public(Evolving)`.
  4. Production code must not call methods annotated with `@VisibleForTesting`.
  5. `ConfigOption` fields in `org.apache.flink.table..` must live in classes whose simple name ends with `Options` (or in `FactoryUtil`).
  6. Options for connectors/formats should reside in a consistent package and be public API.
  7. Connector production code must only depend on public API outside connector packages.

### Java vs Scala

ArchUnit *technically* handles JVM bytecode, but Scala-generated classes
produce false positives (synthetic accessors, traits with companion
objects, …). The base module ships:

- `ImportOptions.ExcludeScalaImportOption` — drops anything under
  `.../scala/...` directories during ArchUnit's classpath import.
- `SourcePredicates.areJavaClasses()` — best-effort run-time filter
  (looks at the source file extension via `JavaClass.getSource()` and
  the package; falls back to "no Scala marker").
- `GivenJavaClasses` — the convention for *every* rule: instead of
  `classes()` write `javaClassesThat()` so Scala classes are filtered
  before any predicate is applied.

This is documented in the parent README's "How do I test Scala classes?"
section: "ArchUnit does not work well with Scala classes. All rules
should exclude non-Java classes."

### Production rules (`-production`)

`ProductionCodeArchitectureBase` is a JUnit 5 *test container*:

```java
public class ProductionCodeArchitectureBase {
    @ArchTest public static final ArchTests API_ANNOTATIONS  = ArchTests.in(ApiAnnotationRules.class);
    @ArchTest public static final ArchTests TABLE_API        = ArchTests.in(TableApiRules.class);
    @ArchTest public static final ArchTests CONNECTORS       = ArchTests.in(ConnectorRules.class);
    @ArchTest public static final ArchTests CONFIGURATION_ACCESS =
            ArchTests.in(CheckpointingConfigurationAccessRules.class);
}
```

`ArchitectureTest` (`src/test/java/...`) is the actual JUnit entry
point. Its `@AnalyzeClasses` annotation tells ArchUnit:

- import everything in `org.apache.flink`,
- exclude test classes (`ImportOption.DoNotIncludeTests`),
- exclude Scala (`ExcludeScalaImportOption`),
- exclude shaded code (`ExcludeShadedImportOption`).

surefire is configured with `forkCount=1` and `-Xmx${flink.XmxMax}`
because ArchUnit loads every Flink class into memory at once (lots of
classes → big heap).

### Rule modules at a glance

- **`ApiAnnotationRules`** — six rules. They enforce that every API class
  is annotated, that `@Public` methods don't leak `@PublicEvolving`/
  `@Internal` types, that production code doesn't call
  `@VisibleForTesting`, etc.
- **`TableApiRules`** — `ConfigOption` location convention (`*Options`
  class or `FactoryUtil`), plus other Table-API-specific structural
  rules.
- **`ConnectorRules`** — code under `org.apache.flink.connector..` and
  `org.apache.flink.streaming.connectors..` may only refer to
  `@Public`/`@PublicEvolving` Flink classes outside the connector
  package (or to anything under `org.apache.flink.util..`, or to
  non-Flink code).
- **`CheckpointingConfigurationAccessRules`** — bans direct
  `Configuration.get(CheckpointingOptions.X)` for a hardcoded set of
  options that have validation helpers (`CheckpointConfig`,
  `ReadableConfig.get*` wrappers) — production code must go through the
  helpers.

### Test-code rules (`-test`)

These rules are imported *by each module under test*. The shared rules
live in:

- **`ITCaseRules`** — two rules:
  - `INTEGRATION_TEST_ENDING_WITH_ITCASE` — every non-abstract subclass
    of `org.apache.flink.test.util.AbstractTestBase` must end in
    `ITCase`.
  - `ITCASE_USE_MINICLUSTER` — every `*ITCase` top-level non-abstract
    class must register a `MiniClusterExtension` (JUnit 5), or an
    `InternalMiniClusterExtension` (only for `flink-runtime`), or a
    `MiniClusterWithClientResource` `@Rule`/`@ClassRule` (legacy JUnit
    4). The detection uses `containAnyFieldsInClassHierarchyThat(...)`
    helpers from `Predicates`.
- **`BanJunit4Rules`** — `NO_NEW_ADDED_JUNIT4_TEST_RULE`: no class under
  `org.apache.flink..` may depend on anything in package `junit` or
  `org.junit`. **Not** included by default in
  `TestCodeArchitectureTestBase`; modules opt in once their JUnit 5
  migration is complete.

`TestCodeArchitectureTestBase` itself is currently minimal:

```java
public class TestCodeArchitectureTestBase {
    @ArchTest public static final ArchTests ITCASE = ArchTests.in(ITCaseRules.class);
}
```

Per-module setup (recipe from `flink-architecture-tests-test/README.md`):

1. Create `src/test/resources/archunit.properties` and
   `log4j2-test.properties` (templates in the `-test` submodule).
2. Add a class under `org.apache.flink.architecture.TestCodeArchitectureTest`
   in the module's test sources.
3. Include the common tests:
   `@ArchTest public static final ArchTests COMMON_TESTS = ArchTests.in(TestCodeArchitectureTestBase.class);`
4. Optionally add module-specific rules in
   `org.apache.flink.architecture.rules.*`.

## Important public APIs

- **`org.apache.flink.architecture.common.GivenJavaClasses`** —
  `javaClassesThat()`, `noJavaClassesThat()`. The *only* correct way to
  start an ArchUnit rule in this project (avoids Scala false positives).
- **`org.apache.flink.architecture.common.Predicates`** —
  `areDirectlyAnnotatedWithAtLeastOneOf(annotations...)`,
  `arePublicStaticFinalOfTypeWithAnnotation(fqType, annotation)`,
  `containAnyFieldsInClassHierarchyThat(...)`, `exactlyOneOf(...)`,
  `getClassSimpleNameFromFqName(...)`. Note the JavaDoc tip: *prefer
  FQN strings over `Class<?>` literals* to avoid forcing the build to
  reach into untested modules.
- **`org.apache.flink.architecture.common.Conditions`** — `fulfill(predicate)`,
  `haveLeafTypes(predicate)` (recurses into generic parameter types).
- **`org.apache.flink.architecture.common.ImportOptions`** —
  `MavenMainClassesOnly`, `ExcludeScalaImportOption`,
  `ExcludeShadedImportOption`.
- **`ProductionCodeArchitectureBase` / `TestCodeArchitectureTestBase`** —
  the JUnit 5 test containers; downstream modules embed them via
  `@ArchTest public static final ArchTests X = ArchTests.in(...)`.

## Internal flows (if applicable)

### Adding or changing a rule

1. Implement the rule in `flink-architecture-tests-production/.../rules/`
   or `flink-architecture-tests-test/.../rules/`.
2. Wrap it in `FreezingArchRule.freeze(...)`.
3. Give it a fixed description via `.as("...")`. **Don't** rely on
   ArchUnit's auto-generated description — changing it generates a new
   store key and orphans the old one.
4. Enable `freeze.store.default.allowStoreCreation=true` *temporarily*
   in `archunit.properties`, run the tests, then revert the flag.
5. Commit the new store files under `archunit-violations/`.

### Reproducing CI freeze state

```bash
rm -rf `find . -type d -name archunit-violations`
mvn test -Dtest="*TestCodeArchitectureTest*" \
         -DfailIfNoTests=false \
         -Darchunit.freeze.refreeze=true \
         -Darchunit.freeze.store.default.allowStoreCreation=true \
         -Dfast
```

This is the recipe from `flink-architecture-tests-test/README.md` for
regenerating *all* test-code violation stores after a rule change.

### Production-code run

`mvn -pl flink-architecture-tests/flink-architecture-tests-production test`
runs `ArchitectureTest`, which loads `org.apache.flink..` (minus test /
Scala / shaded), then runs ~12 `@ArchTest` rules across the four rule
classes. With ~50k Flink classes in memory the JVM heap usage is high
— the `surefire-plugin` config sets `-Xmx${flink.XmxMax}` (typically
2g+ in CI).

## Tests

- `flink-architecture-tests-base/src/test/java/PredicatesTest.java` —
  unit tests for the predicate helpers (note: in the *default* package,
  not under `org.apache.flink`).
- `flink-architecture-tests-production/src/test/java/.../ArchitectureTest.java`
  — the only "real" architectural test runner. Tagged
  `@ArchTag("FailsOnJava11")` and `@ArchTag("FailsOnJava17")` on
  `ConnectorRules.CONNECTOR_CLASSES_ONLY_DEPEND_ON_PUBLIC_API`, meaning
  CI skips it on those JDKs (a known limitation around bytecode
  features ArchUnit can't yet parse on newer JDKs).
- `flink-architecture-tests-test` has **no tests of its own** — it's a
  rule library. Downstream modules execute the rules through their own
  `TestCodeArchitectureTest` classes (e.g.
  `flink-core/src/test/java/org/apache/flink/architecture/TestCodeArchitectureTest.java`).

## Pitfalls & gotchas

- **Always use `javaClassesThat()` / `noJavaClassesThat()`** — never
  ArchUnit's bare `classes()` / `noClasses()`. Scala compiles to JVM
  bytecode that breaks several rules in subtle ways.
- **Pin rule descriptions with `.as(...)`.** The description is the key
  into the violation store. If you let ArchUnit auto-generate the
  description and then change the rule (even cosmetically), the key
  changes and your old violations vanish from the store, hiding
  regressions.
- **Don't enable `freeze.refreeze=true` in committed
  `archunit.properties`.** It silently writes the current state of
  violations to the store — accepting them as "expected". Use it only
  locally and revert.
- **Adding a new violation should be a last resort.** From the README:
  *"New violations should be avoided at all costs."* Try to fix the
  underlying code; only freeze if the rule itself is wrong (open a
  JIRA).
- **Heap usage.** ArchUnit imports the entire `org.apache.flink..`
  classpath into memory. Don't add `-Xmx` overrides locally lower than
  what CI uses, or you'll get spurious `OutOfMemoryError`s.
- **Module dependency loops.** `Predicates` JavaDoc warns: prefer
  FQN-string predicates (`isAssignableTo("org.apache.flink.test.util.AbstractTestBase")`)
  over `Class<?>` literals. Using a literal forces this module to
  compile against the literal's owning module, which can create cycles
  (and in the test-code submodule, you usually don't *want* a hard
  compile dep on every module you'll inspect).
- **`flink-architecture-tests-test` includes `junit-vintage-engine`.**
  That looks like a bug (the module exists to ban JUnit 4!) but it's
  deliberate: downstream modules still on JUnit 4 must be able to load
  the rule classes. Don't remove it until the JUnit 4 → 5 migration is
  finished.
- **Per-module `archunit-violations/`** directories live under the
  *consuming* module (e.g. `flink-core/archunit-violations/`), not
  under `flink-architecture-tests/`. Be careful when grepping for
  violations.
- **`ConnectorRules` is JDK-sensitive.** Tagged
  `FailsOnJava11`/`FailsOnJava17` — if you add new rules of the same
  shape (deep dependency analysis), test them on the JDK matrix.

## Related modules / docs

- `flink-annotations/CLAUDE.md` — defines the `@Public`/`@PublicEvolving`/
  `@Internal`/`@Experimental`/`@VisibleForTesting` annotations that
  `ApiAnnotationRules` and `ConnectorRules` reflect on.
- `flink-core-api/CLAUDE.md` — every class in its `org.apache.flink..api..`
  packages is checked by `ApiAnnotationRules.ANNOTATED_APIS`.
- `flink-architecture-tests/README.md` — top-level operational guide
  (freeze, refreeze, Scala caveats).
- `flink-architecture-tests/flink-architecture-tests-production/README.md`
  — quick-start for adding production rules.
- `flink-architecture-tests/flink-architecture-tests-test/README.md` —
  step-by-step recipe for plugging a new submodule into the test-code
  architecture suite.
- No `codedocs/*.md` deep dive yet for architectural tests.
