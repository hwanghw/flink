# `flink-annotations`

## Purpose

Tiny "leaf" module that holds Flink's API-stability annotations
(`@Public`, `@PublicEvolving`, `@Experimental`, `@Internal`,
`@VisibleForTesting`), a few documentation-generator annotations, and the
`FlinkVersion` enum. It exists so that the most fundamental classifier
annotations (used to mark every public-facing type in the codebase) live in
a module with *zero* dependencies. Almost every other Flink module pulls
this jar in, so making it standalone keeps the dependency graph clean and
the jar small.

The annotations are runtime-retained (`RetentionPolicy.RUNTIME`), which lets
ArchUnit-based architecture rules (see `flink-architecture-tests`) and the
docs generator (`flink-docs`) inspect them via reflection.

## Where it fits in the Flink stack

```
                +-----------------------+
                |  flink-annotations    |  <-- this module, leaf
                +-----------+-----------+
                            ^
                            | (compile dep)
        +-------------------+--------------------+
        |       |          |        |            |
   flink-core-api  flink-core  flink-metrics-*  flink-architecture-tests-production
        ...
```

- **Consumed by:** essentially every Flink module that defines public types
  (≈ 20+ direct consumers in this checkout). The most important ones are
  `flink-core-api`, `flink-core`, the `flink-metrics-*` family, the
  `flink-table-*` modules, and `flink-architecture-tests-production`.
- **Depends on:** *nothing* outside the JDK. The pom declares no
  `<dependencies>` block at all.

## Maven coordinates

- **Group:** `org.apache.flink`
- **Artifact:** `flink-annotations`
- **Packaging:** `jar`
- **Version:** inherits `2.3-SNAPSHOT` from `flink-parent`
- **Sub-modules:** none

## Dependencies

### Direct (from `pom.xml`)

None. The pom is a bare parent reference plus `<artifactId>` /
`<packaging>` — deliberately so. Adding *any* dependency here would force
it into every Flink jar.

### Used by (subset)

Modules that declare `<artifactId>flink-annotations</artifactId>` directly
(roughly 20 in this branch):

- `flink-core-api` (compile)
- `flink-core` (compile)
- `flink-docs` (consumes the `@Documentation` annotations to render config tables)
- `flink-architecture-tests-production` (rules in `ApiAnnotationRules` reflect on these)
- `flink-architecture-tests` (parent pom only — declares the dep in `dependencyManagement`)
- `flink-format-common`, `flink-failure-enricher-test`
- `flink-metrics-core` and every reporter (`-jmx`, `-slf4j`, `-prometheus`,
  `-statsd`, `-graphite`, `-datadog`, `-dropwizard`)
- `flink-table-calcite-bridge`, `flink-sql-parser`, `flink-sql-gateway-api`,
  `flink-sql-jdbc-driver-bundle`
- `flink-migration-test-utils`
- `tools/ci/flink-ci-tools`

Transitively *every* Flink module ends up with this jar on its classpath
because `flink-core` pulls it in.

## Source layout

Only 10 source files. The whole module fits on one screen:

```
src/main/java/org/apache/flink/
├── FlinkVersion.java                       # major.minor version enum
└── annotation/
    ├── Public.java                         # binary-stable across minor releases
    ├── PublicEvolving.java                 # user-facing, may evolve
    ├── Experimental.java                   # may change/disappear
    ├── Internal.java                       # stable but Flink-internal
    ├── VisibleForTesting.java              # widen visibility purely for tests
    └── docs/
        ├── Documentation.java              # container for OverrideDefault / Section / TableOption / SuffixOption / ExcludeFromDocumentation
        ├── ConfigGroup.java                # groups ConfigOption fields into doc sections
        ├── ConfigGroups.java               # @Repeatable wrapper for @ConfigGroup
        └── FlinkJsonSchema.java            # marks classes whose JSON schema should be generated
```

No `src/test/java/`. Annotations are validated indirectly by
`flink-architecture-tests-production/ApiAnnotationRules`.

## Architecture & key concepts

### API stability annotations (the headline feature)

The four-level hierarchy mirrors Flink's compatibility promise. Compatibility
guarantees decrease as you go down:

| Annotation       | Audience  | Compatibility                                                   |
| ---------------- | --------- | --------------------------------------------------------------- |
| `@Public`        | end users | binary-stable across minor releases (1.0 → 1.1 → 1.2)           |
| `@PublicEvolving`| end users | semantically stable, signature may change between minor versions|
| `@Experimental`  | end users | no guarantees, may change or disappear at any time              |
| `@Internal`      | Flink devs| stable within Flink internals, can break across minor releases  |
| `@VisibleForTesting` | tests | widen access (e.g. method should morally be `private`)         |

Notes on the implementation:

- `@Public` itself is annotated with `@Public` (self-referential) — that's
  fine since it's a marker.
- `@VisibleForTesting` is `@Internal` (not `RUNTIME`-retained) — it is
  treated as a compile-time hint only and ArchUnit rules check it via
  source-level introspection.
- All but `VisibleForTesting` carry `RetentionPolicy.RUNTIME`; the docs
  generator and ArchUnit need them at runtime.

`@Internal` has the broadest `@Target` set: `TYPE, METHOD, CONSTRUCTOR,
FIELD`. `@Public` only targets `TYPE` (you cannot annotate a single method
as `@Public` because public methods inherit their class's stability).
`@PublicEvolving` and `@Experimental` target all four.

### `FlinkVersion`

Enum of every Flink version from `v1_3` through `v2_3`. Used by:

- migration test infrastructure to locate snapshot files on disk
  (`versionStr` is the human form `"1.3"` etc., and *must not change*);
- SQL / Table API upgrade tests;
- `byCode(String)` / `valueOf(int, int)` / `rangeOf(start, end)` helpers
  for filtering subsets of versions.

The enum **ordinal** is significant — `isNewerVersionThan` compares
ordinals. Adding a new version means appending it to the end.

### `annotation/docs/`

A grab-bag of `@Internal` annotations consumed by `flink-docs` to generate
the user-facing config tables and REST API docs:

- `Documentation.OverrideDefault` — show a different "default" in docs than
  the code uses (e.g. when a runtime value is computed but the doc default
  is a friendly string).
- `Documentation.Section` (with `Documentation.Sections` constants) —
  places a `ConfigOption` into one or more named sections such as
  `expert_rocksdb`, `common_memory`, `security_ssl`. Lower `position()`
  sorts earlier. Adding a new section requires adding a constant here and
  hooking it up in the docs generator.
- `Documentation.TableOption` — marks Table API config options with their
  `ExecMode` (BATCH / STREAMING / BATCH_STREAMING).
- `Documentation.SuffixOption` — for config options whose key is just a
  suffix; the prefix is injected at runtime (used heavily by metric
  reporter configs).
- `Documentation.ExcludeFromDocumentation` — hide a `ConfigOption` or REST
  message header from the rendered docs (optionally with a reason string).
- `ConfigGroup` / `ConfigGroups` — repeatable annotation that splits a
  large options class into multiple HTML pages keyed by `keyPrefix`.
- `FlinkJsonSchema.AdditionalFields` — declares that a JSON-typed class
  accepts dynamic extra fields of a given type, used by REST API JSON
  schema generation.

## Important public APIs

- **`@Public`, `@PublicEvolving`, `@Experimental`, `@Internal`,
  `@VisibleForTesting`** — see the table above. Every public-facing class
  in Flink should carry exactly one of these (the
  `ApiAnnotationRules.ANNOTATED_APIS` ArchUnit rule enforces it for classes
  under `org.apache.flink..api..`).
- **`FlinkVersion`** — use `FlinkVersion.current()` to get the version of
  the current branch; use `FlinkVersion.byCode("1.18")` to look up by
  string; use `FlinkVersion.rangeOf(v1_15, v1_18)` for ranges in migration
  tests.
- **`Documentation.Section` / `Sections`** — when adding a new
  `ConfigOption`, annotate it with `@Documentation.Section(...)` so it
  shows up in the right docs page.

## Internal flows (if applicable)

Not really applicable — the module is annotations only. The interesting
flow is *consumption*, which happens elsewhere:

1. `flink-architecture-tests-production/ApiAnnotationRules` loads classes
   under `org.apache.flink..api..`, asks ArchUnit for each class's
   annotations, and asserts `@Public / @PublicEvolving / @Experimental /
   @Internal / @Deprecated` presence and consistency of return / argument
   types.
2. `flink-docs` scans `ConfigOption` fields, reads their
   `@Documentation.*` annotations, and renders Markdown / HTML tables.
3. The `@VisibleForTesting` annotation is referenced by an ArchUnit rule
   that forbids production code from calling such methods (rule
   "Production code must not call methods annotated with @VisibleForTesting").

## Tests

No tests in this module. Behavioural validation lives in
`flink-architecture-tests-production` (rules in `ApiAnnotationRules`).

## Pitfalls & gotchas

- **Never add a dependency to this module.** It is a deliberate root of
  the dependency graph; adding anything here adds it to every Flink jar.
- **Adding a new `FlinkVersion`:** append to the end of the enum, never
  re-order, never change a `versionStr` — snapshot file paths on disk are
  derived from the strings, and ordinal comparisons rely on declaration
  order.
- **`@Public` self-annotation:** `Public` is annotated `@Public` (and the
  other three classifier annotations are also `@Public`). This is
  intentional — it means the annotations themselves are part of the
  binary-stable surface. Don't remove this.
- **`@VisibleForTesting` retention:** it has *no* `@Retention` declaration,
  so it defaults to `CLASS` (not `RUNTIME`). It is intended only for
  source-level documentation and bytecode-time ArchUnit checks. Don't
  reflect on it at runtime.
- **`Documentation.Sections` constants are the source of truth.** If you
  want to add a new docs section, add the constant here first; the docs
  generator picks them up by name.
- **ConfigGroup `keyPrefix` collisions** silently produce wrong docs. Keep
  prefixes unique within an options class.

## Related modules / docs

- `flink-core-api/CLAUDE.md` — sibling module that depends on this and
  defines the truly-stable user-facing types (functions, state
  declarations, watermarks, tuples).
- `flink-architecture-tests/CLAUDE.md` — describes the ArchUnit rules that
  *enforce* correct usage of these annotations.
- `flink-docs/` (no CLAUDE.md yet) — primary consumer of the
  `@Documentation` family.
- No `codedocs/*.md` deep-dive yet for the annotations machinery; the
  closest tangentially relevant docs are
  `codedocs/flink-memory-configuration.md` (which discusses `ConfigOption`
  surface).
