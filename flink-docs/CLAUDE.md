# `flink-docs`

## Purpose
Build-time documentation generators that produce reference HTML/YAML
files directly from Flink's Java source via reflection and ASM
bytecode inspection. The module does **not** contain the Flink user
manual itself — that lives under `/docs` (Hugo site). `flink-docs`
generates the auto-derived fragments that the Hugo site embeds
(e.g. configuration option tables and REST API descriptions).

## Where it fits
The actual website source (Markdown, Hugo layouts, theme) lives in
`/home/wangh/src/myflink/docs/`. That site references this module's
output via Hugo shortcodes such as
`{{< generated/<file-name> >}}`. Concretely:

- HTML fragments are written into `docs/layouts/shortcodes/generated/`.
- OpenAPI YAML specs are written into `docs/static/generated/`.

Both paths are configured in `pom.xml` (`generated.docs.dir`,
`generated.static.dir`) and committed to the repo, so regenerating
them is part of any PR that touches `ConfigOption`s, REST handlers,
or message bodies.

## Maven coordinates
- `groupId`: `org.apache.flink`
- `artifactId`: `flink-docs`
- Parent: `flink-parent` (version `2.3-SNAPSHOT`)
- Registered as module in the top-level reactor (`pom.xml` line 101).

## Dependencies

### Direct
Heavyweight — depends on most modules whose options/REST endpoints
must be documented. The generator runs by **classloading** those
modules and reflecting on them, so anything that exposes a
documented surface must be on the classpath:

- `flink-annotations`, `flink-core`, `flink-streaming-java`,
  `flink-runtime` (+ test-jar), `flink-runtime-web`
- `flink-table-api-java`, `flink-sql-client`, `flink-sql-gateway`
  (+ test-jar)
- Metrics reporters: `flink-metrics-prometheus`,
  `flink-metrics-influxdb`, `flink-metrics-otel`
- Deployment: `flink-yarn`, `flink-kubernetes`
- State backends: `flink-statebackend-rocksdb`,
  `flink-statebackend-forst`, `flink-dstl-dfs`
- Misc: `flink-python`, `flink-cep`, `flink-external-resource-gpu`,
  `flink-model-openai`, `flink-model-triton`
- Shaded: `flink-shaded-asm-9` (bytecode scanning for
  annotation verification), `flink-shaded-netty`,
  `flink-shaded-jackson-module-jsonSchema`, `flink-shaded-swagger`
- Test-only: `flink-test-utils-junit`, `jsoup`

### Used by — including CI/build infrastructure
- Hugo site under `/docs/` (consumes generated fragments at site
  build time — see `docs/README.md`).
- `tools/ci/flink-ci-tools` and the broader CI scripts treat this
  module specially (e.g. `ScalaSuffixChecker` excludes `flink-docs`
  because it depends on nearly everything).
- The module sets `japicmp.skip=true` and skips dependency
  convergence enforcement — it is **not** published or
  binary-compatibility tested.

## Source layout
- `src/main/java/org/apache/flink/docs/configuration/`
  - `ConfigOptionsDocGenerator` — entry point for config-option HTML
    tables and per-section "common" tables.
- `src/main/java/org/apache/flink/docs/rest/`
  - `RestAPIDocGenerator` — shared HTML generation logic
    (`createHtmlFile`).
  - `OpenApiSpecGenerator` — shared OpenAPI 3 YAML generation logic
    (uses Swagger).
  - `RuntimeRestAPIDocGenerator`, `RuntimeOpenApiSpecGenerator` —
    entry points for the JobManager Dispatcher REST API.
  - `SqlGatewayRestAPIDocGenerator`,
    `SqlGatewayOpenApiSpecGenerator` — entry points for the SQL
    Gateway REST API.
  - `ApiSpecGeneratorUtils` — small helper (filters
    `@Documentation.ExcludeFromDocumentation`, resolves
    `@FlinkJsonSchema.AdditionalFields`).
- `src/main/java/org/apache/flink/docs/util/`
  - `ConfigurationOptionLocator` — hard-coded list of
    `(module, package)` pairs to scan; the
    `LOCATIONS` array is the master registry of which packages
    contribute `ConfigOption`s. Also holds `EXCLUSIONS`.
  - `OptionsClassLocation`, `OptionWithMetaInfo`, `Utils`.
- `src/main/resources/log4j.properties` — generator logging config.

## Architecture & key concepts
- **Annotation-driven, reflection-based.** Generators classload
  `*Options` / `*Config` / `*Parameters` classes that match
  `ConfigurationOptionLocator.CLASS_NAME_PATTERN`, then iterate
  public static fields of type `ConfigOption`. The list of scanned
  locations is the static `LOCATIONS` array — adding options in a new
  package requires editing that list.
- **Annotations consumed** (all from `flink-annotations`):
  - `@Public` / `@PublicEvolving` / `@Experimental` — required on
    every `*Options` class; verified via ASM `ClassReader` (since
    reflection may not see source-only annotations).
  - `@Documentation.Section` — groups options into common
    cross-cutting reference tables.
  - `@Documentation.TableOption` — controls table presentation
    (batch/streaming exec mode flags).
  - `@Documentation.SuffixOption` — multiple options share a prefix.
  - `@Documentation.OverrideDefault` — display value differs from
    the actual default (e.g. derived at runtime).
  - `@Documentation.ExcludeFromDocumentation` — opt-out.
  - `@ConfigGroup` / `@ConfigGroups` — split one `*Options` class
    into multiple HTML files.
  - `@FlinkJsonSchema.AdditionalFields` — REST bodies with dynamic
    keys (e.g. maps).
- **REST docs**: rely on `DocumentingRestEndpoint` from `flink-runtime`
  (and `DocumentingDispatcherRestEndpoint`,
  `DocumentingSqlGatewayRestEndpoint`) — these subclasses expose
  registered `MessageHeaders`/handlers without spinning up a real
  HTTP server. The OpenAPI generator uses
  `flink-shaded-swagger` + `flink-shaded-jackson-module-jsonSchema`
  to emit OpenAPI 3 YAML and to detect name clashes between
  top-level/inner schema classes (see
  `NameClashDetectingTypeNameResolver`).
- **API versioning**: REST generators iterate
  `RuntimeRestAPIVersion` / `SqlGatewayRestAPIVersion` and emit one
  file per version, skipping `V0` (test-only).

## How it integrates with the build (Maven phases)
- The module is **not** built into the regular reactor's package
  flow for doc generation — generation is gated by activation
  properties so a normal `mvn package` does **not** write docs.
- Two profiles, both binding `maven-antrun-plugin` to the
  **`package`** phase via Ant `<java>` tasks (fork=true, fail on
  error), passing `${rootDir}` and the generated dirs:
  - `-Dgenerate-rest-docs` runs four generators:
    `RuntimeRestAPIDocGenerator`, `RuntimeOpenApiSpecGenerator`,
    `SqlGatewayRestAPIDocGenerator`, `SqlGatewayOpenApiSpecGenerator`.
  - `-Dgenerate-config-docs` runs `ConfigOptionsDocGenerator` with
    `args[0]=outputDir`, `args[1]=rootDir`.
- Typical invocations (from `README.md`):
  - `./mvnw package -Dgenerate-rest-docs -pl flink-docs -am -nsu -DskipTests`
  - `./mvnw package -Dgenerate-config-docs -pl flink-docs -am -nsu -DskipTests -Pskip-webui-build`
- Output directories: `${rootDir}/docs/layouts/shortcodes/generated/`
  (HTML) and `${rootDir}/docs/static/generated/` (OpenAPI YAML).

## Important public APIs (utility classes)
- `ConfigOptionsDocGenerator.main(String[] outputDir, String rootDir)` —
  scans `LOCATIONS`, emits `<prefix>_configuration.html` and
  `<section>_section.html` files.
- `ConfigOptionsDocGenerator.extractConfigOptions(Class<?>)` /
  `verifyClassAnnotation(Class<?>)` — `@VisibleForTesting` entry
  points used by tests and downstream tools.
- `ConfigurationOptionLocator.discoverOptionsAndApply(rootDir, consumer)` —
  generic walker; external callers can pass any consumer.
- `ConfigurationOptionLocator.LOCATIONS` (private but central) —
  **edit this** to register a new options package.
- `RestAPIDocGenerator.createHtmlFile(DocumentingRestEndpoint, RestAPIVersion, Path)` —
  shared helper used by the four `*main()` entry points.
- `OpenApiSpecGenerator` — Swagger-based equivalent for OpenAPI 3.
- `ApiSpecGeneratorUtils.shouldBeDocumented(MessageHeaders)` /
  `findAdditionalFieldType(Class)`.

## Tests
- `ConfigOptionsDocGeneratorTest` — unit tests for HTML formatting,
  enum rendering, `@ConfigGroup` behavior.
- `ConfigOptionsDocsCompletenessITCase` — **IT case** that fails the
  build if the generated docs on disk are stale (missing options,
  description drift). Run via the standard test phase; depends on
  `rootDir` being passed as a system property
  (see `maven-surefire-plugin` config).
- `ConfigOptionsYamlSpecTest` — checks the YAML specs for SQL
  options.
- `RestAPIDocGeneratorTest`, `OpenApiSpecGeneratorTest` — including
  the name-clash detector (`data/clash/...` test fixtures with two
  classes sharing a simple name).
- Tests use `jsoup` to parse generated HTML.

## Pitfalls & gotchas
- **`LOCATIONS` is a static array.** Adding `ConfigOption`s in a new
  package without updating `ConfigurationOptionLocator.LOCATIONS`
  results in silently undocumented options.
- **Class-name pattern.** Only classes ending in `Options`, `Config`,
  or `Parameters` are scanned. Renaming an options class outside
  this convention drops it from docs.
- **`@Public` family required.** `verifyClassAnnotation` will throw
  if an `*Options` class with any `ConfigOption` lacks `@Public`,
  `@PublicEvolving`, or `@Experimental`. The check uses ASM, not
  reflection, so even `RetentionPolicy.SOURCE` would not save you.
- **Generation is not part of the default build.** Profiles must be
  activated by `-D` properties. Generated files are committed; if
  they drift, `ConfigOptionsDocsCompletenessITCase` fails.
- **Heavy transitive classpath.** Because the generator must load
  every documented module, adding a new module dep here can create
  unexpected reactor build-order constraints. Dependency
  convergence enforcement is explicitly skipped for this module.
- **CI exclusions.** `flink-ci-tools`'s `ScalaSuffixChecker`
  explicitly excludes `flink-docs` — do not treat its dependency
  graph as representative.
- **`-Pskip-webui-build`** is generally needed when running locally
  to avoid pulling in the web UI Node toolchain.
- **Forked JVMs.** Each generator runs in its own JVM
  (`<java fork="true">`), so don't expect static state to leak
  between them.

## Related modules / docs
- `/docs/` — the Hugo source for nightlies.apache.org/flink.
- `/docs/README.md` — describes how to build the site and where
  generated content fits.
- `/flink-docs/README.md` — quick-start for regenerating docs.
- `flink-annotations` (`org.apache.flink.annotation.docs.*`) —
  defines the annotations consumed here.
- `flink-runtime` (`org.apache.flink.runtime.rest.util.DocumentingRestEndpoint`)
  and `flink-sql-gateway` (`DocumentingSqlGatewayRestEndpoint`) —
  source of REST API metadata.
- `tools/ci/flink-ci-tools` — sister build-time tools (license,
  shading, scala-suffix checks).
