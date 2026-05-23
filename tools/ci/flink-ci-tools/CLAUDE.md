# `flink-ci-tools`

## Purpose
A small JVM-based toolbox of *build-time* validators used by Flink's
CI pipeline. It does **not** ship with Flink and is never deployed —
each tool is a Java `main()` invoked via
`mvn exec:java -pl tools/ci/flink-ci-tools` from a shell wrapper
under `tools/ci/`. The checks parse Maven plugin log output (shade,
dependency:tree, dependency:copy, deploy) plus `NOTICE` files and
fail the build on common packaging mistakes.

## Where it fits
Lives under `tools/ci/` alongside the shell scripts that drive it.
The shell drivers are themselves invoked by `tools/ci/compile.sh`
(which the GitHub Actions / GHA workflows under `.github/workflows`
call as part of the "compile" CI stage):

```
.github/workflows/*           # CI entry
  -> tools/ci/compile.sh
       -> verify_bundled_optional.sh -> ShadeOptionalChecker
       -> verify_scala_suffixes.sh   -> ScalaSuffixChecker
       -> license_check.sh           -> LicenseChecker
                                         -> NoticeFileChecker
                                         -> JarFileChecker
```

The module sets `japicmp.skip=true`; nothing here is part of the
published Flink artifact set.

## Maven coordinates
- `groupId`: `org.apache.flink`
- `artifactId`: `flink-ci-tools`
- `version`: `2.3-SNAPSHOT`
- Parent: `flink-parent` via `<relativePath>../../..</relativePath>`.
- Registered in the top-level reactor at `pom.xml`
  (`<module>tools/ci/flink-ci-tools</module>`).

## Dependencies

### Direct
Deliberately tiny — these tools must compile early and not pull in
the rest of Flink:

- `flink-annotations` (for `@VisibleForTesting`)
- `com.google.guava:guava:30.0-jre` (used by `JarFileChecker`)
- `log4j-slf4j-impl`, `log4j-api`, `log4j-core`
- Test: `flink-test-utils-junit` (JUnit 5 extension auto-registered
  via `META-INF/services`)

Note: there are **no** dependencies on Flink runtime modules — the
tools work by reading Maven *log output* as text, not by introspecting
loaded classes. This keeps the module buildable independently of
the rest of the reactor.

### Used by — including CI/build infrastructure
- Shell driver scripts (run by CI and also runnable manually):
  - `tools/ci/license_check.sh` -> `LicenseChecker`
  - `tools/ci/verify_bundled_optional.sh` -> `ShadeOptionalChecker`
  - `tools/ci/verify_scala_suffixes.sh` -> `ScalaSuffixChecker`
- `tools/ci/compile.sh` (the master CI compile stage) chains all
  three above.
- GitHub Actions workflows under `.github/workflows/` call
  `compile.sh` transitively.

## Source layout
All under `src/main/java/org/apache/flink/tools/ci/`:

- `licensecheck/`
  - `LicenseChecker` — combined entry point (NOTICE + jar
    inspection); `main(buildOut, flinkRoot, deployedRoot)`.
  - `NoticeFileChecker` — parses NOTICE files, cross-references
    against bundled dependencies (shade output) and deploy output.
  - `JarFileChecker` — walks a deployed Maven repo, opens each
    `.jar` as a `FileSystems.newFileSystem` zip FS and inspects
    `META-INF/NOTICE`, `LICENSE`, etc. for common mistakes.
- `optional/`
  - `ShadeOptionalChecker` — `main(shadeOut, depOut)`. Ensures every
    bundled dependency is marked `<optional>` (or transitively under
    an optional dependency) in the pom; required because Maven 3.3+
    no longer mutates the in-memory dep tree at runtime.
- `suffixcheck/`
  - `ScalaSuffixChecker` — `main(buildOut, flinkRoot)`. Verifies
    that any module pulling in a `_2.1x`-suffixed scala-dependent
    artifact itself carries a scala suffix in its `artifactId`.
- `utils/`
  - `shared/` — shared model types
    - `Dependency` — immutable `(groupId, artifactId, version,
      classifier?, scope?, optional?)` value object; some fields are
      `Optional` because `dependency:copy` output omits scope/optional.
    - `DependencyTree` — recursive tree built from `dependency:tree`
      output; exposes `flatten()`.
    - `ParserUtils` — common log-parsing helpers.
  - `shade/ShadeParser` — parses `maven-shade-plugin` output blocks
    (`shade-flink`, `shade-dist`, `default` executions).
  - `dependency/DependencyParser` — parses both `dependency:tree`
    and `dependency:copy` outputs into `Map<module, ...>`.
  - `notice/NoticeParser` + `NoticeContents` — parses Flink's
    NOTICE-file format (`- groupId:artifactId:version` lines and
    `This project bundles "..."` phrasing — see regex in
    `NoticeFileChecker`).
  - `deploy/DeployParser` — extracts the set of modules actually
    deployed from the Maven deploy log.
- `src/main/resources/`
  - `log4j2.properties`
  - `modules-defining-excess-dependencies.modulelist` — text list
    of modules (currently just `flink-python`) that legitimately
    bundle dependencies the shade plugin output does not show.

## Architecture & key concepts
- **Log-driven, not classpath-driven.** All checkers consume a
  captured Maven build log (`mvn ... > build.out`) plus, in some
  cases, the contents of a `-DaltDeploymentRepository` target.
  Drivers in `tools/ci/*.sh` do the redirection.
- **Parsing layer** lives under `utils/`; each Maven plugin's text
  output has a small parser:
  - `ShadeParser` keys on `:shade (shade-flink|shade-dist|default) @ <module>`.
  - `DependencyParser` keys on `maven-dependency-plugin:*:tree` /
    `:copy @ <module>` headers and parses indented children into a
    `DependencyTree`.
  - `NoticeParser` consumes NOTICE plaintext.
  - `DeployParser` extracts modules from deploy lines.
- **Checker layer** turns parsed data into pass/fail. Failures print
  human-readable guidance (see e.g. the multi-line `LOG.error` blocks
  in `ShadeOptionalChecker.main`).
- **Exit codes**: each `main()` calls `System.exit(1)` on violation.
  The wrapping shell scripts surface this back to CI.
- **Scope of `Dependency.optional`**: a tri-state (`Boolean` /
  `Optional<Boolean>`) because `dependency:copy` does not emit the
  `(optional)` marker that `dependency:tree` does.

## How it integrates with the build
- Tools are invoked via `exec:java`, e.g.:
  ```
  ./mvnw -pl tools/ci/flink-ci-tools exec:java \
      -Dexec.mainClass=org.apache.flink.tools.ci.licensecheck.LicenseChecker \
      -Dexec.args="<maven-build-output> <flink-root> <deployed-root>"
  ```
- Prep step is to capture the Maven output:
  `./mvnw clean deploy -DaltDeploymentRepository=validation_repository::default::file:<deployedRoot> > build.out`
  (see `license_check.sh` usage banner).
- No special Maven lifecycle binding; the module just needs to be
  `mvn install`-ed (or `-am`-compiled) before any tool is invoked.

## Important public APIs (utility classes)
Although nothing here is a published API, internally these are
re-used across tools:

- `utils.shared.Dependency` — immutable value type; uses static
  factories `Dependency.create(...)`.
- `utils.shared.DependencyTree` — node-oriented dep tree with
  `flatten()` stream.
- `utils.shade.ShadeParser.parseShadeOutput(Path) : Map<String, Set<Dependency>>`.
- `utils.dependency.DependencyParser`:
  - `parseDependencyTreeOutput(Path) : Map<String, DependencyTree>`
  - `parseDependencyCopyOutput(Path) : Map<String, Set<Dependency>>`
- `utils.notice.NoticeParser` — `(modulePath) -> NoticeContents`.
- `utils.deploy.DeployParser.parseDeployOutput(File) : Set<String>`.
- `licensecheck.JarFileChecker.checkPath(Path) : int` — count of
  severe issues; `0` means clean.
- `licensecheck.NoticeFileChecker.run(File buildResult, Path root) : int`.
- `optional.ShadeOptionalChecker.checkOptionalFlags(...)` —
  `@VisibleForTesting` core algorithm.

## Tests
JUnit 5 (extension auto-registered via the
`META-INF/services/org.junit.jupiter.api.extension.Extension`
file). Mostly parser-focused so they don't need an actual Maven
build:

- `utils/dependency/DependencyParserTreeTest`,
  `DependencyParserCopyTest` — fixture-driven parser checks for both
  `dependency:tree` and `dependency:copy` outputs.
- `utils/shade/ShadeParserTest`
- `utils/notice/NoticeParserTest`
- `utils/deploy/DeployParserTest`
- `utils/shared/DependencyTreeTest`
- `optional/ShadeOptionalCheckerTest` — covers the
  optional-flag algorithm including the multi-path edge case
  documented in `ShadeOptionalChecker`'s class javadoc.
- `licensecheck/NoticeFileCheckerTest`,
  `licensecheck/JarFileCheckerTest`.

Test sample input strings are inlined in the test classes, so the
expected log format is documented by example.

## Pitfalls & gotchas
- **Output format is the contract.** The parsers rely on exact
  Maven plugin log shapes — see e.g. the regex in
  `DependencyParser` matching `maven-dependency-plugin:[^:]+:tree`
  and the shade execution names
  (`shade-flink|shade-dist|default`). Upgrading shade/dependency
  plugin versions can silently break parsing; the suffix checker
  even self-detects this (`"Parsing found 0 ...modules; the parsing
  is likely broken."` log lines) and exits non-zero.
- **Drivers redirect stdout, not stderr.** Make sure to capture the
  Maven output the way `license_check.sh` does; using `tee` or
  altering verbosity can drop required lines (e.g. when running
  with `-q`).
- **The shade plugin must actually run.** Skipping shade (e.g.
  `-Dshade.skip`) yields empty parser results and trips the "likely
  broken" guard.
- **Module-name stripping.** `ScalaSuffixChecker.stripScalaSuffix`
  treats `_2.12` etc. as part of the module name; hard-coded
  `EXCLUDED_MODULES` skips `flink-rpc-akka` and
  `flink-table-planner-loader` (loaded via separate classloaders).
  `flink-docs` is excluded inside `ScalaSuffixChecker.java` (line
  ~160) because it depends on essentially every module.
- **Modules legitimately bundling "extra" deps** must be listed in
  `modules-defining-excess-dependencies.modulelist` or
  `NoticeFileChecker` will flag them.
- **Hardcoded Guava 30.0-jre.** Independent of the
  `flink-shaded-guava` used by the rest of Flink — do not
  consolidate without thought.
- **No Flink runtime on the classpath.** Don't be tempted to import
  Flink types beyond `@VisibleForTesting` from `flink-annotations`;
  doing so would cause the module to be built *after* most of
  Flink, breaking the CI ordering.
- **Build-output paths.** Some drivers pass absolute paths; under
  Windows/WSL mixed-mode setups, ensure paths passed to
  `JarFileChecker` are POSIX-style (it uses `FileSystems` with
  `jar:file:` URIs).

## Related modules / docs
- `tools/ci/` shell scripts: `compile.sh`, `license_check.sh`,
  `verify_bundled_optional.sh`, `verify_scala_suffixes.sh`,
  `maven-utils.sh`, `watchdog.sh`.
- `.github/workflows/` — top-level CI entry that runs `compile.sh`.
- `flink-docs` — sister build-time module (documentation
  generators); also `japicmp.skip`-ed and explicitly excluded from
  the scala suffix check here.
- Apache Flink dependency-management wiki page (linked from
  `ShadeOptionalChecker`'s error output:
  `https://cwiki.apache.org/confluence/display/FLINK/Dependencies`).
