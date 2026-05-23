# `flink-runtime-web`

## Purpose

Hosts the **Flink Web Dashboard** — both the Angular single-page
application that ships at `http://localhost:8081/` and the small Java
backend that adds job-submission REST handlers and serves the static
UI bundle. Also contains the **History Server**, a standalone process
that browses archived completed jobs.

Two artifacts live here:

1. The Angular SPA under `web-dashboard/` (separate `package.json`,
   built with `@angular/cli` 20 via the `frontend-maven-plugin`). Its
   compiled output ends up under `web-dashboard/web/` and is copied
   into the jar's classpath as `web/...`.
2. The Java module under `src/` that contributes the
   `WebSubmissionExtension` (jar-upload + jar-run REST handlers), the
   `HistoryServer` daemon, plus a few shared Netty helpers
   (`HttpRequestHandler`, `PipelineErrorHandler`).

## Where it fits

Sits **on top of** `flink-runtime` and `flink-clients`. It does **not**
define any RPC gateways; it consumes them. The runtime's
`WebMonitorEndpoint` (in `flink-runtime`) loads this module's
`WebSubmissionExtension` reflectively, which is why the runtime can
boot without flink-runtime-web on the classpath (REST works, but jar
upload and the dashboard tab don't).

```
[ browser ]
    │  HTTP/JSON
    ▼
[ WebMonitorEndpoint  (flink-runtime, Netty REST server) ]
    │
    ├── built-in handlers (jobs, vertices, checkpoints, ...)
    └── WebSubmissionExtension (flink-runtime-web) ───┐
            ├── JarUploadHandler                       │
            ├── JarListHandler                         │ uses DispatcherGateway
            ├── JarRunHandler / JarRunApplicationHandler
            ├── JarPlanHandler
            └── JarDeleteHandler                       │
                                                       ▼
                                              [ Dispatcher (flink-runtime) ]
```

## Maven coordinates

`<groupId>org.apache.flink</groupId>` /
`<artifactId>flink-runtime-web</artifactId>`, packaging `jar`.

## Dependencies

### Direct

- `flink-runtime` — REST handler base classes, `DispatcherGateway`,
  `Router`, message-headers infrastructure.
- `flink-clients` — `PackagedProgram`, `ApplicationRunner`,
  `DetachedApplicationRunner` (used by `JarRunApplicationHandler`).
- `flink-shaded-netty` — the HTTP transport.
- `flink-shaded-jackson` — JSON serialization for handler bodies.
- `flink-shaded-guava` — usual collection helpers.
- Tests: `flink-test-utils`, `flink-test-utils-junit`,
  `flink-shaded-jackson-module-jsonSchema`, `commons-io`.

The frontend build pulls a private NPM tree (`@angular/*`,
`ng-zorro-antd`, `d3`, `d3-flame-graph`, `dagre`, `monaco-editor`,
`@antv/g2`, `rxjs`, `zone.js`) — see `web-dashboard/package.json`.

### Used by

- `flink-dist` — bundles the jar (and therefore the SPA static
  assets) into the assembled distribution.
- `flink-docs` — depends on this module to generate REST API and
  config docs.

Note that `flink-runtime` itself **does not** depend on
flink-runtime-web; the relationship is purely "additive".

## Source layout

```
src/main/java/org/apache/flink/runtime/webmonitor/
├── HttpRequestHandler.java        # Netty handler that bridges chunked
│                                    HTTP uploads into Flink REST
├── PipelineErrorHandler.java      # Catches uncaught exceptions in the
│                                    Netty pipeline and emits HTTP 500
├── WebSubmissionExtension.java    # Implements WebMonitorExtension;
│                                    contributes all Jar*Handlers
├── handlers/
│   ├── JarUploadHandler.java + JarUploadHeaders + ResponseBody
│   ├── JarListHandler.java   + JarListHeaders   + JarListInfo
│   ├── JarRunHandler.java    + JarRunHeaders / Body / Response
│   ├── JarRunApplicationHandler.java  (Application Mode submission)
│   ├── JarPlanHandler.java   + JarPlanGet/Post Headers
│   ├── JarDeleteHandler.java
│   ├── *MessageParameters / *QueryParameter / *PathParameter
│   └── utils/  (TestProgram used by handler tests)
└── history/
    ├── HistoryServer.java                  # standalone main()
    ├── HistoryServerArchiveFetcher.java    # pulls archives from FS
    ├── HistoryServerApplicationArchiveFetcher.java
    ├── HistoryServerStaticFileServerHandler.java
    └── retaining/   (CompositeArchiveRetainedStrategy and friends)

src/main/resources/
└── (logging defaults; no compile-time UI assets here)

web-dashboard/                # the Angular workspace
├── package.json              # @angular/cli 20, ng-zorro-antd
├── angular.json
├── tsconfig.json
├── proxy.conf.json           # dev-mode proxy → localhost:8081
├── dev/generate_notice.sh    # NPM-NOTICE generator
├── node/                     # node + npm installed by frontend-maven-plugin
├── src/
│   ├── app/
│   │   ├── pages/            # overview, job-manager, task-manager,
│   │   │                       job, submit, application
│   │   ├── components/       # shared widgets (charts, tables)
│   │   ├── core/             # module-level providers
│   │   ├── services/         # HTTP clients for REST endpoints
│   │   ├── interfaces/       # TS types matching server response bodies
│   │   ├── routes.ts
│   │   └── app.component.ts
│   └── styles, assets, i18n
└── web/                      # build output, embedded in the jar
                              # (resources directory included via the
                              # <resource> declaration in pom.xml)
```

## Architecture & key concepts

### Server backend (Java, Netty)

- Reuses `flink-runtime`'s REST stack
  (`AbstractRestHandler`, `Router`, `MessageHeaders`,
  `MessageParameters`, `RequestBody`, `ResponseBody`).
- Adds a `WebSubmissionExtension` (implements
  `WebMonitorExtension` from runtime). At boot, the
  `WebMonitorEndpoint` discovers it on the classpath and registers its
  handlers under `/jars/...`.
- Static assets are served by `WebFrontendBootstrap` / equivalent
  static handler in runtime, reading from the `web/` resource folder
  bundled into this jar.

### History server

- A separate JVM process started by
  `org.apache.flink.runtime.webmonitor.history.HistoryServer.main()`.
- Periodically scans a filesystem path
  (`historyserver.archive.fs.dir`) for newly archived job JSONs
  uploaded by completed JobManagers and serves them as a read-only
  dashboard.
- Authentication / SSL handled by the same `SSLUtils`,
  `SecurityConfiguration` plumbing as the live dashboard.
- Retention policy plug-points live under `history/retaining/`.

### Frontend (Angular 20 + NG-ZORRO)

- Page hierarchy (under `src/app/pages/`):
  - `overview/` — cluster summary
  - `job-manager/` — JM logs, config, thread dump
  - `task-manager/` — list + per-TM logs / metrics
  - `job/` — running job: graph, vertex detail, checkpoints,
    accumulators, watermarks, backpressure, flame graph (uses
    `d3-flame-graph`)
  - `submit/` — JAR upload, parallelism / entry-class form, run /
    show plan
  - `application/` — application-mode submissions
- The graph view uses `dagre` for layout + `@antv/g2` for charts.
- All HTTP calls go through `app.interceptor.ts`, which strips a
  `/proxy/` prefix used during local development.

### Build process (frontend-maven-plugin)

- The Maven build pulls **node 22.16.0** and **npm 10.9.0** via
  `frontend-maven-plugin` 1.15.1.
- Goals executed during `mvn package`:
  1. `install-node-and-npm`
  2. `npm ci --cache-max=0 --no-save`
  3. `npm run ci-check` (= `lint` + `build`)
- Output `web-dashboard/web/` is included as a Maven `<resource>` so
  the assets land at `web/...` inside the jar.
- The `skip-webui-build` profile turns this off (used by quick
  iterative builds when only Java has changed).
- The `use-alibaba-mirror` profile points npm at an Alibaba mirror
  for offline CI environments in China.

## Important public APIs

- `WebSubmissionExtension` — constructed by
  `WebMonitorUtils.loadWebSubmissionExtension(...)` in `flink-
  runtime`. The constructor takes a `GatewayRetriever<? extends
  DispatcherGateway>`, the upload directory, an executor, and a
  timeout.
- `JarRunHandler` / `JarRunApplicationHandler` — translate REST POSTs
  into `Dispatcher.submitJob` / application-mode runs. Take a
  `Supplier<ApplicationRunner>` so tests can inject a fake.
- `HistoryServer#main(String[])` — entry point for the
  `historyserver.sh` script.
- All `*Headers` classes are picked up by `flink-docs` to generate
  REST documentation.

## Internal flows

### Job submission via the UI

1. User drags JAR onto **Submit New Job** page. The browser POSTs
   `multipart/form-data` to `POST /jars/upload`.
2. `HttpRequestHandler` reassembles the chunked upload into a
   temporary file under `web.upload.dir`.
3. `JarUploadHandler` moves the file into the configured jar dir and
   returns a `JarUploadResponseBody` with a server-generated jar id.
4. The UI polls `GET /jars` (`JarListHandler`) to refresh the list.
5. User fills in entry class / parallelism / args and clicks **Submit**;
   the page sends `POST /jars/:jarid/run` with a
   `JarRunRequestBody`.
6. `JarRunHandler` instantiates a `PackagedProgram` and a
   `DetachedApplicationRunner`, retrieves the current
   `DispatcherGateway`, and calls `submitJob`. The returned
   `JobID` is sent back in `JarRunResponseBody`.

### History server boot

1. `historyserver.sh` invokes `HistoryServer.main`.
2. Loads `HistoryServerOptions`, builds an `ArchiveFetcher`
   pointing at `historyserver.archive.fs.dir` (uses **flink-
   filesystems** to talk to S3/HDFS/GCS).
3. Spins up a Netty HTTP server on `historyserver.web.port` serving
   archived JSONs.
4. A timer task re-fetches archives every
   `historyserver.archive.fs.refresh-interval`.

## Tests

- Handler unit tests live next to the handlers
  (`Jar*HandlerTest`, `WebSubmissionExtensionTest`).
- A handful of small JARs are built during the test phase
  (`test-program`, `parameter-program`,
  `parameter-program-with-eager-sink`, `output-test-program`) — see
  the `maven-jar-plugin` executions in `pom.xml`. These are fed into
  `JarRunHandler` integration tests to exercise the full submission
  pipeline against a `MiniCluster`.
- `HistoryServerTest`, `HistoryServerStaticFileServerHandlerTest`,
  `HistoryServerArchiveFetcherTest` cover archive scanning and
  serving.
- Frontend has no unit tests committed; `npm run ci-check` runs
  ESLint + a production build as the only gate.

## Pitfalls & gotchas

- Do **not** edit files under `web-dashboard/web/` directly — they
  are build artifacts. Edit `web-dashboard/src/`, then `npm run
  build`, then rebuild flink-dist (`mvn -DskipTests -pl flink-
  runtime-web,flink-dist clean package`).
- Local dev workflow: in one terminal run a Flink cluster
  (`start-cluster.sh`); in another run `cd web-dashboard && npm run
  proxy` to start `ng serve --proxy-config proxy.conf.json` at
  `http://localhost:4200`. The proxy forwards `/jobs`, `/jars`, etc.
  to the live REST endpoint on `:8081`.
- The Maven build downloads node + npm even when only Java changed —
  pass `-Dskip.npm` to engage the `skip-webui-build` profile and skip
  the front-end build.
- The `--add-opens=java.base/java.util=ALL-UNNAMED` surefire flag is
  required on JDK 17+ because DataStream V2 sink translator
  registration uses reflection on a final `Map`.
- Job-submission JARs uploaded by users live in `web.upload.dir`.
  This is **not** automatically cleaned; `JarDeleteHandler` removes
  individual jars on request only.
- The `HistoryServer` is a separate JVM. It does not participate in HA
  and stores nothing in ZK / K8s; restarting it just re-scans the
  archive directory.
- For Application Mode submissions, `JarRunApplicationHandler` calls
  `DispatcherGateway.dispatchApplication`. The application JAR is run
  on the JobManager, **not** on the client / browser side.

## Related modules / docs

- `flink-runtime` — the REST stack, `WebMonitorEndpoint`,
  `RestServerEndpoint`, `Router`. This module plugs in there.
- `flink-clients` — provides `PackagedProgram`, the entry-point for
  user JARs.
- `flink-filesystems` — used by the History Server to read archived
  job JSONs from S3 / GCS / HDFS.
- `flink-docs` — consumes `*Headers` classes here to generate the
  REST reference page.
- `README.md` — local-dev instructions (npm install, npm run
  proxy, etc.).
