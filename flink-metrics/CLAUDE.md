# `flink-metrics`

Parent Maven aggregator pom for Flink's metric/event/trace system. Defines the
common metric abstractions (in `flink-metrics-core`) plus one
self-contained jar per reporter backend (JMX, Prometheus, Datadog, …).
Every Flink component — JM, TM, operators, connectors, state backends —
funnels metrics through these interfaces.

## Purpose

- Aggregator-only (`<packaging>pom</packaging>`) — produces no jar of its own.
- Houses the user-facing metric API (`Metric`, `MetricGroup`, `Counter`,
  `Gauge`, `Histogram`, `Meter`) and the SPI for exporting them
  (`MetricReporter`, `MetricReporterFactory`).
- Each sub-module is a shaded, self-contained reporter jar that ships under
  `opt/` in the distribution and is loaded as a plugin (per the Flink plugin
  classloader isolation) when configured via `metrics.reporter.<name>.factory.class`.
- Also hosts the experimental Spans/Traces and Events APIs that share the same
  reporter plugin model.

## Where it fits

```
operator / connector / state backend
        |
        v  registers Counter/Gauge/Histogram/Meter
flink-metrics-core (MetricGroup, Metric)
        |
        v  notifyOfAddedMetric / notifyOfRemovedMetric
MetricRegistryImpl in flink-runtime
        |
        v  one ReporterSetup per configured reporter
flink-metrics-<backend>  (MetricReporter impls, loaded as plugins)
        |
        v
external backend (JMX, Prometheus, Datadog, Graphite, InfluxDB, StatsD, OTel, slf4j)
```

## Maven coordinates

- Group: `org.apache.flink`
- Artifact: `flink-metrics`
- Version: `2.3-SNAPSHOT`
- Parent: `flink-parent`
- Packaging: `pom`

Parent-level dependencies are intentionally `provided` (`slf4j-api`, `jsr305`)
so reporter jars-with-dependencies stay slim — `flink-dist` already provides
them.

### Sub-modules

| Sub-module | Purpose |
| ---------- | ------- |
| `flink-metrics-core` | The metric API + SPI. Pure interfaces; no transport. Depends only on `flink-annotations`. |
| `flink-metrics-dropwizard` | Adapter that bridges Dropwizard `Metric` to Flink `Metric` (and vice-versa). Used as a base by reporters built on Codahale/Dropwizard, plus user-supplied Dropwizard histograms. |
| `flink-metrics-jmx` | Exposes metrics as JMX MBeans. Default reporter when no backend is configured. |
| `flink-metrics-prometheus` | `PrometheusReporter` (pull, opens an HTTP endpoint) and `PrometheusPushGatewayReporter` (push). Bundles `io.prometheus:simpleclient` via shade. |
| `flink-metrics-graphite` | TCP/UDP push to Graphite Carbon. Built on `flink-metrics-dropwizard`. |
| `flink-metrics-influxdb` | HTTP push to InfluxDB 1.x line protocol. |
| `flink-metrics-statsd` | UDP push to a StatsD daemon. |
| `flink-metrics-datadog` | HTTPS push to the Datadog metrics API. |
| `flink-metrics-slf4j` | Logs metric samples (plus events and spans) via slf4j — handy for debugging. |
| `flink-metrics-otel` | OpenTelemetry exporter for metrics, events, and traces. |

## Dependencies

### Direct

The aggregator pom only declares `slf4j-api` and `jsr305` (both `provided`).
Each sub-module pulls in the third-party client library it needs (shaded into
its own jar) and `flink-metrics-core` (`provided`).

`flink-metrics-core` itself depends on `flink-annotations` only — it has no
Flink runtime dependency.

### Used by

- `flink-core-api` and `flink-runtime` — depend on `flink-metrics-core` to
  define metric-accepting interfaces (`RuntimeContext.getMetricGroup()`,
  `MetricRegistry`, reporter wiring).
- `flink-dist` — bundles the reporter jars (jmx, prometheus shipped in
  `lib/`; the rest go to `opt/` for plugin loading).
- Every operator, connector, source/sink, and state backend reaches the API
  via `RuntimeContext.getMetricGroup()` or via injected `MetricGroup`s.

## Source layout

```
flink-metrics/
  flink-metrics-core/
    org.apache.flink.metrics             # Metric, Counter, Gauge, Histogram, Meter, View, MeterView
    org.apache.flink.metrics.groups      # MetricGroup specializations: SourceReaderMetricGroup, SinkWriterMetricGroup, OperatorIOMetricGroup, CacheMetricGroup, ...
    org.apache.flink.metrics.reporter    # MetricReporter, MetricReporterFactory, AbstractReporter, Scheduled
    org.apache.flink.traces              # Span, SpanBuilder (experimental tracing)
    org.apache.flink.traces.reporter     # TraceReporter, TraceReporterFactory
    org.apache.flink.events              # Event, EventBuilder, Events (experimental event API)
    org.apache.flink.events.reporter     # EventReporter, EventReporterFactory
  flink-metrics-dropwizard/              # Flink<->Dropwizard wrappers + ScheduledDropwizardReporter
  flink-metrics-<backend>/               # one self-contained reporter jar each
```

`flink-metrics-core` is API-only: the implementation of `MetricRegistry`, the
scope/group hierarchy (`AbstractMetricGroup`, `TaskManagerMetricGroup`,
`JobMetricGroup`, …), and the runtime histograms (`DescriptiveStatisticsHistogram`)
live in `flink-runtime`.

## Architecture & key concepts

- **Four metric kinds.** `Counter` (monotonic int), `Gauge<T>` (sampled value),
  `Histogram` (distribution via `HistogramStatistics`), `Meter` (rate; default
  `MeterView` derives it from a counter sample).
- **`MetricGroup` hierarchy.** A `MetricGroup` is a named container with
  `counter()`, `gauge()`, `histogram()`, `meter()`, and `addGroup()` factory
  methods. The hierarchy maps to a scope string (e.g. `host.tm.job.operator.metric`)
  that reporters consume via `getMetricIdentifier()` / `getScopeComponents()`.
  Specializations in `metrics.groups` (`SourceReaderMetricGroup`, `SinkWriterMetricGroup`,
  `OperatorIOMetricGroup`, `CacheMetricGroup`, …) carry standardized metrics
  defined by Flink FLIPs (FLIP-179 connector metrics, FLIP-33).
- **Reporter SPI.** A reporter implements `MetricReporter` (the `Reporter`
  super-type adds `open()`/`close()`) and is constructed by a
  `MetricReporterFactory#createMetricReporter(Properties)`. Periodic reporters
  also implement `Scheduled#report()` so `MetricRegistryImpl` can call them
  at `metrics.reporter.<name>.interval`.
- **Plugin loading.** Each reporter jar contains
  `META-INF/services/org.apache.flink.metrics.reporter.MetricReporterFactory`.
  At startup `ReporterSetup.fromConfiguration()` in `flink-runtime` uses the
  Flink `PluginManager` to load each configured reporter from its own
  classloader (so `flink-metrics-prometheus`'s shaded `io.prometheus.client.*`
  doesn't collide across reporters).
- **Spans and Events.** `traces.SpanBuilder` and `events.EventBuilder` are
  exposed via `MetricGroup.addSpan(...)` / `addEvent(...)` (experimental).
  They share the same factory+SPI shape: `TraceReporterFactory` lives in
  `META-INF/services/...TraceReporterFactory`, ditto `EventReporterFactory`.
  `flink-metrics-otel` and `flink-metrics-slf4j` implement all three.
- **Configurable scope and filtering.** Reporter scope delimiter, included
  variables, and filters are configured in `MetricOptions` and consumed by
  `ReporterSetup`/`ReporterSetupBuilder` in `flink-runtime`.

## Important public APIs

- `org.apache.flink.metrics.Metric` — root marker interface; subtypes
  `Counter`, `Gauge<T>`, `Histogram`, `Meter`, `View`.
- `org.apache.flink.metrics.MetricGroup` — registers metrics + creates
  subgroups. Returned by `RuntimeContext.getMetricGroup()`.
- `org.apache.flink.metrics.reporter.MetricReporter` + `MetricReporterFactory`
  — implement these in a custom reporter jar and register via
  `META-INF/services`. Add an entry to `metrics.reporters` /
  `metrics.reporter.<name>.factory.class` to enable.
- `org.apache.flink.metrics.MetricConfig` — `Properties` subclass holding a
  reporter's configuration (auto-populated from
  `metrics.reporter.<name>.<key>`).
- `org.apache.flink.metrics.SimpleCounter` /
  `org.apache.flink.metrics.ThreadSafeSimpleCounter` — default counter impls.
- `org.apache.flink.metrics.MeterView` — wraps a `Counter` and computes
  a moving-average rate; updated by `View#update()` triggered from a runtime
  view-updater thread.

## Internal flows

- **Metric registration.** Operator calls `metricGroup.counter("name")`
  → `AbstractMetricGroup.addMetric()` in `flink-runtime`
  → `MetricRegistryImpl.register(metric, name, group)`
  → for each configured `ReporterSetup`, calls
  `reporter.notifyOfAddedMetric(metric, qualifiedName, group)`.
- **Periodic report.** `MetricRegistryImpl`'s scheduled executor invokes
  `Scheduled#report()` on reporters that implement it at the configured
  interval; the reporter walks its cached metric map and pushes to the
  backend (Prometheus: serializes to Prom text format for the HTTP scrape;
  Datadog: builds an HTTP request; StatsD: UDP send; etc.).
- **Pull-style reporters.** Prometheus' `PrometheusReporter` starts an
  embedded HTTP server (port from `metrics.reporter.<name>.port`) that on
  scrape walks the metric snapshot. JMX exposes each metric as an MBean
  immediately on registration — no scheduled flush.
- **View updates.** `MeterView` (and any `View` subclass) is driven by a
  separate `ViewUpdater` task in `MetricRegistryImpl` so the moving-average
  state stays fresh between reporter intervals.

## Tests

- Per-reporter unit tests under `src/test/java`: e.g.
  `PrometheusReporterTest`, `DatadogHttpReporterTest`,
  `InfluxdbReporterTest`, `StatsDReporterTest`, `JMXReporterTest`,
  `GraphiteReporterTest`. They typically spin up an in-memory server (a
  mock HTTP server, a UDP socket, an MBeanServer, …) and assert the wire
  format.
- `flink-metrics-core` carries API contract tests for `MeterView`,
  `MetricConfig`, and the standard `MetricGroup` factory methods.
- Integration with the registry/reporter lifecycle is tested in
  `flink-runtime`'s `MetricRegistryImplTest`, `ReporterSetupTest`.

## Pitfalls & gotchas

- **Plugin classloader isolation.** Reporter jars in `plugins/` get their own
  classloader. Anything they reference outside their shaded contents must be
  in `flink-dist`'s `lib/`. Putting a reporter in `lib/` (instead of
  `plugins/`) re-introduces dependency conflicts the shading was meant to
  avoid.
- **`MetricReporterFactory` is mandatory in modern Flink.** The legacy
  `metrics.reporter.<name>.class` config (instantiating `MetricReporter` by
  reflection) has been removed; you must register a factory.
- **`@Public` API stability.** `Metric`, `MetricGroup`, `Counter`, `Gauge`,
  `Histogram`, `Meter`, `MetricReporter`, and `MetricReporterFactory` are
  `@Public` — changes break user reporters. Spans/Events are still
  `@Experimental`.
- **Histogram allocation cost.** `Histogram.update()` is called on the hot
  path; the default `DescriptiveStatisticsHistogram` (in `flink-runtime`)
  is sliding-window with synchronization — be careful enabling on per-record
  metrics.
- **Reporter intervals vs view updaters.** `MeterView` updates on its own
  schedule. If you set a very long reporter interval the meter still updates
  internally, but you'll only see rate samples at the report cadence.
- **Counter is signed.** `Counter.inc(n)` accepts negative `n` and `dec()`
  exists. Reporters that mandate monotonic counters (Prometheus, Datadog)
  may behave unexpectedly if you decrement.
- **Variable exclusion.** Each reporter has its own
  `metrics.reporter.<name>.scope.variables.excludes` to drop high-cardinality
  variables (e.g. `<task_attempt_id>`) — forgetting this on Prometheus
  explodes label cardinality.
- **Shaded vs unshaded clients.** Most reporter pom files shade their backend
  client (`maven-shade-plugin` with `<include>io.prometheus:*</include>` /
  `<include>com.datadoghq:*</include>`). User code that wants to use the
  raw client should not pull the reporter jar.

## Related modules / codedocs

- `flink-runtime/.../metrics/{MetricRegistryImpl,ReporterSetup,AbstractReporterSetup,
  ReporterSetupBuilder,TraceReporterSetup,EventReporterSetup}.java` — the
  glue that turns the reporter API into a running reporter.
- `flink-runtime/.../metrics/groups` — concrete `MetricGroup` implementations
  (`TaskManagerMetricGroup`, `JobManagerJobMetricGroup`,
  `OperatorMetricGroup`, …).
- `flink-core/.../configuration/MetricOptions.java` — all `metrics.*` config
  options.
- `flink-state-backends/flink-statebackend-rocksdb` —
  `RocksDBNativeMetricMonitor` is an example of a backend exposing its
  internals through `flink-metrics-core`.
- `flink-dstl/flink-dstl-dfs` — `ChangelogStorageMetricGroup` is a custom
  `ProxyMetricGroup` built on this API.
- `codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md`,
  `codedocs/flink-exactly-once-checkpointing-deep-dive.md` — backend code
  paths whose instrumentation flows through these reporters.
