# `flink-connectors`

## Purpose

Aggregator (POM-only) module for first-party Flink connectors — the source / sink
implementations that bridge Flink pipelines with external systems (filesystems,
message queues, databases, generators, etc.) and the shared framework code those
connectors build on.

In Flink 2.x the vast majority of "real" connectors (Kafka, Kinesis, Pulsar,
MongoDB, Elasticsearch, JDBC, Cassandra, RabbitMQ, HBase, AWS sinks, etc.) have
been moved to **their own repositories** under the `apache/flink-connector-*`
organization so they can release on independent cadences. What remains in-tree
here is:

1. The connector **framework** (`flink-connector-base`) used by every external
   connector.
2. The **file** connector + its bucketed sink building blocks.
3. The **datagen** source (for testing / examples / `gen_id`-style generators).
4. Hadoop **InputFormat compatibility** glue.

## Where it fits

```
User DataStream / Table API code
         |
         v
  Source<T,Split,EnumChk>  or  Sink<InputT>   (new API, this module)
         |                              ^
         |  legacy fallback:            |
         v                              |
  SourceFunction / SinkFunction (legacy, in flink-streaming-java)
```

Each connector exposes either:

- A **new-API** `Source` / `Sink` (preferred — checkpointable, backpressure-aware,
  unified batch + streaming), or
- A **legacy** `SourceFunction` / `SinkFunction` (kept for backwards compat —
  these live in `flink-streaming-java`, not here; e.g.
  `TwoPhaseCommitSinkFunction` at
  `flink-streaming-java/.../sink/legacy/TwoPhaseCommitSinkFunction.java`).

## Maven coordinates

Parent POM (`packaging=pom`), `org.apache.flink:flink-connectors:2.3-SNAPSHOT`.

Sub-modules declared in `pom.xml`:

### Framework

- **`flink-connector-base`** — common base classes and utilities used by both
  in-tree and out-of-tree connectors. The "SDK" for writing a Flink connector:
  - `org.apache.flink.connector.base.DeliveryGuarantee` — `NONE` / `AT_LEAST_ONCE`
    / `EXACTLY_ONCE` enum used by sinks.
  - `source.reader.*` — `SourceReaderBase`,
    `SingleThreadMultiplexSourceReaderBase`, `RecordEmitter`, `RecordEvaluator`,
    `RecordsWithSplitIds`, fetcher and `SplitReader` SPI. Concrete connectors
    extend `SourceReaderBase` and supply a `SplitReader` rather than
    re-implementing the threading and checkpoint plumbing.
  - `source.hybrid.HybridSource` — switching source that chains multiple
    underlying sources (e.g. read historical files then switch to Kafka).
  - `sink.AsyncSinkBase` / `sink.writer.AsyncSinkWriter` — base for async
    request-batching sinks used by Kinesis / DynamoDB / Firehose connectors;
    handles batching, retries, rate-limiting (AIMD), in-flight tracking.
  - `sink.writer.strategy.*` — congestion-control / rate-limiting strategies
    (AIMD, CongestionControl) shared by async sinks.

### In-tree connectors

- **`flink-file-sink-common`** — shared bucket-writer primitives used by
  `FileSink` and downstream connectors (e.g. Iceberg, Hive). Contains
  `BucketWriter`, `BucketAssigner`, `RollingPolicy`, `PartFileInfo`,
  `InProgressFileWriter`, `BulkPartWriter` / `RowWisePartWriter`, plus
  `OutputFileConfig`. The compaction-aware `CompactingFileWriter` lives here.
- **`flink-connector-files`** — the unified file source + sink:
  - `file.src.FileSource` (new-API, `AbstractFileSource` based) — split-based
    bounded or continuously-monitored file reading; pluggable `FileEnumerator`
    (recursive splitting / non-splitting / regex filters) and `BulkFormat` /
    `StreamFormat` readers (`TextLineInputFormat`, Avro, Parquet, etc.).
  - `file.sink.FileSink` (new-API) — exactly-once bucketed file sink with
    `committer` + `compactor` (small-file compaction) sub-packages.
  - `file.table.*` — Table-API integration (`FileSystemTableSource` /
    `FileSystemTableSink`, partition writers, `FileSystemCommitter`,
    `MetastoreCommitPolicy` for Hive-style partition commit notification).
- **`flink-connector-datagen`** — `DataGeneratorSource` (new-API) plus the
  `GeneratorFunction` SPI. Used by `StreamExecutionEnvironment.fromSequence(...)`,
  `fromElements(...)` and as a test / example source. Replaces the older
  legacy `DataGeneratorSource` SourceFunction.
- **`flink-connector-datagen-test`** — pure test module exercising datagen +
  generator source ITCases (`DataGeneratorSourceITCase`,
  `FromElementsGeneratorSourceITCase`) and architecture tests.
- **`flink-hadoop-compatibility`** — `HadoopInputs`, `HadoopUtils`, plus
  `mapred` adapters to run Hadoop `InputFormat` / `OutputFormat` implementations
  under Flink. Useful for legacy MR-format integration; not a connector to a
  specific system.

### Externalized connectors (NOT in this module anymore)

These previously lived under `flink-connectors/flink-connector-*` but have been
extracted to their own repos in the 2.x line. **Do not look for them here**:

- `flink-connector-kafka` → `apache/flink-connector-kafka`
- `flink-connector-kinesis`, `flink-connector-aws-*` →
  `apache/flink-connector-aws`
- `flink-connector-pulsar` → `apache/flink-connector-pulsar`
- `flink-connector-mongodb` → `apache/flink-connector-mongodb`
- `flink-connector-elasticsearch` → `apache/flink-connector-elasticsearch`
- `flink-connector-jdbc` → `apache/flink-connector-jdbc`
- `flink-connector-cassandra` → `apache/flink-connector-cassandra`
- `flink-connector-rabbitmq` → `apache/flink-connector-rabbitmq`
- `flink-connector-hbase` → `apache/flink-connector-hbase`
- Iceberg / Delta / Hudi connectors live in their respective project repos.

Tests, examples, and tutorial code in this repo will reference those artifacts
via Maven coordinates only.

## Architecture and key concepts

### Source API (new, Flink 1.13+, FLIP-27)

Core abstractions (defined in `flink-core` / `flink-core-api`, implemented by
this module's connectors):

- `Source<T, SplitT extends SourceSplit, EnumChkT>` — top-level factory:
  - `createReader(SourceReaderContext)` → `SourceReader<T, SplitT>`
  - `createEnumerator(SplitEnumeratorContext)` → `SplitEnumerator<SplitT, EnumChkT>`
  - `getSplitSerializer()` / `getEnumeratorCheckpointSerializer()` for state.
- `SplitEnumerator` runs on the **JobManager**: discovers work, assigns splits
  to readers, checkpoints its own state (e.g. last-discovered file timestamp).
- `SourceReader` runs on the **TaskManagers**: consumes assigned splits and
  emits records to the engine. Most connectors extend `SourceReaderBase` from
  `flink-connector-base` and only have to implement a `SplitReader` that knows
  how to fetch a chunk of records for one split.
- `SourceSplit` — a unit of work (e.g. one file + offset range, one Kafka
  partition + offset range).

### Sink API V2 (new, FLIP-191 / FLIP-143)

- `Sink<InputT>` (in `flink-core` `api/connector/sink2`) — factory for
  `SinkWriter<InputT>` per subtask.
- Optional mix-ins:
  - `TwoPhaseCommittingSink` / `Committer<CommT>` — exactly-once via 2PC. The
    writer emits committables on checkpoint; a downstream `Committer` operator
    commits them after the checkpoint completes (matches the
    pre-commit / commit transactional pattern).
  - `StatefulSink` — writer with explicit checkpointable state.
  - `SupportsWriterState`, `SupportsCommitter`, etc.
- The DataStream V2 split (`api/connector/dsv2/Source` / `Sink` in
  `flink-core-api`) is the lighter-weight surface area for the next-gen
  DataStream V2 API; in-tree connectors here implement both where applicable.

### Sink patterns

See `codedocs/flink-sink-patterns-comparison.md` for the full comparison. In
brief:

- **At-least-once / idempotent**: simplest. SinkWriter flushes on every
  checkpoint; duplicates handled by the external system (e.g. upserts by
  primary key, ES `_id`). No committer.
- **Exactly-once via 2PC** (transactional): `TwoPhaseCommittingSink`. Examples:
  `FileSink` (rename pending file on commit), Kafka producer transactions,
  JDBC XA. The `Committer` runs **after** the checkpoint barrier is fully
  acknowledged, so commits are guaranteed-not-rolled-back.
- **Async batched sink**: extend `AsyncSinkBase` /`AsyncSinkWriter` from
  `flink-connector-base` — batches, rate-limits, retries, and exposes
  delivery-guarantee semantics. Used for Kinesis / DynamoDB / Firehose.

### Hybrid sources

`org.apache.flink.connector.base.source.hybrid.HybridSource` lets a pipeline
chain heterogeneous sources end-to-end (e.g. read a backfill from S3, then
switch to a live Kafka topic) while preserving event-time and a single
checkpoint stream. The `HybridSourceSplitEnumerator` tracks which child source
is active and emits `SwitchSourceEvent` to readers.

## Important public APIs (entry points to remember)

- `org.apache.flink.api.connector.source.Source` — new source factory.
- `org.apache.flink.api.connector.sink2.Sink` — sink V2 factory.
- `org.apache.flink.api.connector.dsv2.Source` / `Sink` — DataStream V2 surface.
- `org.apache.flink.connector.base.DeliveryGuarantee`.
- `org.apache.flink.connector.base.source.reader.SourceReaderBase` /
  `SingleThreadMultiplexSourceReaderBase`.
- `org.apache.flink.connector.base.source.reader.splitreader.SplitReader`.
- `org.apache.flink.connector.base.sink.AsyncSinkBase` /
  `AsyncSinkBaseBuilder`.
- `org.apache.flink.connector.base.source.hybrid.HybridSource`.
- `org.apache.flink.connector.file.src.FileSource` /
  `org.apache.flink.connector.file.sink.FileSink`.
- `org.apache.flink.connector.datagen.source.DataGeneratorSource` /
  `GeneratorFunction`.
- Legacy (kept for compat, **avoid in new code**):
  `org.apache.flink.streaming.api.functions.sink.legacy.TwoPhaseCommitSinkFunction`.

## Internal flows

1. **Split discovery and assignment** — `SplitEnumerator` on the JobManager
   discovers splits (file scan, Kafka metadata fetch, etc.). Assigns to
   readers via `SplitEnumeratorContext.assignSplit`. State checkpointed in
   `EnumChkT`.
2. **Reader pull loop** — `SourceReaderBase` runs a fetcher thread that calls
   the supplied `SplitReader.fetch()`. Records are buffered as
   `RecordsWithSplitIds`, then drained by the main task thread via
   `pollNext(ReaderOutput)`, which delegates to a `RecordEmitter` to convert
   the raw record into the engine record (and update split state for
   checkpointing).
3. **Checkpoint barriers through sinks** — the engine injects a checkpoint
   barrier; `SinkWriter.flush(true)` is called, then `prepareCommit()` returns
   committables that travel to the `Committer` operator. Once the checkpoint
   is **complete** (notification), `Committer.commit()` is invoked. This is
   how `FileSink` makes "pending → finished" file rename atomic with respect
   to the Flink checkpoint.

## Tests

Each sub-module follows the standard pattern:
- `src/test/java/.../*Test.java` — unit tests.
- `src/test/java/.../*ITCase.java` — integration tests using
  `MiniClusterWithClientResource` / `MiniClusterExtension`.
- `archunit-violations/` per module — pinned ArchUnit violations to prevent
  layering / API regressions.
- `flink-connector-datagen-test` is the dedicated ITCase module for datagen
  (kept separate so the source module stays light).

For external connectors, mock the external system with embedded fakes (e.g.
embedded Kafka / Pulsar in their respective repos) — there is no shared
testing harness here beyond what `flink-connector-base` and `flink-test-utils`
provide (`SourceReaderTestBase`, `SinkWriterMetricsTestBase`, etc., live in
`flink-connector-base` test-jars).

## Pitfalls and gotchas

- **New `Source` vs legacy `SourceFunction`** — always prefer the new API.
  Legacy `SourceFunction` lacks split-based parallel discovery, has weak
  checkpoint semantics, and is being deprecated. `RichParallelSourceFunction`
  patterns should be ported to `Source` + `SplitEnumerator` + `SourceReader`.
- **Exactly-once requires a committer** — using `DeliveryGuarantee.EXACTLY_ONCE`
  on a sink that has no `TwoPhaseCommittingSink` implementation silently
  degrades. Check the connector docs and prefer transactional or idempotent
  paths.
- **2PC commit is async** — the `Committer` runs after `notifyCheckpointComplete`,
  so end-to-end visibility lags one checkpoint interval. Tune
  `execution.checkpointing.interval` accordingly for downstream consumers.
- **Backpressure in async sinks** — `AsyncSinkWriter` keeps a bounded
  in-flight queue; misconfiguring `maxBatchSize` / `maxInFlightRequests` /
  `maxBufferedRequests` against the target service rate causes either
  starvation or OOM. The AIMD strategy auto-adjusts but only if you let it.
- **Watermark generation in sources** — supplied via
  `WatermarkStrategy` on the `fromSource(...)` call; the new `Source` API
  delegates per-split watermark tracking, which prevents the "idle partition
  holding back watermarks" issue that plagued legacy sources. Verify
  `withIdleness(...)` is set for sparse partitions.
- **Externalized connector version drift** — the externalized connector repos
  release on independent cadences. Pin compatible versions in your job's
  `pom.xml`; do not assume the connector ships with Flink itself.
- **Hadoop compatibility is a legacy bridge** —
  `flink-hadoop-compatibility` is for running Hadoop `InputFormat` /
  `OutputFormat`. Do not confuse with the `flink-shaded-hadoop` runtime
  dependency used by HDFS / YARN.

## Related modules and codedocs

- `flink-streaming-java` — hosts legacy `SourceFunction` / `SinkFunction`
  and `TwoPhaseCommitSinkFunction`; runtime operators that wrap the
  new-API sources / sinks live here too
  (`SourceOperator`, `SinkWriterOperator`, `CommitterOperator`).
- `flink-core` / `flink-core-api` — defines `Source`, `Sink` (v2), DSv2
  `Source` / `Sink`, `SourceSplit`, `SplitEnumerator`, watermark types.
- `flink-runtime` — `SourceCoordinator` (runs the `SplitEnumerator` on JM),
  checkpoint coordination feeding the committer.
- `flink-table-planner` — Table-API `DynamicTableSource` / `DynamicTableSink`
  adapters that wrap the connectors in this module.

Deep dives in `codedocs/`:

- `codedocs/flink-kafka-connector-source-sink-architecture.md` — externalized
  Kafka connector internals (still uses the framework here).
- `codedocs/flink-iceberg-sink-connector-deep-dive.md` — Iceberg sink architecture,
  reusing `flink-file-sink-common` primitives.
- `codedocs/flink-iceberg-source-connector-deep-dive.md` — Iceberg source
  (FLIP-27 enumerator + readers, incremental append scan, MOR read-time deletes).
- `codedocs/flink-cassandra-connector-wal-exactly-once-architecture.md` —
  Cassandra connector exactly-once via WAL.
- `codedocs/flink-sink-patterns-comparison.md` — full sink-pattern comparison
  (at-least-once vs 2PC vs idempotent vs async batched).
