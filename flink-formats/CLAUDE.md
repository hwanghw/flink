# `flink-formats`

## Purpose
Aggregator parent module for Flink's serialization-format integrations: Avro, Parquet, ORC, CSV, JSON, Protobuf, raw/compressed bytes, Hadoop sequence files, etc. Each sub-module produces a self-contained jar that supplies a `(De)SerializationSchema`, `BulkWriter` / `BulkFormat`, or `StreamFormat` implementation that connectors and the Table API can plug in. Nothing here runs by itself — formats are always consumed by a connector or Table source/sink.

## Where it fits
- **DataStream connectors** pull formats in via builder APIs (e.g. `FileSink.forBulkFormat(path, ParquetAvroWriters.forSpecificRecord(...))`, `KafkaSource.builder().setDeserializer(...)`).
- **Table / SQL API** discovers formats via Java SPI from DDL: `WITH ('format' = 'avro', ...)`. Each module exposes a `*FormatFactory` registered in `META-INF/services/org.apache.flink.table.factories.Factory`.
- **`flink-sql-*` shaded variants** (built only when the `sql-jars` profile is active — default-on) bundle the format + transitive deps into a single uber-jar for SQL Client / `lib/` drop-in use.

## Maven coordinates
Parent pom packaging `pom`. Direct children (declared in `pom.xml` `<modules>`):

### Schema-aware (block / record formats)
- `flink-avro` — Apache Avro binary + JSON-Avro. Includes `AvroSerializer` (built-in Flink type-system integration), `AvroRowData*Schema`, `AvroSchemaConverter`, `AvroFileFormatFactory` (file source/sink).
- `flink-avro-confluent-registry` — Confluent Schema-Registry-backed Avro. Adds magic-byte framing, schema caching, plus Debezium-Avro variant.
- `flink-parquet` — Apache Parquet (version pinned via `flink.format.parquet.version`, currently `1.15.2`). Vectorized columnar reader (`ParquetVectorizedInputFormat`), row-based RowData writer, plus Avro/Protobuf bridges.
- `flink-orc` — Apache ORC with Hive shims (`OrcShimV200`/`V210`/`V230`). Vectorized batch reader, `RowDataVectorizer`, predicate pushdown via `OrcFilters`.
- `flink-orc-nohive` — Same ORC functionality but compiled against `orc-core` (no Hive runtime) for clusters that cannot ship Hive jars.
- `flink-sequence-file` — Hadoop `SequenceFile` writer (key/value Writable pairs).
- `flink-hadoop-bulk` — Generic path-based bulk writer that wraps any Hadoop `OutputFormat`; provides a two-phase commit via `HadoopFileCommitter` for atomic rename semantics on HDFS/S3A.

### Schemaless / text
- `flink-csv` — RFC-4180 CSV with `CsvReaderFormat` (StreamFormat), `RowCsvInputFormat`, and `CsvBulkWriter`.
- `flink-json` — JSON via Jackson. Includes specialized CDC dialects: `canal`, `debezium`, `maxwell`, `ogg`.
- `flink-protobuf` — Protocol Buffers. Uses **runtime code generation** (`PbCodegen*`) to emit per-message ser/de classes for speed.
- `flink-compress` — Pluggable compression bulk writer (`CompressWriterFactory`) over any Hadoop `CompressionCodec` (gzip/snappy/bzip2/lz4/...).

### Shared
- `flink-format-common` — Tiny utility module: `TimestampFormat` (SQL vs ISO_8601), `TimeFormats`, `Converter`. Pulled in by every format that handles temporal types.

### SQL uber-jars (profile `sql-jars`, active by default)
`flink-sql-csv`, `flink-sql-json`, `flink-sql-orc`, `flink-sql-parquet`, `flink-sql-avro`, `flink-sql-avro-confluent-registry`, `flink-sql-protobuf` — shaded fat-jars; do not contain any source, just `pom.xml` + relocation rules.

## Dependencies

### Direct (from the parent pom)
- `org.slf4j:slf4j-api` (provided)
- `flink-test-utils-junit` (test compile)

Everything else (Flink core APIs, Hadoop, Avro, Parquet, ORC, Jackson, …) is declared per sub-module. The parent intentionally marks slf4j as `provided` so format jars never leak it into uber-jars.

### Used by
- **`flink-connector-files`** — primary consumer for `BulkFormat`/`BulkWriter` (file source/sink).
- **`flink-connector-kafka`**, `flink-connector-pulsar`, etc. — use `DeserializationSchema` / `SerializationSchema`.
- **`flink-table-runtime`** + **`flink-table-planner`** — discover `*FormatFactory` via SPI.
- **`flink-python`** — exposes a subset (CSV/JSON/Avro) to PyFlink.
- **`flink-connector-hive`** — uses `flink-orc` shims and `flink-parquet` vector readers.

## Source layout per sub-module
Each format follows the same pattern (paths relative to module root):

| Module | Schema/factory | DataStream API | Table/RowData API |
|---|---|---|---|
| `flink-avro` | `AvroSchemaConverter`, `AvroFormatFactory`, `AvroFileFormatFactory` | `AvroInputFormat`, `AvroOutputFormat`, `AvroWriters`, `AbstractAvroBulkFormat` | `AvroRowDataDeserializationSchema`, `AvroRowDataSerializationSchema`, `AvroToRowDataConverters`, `RowDataToAvroConverters` |
| `flink-parquet` | `ParquetSchemaConverter`, `ParquetFileFormatFactory` | `ParquetWriterFactory`, `ParquetBulkWriter`, `AvroParquetReaders`, `AvroParquetWriters` | `ParquetColumnarRowInputFormat`, `ParquetVectorizedInputFormat`, `row/ParquetRowDataBuilder`, `row/ParquetRowDataWriter` |
| `flink-orc` | `OrcFileFormatFactory`, `OrcSplitReaderUtil` | `writer/OrcBulkWriterFactory`, `Vectorizer` | `OrcColumnarRowInputFormat`, `vector/RowDataVectorizer`, `OrcFilters` |
| `flink-csv` | `CsvFormatFactory`, `CsvFileFormatFactory`, `CsvCommons` | `CsvReaderFormat`, `CsvBulkWriter`, `RowCsvInputFormat` | `CsvRowDataDeserializationSchema`, `CsvRowDataSerializationSchema`, `CsvToRowDataConverters` |
| `flink-json` | `JsonFormatFactory`, `JsonRowSchemaConverter` | `JsonDeserializationSchema`, `JsonSerializationSchema` | `JsonRowDataDeserializationSchema`, `JsonParserRowDataDeserializationSchema`, `RowDataToJsonConverters` + `canal/`, `debezium/`, `maxwell/`, `ogg/` |
| `flink-protobuf` | `PbFormatFactory`, `PbFileFormatFactory` | n/a (Table-focused) | `PbRowDataSerializationSchema`, `PbRowDataDeserializationSchema`, `serialize/PbCodegen*`, `deserialize/PbCodegen*` |
| `flink-avro-confluent-registry` | `RegistryAvroFormatFactory`, `debezium/DebeziumAvroFormatFactory` | `ConfluentRegistryAvro(De)SerializationSchema` | `CachedSchemaCoderProvider`, `ConfluentSchemaRegistryCoder` |
| `flink-orc-nohive` | (inherits `OrcFileFormatFactory` via shim) | `OrcNoHiveBulkWriterFactory`, `OrcNoHiveColumnarRowInputFormat` | reuses `flink-orc` converters |
| `flink-sequence-file` | n/a (DataStream only) | `SequenceFileWriter`, `SequenceFileWriterFactory` | n/a |
| `flink-compress` | n/a | `CompressWriterFactory`, `CompressWriters`, `extractor/Extractor` | n/a |
| `flink-hadoop-bulk` | n/a | `HadoopPathBasedBulkWriter`, `HadoopPathBasedPartFileWriter`, `committer/HadoopRenameFileCommitter` | n/a |

## Architecture & key concepts

### Format factory discovery
`*FormatFactory` classes implement `DeserializationFormatFactory` / `SerializationFormatFactory` / `BulkReaderFormatFactory` / `BulkWriterFormatFactory` and are registered via `META-INF/services/org.apache.flink.table.factories.Factory`. The Table planner resolves `'format' = 'xyz'` by matching `factoryIdentifier()`.

### `BulkFormat` vs `StreamFormat`
- **BulkFormat** (Parquet, ORC, Avro file, sequence-file) — block-oriented; reads a whole row-group / stripe into a vectorized batch. Implemented via `BulkFormat` (file source) and `BulkWriter` (file sink). Better throughput, requires schema.
- **StreamFormat** (CSV, JSON, raw line) — record-oriented; reads one record at a time. Implemented via `StreamFormat` / `DeserializationSchema`. Simpler, no schema framing.

### RowData conversion layer
Every schema-aware format ships paired `*ToRowDataConverters` and `RowDataTo*Converters` classes. These translate between the format's native record (`GenericRecord`, `ColumnVector`, `JsonNode`, `Message`, …) and Flink's internal `RowData` representation used by the Table runtime.

### Vectorized columnar readers (Parquet/ORC)
`flink-parquet/vector/` and `flink-orc/vector/` expose batches as Flink's `ColumnarRowData` over `ColumnVector`s. The Parquet readers under `vector/reader/` decode one column at a time; ORC uses Hive's `VectorizedRowBatch` adapted via `AbstractOrcColumnVector` subclasses. Predicate pushdown lives in `OrcFilters` (ORC) and is row-group-only for Parquet.

### Schema evolution
- **Avro** — writer/reader-schema resolution is done by Avro itself; `AvroSerializerSnapshot` carries the writer schema in checkpoints so state can be restored after a schema change.
- **Confluent registry** — schema id is read from the 5-byte magic prefix; the reader fetches the writer schema by id and uses Avro's resolver. `CachedSchemaCoderProvider` caches them per task.
- **Parquet** — column projection by name; missing columns become `null`. Type widening is *not* automatic — re-deriving the `MessageType` from the new Flink schema is required.
- **ORC** — projection by index (via `OrcShim.computeProjectionMask`); column renames are not supported.

### Protobuf code generation
`PbCodegenSerializeFactory` / `PbCodegenDeserializeFactory` build Java source at runtime via `PbCodegenAppender` (`StringBuilder`-based), then Janino-compile it. This avoids reflection per record but means the first call after deploy pays a compile cost; classloading bugs typically surface here.

## Important public APIs
- `org.apache.flink.formats.avro.AvroDeserializationSchema` / `AvroSerializationSchema` — DataStream entry points (factory methods `forGeneric`, `forSpecific`).
- `org.apache.flink.formats.avro.RegistryAvroDeserializationSchema` (+ Confluent subclass).
- `org.apache.flink.formats.parquet.ParquetWriterFactory` / `ParquetBulkWriter`; `row.ParquetRowDataBuilder.createWriterFactory(...)` for Table API.
- `org.apache.flink.formats.parquet.avro.AvroParquetReaders` / `AvroParquetWriters` — most common DataStream entry point.
- `org.apache.flink.orc.writer.OrcBulkWriterFactory` (Hive variant) / `org.apache.flink.orc.nohive.OrcNoHiveBulkWriterFactory`.
- `org.apache.flink.formats.csv.CsvReaderFormat.forSchema(...)` / `.forPojo(...)`.
- `org.apache.flink.formats.json.JsonDeserializationSchema` / `JsonSerializationSchema` (+ Debezium/Canal/Maxwell/Ogg variants).
- `org.apache.flink.formats.compress.CompressWriters.forExtractor(...)`.
- `org.apache.flink.formats.common.TimestampFormat` (`SQL` vs `ISO_8601`) — shared option enum referenced by JSON, CSV, Debezium-Avro factories.

## Pitfalls & gotchas
- **Confluent registry**: schema-id mapping is **subject + version → id**. If the subject naming strategy differs between producer (`TopicNameStrategy`) and Flink (`RecordNameStrategy`), deserialization will pull the wrong schema. `CachedSchemaCoderProvider` caches forever; restart the job to invalidate.
- **Parquet row-group sizing**: default 128 MB row groups are huge — a Flink TM with a small managed-memory fraction will OOM. Tune `parquet.block.size` and `parquet.page.size` per sink; the writer respects `org.apache.parquet.hadoop.ParquetWriter` defaults if not overridden.
- **Parquet logical types**: timestamps in Parquet may be `INT96` (legacy Hive), `TIMESTAMP_MILLIS`, or `TIMESTAMP_MICROS`. `ParquetSchemaConverter` handles all three but the reader has to know which; mismatched precision silently truncates.
- **ORC Hive shim vs no-Hive**: `flink-orc` brings `hive-storage-api`; if your cluster classpath has a conflicting Hive version, switch to `flink-orc-nohive`. Mixing both on the same classpath causes `NoSuchMethodError` in `VectorizedRowBatch`.
- **ORC vectorized vs row reader**: `AbstractOrcFileInputFormat` is vectorized only — there is no row-oriented reader. Custom `Vectorizer` implementations must handle batch fill themselves; partial batches at end-of-stripe are a common bug.
- **CSV null handling**: empty fields are not `null` — they are empty strings. Use `csv.null-literal` to map a sentinel. Quoted empty `""` is also a string, never null.
- **CSV ignore-parse-errors**: `csv.ignore-parse-errors=true` swallows everything including schema mismatches; failures become silent `null` rows. Prefer hardening the upstream producer.
- **JSON deep nesting / dynamic schemas**: `JsonRowDataDeserializationSchema` requires a static `RowType`. For dynamic JSON, use a `STRING` column and parse downstream — there is no built-in "ANY JSON" type. `JsonParserRowDataDeserializationSchema` (streaming Jackson parser) is faster on wide records but allocates more on deep nesting.
- **Avro `GenericRecord` Kryo fallback**: Without `flink-avro` on the classpath, Flink falls back to Kryo for `GenericRecord`, which is slow and not schema-evolution-safe. Always shade `flink-avro` into your user-jar.
- **Protobuf codegen + classloading**: Janino-compiled classes are loaded into the user classloader. Hot-redeploying a job with a *modified* `.proto` while the JM cache still has the old class produces `IncompatibleClassChangeError`. Restart the cluster or rename the message.
- **Hadoop-bulk committer**: `HadoopRenameFileCommitter` relies on atomic rename. On S3 (s3a, no Magic committer) rename is **not atomic** — use the S3-specific committer or accept partial-file risk on JM failover.

## Related modules / docs
- `flink-connector-files/CLAUDE.md` — primary consumer of `BulkFormat`/`BulkWriter`.
- `flink-connectors/flink-connector-kafka/CLAUDE.md` — `DeserializationSchema` consumer; see Confluent-Avro section.
- `flink-table/flink-table-common/CLAUDE.md` — defines the `*FormatFactory` SPI contracts these modules implement.
- `flink-table/flink-table-planner/CLAUDE.md` — explains how `WITH ('format' = ...)` is resolved.
- `flink-connector-hive/CLAUDE.md` — heavy user of `flink-orc` and `flink-parquet` vector readers.
