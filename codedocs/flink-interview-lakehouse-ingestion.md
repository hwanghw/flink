# Streaming Lakehouse Ingestion (Kafka → Flink → Iceberg) — System Design Interview (45 min)

Design a streaming ingestion pipeline that consumes CDC (Change Data Capture) events from
multiple database tables via Kafka, processes them through Flink, and writes them to an
Iceberg lakehouse with exactly-once semantics, schema evolution, and upsert support.

---

## 1. Requirements Gathering (5 min)

| Requirement | Answer (assumed) |
|---|---|
| Event source | Kafka topics: `cdc.orders`, `cdc.users`, `cdc.products` (Debezium CDC, Avro) |
| CDC format | Debezium envelope: `{before, after, op, ts_ms, source}` |
| Operations | INSERT (op=c), UPDATE (op=u), DELETE (op=d) |
| Schema evolution | Columns added/renamed over time — must handle without pipeline restart |
| Avg event size | ~1 KB (Avro with schema registry) |
| Peak throughput | 100,000 events/sec across all topics |
| Kafka partitions | 32 per topic (96 total) |
| Latency SLA | < 2 minutes (data visible in Iceberg within 2 min of source change) |
| Delivery guarantee | Exactly-once (financial data — no duplicates, no losses) |
| Sink | Apache Iceberg tables (one per source table) on S3 |
| Iceberg format version | V2 (required for equality deletes / upsert) |
| Query engine | Trino / Spark for downstream analytics |
| Table size | orders: 500M rows (100 GB), users: 50M rows (10 GB), products: 5M rows (1 GB) |
| Update ratio | orders: 30% updates, users: 5% updates, products: 1% updates |

---

## 2. Capacity Estimation (5 min)

```
Data rate:
  100K events/sec × 1 KB = 100 MB/s (peak)
  Sustained: ~50K events/sec = 50 MB/s

Source parallelism:
  3 topics × 32 partitions = 96 → source parallelism = 96
  (or use Dynamic Iceberg Sink with single multi-topic source)

State:
  Minimal Flink state — the connector writes files directly to S3.
  IcebergCommitter maintains: Map<checkpointId, DeltaManifests>
  ~10 MB of manifest metadata in operator state.
  No per-key state needed (CDC events are self-describing).

TaskManagers:
  96 / 4 slots = 24 TMs
  Memory: 2 GB heap + 1 GB managed + 256 MB network = ~4 GB each
  Total: 24 × 4 GB = 96 GB cluster

Checkpoint:
  Interval: 60 seconds (matches 2-min latency SLA)
  State: ~10 MB (just manifest metadata — very small)
  Duration: < 1 second

Iceberg file output:
  100 MB/s ÷ 96 writers ≈ 1 MB/s per writer
  Checkpoint every 60s → each writer produces ~60 MB per file (target 128 MB not reached)
  96 writers × 1 file/checkpoint × 60 checkpoints/hr = 5,760 files/hr → need compaction!
  Compaction: PostCommitTopology runs RewriteDataFiles periodically
```

---

## 3. Architecture (15 min)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    STREAMING LAKEHOUSE INGESTION                              │
│                                                                               │
│  ┌────────────┐    ┌──────────────────────────────────────────────┐          │
│  │ MySQL /    │    │              Flink Cluster                    │          │
│  │ PostgreSQL │    │                                              │          │
│  │            │    │  ┌──────────────────────────────────────┐    │          │
│  │ Debezium   │    │  │  KafkaSource (p=96)                  │    │          │
│  │ CDC        │    │  │  Topics: cdc.orders, cdc.users,      │    │          │
│  │    ↓       │    │  │          cdc.products                │    │          │
│  │ cdc.orders │───▶│  │  Avro + Schema Registry              │    │          │
│  │ cdc.users  │───▶│  │  Watermark: processing time          │    │          │
│  │ cdc.products───▶│  └──────────────┬───────────────────────┘    │          │
│  │ (Kafka)    │    │                 │                             │          │
│  └────────────┘    │                 │ DebeziumEnvelope<RowData>   │          │
│                    │                 ▼                             │          │
│                    │  ┌──────────────────────────────────────┐    │          │
│                    │  │  CDC Transform (p=96)                │    │          │
│                    │  │                                      │    │          │
│                    │  │  1. Extract: before/after → RowData  │    │          │
│                    │  │  2. Map op to RowKind:               │    │          │
│                    │  │     c → INSERT                       │    │          │
│                    │  │     u → UPDATE_BEFORE + UPDATE_AFTER │    │          │
│                    │  │     d → DELETE                       │    │          │
│                    │  │  3. Route by source table name       │    │          │
│                    │  └──────────────┬───────────────────────┘    │          │
│                    │                 │                             │          │
│                    │       ┌─────────┼─────────┐                  │          │
│                    │       ▼         ▼         ▼                  │          │
│                    │  ┌─────────┐┌─────────┐┌─────────┐          │          │
│                    │  │ Iceberg ││ Iceberg ││ Iceberg │          │          │
│                    │  │ Sink    ││ Sink    ││ Sink    │          │          │
│                    │  │ orders  ││ users   ││products │          │          │
│                    │  │(upsert) ││(upsert) ││(append) │          │          │
│                    │  └────┬────┘└────┬────┘└────┬────┘          │          │
│                    │       │          │          │                │          │
│                    │       ▼          ▼          ▼                │          │
│                    │  ┌──────────────────────────────────────┐    │          │
│                    │  │  PostCommitTopology:                  │    │          │
│                    │  │  Compaction (RewriteDataFiles)        │    │          │
│                    │  │  Snapshot Expiry (ExpireSnapshots)    │    │          │
│                    │  └──────────────────────────────────────┘    │          │
│                    │                                              │          │
│                    │  ┌──────────────────┐                        │          │
│                    │  │ Checkpoint (S3)  │                        │          │
│                    │  └──────────────────┘                        │          │
│                    └──────────────────────────────────────────────┘          │
│                                                                               │
│                    ┌─────────────────────────────────────┐                   │
│                    │  S3: Iceberg Tables                  │                   │
│                    │                                      │                   │
│                    │  s3://lakehouse/orders/               │                   │
│                    │    data/  metadata/                   │                   │
│                    │  s3://lakehouse/users/                │                   │
│                    │    data/  metadata/                   │                   │
│                    │  s3://lakehouse/products/             │                   │
│                    │    data/  metadata/                   │                   │
│                    └─────────────────────────────────────┘                   │
│                                                                               │
│                    ┌─────────────┐                                           │
│                    │ Trino/Spark │ ← Query for analytics                     │
│                    └─────────────┘                                           │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Alternative: Dynamic Iceberg Sink** — Instead of 3 separate sinks, use a single
`DynamicIcebergSink` that routes events to the correct table based on a routing field
in the record. Simplifies the pipeline topology and supports dynamic table creation.

---

## 4. Deep Dive — 4 Key Design Decisions

### Decision 1: Exactly-Once to Iceberg — How It Works

This is the most important decision. Iceberg's exactly-once is fundamentally different
from Kafka sink's 2PC.

```
┌──────────────────────────────────────────────────────────────────────┐
│  ICEBERG EXACTLY-ONCE PROTOCOL                                        │
│                                                                       │
│  PHASE 1: WRITE (on checkpoint barrier)                               │
│  ─────────────────────────────────────                                │
│  Each IcebergSinkWriter flushes open Parquet files to S3.            │
│  Files exist on S3 but are NOT in any Iceberg snapshot.              │
│  Returns WriteResult { DataFile[], DeleteFile[] }                    │
│                                                                       │
│  PHASE 2: COMMIT (on notifyCheckpointComplete)                        │
│  ──────────────────────────────────────────────                       │
│  IcebergCommitter:                                                    │
│    1. Read DeltaManifests from IcebergCommittable                    │
│    2. table.newRowDelta()                                            │
│       .addRows(dataFiles)                                            │
│       .addDeletes(deleteFiles)                                       │
│    3. Set snapshot properties:                                        │
│       flink.max-committed-checkpoint-id = 42                         │
│       flink.job-id = <uuid>                                          │
│    4. operation.commit() → atomic snapshot visible                   │
│                                                                       │
│  IDEMPOTENCY (on recovery):                                          │
│  ─────────────────────────                                            │
│  On restart, IcebergCommitter checks:                                 │
│    SinkUtil.getMaxCommittedCheckpointId(table, jobId, operatorId)    │
│    → Returns 41 (from snapshot properties)                           │
│    → Only commits checkpoint 42+ (skips already-committed data)      │
│                                                                       │
│  WHY NOT JUST USE FLINK CHECKPOINT STATE?                             │
│  Flink state can lose track if job crashes between Iceberg commit    │
│  and Flink state checkpoint. Iceberg snapshot properties are the     │
│  ground truth that survives across job restarts and savepoint        │
│  restores.                                                            │
└──────────────────────────────────────────────────────────────────────┘
```

Ref: `flink-iceberg-sink-connector-deep-dive.md`

### Decision 2: Upsert Mode (MOR) for CDC Updates

CDC events include UPDATE and DELETE operations. The sink must handle them:

```
Iceberg Format V2 + upsert-enabled = true

  For each CDC record:
    INSERT (op=c):
      → Write DataFile with the new row

    UPDATE (op=u):
      → Write equality DELETE for the primary key (e.g., order_id)
      → Write DataFile with the updated row
      (Iceberg resolves on read: delete old row, return new row)

    DELETE (op=d):
      → Write equality DELETE for the primary key

  This is Merge-on-Read (MOR):
    Writes are fast (append-only: data files + delete files)
    Reads are slower (must merge data files with delete files)

  COMPACTION cleans up MOR overhead:
    PostCommitTopology → RewriteDataFiles
    Merges data + deletes into clean data files
    Converts MOR → COW for read performance
    Schedule: every 10 minutes or when delete file ratio > 20%
```

Ref: `flink-iceberg-sink-connector-deep-dive.md` (Part 8: MOR vs COW)

### Decision 3: Schema Evolution Without Pipeline Restart

Database schemas evolve: columns added, types widened. The pipeline must handle this.

```
Strategy: Avro + Schema Registry + Iceberg schema evolution

  1. Debezium captures schema changes and registers new Avro schema
  2. Flink deserializer reads schema from registry (always latest)
  3. New columns appear as nullable fields in RowData
  4. Iceberg sink detects schema difference:
     - ADD COLUMN: Iceberg table evolution adds the new column
     - WIDEN TYPE (int → long): Iceberg supports type promotion
     - RENAME: NOT automatic — requires manual ALTER TABLE

  With DynamicIcebergSink:
    Schema evolution is handled automatically.
    Sink compares incoming schema with table schema on each commit.
    Adds missing columns via table.updateSchema().

  Without DynamicIcebergSink:
    Must handle schema drift in a custom MapFunction or
    configure the sink to auto-evolve schema.

  KEY CONSTRAINT:
    Iceberg NEVER drops columns automatically.
    Removed source columns stay as NULL in the table.
    This is intentional — prevents accidental data loss.
```

### Decision 4: Concurrent Writers & Small File Problem

Multiple Flink subtasks write files simultaneously, creating many small files:

```
THE SMALL FILE PROBLEM:
  96 writers, checkpoint every 60s, total throughput 100 MB/s
  Each writer produces: 100 MB/s ÷ 96 ≈ 1 MB/s
  Each writer per checkpoint: 1 MB/s × 60s = ~60 MB per file

  Target file size (write.target-file-size-bytes, default 128 MB) controls
  mid-interval rollover — but checkpoint fires first (60s < 128s to fill 128 MB),
  so checkpoint interval is the binding constraint, not target file size.

  Files produced: 96 writers × 1 file/checkpoint = 96 files per checkpoint
                  96 × 60 checkpoints/hr = 5,760 files per hour
  Each file: ~60 MB (vs. 128 MB ideal → files are 2× too small)

  Too many undersized files → slow queries (each file = 1 read I/O)

  Target file size only kicks in when:
    throughput_per_writer × checkpoint_interval > target
    e.g. 3 MB/s × 60s = 180 MB > 128 MB → writer rolls over mid-checkpoint

SOLUTIONS:

  1. REDUCE WRITER PARALLELISM for the sink:
     Source p=96, but IcebergSink writer p=16
     Fewer writers → larger files → fewer files
     Trade-off: higher per-writer throughput needed

  2. COMPACTION (PostCommitTopology):
     RewriteDataFiles merges small files into 128-256 MB files
     Runs as part of the Flink pipeline (PostCommitTopology)
     Or as a separate Spark/Flink batch job

  3. PARTITION DESIGN:
     Partition by date (daily): fewer files per partition
     Hidden partitioning: hours(timestamp) → Iceberg auto-creates
     Avoid over-partitioning: partition by hour only if needed

CONCURRENT WRITE SAFETY:
  Multiple Flink jobs (or Flink + Spark) can write to the same table.
  Iceberg uses optimistic concurrency: metadata version CAS.
  Append-only commits almost never conflict.
  RowDelta (upsert) may conflict if equality deletes overlap.
  → Automatic retry with exponential backoff (up to 4 attempts).
```

Ref: `flink-iceberg-sink-connector-deep-dive.md` (Part 7: Concurrent Update Handling)

---

## 5. Worth Mentioning / Further Improvements

| Topic | Detail |
|-------|--------|
| **Dynamic Iceberg Sink** | Single sink handles multiple tables dynamically — routes by record metadata. Supports auto table creation and schema evolution. |
| **Deletion Vectors (DVs)** | Iceberg V3 feature: marks deleted rows in data files without separate delete files. Faster reads than equality deletes. |
| **Branch writes** | Write to an Iceberg branch (e.g., `staging`) for validation before merging to `main`. |
| **Monitoring** | Track: commit latency, file count per snapshot, equality delete ratio, compaction lag. |
| **Backfill** | Initial load: Flink batch job reads full MySQL table → writes to Iceberg. Then switch to CDC streaming. |
| **Data quality** | Add a validation step: null checks, range checks, dedup on primary key before writing. |
| **Multi-catalog** | Use Iceberg REST catalog (Polaris, Nessie) for multi-engine access (Flink + Trino + Spark). |
| **Partition evolution** | Iceberg supports changing partition scheme without rewriting data (e.g., daily → hourly). |

---

## Appendix: Code Example

```java
public class LakehouseIngestionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("s3://checkpoints/lakehouse-ingestion");
        env.setStateBackend(new HashMapStateBackend()); // minimal state for Iceberg sink

        // Source: multi-topic Kafka with Debezium CDC
        KafkaSource<DebeziumEvent> source = KafkaSource.<DebeziumEvent>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("cdc.orders", "cdc.users", "cdc.products")
                .setGroupId("lakehouse-ingestion")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new DebeziumAvroDeserializer(schemaRegistryUrl))
                .build();

        DataStream<DebeziumEvent> cdcStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(), // CDC uses processing time
                "kafka-cdc-source"
        ).uid("kafka-cdc-source");

        // Transform: extract RowData with RowKind from Debezium envelope
        DataStream<RowData> ordersStream = cdcStream
                .filter(e -> e.getSourceTable().equals("orders"))
                .map(new DebeziumToRowDataMapper("orders"))
                .uid("orders-transform");

        DataStream<RowData> usersStream = cdcStream
                .filter(e -> e.getSourceTable().equals("users"))
                .map(new DebeziumToRowDataMapper("users"))
                .uid("users-transform");

        DataStream<RowData> productsStream = cdcStream
                .filter(e -> e.getSourceTable().equals("products"))
                .map(new DebeziumToRowDataMapper("products"))
                .uid("products-transform");

        // Sink: Iceberg tables with upsert (MOR)
        TableLoader ordersLoader = TableLoader.fromHadoopTable("s3://lakehouse/orders");
        IcebergSink ordersSink = IcebergSink.forRowData(ordersLoader)
                .tableSchema(ordersSchema)
                .upsert(true)
                .equalityFieldColumns(Arrays.asList("order_id"))
                .distributionMode(DistributionMode.HASH)
                .writeParallelism(16)
                .set("write-format", "parquet")
                .set("target-file-size-bytes", String.valueOf(128 * 1024 * 1024))
                .build();
        ordersStream.sinkTo(ordersSink).uid("iceberg-orders-sink");

        TableLoader usersLoader = TableLoader.fromHadoopTable("s3://lakehouse/users");
        IcebergSink usersSink = IcebergSink.forRowData(usersLoader)
                .tableSchema(usersSchema)
                .upsert(true)
                .equalityFieldColumns(Arrays.asList("user_id"))
                .distributionMode(DistributionMode.HASH)
                .writeParallelism(8)
                .build();
        usersStream.sinkTo(usersSink).uid("iceberg-users-sink");

        TableLoader productsLoader = TableLoader.fromHadoopTable("s3://lakehouse/products");
        IcebergSink productsSink = IcebergSink.forRowData(productsLoader)
                .tableSchema(productsSchema)
                .upsert(false) // append-only (low update rate)
                .distributionMode(DistributionMode.NONE)
                .writeParallelism(4)
                .build();
        productsStream.sinkTo(productsSink).uid("iceberg-products-sink");

        env.execute("Streaming Lakehouse Ingestion");
    }
}

// CDC Transform: Debezium envelope → RowData with RowKind
public class DebeziumToRowDataMapper extends RichMapFunction<DebeziumEvent, RowData> {

    private final String tableName;

    public DebeziumToRowDataMapper(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public RowData map(DebeziumEvent event) {
        GenericRowData row;

        switch (event.getOp()) {
            case "c": // CREATE (INSERT)
                row = convertToRowData(event.getAfter());
                row.setRowKind(RowKind.INSERT);
                return row;

            case "u": // UPDATE
                // For upsert mode: emit UPDATE_AFTER only
                // The IcebergSink handles the equality delete internally
                row = convertToRowData(event.getAfter());
                row.setRowKind(RowKind.UPDATE_AFTER);
                return row;

            case "d": // DELETE
                row = convertToRowData(event.getBefore());
                row.setRowKind(RowKind.DELETE);
                return row;

            default:
                throw new IllegalArgumentException("Unknown op: " + event.getOp());
        }
    }

    private GenericRowData convertToRowData(Map<String, Object> fields) {
        // Convert Debezium field map to Flink GenericRowData
        // Schema-aware conversion using table's column definitions
        GenericRowData row = new GenericRowData(fields.size());
        int i = 0;
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            row.setField(i++, entry.getValue());
        }
        return row;
    }
}

// Alternative: Using Flink SQL (much simpler for straightforward CDC)
//
// CREATE TABLE cdc_orders (
//   order_id BIGINT,
//   user_id BIGINT,
//   amount DECIMAL(10,2),
//   status STRING,
//   updated_at TIMESTAMP(3),
//   PRIMARY KEY (order_id) NOT ENFORCED
// ) WITH (
//   'connector' = 'kafka',
//   'topic' = 'cdc.orders',
//   'properties.bootstrap.servers' = 'kafka:9092',
//   'format' = 'debezium-avro-confluent',
//   'debezium-avro-confluent.url' = 'http://schema-registry:8081'
// );
//
// CREATE TABLE iceberg_orders (
//   order_id BIGINT,
//   user_id BIGINT,
//   amount DECIMAL(10,2),
//   status STRING,
//   updated_at TIMESTAMP(3),
//   PRIMARY KEY (order_id) NOT ENFORCED
// ) WITH (
//   'connector' = 'iceberg',
//   'catalog-name' = 'lakehouse',
//   'catalog-type' = 'hadoop',
//   'warehouse' = 's3://lakehouse/',
//   'format-version' = '2',
//   'write.upsert.enabled' = 'true'
// );
//
// INSERT INTO iceberg_orders SELECT * FROM cdc_orders;
```
