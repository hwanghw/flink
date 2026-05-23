# Real-Time Click Analytics Dashboard — System Design Interview (45 min)

Design a real-time analytics system that processes clickstream events from web/mobile apps
via Kafka, computes per-page and per-campaign metrics (unique visitors, click counts,
conversion rates) using Flink, and serves them to a live dashboard.

---

## 1. Requirements Gathering (5 min)

| Requirement | Answer (assumed) |
|---|---|
| Event source | Kafka topic `clickstream` (JSON) |
| Event schema | userId, sessionId, pageUrl, campaignId, action (view/click/purchase), timestamp, deviceType |
| Avg event size | ~300 bytes |
| Peak throughput | 500,000 events/sec |
| Kafka partitions | 128 |
| Latency SLA | < 5 seconds (dashboard refresh rate) |
| Delivery guarantee | At-least-once + idempotent sink (OK for analytics — small count errors acceptable) |
| Metrics to compute | 1. Unique visitors per page (1-min tumbling window), 2. Click count per campaign (1-min tumbling + 1-hour sliding), 3. Conversion rate per campaign (clicks → purchases ratio) |
| Active pages | ~100,000 |
| Active campaigns | ~10,000 |
| Sink | Redis (real-time dashboard) + Elasticsearch (historical drill-down) |
| Deduplication | Users may send duplicate clicks (retries) — dedup by (userId, pageUrl, 1-min window) |

---

## 2. Capacity Estimation (5 min)

```
Data rate:
  500K events/sec × 300 B = 150 MB/s (peak)
  Sustained: ~200K events/sec = 60 MB/s

State size:
  Dedup state: ~5M unique (userId, pageUrl) per minute × 32 B = 160 MB
  Window state: 100K pages × 8 B (counter) = 0.8 MB (trivial)
  Campaign windows: 10K campaigns × (1 min + 60 × 1 min sliding) = ~5 MB
  Total active state: ~200 MB (HashMap state backend is fine!)

Source parallelism:
  128 Kafka partitions → source parallelism = 128

Operator parallelism:
  Dedup + window: 128 (match source)
  Sink: 32 (Redis can handle batched writes)

TaskManagers:
  128 / 4 slots = 32 TMs
  Memory: 2 GB heap (HashMap state) + 1 GB overhead = ~4 GB each
  Total: 32 × 4 GB = 128 GB cluster

Checkpoint:
  State: ~200 MB (HashMap, full checkpoint)
  Interval: 60 seconds (OK for analytics latency SLA)
  Duration: < 1 second
```

---

## 3. Architecture (15 min)

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    REAL-TIME CLICK ANALYTICS                               │
│                                                                            │
│  ┌────────────┐    ┌──────────────────────────────────────┐               │
│  │ Web/Mobile │    │           Flink Cluster               │               │
│  │ Apps       │    │                                      │               │
│  │            │    │  ┌────────────────────────────────┐  │               │
│  │ clickstream│    │  │  KafkaSource (p=128)           │  │               │
│  │ (Kafka,    │───▶│  │  Deserialize + watermark       │  │               │
│  │  128 parts)│    │  │  forBoundedOutOfOrderness(10s) │  │               │
│  │            │    │  └───────────┬────────────────────┘  │               │
│  └────────────┘    │              │                        │               │
│                    │              │                        │               │
│                    │    ┌─────────┴─────────┐              │               │
│                    │    │                   │              │               │
│                    │    ▼                   ▼              │               │
│                    │  ┌──────────┐  ┌──────────────────┐  │               │
│                    │  │ Branch 1 │  │ Branch 2         │  │               │
│                    │  │          │  │                  │  │               │
│                    │  │ keyBy    │  │ keyBy            │  │               │
│                    │  │ (pageUrl)│  │ (campaignId)     │  │               │
│                    │  │          │  │                  │  │               │
│                    │  │ Dedup +  │  │ Tumbling 1m:     │  │               │
│                    │  │ Tumbling │  │  click count     │  │               │
│                    │  │ 1m:      │  │ Sliding 1h/1m:   │  │               │
│                    │  │ UV count │  │  click trend     │  │               │
│                    │  │          │  │ Conversion rate  │  │               │
│                    │  └────┬─────┘  └────────┬─────────┘  │               │
│                    │       │                  │            │               │
│                    │       ▼                  ▼            │               │
│                    │  ┌────────────────────────────────┐  │  ┌──────────┐│
│                    │  │  RedisSink (p=32)              │──┼─▶│  Redis   ││
│                    │  │  HSET page:{url} uv_count val  │  │  │(dashboard││
│                    │  └────────────────────────────────┘  │  └──────────┘│
│                    │  ┌────────────────────────────────┐  │  ┌──────────┐│
│                    │  │  ElasticsearchSink (p=16)      │──┼─▶│  ES      ││
│                    │  │  Bulk index, 5s flush          │  │  │(history) ││
│                    │  └────────────────────────────────┘  │  └──────────┘│
│                    │                                      │               │
│                    │  ┌──────────────────┐                │               │
│                    │  │ Checkpoint (S3)  │                │               │
│                    │  └──────────────────┘                │               │
│                    └──────────────────────────────────────┘               │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Deep Dive — 4 Key Design Decisions

### Decision 1: Deduplication Strategy

Users send duplicate clicks (network retries, double-taps). Must dedup before counting.

```
Approach: KeyedProcessFunction with MapState<String, Boolean>

  keyBy(userId % bucketNum + pageUrl)
  For each event:
    dedupKey = userId + ":" + pageUrl + ":" + windowTimestamp
    if (seen.contains(dedupKey)) → drop
    else → seen.put(dedupKey, true) → emit downstream

  State TTL: 2 minutes (covers 1-min window + late data buffer)
    StateTtlConfig.newBuilder(Duration.ofMinutes(2))
      .cleanupFullSnapshot()  // or cleanupInRocksdbCompactFilter()

  Why not Bloom filter?
    100K unique per minute is small enough for exact dedup.
    Bloom filter for >10M uniques/min where false positives are OK.
```

Why dedup before windowing: counting UVs inside the window aggregate function would require HyperLogLog (approximate). Dedup upstream gives exact counts with simpler window logic.

Ref: `flink-state-ttl-architecture.md`

### Decision 2: Window Design (Tumbling vs Sliding)

```
Metric: Page UV (1-minute tumbling)
  .window(TumblingEventTimeWindows.of(Time.minutes(1)))
  .aggregate(new CountAggregator())
  Simple, low state: 1 counter per key per window.

Metric: Campaign trend (1-hour sliding, 1-min slide)
  .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(1)))
  .aggregate(new CountAggregator())
  State multiplier: 60 windows overlap → 60× state per key!
  10K campaigns × 60 windows × 8 B = 4.8 MB (still manageable)

  ALTERNATIVE: Use 1-min tumbling windows + application-level
  rollup in Redis (SUM last 60 1-min buckets). Reduces Flink state
  at the cost of slightly more Redis logic.

Metric: Conversion rate
  Count clicks and purchases separately in 1-min tumbling windows.
  Compute ratio in sink or downstream.
  Avoid division in Flink (streaming division is tricky with retractions).
```

Ref: `flink-window-state-rocksdb-checkpoint-deep-dive.md`, `flink-accumulating-vs-purging-and-allowed-lateness.md`

### Decision 3: At-Least-Once + Idempotent Sink (Not Exactly-Once)

```
Why NOT exactly-once?
  - Analytics dashboard tolerates minor count imprecision (~0.01%)
  - Exactly-once adds latency (barrier alignment) and complexity (2PC)
  - Redis doesn't support 2PC

Why at-least-once + idempotent?
  - Checkpoint mode: AT_LEAST_ONCE (no barrier alignment wait)
  - Redis sink: HSET with key = "page:{url}:min:{timestamp}"
    → Idempotent: writing the same count twice is harmless (overwrite)
  - ES sink: document ID = "{campaignId}:{windowTimestamp}"
    → Idempotent: same doc ID → update-in-place

  This gives effectively-exactly-once semantics with lower latency
  and simpler operations than true 2PC.
```

Ref: `flink-sink-patterns-comparison.md`, `flink-exactly-once-checkpointing-deep-dive.md`

### Decision 4: Watermark & Late Data Handling

```
Out-of-orderness: 10 seconds (mobile events may arrive late)
  WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))

Source idleness: 1 minute
  .withIdleness(Duration.ofMinutes(1))
  Without this: one idle Kafka partition stalls ALL downstream windows.

Late data: side-output + periodic correction
  .allowedLateness(Time.minutes(1))
  .sideOutputLateData(lateTag)

  Late events go to a side-output stream → batch correction to Redis
  (increment the count for the past window).

  Why not longer allowed lateness?
  Each minute of allowed lateness keeps window state alive.
  1 min extra × 100K pages = ~1 MB extra state (acceptable).
  10 min extra = 10 MB (still OK but diminishing returns).
```

Ref: `flink-accumulating-vs-purging-and-allowed-lateness.md`

---

## 5. Worth Mentioning / Further Improvements

| Topic | Detail |
|-------|--------|
| **HyperLogLog for UV** | For 10M+ unique visitors per page, switch from exact dedup to HyperLogLog (DataSketches library). ~1 KB per sketch, <2% error. |
| **Session windows** | For session-based analytics (bounce rate), use SessionWindows with 30-min gap. |
| **Pre-aggregation for skew** | If one page (homepage) gets 50% of traffic: local pre-aggregate per subtask, then merge globally. |
| **Dashboard backfill** | On pipeline restart, Redis may show stale data. Backfill from ES or replay Kafka offsets. |
| **Autoscaling** | Peak at 500K/s but sustained at 200K/s → autoscaler can save 60% resources off-peak. |
| **Multi-tenancy** | Multiple clients sharing the platform → keyBy(tenantId + pageUrl) to isolate state. |

---

## Appendix: Code Example

```java
public class ClickAnalyticsJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("s3://checkpoints/click-analytics");
        // HashMap state backend — total state < 500 MB
        env.setStateBackend(new HashMapStateBackend());

        // Source
        KafkaSource<ClickEvent> source = KafkaSource.<ClickEvent>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("clickstream")
                .setGroupId("click-analytics")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setDeserializer(new ClickEventDeserializer())
                .build();

        OutputTag<ClickEvent> lateTag = new OutputTag<>("late-events") {};

        DataStream<ClickEvent> clicks = env.fromSource(
                source,
                WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((e, ts) -> e.getTimestamp())
                        .withIdleness(Duration.ofMinutes(1)),
                "kafka-source"
        ).uid("kafka-source");

        // Branch 1: Per-page unique visitors (1-min tumbling)
        SingleOutputStreamOperator<PageMetric> pageUV = clicks
                .keyBy(ClickEvent::getPageUrl)
                .process(new DeduplicateFunction()) // dedup by userId within window
                .uid("page-dedup")
                .keyBy(ClickEvent::getPageUrl)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(Duration.ofMinutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new UVCountAggregator(), new PageMetricWindowFunction())
                .uid("page-uv-window");

        // Branch 2: Per-campaign click count (1-min tumbling)
        DataStream<CampaignMetric> campaignClicks = clicks
                .filter(e -> e.getAction().equals("click"))
                .keyBy(ClickEvent::getCampaignId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountAggregator(), new CampaignMetricWindowFunction())
                .uid("campaign-click-window");

        // Sink 1: Redis (dashboard)
        pageUV.addSink(new RedisSink<>(redisConfig, new PageMetricRedisMapper()))
                .uid("redis-page-sink");
        campaignClicks.addSink(new RedisSink<>(redisConfig, new CampaignMetricRedisMapper()))
                .uid("redis-campaign-sink");

        // Sink 2: Elasticsearch (historical)
        pageUV.sinkTo(elasticsearchSink("page-metrics"))
                .uid("es-page-sink");
        campaignClicks.sinkTo(elasticsearchSink("campaign-metrics"))
                .uid("es-campaign-sink");

        // Late data handling
        pageUV.getSideOutput(lateTag)
                .addSink(new RedisCorrectionSink()) // increment past window counts
                .uid("late-data-correction");

        env.execute("Real-Time Click Analytics");
    }
}

// Deduplication: exact dedup using MapState with TTL
public class DeduplicateFunction
        extends KeyedProcessFunction<String, ClickEvent, ClickEvent> {

    private MapState<String, Boolean> seen;

    @Override
    public void open(OpenContext ctx) {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofMinutes(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupFullSnapshot()
                .build();

        MapStateDescriptor<String, Boolean> desc =
                new MapStateDescriptor<>("dedup-state", String.class, Boolean.class);
        desc.enableTimeToLive(ttl);
        seen = getRuntimeContext().getMapState(desc);
    }

    @Override
    public void processElement(ClickEvent event, Context ctx, Collector<ClickEvent> out)
            throws Exception {
        String dedupKey = event.getUserId() + ":" + minuteBucket(event.getTimestamp());
        if (!seen.contains(dedupKey)) {
            seen.put(dedupKey, true);
            out.collect(event);
        }
        // Duplicate → silently dropped
    }

    private String minuteBucket(long timestamp) {
        return String.valueOf(timestamp / 60_000);
    }
}
```
