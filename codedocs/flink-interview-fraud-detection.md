# Real-Time Fraud Detection — System Design Interview (45 min)

Design a real-time fraud detection system that processes payment transactions from Kafka, applies rule-based and pattern-based fraud detection using Flink, and routes flagged transactions to an alert service while persisting all decisions for audit.

---

## 1. Requirements Gathering (5 min)

Establish these through Q&A:

| Requirement | Answer (assumed) |
|---|---|
| Event source | Kafka topic `transactions` (JSON, Avro) |
| Event schema | userId, merchantId, amount, currency, timestamp, location (lat/lng), ipAddress, cardId |
| Avg event size | ~500 bytes |
| Peak throughput | 50,000 txn/sec (Black Friday: 200K/sec) |
| Kafka partitions | 64 |
| Latency SLA | < 200ms p99 (must respond before auth timeout) |
| Delivery guarantee | Exactly-once (cannot double-flag or miss fraud) |
| State per user | ~2 KB (recent txn history, velocity counters, location) |
| Active users | 50 million (but only ~5M active in any hour) |
| Sink - alerts | Kafka topic `fraud-alerts` → downstream alert service |
| Sink - audit | All decisions (fraud + legit) to data lake for compliance |

---

## 2. Capacity Estimation (5 min)

```
Data rate:
  50K txn/sec × 500 B = 25 MB/s (sustained)
  200K × 500 B = 100 MB/s (peak)

State size:
  5M active users × 2 KB = 10 GB active state
  (50M total users × 2 KB = 100 GB if no TTL — need TTL!)

Source parallelism:
  64 Kafka partitions → source parallelism = 64

Operator parallelism:
  Match source: 64 (avoid extra shuffle after keyBy)

TaskManagers:
  64 / 4 slots = 16 TMs
  Memory: 4 GB managed (RocksDB) + 2 GB heap + overhead = ~8 GB each
  Total: 16 × 8 GB = 128 GB cluster

Checkpoint:
  State: 10 GB (incremental, delta ~500 MB per checkpoint)
  Interval: 30 seconds (low for fast recovery)
  Duration: ~2-3 seconds (incremental to S3)
```

---

## 3. Architecture (15 min — includes deep dive)

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    REAL-TIME FRAUD DETECTION                               │
│                                                                            │
│  ┌────────────┐    ┌──────────────────────────────────────┐               │
│  │ Payment    │    │           Flink Cluster               │               │
│  │ Gateway    │    │                                      │               │
│  │            │    │  ┌────────────────────────────────┐  │               │
│  │ transactions    │  │  KafkaSource (p=64)            │  │               │
│  │ (Kafka, 64p)───▶│  │  Deserialize + assign watermark│  │               │
│  │            │    │  └───────────┬────────────────────┘  │               │
│  └────────────┘    │              │                        │               │
│                    │              │ keyBy(userId)          │               │
│                    │              ▼                        │               │
│                    │  ┌────────────────────────────────┐  │               │
│                    │  │  FraudDetectionProcess (p=64)  │  │               │
│                    │  │  KeyedProcessFunction           │  │               │
│                    │  │                                │  │               │
│                    │  │  Per-user state:                │  │               │
│                    │  │   - last 10 txn amounts         │  │               │
│                    │  │   - last location + timestamp   │  │               │
│                    │  │   - txn count in last 1h        │  │               │
│                    │  │   - small-then-large flag       │  │               │
│                    │  │                                │  │               │
│                    │  │  Rules:                         │  │  ┌──────────┐│
│                    │  │   1. Velocity: >10 txn/hour    │──┼─▶│ fraud-   ││
│                    │  │   2. Geo: impossible travel     │  │  │ alerts   ││
│                    │  │   3. Amount: sudden spike >10x  │  │  │ (Kafka)  ││
│                    │  │   4. Pattern: small→large      │  │  └──────────┘│
│                    │  │                                │  │               │
│                    │  │  Timers: clean up after 24h    │  │               │
│                    │  └───────────┬────────────────────┘  │               │
│                    │              │                        │               │
│                    │              │ all decisions          │               │
│                    │              ▼                        │               │
│                    │  ┌────────────────────────────────┐  │  ┌──────────┐│
│                    │  │  AuditSink (p=32)              │──┼─▶│ S3/HDFS  ││
│                    │  │  FileSink (Parquet, exactly-once)│  │  │ (audit)  ││
│                    │  └────────────────────────────────┘  │  └──────────┘│
│                    │                                      │               │
│                    │  ┌──────────────────┐                │               │
│                    │  │ Checkpoint Store │                │               │
│                    │  │ (S3, incremental)│                │               │
│                    │  └──────────────────┘                │               │
│                    └──────────────────────────────────────┘               │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Deep Dive — 4 Key Design Decisions

### Decision 1: Per-User State Design & TTL

This is the core of the system. Each user needs stateful tracking:

```
Per-user state (managed by RocksDB):

  ValueState<UserProfile>:
    lastLocation: (lat, lng)
    lastTxnTimestamp: long
    recentAmounts: CircularBuffer<Double> (last 10)
    hourlyTxnCount: int
    smallThenLargeFlag: boolean

  State TTL: 24 hours (StateTtlConfig)
    Users who haven't transacted in 24h → state evicted
    Without TTL: 50M users × 2 KB = 100 GB (unmanageable)
    With TTL: ~5M active × 2 KB = 10 GB (fits in RocksDB)

  Timer: registerEventTimeTimer(currentTime + 24h)
    Explicit cleanup for complex patterns that span hours
```

Why this matters: state design determines memory, checkpoint size, and correctness. Too much state per key = slow checkpoints. Too little = missed fraud patterns.

Ref: `flink-state-ttl-architecture.md`, `flink-rocksdb-state-backend-tuning.md`

### Decision 2: Sub-200ms Latency Under Exactly-Once

Exactly-once checkpoint barriers add latency. With a 200ms SLA:

```
Tuning for low latency:
  - buffer.timeout: 1ms (don't batch, send immediately)
  - Checkpoint interval: 30s (not too frequent to avoid barrier overhead)
  - Unaligned checkpoints: ENABLED (barriers don't wait for alignment)
  - Checkpoint mode: EXACTLY_ONCE (non-negotiable for financial data)
  - Processing: no windows — pure KeyedProcessFunction (event-at-a-time)

Why no windows?
  Windows add latency (wait for watermark to close window).
  Fraud detection must evaluate EACH transaction immediately.
  Use per-key counters + timers instead of windows.
```

Ref: `flink-exactly-once-checkpointing-deep-dive.md`, `flink-network-credit-flow-control-tuning.md`

### Decision 3: Fraud Rule Implementation (CEP vs ProcessFunction)

```
Option A: Flink CEP (Complex Event Processing)
  + Declarative pattern definition
  + Built-in pattern timeout
  - Limited flexibility for complex rules
  - Hard to combine multiple patterns per key

Option B: KeyedProcessFunction (RECOMMENDED)
  + Full control over state and timers
  + Can implement any rule combination
  + Easy to add/modify rules without pipeline restart
  - More code to write

Recommended: KeyedProcessFunction
  Each processElement() call evaluates ALL rules against updated state.
  Timer-based cleanup avoids unbounded state.
  Side outputs route fraud alerts to Kafka, all decisions to audit.
```

### Decision 4: Handling Hot Merchants (Data Skew)

```
Problem:
  A popular merchant (Amazon, Walmart) may see 100x more txn than others.
  keyBy(userId) distributes well (many users).
  But if you also need per-merchant aggregation: keyBy(merchantId) → SKEW.

Solution: Two-stage processing
  Stage 1: keyBy(userId) → per-user fraud rules (no skew issue)
  Stage 2: keyBy(merchantId + salt) → per-merchant velocity
           → keyBy(merchantId) → merge

  Or: broadcast merchant risk scores as a BroadcastState
      (small table, updated periodically from risk service)
```

Ref: `flink-blacklist-filtering-solutions.md`

---

## 5. Worth Mentioning / Further Improvements

| Topic | Detail |
|-------|--------|
| **ML model scoring** | Async I/O to ML inference service for risk scoring alongside rules |
| **Dynamic rule updates** | Broadcast stream of rules from a config topic — no pipeline restart |
| **A/B testing** | Side-output to shadow pipeline with new rules, compare false positive rates |
| **Feedback loop** | Chargebacks from downstream feed back as training data (separate batch pipeline) |
| **Monitoring** | Alert on: consumer lag (falling behind), false positive rate spike, state size growth |
| **Autoscaling** | K8s operator autoscaler for Black Friday traffic spikes (10x) |
| **Replay & backfill** | Bounded Kafka source mode to replay historical txn for rule validation |

---

## Appendix: Code Example

```java
public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableUnalignedCheckpoints(); // barriers don't wait for alignment
        env.getCheckpointConfig().setCheckpointStorage("s3://checkpoints/fraud-detection");
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true)); // incremental
        env.setBufferTimeout(1); // 1ms: don't batch network buffers, emit immediately

        // Source: Kafka transactions
        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("transactions")
                .setGroupId("fraud-detector")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setDeserializer(new TransactionDeserializer())
                .build();

        DataStream<Transaction> txnStream = env.fromSource(
                source,
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((txn, ts) -> txn.getTimestamp())
                        .withIdleness(Duration.ofMinutes(1)),
                "kafka-source"
        ).uid("kafka-source");

        // Process: per-user fraud detection
        OutputTag<FraudAlert> alertTag = new OutputTag<>("fraud-alerts") {};

        SingleOutputStreamOperator<AuditRecord> results = txnStream
                .keyBy(Transaction::getUserId)
                .process(new FraudDetectionFunction(alertTag))
                .uid("fraud-detector");

        // Sink 1: fraud alerts to Kafka
        DataStream<FraudAlert> alerts = results.getSideOutput(alertTag);
        KafkaSink<FraudAlert> alertSink = KafkaSink.<FraudAlert>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(new FraudAlertSerializer("fraud-alerts"))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("fraud-alert-sink")
                .build();
        alerts.sinkTo(alertSink).uid("alert-sink");

        // Sink 2: all decisions to S3 (audit)
        FileSink<AuditRecord> auditSink = FileSink
                .forBulkFormat(new Path("s3://audit/fraud-decisions"),
                        ParquetWriterFactory.create(AuditRecord.class))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withMaxPartSize(MemorySize.ofMebiBytes(128))
                        .withRolloverInterval(Duration.ofMinutes(5))
                        .build())
                .build();
        results.sinkTo(auditSink).uid("audit-sink");

        env.execute("Real-Time Fraud Detection");
    }
}

// Core fraud detection logic
public class FraudDetectionFunction
        extends KeyedProcessFunction<String, Transaction, AuditRecord> {

    private final OutputTag<FraudAlert> alertTag;
    private ValueState<UserFraudState> userState;

    public FraudDetectionFunction(OutputTag<FraudAlert> alertTag) {
        this.alertTag = alertTag;
    }

    @Override
    public void open(OpenContext ctx) {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofHours(24))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .cleanupInRocksdbCompactFilter(1000)
                .build();

        ValueStateDescriptor<UserFraudState> desc =
                new ValueStateDescriptor<>("user-fraud-state", UserFraudState.class);
        desc.enableTimeToLive(ttl);
        userState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(Transaction txn, Context ctx, Collector<AuditRecord> out)
            throws Exception {
        UserFraudState state = userState.value();
        if (state == null) state = new UserFraudState();

        List<String> violations = new ArrayList<>();

        // Rule 1: Velocity check (>10 txn/hour)
        state.incrementHourlyCount();
        if (state.getHourlyTxnCount() > 10) {
            violations.add("VELOCITY_EXCEEDED");
        }

        // Rule 2: Impossible travel
        if (state.getLastLocation() != null) {
            double distKm = haversine(state.getLastLocation(), txn.getLocation());
            long timeSec = (txn.getTimestamp() - state.getLastTxnTimestamp()) / 1000;
            if (timeSec > 0 && distKm / timeSec > 0.3) { // >1000 km/h
                violations.add("IMPOSSIBLE_TRAVEL");
            }
        }

        // Rule 3: Amount spike (>10x average)
        double avgAmount = state.getAverageRecentAmount();
        if (avgAmount > 0 && txn.getAmount() > avgAmount * 10) {
            violations.add("AMOUNT_SPIKE");
        }

        // Rule 4: Small-then-large pattern
        if (txn.getAmount() < 1.0) {
            state.setSmallTxnFlag(true);
        } else if (state.isSmallTxnFlag() && txn.getAmount() > 500) {
            violations.add("SMALL_THEN_LARGE");
            state.setSmallTxnFlag(false);
        }

        // Update state
        state.addRecentAmount(txn.getAmount());
        state.setLastLocation(txn.getLocation());
        state.setLastTxnTimestamp(txn.getTimestamp());
        userState.update(state);

        // Register cleanup timer
        ctx.timerService().registerEventTimeTimer(
                txn.getTimestamp() + Duration.ofHours(24).toMillis());

        // Emit results
        boolean isFraud = !violations.isEmpty();
        AuditRecord audit = new AuditRecord(txn, isFraud, violations);
        out.collect(audit);

        if (isFraud) {
            ctx.output(alertTag, new FraudAlert(txn, violations));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AuditRecord> out) {
        // Timer-based cleanup (supplements TTL for complex patterns)
        // TTL handles most cleanup; timer handles edge cases
    }
}
```

## BroadcastState: Dynamic Rule Updates

Rules like velocity thresholds or blocked merchant lists change at runtime without a job restart.
Broadcast the rules stream to all parallel instances of the fraud detector via BroadcastState.

```java
// Rule POJO updated periodically from a risk service
public class FraudRule {
    public String ruleId;       // e.g. "VELOCITY_LIMIT"
    public double threshold;    // e.g. 10.0 (max txn/hour)
}

// Descriptor shared between broadcast side and process side
MapStateDescriptor<String, FraudRule> RULE_STATE_DESC = new MapStateDescriptor<>(
        "fraud-rules", String.class, FraudRule.class);

// Rules source: Kafka topic updated by risk service
DataStream<FraudRule> rulesStream = env.fromSource(
        KafkaSource.<FraudRule>builder()
                .setTopics("fraud-rules")
                ...build(),
        WatermarkStrategy.noWatermarks(),
        "rules-source"
);

// Broadcast rules to all parallel subtasks
BroadcastStream<FraudRule> broadcastRules = rulesStream.broadcast(RULE_STATE_DESC);

// Connect transaction stream with broadcast rules
txnStream
    .keyBy(Transaction::getUserId)
    .connect(broadcastRules)
    .process(new FraudDetectionWithRulesFunction(alertTag));

// -------------------------------------------------------

public class FraudDetectionWithRulesFunction
        extends KeyedBroadcastProcessFunction<String, Transaction, FraudRule, AuditRecord> {

    // Called for each rule update — runs on all parallel instances
    @Override
    public void processBroadcastElement(FraudRule rule, Context ctx,
                                        Collector<AuditRecord> out) throws Exception {
        ctx.getBroadcastState(RULE_STATE_DESC).put(rule.ruleId, rule);
    }

    // Called for each transaction — reads rules from broadcast state
    @Override
    public void processElement(Transaction txn, ReadOnlyContext ctx,
                               Collector<AuditRecord> out) throws Exception {
        ReadOnlyBroadcastState<String, FraudRule> rules = ctx.getBroadcastState(RULE_STATE_DESC);

        FraudRule velocityRule = rules.get("VELOCITY_LIMIT");
        double velocityLimit = velocityRule != null ? velocityRule.threshold : 10.0; // fallback default

        // Use velocityLimit in detection logic instead of hardcoded constant
        ...
    }
}
```

Key properties of BroadcastState:
- Rules stream is replicated to ALL parallel instances (not partitioned)
- `processBroadcastElement` runs on all subtasks — keep it fast, no heavy computation
- `processElement` has read-only access to broadcast state (prevents race conditions)
- State survives checkpoint/restore — rules persist across restarts
