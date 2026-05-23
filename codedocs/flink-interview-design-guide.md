# Flink Streaming Ingestion: System Design Interview Guide

A framework for designing (or interviewing on) a Flink-based streaming ingestion pipeline.
Covers requirements gathering, capacity estimation, and design decisions.

---

## 1. Questions to Ask (Requirements Gathering)

Before drawing any boxes, nail down the constraints. These 7 questions are the **highest
leverage** — the answers drive every downstream design decision. Ask them in this order.

(Full question list with 26 questions across 6 categories in Appendix A.)

| # | Question | Why it's top-7 |
|---|----------|----------------|
| 1 | What's the **peak events/sec** and **data rate** (MB/s)? | Determines parallelism, TM count, and whether you're in the "big state" regime |
| 2 | What's the **end-to-end latency SLA**? (sub-second? seconds? minutes?) | The single most impactful constraint. Sub-second → low-latency tuning. Minutes → high-throughput batching. |
| 3 | **Exactly-once**, **at-least-once**, or **best-effort**? | Drives checkpoint mode, sink choice, recovery complexity, and throughput overhead |
| 4 | What **processing** happens? (stateless filter? keyed aggregation? windowed join?) | Stateless = trivial. Keyed + windowed = RocksDB, checkpoints, TTL, skew — totally different system. |
| 5 | How much **state** per key? How many keys? | `total_state = keys × state_per_key`. Determines state backend, checkpoint sizing, and cost. |
| 6 | What's the **sink**? Does it support **transactions or idempotent writes**? | Kafka (2PC), Iceberg (snapshot commit), Redis (upsert) — the sink determines achievable E2E guarantees. |
| 7 | How many **source partitions**? | Hard ceiling on source parallelism. Also affects max-parallelism and key group planning. |

---

## 2. Capacity Estimation

### Step 1: Data Volume

```
Given:
  events_per_sec     = 100,000 (peak)
  avg_event_size     = 500 bytes
  num_keys           = 10,000,000
  state_per_key      = 200 bytes
  out_of_orderness   = 30 seconds
  window_size        = 5 minutes (tumbling)

Derived:
  data_rate          = 100,000 × 500 B    = 50 MB/s (peak)
  daily_volume       = 50 MB/s × 86,400   = 4.3 TB/day
  total_state_size   = 10M × 200 B        = 2 GB (active state in RocksDB)
```

### Step 2: Source Parallelism

```
Source parallelism is bounded by:
  1. Number of source partitions (hard ceiling)
  2. Per-subtask throughput capacity

  kafka_partitions = 32
  per_subtask_throughput = 50 MB/s (typical for Kafka source)

  min_source_parallelism = data_rate / per_subtask_throughput
                         = 50 / 50 = 1 (throughput-wise)

  actual_source_parallelism = kafka_partitions = 32
  (can't exceed partitions, but can be less with consumer groups)

  → Use 32 (match partitions for even distribution)
```

### Step 3: Operator Parallelism

```
For stateful operators, consider:
  1. State per subtask = total_state / parallelism
  2. CPU per subtask (profile your UDF)
  3. Network fan-out (keyBy redistributes across all subtasks)

  operator_parallelism = 32 (match source to avoid extra shuffle)

  state_per_subtask = 2 GB / 32 = 64 MB (easily fits in RocksDB)
```

### Step 4: TaskManager Sizing

```
Per TaskManager:
  slots_per_tm       = 4 (common: 1 slot per CPU core group)
  parallelism        = 32
  num_task_managers   = 32 / 4 = 8

Memory per TaskManager:
  ┌────────────────────────────────────────────────┐
  │  JVM Heap:     1-2 GB   (framework + overhead) │
  │  Managed:      2-4 GB   (RocksDB state)        │
  │  Network:      256 MB   (buffers)               │
  │  JVM Metaspace: 256 MB                          │
  │  JVM Overhead: 256 MB                           │
  │  ─────────────────────────                      │
  │  Total:        ~4-8 GB per TM                   │
  └────────────────────────────────────────────────┘

  Total cluster memory = 8 TMs × 6 GB = 48 GB
```

### Step 5: Checkpoint Sizing

```
Checkpoint size ≈ total_state_size = 2 GB (for full checkpoint)

With incremental checkpoints:
  delta_per_checkpoint ≈ 5-20% of total state (depends on churn)
                       ≈ 100-400 MB

Checkpoint interval  = 60 seconds (common)
Checkpoint duration  = state_size / upload_bandwidth
                     = 400 MB / 200 MB/s = 2 seconds (incremental)

Storage (S3/HDFS):
  retained_checkpoints = 3 (default)
  storage = 3 × 2 GB = 6 GB (for full, including shared SST files)
```

### Step 6: Network Estimation

```
Network buffers needed per channel:
  buffers = (throughput × roundtrip_ms) / segment_size
          = (50 MB/s × 1ms) / 32 KB
          ≈ 2 buffers (per channel, actively used)

Total network memory per TM:
  input_channels_per_slot  = source_parallelism = 32
  output_channels_per_slot = downstream_parallelism = 32

  input_buffers  = 32 × 2 (exclusive) + 8 (floating) = 72
  output_buffers = 32 × 10 (max-per-channel) = 320 (upper bound)

  per_slot = (72 + 320) × 32 KB ≈ 12 MB
  per_tm   = 4 slots × 12 MB = 48 MB

  → Default 64 MB minimum is sufficient for this workload.
```

### Quick Estimation Cheat Sheet

```
┌────────────────────────────────────────────────────────────────┐
│                    CAPACITY ESTIMATION                         │
│                                                                │
│  events/sec × avg_bytes = data_rate (MB/s)                     │
│  data_rate × 86400     = daily_volume (TB/day)                │
│                                                                │
│  num_keys × state_per_key = total_state                        │
│  total_state / parallelism = state_per_subtask                 │
│    → if > 1 GB: definitely use RocksDB                         │
│    → if < 100 MB: HashMapStateBackend may be fine              │
│                                                                │
│  source_parallelism ≤ source_partitions                        │
│  operator_parallelism ≈ source_parallelism (avoid extra shuffle)│
│                                                                │
│  num_TMs = parallelism / slots_per_TM                          │
│  memory_per_TM = heap + managed + network + overhead           │
│    heap:    1-2 GB (framework)                                 │
│    managed: 2× state_per_slot (RocksDB needs headroom)         │
│    network: 64 MB – 256 MB (depends on fan-out)                │
│                                                                │
│  checkpoint_interval: 30s – 5min (lower = faster recovery,     │
│                                    higher = less overhead)     │
│                                                                │
│  checkpoint_size ≈ state × churn_rate (incremental)            │
│                 ≈ state (full)                                 │
└────────────────────────────────────────────────────────────────┘
```

---

## 3. Interview Flow (45 minutes)

```
┌──────────────────────────────────────────────────────────────────────┐
│  45-MINUTE INTERVIEW TIMELINE                                         │
│                                                                       │
│  0:00 ────── PHASE 1: CLARIFY (5 min) ──────                        │
│  Ask requirements questions from Section 1.                           │
│  Nail down: data rate, latency SLA, delivery guarantee, state size.  │
│  This sets up EVERY decision that follows.                            │
│                                                                       │
│  0:05 ────── PHASE 2: ESTIMATE (5 min) ──────                       │
│  Back-of-envelope from Section 2.                                     │
│  Parallelism, TM count, memory, checkpoint sizing.                   │
│  Shows you can reason about scale before touching the whiteboard.     │
│                                                                       │
│  0:10 ────── PHASE 3: ARCHITECTURE + KEY DECISIONS (15 min) ─────── │
│  Draw high-level pipeline:                                            │
│                                                                       │
│    ┌────────┐    ┌─────────────────────────────┐    ┌────────┐      │
│    │ Source  │───▶│  Flink: Source→Process→Sink  │───▶│  Sink  │      │
│    │(Kafka) │    │     ║         ║               │    │(Iceberg│      │
│    │32 parts│    │   state     state             │    │ /S3/DB)│      │
│    └────────┘    │     ║         ║               │    └────────┘      │
│                  │  ┌──────────────┐             │                    │
│                  │  │Checkpoint    │             │                    │
│                  │  │Store (S3)    │             │                    │
│                  │  └──────────────┘             │                    │
│                  └─────────────────────────────┘                     │
│                                                                       │
│  Then walk through the 7 Key Decisions (Section 4) — spend the      │
│  most time on decisions #1-#3 which have the deepest trade-offs.     │
│                                                                       │
│  0:25 ────── PHASE 4: FAILURE & RECOVERY (10 min) ──────            │
│  Cover decision #7 in depth:                                         │
│    - Checkpoint-based recovery flow                                  │
│    - Recovery time estimation (state restore + catch-up)             │
│    - HA for JobManager (K8s ConfigMap leader election)               │
│    - What happens to running tasks during failover                   │
│                                                                       │
│  0:35 ────── PHASE 5: PRODUCTION READINESS (10 min) ────            │
│  Show operational maturity:                                           │
│    - Key monitoring metrics (consumer lag, checkpoint duration,       │
│      backpressure, watermark lag, state growth)                      │
│    - Schema evolution strategy (Avro + registry)                     │
│    - Pipeline upgrade path (savepoint → deploy → restore)            │
│    - Autoscaling (if on K8s: operator autoscaler)                    │
│                                                                       │
│  0:45 ────── END ──────                                              │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 4. The 7 Key Design Decisions

These are the decisions that differentiate a strong candidate. Each should be **justified
by requirements** from Section 1, not stated as dogma.

### Decision 1: Exactly-Once vs At-Least-Once Delivery

**The single most impactful decision.** It determines checkpoint mode, sink choice,
recovery behavior, and throughput overhead.

```
┌──────────────────────────────────────────────────────────────────────┐
│  DELIVERY GUARANTEE TRADE-OFF                                         │
│                                                                       │
│  EXACTLY-ONCE                      AT-LEAST-ONCE                      │
│  ─────────────                     ──────────────                     │
│  Checkpoint mode: EXACTLY_ONCE     Checkpoint mode: AT_LEAST_ONCE    │
│  Barrier alignment: yes (wait)     Barrier alignment: no (fast)      │
│  Sink: must support 2PC or         Sink: any (idempotent upsert      │
│        atomic commit                     handles duplicates)          │
│  Latency: higher (barrier wait)    Latency: lower                    │
│  Throughput: ~10-30% overhead      Throughput: near-zero overhead    │
│                                                                       │
│  When to choose EXACTLY-ONCE:                                         │
│    - Financial transactions, billing, audit trails                    │
│    - Sink doesn't support idempotent writes                          │
│                                                                       │
│  When AT-LEAST-ONCE + IDEMPOTENT is better:                          │
│    - Sink supports upsert (Iceberg, Cassandra, Redis)                │
│    - Duplicates are tolerable or handled downstream                  │
│    - Latency SLA is sub-second                                       │
└──────────────────────────────────────────────────────────────────────┘
```

**How to answer**: "Given the latency SLA of X and the sink being Y, I would choose Z
because..."

Ref: `flink-exactly-once-checkpointing-deep-dive.md`, `flink-kafka-connector-source-sink-architecture.md`

### Decision 2: State Backend & Checkpoint Strategy

**State backend + checkpoint config together determine recovery speed, memory footprint,
and operational complexity.**

```
┌──────────────────────────────────────────────────────────────────────┐
│  STATE BACKEND DECISION MATRIX                                        │
│                                                                       │
│  Total state < 100 MB?                                                │
│    → HashMapStateBackend (all in JVM heap, fastest)                  │
│                                                                       │
│  Total state 100 MB – multi-GB?                                      │
│    → RocksDB + incremental checkpoints                               │
│      state_per_subtask = total_state / parallelism                   │
│      Tune: managed memory, block cache, write buffer                 │
│                                                                       │
│  CHECKPOINT STRATEGY                                                  │
│  ────────────────────                                                 │
│  Interval:     30s (low latency) – 5min (high throughput)            │
│  Type:         Incremental (if state > 100 MB)                       │
│  Unaligned:    Enable if backpressure causes checkpoint timeouts     │
│  Storage:      S3/HDFS (shared, survives TM loss)                    │
│  Retained:     3 (default, allows rollback)                          │
│                                                                       │
│  KEY FORMULA:                                                         │
│  recovery_time = state_download + state_rebuild + catch_up           │
│  catch_up = (checkpoint_interval × input_rate) /                     │
│             (max_throughput - input_rate)                              │
│  → If max_throughput ≤ input_rate, you NEVER catch up!              │
│    Must provision headroom (autoscaler target_util = 0.7)            │
└──────────────────────────────────────────────────────────────────────┘
```

Ref: `flink-rocksdb-state-backend-tuning.md`, `flink-unaligned-checkpoint-watermark-recovery.md`,
`flink-memory-configuration.md`

### Decision 3: Parallelism & Resource Sizing

```
┌──────────────────────────────────────────────────────────────────────┐
│  PARALLELISM RULES                                                    │
│                                                                       │
│  1. Source parallelism ≤ source partitions                           │
│     (idle subtasks waste slots)                                      │
│                                                                       │
│  2. Operator parallelism — match source to avoid extra shuffle       │
│     Exception: sink may need different parallelism for rate limits   │
│                                                                       │
│  3. max-parallelism — SET AT FIRST DEPLOY (can't change later!)     │
│     Use highly composite number (720, 360, 240)                      │
│     This determines key group count for state redistribution         │
│                                                                       │
│  4. Slots per TM — typically 1 per CPU core group (2-4)             │
│     More slots per TM = shared overhead, less isolation              │
│                                                                       │
│  MEMORY PER TASKMANAGER                                               │
│  ┌────────────────────────────────────────────────┐                  │
│  │  JVM Heap:     1-2 GB   (framework + overhead) │                  │
│  │  Managed:      2-4 GB   (RocksDB state)        │                  │
│  │  Network:      64-256MB (buffers)               │                  │
│  │  JVM Overhead: 256 MB                           │                  │
│  │  ─────────────────────────                      │                  │
│  │  Total:        ~4-8 GB per TM                   │                  │
│  └────────────────────────────────────────────────┘                  │
└──────────────────────────────────────────────────────────────────────┘
```

Ref: `flink-memory-configuration.md`, `flink-autoscaler-deep-dive.md`

### Decision 4: Watermark & Event-Time Semantics

```
┌──────────────────────────────────────────────────────────────────────┐
│  WATERMARK STRATEGY                                                   │
│                                                                       │
│  Out-of-orderness known (e.g., 30s)?                                 │
│    → forBoundedOutOfOrderness(Duration.ofSeconds(30))                │
│                                                                       │
│  Some partitions may go idle?                                         │
│    → withIdleness(Duration.ofMinutes(1))                             │
│    Without this: one idle partition stalls the entire watermark!      │
│                                                                       │
│  Late data handling:                                                  │
│    → allowedLateness(Duration.ofMinutes(5))                          │
│      + sideOutputLateData(lateTag)                                   │
│    → Late events: side-output to DLQ or separate processing         │
│                                                                       │
│  Window firing semantics:                                             │
│    ACCUMULATING (default): fire retains window state, re-fires       │
│    PURGING: fire clears window state, one-shot                       │
│    → Accumulating + allowed lateness = retractions to downstream     │
└──────────────────────────────────────────────────────────────────────┘
```

Ref: `flink-accumulating-vs-purging-and-allowed-lateness.md`,
`flink-window-event-assignment-and-state-lifecycle.md`

### Decision 5: Handling Data Skew

```
┌──────────────────────────────────────────────────────────────────────┐
│  DATA SKEW SOLUTIONS (pick based on workload)                         │
│                                                                       │
│  Problem:                                                             │
│    keyBy("user_id") where 1% of users generate 50% of events        │
│    → some subtasks get 50× more data → backpressure + slow           │
│                                                                       │
│  Solution 1: LOCAL PRE-AGGREGATION (best for aggregations)           │
│    Source → keyBy(key + subtaskId) → local aggregate                 │
│           → keyBy(key) → global aggregate                            │
│    Reduces data 10-100× before the skewed shuffle                    │
│                                                                       │
│  Solution 2: SALTING                                                  │
│    keyBy(key + random_salt) → process → keyBy(key) → merge          │
│    Spreads hot keys across N subtasks                                │
│                                                                       │
│  Solution 3: CUSTOM PARTITIONER                                      │
│    .partitionCustom() — route hot keys to dedicated subtasks         │
│    Good when you know which keys are hot                             │
│                                                                       │
│  How to detect: busyTimeMsPerSecond varies wildly across subtasks    │
└──────────────────────────────────────────────────────────────────────┘
```

Ref: `flink-blacklist-filtering-solutions.md`, `flink-autoscaler-deep-dive.md`

### Decision 6: Sink Design & End-to-End Guarantees

The sink determines whether "exactly-once" is achievable end-to-end:

```
┌──────────────────────────────────────────────────────────────────────┐
│  SINK PATTERN COMPARISON                                              │
│                                                                       │
│  Sink Type       E2E Exactly-Once?    Mechanism                      │
│  ─────────       ────────────────     ─────────                      │
│  Kafka           YES                  2-Phase Commit (transaction)   │
│  Iceberg         YES                  Atomic snapshot commit +       │
│                                       checkpoint ID in snapshot      │
│  Cassandra       YES (WAL-based)      Write-ahead log + replay      │
│  JDBC (upsert)   Effectively yes      Idempotent upsert (dedup)     │
│  JDBC (insert)   NO                   No transaction support         │
│  Elasticsearch   Effectively yes      Document ID = natural dedup    │
│  S3 (FileSink)   YES                  Pending → committed on ckpt   │
│                                                                       │
│  KEY INSIGHT: if the sink supports idempotent writes (upsert by     │
│  primary key), you don't need exactly-once checkpoints — at-least-  │
│  once + idempotent sink = effectively exactly-once, with lower      │
│  latency and simpler operations.                                      │
└──────────────────────────────────────────────────────────────────────┘
```

Ref: `flink-sink-patterns-comparison.md`, `flink-kafka-connector-source-sink-architecture.md`,
`flink-iceberg-connector-deep-dive.md`, `flink-cassandra-connector-wal-exactly-once-architecture.md`

### Decision 7: Failure Recovery & Availability

```
┌──────────────────────────────────────────────────────────────────────┐
│  FAILURE SCENARIOS & RECOVERY                                         │
│                                                                       │
│  Task failure:                                                        │
│    → Restart from latest checkpoint                                  │
│    → Rewind Kafka offsets to checkpoint position                     │
│    → Re-process events from checkpoint to present                    │
│    → Duration: state_restore + catch_up                              │
│                                                                       │
│  TaskManager crash:                                                   │
│    → Same flow, but state must download from S3 (no local cache)    │
│    → Mitigation: task-local recovery stores state copy on TM disk   │
│                                                                       │
│  JobManager crash (K8s HA):                                           │
│    → Standby JM wins ConfigMap-based leader election (~17s)          │
│    → Recovers JobGraph + checkpoints from DFS                        │
│    → TaskManagers reconnect via ConfigMap watcher                    │
│    → All tasks restart from checkpoint                               │
│    → Total: ~17s election + state restore + catch-up                 │
│                                                                       │
│  RECOVERY TIME FORMULA:                                               │
│  ──────────────────────                                               │
│  restore_time = state_download + state_rebuild + catch_up            │
│  catch_up = (checkpoint_interval × input_rate) /                     │
│             (max_throughput - input_rate)                              │
│                                                                       │
│  CRITICAL: max_throughput MUST exceed input_rate                     │
│  or the pipeline never catches up!                                    │
│  → Provision at 70% utilization (autoscaler default)                 │
└──────────────────────────────────────────────────────────────────────┘
```

Ref: `flink-kubernetes-ha-deep-dive.md`, `flink-exactly-once-checkpointing-deep-dive.md`

---

## 5. Appendix A: Full Requirements Question List

The 7 questions in Section 1 are the highest priority. Below is the complete set across
6 categories — use these for deeper requirement gathering when the interviewer provides
more time or when drilling into a specific area.

### Data Characteristics

| Question | Why it matters |
|----------|---------------|
| What's the **event schema**? Fixed or evolving? | Determines serialization format (Avro for evolution, Protobuf for speed, JSON for flexibility) |
| What's the **event size**? Avg and P99? | Drives buffer sizing, network memory, serialization cost. 100 bytes vs 10 KB are different worlds. |
| Is the data **keyed**? By what? What's the **key cardinality**? | Determines state size, key group count, potential for data skew |
| What's the **key distribution**? Uniform or skewed (hot keys)? | Skew → some subtasks get 100x more data → need pre-aggregation or custom partitioning |
| Is the data **ordered**? Does order matter for correctness? | Affects watermark strategy, allowed lateness, idleness handling |
| What's the **out-of-orderness**? Seconds, minutes, hours? | Directly sets `forBoundedOutOfOrderness()`, affects window latency and state retention |

### Volume & Throughput

| Question | Why it matters |
|----------|---------------|
| What's the **peak events/sec**? What's the **sustained rate**? | Peak determines provisioning; sustained determines steady-state cost |
| What's the **peak data rate** (MB/s or GB/s)? | Drives network memory, TaskManager sizing, source parallelism |
| Is there **seasonality** or **burst patterns**? (e.g., 10x at midnight) | Determines whether you need autoscaling, reactive mode, or over-provisioning |
| What's the **data growth rate**? (e.g., 2x per year) | Affects how much headroom to build in |

### Latency & Delivery Guarantees

| Question | Why it matters |
|----------|---------------|
| What's the **end-to-end latency SLA**? (seconds? minutes? sub-second?) | The single most impactful constraint. Sub-second → low-latency tuning. Minutes → high-throughput tuning. |
| **Exactly-once**, **at-least-once**, or **best-effort**? | Exactly-once requires checkpointing + transactional sinks (2PC). At-least-once is simpler and faster. |
| Is **idempotent** processing acceptable? | If sinks are idempotent (upsert), at-least-once + dedup is often better than exactly-once |
| What happens on **late data**? Drop? Side-output? Correct previous results? | Determines allowed lateness, side outputs, retraction/update logic |

### Source & Sink

| Question | Why it matters |
|----------|---------------|
| What's the **source**? (Kafka, Kinesis, Pulsar, CDC, files?) | Determines connector, parallelism model, offset management |
| How many **source partitions/shards**? | Sets the ceiling for source parallelism (1 subtask per partition) |
| What's the **sink**? (Kafka, S3, JDBC, Elasticsearch, data lake?) | Determines sink parallelism, batching, transactional guarantees |
| Does the sink support **transactions/2PC**? | Required for exactly-once with Kafka sink. Not available for most JDBC sinks. |
| Is the sink **rate-limited**? (e.g., DynamoDB WCU, API rate limits) | Need async I/O or backpressure-aware batching |

### State & Processing

| Question | Why it matters |
|----------|---------------|
| What **processing** happens? (filter? enrich? aggregate? join? dedup?) | Simple filter = stateless, low cost. Window aggregate = stateful, needs RocksDB + checkpoints. |
| How much **state** per key? How many keys? | Total state = keys × state_per_key. Drives state backend choice and checkpoint sizing. |
| What's the **state TTL**? | Controls state growth. Without TTL, state grows unbounded. |
| Are there **windows**? What kind? (tumbling, sliding, session?) | Sliding windows with small slide = many overlapping windows = N× state multiplication |
| Any **multi-stream joins**? | Joins require buffering both sides → large state, complex watermark alignment |

### Operations

| Question | Why it matters |
|----------|---------------|
| What's the **recovery time objective (RTO)**? | Determines checkpoint interval, state backend, local recovery strategy |
| What's the **deployment model**? (K8s, YARN, standalone?) | Affects resource management, autoscaling, failure handling |
| Do you need to **rescale** the pipeline? How often? | Savepoint → rescale → restart. Affects state format and key group settings. |

---

## 6. Appendix B: All Design Decisions Ranked

Complete list from most to least important. The top 7 (marked with **) are covered in
depth in Section 4. The rest are follow-up discussion topics if time permits or if the
interviewer drills into a specific area.

| Rank | Decision | One-Line Justification | Ref Doc |
|------|----------|----------------------|---------|
| **1** | **Exactly-once vs at-least-once** | Drives checkpoint mode, sink choice, throughput, and complexity | `flink-exactly-once-checkpointing-deep-dive.md` |
| **2** | **State backend (RocksDB vs HashMap)** | Determines memory model, checkpoint type, and operational complexity | `flink-rocksdb-state-backend-tuning.md` |
| **3** | **Checkpoint strategy (interval, incremental, unaligned)** | Directly impacts RTO, throughput overhead, and recovery time | `flink-unaligned-checkpoint-watermark-recovery.md` |
| **4** | **Parallelism & resource sizing** | Under-provisioned = backpressure; over-provisioned = wasted cost | `flink-memory-configuration.md` |
| **5** | **Watermark & event-time handling** | Wrong watermark = incorrect windows, stalled pipelines, or lost data | `flink-accumulating-vs-purging-and-allowed-lateness.md` |
| **6** | **Sink pattern & end-to-end guarantee** | Sink capability determines achievable delivery semantics | `flink-sink-patterns-comparison.md` |
| **7** | **Failure recovery & HA** | Shows understanding of production reliability requirements | `flink-kubernetes-ha-deep-dive.md` |
| 8 | Data skew / hot key handling | Pre-aggregation, salting, custom partitioner | `flink-blacklist-filtering-solutions.md` |
| 9 | State TTL & cleanup strategy | Without TTL, state grows unbounded → OOM | `flink-state-ttl-architecture.md` |
| 10 | Serialization format | Avro (evolution) vs Protobuf (speed) vs POJO (convenience) | `flink-serialization-deep-dive.md` |
| 11 | KeyBy design (key choice & cardinality) | Bad key = skew, unbounded state, or incorrect semantics | — |
| 12 | Monitoring & alerting metrics | Consumer lag, checkpoint duration, backpressure, watermark lag | — |
| 13 | Network buffer tuning (latency vs throughput) | Buffer timeout, segment size, credit-based flow control | `flink-network-credit-flow-control-tuning.md` |
| 14 | Window type selection (tumbling/sliding/session) | Sliding windows multiply state by window/slide ratio | `flink-window-state-rocksdb-checkpoint-deep-dive.md` |
| 15 | Schema evolution strategy | Must plan for schema changes without pipeline restart | `flink-serialization-deep-dive.md` |
| 16 | Autoscaling | Vertex-level parallelism adjustment based on busy time + backlog | `flink-autoscaler-deep-dive.md` |
| 17 | Pipeline upgrade path | Savepoint → stop → deploy → restore. Requires uid() on all operators. | — |
| 18 | Rescaling & max-parallelism | max-parallelism frozen at first deploy; use composite number (720) | `flink-autoscaler-deep-dive.md` |
| 19 | Sink-specific tuning (Iceberg/Kafka/JDBC) | Batch size, commit frequency, 2PC vs idempotent | `flink-iceberg-connector-deep-dive.md` |
| 20 | Cost optimization | Spot for TMs, reserved for JM, right-size managed memory | `flink-memory-configuration.md` |
| 21 | Multi-tenancy & isolation | Separate jobs vs keyBy with tenant prefix | — |
---

## Related Documents in This Repo

- `flink-exactly-once-checkpointing-deep-dive.md` — checkpoint barriers, exactly-once vs end-to-end
- `flink-kafka-connector-source-sink-architecture.md` — Kafka source/sink architecture, 2PC
- `flink-iceberg-connector-deep-dive.md` — Iceberg sink, snapshot commit, MOR/COW
- `flink-cassandra-connector-wal-exactly-once-architecture.md` — Cassandra WAL-based exactly-once
- `flink-sink-patterns-comparison.md` — sink delivery guarantees comparison
- `flink-rocksdb-state-backend-tuning.md` — RocksDB memory, compaction, checkpoints
- `flink-memory-configuration.md` — JM/TM memory models, OOM debugging
- `flink-network-credit-flow-control-tuning.md` — network tuning for throughput vs latency
- `flink-unaligned-checkpoint-watermark-recovery.md` — unaligned checkpoints & watermark state
- `flink-accumulating-vs-purging-and-allowed-lateness.md` — window firing & allowed lateness
- `flink-window-event-assignment-and-state-lifecycle.md` — window assignment & lifecycle
- `flink-window-state-rocksdb-checkpoint-deep-dive.md` — window state internals
- `flink-serialization-deep-dive.md` — serialization formats & type system
- `flink-state-ttl-architecture.md` — State TTL cleanup strategies
- `flink-blacklist-filtering-solutions.md` — filtering patterns for large ID sets
- `flink-autoscaler-deep-dive.md` — autoscaler metrics, algorithm, configuration
- `flink-kubernetes-ha-deep-dive.md` — K8s HA, ConfigMap leader election, failover
