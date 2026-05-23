# Flink Autoscaler Deep Dive

How the Flink Kubernetes Operator Autoscaler collects metrics, evaluates load, computes
target parallelism per vertex, and executes scaling decisions — with full formulas and
source-code references.

Based on the
[official autoscaler docs](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/autoscaler/),
[FLIP-271](https://cwiki.apache.org/confluence/display/FLINK/FLIP-271:+Autoscaling),
the OSDI'18 paper *"Three steps is all you need"* (Kalavri et al.), and the
[flink-autoscaler source code](https://github.com/apache/flink-kubernetes-operator/tree/main/flink-autoscaler).

---

# Part 1: Overview

## What the Autoscaler Does

The autoscaler adjusts **per-vertex parallelism** (not whole-job parallelism) so that every
operator in the pipeline runs backpressure-free at a user-defined target utilization.

Unlike CPU/memory-based autoscalers, it reasons about **data throughput**: how fast records
flow in, how fast each operator can process them, and how much backlog has accumulated.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    WHY VERTEX-LEVEL SCALING MATTERS                     │
│                                                                         │
│  Uniform scaling (all operators same parallelism):                      │
│                                                                         │
│    Source(p=10) ──▶ Filter(p=10) ──▶ Aggregate(p=10) ──▶ Sink(p=10)    │
│    100K rec/s        100K rec/s       1K rec/s           1K rec/s       │
│    busy=80%          busy=5%  ✗       busy=90%           busy=2%  ✗    │
│                      (wasted)                            (wasted)       │
│                                                                         │
│  Vertex-level scaling:                                                  │
│                                                                         │
│    Source(p=10) ──▶ Filter(p=1) ──▶ Aggregate(p=12) ──▶ Sink(p=1)     │
│    100K rec/s       100K rec/s      1K rec/s            1K rec/s        │
│    busy=70%         busy=70% ✓      busy=70%  ✓         busy=70% ✓    │
│                                                                         │
│  Same throughput, 14 slots instead of 40.                               │
└─────────────────────────────────────────────────────────────────────────┘
```

## The "Three Steps" Principle

The algorithm is proven to converge in **at most 3 scaling iterations** (often just 1):

1. **Measure** — Collect busy time, throughput, and backlog from each vertex
2. **Propagate** — Walk the DAG from sources to sinks, computing the target data rate
   for every vertex based on its upstream output rates
3. **Scale** — Compute per-vertex parallelism from (target rate / true processing rate)
   at the desired utilization

---

# Part 2: Autoscaler Architecture

## Pipeline Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                     AUTOSCALER PIPELINE                               │
│                                                                       │
│  ┌─────────────────┐    ┌─────────────────┐    ┌──────────────────┐  │
│  │  1. METRIC       │    │  2. METRIC       │    │  3. SCALING      │  │
│  │     COLLECTION   │───▶│     EVALUATION   │───▶│     EXECUTION    │  │
│  │                  │    │                  │    │                  │  │
│  │  ScalingMetric-  │    │  ScalingMetric-  │    │  ScalingExecutor │  │
│  │  Collector       │    │  Evaluator       │    │  + JobVertex-    │  │
│  │                  │    │                  │    │    Scaler         │  │
│  └────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘  │
│           │                       │                       │            │
│      Flink REST API          Compute TPR,            Apply new        │
│      every cycle             target rates,           parallelism      │
│      (metrics window)        thresholds              (restart or      │
│                                                       in-place)       │
│                                                                       │
│  Orchestrated by: JobAutoScalerImpl                                   │
│  State stored in: AutoScalerStateStore                                │
└──────────────────────────────────────────────────────────────────────┘
```

## Control Flow Detail

```
JobAutoScalerImpl.scale()
  │
  ├── 1. ScalingMetricCollector.updateMetrics()
  │       ├── Fetch job topology (REST: /jobs/:id/plan)
  │       ├── Fetch vertex metrics (REST: /jobs/:id/vertices/:vid)
  │       │     busyTimeMsPerSecond, numRecordsInPerSecond,
  │       │     numRecordsOutPerSecond, pendingRecords
  │       ├── Accumulate into sliding window (default 15 min)
  │       └── Check stabilization interval (skip if just restarted)
  │
  ├── 2. ScalingMetricEvaluator.evaluate()
  │       ├── For each vertex:
  │       │     ├── Compute True Processing Rate (TPR)
  │       │     ├── Compute target data rate (from upstream outputs)
  │       │     └── Compute scale-up / scale-down thresholds
  │       └── Return Map<JobVertexID, EvaluatedScalingMetric>
  │
  └── 3. ScalingExecutor.scaleResource()
          ├── For each vertex:
          │     ├── JobVertexScaler.computeScaleTargetParallelism()
          │     ├── Apply min/max bounds, key-group alignment
          │     └── Apply delayed scale-down logic
          ├── Validate memory pressure (GC, heap usage)
          └── Trigger rescaling (job upgrade or in-place via REST)
```

---

# Part 3: Metrics Used for Scaling

## Primary Metrics (Collected from Flink)

| Metric | Source | Description |
|--------|--------|-------------|
| `busyTimeMsPerSecond` | Task | Milliseconds per second spent processing data (0–1000). Excludes idle & backpressure time. |
| `idleTimeMsPerSecond` | Task | Time waiting for input (no records available) |
| `backPressuredTimeMsPerSecond` | Task | Time waiting for output buffer space |
| `numRecordsInPerSecond` | Task/Operator | Records flowing into the vertex per second |
| `numRecordsOutPerSecond` | Task/Operator | Records flowing out of the vertex per second |
| `pendingRecords` | Source | Unconsumed records in the source (e.g., Kafka lag) |
| `pendingBytes` | Source | Unconsumed bytes in the source |
| `currentEmitEventTimeLag` | Source | Delay between event time and emit time |

## Derived Metrics (Computed by Autoscaler)

| Metric (ScalingMetric enum) | Formula / Meaning |
|-----------------------------|-------------------|
| `LOAD` | `busyTimeMsPerSecond / 1000` — utilization ratio [0, 1] |
| `TRUE_PROCESSING_RATE` | Records/sec the vertex can sustain at full capacity |
| `OBSERVED_TPR` | TPR measured during backlog catch-up (lag-based) |
| `TARGET_DATA_RATE` | Required processing rate (from upstream outputs + backlog) |
| `CATCH_UP_DATA_RATE` | Extra rate needed to drain accumulated lag |
| `LAG` | Total pending records at source vertex |
| `SCALE_UP_RATE_THRESHOLD` | Rate boundary above which scale-up triggers |
| `SCALE_DOWN_RATE_THRESHOLD` | Rate boundary below which scale-down triggers |
| `EXPECTED_PROCESSING_RATE` | Predicted rate after applying the new parallelism |
| `GC_PRESSURE` | Max GC time % across task managers |
| `HEAP_MEMORY_USED` | Heap consumption (for memory-pressure gating) |

Source: `ScalingMetric.java`, `EvaluatedScalingMetric.java`

---

# Part 4: How Metrics Are Calculated in Flink Runtime

## Busy Time — The Core Utilization Signal

The autoscaler's most important input. A task alternates between three states in its
mailbox processing loop:

```
┌──────────────────────────────────────────────────────────────────┐
│                    TASK TIME BREAKDOWN (per second)               │
│                                                                   │
│  ◀────────────────── 1000 ms ──────────────────────▶             │
│                                                                   │
│  ┌────────────┐┌──────────────────┐┌───────────────┐             │
│  │ IDLE TIME  ││    BUSY TIME     ││ BACK-PRESSURE │             │
│  │            ││                  ││    TIME        │             │
│  │ Waiting    ││ Processing       ││ Waiting for   │             │
│  │ for input  ││ records          ││ output buffer │             │
│  │            ││                  ││               │             │
│  │ input not  ││ (the remainder)  ││ output not    │             │
│  │ available  ││                  ││ available     │             │
│  └────────────┘└──────────────────┘└───────────────┘             │
│                                                                   │
│  busyTime = 1000 - idleTime - backPressuredTime                  │
│                                                                   │
│  Example: idle=200ms, backpressure=100ms → busy=700ms            │
│           → LOAD = 700/1000 = 0.70 (70% utilized)               │
└──────────────────────────────────────────────────────────────────┘
```

### Where It's Computed

**`TaskIOMetricGroup.java`** (flink-runtime):

```java
// The actual calculation:
double getBusyTimePerSecond() {
    double busyTime = idleTimePerSecond.getValue()
                    + getBackPressuredTimeMsPerSecond();
    return busyTimeEnabled
        ? 1000.0 - Math.min(busyTime, 1000.0)
        : Double.NaN;
}
```

The idle and backpressure timers are `TimerGauge` instances — they use `markStart()` /
`markEnd()` pairs in the mailbox loop and maintain a rolling 60-second window.

### Where the Timing Is Triggered

**`StreamTask.java`** (flink-streaming-java) — inside the mailbox default action:

```java
// Simplified from StreamTask.processInput()
if (!recordWriter.isAvailable()) {
    // Output buffer full → track SOFT BACK-PRESSURE
    timer = ioMetrics.getSoftBackPressuredTimePerSecond();
    resumeFuture = recordWriter.getAvailableFuture();
} else if (!inputProcessor.isAvailable()) {
    // No input data → track IDLE TIME
    timer = ioMetrics.getIdleTimeMsPerSecond();
    resumeFuture = inputProcessor.getAvailableFuture();
}
// Suspend processing, start timing
controller.suspendDefaultAction(timer);
// When data/buffer becomes available → timer.markEnd()
```

Key insight: **busy time is never measured directly** — it's the leftover after subtracting
idle + backpressure from 1000ms. If a task is neither waiting for input nor blocked on
output, it must be processing records.

Source: `TaskIOMetricGroup.java`, `StreamTask.java`, `TimerGauge.java`

## Record Throughput — numRecordsIn/Out

Simple `SumCounter` instances on `TaskIOMetricGroup` that aggregate across all operators
in the task's chain. The per-second rate variants (`numRecordsInPerSecond`) are computed
by the metrics reporter using the counter's delta over time.

```java
// TaskIOMetricGroup.java
private final SumCounter numRecordsIn;   // IO_NUM_RECORDS_IN
private final SumCounter numRecordsOut;  // IO_NUM_RECORDS_OUT
// Rate meters derived automatically by metrics system
```

Source: `TaskIOMetricGroup.java`, `InternalOperatorIOMetricGroup.java`, `MetricNames.java`

## Pending Records (Lag) — Source Backlog

Reported by source connectors that implement the new Source API. Each source reader
registers a gauge:

```java
// InternalSourceReaderMetricGroup.java
public void setPendingRecordsGauge(Gauge<Long> pendingRecordsGauge) {
    gauge(MetricNames.PENDING_RECORDS, pendingRecordsGauge);
}
```

For **Kafka**, this is `endOffset - currentOffset` across all assigned partitions.
Sources that don't expose `pendingRecords` cannot use backlog-based scaling — only
busy-time-based scaling applies.

## Event Time Lag

Tracks the difference between event time and processing time at the source:

```java
// InternalSourceReaderMetricGroup.java
long getEmitTimeLag() {
    return lastEventTime != NO_TIMESTAMP
        ? getLastEmitTime() - lastEventTime
        : UNDEFINED;
}
```

A `PausableRelativeClock` in `SourceOperator` pauses the emit-time clock during
backpressure so the lag metric reflects true source delay, not downstream slowness.

Source: `InternalSourceReaderMetricGroup.java`, `SourceOperator.java`

---

# Part 5: The Three-Step Algorithm

## Step 1: Measure True Processing Rate (TPR) per Vertex

TPR answers: *"If this operator were running at 100% utilization, how many records/sec
could it process?"*

### Busy-Time-Based TPR (Default)

```
                     numRecordsInPerSecond
TPR_busy_time = ─────────────────────────────
                  busyTimeMsPerSecond / 1000

Example:
  Input rate = 7,000 rec/s
  Busy time  = 700 ms/s  (70% utilized)
  TPR = 7,000 / 0.70 = 10,000 rec/s at full capacity
```

### Observed / Lag-Based TPR

When the vertex is catching up on backlog (processing faster than input arrives),
the autoscaler can measure TPR directly from the observed throughput:

```
                       numRecordsInPerSecond
TPR_observed = ─────────────────────────────────────
                 1 - backPressureRatio

Used when: LAG > LAG_THRESHOLD × InputRate
```

### Which TPR Is Selected?

```
┌──────────────────────────────────────────────────────────┐
│              TPR SELECTION LOGIC                          │
│              (ScalingMetricEvaluator.selectTprMetric)     │
│                                                          │
│  Is there significant backlog?                           │
│    │                                                     │
│    ├── NO  → Use TPR_busy_time                           │
│    │                                                     │
│    └── YES → Compare both:                               │
│              │                                           │
│              ├── TPR_observed ≥ 85% of TPR_busy_time     │
│              │   → Use TPR_busy_time (they agree)        │
│              │                                           │
│              └── TPR_observed < 85% of TPR_busy_time     │
│                  → Use TPR_observed (more accurate       │
│                    under sustained load)                  │
│                                                          │
│  The 15% threshold is configurable via                   │
│  observed-true-processing-rate.switch-threshold          │
└──────────────────────────────────────────────────────────┘
```

Why the distinction? Busy-time-based TPR can **overestimate** capacity during sustained
high load because GC pauses, serialization costs, and cache effects reduce real throughput
in ways that aren't captured by the busy/idle timer split.

Source: `ScalingMetricEvaluator.java`, `ScalingMetrics.java`

## Step 2: Compute Target Data Rate per Vertex

Walk the DAG topologically from sources to sinks:

```
┌─────────────────────────────────────────────────────────────────┐
│              TARGET RATE PROPAGATION                              │
│                                                                   │
│  Kafka Source                                                     │
│  (50K rec/s input,    ──── target_rate = input_rate + lag_rate    │
│   10K backlog,               = 50K + 10K/catchup_duration        │
│   catchup=30min)             = 50K + 5.6 = ~50,006 rec/s         │
│       │                                                           │
│       │  output_ratio = numRecordsOut / numRecordsIn = 0.8       │
│       ▼                                                           │
│  Filter Operator     ──── target_rate = upstream_output_rate      │
│  (80% selectivity)          = 50,006 × 0.8 = 40,005 rec/s       │
│       │                                                           │
│       │  output_ratio = 1.0                                      │
│       ▼                                                           │
│  Aggregator          ──── target_rate = 40,005 rec/s             │
│       │                                                           │
│       │  output_ratio = 0.01 (window aggregation)                │
│       ▼                                                           │
│  Sink                ──── target_rate = 400 rec/s                │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

For **source vertices**, the target rate includes catch-up for backlog:

```
target_rate_source = input_rate + (LAG / catch_up_duration)
                   + (input_rate × restart_time / catch_up_duration)
                     ↑                              ↑
                     drain accumulated lag           account for lag growth
                                                    during restart
```

For **downstream vertices**:

```
target_rate = Σ (upstream_output_rate × edge_output_ratio)
```

Source: `ScalingMetricEvaluator.evaluate()`, `AutoScalerUtils.getTargetProcessingCapacity()`

## Step 3: Compute New Parallelism per Vertex

```
                    target_data_rate
scale_factor = ────────────────────────────────────
                avg_true_processing_rate × target_utilization

new_parallelism = current_parallelism × scale_factor
```

Expanded with all components from `AutoScalerUtils.getTargetProcessingCapacity()`:

```
Target Processing Capacity =
    (LAG / catch_up_duration)                         ← drain backlog
  + (input_rate × restart_time / catch_up_duration)   ← compensate restart lag
  + (input_rate / target_utilization)                  ← steady-state at target util

new_parallelism = current_parallelism × (capacity / avg_TPR)
```

### Constraints Applied

```
┌──────────────────────────────────────────────────────────┐
│  PARALLELISM CONSTRAINTS (JobVertexScaler)                │
│                                                          │
│  1. Scale factor clamped:                                │
│     max_scale_down_factor ≤ factor ≤ max_scale_up_factor │
│     (default: 0.6 ≤ factor ≤ 100000)                    │
│                                                          │
│  2. Vertex bounds:                                       │
│     vertex.min-parallelism ≤ p ≤ vertex.max-parallelism  │
│     (default: 1 ≤ p ≤ 200)                              │
│                                                          │
│  3. Key-group alignment:                                 │
│     max_parallelism % new_parallelism == 0               │
│     (prevents data skew across subtasks)                 │
│                                                          │
│  4. Source partition alignment:                           │
│     new_parallelism ≤ num_source_partitions              │
│     (idle subtasks with no partition are wasteful)        │
│                                                          │
│  5. Round to nearest valid divisor of max-parallelism    │
└──────────────────────────────────────────────────────────┘
```

Source: `JobVertexScaler.computeScaleTargetParallelism()`, `AutoScalerUtils.java`

---

# Part 6: Utilization Target & Scaling Thresholds

The autoscaler does NOT scale on every minor fluctuation. It defines a **dead zone**
around the target utilization where no action is taken.

```
┌──────────────────────────────────────────────────────────────────┐
│              UTILIZATION SCALING BANDS                             │
│                                                                   │
│  target_utilization = 0.7  (70%)                                 │
│  utilization_boundary = 0.3                                       │
│                                                                   │
│  0%     40%              70%              100%                    │
│  │───────│────────────────│────────────────│──────▶ utilization   │
│          │                │                │                       │
│          │   NO SCALING   │   NO SCALING   │                      │
│          │   (within      │   (within      │                      │
│          │    bounds)     │    bounds)     │                      │
│          │                │                │                       │
│  ◀───────┤                                ├──────▶               │
│  SCALE   │     target - boundary = 0.4    │  SCALE                │
│  DOWN    │     target + boundary = 1.0    │  UP                   │
│          │                                │                       │
│  Util    │   Dead zone: 40% – 100%       │  Util                 │
│  < 40%   │   No scaling triggered         │  > 100%              │
│          │                                │  (backpressured)      │
└──────────────────────────────────────────────────────────────────┘
```

This translates to **processing rate thresholds**:

```
scale_up_threshold   = target_rate / (target_util - boundary)
                     = target_rate / 0.4

scale_down_threshold = target_rate / (target_util + boundary)
                     = target_rate / 1.0

If current_rate < scale_down_threshold → SCALE DOWN
If current_rate > scale_up_threshold   → SCALE UP
Otherwise                              → NO ACTION
```

When backlog is present (actively catching up), the scale-up threshold uses maximum
utilization (1.0) to trigger scale-out more aggressively.

Source: `ScalingMetricEvaluator.java`

---

# Part 7: Stabilization & Cooldown Mechanisms

## Overview

```
┌────────────────────────────────────────────────────────────────────┐
│             ANTI-OSCILLATION SAFEGUARDS                             │
│                                                                     │
│  ┌──────────────┐   ┌──────────────┐   ┌────────────────────────┐  │
│  │ STABILIZATION│   │ SCALE-DOWN   │   │ INEFFECTIVE SCALING    │  │
│  │ INTERVAL     │   │ INTERVAL     │   │ DETECTION              │  │
│  │              │   │              │   │                        │  │
│  │ 5 min after  │   │ 1 hour delay │   │ If scale-up doesn't   │  │
│  │ restart:     │   │ before       │   │ improve throughput by  │  │
│  │ ignore all   │   │ executing    │   │ ≥10%, mark vertex as  │  │
│  │ metrics      │   │ any scale-   │   │ "ineffective" and     │  │
│  │              │   │ down         │   │ block further scale-up │  │
│  └──────────────┘   └──────────────┘   └────────────────────────┘  │
│                                                                     │
│  ┌──────────────┐   ┌──────────────┐                               │
│  │ METRICS      │   │ SCALE FACTOR │                               │
│  │ WINDOW       │   │ LIMITS       │                               │
│  │              │   │              │                               │
│  │ Average over │   │ Max down:    │                               │
│  │ 15 min to    │   │ 0.6× current │                               │
│  │ smooth       │   │ Max up:      │                               │
│  │ fluctuations │   │ 100000×      │                               │
│  └──────────────┘   └──────────────┘                               │
└────────────────────────────────────────────────────────────────────┘
```

## Stabilization Interval (default 5 min)

After a job restarts (due to scaling or failure), metrics are unreliable — caches are cold,
state is being restored, Kafka consumer groups are rebalancing. The autoscaler ignores all
metrics during this window.

Config: `job.autoscaler.stabilization.interval`

## Scale-Down Interval (default 1 hour)

When a scale-down is recommended, the autoscaler **defers** it. This serves two purposes:

1. **Batches multiple reductions** — If load continues dropping, the next evaluation may
   recommend an even smaller parallelism, avoiding two restarts where one suffices
2. **Reduces restart frequency** — Each restart incurs downtime; fewer restarts = higher
   availability

Implementation: `DelayedScaleDown` uses a **monotonic deque** to track the maximum
recommended parallelism within the window. Only when the window expires does the scale-down
execute.

Config: `job.autoscaler.scale-down.interval`

## Ineffective Scaling Detection

If a scale-up is applied but the observed throughput increase is < 10% of the expected
increase, the vertex is marked as "ineffectively scaled". This prevents the autoscaler
from repeatedly scaling up a vertex that is bottlenecked by something parallelism can't
fix (e.g., a slow external service, lock contention, data skew).

Source: `JobVertexScaler.java`, `DelayedScaleDown.java`, `ScalingTracking.java`

---

# Part 8: Key Group & Partition Alignment

## The Divisor Constraint

Flink distributes state across **key groups**, and key groups are assigned to subtasks.
If `new_parallelism` is not a divisor of `max-parallelism`, some subtasks get more key
groups than others → **data skew**.

```
max-parallelism = 720

Good parallelism values (divisors of 720):
  1, 2, 3, 4, 5, 6, 8, 9, 10, 12, 15, 16, 18, 20, 24, 30,
  36, 40, 45, 48, 60, 72, 80, 90, 120, 144, 180, 240, 360, 720
  → 30 valid choices!

Bad max-parallelism (e.g., 128 = 2^7):
  1, 2, 4, 8, 16, 32, 64, 128
  → Only 8 valid choices, all powers of 2
```

**Recommendation**: Set `pipeline.max-parallelism` to a **highly composite number**:
`720`, `360`, `240`, `180`, `120`.

## Source Partition Alignment

For sources like Kafka, each subtask reads from one or more partitions. If
`parallelism > num_partitions`, some subtasks sit idle.

Two alignment modes exist:

| Mode | Behavior |
|------|----------|
| `EVENLY_SPREAD` | Parallelism must divide partition count evenly (strict) |
| `MAXIMIZE_UTILISATION` | Parallelism ≤ partitions, no divisor requirement (flexible) |

Source: `JobVertexScaler.java`

---

# Part 9: Putting It All Together — End-to-End Example

## Scenario

A Kafka-sourced pipeline experiences a 2x traffic spike. The Kafka topic has 100 partitions,
`pipeline.max-parallelism = 720`, `target_utilization = 0.7`, `boundary = 0.3`,
`catch_up_duration = 30 min (1800s)`, `restart_time = 5 min (300s)`.

```
┌──────────────────────────────────────────────────────────────────────┐
│  CURRENT STATE (before scaling)                                       │
│                                                                       │
│    Source(p=50)  ──▶ Filter(p=10) ──▶ Agg(p=30) ──▶ Sink(p=5)       │
│                                                                       │
│    input=50K/s       input=40K/s      input=40K/s    input=400/s     │
│    output=50K/s      output=40K/s     output=400/s   output=400/s    │
│    busy=700ms        busy=400ms       busy=950ms     busy=200ms      │
│    LAG=500K rec      output_ratio     output_ratio   output_ratio    │
│                      = 40K/50K = 0.8  = 400/40K      = 400/400       │
│                                       = 0.01         = 1.0           │
│                                                                       │
│  Traffic just doubled: new input rate will be 100K rec/s              │
└──────────────────────────────────────────────────────────────────────┘
```

## Step 1: Compute True Processing Rate (TPR) per Vertex

TPR = what this vertex could process at 100% utilization (extrapolated from current load).

```
┌─────────────────────────────────────────────────────────────────────┐
│  VERTEX         FORMULA                         RESULT              │
│  ──────         ───────                         ──────              │
│                                                                     │
│  Source    TPR = 50,000 / (700/1000)          = 71,429 rec/s       │
│                 ───────   ─────────             (total across       │
│                 input     busy ratio             all 50 subtasks)   │
│                 rate      = 0.70                                    │
│                                                                     │
│  Filter   TPR = 40,000 / (400/1000)          = 100,000 rec/s      │
│                          busy ratio = 0.40     (very underloaded!) │
│                                                                     │
│  Agg      TPR = 40,000 / (950/1000)          = 42,105 rec/s       │
│                          busy ratio = 0.95     (nearly saturated)  │
│                                                                     │
│  Sink     TPR =    400 / (200/1000)          = 2,000 rec/s        │
│                          busy ratio = 0.20     (mostly idle)       │
│                                                                     │
│  Per-subtask TPR (used for parallelism calc):                       │
│  Source: 71,429 / 50 = 1,429 rec/s per subtask                    │
│  Filter: 100,000 / 10 = 10,000 rec/s per subtask                  │
│  Agg:    42,105 / 30 = 1,404 rec/s per subtask                    │
│  Sink:   2,000 / 5 = 400 rec/s per subtask                        │
└─────────────────────────────────────────────────────────────────────┘
```

## Step 2: Compute Target Data Rate per Vertex

Walk the DAG from source to sink. The source must also drain the backlog.

```
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCE target rate:                                                │
│                                                                     │
│    steady_state      = input_rate / target_util                     │
│                      = 100,000 / 0.7                                │
│                      = 142,857 rec/s                                │
│                                                                     │
│    lag_catchup       = LAG / catch_up_duration                      │
│                      = 500,000 / 1800                               │
│                      = 278 rec/s                                    │
│                                                                     │
│    restart_catchup   = input_rate × restart_time / catch_up_dur     │
│                      = 100,000 × 300 / 1800                        │
│                      = 16,667 rec/s                                 │
│                        ↑ During 5 min restart, 100K/s × 300s =      │
│                          30M records accumulate. Drain them in      │
│                          1800s = 16,667/s extra needed              │
│                                                                     │
│    TOTAL TARGET      = 142,857 + 278 + 16,667                      │
│                      = 159,802 rec/s                                │
│                                                                     │
│  ────────────────────────────────────────────────────────────────── │
│                                                                     │
│  FILTER target rate:                                                │
│    = source_target × output_ratio                                   │
│    = 159,802 × 0.8                                                  │
│    = 127,842 rec/s                                                  │
│                                                                     │
│  AGG target rate:                                                   │
│    = filter_target × output_ratio_filter                            │
│    = 127,842 × 1.0   (filter passes all output to agg)             │
│    = 127,842 rec/s                                                  │
│                                                                     │
│  SINK target rate:                                                  │
│    = agg_target × output_ratio_agg                                  │
│    = 127,842 × 0.01                                                 │
│    = 1,278 rec/s                                                    │
└─────────────────────────────────────────────────────────────────────┘
```

## Step 3: Compute New Parallelism per Vertex

```
new_parallelism = target_rate / per_subtask_TPR
```

The per-subtask TPR represents how much one subtask can handle at 100% — dividing the
target rate by it gives the number of subtasks needed.

```
┌──────────────────────────────────────────────────────────────────────┐
│  VERTEX   RAW CALC                        CONSTRAINTS → FINAL       │
│  ──────   ────────                        ──────────────────        │
│                                                                      │
│  Source   159,802 / 1,429 = 111.8        Kafka has 100 partitions  │
│                                           p > 100 → idle subtasks   │
│                                           CLAMP to 100              │
│                                           720 % 100 = 20 (not 0!)  │
│                                           Nearest divisor of 720    │
│                                           ≤ 100 → p = 90           │
│                                           → Source: p = 90  ✓      │
│                                                                      │
│  Filter   127,842 / 10,000 = 12.8       Round up → 13             │
│                                           720 % 13 = 5 (not 0!)    │
│                                           Nearest divisor: 15       │
│                                           → Filter: p = 15  ✓     │
│                                                                      │
│  Agg      127,842 / 1,404 = 91.1        720 % 91 = 10 (not 0!)   │
│                                           Nearest divisor: 90       │
│                                           Within bounds [1, 200]    │
│                                           → Agg: p = 90  ✓        │
│                                                                      │
│  Sink     1,278 / 400 = 3.2             720 % 3 = 0 ✓             │
│                                           But scale_down_factor:    │
│                                           current=5, 5×0.6=3       │
│                                           3.2 > 3 → allowed        │
│                                           Round to divisor: 3       │
│                                           → Sink: p = 3  ✓        │
│                                                                      │
│  RESULT:                                                             │
│    Source(p=90) → Filter(p=15) → Agg(p=90) → Sink(p=3)             │
└──────────────────────────────────────────────────────────────────────┘
```

### Why Each Vertex Got Its Specific Number

| Vertex | Why This Number |
|--------|----------------|
| **Source p=90** | Raw calc said 112, but capped at 100 (Kafka partitions). Then 100 is not a divisor of 720, so rounded down to 90 (720/8). Avoids idle subtasks AND key-group skew. |
| **Filter p=15** | Only needs 12.8 subtasks — very cheap operator (high per-subtask TPR). 13 isn't a divisor of 720, rounded to 15 (720/48). Filter was already underloaded at p=10, so modest increase suffices. |
| **Agg p=90** | This is the bottleneck — was at 95% utilization. Needs 91.1 subtasks but 91 isn't a divisor of 720, so rounded to 90 (720/8). Gets the biggest absolute increase (30→90 = 3x). |
| **Sink p=3** | Output ratio is 0.01 (window aggregation reduces volume 100x). Only needs 3.2 subtasks. Actually **scales down** from 5→3 because it was over-provisioned. |

## When Does the Autoscaler Stop Scaling?

After applying the new parallelism, the autoscaler re-evaluates on the next cycle.
Scaling stops when ALL vertices fall within the utilization dead zone:

```
┌──────────────────────────────────────────────────────────────────────┐
│  AFTER SCALING — NEXT EVALUATION CYCLE                               │
│                                                                       │
│  Source(p=90):                                                        │
│    new_input = 100K rec/s                                             │
│    capacity  = 90 × 1,429 = 128,610 rec/s                            │
│    util = 100K / 128,610 = 0.78  (78%)                               │
│    Dead zone: [0.4, 1.0]  →  0.40 ≤ 0.78 ≤ 1.00  → NO SCALING ✓   │
│                                                                       │
│  Filter(p=15):                                                        │
│    new_input = 100K × 0.8 = 80K rec/s                                │
│    capacity  = 15 × 10,000 = 150,000 rec/s                           │
│    util = 80K / 150K = 0.53  (53%)                                   │
│    Dead zone: [0.4, 1.0]  →  0.40 ≤ 0.53 ≤ 1.00  → NO SCALING ✓   │
│                                                                       │
│  Agg(p=90):                                                          │
│    new_input = 80K rec/s                                              │
│    capacity  = 90 × 1,404 = 126,360 rec/s                            │
│    util = 80K / 126,360 = 0.63  (63%)                                │
│    Dead zone: [0.4, 1.0]  →  0.40 ≤ 0.63 ≤ 1.00  → NO SCALING ✓   │
│                                                                       │
│  Sink(p=3):                                                          │
│    new_input = 80K × 0.01 = 800 rec/s                                │
│    capacity  = 3 × 400 = 1,200 rec/s                                 │
│    util = 800 / 1,200 = 0.67  (67%)                                  │
│    Dead zone: [0.4, 1.0]  →  0.40 ≤ 0.67 ≤ 1.00  → NO SCALING ✓   │
│                                                                       │
│  ALL VERTICES IN DEAD ZONE → AUTOSCALER IS STABLE                    │
│  No further scaling actions will be taken until load changes.         │
└──────────────────────────────────────────────────────────────────────┘
```

## Seven Reasons the Autoscaler Will NOT Scale Further

```
┌──────────────────────────────────────────────────────────────────────┐
│  WHEN THE AUTOSCALER HOLDS STEADY (no more scaling)                   │
│                                                                       │
│  1. UTILIZATION IN DEAD ZONE                                          │
│     All vertices: (target - boundary) ≤ util ≤ (target + boundary)   │
│     With defaults: 40% ≤ util ≤ 100%                                 │
│     → No threshold crossed, no action needed.                         │
│                                                                       │
│  2. STABILIZATION INTERVAL NOT ELAPSED                                │
│     Job just restarted after previous scaling.                        │
│     Metrics are unreliable (cold caches, state restore).              │
│     → Wait 5 minutes before even looking at metrics.                  │
│                                                                       │
│  3. SCALE-DOWN DEFERRED                                               │
│     Scale-down recommended but within the 1-hour delay window.       │
│     → Wait for window to expire, batch multiple reductions.           │
│                                                                       │
│  4. ALREADY AT BOUNDS                                                 │
│     Vertex hit vertex.max-parallelism (200) or min-parallelism (1).  │
│     → Can't go higher/lower regardless of metrics.                    │
│                                                                       │
│  5. INEFFECTIVE SCALING DETECTED                                      │
│     Previous scale-up didn't improve throughput by ≥10%.              │
│     Bottleneck is elsewhere (external DB, data skew, lock).           │
│     → Vertex marked ineffective, further scale-up blocked.            │
│                                                                       │
│  6. SOURCE PARTITION CEILING                                          │
│     Source parallelism = num Kafka partitions.                        │
│     Going higher means idle subtasks with no data.                    │
│     → Capped at partition count.                                      │
│                                                                       │
│  7. SCALE FACTOR TOO SMALL                                           │
│     Computed new parallelism rounds to same divisor of max-parallel. │
│     E.g., calc says 91 but nearest divisors are 90 and 120.         │
│     If current is already 90, no change occurs.                      │
│     → Rounding absorbs small fluctuations.                            │
└──────────────────────────────────────────────────────────────────────┘
```

---

# Part 10: Configuration Reference

## Essential Settings

| Config Key | Default | Description |
|-----------|---------|-------------|
| `job.autoscaler.enabled` | `false` | Master switch |
| `job.autoscaler.scaling.enabled` | `true` | `false` = metrics-only mode (observe without acting) |
| `job.autoscaler.target.utilization` | `0.7` | Target busy-time ratio (70%) |
| `job.autoscaler.target.utilization.boundary` | `0.3` | Dead-zone width around target |
| `pipeline.max-parallelism` | — | **Critical**: use highly composite number (720 recommended) |

## Timing & Windows

| Config Key | Default | Description |
|-----------|---------|-------------|
| `job.autoscaler.metrics.window` | `15m` | Sliding window for metric averaging |
| `job.autoscaler.stabilization.interval` | `5m` | Cooldown after job restart |
| `job.autoscaler.scale-down.interval` | `1h` | Delay before executing scale-down |
| `job.autoscaler.restart.time` | `5m` | Expected restart duration (for catch-up calc) |
| `job.autoscaler.catch-up.duration` | `30m` | Target time to drain backlog |

## Parallelism Bounds

| Config Key | Default | Description |
|-----------|---------|-------------|
| `job.autoscaler.vertex.min-parallelism` | `1` | Floor for any vertex |
| `job.autoscaler.vertex.max-parallelism` | `200` | Ceiling for any vertex |
| `job.autoscaler.scale-down.max-factor` | `0.6` | Min fraction of current parallelism |
| `job.autoscaler.scale-up.max-factor` | `100000` | Max multiple of current parallelism |

## Advanced

| Config Key | Default | Description |
|-----------|---------|-------------|
| `job.autoscaler.observed-true-processing-rate.switch-threshold` | `0.15` | Switch to observed TPR if busy-time TPR is 15%+ higher |
| `job.autoscaler.scaling.effectiveness.threshold` | `0.1` | Min throughput improvement to consider scaling effective |
| `job.autoscaler.memory.tuning.enabled` | `false` | Enable memory-aware scaling (GC pressure, heap usage) |

Source: `AutoScalerOptions.java`

---

# Part 11: Deployment Modes

## Kubernetes Operator (Primary)

The autoscaler runs **inside the Flink Kubernetes Operator** as a reconciliation loop.
Scaling is applied via job upgrades (stop → restore from savepoint → start with new
parallelism) or, with Flink 1.18+, via in-place rescaling using the adaptive scheduler.

```yaml
# FlinkDeployment CR
spec:
  flinkConfiguration:
    job.autoscaler.enabled: "true"
    job.autoscaler.target.utilization: "0.7"
    job.autoscaler.stabilization.interval: "5m"
    pipeline.max-parallelism: "720"
    # For in-place scaling (Flink 1.18+):
    jobmanager.scheduler: adaptive
```

## Standalone Autoscaler

For non-Kubernetes deployments (YARN, standalone clusters). Runs as a separate JVM
process that monitors Flink via REST API and applies scaling decisions.

```
java -cp flink-autoscaler-standalone-*.jar \
  org.apache.flink.autoscaler.standalone.StandaloneAutoscalerEntrypoint \
  --autoscaler.standalone.fetcher.flink-cluster.host localhost \
  --autoscaler.standalone.fetcher.flink-cluster.port 8081
```

Supports JDBC state stores (MySQL, PostgreSQL, Derby) for persisting scaling history.

---

# Part 12: Key Classes Reference

## Autoscaler Core (`flink-kubernetes-operator/flink-autoscaler`)

| Class | Role |
|-------|------|
| `JobAutoScalerImpl` | Entry point — orchestrates collect → evaluate → scale pipeline |
| `ScalingMetricCollector` | Fetches raw metrics from Flink REST API, maintains metric history window |
| `ScalingMetricEvaluator` | Computes TPR, target rates, scale thresholds from raw metrics |
| `JobVertexScaler` | Core algorithm: computes target parallelism, applies constraints, detects ineffective scaling |
| `ScalingExecutor` | Orchestrates per-vertex scaling, validates memory pressure, triggers rescale |
| `AutoScalerUtils` | Helper: target capacity formula, scaling coefficient calculations |
| `DelayedScaleDown` | Defers scale-down with monotonic deque to batch reductions |
| `ScalingTracking` | Tracks restart times for accurate catch-up calculations |
| `ScalingMetric` | Enum of all metric types (LOAD, TRUE_PROCESSING_RATE, LAG, etc.) |
| `EvaluatedScalingMetric` | Holds current + average values for a metric after evaluation |
| `AutoScalerOptions` | All configuration keys with defaults and descriptions |

## Flink Runtime Metrics (`flink-runtime`)

| Class | Role |
|-------|------|
| `TaskIOMetricGroup` | Defines busyTime, idleTime, backPressuredTime, numRecordsIn/Out gauges |
| `InternalOperatorIOMetricGroup` | Per-operator record counters (aggregated into task-level) |
| `InternalSourceReaderMetricGroup` | pendingRecords, pendingBytes, currentEmitEventTimeLag gauges |
| `TimerGauge` | Rolling-window timer (markStart/markEnd) for busy/idle/backpressure timing |
| `MetricNames` | String constants for all metric names |
| `StreamTask` | Mailbox loop where idle/backpressure timing is triggered |
| `SourceOperator` | Registers backpressure listener for accurate emit-time lag |

