# Flink Memory Configuration, Monitoring, and OOM Debugging

A practical guide covering memory models for both **JobManager** and **TaskManager**,
tuning for large RocksDB state and network buffers, monitoring via metrics, and debugging
out-of-memory issues.

Based on the official docs
([JM memory](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/mem_setup_jobmanager/),
[TM memory](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/mem_setup_tm/),
[network tuning](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/memory/network_mem_tuning/)),
source code, and production experience.

---

# Part I: JobManager Memory

## 1. JobManager Memory Model

The JM model is intentionally simple — it only coordinates jobs, it doesn't process data or
manage state. There's no managed memory, no network buffers, just heap and off-heap.

```
┌──────────────────────────────────────────────────────────────────┐
│                    Total Process Memory                          │
│              jobmanager.memory.process.size (1600m default)      │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                  Total Flink Memory                        │  │
│  │            jobmanager.memory.flink.size                    │  │
│  │                                                            │  │
│  │  ┌─────────────────────────────────────────────────────┐   │  │
│  │  │  JVM Heap (derived or explicit)                     │   │  │
│  │  │  jobmanager.memory.heap.size                        │   │  │
│  │  │                                                     │   │  │
│  │  │  ── ExecutionGraph, scheduling metadata,           │   │  │
│  │  │     checkpoint coordination, REST API,              │   │  │
│  │  │     user code during job submission ──              │   │  │
│  │  └─────────────────────────────────────────────────────┘   │  │
│  │                                                            │  │
│  │  ┌─────────────────────────────────────────────────────┐   │  │
│  │  │  Off-Heap Memory (128MB default)                    │   │  │
│  │  │  jobmanager.memory.off-heap.size                    │   │  │
│  │  │                                                     │   │  │
│  │  │  ── Pekko/Akka network communication,              │   │  │
│  │  │     direct buffers, native allocations ──           │   │  │
│  │  └─────────────────────────────────────────────────────┘   │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌────────────────────── JVM Overhead ───────────────────────┐  │
│  │  JVM Metaspace (256MB default)                            │  │
│  │  jobmanager.memory.jvm-metaspace.size                     │  │
│  │                                                            │  │
│  │  JVM Overhead (10% of total process, default)             │  │
│  │  jobmanager.memory.jvm-overhead.fraction = 0.1            │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

### Configuration Reference

| Component | Config Option | Default | Notes |
|-----------|--------------|---------|-------|
| Total Process Memory | `jobmanager.memory.process.size` | 1600m | Set this OR Flink memory, not both |
| Total Flink Memory | `jobmanager.memory.flink.size` | — | Heap + off-heap |
| JVM Heap | `jobmanager.memory.heap.size` | (derived) | Do NOT set alongside total memory |
| Off-Heap | `jobmanager.memory.off-heap.size` | 128MB | Direct + native memory |
| Enable Direct Limit | `jobmanager.memory.enable-jvm-direct-memory-limit` | false | Sets `-XX:MaxDirectMemorySize` |
| JVM Metaspace | `jobmanager.memory.jvm-metaspace.size` | 256MB | |
| JVM Overhead Fraction | `jobmanager.memory.jvm-overhead.fraction` | 0.1 | Bounded by min/max |
| JVM Overhead Min | `jobmanager.memory.jvm-overhead.min` | 192MB | |
| JVM Overhead Max | `jobmanager.memory.jvm-overhead.max` | 1GB | |

Source: `JobManagerOptions.java`

### JM vs TM: Why the Difference

| Aspect | JobManager | TaskManager |
|--------|-----------|-------------|
| Heap divisions | Single JVM Heap | Framework Heap + Task Heap |
| Managed Memory | None | Yes (RocksDB, sorting, Python) |
| Network Memory | None | Yes (shuffle buffers) |
| Off-Heap | Single pool | Framework Off-Heap + Task Off-Heap |
| Complexity | Simple (2 Flink components) | Complex (6 Flink components) |

The TM needs managed memory and network buffers because it processes data. The JM only
coordinates — its model is intentionally minimal.

---

## 2. What Drives JM Heap Usage

The JM heap is consumed by:

- **ExecutionGraph** — the runtime representation of your job DAG. More operators and
  parallelism = larger graph. A job with hundreds of operators can consume 100s of MB.
- **Checkpoint coordination** — the JM tracks pending checkpoints and their metadata.
  Many concurrent checkpoints or large checkpoint metadata increases heap pressure.
- **REST API** — serves job details, metrics, plan visualizations. Heavy polling
  (e.g., Grafana dashboards) adds load.
- **User code** — `main()` logic, batch source initialization, checkpoint completion
  callbacks run on the JM.
- **HA recovery** — on failover, the new leader JM loads ALL job graphs and checkpoint
  metadata simultaneously. This is the peak memory moment.

### When to Increase JM Memory

```
Default (1600m) is fine for:
  └── Single job, moderate DAG, per-job mode

Increase to 2-4GB for:
  ├── Session cluster with multiple concurrent jobs
  ├── Large DAGs (hundreds of operators)
  ├── HA mode (recovery loads all jobs at once)
  └── Heavy REST API usage (monitoring dashboards)

Increase to 4-8GB for:
  ├── Session cluster with many large jobs
  ├── Flink CDC with many tables (metaspace pressure too)
  └── Large-scale batch jobs with complex scheduling
```

---

## 3. JM OOM Patterns and Fixes

| Exception / Symptom | Cause | Fix |
|---------------------|-------|-----|
| `OutOfMemoryError: Java heap space` | Large job graphs, many concurrent jobs, HA recovery spike | Increase `jobmanager.memory.process.size` or `heap.size` |
| `OutOfMemoryError: Direct buffer memory` | Pekko/Akka network communication, many TM connections | Increase `jobmanager.memory.off-heap.size`; enable `enable-jvm-direct-memory-limit` |
| `OutOfMemoryError: Metaspace` | Many user code classloaders (session mode), Flink CDC with many tables | Increase `jobmanager.memory.jvm-metaspace.size` (e.g., 512m-1g) |
| Container killed (exit code 137) | Total process exceeds container limit, glibc memory bloat | Increase `process.size`; set `MALLOC_ARENA_MAX=1` |

### Example Configurations

**Small (single job, per-job mode):**
```yaml
jobmanager.memory.process.size: 1600m
```

**Medium (session cluster, several jobs):**
```yaml
jobmanager.memory.process.size: 2048m
jobmanager.memory.jvm-metaspace.size: 384m
```

**Large (many jobs, HA, large DAGs):**
```yaml
jobmanager.memory.process.size: 4096m
jobmanager.memory.off-heap.size: 256m
jobmanager.memory.jvm-metaspace.size: 512m
```

---

# Part II: TaskManager Memory

## 4. Total Process Memory Model

Every TaskManager JVM process has a fixed memory budget. Flink divides it into regions,
each serving a different purpose. Understanding this layout is essential before tuning anything.

```
┌──────────────────────────────────────────────────────────────────┐
│                    Total Process Memory                          │
│                taskmanager.memory.process.size                   │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                  Total Flink Memory                        │  │
│  │            taskmanager.memory.flink.size                   │  │
│  │                                                            │  │
│  │  ┌─────────────────── JVM Heap ──────────────────────┐    │  │
│  │  │                                                    │    │  │
│  │  │  Framework Heap (128MB default)                    │    │  │
│  │  │  taskmanager.memory.framework.heap.size            │    │  │
│  │  │  ── Flink runtime internals ──                     │    │  │
│  │  │                                                    │    │  │
│  │  │  Task Heap (remainder after other components)      │    │  │
│  │  │  taskmanager.memory.task.heap.size                 │    │  │
│  │  │  ── User code: operators, UDFs, state (HashMap) ── │    │  │
│  │  │                                                    │    │  │
│  │  └────────────────────────────────────────────────────┘    │  │
│  │                                                            │  │
│  │  ┌─────────────────── Off-Heap ──────────────────────┐    │  │
│  │  │                                                    │    │  │
│  │  │  Managed Memory (40% of Flink memory, default)     │    │  │
│  │  │  taskmanager.memory.managed.fraction = 0.4         │    │  │
│  │  │  ── RocksDB, sorting, batch caching, Python ──     │    │  │
│  │  │                                                    │    │  │
│  │  │  Network Memory (10% of Flink memory, default)     │    │  │
│  │  │  taskmanager.memory.network.fraction = 0.1         │    │  │
│  │  │  ── Shuffle buffers between tasks ──               │    │  │
│  │  │                                                    │    │  │
│  │  │  Framework Off-heap (128MB default)                │    │  │
│  │  │  taskmanager.memory.framework.off-heap.size        │    │  │
│  │  │                                                    │    │  │
│  │  │  Task Off-heap (0 default)                         │    │  │
│  │  │  taskmanager.memory.task.off-heap.size             │    │  │
│  │  │  ── User code direct/native allocations ──         │    │  │
│  │  │                                                    │    │  │
│  │  └────────────────────────────────────────────────────┘    │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌────────────────────── JVM Overhead ───────────────────────┐  │
│  │  JVM Metaspace (256MB default)                            │  │
│  │  taskmanager.memory.jvm-metaspace.size                    │  │
│  │  ── Class metadata, method bytecode ──                    │  │
│  │                                                            │  │
│  │  JVM Overhead (10% of total process, default)             │  │
│  │  taskmanager.memory.jvm-overhead.fraction = 0.1           │  │
│  │  ── Thread stacks, code cache, GC, JNI ──                │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

### Configuration Reference

| Component | Config Option | Default | Notes |
|-----------|--------------|---------|-------|
| Total Process Memory | `taskmanager.memory.process.size` | — | Set this OR Flink memory, not both |
| Total Flink Memory | `taskmanager.memory.flink.size` | — | Excludes JVM overhead |
| Framework Heap | `taskmanager.memory.framework.heap.size` | 128MB | Rarely needs tuning |
| Task Heap | `taskmanager.memory.task.heap.size` | (derived) | User code + HashMap state backend |
| Managed Memory | `taskmanager.memory.managed.size` | — | Explicit size, overrides fraction |
| Managed Fraction | `taskmanager.memory.managed.fraction` | 0.4 | Of total Flink memory |
| Network Memory | `taskmanager.memory.network.fraction` | 0.1 | Bounded by min/max |
| Network Min | `taskmanager.memory.network.min` | 64MB | |
| Network Max | `taskmanager.memory.network.max` | 1GB | |
| Framework Off-heap | `taskmanager.memory.framework.off-heap.size` | 128MB | |
| Task Off-heap | `taskmanager.memory.task.off-heap.size` | 0 | For user native allocations |
| JVM Metaspace | `taskmanager.memory.jvm-metaspace.size` | 256MB | |
| JVM Overhead Fraction | `taskmanager.memory.jvm-overhead.fraction` | 0.1 | Bounded by min/max |
| JVM Overhead Min | `taskmanager.memory.jvm-overhead.min` | 192MB | |
| JVM Overhead Max | `taskmanager.memory.jvm-overhead.max` | 1GB | |

### Two Configuration Approaches

**Approach 1: Set total memory (recommended for most deployments)**

Set `taskmanager.memory.process.size` (for standalone/containers) or
`taskmanager.memory.flink.size` (for YARN/K8s where the container size is set externally).
Flink derives everything else from defaults and fractions.

**Approach 2: Set individual components**

Set `taskmanager.memory.task.heap.size` and `taskmanager.memory.managed.size` explicitly.
Use when you need precise control. Do NOT combine with `process.size`/`flink.size` — Flink
will reject conflicting settings.

---

## 5. Managed Memory Deep Dive: RocksDB

Managed memory is native (off-heap) memory managed by Flink. For streaming jobs with
`EmbeddedRocksDBStateBackend`, it is the **primary knob** controlling RocksDB memory usage.

### How Managed Memory Flows to RocksDB

```
taskmanager.memory.managed.fraction = 0.4 (of total Flink memory)
                    │
                    ▼
    ┌───── Consumer Weights ──────┐
    │  OPERATOR : STATE_BACKEND : │   taskmanager.memory.managed.consumer-weights
    │  PYTHON                     │   default: OPERATOR:70, STATE_BACKEND:70, PYTHON:30
    └─────────────┬───────────────┘
                  │
                  ▼ (STATE_BACKEND's share)
    ┌──────────────────────────────────────────────────────────┐
    │              RocksDB Managed Memory (per slot)           │
    │         state.backend.rocksdb.memory.managed: true       │
    │                                                          │
    │  Flink creates a SHARED LRU Cache + WriteBufferManager   │
    │  that ALL RocksDB column families in this slot use.      │
    │                                                          │
    │  ┌──────────────────────────────────────────────────┐    │
    │  │         Write Buffer Manager (50%)               │    │
    │  │    write-buffer-ratio = 0.5 (default)            │    │
    │  │                                                  │    │
    │  │    Global cap on MemTable memory across all CFs. │    │
    │  │    When exceeded → oldest MemTable flushed.      │    │
    │  └──────────────────────────────────────────────────┘    │
    │                                                          │
    │  ┌──────────────────────────────────────────────────┐    │
    │  │         Block Cache (remaining ~40%)             │    │
    │  │                                                  │    │
    │  │    LRU cache for SST data blocks.               │    │
    │  │    Shared across all column families.            │    │
    │  └──────────────────────────────────────────────────┘    │
    │                                                          │
    │  ┌──────────────────────────────────────────────────┐    │
    │  │    High-Priority Pool (10%)                      │    │
    │  │    high-prio-pool-ratio = 0.1 (default)          │    │
    │  │                                                  │    │
    │  │    Reserved for index + filter blocks.           │    │
    │  │    NOT evicted by data block pressure.           │    │
    │  └──────────────────────────────────────────────────┘    │
    └──────────────────────────────────────────────────────────┘
```

### The Actual Formulas

From `RocksDBMemoryControllerUtils.java:75-98`:

The WriteBufferManager can temporarily use up to 1.5x its configured capacity before triggering
a flush. Flink accounts for this overuse by inflating the cache capacity:

```
cache_capacity              = (3 - writeBufferRatio) * totalMemorySize / 3
writeBufferManagerCapacity  = 2 * totalMemorySize * writeBufferRatio / 3
```

**Example:** 1GB managed memory, writeBufferRatio=0.5

```
cache_capacity             = (3 - 0.5) * 1GB / 3 = 833MB
writeBufferManagerCapacity = 2 * 1GB * 0.5 / 3   = 333MB  (can spike to ~500MB)
high-prio reserved         = 833MB * 0.1          = 83MB   (for indexes/filters)
```

The validation in `RocksDBMemoryConfiguration.java:187-203` ensures:

```
2 * writeBufferRatio / (3 - writeBufferRatio) + highPriorityPoolRatio < 1.0
```

This guarantees the write buffer manager capacity plus the high-priority pool don't exceed
the cache capacity.

### RocksDB Memory Config Options

| Config Option | Default | Description |
|---------------|---------|-------------|
| `state.backend.rocksdb.memory.managed` | `true` | Use Flink's managed memory budget |
| `state.backend.rocksdb.memory.fixed-per-slot` | — | Fixed memory per slot (overrides managed) |
| `state.backend.rocksdb.memory.fixed-per-tm` | — | Fixed memory per TM (breaks slot isolation) |
| `state.backend.rocksdb.memory.write-buffer-ratio` | `0.5` | Fraction for write buffers |
| `state.backend.rocksdb.memory.high-prio-pool-ratio` | `0.1` | Fraction for index/filter blocks |
| `state.backend.rocksdb.memory.partitioned-index-filters` | `false` | Load index/filter blocks on demand |

Source: `RocksDBOptions.java:122-191`

---

## 6. Network Memory Deep Dive

Network memory holds **shuffle buffers** — the in-flight data moving between tasks via Flink's
network stack. It directly affects throughput, checkpoint speed, and backpressure behavior.

### How Network Buffers Work

```
┌────── Upstream Subtask ──────┐          ┌────── Downstream Subtask ──────┐
│                              │          │                                │
│  ResultPartition             │          │  SingleInputGate               │
│  ┌────────────────────────┐  │          │  ┌──────────────────────────┐  │
│  │ Subpartition 0 [buf]   │──┼── TCP ──┼─►│ RemoteInputChannel 0     │  │
│  │ Subpartition 1 [buf]   │──┼── TCP ──┼─►│ RemoteInputChannel 1     │  │
│  │ Subpartition 2 [buf]   │──┼── TCP ──┼─►│ RemoteInputChannel 2     │  │
│  └────────────────────────┘  │          │  └──────────────────────────┘  │
│                              │          │                                │
│  Output buffers:             │          │  Input buffers:                │
│  max-buffers-per-channel=10  │          │  ┌ Exclusive: 2 per channel   │
│  max-overdraft-buffers=20    │          │  │  (always reserved)         │
│                              │          │  ├ Floating: 8 per gate       │
│                              │          │  │  (shared across channels)  │
│                              │          │  └ Total target per gate:     │
│                              │          │    #channels × 2 + 8         │
└──────────────────────────────┘          └────────────────────────────────┘

        Credit-Based Flow Control
        ══════════════════════════
        1. Receiver announces available buffers as CREDITS to sender
        2. Sender only transmits if credit > 0 (1 buffer = 1 credit)
        3. Along with data, sender reports BACKLOG SIZE
        4. Receiver uses backlog to request floating buffers from local pool
```

### Buffer Pool Size Calculation

From `NettyShuffleEnvironmentConfiguration.java`:

```
Target buffer pool size = #channels × buffers-per-channel + floating-buffers-per-gate

Total network buffers   = network_memory_size / segment_size
                        = network_memory_size / 32KB (default)
```

**Example:** 3 upstream subtasks, default settings:
```
Per gate: 3 × 2 (exclusive) + 8 (floating) = 14 buffers = 448 KB
```

With multiple input gates (e.g., a join with 2 inputs at parallelism 100):
```
Gate 1: 100 × 2 + 8 = 208 buffers
Gate 2: 100 × 2 + 8 = 208 buffers
Total:  416 buffers = 13 MB per subtask
```

### Network Memory Configuration

| Config Option | Default | Description |
|---------------|---------|-------------|
| `taskmanager.memory.network.fraction` | 0.1 | Fraction of Flink memory for network buffers |
| `taskmanager.memory.network.min` | 64MB | Lower bound |
| `taskmanager.memory.network.max` | 1GB | Upper bound |
| `taskmanager.memory.segment-size` | 32KB | Size of each buffer segment |
| `taskmanager.network.memory.buffers-per-channel` | 2 | Exclusive buffers per input channel |
| `taskmanager.network.memory.floating-buffers-per-gate` | 8 | Floating buffers per input gate |
| `taskmanager.network.memory.max-buffers-per-channel` | 10 | Output buffer cap per subpartition |
| `taskmanager.network.memory.max-overdraft-buffers-per-gate` | 20 | Extra buffers when backpressured |
| `taskmanager.network.memory.read-buffer.required-per-gate.max` | MAX_INT (streaming) / 1000 (batch) | Threshold for required vs optional buffers |
| `taskmanager.network.sort-shuffle.min-buffers` | 512 | Write buffers for sort-shuffle (batch) |

Source: `TaskManagerOptions.java`, `NettyShuffleEnvironmentOptions.java`,
`NettyShuffleEnvironmentConfiguration.java`

### Buffer Debloating

Flink 1.14+ can **automatically adjust buffer sizes** based on measured throughput, balancing
throughput against checkpoint speed. Without debloating, static buffer configuration leads to
either too much in-flight data (slow checkpoints) or too little (reduced throughput).

```
                    Without debloating              With debloating
                    ══════════════════              ═══════════════

In-flight data:     Fixed, often too large          Auto-adjusted to target
                    ┌────────────────┐              ┌────────────────┐
                    │████████████████│ 32KB/buf     │████████░░░░░░░░│ ~4KB/buf
                    │████████████████│ × many bufs  │████████░░░░░░░░│ (capped)
                    └────────────────┘              └────────────────┘

Checkpoint barrier  Must wait for all               Less data to drain
propagation:        in-flight data to drain          → faster checkpoints
```

**Configuration:**

```yaml
taskmanager.network.memory.buffer-debloat.enabled: true   # default: false
taskmanager.network.memory.buffer-debloat.target: 1s      # target consumption time
taskmanager.network.memory.buffer-debloat.period: 200ms   # recalculation interval
taskmanager.network.memory.buffer-debloat.samples: 20     # throughput averaging window
taskmanager.network.memory.buffer-debloat.threshold-percentages: 25  # change threshold
```

**When to use:** Enable when checkpoint durations are high due to in-flight data.
Most visible with aligned checkpoints. Does NOT reduce actual memory allocation — it
only caps used buffer size within each buffer.

**Limitation at high parallelism:** Above ~200 parallelism, debloating may underperform.
Increase `floating-buffers-per-gate` to match parallelism in those cases.

### Sort-Shuffle vs Hash-Shuffle (Batch Jobs)

For **batch** jobs, Flink offers two shuffle strategies with different memory characteristics:

| Aspect | Hash-Shuffle | Sort-Shuffle |
|--------|-------------|-------------|
| Memory scaling | O(downstream parallelism) per result partition | Fixed, independent of parallelism |
| File I/O | One file per subpartition (random I/O) | Single file per partition (sequential I/O) |
| At parallelism 10,000 | ~320MB per result partition minimum | Fixed buffer budget |
| Default since | Legacy | Flink 1.12+ (for batch) |

**Recommendation:** Sort-shuffle is strongly preferred for batch jobs at scale.

---

## 7. Tuning for Large RocksDB State (Managed Memory)

### Decision Tree

```
Large state (tens of GB+)?
│
├── Step 1: Increase managed memory
│   taskmanager.memory.managed.fraction: 0.5 → 0.7
│   (This is the SINGLE MOST IMPACTFUL change)
│
├── Step 2: Enable incremental checkpoints
│   state.backend.incremental: true
│   (See flink-rocksdb-state-backend-tuning.md for details)
│
├── Step 3: Tune write-buffer-ratio based on workload
│   │
│   ├── Write-heavy (high ingestion, many updates)?
│   │   Increase to 0.6-0.65 (more MemTable room → fewer flushes)
│   │
│   └── Read-heavy (lots of state lookups)?
│       Keep at 0.5 or lower (more block cache → fewer disk reads)
│
├── Step 4: Use local SSDs
│   state.backend.rocksdb.localdir: /local-ssd/flink/rocksdb
│   (NFS/HDFS → severe I/O bottleneck that looks like "slow processing")
│   (Multiple local dirs → RocksDB spreads I/O across disks)
│
├── Step 5: Enable partitioned index filters (very large state)
│   state.backend.rocksdb.memory.partitioned-index-filters: true
│   (Loads only the needed partition of index/filter blocks instead
│    of the entire block. Saves memory when SST files are large.)
│
└── Step 6: Account for column family count
    Each keyed state = 1 Column Family = its own MemTable(s).
    App with 4 operators × 3 states = 12 CFs per slot.
    With slot sharing, this multiplies further.
    More CFs = more MemTables = need more managed memory.
```

---

## 8. Tuning Network Memory

### Decision Tree

```
Network-related issue?
│
├── "Insufficient number of network buffers"
│   │
│   ├── Calculate required buffers:
│   │   Per gate: #upstream_subtasks × buffers-per-channel + floating-buffers-per-gate
│   │   Sum across ALL gates in the slot
│   │
│   ├── Compare against available:
│   │   network_memory / segment_size (32KB)
│   │
│   └── Fix:
│       ├── Increase taskmanager.memory.network.fraction (or network.min)
│       ├── Reduce buffers-per-channel (down to 0 for all-floating mode)
│       └── For batch: use sort-shuffle to decouple from parallelism
│
├── Slow checkpoints (high in-flight data)?
│   │
│   ├── First: enable buffer debloating
│   │   taskmanager.network.memory.buffer-debloat.enabled: true
│   │
│   ├── If debloating not enough: reduce segment-size (e.g., 32KB → 8KB)
│   │   and/or reduce buffers-per-channel
│   │
│   └── Consider unaligned checkpoints for backpressured jobs
│       execution.checkpointing.unaligned.enabled: true
│
├── High parallelism (>200)?
│   │
│   ├── Increase floating-buffers-per-gate to match downstream parallelism
│   │
│   └── Increase network memory fraction accordingly
│
└── Backpressure from network buffers?
    │
    ├── Check outPoolUsage / inPoolUsage metrics (Section 9b)
    │
    ├── If outPoolUsage ~100%: downstream is slow, not a buffer config issue
    │
    └── If inPoolUsage ~100% with low floatingBuffersUsage:
        increase floating-buffers-per-gate
```

### Required Buffer Estimation Formula

```
buffers_per_gate = upstream_parallelism × buffers_per_channel + floating_buffers_per_gate

total_buffers_per_slot = sum(buffers_per_gate for each input gate in slot)
                       + output_buffers (roughly downstream_parallelism per output)

required_network_memory = total_buffers_per_slot × segment_size
```

**Quick estimate for a cluster:**
```
required_buffers ≈ 4 × num_task_managers × slots_per_tm²
```

At 32KB per buffer: a cluster of 20 TMs with 8 slots each needs ~160MB network memory per TM.

---

# Part III: Monitoring and Debugging

## 9. Monitoring Memory Usage

### 9a. Flink Web UI

**JobManager:** The JM overview page shows heap and non-heap usage. For detailed metrics,
use the REST API: `/jobmanager/metrics`. Key metrics to check:
`Status.JVM.Memory.Heap.Used`, `Status.JVM.Memory.Heap.Max`,
`Status.JVM.Memory.Metaspace.Used`.

**TaskManager:** The TM tab shows a per-TM memory breakdown:

- JVM Heap: used / committed / max
- Non-Heap: used / committed / max
- Direct buffers: count / memory used / total capacity
- Mapped buffers: count / memory used / total capacity
- Network shuffle: segments available / used / total
- Managed memory is visible via the metrics subtab

The REST API provides the same data programmatically:
`/taskmanagers/<tm-id>/metrics` — returns all registered metrics.
See `TaskManagerMetricsInfo.java` for the full list of fields.

### 9b. Key Metrics to Watch

These metrics are registered in `MetricUtils.java` (`flink-runtime`) and are always available.

| Category | Metric | What to Watch For |
|----------|--------|-------------------|
| **Heap** | `Status.JVM.Memory.Heap.Used` | Trending toward `Max` = heap OOM imminent |
| **Heap** | `Status.JVM.Memory.Heap.Max` | Upper bound; compare with `Used` |
| **Non-Heap** | `Status.JVM.Memory.NonHeap.Used` | Includes code cache, compressed class space |
| **Metaspace** | `Status.JVM.Memory.Metaspace.Used` | Steady climb without plateau = classloader leak |
| **Direct** | `Status.JVM.Memory.Direct.MemoryUsed` | Network buffers + framework direct memory |
| **Direct** | `Status.JVM.Memory.Direct.TotalCapacity` | Should stay within configured limits |
| **Managed** | `Status.Flink.Memory.Managed.Used` | RocksDB/batch algorithm budget utilization |
| **Managed** | `Status.Flink.Memory.Managed.Total` | Configured budget |
| **GC** | `Status.JVM.GarbageCollector.<name>.Count` | Sudden spikes = memory pressure |
| **GC** | `Status.JVM.GarbageCollector.<name>.TimeMsPerSecond` | >200ms/sec = severe GC pressure |
| **Network** | `Shuffle.Netty.Input.Buffers.inPoolUsage` | >0.9 = backpressure from buffer exhaustion |
| **Network** | `Shuffle.Netty.Output.Buffers.outPoolUsage` | >0.9 = downstream can't consume fast enough |
| **Network** | `Shuffle.Netty.Input.Buffers.floatingBuffersUsage` | Floating buffer utilization across channels |
| **Network** | `Shuffle.Netty.Input.Buffers.exclusiveBuffersUsage` | Per-channel exclusive buffer utilization |
| **Backpressure** | `backPressuredTimeMsPerSecond` | Time spent waiting for credits (per subtask) |
| **Backpressure** | `busyTimeMsPerSecond` | Time doing actual work |
| **Backpressure** | `idleTimeMsPerSecond` | Time waiting for input |

Enable `taskmanager.network.detailed-metrics: true` for per-subpartition metrics
(disabled by default due to cardinality at high parallelism).

### 9c. RocksDB-Specific Metrics

**These are ALL disabled by default** — you must explicitly enable them in config. They are
defined in `RocksDBNativeMetricOptions.java`. Enabling too many has a performance cost
(each metric calls into RocksDB native code via JNI), so enable selectively.

#### Must-Enable for Production

```yaml
# Memory monitoring
state.backend.rocksdb.metrics.block-cache-usage: true      # cache memory used
state.backend.rocksdb.metrics.block-cache-capacity: true    # cache capacity
state.backend.rocksdb.metrics.cur-size-all-mem-tables: true # total MemTable memory

# Write health
state.backend.rocksdb.metrics.actual-delayed-write-rate: true  # non-zero = throttled!
state.backend.rocksdb.metrics.is-write-stopped: true           # 1 = writes STOPPED
state.backend.rocksdb.metrics.mem-table-flush-pending: true    # pending flushes

# Compaction health
state.backend.rocksdb.metrics.num-running-compactions: true
state.backend.rocksdb.metrics.estimate-pending-compaction-bytes: true
```

#### Enable for Deeper Diagnosis

```yaml
# Cache effectiveness (statistics-based, database level)
state.backend.rocksdb.metrics.block-cache-hit: true
state.backend.rocksdb.metrics.block-cache-miss: true
# hit_ratio = hit / (hit + miss) — below 0.8 means cache is too small

# Write amplification
state.backend.rocksdb.metrics.compaction-read-bytes: true
state.backend.rocksdb.metrics.compaction-write-bytes: true
state.backend.rocksdb.metrics.bytes-written: true

# Stall monitoring
state.backend.rocksdb.metrics.stall-micros: true  # cumulative write stall time

# Memory breakdown
state.backend.rocksdb.metrics.estimate-table-readers-mem: true  # SST reader memory (off-budget!)
state.backend.rocksdb.metrics.block-cache-pinned-usage: true    # pinned blocks (can't be evicted)

# State size
state.backend.rocksdb.metrics.estimate-live-data-size: true
state.backend.rocksdb.metrics.live-sst-files-size: true
```

#### Interpreting Key Metrics

| Symptom | Metrics to Check | Likely Problem |
|---------|------------------|----------------|
| High read latency | `block-cache-hit` / `block-cache-miss` ratio < 0.8 | Block cache too small; increase managed memory or decrease `write-buffer-ratio` |
| Write stalls | `actual-delayed-write-rate` > 0 or `is-write-stopped` = 1 | Compaction can't keep up; increase managed memory, check disk I/O |
| Frequent flushes | `mem-table-flush-pending` consistently > 0 | Write buffer too small; increase `write-buffer-ratio` |
| Growing state | `estimate-live-data-size` climbing without bound | Missing state TTL or no key expiration |
| High compaction load | `num-running-compactions` at max, `estimate-pending-compaction-bytes` growing | Disk too slow, or state churn too high |

### 9d. Network Buffer Metrics and Backpressure Diagnosis

#### Finding the Backpressure Bottleneck

```
Operator chain:  Source → Map → KeyBy → Aggregate → Sink

Step 1: Find where backPressuredTimeMsPerSecond transitions HIGH → LOW
        going downstream. The bottleneck is AT or JUST DOWNSTREAM of
        that transition.

                Source    Map    Aggregate    Sink
backpressured:  HIGH     HIGH     LOW         LOW
                                  ▲
                                  └── Bottleneck is here

Step 2: Check the bottleneck operator's metrics:

  inPoolUsage ~100%
  └── This operator's INPUT is full. It's the slow consumer.
      Fix: optimize this operator or scale its parallelism.

  outPoolUsage ~100% but inPoolUsage low
  └── NETWORK bottleneck between this operator and downstream.
      Fix: check network bandwidth, increase buffers.

  floatingBuffersUsage ~100% with exclusiveBuffersUsage high
  └── All channels are backpressured equally (no data skew).
      Fix: scale downstream parallelism.

  floatingBuffersUsage ~100% but only some channels high
  └── Data skew — some channels receive more data.
      Fix: rebalance partitioning, or use reinterpretAsKeyedStream.
```

#### Interpreting Network Metrics

| Symptom | Metrics | Problem | Fix |
|---------|---------|---------|-----|
| Upstream operators stuck | `outPoolUsage` ~100% upstream, `inPoolUsage` ~100% downstream | Downstream operator too slow | Optimize or scale downstream operator |
| Checkpoint barrier stuck | High `backPressuredTimeMsPerSecond` across pipeline | Too much in-flight data | Enable buffer debloating or unaligned checkpoints |
| Buffer pool exhaustion | `Insufficient number of network buffers` exception | Not enough network memory for job's connectivity | Increase `network.fraction` or reduce `buffers-per-channel` |
| Uneven throughput | Some `exclusiveBuffersUsage` high, others low | Data skew across channels | Fix partitioning or use `rebalance()` |

### 9e. Prometheus/Grafana Setup

Configure the metric reporter in `config.yaml`:

```yaml
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249
```

Example PromQL queries:

```promql
# Heap usage percentage
flink_taskmanager_Status_JVM_Memory_Heap_Used
  / flink_taskmanager_Status_JVM_Memory_Heap_Max * 100

# GC time per second (high = trouble)
rate(flink_taskmanager_Status_JVM_GarbageCollector_All_Time[5m]) * 1000

# Managed memory usage
flink_taskmanager_Status_Flink_Memory_Managed_Used
  / flink_taskmanager_Status_Flink_Memory_Managed_Total * 100

# Direct memory usage
flink_taskmanager_Status_JVM_Memory_Direct_MemoryUsed

# RocksDB cache hit ratio (if enabled)
rate(flink_taskmanager_rocksdb_block_cache_hit[5m])
  / (rate(flink_taskmanager_rocksdb_block_cache_hit[5m])
     + rate(flink_taskmanager_rocksdb_block_cache_miss[5m]))

# Network buffer pool usage (input side, >0.9 = backpressure)
flink_taskmanager_job_task_Shuffle_Netty_Input_Buffers_inPoolUsage

# Backpressure time ratio (high = operator is blocked)
flink_taskmanager_job_task_backPressuredTimeMsPerSecond / 1000

# Busy time ratio (high = operator is doing work, not blocked)
flink_taskmanager_job_task_busyTimeMsPerSecond / 1000
```

Pre-built Grafana dashboards: [ID 14840](https://grafana.com/grafana/dashboards/14840-flink-metrics/),
[ID 11049](https://grafana.com/grafana/dashboards/11049-flink-dashboard/).

---

## 10. Debugging OOM Issues

### 10a. Identify the OOM Type

**JobManager OOM:**

```
JM error?
│
├── "OutOfMemoryError: Java heap space"
│   Cause:  Large job graphs, many concurrent jobs (session cluster),
│           HA recovery loading all jobs at once
│   Fix:    Increase jobmanager.memory.process.size or heap.size
│
├── "OutOfMemoryError: Direct buffer memory"
│   Cause:  Pekko/Akka communication with many TMs
│   Fix:    Increase jobmanager.memory.off-heap.size
│           Enable jobmanager.memory.enable-jvm-direct-memory-limit
│
├── "OutOfMemoryError: Metaspace"
│   Cause:  Many user code classloaders (session mode), Flink CDC with many tables
│   Fix:    Increase jobmanager.memory.jvm-metaspace.size (512m-1g)
│
└── Container killed (exit code 137)
    Cause:  Total memory exceeds container, glibc memory bloat
    Fix:    Increase process.size; set MALLOC_ARENA_MAX=1
```

**TaskManager OOM:**

```
What error/symptom do you see?
│
├── "java.lang.OutOfMemoryError: Java heap space"
│   Region: Task Heap
│   Cause:  Large user objects, unbounded state with HashMapStateBackend
│   Fix:    Increase taskmanager.memory.task.heap.size
│           or switch to RocksDB state backend for large state
│
├── "java.lang.OutOfMemoryError: Direct buffer memory"
│   Region: Direct / Network
│   Cause:  Network buffers exceeding limit, or framework direct allocs
│   Fix:    Increase taskmanager.memory.network.max
│           or taskmanager.memory.framework.off-heap.size
│
├── "java.lang.OutOfMemoryError: Metaspace"
│   Region: JVM Metaspace
│   Cause:  Classloader leak, too many dynamically loaded classes
│   Fix:    Increase taskmanager.memory.jvm-metaspace.size (default 256MB)
│           Note: usually indicates a code bug, not just a config issue
│
├── "Insufficient number of network buffers"
│   Region: Network Memory
│   Cause:  Sum of (upstream_parallelism × buffers-per-channel +
│           floating-buffers-per-gate) across all gates exceeds
│           available network buffers (network_memory / segment_size)
│   Fix:    Increase taskmanager.memory.network.fraction or network.min
│           OR reduce buffers-per-channel (even to 0 for all-floating mode)
│           OR for batch: switch to sort-shuffle
│
└── Container killed (exit code 137, SIGKILL, OOMKilled)
    Region: Native / total process
    Cause:  Total JVM process exceeds container limit
    │
    ├── RocksDB managed memory disabled?
    │   → Enable state.backend.rocksdb.memory.managed: true
    │
    ├── Many CFs with slot sharing?
    │   → Increase taskmanager.memory.managed.fraction
    │
    ├── JVM overhead too small?
    │   → Increase taskmanager.memory.jvm-overhead.fraction (default 0.1)
    │     or set jvm-overhead.min higher
    │
    └── Native memory leak (e.g., jemalloc during savepoints)?
        → See Known Issues below
```

### 10b. Diagnostic Steps

**Step 1: Check the Flink Web UI**

Go to TaskManager tab → select the failed TM → check the memory breakdown.
Compare `Used` vs `Max` for each region. The exhausted region tells you which
config to adjust.

**Step 2: Check the logs**

Grep for these patterns in TaskManager logs:

```bash
grep -E "OutOfMemoryError|exit code 137|Insufficient number of network buffers" taskmanager.log
grep "Full GC" gc.log      # if GC logging is enabled
```

**Step 3: Check metrics history**

If you have Prometheus/Grafana, check the time-series leading up to the crash:
- Was heap climbing steadily? → Likely unbounded state or memory leak
- Did GC time spike? → Heap pressure before OOM
- Was managed memory at 100%? → RocksDB needs more room
- Did direct memory spike? → Network buffer issue

**Step 4: Enable diagnostic tools**

```yaml
# Periodic memory logging to TaskManager log
# Uses MemoryLogger (flink-runtime) to print heap, non-heap, direct, GC stats
taskmanager.debug.memory.log: true
taskmanager.debug.memory.log-interval: 10000  # every 10 seconds
```

JVM flags for deeper diagnosis:

```yaml
env.java.opts.taskmanager: >-
  -XX:+HeapDumpOnOutOfMemoryError
  -XX:HeapDumpPath=/tmp/flink-heapdump
  -XX:NativeMemoryTracking=detail
```

Then on a running TM:
```bash
# JVM native memory breakdown (does NOT include RocksDB/JNI allocations)
jcmd <pid> VM.native_memory summary

# For RocksDB native memory leaks, use jemalloc profiling:
# Set env var before starting:
#   MALLOC_CONF=prof:true,prof_prefix:/tmp/jeprof
# Then analyze with jeprof
```

Flink 1.19+ includes integrated async-profiler support — you can trigger CPU/memory
profiling directly from the Web UI or REST API.

### 10c. Known Issues

**jemalloc OOM during savepoints (FLINK-38212)**

Observed in Flink 1.20.2 and 2.1.0. During savepoint creation, a memory leak in
jemalloc (the default allocator since Flink 1.12) can cause OOM. Workaround:

```bash
export MALLOC_ARENA_MAX=1
```

Set this in the TaskManager environment before starting the cluster.

---

## 11. Example Production Configuration

### JobManager

```yaml
# ─── JM Memory ───
# Session cluster with multiple jobs and HA
jobmanager.memory.process.size: 4096m
jobmanager.memory.off-heap.size: 256m
jobmanager.memory.jvm-metaspace.size: 512m

# ─── Diagnostics ───
env.java.opts.jobmanager: >-
  -XX:+HeapDumpOnOutOfMemoryError
  -XX:HeapDumpPath=/tmp/flink-jm-heapdump
```

### TaskManager

A complete configuration for a large-state streaming job with RocksDB:

```yaml
# ─── Memory Layout ───
taskmanager.memory.process.size: 8g

# Increase managed memory for large RocksDB state (default 0.4)
taskmanager.memory.managed.fraction: 0.6

# ─── State Backend ───
state.backend.type: rocksdb
state.backend.incremental: true
state.backend.rocksdb.localdir: /local-ssd/flink/rocksdb

# ─── RocksDB Memory Control ───
state.backend.rocksdb.memory.managed: true
state.backend.rocksdb.memory.write-buffer-ratio: 0.5
state.backend.rocksdb.memory.high-prio-pool-ratio: 0.1
# For very large state (100GB+), enable partitioned index filters:
# state.backend.rocksdb.memory.partitioned-index-filters: true

# ─── Network Memory ───
# Default 10% is usually fine. Increase for high-parallelism jobs.
# taskmanager.memory.network.fraction: 0.1
# Enable buffer debloating for faster checkpoints:
taskmanager.network.memory.buffer-debloat.enabled: true
# For parallelism > 200, increase floating buffers:
# taskmanager.network.memory.floating-buffers-per-gate: 256

# ─── RocksDB Metrics (essential) ───
state.backend.rocksdb.metrics.block-cache-usage: true
state.backend.rocksdb.metrics.block-cache-capacity: true
state.backend.rocksdb.metrics.cur-size-all-mem-tables: true
state.backend.rocksdb.metrics.actual-delayed-write-rate: true
state.backend.rocksdb.metrics.is-write-stopped: true
state.backend.rocksdb.metrics.mem-table-flush-pending: true
state.backend.rocksdb.metrics.num-running-compactions: true
state.backend.rocksdb.metrics.estimate-live-data-size: true

# ─── Monitoring ───
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249

# ─── Diagnostics ───
taskmanager.debug.memory.log: true
env.java.opts.taskmanager: >-
  -XX:+HeapDumpOnOutOfMemoryError
  -XX:HeapDumpPath=/tmp/flink-heapdump

# ─── Container safety ───
# If running in K8s/YARN with strict memory limits, ensure JVM overhead
# has enough room for thread stacks, GC, and JNI:
taskmanager.memory.jvm-overhead.fraction: 0.1
taskmanager.memory.jvm-overhead.min: 256m
```

### What This Gives You (8GB total)

```
Total Process Memory:         8192 MB
├── JVM Overhead:             ~820 MB  (10% of 8192, clamped by min/max)
│   ├── Metaspace:            256 MB
│   └── Overhead:             ~564 MB  (thread stacks, GC, JNI)
│
└── Total Flink Memory:       ~7372 MB
    ├── Framework Heap:        128 MB
    ├── Task Heap:            ~2221 MB  (derived: what's left)
    ├── Managed Memory:       ~4423 MB  (60% of Flink memory → RocksDB)
    │   ├── Write Buffers:    ~1474 MB  (50%)
    │   ├── Block Cache:      ~3686 MB  (capacity, includes write buffer headroom)
    │   └── Index/Filter:      ~369 MB  (10% of cache, high-priority)
    ├── Network Memory:        ~737 MB  (10% of Flink memory)
    ├── Framework Off-heap:    128 MB
    └── Task Off-heap:           0 MB
```

---

## 12. Common Pitfalls

| Pitfall | Why It's Bad | Fix |
|---------|-------------|-----|
| Disabling `rocksdb.memory.managed` | RocksDB allocates native memory without limits → container OOM | Always keep `managed: true` in containers |
| Many column families + slot sharing | Each CF needs MemTables; shared managed budget gets spread thin | Increase `managed.fraction` or reduce slot sharing |
| Setting `high-prio-pool-ratio: 0` | Index/filter blocks compete with data blocks and get evicted → every read reloads indexes from disk | Keep at 0.1 or higher |
| NFS/HDFS for `rocksdb.localdir` | Network I/O bottleneck that manifests as "slow processing" | Use local SSDs |
| Not enabling RocksDB metrics | You're flying blind — can't diagnose cache misses, write stalls, or memory pressure | Enable the essential metrics listed in Section 9c |
| Setting both `process.size` and `task.heap.size` | Flink rejects conflicting explicit settings | Use one approach or the other (Section 4) |
| Default network memory at high parallelism | `#channels × 2 + 8` per gate adds up fast; buffer pool exhaustion | Increase `network.fraction` or reduce `buffers-per-channel` |
| Not enabling buffer debloating | Static buffer sizes cause either slow checkpoints (too much in-flight data) or reduced throughput (too little) | Enable `buffer-debloat.enabled: true` |
| Hash-shuffle at high batch parallelism | O(parallelism) memory per result partition → OOM | Use sort-shuffle (default since Flink 1.12) |
| Default JM memory for session clusters | 1600m is tight for multiple jobs; HA recovery loads all jobs at once | Increase `jobmanager.memory.process.size` to 2-4g |
| JM Metaspace default with many classloaders | 256m fills up with many submitted jobs in session mode | Increase `jobmanager.memory.jvm-metaspace.size` to 512m+ |
| Setting `jobmanager.memory.heap.size` with `process.size` | Flink rejects conflicting explicit settings | Use one or the other |

---

## References

- [Set up JobManager Memory](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/mem_setup_jobmanager/)
- [Set up TaskManager Memory](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/mem_setup_tm/)
- [Memory Tuning Guide](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/mem_tuning/)
- [Memory Troubleshooting](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/mem_trouble/)
- [State Backends](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/state_backends/)
- [Large State Tuning](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/large_state_tuning/)
- [Flink Metrics](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/)
- [Network Memory Tuning](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/memory/network_mem_tuning/)
- [Monitoring Back Pressure](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/monitoring/back_pressure/)
- [Checkpointing Under Backpressure](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/checkpointing_under_backpressure/)
- [A Deep-Dive into Flink's Network Stack (blog)](https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack/)
- [How to Identify the Source of Backpressure (blog)](https://flink.apache.org/2021/07/07/how-to-identify-the-source-of-backpressure/)
- [Using RocksDB State Backend (blog)](https://flink.apache.org/2021/01/18/using-rocksdb-state-backend-in-apache-flink-when-and-how/)
- [Managing RocksDB Memory (Ververica)](https://www.ververica.com/blog/manage-rocksdb-memory-size-apache-flink)
- See also: `codedocs/flink-rocksdb-state-backend-tuning.md` for RocksDB internals, incremental checkpoints, and Column Family details

### Source Code References

- `RocksDBMemoryControllerUtils.java:75-98` — cache/write-buffer capacity formulas
- `RocksDBMemoryConfiguration.java:187-203` — validation constraints
- `RocksDBOptions.java:122-191` — all RocksDB memory config options with defaults
- `RocksDBNativeMetricOptions.java` — all RocksDB metric config options
- `MetricUtils.java` — JVM/managed/GC/network metric registration
- `MemoryLogger.java` — periodic memory logging implementation
- `TaskManagerOptions.java` — network memory fraction/min/max, buffer debloating options
- `NettyShuffleEnvironmentOptions.java` — buffer-per-channel, floating buffers, sort-shuffle, compression
- `NettyShuffleEnvironmentConfiguration.java` — hardcoded defaults, buffer count calculation
- `BufferDebloater.java` — buffer debloating algorithm implementation
- `JobManagerOptions.java` — JM memory config options with defaults
- `JobManagerProcessUtils.java` — JM memory calculation logic
