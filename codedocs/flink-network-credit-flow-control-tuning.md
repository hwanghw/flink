# Flink Network Stack: Credit-Based Flow Control & Tuning

A guide to how Flink moves data between operators, the credit-based flow control protocol,
and how to tune network parameters for **high throughput** vs **low latency** workloads.

Sources:
- [Network Memory Tuning](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/memory/network_mem_tuning/)
- [Deep Dive into Flink's Network Stack](https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack/)
- [Network Stack Vol. 2: Monitoring & Backpressure](https://flink.apache.org/2019/07/23/flink-network-stack-vol.-2-monitoring-metrics-and-that-backpressure-thing/)
- Flink source code: `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/`

---

## 1. Network Stack Architecture

When two operators in a Flink pipeline run on different TaskManagers (or different slots), records
travel through the network stack. Here is the end-to-end data path:

```
 ┌──────────────────────── Sender TaskManager ────────────────────────┐
 │                                                                    │
 │  Operator A                                                        │
 │      │                                                             │
 │      │ emit record                                                 │
 │      ▼                                                             │
 │  RecordWriter                                                      │
 │      │                                                             │
 │      │ serialize → write bytes into buffer                         │
 │      ▼                                                             │
 │  ┌──────────────── ResultPartition ──────────────────┐             │
 │  │                                                   │             │
 │  │  ResultSubpartition 0  ← for downstream subtask 0 │             │
 │  │  ┌────┐┌────┐┌────┐                              │             │
 │  │  │buf ││buf ││buf │  (output buffer queue)        │             │
 │  │  └────┘└────┘└────┘                              │             │
 │  │                                                   │             │
 │  │  ResultSubpartition 1  ← for downstream subtask 1 │             │
 │  │  ┌────┐┌────┐                                    │             │
 │  │  │buf ││buf │                                    │             │
 │  │  └────┘└────┘                                    │             │
 │  └──────────────────────────────────────────────────┘             │
 │      │                                                             │
 │      │ Netty server reads buffers                                  │
 │      ▼                                                             │
 │  ┌─────────┐                                                       │
 │  │  Netty  │ ── multiplexes subpartitions into TCP channels ──►    │
 │  └─────────┘                                                       │
 └────────────────────────────────────────────────────────────────────┘
                              │
                         TCP / Network
                              │
 ┌──────────────────────── Receiver TaskManager ──────────────────────┐
 │                                                                    │
 │  ┌─────────┐                                                       │
 │  │  Netty  │ ◄── receives buffers from TCP channel                 │
 │  └─────────┘                                                       │
 │      │                                                             │
 │      │ write buffer to correct input channel                       │
 │      ▼                                                             │
 │  ┌──────────────────── InputGate ───────────────────────┐          │
 │  │                                                      │          │
 │  │  InputChannel 0  ← from upstream subtask 0           │          │
 │  │  ┌────┐┌────┐┌────┐                                │          │
 │  │  │buf ││buf ││buf │  (received buffer queue)        │          │
 │  │  └────┘└────┘└────┘                                │          │
 │  │                                                      │          │
 │  │  InputChannel 1  ← from upstream subtask 1           │          │
 │  │  ┌────┐┌────┐                                      │          │
 │  │  │buf ││buf │                                      │          │
 │  │  └────┘└────┘                                      │          │
 │  │                                                      │          │
 │  │  Floating Buffer Pool (shared across channels)       │          │
 │  │  ┌────┐┌────┐┌────┐┌────┐┌────┐┌────┐┌────┐┌────┐ │          │
 │  │  │    ││    ││    ││    ││    ││    ││    ││    │ │          │
 │  │  └────┘└────┘└────┘└────┘└────┘└────┘└────┘└────┘ │          │
 │  └──────────────────────────────────────────────────────┘          │
 │      │                                                             │
 │      │ deserialize                                                 │
 │      ▼                                                             │
 │  Operator B                                                        │
 └────────────────────────────────────────────────────────────────────┘
```

### Key Abstractions

| Component | What it is | One per... |
|-----------|-----------|------------|
| **ResultPartition** | Container for one subtask's output | Producer subtask |
| **ResultSubpartition** | Logical output channel to one downstream subtask | (Producer, Consumer) pair |
| **InputGate** | Container for one subtask's input from one upstream operator | Consumer subtask per upstream op |
| **InputChannel** | Logical input channel from one upstream subtask | (Consumer, Producer) pair |
| **NetworkBufferPool** | Global pool of memory segments (32 KB each) | TaskManager |
| **LocalBufferPool** | Per-gate pool drawn from the global pool | InputGate or ResultPartition |

**TCP multiplexing:** Multiple subpartitions between the same two TaskManagers share a single TCP
connection. This is efficient but means one slow channel can affect others on the same connection
— credit-based flow control solves this.

---

## 2. Credit-Based Flow Control Protocol

### The Problem Without Flow Control

Without flow control, when the sender's output buffer pool is full, the **entire producer is
blocked** — it can't send on *any* channel, even unrelated ones. And on the receiver side,
if one input channel's buffers are full, the Netty thread stops reading from the TCP connection,
blocking *all* channels multiplexed on that connection.

### The Solution: Credits

Credit-based flow control (introduced in Flink 1.5) makes backpressure **per-logical-channel**
instead of per-TCP-connection.

**Core rule:** The sender can only send a buffer if it has credit for that channel. 1 buffer = 1 credit.

### The Full Credit Cycle

```
   Receiver (Consumer)                              Sender (Producer)
   ══════════════════                               ══════════════════

   1. Setup: allocate exclusive
      buffers (e.g., 2 per channel)
              │
              │  PartitionRequest
              │  (initialCredit = 2)
              ├─────────────────────────────────►  2. Store credits
              │                                       numCreditsAvailable = 2
              │
              │                                    3. Has data + credit > 0?
              │                                       YES → send buffer
              │                                       credit -= 1
              │
              │    BufferResponse                     backlog = 5
              │◄──(data + backlog=5)───────────────  (tells receiver:
              │                                       "I have 5 more buffers
              │                                        queued to send")
              │
   4. Receive buffer, queue it
      See backlog=5 → request
      floating buffers from pool
      (need 5 + 2 = 7 total)
              │
              │  Floating buffers arrive
              │  from LocalBufferPool
              │
   5. Announce new credits
              │
              │  AddCredit(credit=5)
              ├─────────────────────────────────►  6. numCreditsAvailable += 5
              │                                       resume sending
              │
              │    BufferResponse                  7. Send more data
              │◄──(data + backlog=2)───────────────   credit -= 1 each time
              │    BufferResponse
              │◄──(data + backlog=1)───────────────
              │    BufferResponse
              │◄──(data + backlog=0)───────────────
              │
   8. Process buffers, recycle
      them back to buffer pool
      → generates more credits
              │
              │  AddCredit(credit=3)
              ├─────────────────────────────────►  9. Can send again when
              │                                       new data arrives
```

### Exclusive vs Floating Buffers

```
 ┌──────────────────── InputGate ──────────────────────────────┐
 │                                                             │
 │  InputChannel 0        InputChannel 1        InputChannel 2 │
 │  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐ │
 │  │ ┌──┐ ┌──┐   │      │ ┌──┐ ┌──┐   │      │ ┌──┐ ┌──┐   │ │
 │  │ │E ││E │   │      │ │E ││E │   │      │ │E ││E │   │ │
 │  │ └──┘ └──┘   │      │ └──┘ └──┘   │      │ └──┘ └──┘   │ │
 │  │  exclusive   │      │  exclusive   │      │  exclusive   │ │
 │  │  (fixed, 2)  │      │  (fixed, 2)  │      │  (fixed, 2)  │ │
 │  └─────────────┘      └─────────────┘      └─────────────┘ │
 │                                                             │
 │  ┌───────────── Floating Buffer Pool ─────────────────────┐ │
 │  │  ┌──┐ ┌──┐ ┌──┐ ┌──┐ ┌──┐ ┌──┐ ┌──┐ ┌──┐             │ │
 │  │  │F ││F ││F ││F ││F ││F ││F ││F │             │ │
 │  │  └──┘ └──┘ └──┘ └──┘ └──┘ └──┘ └──┘ └──┘             │ │
 │  │  shared across ALL channels (8 default)                 │ │
 │  │  dynamically assigned based on backlog                  │ │
 │  └─────────────────────────────────────────────────────────┘ │
 │                                                             │
 │  Total input buffers = 3 × 2 (exclusive) + 8 (floating)    │
 │                      = 14 buffers per gate                  │
 └─────────────────────────────────────────────────────────────┘
```

| Buffer type | Allocation | Purpose | Config |
|-------------|-----------|---------|--------|
| **Exclusive** | Fixed per channel at setup | Guarantees minimum throughput even without floating buffers. Each channel always has its own buffers. | `taskmanager.network.memory.buffers-per-channel` (default: 2) |
| **Floating** | Dynamic, shared pool per gate | Absorbs bursts and skewed traffic. Channels with larger backlogs get more floating buffers. | `taskmanager.network.memory.floating-buffers-per-gate` (default: 8) |
| **Overdraft** | Emergency, per gate | Extra buffers for unaligned checkpoint completion when backpressured. | `taskmanager.network.memory.max-overdraft-buffers-per-gate` (default: 5) |

**Why this split?** With uniform traffic, exclusive buffers handle everything. With skewed traffic
(e.g., hot keys), floating buffers flow to the busy channels — much better than giving every
channel a large fixed allocation.

### The Three Flush Triggers

A buffer is sent from the output subpartition to the Netty layer when:

```
                          Record arrives
                               │
                               ▼
                     ┌───────────────────┐
                     │  Write to current │
                     │  buffer           │
                     └────────┬──────────┘
                              │
              ┌───────────────┼───────────────┐
              │               │               │
              ▼               ▼               ▼
     ┌────────────────┐ ┌──────────┐ ┌────────────────┐
     │ Buffer is FULL │ │ Timeout  │ │ Special event  │
     │ (32 KB default)│ │ expires  │ │ (checkpoint    │
     │                │ │ (100ms   │ │  barrier, end  │
     │ → flush now,   │ │  default)│ │  of partition) │
     │   get new buf  │ │          │ │                │
     │                │ │ → flush  │ │ → flush now,   │
     │                │ │   partial│ │   bypass timer │
     └────────────────┘ └──────────┘ └────────────────┘

     High throughput       Both        Correctness
     (buffers are full)   (ensures      (barriers must
                          delivery)     not be delayed)
```

The **buffer timeout** is the critical knob for the latency vs throughput trade-off.

---

## 3. Buffer Timeout vs Credit: What Actually Happens

Section 2 described the three flush triggers. This section traces the exact code path for the most
interesting case: **the buffer timeout fires on a partially-filled buffer**. Two sub-cases:

- **Credit available** → buffer is sent immediately through Netty
- **No credit** → buffer waits; re-sent when the next `AddCredit` arrives

### 3.1 Full Code Chain

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │  TASK THREAD                                                        │
 │                                                                     │
 │  1. RecordWriter — OutputFlusher thread (daemon)                    │
 │                                                                     │
 │     private class OutputFlusher extends Thread {                    │
 │         public void run() {                                         │
 │             while (running) {                                       │
 │                 Thread.sleep(timeout);  // e.g. 100ms               │
 │                 flushAll();             // ← timeout fires here     │
 │             }                                                       │
 │         }                                                           │
 │     }                                                               │
 │                                                                     │
 │     // Special case: timeout=0 → no background thread.             │
 │     // Instead, emit() calls flush() after EVERY record:            │
 │     //   this.flushAlways = (timeout == FLUSH_AFTER_EVERY_RECORD)   │
 │     //   if (flushAlways) targetPartition.flush(targetSubpartition) │
 │                                                                     │
 │  2. RecordWriter.flushAll() → ResultPartition.flushAll()            │
 │     → BufferWritingResultPartition.flushAllSubpartitions()          │
 │     → for each subpartition: subpartition.flush()                   │
 │                                                                     │
 │  3. PipelinedSubpartition.flush()  [line 673]                       │
 │                                                                     │
 │     public void flush() {                                           │
 │         synchronized (buffers) {                                    │
 │             if (buffers.isEmpty() || flushRequested) return;        │
 │             boolean isDataAvailableInUnfinishedBuffer =             │
 │                 buffers.size() == 1                                 │
 │                 && buffers.peek().getBufferConsumer()               │
 │                             .isDataAvailable();                     │
 │             // KEY: mark even an unfinished buffer as "ready"       │
 │             flushRequested = buffers.size() > 1                     │
 │                           || isDataAvailableInUnfinishedBuffer;     │
 │         }                                                           │
 │         if (notifyDataAvailable) notifyDataAvailable();  // ①       │
 │     }                                                               │
 │                                                                     │
 │     // isDataAvailableUnsafe() [line 607] now returns true:         │
 │     //   return !isBlocked                                          │
 │     //       && (flushRequested || getNumberOfFinishedBuffers() > 0)│
 └─────────────────────────────────────────────────────────────────────┘
                              │ ①  notifyDataAvailable()
                              │    → readView.notifyDataAvailable()
                              │      → CreditBasedSequenceNumberingViewReader
                              │         .notifyDataAvailable(view) [line 294]
                              │           → requestQueue.notifyReaderNonEmpty(this)
                              ▼
 ┌─────────────────────────────────────────────────────────────────────┐
 │  NETTY I/O THREAD  (EventLoop)                                      │
 │                                                                     │
 │  4. PartitionRequestQueue.notifyReaderNonEmpty()  [line 84]         │
 │                                                                     │
 │     void notifyReaderNonEmpty(NetworkSequenceViewReader reader) {   │
 │         // Cross from task thread to Netty I/O thread               │
 │         ctx.executor().execute(                                     │
 │             () -> ctx.pipeline().fireUserEventTriggered(reader));   │
 │     }                                                               │
 │                                                                     │
 │  5. userEventTriggered() → enqueueAvailableReader()  [line 104]     │
 │                                                                     │
 │     private void enqueueAvailableReader(                            │
 │             NetworkSequenceViewReader reader) throws Exception {    │
 │                                                                     │
 │         // ── CREDIT GATE ─────────────────────────────────────── ──│
 │         AvailabilityWithBacklog avail =                             │
 │             reader.getAvailabilityAndBacklog();                     │
 │         //   → CreditBasedSequenceNumberingViewReader [line 192]:   │
 │         //     return subpartitionView.getAvailabilityAndBacklog(   │
 │         //                numCreditsAvailable > 0);  ← credit flag  │
 │         //   → PipelinedSubpartition [line 592]:                    │
 │         //     if (isCreditAvailable)                               │
 │         //         isAvailable = isDataAvailableUnsafe()            │
 │         //             ← true because flushRequested == true        │
 │         //     else                                                 │
 │         //         isAvailable = nextBufferType.isEvent()           │
 │         //             ← false for data buffers                     │
 │         // ────────────────────────────────────────────────────── ──│
 │                                                                     │
 │         if (!avail.isAvailable()) {                                 │
 │             // ── PATH B: NO CREDIT ────────────────────────────────│
 │             // Announce backlog so consumer can request floats      │
 │             if (backlog > 0 && reader.needAnnounceBacklog())        │
 │                 announceBacklog(reader, backlog);                   │
 │             return;  // ← buffer stays, nothing sent yet            │
 │         }                                                           │
 │                                                                     │
 │         // ── PATH A: CREDIT AVAILABLE ─────────────────────────── │
 │         boolean triggerWrite = availableReaders.isEmpty();          │
 │         registerAvailableReader(reader);                            │
 │         if (triggerWrite)                                           │
 │             writeAndFlushNextMessageIfPossible(ctx.channel()); // ② │
 │     }                                                               │
 │                                                                     │
 │  6. writeAndFlushNextMessageIfPossible()  [line 297]  (Path A only) │
 │                                                                     │
 │     NetworkSequenceViewReader reader = pollAvailableReader();       │
 │     BufferAndAvailability next = reader.getNextBuffer();            │
 │     //   → CreditBasedSequenceNumberingViewReader [line 255]:       │
 │     //     BufferAndBacklog next = subpartitionView.getNextBuffer() │
 │     //     if (next.buffer().isBuffer())                            │
 │     //         --numCreditsAvailable;  // ← credit decremented      │
 │     //     if (numCreditsAvailable < 0) throw ...  // safety check  │
 │                                                                     │
 │     channel.writeAndFlush(new BufferResponse(                       │
 │         next.buffer(), seqNum, receiverId,                          │
 │         subpartitionId, numPartialBufs, backlog));                  │
 │     // ← partial buffer sent over the network immediately           │
 └─────────────────────────────────────────────────────────────────────┘
```

### 3.2 Two-Path Summary

```
Timeout fires (OutputFlusher.sleep() expires)
     │
     flushAll() → PipelinedSubpartition.flush()
     │  flushRequested = true
     │  isDataAvailableUnsafe() now returns true for the partial buffer
     │  notifyDataAvailable()  ─── crosses to Netty I/O thread ───►
     │
     PartitionRequestQueue.enqueueAvailableReader()
          │
          │  getAvailabilityAndBacklog(numCreditsAvailable > 0)
          │         │
          │   PipelinedSubpartition.getAvailabilityAndBacklog(isCreditAvailable):
          │     if (isCreditAvailable) → isAvailable = isDataAvailableUnsafe()
          │                                           = true (flushRequested)
          │     else                  → isAvailable = nextBufferType.isEvent()
          │                                           = false (it's a data buf)
          │
          ├── numCreditsAvailable > 0  ──────────────────────────────────────┐
          │   isAvailable = true                                              │
          │                                                                   ▼
          │                                          writeAndFlushNextMessageIfPossible()
          │                                            reader.getNextBuffer()
          │                                            --numCreditsAvailable
          │                                            channel.writeAndFlush(BufferResponse)
          │                                            ← partial buffer sent NOW
          │
          └── numCreditsAvailable == 0 ─────────────────────────────────────┐
              isAvailable = false                                             │
              (announce backlog if needed)                                   │
              return  ← buffer stays in PipelinedSubpartition                │
                                                                             │
              ... later: consumer recycles a buffer ...                      │
              ... RemoteInputChannel.unannouncedCredit++ ...                 │
              ... AddCredit sent to producer ...                              │
              ... PartitionRequestServerHandler.addCredit()                  │
              ... CreditBasedSequenceNumberingViewReader.addCredit()         │
                  numCreditsAvailable += creditDeltas              ◄─────────┘
              ... notifyDataAvailable() called again ...
              ... same path above, now credit > 0 → buffer sent
```

### 3.3 Key Insight: `flushRequested` bridges timeout and credit

The two mechanisms are **independent** and operate at different layers:

| | Buffer Timeout | Credit |
|-|----------------|--------|
| **What it controls** | Whether a partial buffer is eligible to send | Whether the sender is **allowed** to send at all |
| **Where it lives** | `PipelinedSubpartition.flushRequested` (producer side) | `CreditBasedSequenceNumberingViewReader.numCreditsAvailable` (producer side, managed by consumer) |
| **Set by** | `OutputFlusher` thread calling `flush()` | Consumer's `AddCredit` message |
| **Cleared by** | `pollBuffer()` draining the buffer from the queue | `getNextBuffer()` decrementing the counter |

`flushRequested = true` makes `isDataAvailableUnsafe()` return `true`, which makes
`getAvailabilityAndBacklog(isCreditAvailable=true)` return `isAvailable=true`.
But if `numCreditsAvailable == 0`, the call is `getAvailabilityAndBacklog(false)`, which skips
`isDataAvailableUnsafe()` entirely — data buffers are **never available when credit is zero**,
regardless of `flushRequested`.

So: the timeout makes the buffer *visible*; credit makes it *sendable*.

---

## 4. Buffer Memory Model

All network buffers come from a fixed pool of memory allocated at TaskManager startup:

```
 ┌──────────────────── TaskManager Memory ──────────────────────────┐
 │                                                                  │
 │  ┌─── Network Memory ────────────────────────────────────────┐   │
 │  │  taskmanager.memory.network.fraction (default: 0.1)       │   │
 │  │  taskmanager.memory.network.min (default: 64 MB)          │   │
 │  │  taskmanager.memory.network.max (default: 1 GB)           │   │
 │  │                                                           │   │
 │  │  ┌──────────────── NetworkBufferPool ──────────────────┐  │   │
 │  │  │  Pool of fixed-size segments                        │  │   │
 │  │  │  Segment size: taskmanager.memory.segment-size      │  │   │
 │  │  │               (default: 32 KB)                      │  │   │
 │  │  │                                                     │  │   │
 │  │  │  Total segments = network_memory / segment_size     │  │   │
 │  │  │  e.g., 64 MB / 32 KB = 2048 segments               │  │   │
 │  │  │                                                     │  │   │
 │  │  │  Allocated to LocalBufferPools on demand:           │  │   │
 │  │  │                                                     │  │   │
 │  │  │  ┌─ Output Pool ─┐  ┌── Input Pool ──┐             │  │   │
 │  │  │  │ (per Result    │  │ (per InputGate) │             │  │   │
 │  │  │  │  Partition)    │  │                 │             │  │   │
 │  │  │  │ max-buffers-   │  │ exclusive +     │             │  │   │
 │  │  │  │ per-channel    │  │ floating        │             │  │   │
 │  │  │  └────────────────┘  └─────────────────┘             │  │   │
 │  │  └─────────────────────────────────────────────────────┘  │   │
 │  └───────────────────────────────────────────────────────────┘   │
 │                                                                  │
 │  JVM Heap, Managed Memory, etc.                                  │
 └──────────────────────────────────────────────────────────────────┘
```

### Input Buffer Pool Size Formula

```
Input pool size = (#channels × buffers-per-channel) + floating-buffers-per-gate

Example: 4 upstream subtasks, defaults:
         = (4 × 2) + 8 = 16 buffers
         = 16 × 32 KB = 512 KB per input gate
```

### How Many Buffers Do You Actually Need?

The **buffer throughput formula** helps size things right:

```
buffers_needed = (throughput_bytes_per_sec × roundtrip_ms) / segment_size_bytes

Example: 320 MB/s throughput, 1 ms round-trip, 32 KB segments:
         = (320 × 1024 × 1024 × 0.001) / (32 × 1024)
         ≈ 10 actively-used buffers
```

If `buffers_needed > exclusive_buffers`, the channel will need floating buffers. If the floating
pool is also exhausted, credits drop to zero and the sender is throttled (backpressure).

---

## 5. Case 1: High Throughput Tuning

**Goal:** Maximize bytes/sec. Latency of 100ms+ is acceptable.

### The Key Insight

For high throughput, you want **full buffers** sent as infrequently as possible. Each buffer
carries 32 KB of data in one network operation — much more efficient than sending many small
partially-filled buffers.

```
 HIGH THROUGHPUT MODE:

 Buffer timeout = 100ms (default) or higher

 Time ──────────────────────────────────────────────────►

 ┌────────────────────────────────────────┐
 │ Buffer fills up naturally before       │  ← GOOD: full buffers
 │ timeout expires → flush when full      │     max data per network op
 │ [████████████████████████████ 32KB]    │
 └────────────────────────────────────────┘

 Records per buffer: many (hundreds to thousands)
 Network ops per second: fewer
 CPU overhead: lower
 Throughput: maximum
```

### Recommended Configuration

```yaml
# Buffer timeout: keep default or increase for even more batching
# env.setBufferTimeout(100)  ← default, good for throughput
# env.setBufferTimeout(200)  ← more batching, higher latency

# Segment size: keep default 32 KB. Only increase if seeing network overhead
# with very high-throughput pipelines (rare).
taskmanager.memory.segment-size: 32kb   # default

# Exclusive buffers: 2 per channel (default) is usually sufficient.
# Increase if throughput is limited AND credit round-trip is the bottleneck
# (diagnosed by: low outPoolUsage but high inPoolUsage on receiver).
taskmanager.network.memory.buffers-per-channel: 2   # default

# Floating buffers: 8 per gate (default). Increase for skewed workloads.
taskmanager.network.memory.floating-buffers-per-gate: 8   # default

# Network memory: increase if you have many channels (high parallelism × fan-out).
taskmanager.memory.network.fraction: 0.1   # default, increase if needed
taskmanager.memory.network.min: 64mb
taskmanager.memory.network.max: 1gb
```

### When to Increase `buffers-per-channel`

If you see this pattern in metrics:

```
Sender:   outPoolUsage = HIGH (data queued, can't send — credit exhausted)
Receiver: inPoolUsage  = LOW  (has room, draining fast)

Diagnosis: credit starvation.
           outPoolUsage is HIGH because unsent buffers are still holding
           their MemorySegments (segments are freed only AFTER Netty sends
           the data, not after the consumer processes it).
           With only 2 exclusive credits, the sender bursts 2 buffers,
           exhausts credit, and queues up further buffers — outPoolUsage spikes.
           Meanwhile the receiver has room but isn't getting data fast enough.

Fix: increase buffers-per-channel (e.g., 2 → 4)
     This allocates more exclusive buffers on the receiver at startup,
     which are announced as initial credits in the PartitionRequest.
     The sender can now keep N buffers in flight before waiting for
     AddCredit, reducing the credit-starvation spike.
```

> **Why outPoolUsage = LOW does NOT mean "can't send":**
> LOW outPoolUsage means the sender's output pool is mostly empty — i.e., data IS flowing and
> segments are being freed quickly after Netty sends them. This is the healthy or under-utilized
> state (see matrix below). If the sender truly "has data but can't send", those unsent buffers
> still hold their segments → outPoolUsage goes HIGH, not LOW.

### Buffer Debloating (Recommended for Checkpointing)

Even in high-throughput mode, large in-flight data volumes make checkpoints slower (especially
aligned checkpoints). Buffer debloating automatically adjusts buffer usage to match actual
throughput, reducing in-flight data without sacrificing throughput:

```yaml
taskmanager.network.memory.buffer-debloat.enabled: true
```

```
 Without debloating:                    With debloating:

 In-flight buffers: 16 × 32KB          In-flight buffers: 4 × 32KB
 = 512 KB per channel                  = 128 KB per channel
 (many buffers allocated but            (only what's needed for
  sitting idle waiting)                  current throughput)

 Checkpoint barrier must               Checkpoint barrier passes
 wait for all 512KB to drain  ←SLOW    through only 128KB    ←FAST
```

### Diagnosis: Low Buffer Fill Rate

```
 avg_bytes_per_buffer = numBytesOutRemote / numBuffersOutRemote

 If avg_bytes_per_buffer << 32 KB:
   → Buffers are being flushed before they're full
   → Either timeout is too low, or throughput is naturally low
   → If you want more throughput: increase buffer timeout
```

---

## 6. Case 2: Low Latency Tuning

**Goal:** Minimize end-to-end record delivery time. Willing to trade throughput.

### The Key Insight

For low latency, you want records sent **as soon as possible**, even in partially-filled buffers.
The buffer timeout controls the maximum time a record sits waiting in a buffer.

```
 LOW LATENCY MODE:

 Buffer timeout = 0ms to 10ms

 Time ──────────────────────────────────────────────────►

 ┌──────────┐
 │ Timeout  │  ← buffer flushed early (partially filled)
 │ fires    │     low data per network op
 │ [██░░░░░░░░░░░░░░░░░░░░░░░░░ ~2KB]  │
 └──────────┘

 Records per buffer: few (1-10)
 Network ops per second: many more
 CPU overhead: higher (more syscalls, more Netty flushes)
 Throughput: ~75% of maximum at 1ms timeout
```

### Latency Accumulation Across Hops

Each network shuffle adds latency. In the worst case, a record waits `buffer_timeout / 2` at
each hop (on average, it arrives halfway through the timeout window):

```
 Source → Operator A → Operator B → Operator C → Sink

 Network hops with shuffle: 4 (if all on different TMs)

 Expected network latency per record:
 = num_hops × (buffer_timeout / 2)

 With timeout = 100ms:  4 × 50ms  = 200ms  average network latency
 With timeout = 10ms:   4 × 5ms   = 20ms
 With timeout = 1ms:    4 × 0.5ms = 2ms
 With timeout = 0ms:    ~0ms      (flush every record immediately)
                                   but high CPU and reduced throughput
```

### Recommended Configuration

```yaml
# Buffer timeout: the most impactful setting for latency.
# Set via API:
#   env.setBufferTimeout(1)   ← 1ms, good balance
#   env.setBufferTimeout(0)   ← flush every record, lowest latency
#                                but significant throughput reduction

# Segment size: consider reducing for low-latency workloads.
# Smaller segments mean less wasted memory in partially-filled buffers.
# Also reduces checkpoint data for unaligned checkpoints.
taskmanager.memory.segment-size: 4kb   # or 8kb (default is 32kb)

# Exclusive buffers: can reduce to 1 to save memory,
# since credit round-trip is less critical when buffers flush fast.
taskmanager.network.memory.buffers-per-channel: 1   # save memory

# Floating buffers: can reduce slightly since buffers cycle faster.
taskmanager.network.memory.floating-buffers-per-gate: 4   # less idle memory

# Network memory: can reduce since each buffer is smaller and fewer are needed.
taskmanager.memory.network.min: 32mb
```

### Trade-offs to Be Aware Of

```
 Buffer     Throughput    Avg Network     CPU         Memory
 Timeout    (vs max)      Latency/hop     Overhead    Efficiency
 ════════   ══════════    ═══════════     ════════    ══════════
 100ms      100%          50ms            Low         High (full bufs)
 10ms       ~95%          5ms             Medium      Medium
 1ms        ~75%          0.5ms           Higher      Lower (partial)
 0ms        ~50-60%       ~0ms            Highest     Lowest
```

**Important:** Even at `bufferTimeout = 0`, there is still some batching — records that arrive
while a buffer is being serialized will share the same buffer. The timeout only controls how
long an incomplete buffer waits before being flushed.

### Reducing Segment Size: Impact

```
 Segment size = 32 KB (default):

   Record [100 bytes] → sits in 32 KB buffer
   31,900 bytes wasted if flushed early by timeout
   Network sends 32 KB for 100 bytes of data

 Segment size = 4 KB:

   Record [100 bytes] → sits in 4 KB buffer
   3,900 bytes wasted if flushed early
   Network sends 4 KB for 100 bytes of data  ← much less waste

 BUT: more segments needed per unit of data when throughput is high
      → more buffer management overhead
```

---

## 7. Monitoring & Diagnosing Network Issues

### Key Metrics

| Metric | What it measures | Healthy range |
|--------|-----------------|---------------|
| `outPoolUsage` | Fraction of **this operator's own** output LocalBufferPool in use (shared across all output subpartitions) | < 0.7 (no backpressure) |
| `inPoolUsage` | Fraction of **this operator's own** input LocalBufferPool in use (exclusive + floating buffers combined) | < 0.7 |
| `floatingBuffersUsage` | Fraction of floating buffers assigned | < 0.8 |
| `exclusiveBuffersUsage` | Fraction of exclusive buffers in use | < 0.8 |
| `numBytesInRemote` / `numBuffersInRemote` | Avg bytes per buffer (fill rate) | Close to segment-size |

### How the Flink UI Backpressure % Is Calculated

The percentage shown in the Flink UI (e.g. 70%) is a **time-domain metric**, not a
buffer-pool-occupancy metric. It measures how long the task thread was blocked waiting for
an output buffer, expressed as a fraction of wall-clock time.

#### Code path: where the clock starts and stops

```
Task thread calls RecordWriter.emit()
    │
    ▼
BufferWritingResultPartition.requestBufferBuilder()
    │
    ├─ LocalBufferPool has a free segment? ──YES──► return immediately
    │                                               (no backpressure tick)
    │
    └─ NO free segment (outPoolUsage = 100%)
           │
           ▼
       hardBackPressuredTimeMsPerSecond.markStart()   ← clock starts
           │
           ▼
       requestBufferBuilderBlocking()  ◄── task thread BLOCKS here
           │
           │   (Netty I/O thread finishes sending a buffer → segment returned to pool)
           │
           ▼
       buffer acquired
           │
           ▼
       hardBackPressuredTimeMsPerSecond.markEnd()     ← clock stops
```

Source: `BufferWritingResultPartition.java`
```java
hardBackPressuredTimeMsPerSecond.markStart();
bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);
hardBackPressuredTimeMsPerSecond.markEnd();
```

#### TimerGauge: rolling 60-second window

`TimerGauge` (flink-runtime/.../metrics/TimerGauge.java) accumulates the elapsed blocking
milliseconds and exposes `getValue()` as **milliseconds of blocking per second**, computed
over a rolling 60-second window.

#### Metric names

| Metric | Meaning |
|--------|---------|
| `backPressuredTimeMsPerSecond` | `softBackPressured + hardBackPressured` (total) |
| `hardBackPressuredTimeMsPerSecond` | blocking on `requestBufferBuilderBlocking()` |
| `softBackPressuredTimeMsPerSecond` | writer-side throttling (rare, not buffer-pool-driven) |
| `idleTimeMsPerSecond` | task waiting for input (no records to process) |
| `busyTimeMsPerSecond` | task actively processing (1000 − backPressured − idle) |

#### REST → UI conversion (`JobVertexBackPressureHandler.java`)

```java
double ratio = metricStore.getMetric("backPressuredTimeMsPerSecond", "0") / 1_000.0;
```

Dividing ms-per-second by 1 000 converts it to a fraction of wall-clock time.
**70% in the UI = task was blocked 700 ms out of every 1 000 ms.**

The vertex-level percentage is the **maximum across all subtasks** of that vertex.

#### Thresholds

| ratio | UI label |
|-------|----------|
| ≤ 10% | OK       |
| 10% – 50% | LOW  |
| > 50% | HIGH     |

#### Relationship to `outPoolUsage`

`outPoolUsage → 100%` is the **cause**: all `MemorySegment`s in the operator's
`LocalBufferPool` are held by in-flight buffers not yet sent by Netty.

UI backpressure % is the **effect**: how long the task was stalled as a result.

```
outPoolUsage  ─── space domain (how full is the pool right now?)
backpressure% ─── time domain  (what fraction of time was the task blocked?)
```

They move together under steady backpressure, but differ during transient spikes:
`outPoolUsage` can briefly hit 100% without the % rising much if Netty drains quickly.

---

### Backpressure Diagnosis Matrix

The two metrics tell different stories depending on which operators you compare.
There are two complementary views, and mixing them in one table (as many guides do)
produces contradictions like "both HIGH/LOW rows have different causes" — because
they were measuring different things.

#### View 1 — Per-task (both columns = the SAME operator X)

Use this to find **which task** in the pipeline is the bottleneck.

```
 ┌────────────────────────┬────────────────────────┬──────────────────────────────────────┐
 │ X outPoolUsage         │ X inPoolUsage          │ What it means                        │
 │ (X's own output pool)  │ (X's own input pool)   │                                      │
 ├────────────────────────┼────────────────────────┼──────────────────────────────────────┤
 │ HIGH                   │ LOW                    │ X is backpressured by something      │
 │                        │                        │ DOWNSTREAM. X produces fine but      │
 │                        │                        │ can't push out.                      │
 │                        │                        │ → Walk downstream to find where      │
 │                        │                        │   inPoolUsage first goes HIGH        │
 ├────────────────────────┼────────────────────────┼──────────────────────────────────────┤
 │ LOW                    │ HIGH                   │ X IS the bottleneck. X can't         │
 │                        │                        │ process input fast enough.           │
 │                        │                        │ Upstream will soon be backpressured. │
 │                        │                        │ → Check X's CPU, GC, I/O;           │
 │                        │                        │   increase X's parallelism           │
 ├────────────────────────┼────────────────────────┼──────────────────────────────────────┤
 │ HIGH                   │ HIGH                   │ X is BLOCKED by its downstream.      │
 │                        │                        │ Downstream pressure fills X's output │
 │                        │                        │ → X's thread stalls → X's input fills│
 │                        │                        │ X itself is NOT the root bottleneck. │
 │                        │                        │ → Walk downstream to find the first  │
 │                        │                        │   task with LOW out + HIGH in        │
 ├────────────────────────┼────────────────────────┼──────────────────────────────────────┤
 │ LOW                    │ LOW                    │ X is healthy, flowing freely.        │
 │                        │                        │ → No action needed                   │
 └────────────────────────┴────────────────────────┴──────────────────────────────────────┘
```

**How to find the bottleneck task:** Scan the pipeline from sink to source. The first task X
where `inPoolUsage` is HIGH but `outPoolUsage` is NOT high is the slow processor (LOW out +
HIGH in). Everything upstream of X will show HIGH out + HIGH in — that is the cascaded symptom
of X's backpressure, NOT those tasks being slow themselves. Do not "fix" a HIGH/HIGH task;
walk downstream until you find the LOW out + HIGH in task — that is the root cause.

#### View 2 — Per-link (columns = SENDER A's output vs RECEIVER B's input, one A→B exchange)

Use this to distinguish **consumer slowness** from a **network/credit problem** on a specific link.

```
 ┌────────────────────────┬────────────────────────┬──────────────────────────────────────┐
 │ A outPoolUsage         │ B inPoolUsage          │ What it means                        │
 │ (sender's output pool) │ (receiver's input pool)│                                      │
 ├────────────────────────┼────────────────────────┼──────────────────────────────────────┤
 │ HIGH                   │ HIGH                   │ B is slow → B's input fills → B      │
 │                        │                        │ stops sending credits → A's output   │
 │                        │                        │ fills. Genuine consumer backpressure.│
 │                        │                        │ → Fix B (use per-task view on B)     │
 ├────────────────────────┼────────────────────────┼──────────────────────────────────────┤
 │ HIGH                   │ LOW                    │ A has data, B has room, but data     │
 │                        │                        │ isn't flowing. The bottleneck is     │
 │                        │                        │ BETWEEN A and B: network bandwidth   │
 │                        │                        │ or credit starvation.                │
 │                        │                        │ → Check network bandwidth;           │
 │                        │                        │   increase buffers-per-channel       │
 ├────────────────────────┼────────────────────────┼──────────────────────────────────────┤
 │ LOW                    │ HIGH                   │ Rare: A is idle/slow (source dries   │
 │                        │                        │ up). B is draining a prior burst.    │
 │                        │                        │ → Check A's source throughput        │
 ├────────────────────────┼────────────────────────┼──────────────────────────────────────┤
 │ LOW                    │ LOW                    │ Healthy. Data flows freely.          │
 │                        │                        │ → No action needed                   │
 └────────────────────────┴────────────────────────┴──────────────────────────────────────┘
```

**Why HIGH/HIGH ≠ HIGH/LOW on the per-link view:**
When B (the consumer) is slow, B's input fills up. B's `LocalBufferPool` is exhausted, so B
cannot allocate buffers to receive new data. B stops sending `AddCredit` messages. A's output
buffers pile up waiting for credit. **Both sides fill up together.** If B had room (LOW inPool)
but A was still backing up (HIGH outPool), the problem cannot be B's processing speed — data
that arrives at B is being drained fast. The bottleneck must be the delivery path itself.

### Floating vs Exclusive Buffer Analysis

When `inPoolUsage` is high, check which type is saturated:

```
 floatingBuffersUsage HIGH + exclusiveBuffersUsage LOW:
   → Backpressure on FEW input channels (skewed traffic)
   → Floating buffers are all assigned to the hot channels
   → Fix: increase floating-buffers-per-gate

 floatingBuffersUsage HIGH + exclusiveBuffersUsage HIGH:
   → Backpressure on MOST/ALL input channels
   → Uniform backpressure from downstream
   → Fix: address the downstream bottleneck

 floatingBuffersUsage LOW:
   → No input-side backpressure
```

### Decision Tree

```
Is your pipeline slow?
│
├── Step 1: Use PER-TASK view. Check each operator's own inPoolUsage vs outPoolUsage.
│   │
│   ├── Find the first operator X where inPoolUsage is HIGH but outPoolUsage is NOT high.
│   │   X is the bottleneck (processing slower than it receives).
│   │   → Check X's CPU, GC, I/O. Increase X's parallelism.
│   │
│   └── Every operator upstream of X will show outPoolUsage HIGH (backpressured by X).
│       This is expected — fix X and upstream clears automatically.
│
├── Step 2: Once you have a candidate, use PER-LINK view to confirm it's X and not the network.
│   │
│   ├── Compare sender (X-1) outPoolUsage vs receiver (X) inPoolUsage.
│   │   │
│   │   ├── BOTH HIGH → confirmed: X is slow (consumer backpressure). Fix X.
│   │   │
│   │   └── Sender HIGH + Receiver LOW → network/credit bottleneck BETWEEN X-1 and X.
│   │                                    X-1 can't push despite X having room.
│   │                                    → Check network bandwidth.
│   │                                    → Increase buffers-per-channel.
│   │
│   └── Check buffer fill rate on operator X
│   │           │
│   │           ├── LOW fill rate → buffer timeout too small
│   │           │                   for current throughput
│   │           │
│   │           └── HIGH fill rate → genuine throughput limit
│   │                                → increase parallelism
│   │
│   └── outPoolUsage LOW everywhere?
│       │
│       └── Pipeline is not network-bound
│           → Check CPU, disk I/O, source/sink throughput
```

---

## 8. Configuration Quick Reference

### All Network Config Parameters

| Config | Default | Description |
|--------|---------|-------------|
| `taskmanager.memory.segment-size` | `32 KB` | Size of each network buffer |
| `taskmanager.memory.network.fraction` | `0.1` | Fraction of total memory for network |
| `taskmanager.memory.network.min` | `64 MB` | Minimum network memory |
| `taskmanager.memory.network.max` | `1 GB` | Maximum network memory |
| `taskmanager.network.memory.buffers-per-channel` | `2` | Exclusive buffers per input channel |
| `taskmanager.network.memory.floating-buffers-per-gate` | `8` | Floating buffers per input gate |
| `taskmanager.network.memory.max-buffers-per-channel` | `10` | Max output buffers per subpartition |
| `taskmanager.network.memory.max-overdraft-buffers-per-gate` | `5` | Emergency buffers for unaligned checkpoints |
| `taskmanager.network.memory.read-buffer.required-per-gate.max` | `Integer.MAX_VALUE` | Threshold for required vs optional buffers |
| `taskmanager.network.memory.buffer-debloat.enabled` | `false` | Auto-adjust buffer usage to throughput |
| `taskmanager.network.memory.buffer-debloat.target` | `1000 ms` | Target consumption time for debloating |
| `execution.buffer-timeout` | `100 ms` | Buffer flush interval (also: `env.setBufferTimeout()`) |

### Side-by-Side: High Throughput vs Low Latency

| Parameter | High Throughput | Low Latency |
|-----------|----------------|-------------|
| **`execution.buffer-timeout`** | `100ms` (default) or higher | `0ms` – `10ms` |
| **`taskmanager.memory.segment-size`** | `32 KB` (default) | `4 KB` – `8 KB` |
| **`buffers-per-channel`** | `2` (default); `4` if credit RTT is bottleneck | `1` (save memory) |
| **`floating-buffers-per-gate`** | `8` (default); increase for skewed workloads | `4` (less idle memory) |
| **`network.fraction`** | `0.1` – `0.15` (more buffers, more in-flight data) | `0.05` – `0.1` (less needed) |
| **`buffer-debloat.enabled`** | `true` (reduce checkpoint size) | Optional (buffers already small) |
| **Throughput** | Maximum | ~50–75% of max |
| **Avg network latency/hop** | `~50 ms` | `< 1 ms` |
| **Checkpoint impact** | Higher in-flight data (use debloating) | Lower in-flight data |
| **CPU overhead** | Lower | Higher (more flushes) |

### Quick-Start Snippets

**High throughput job:**
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setBufferTimeout(100);  // default, maximum batching

// In flink-conf.yaml:
// taskmanager.memory.segment-size: 32kb
// taskmanager.network.memory.buffer-debloat.enabled: true
// taskmanager.memory.network.fraction: 0.1
```

**Low latency job:**
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setBufferTimeout(1);  // flush every 1ms

// In flink-conf.yaml:
// taskmanager.memory.segment-size: 4kb
// taskmanager.network.memory.buffers-per-channel: 1
// taskmanager.network.memory.floating-buffers-per-gate: 4
// taskmanager.memory.network.min: 32mb
```

---

## 9. Key Source Files

| File | What it does |
|------|-------------|
| `RemoteInputChannel.java` | Receiver: manages exclusive/floating buffers, tracks `unannouncedCredit`, requests floating buffers based on sender backlog |
| `BufferManager.java` | Allocates exclusive and floating buffers for input channels |
| `CreditBasedSequenceNumberingViewReader.java` | Sender: tracks `numCreditsAvailable`, only sends when credit > 0 |
| `PartitionRequestQueue.java` | Server-side Netty handler: reads buffers from subpartitions when credit available |
| `CreditBasedPartitionRequestClientHandler.java` | Client-side Netty handler: receives buffers, queues credit announcements |
| `PartitionRequestServerHandler.java` | Routes `AddCredit`, `PartitionRequest` messages to readers |
| `NettyMessage.java` | Defines all protocol messages: `BufferResponse`, `AddCredit`, `BacklogAnnouncement`, etc. |
| `NetworkBufferPool.java` | Global buffer pool per TaskManager |
| `LocalBufferPool.java` | Per-gate buffer pool drawn from global pool |

