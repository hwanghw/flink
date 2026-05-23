# Flink Network Stack: Direct Memory, Netty, and Java NIO

This document explains how Apache Flink uses off-heap (direct) memory for inter-TaskManager network
communication, how Netty and Java NIO fit into that picture, and what the performance implications
are. All code snippets are taken directly from this repository.

---

## 1. End-to-End Architecture

```
  PRODUCER TaskManager
  ┌──────────────────────────────────────────────────────────────────┐
  │  Task thread                                                     │
  │    RecordWriter.addRecord(record)                                │
  │         │                                                        │
  │    RecordSerializer → BufferBuilder (writes into MemorySegment)  │
  │         │                                                        │
  │    PipelinedSubpartition  (per downstream consumer)             │
  │         │  BufferConsumer queue                                  │
  │         │                                                        │
  │  Network I/O thread  (Netty EventLoop)                          │
  │    PartitionRequestQueue.writeSomeData()                         │
  │         │  respects channel writability & credit budget          │
  │         │                                                        │
  │    NettyMessage.BufferResponse.write()                           │
  │         │  header ByteBuf (direct) + data MemorySegment          │
  │         ▼                                                        │
  │    channel.writeAndFlush()  ──────────────►  TCP socket         │
  └──────────────────────────────────────────────────────────────────┘
                                   │
                              TCP / IP
                         (optional LZ4/ZSTD)
                                   │
  CONSUMER TaskManager             ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  Network I/O thread  (Netty EventLoop)                          │
  │    CreditBasedPartitionRequestClientHandler.channelRead0()       │
  │         │  decode NettyMessage.BufferResponse                    │
  │         │                                                        │
  │    RemoteInputChannel.onBuffer()                                 │
  │         │  enqueue into receivedBuffers (PrioritizedDeque)       │
  │         │  send AddCredit message back to producer               │
  │         │                                                        │
  │  Task thread                                                     │
  │    InputGate.getNext()                                           │
  │         │  poll from RemoteInputChannel                          │
  │         │                                                        │
  │    RecordReader → deserialize → business logic                   │
  │         │                                                        │
  │    Buffer.recycle()                                              │
  │         │                                                        │
  │    LocalBufferPool → NetworkBufferPool                           │
  └──────────────────────────────────────────────────────────────────┘
```

---

## 2. Direct Memory Layer

### 2.1 MemorySegment — the memory primitive

`MemorySegment` (`flink-core/.../memory/MemorySegment.java`) is Flink's unified abstraction over
three memory types: on-heap byte array, off-heap `ByteBuffer` (direct), and off-heap unsafe memory.
The network stack always uses the **off-heap direct** variant.

```java
// MemorySegment.java (simplified)
public final class MemorySegment {

    // Uses sun.misc.Unsafe for all multi-byte reads/writes — JIT-compiled
    // to native MOV instructions, bypassing JVM bounds-check overhead.
    private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

    // Non-null for on-heap; null for off-heap.
    @Nullable private final byte[] heapMemory;

    // Holds a reference to the direct ByteBuffer so GC cannot collect it
    // while this segment is alive.
    @Nullable private ByteBuffer offHeapBuffer;

    // Absolute memory address (relative to heap array base if on-heap,
    // or an absolute native address if off-heap).
    private long address;

    // Absolute positioning methods → thread-safe by design
    // (no internal position cursor to race on).
    public void put(int offset, byte[] src, int srcOffset, int length) { ... }
    public void get(int offset, byte[] dst, int dstOffset, int length) { ... }

    // Bulk comparison used by sorted merge / sort operators
    public int compare(MemorySegment other, int posThis, int posOther, int len) { ... }
}
```

Key design choices:
- **No separate subclasses** for on-heap vs off-heap — avoids virtual dispatch on hot paths.
- **Absolute positioning** (`put(offset, ...)`) instead of a stateful position cursor — callers
  from different threads can safely read/write at different offsets concurrently.
- **`sun.misc.Unsafe`** for multi-byte primitives — the JIT inlines these to single native
  instructions (`MOV`, `MOVQ`).

---

### 2.1.1 Two flavors of off-heap memory

Both live outside the JVM heap. The difference is whether the **JVM knows the memory exists**.

#### Off-heap Direct — `allocateUnpooledOffHeapMemory`
```java
// MemorySegmentFactory.java
public static MemorySegment allocateUnpooledOffHeapMemory(int size, Object owner) {
    ByteBuffer memory = ByteBuffer.allocateDirect(size);  // JVM registers this internally
    return new MemorySegment(memory, owner);              // allowWrap=true, cleaner=null
}
```
- The JVM tracks the allocation and attaches a **GC `Cleaner`** to the `ByteBuffer` reference.
  When the `ByteBuffer` becomes GC-eligible, the native memory is freed automatically.
- **Counted against `-XX:MaxDirectMemorySize`.** Exceeding the limit triggers a full GC attempt,
  then `OutOfMemoryError: Direct buffer memory`.
- **Used by `NetworkBufferPool`** for all network I/O buffers.

#### Off-heap Unsafe / Native — `allocateOffHeapUnsafeMemory`
```java
// MemorySegmentFactory.java
public static MemorySegment allocateOffHeapUnsafeMemory(
        int size, Object owner, Runnable customCleanupAction) {
    long address = MemoryUtils.allocateUnsafe(size);               // UNSAFE.allocateMemory() = raw malloc()
    ByteBuffer shell = MemoryUtils.wrapUnsafeMemoryWithByteBuffer(address, size); // synthetic wrapper
    Runnable cleaner = MemoryUtils.createMemoryCleaner(address, customCleanupAction);
    return new MemorySegment(shell, owner, false, cleaner);        // allowWrap=false
}

// MemoryUtils.java
static long allocateUnsafe(long size) {
    return UNSAFE.allocateMemory(Math.max(1L, size));  // pure malloc() — JVM has no idea
}
```
- The JVM is **completely unaware** of this allocation. Only OS virtual address space limits it.
- **Not counted against `-XX:MaxDirectMemorySize`.**
- Must be freed manually via `UNSAFE.freeMemory()`. No GC fallback — leaks are permanent.
- The `ByteBuffer` is a synthetic shell fabricated via `UNSAFE.allocateInstance(DirectByteBuffer.class)`
  with its internal address field overwritten. `allowWrap=false` prevents inadvertent aliasing.
- **Used by Flink managed memory** (batch sort, hash-join, state spilling) —
  `taskmanager.memory.task.off-heap.size`.

#### Critical nuance: "Unsafe access" ≠ "Unsafe allocation"

`MemorySegment` uses `sun.misc.Unsafe` for **reading and writing** all three memory types
(on-heap, direct, and native). That is the *access pattern* — chosen for performance, it applies
universally. The allocation mechanism is orthogonal:

```
Network buffers:  ByteBuffer.allocateDirect()   ← direct-ALLOCATED
                  + UNSAFE.getLong / putLong()   ← unsafe-ACCESSED
                  → JVM tracks it, MaxDirectMemorySize applies

Managed memory:   UNSAFE.allocateMemory()        ← native-ALLOCATED
                  + UNSAFE.getLong / putLong()   ← unsafe-ACCESSED (same access pattern!)
                  → JVM invisible, no size limit
```

#### Comparison

| | Off-heap Direct | Off-heap Unsafe / Native |
|---|---|---|
| **Allocation call** | `ByteBuffer.allocateDirect()` | `UNSAFE.allocateMemory()` |
| **JVM awareness** | Registered and tracked | Invisible to JVM |
| **`-XX:MaxDirectMemorySize`** | Yes — counts against it | No — bypasses this limit |
| **Cleanup** | GC `Cleaner` (automatic) | `UNSAFE.freeMemory()` (manual) |
| **`allowWrap` in `MemorySegment`** | `true` | `false` (prevents aliasing) |
| **Used by in Flink** | `NetworkBufferPool` (network I/O) | Managed memory (sort, hash-join, state) |
| **Access inside `MemorySegment`** | `sun.misc.Unsafe` | same |

---

### 2.2 NetworkBufferPool — the master pool

One `NetworkBufferPool` per TaskManager. It pre-allocates all network direct memory at startup
and hands out `MemorySegment` slices to `LocalBufferPool` instances on demand.

```java
// NetworkBufferPool.java (key fields and constructor)
public class NetworkBufferPool implements BufferPoolFactory, MemorySegmentProvider {

    private final int totalNumberOfMemorySegments;
    private final int memorySegmentSize;

    // All available (not currently lent out) segments
    private final ArrayDeque<MemorySegment> availableMemorySegments;

    // All LocalBufferPools managed by this pool
    private final Set<LocalBufferPool> allBufferPools = new HashSet<>();

    public NetworkBufferPool(
            int numberOfSegmentsToAllocate,
            int segmentSize,
            Duration requestSegmentsTimeout) {

        this.availableMemorySegments = new ArrayDeque<>(numberOfSegmentsToAllocate);

        // Allocate ALL direct memory upfront at startup.
        // This fails fast if -XX:MaxDirectMemorySize is too small.
        for (int i = 0; i < numberOfSegmentsToAllocate; i++) {
            availableMemorySegments.add(
                MemorySegmentFactory.allocateUnpooledOffHeapMemory(segmentSize, null));
        }
    }
}
```

- **Pre-allocation** at startup: one `OutOfMemoryError` on boot rather than a silent OOM under
  load.
- `allocateUnpooledOffHeapMemory` calls `ByteBuffer.allocateDirect(size)` — memory lives in the
  OS process heap, outside the JVM GC heap.
- When a new `LocalBufferPool` is created, `NetworkBufferPool` **dynamically redistributes**
  segments across all pools (shrinks existing ones proportionally).

---

### 2.3 LocalBufferPool — per-task quota

Each task operator output and each input gate has its own `LocalBufferPool`. It enforces a per-task
buffer quota and acts as the first line of backpressure.

```java
// LocalBufferPool.java (key fields)
public class LocalBufferPool implements BufferPool {

    // Source of physical MemorySegments
    private final NetworkBufferPool networkBufferPool;

    // Segments currently available in this pool (not handed out)
    private final ArrayDeque<MemorySegment> availableMemorySegments = new ArrayDeque<>();

    // Listeners notified when a buffer becomes available
    // (used to wake up blocked producers)
    private final ArrayDeque<BufferListener> registeredListeners = new ArrayDeque<>();

    // Hard cap: total segments this pool may hold
    private final int maxNumberOfMemorySegments;

    // Per-subpartition quota: prevents one channel from starving others
    private final int maxBuffersPerChannel;

    // Tracks how many buffers are outstanding per subpartition
    @GuardedBy("availableMemorySegments")
    private final int[] subpartitionBuffersCount;
}
```

Buffer lifecycle:
```
Task requests buffer
    │
    LocalBufferPool.requestBuffer()
    ├── availableMemorySegments not empty → return segment immediately
    └── empty → try NetworkBufferPool.requestMemorySegmentsBlocking()
                  └── block until segment returned by another task
                        │
                  wrap in NetworkBuffer(segment, recycler)
                        │
Task writes records into buffer
        │
Task calls buffer.recycle()
        │
LocalBufferPool.recycle(segment)
    ├── listener waiting? → hand segment directly to listener (zero copy)
    └── no listener      → add back to availableMemorySegments
```

---

### 2.4 BufferBuilder / BufferConsumer — producer/consumer split

The operator (task thread) writes into a `BufferBuilder`; the Netty I/O thread reads from the
associated `BufferConsumer`. This separation allows the two threads to work concurrently on the
same `MemorySegment` without locks.

```java
// Conceptual flow (simplified)

// ── Task thread ──────────────────────────────────────────────────────
BufferBuilder builder = localBufferPool.requestBufferBuilder(subpartitionId);

// Serialize record bytes directly into the MemorySegment (zero copy)
builder.append(serializedRecord);
builder.commit();   // makes written bytes visible to consumer

// Hand a read-only view to the subpartition (no data copy)
BufferConsumer consumer = builder.createBufferConsumer();
pipelinedSubpartition.add(consumer);

// ── Netty I/O thread ─────────────────────────────────────────────────
Buffer buffer = consumer.build();  // sliced view up to current write position
// buffer.asByteBuf() → wraps the MemorySegment as a Netty ByteBuf
channel.writeAndFlush(new NettyMessage.BufferResponse(buffer, ...));
// After send, buffer.recycle() → segment returns to LocalBufferPool
```

---

## 3. Netty Integration

### 3.1 NettyBufferPool — all-direct allocator

Netty needs its own `ByteBuf` instances for message headers and framing. Flink plugs in a custom
`PooledByteBufAllocator` subclass that forces **all allocations to direct memory**.

```java
// NettyBufferPool.java
public class NettyBufferPool extends PooledByteBufAllocator {

    private static final boolean PREFER_DIRECT = true;

    // pageSize << maxOrder = 8192 << 9 = 4 MB per chunk.
    // Reduced from Netty's default 16 MB to limit overhead
    // per-arena after FLINK-10742 zero-copy improvements.
    private static final int PAGE_SIZE  = 8192;
    private static final int MAX_ORDER  = 9;      // → 4 MB chunks

    public NettyBufferPool(int numberOfArenas) {
        super(
            PREFER_DIRECT,
            0,              // ← zero heap arenas: heap allocation is forbidden
            numberOfArenas, // ← recommended: 2 × numTaskSlots
            PAGE_SIZE,
            MAX_ORDER);
    }

    // Redirect any accidental heap-buffer request to a direct buffer.
    @Override public ByteBuf heapBuffer()                          { return directBuffer(); }
    @Override public ByteBuf heapBuffer(int cap)                   { return directBuffer(cap); }
    @Override public CompositeByteBuf compositeHeapBuffer()        { return compositeDirectBuffer(); }
}
```

Why `numberOfArenas = 2 × numSlots`?
Each arena is independent (no cross-arena locking). More arenas → less contention under concurrent
task execution. 2× provides a safety margin for transient spikes without over-allocating.

---

### 3.2 NettyClient — transport auto-selection and bootstrap wiring

```java
// NettyClient.java — init()
void init(final NettyProtocol protocol, NettyBufferPool nettyBufferPool) throws IOException {
    bootstrap = new Bootstrap();

    // Auto-select transport: Epoll on Linux, NIO everywhere else
    if (Epoll.isAvailable()) {
        initEpollBootstrap();   // EpollIoHandler + EpollSocketChannel
        LOG.info("Transport type 'auto': using EPOLL.");
    } else {
        initNioBootstrap();     // NioIoHandler + NioSocketChannel
        LOG.info("Transport type 'auto': using NIO.");
    }

    bootstrap.option(ChannelOption.TCP_NODELAY, true);   // no Nagle — low latency
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

    // All ByteBuf allocations on this channel use NettyBufferPool (direct only)
    bootstrap.option(ChannelOption.ALLOCATOR, nettyBufferPool);
}

// Epoll path
private void initEpollBootstrap() {
    MultiThreadIoEventLoopGroup epollGroup =
        new MultiThreadIoEventLoopGroup(
            config.getClientNumThreads(),
            NettyServer.getNamedThreadFactory(name),
            EpollIoHandler.newFactory());            // ← epoll syscall
    bootstrap.group(epollGroup).channel(EpollSocketChannel.class);
}

// NIO path
private void initNioBootstrap() {
    MultiThreadIoEventLoopGroup nioGroup =
        new MultiThreadIoEventLoopGroup(
            config.getClientNumThreads(),
            NettyServer.getNamedThreadFactory(name),
            NioIoHandler.newFactory());              // ← select() syscall
    bootstrap.group(nioGroup).channel(NioSocketChannel.class);
}
```

---

### 3.3 NettyMessage wire format

Every message between TaskManagers is framed as:

```
+──────────────────+──────────────────+────────+──────────────────────+
│  FRAME LENGTH (4)│  MAGIC NUM (4)   │ ID (1) │   PAYLOAD            │
│   (int, BE)      │  0xBADC0FFE      │        │   (message-specific) │
+──────────────────+──────────────────+────────+──────────────────────+
  ◄──────────────── FRAME_HEADER_LENGTH = 9 bytes ────────────────────►
```

```java
// NettyMessage.java
static final int FRAME_HEADER_LENGTH = 4 + 4 + 1; // length + magic + msgId
static final int MAGIC_NUMBER        = 0xBADC0FFE;

// All message types
BufferResponse        (ID=0)  // actual data buffer — most frequent
PartitionRequest      (ID=2)  // consumer → producer: "give me subpartition X"
AddCredit             (ID=5)  // consumer → producer: "I have N more buffers ready"
CancelPartitionRequest(ID=3)  // consumer → producer: cancel
NewBufferSize         (ID=8)  // dynamic buffer size negotiation
ResumeConsumption     (ID=6)  // resume after backpressure
```

`BufferResponse` is the hot path. Its header carries:

```java
// NettyMessage.BufferResponse (header fields)
static class BufferResponse extends NettyMessage {
    static final byte ID = 0;

    // Header: receiverId(16) + seqNum(4) + backlog(4) + subpartId(4)
    //       + numPartialBufs(4) + dataType(1) + isCompressed(1) + bufSize(4)
    //       = 38 bytes of metadata
    static final int MESSAGE_HEADER_LENGTH = 38;

    final InputChannelID receiverId;  // which downstream channel
    final int sequenceNumber;          // for reordering detection
    final int backlog;                 // producer queue depth → drives credit requests
    final boolean isCompressed;
    final Buffer buffer;               // wraps the MemorySegment (direct memory)
}
```

The `buffer` field is a `NetworkBuffer` wrapping a `MemorySegment`. When Netty writes it to the
socket, the `MemorySegment`'s direct `ByteBuffer` is passed straight to the OS — **no heap copy**.

---

### 3.4 Server-side channel pipeline

```
Incoming TCP bytes
    │
    ▼
NettyMessageDecoder (LengthFieldBasedFrameDecoder subclass)
    │  reassembles frames, strips the 9-byte header
    │  decodes message type, dispatches to:
    ▼
PartitionRequestServerHandler
    │  handles PartitionRequest → creates CreditBasedSequenceNumberingViewReader
    │  handles AddCredit        → updates reader credit counter
    │  handles CancelPartitionRequest → removes reader
    ▼
PartitionRequestQueue
    │  maintains ArrayDeque<NetworkSequenceViewReader> of readers with data
    │  only writes when channel.isWritable() (Netty high-water-mark respected)
    ▼
channel.writeAndFlush(BufferResponse)
    │
    ▼
NettyMessageEncoder
    │  writes frame header ByteBuf (direct, from NettyBufferPool)
    │  appends data MemorySegment as a CompositeByteBuf component
    ▼
OS TCP send buffer
```

---

## 4. Java NIO vs Epoll

Flink uses Netty's event-loop abstraction, which supports two I/O polling mechanisms:

| Aspect | `NioEventLoopGroup` (Java NIO) | `EpollEventLoopGroup` (Linux epoll) |
|--------|-------------------------------|--------------------------------------|
| Syscall | `select()` / `poll()` | `epoll_wait()` |
| Complexity | O(n) — scans all registered fds each call | O(1) — kernel delivers only active fds |
| CPU @ 100k conns | Baseline | ~30–40% lower |
| Platform | Any JVM | Linux kernel ≥ 2.6 |
| Zero-copy support | Yes (off-heap ByteBuffer) | Yes (off-heap ByteBuffer) |
| `SO_REUSEPORT` | No | Yes (via `EpollChannelOption`) |

Flink selects automatically at runtime:

```java
// NettyClient.java
if (Epoll.isAvailable()) {
    // Linux: EpollIoHandler + EpollSocketChannel
    // TCP_KEEPIDLE / TCP_KEEPINTVL / TCP_KEEPCNT configurable
    // via EpollChannelOption (kernel-level, not JDK reflection)
    initEpollBootstrap();
} else {
    // macOS / Windows: NioIoHandler + NioSocketChannel
    // TCP keepalive via ExtendedSocketOptions (JDK 11+ reflection)
    initNioBootstrap();
}
```

In production Flink on Linux, Epoll is almost always active. The key win is at high connection
counts (many source partitions × many sink partitions): the O(1) epoll dispatch keeps I/O thread
CPU flat as parallelism scales up.

---

## 5. Credit-Based Flow Control

Credit-based flow control (FLINK-7282) is Flink's primary backpressure mechanism. It prevents a
fast producer from overwhelming a slow consumer's buffer pool.

### 5.1 Concept

```
  1 credit  =  1 available MemorySegment in the consumer's LocalBufferPool
```

- The producer **only sends a buffer when it holds at least 1 credit** from the consumer.
- Each sent `BufferResponse` **decrements the credit counter by 1**.
- The consumer **announces credits back** via `AddCredit` messages as buffers are recycled.
- The producer **piggybacks its backlog** (queue depth) on every `BufferResponse`, so the consumer
  can request additional floating buffers proactively.

### 5.2 Key classes

**Consumer side — `RemoteInputChannel`**:

```java
// RemoteInputChannel.java (credit-related fields)
public class RemoteInputChannel extends InputChannel {

    // Unique ID shared with the producer to multiplex over one TCP connection
    private final InputChannelID id = new InputChannelID();

    // Exclusive buffers pre-allocated at channel creation
    // → sent as initial credit in PartitionRequest
    private final int initialCredit;

    // Buffers recycled but not yet announced to producer
    // Batched to reduce AddCredit message count
    private final AtomicInteger unannouncedCredit = new AtomicInteger(0);

    // Incoming buffer queue (network I/O thread writes, task thread reads)
    private final PrioritizedDeque<SequenceBuffer> receivedBuffers = new PrioritizedDeque<>();
}
```

When a buffer is consumed and recycled, `unannouncedCredit` is incremented. The Netty I/O thread
periodically flushes `unannouncedCredit` as an `AddCredit` message to the producer.

**Producer side** — `CreditBasedSequenceNumberingViewReader` gates reads on available credit:

```java
// Pseudo-code of the credit gate in PartitionRequestQueue
while (reader.hasCredit() && subpartition.hasData()) {
    Buffer buffer = subpartition.getNextBuffer();
    reader.decrementCredit();
    channel.write(new BufferResponse(buffer, reader.getBacklog(), ...));
}
// channel.isWritable() also checked — respects Netty high-water-mark
```

### 5.3 Backpressure propagation

```
Consumer buffer pool full (LocalBufferPool exhausted)
    │
    RemoteInputChannel cannot get buffer
    │
    No AddCredit sent to producer
    │
    Producer credit = 0
    │
    PartitionRequestQueue stops reading from subpartition
    │
    PipelinedSubpartition queue grows
    │
    BufferBuilder.requestBuffer() blocks (operator thread)
    │
    Operator stops producing records
    │
    ← Backpressure propagated upstream through the job graph ←
```

This propagates within **one network round-trip** — far faster than TCP window-based backpressure
which requires multiple buffer-fulls of data to be in-flight before TCP itself slows down.

---

## 6. Performance Characteristics

### 6.1 GC impact: zero for network buffers

All `MemorySegment` instances backing network buffers live in **native direct memory** — outside
the JVM heap. The GC never touches them. On a busy pipeline processing 1 GB/s of data, this
eliminates hundreds of MB/s of objects that would otherwise cause GC pauses.

Heap memory for network buffers would mean:
- Every buffer allocation creates a short-lived `byte[]` object.
- A 32 KB buffer copied out of a JVM heap array before the OS can read it (kernel requires a
  stable address in native memory; the JVM can move heap objects during GC).
- G1/ZGC still needs to scan and compact the heap, causing stop-the-world pauses proportional
  to live heap size.

With direct memory:
- `ByteBuffer.allocateDirect()` → `malloc()` in the OS. The address is stable. No GC needed.
- The JVM holds only a tiny `Cleaner` reference; the bulk memory is invisible to GC.

### 6.2 Zero-copy on the send path

On the producer side, data flows from `MemorySegment` → TCP socket without any heap copy:

```
RecordSerializer writes → MemorySegment (direct, native address)
                                │
      (no copy: pass native address to OS)
                                │
                          OS TCP send buffer
                                │
                           Network wire
```

On the consumer side, Netty reads from the socket directly into a pre-allocated direct `ByteBuf`
(from `NettyBufferPool`), which is then wrapped as a `NetworkBuffer` and enqueued in
`RemoteInputChannel.receivedBuffers` — again, no heap copy.

### 6.3 Throughput vs. latency tradeoff

The **network buffer flush timeout** is the primary latency knob:

| Config | Default | Effect |
|--------|---------|--------|
| `pipeline.auto-watermark-interval` | — | unrelated |
| implicit flush timeout | 100 ms | batch records into buffers before sending |

A full buffer is flushed immediately. An unfull buffer is flushed after the timeout. Setting
timeout to 0 ms gives minimum latency (flush every record) but destroys throughput — each Netty
write has ~10–20 µs of syscall overhead.

### 6.4 Epoll CPU gain

On Linux, replacing `select()` with `epoll` reduces I/O-thread CPU by ~30–40% at high connection
counts. For a Flink job with 100 source partitions × 100 sinks = 10,000 logical channels over
a handful of TCP connections, the I/O thread still benefits because Netty multiplexes logical
channels over physical TCP connections — but even with fewer physical connections, the I/O thread
spends less time in kernel space with epoll.

### 6.5 Credit-based vs TCP backpressure latency

- **TCP window backpressure** kicks in only after multiple buffer-fulls (≥ 1 MB by default) pile
  up in the kernel send buffer. Propagation delay: seconds.
- **Credit-based** propagates within one RTT (producer stops after the first credit exhaustion).
  Propagation delay: sub-millisecond on a LAN.

---

## 7. Configuration Reference

```
network_memory = max(network.min, min(network.max, flink_total_memory × network.fraction))
total_buffers  = network_memory / segment_size
```

| Config key | Default | Notes |
|------------|---------|-------|
| `taskmanager.memory.network.fraction` | `0.1` | 10% of Flink-managed memory |
| `taskmanager.memory.network.min` | `64 MB` | Floor |
| `taskmanager.memory.network.max` | `1 GB` | Ceiling |
| `taskmanager.memory.segment-size` | `32 KB` | Buffer size; also Netty page granularity |
| `taskmanager.network.netty.num-arenas` | `2 × slots` | NettyBufferPool arenas |
| `taskmanager.network.netty.transport` | `auto` | `auto` / `nio` / `epoll` |
| `taskmanager.network.netty.num-threads.client` | `#cores` | Netty client event-loop threads |
| `taskmanager.network.netty.num-threads.server` | `#cores` | Netty server event-loop threads |
| `taskmanager.network.detailed-metrics` | `false` | Per-channel queue length metrics |
| `taskmanager.network.request-backoff.max` | `10 s` | Max wait for buffer availability |

**Sizing formula**:

```
Required buffers per subtask =
  (numInputChannels × buffersPerChannel)           # exclusive input buffers
  + floatingBuffersPerGate                          # shared input buffers
  + numOutputSubpartitions × maxBuffersPerChannel   # output buffers
```

**Common mistake**: `taskmanager.memory.network.max = 1 GB` is the default ceiling. With many
parallel subtasks and many channels, this can be exhausted, causing
`"Insufficient number of network buffers"`. Increase `network.max` or `network.fraction`.

**JVM flag**: Set `-XX:MaxDirectMemorySize` to at least `network_memory + metaspace + off-heap
state`. Without this, direct allocation silently triggers a full GC when the default limit
(`-Xmx` value) is reached.

---

## 8. Source Files Quick Reference

| Class | Path |
|-------|------|
| `MemorySegment` | `flink-core/src/main/java/org/apache/flink/core/memory/MemorySegment.java` |
| `NetworkBufferPool` | `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/NetworkBufferPool.java` |
| `LocalBufferPool` | `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/LocalBufferPool.java` |
| `NetworkBuffer` | `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/NetworkBuffer.java` |
| `BufferBuilder` | `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/BufferBuilder.java` |
| `NettyBufferPool` | `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/netty/NettyBufferPool.java` |
| `NettyClient` | `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/netty/NettyClient.java` |
| `NettyServer` | `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/netty/NettyServer.java` |
| `NettyMessage` | `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/netty/NettyMessage.java` |
| `RemoteInputChannel` | `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RemoteInputChannel.java` |
| `CreditBasedPartitionRequestClientHandler` | `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/netty/CreditBasedPartitionRequestClientHandler.java` |
| `PartitionRequestQueue` | `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/netty/PartitionRequestQueue.java` |
