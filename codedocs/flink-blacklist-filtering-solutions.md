# Flink Blacklist Filtering: Four Approaches for Large, Periodically-Updated ID Sets

This document compares approaches for filtering records from a Flink stream based on a blacklist of IDs that can range from thousands to billions, updated hourly from an external source (e.g., Redis). Each solution is analyzed with ASCII diagrams, code examples, memory estimations, and scale-specific recommendations.

---

# Part 1: Overview

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                       BLACKLIST FILTERING APPROACHES                                      │
│                                                                                          │
│  ┌─────────────────────┐  ┌──────────────────────┐  ┌─────────────────────────────────┐ │
│  │  Solution A:         │  │  Solution B:          │  │  Solution C:                    │ │
│  │  Async Redis Lookup  │  │  TM-Level Cache       │  │  Broadcast State                │ │
│  │                      │  │                       │  │                                 │ │
│  │  Per-record async    │  │  Static singleton     │  │  Flink-managed state via        │ │
│  │  query to Redis via  │  │  HashSet per TM,      │  │  broadcast stream,              │ │
│  │  AsyncDataStream     │  │  polled from Redis    │  │  checkpointed, no runtime       │ │
│  │                      │  │  periodically         │  │  external dependency            │ │
│  │  Simplest to build   │  │                       │  │                                 │ │
│  │  Highest latency     │  │  Best throughput      │  │  Most Flink-native              │ │
│  │                      │  │  1 copy per TM        │  │  1 copy per SUBTASK             │ │
│  │  Scale: ANY          │  │  Scale: < 100M        │  │  Scale: < 10M                   │ │
│  └─────────────────────┘  └──────────────────────┘  └─────────────────────────────────┘ │
│                                                                                          │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐│
│  │  Solution D: Bloom Filter + Redis Fallback                                           ││
│  │                                                                                      ││
│  │  Two-stage: Bloom filter pre-screen in memory (~2 bits/ID) + Redis only for          ││
│  │  Bloom-positive hits. Reduces Redis calls by 99%+. Scale: 100M – 1B+                 ││
│  └──────────────────────────────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# Part 2: Solution A — Async Redis Lookup

**Approach**: Every record triggers an async Redis `SISMEMBER` check. Non-blacklisted records pass through.

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  SOLUTION A: Async Redis Lookup                                                  │
│                                                                                  │
│  Kafka Source                                                                    │
│      │                                                                           │
│      ▼                                                                           │
│  ┌──────────────────────────────────────────────┐                                │
│  │  AsyncDataStream.unorderedWait(...)           │                                │
│  │                                               │                                │
│  │  BlacklistCheckAsyncFunction                  │                                │
│  │    ├─ open(): create Lettuce async client     │                                │
│  │    │          (connection pool)                │                                │
│  │    │                                          │     ┌──────────────────────┐   │
│  │    ├─ asyncInvoke(record, resultFuture):      │     │   Redis Cluster      │   │
│  │    │    redis.sismember("blacklist", id) ──────┼────►│                      │   │
│  │    │         │                                 │     │   SET: blacklist     │   │
│  │    │         ▼                                 │     │   (millions of IDs)  │   │
│  │    │    .whenComplete:                         │     └──────────────────────┘   │
│  │    │      if NOT member → collect(record)      │                                │
│  │    │      if member     → drop (collect nothing│)                               │
│  │    │                                           │                                │
│  │    ├─ close(): shutdown Redis client           │                                │
│  │    └───────────────────────────────────────────┘                                │
│  │                                                │                                │
│  └────────────────────────────────────────────────┘                                │
│      │                                                                             │
│      ▼                                                                             │
│  Downstream operators (only non-blacklisted records)                               │
└────────────────────────────────────────────────────────────────────────────────────┘
```

## Code Example

```java
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.api.common.functions.OpenContext;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class BlacklistAsyncRedisLookup {

    /**
     * Async function that checks each record's ID against a Redis SET.
     * Non-blacklisted records are collected; blacklisted records are dropped.
     */
    public static class BlacklistCheckAsyncFunction
            extends RichAsyncFunction<Event, Event> {

        private final String redisUri;
        private final String blacklistKey;

        private transient RedisClient redisClient;
        private transient StatefulRedisConnection<String, String> connection;
        private transient RedisAsyncCommands<String, String> asyncCommands;

        public BlacklistCheckAsyncFunction(String redisUri, String blacklistKey) {
            this.redisUri = redisUri;
            this.blacklistKey = blacklistKey;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            redisClient = RedisClient.create(redisUri);
            connection = redisClient.connect();
            asyncCommands = connection.async();
        }

        @Override
        public void asyncInvoke(Event event, ResultFuture<Event> resultFuture) {
            asyncCommands.sismember(blacklistKey, event.getId())
                    .whenComplete((isMember, throwable) -> {
                        if (throwable != null) {
                            resultFuture.completeExceptionally(throwable);
                        } else if (Boolean.TRUE.equals(isMember)) {
                            // Blacklisted — drop the record
                            resultFuture.complete(Collections.emptyList());
                        } else {
                            // Not blacklisted — pass through
                            resultFuture.complete(Collections.singletonList(event));
                        }
                    });
        }

        @Override
        public void close() throws Exception {
            if (connection != null) connection.close();
            if (redisClient != null) redisClient.shutdown();
        }
    }

    // Wiring in main():
    // DataStream<Event> filtered = AsyncDataStream.unorderedWait(
    //     inputStream,
    //     new BlacklistCheckAsyncFunction("redis://localhost:6379", "blacklist"),
    //     5000,                    // timeout per request (ms)
    //     TimeUnit.MILLISECONDS,
    //     100                      // max concurrent async requests
    // );
}
```

## `orderedWait` vs `unorderedWait`

Flink's `AsyncDataStream` offers two modes that control how results are emitted relative to the input order:

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  orderedWait:                                                                    │
│                                                                                  │
│  Input:   r1     r2     r3     r4     r5                                        │
│  Redis:   3ms    50ms   1ms    2ms    1ms                                       │
│           ┌──────────────────────────────┐                                      │
│           │  Internal buffer             │                                      │
│           │  r1 ✓  r2 ⏳  r3 ✓  r4 ✓  r5 ✓ │  ← all must wait for r2 (slowest) │
│           └──────────────────────────────┘                                      │
│  Output:  r1     r2     r3     r4     r5    (same order as input)               │
│  Effective latency: max(all pending) = 50ms                                     │
│                                                                                  │
├──────────────────────────────────────────────────────────────────────────────────┤
│  unorderedWait:                                                                  │
│                                                                                  │
│  Input:   r1     r2     r3     r4     r5                                        │
│  Redis:   3ms    50ms   1ms    2ms    1ms                                       │
│                                                                                  │
│  Output:  r3     r5     r4     r1     r2    (emitted as each completes)          │
│  Effective latency: each record's own Redis time                                 │
│                                                                                  │
│  With EVENT TIME: watermarks act as barriers                                     │
│                                                                                  │
│     ──[r3 r5 r4 r1]──WM──[r8 r6 r7]──WM──                                      │
│                                                                                  │
│  Records before a watermark are emitted before records after it,                 │
│  but WITHIN a watermark interval, order is arbitrary.                            │
│  This preserves event-time correctness for downstream windows.                   │
└──────────────────────────────────────────────────────────────────────────────────┘
```

| Aspect | `orderedWait` | `unorderedWait` |
|--------|---------------|-----------------|
| **Output order** | Same as input | Completion order (with WM barriers for event time) |
| **Latency** | Bottlenecked by slowest in-flight request | Each record emitted as soon as ready |
| **Throughput** | Lower — buffer grows waiting for slow requests | Higher — no head-of-line blocking |
| **Checkpoint overhead** | Higher — buffers results longer in state | Lower — results flushed sooner |
| **Watermark handling** | Preserves order naturally | Watermarks act as order boundaries; within a WM interval, order is arbitrary |
| **Use case** | Event-time windows where exact per-record order matters, pattern matching, ordered sinks | **Blacklist filtering** (order doesn't matter), enrichment where downstream is order-agnostic |

**Recommendation for blacklist filtering**: Use **`unorderedWait`**. Filtering is an order-agnostic operation — it doesn't matter which record gets checked first. `unorderedWait` gives better throughput and lower latency. If downstream operators need event-time ordering (e.g., windows), a subsequent `keyBy` + window will re-establish order via watermarks.

### Example: `unorderedWait` + Downstream Event-Time Window

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  WHY unorderedWait IS SAFE BEFORE EVENT-TIME WINDOWS                             │
│                                                                                  │
│  Source (with event-time timestamps and watermarks)                               │
│    │                                                                             │
│    ▼                                                                             │
│  AsyncDataStream.unorderedWait(blacklistCheck)                                   │
│    │                                                                             │
│    │  Output may be out of order WITHIN a watermark interval:                    │
│    │  ──[r3(t=5) r1(t=2) r5(t=8) r4(t=7)]──WM(t=9)──[r8(t=12) r6(t=10)]──     │
│    │                                                                             │
│    │  But watermarks STILL flow correctly:                                        │
│    │  • WM(t=9) guarantees: no future record has t < 9                           │
│    │  • Records r3,r1,r5,r4 all have t < 9 → all emitted BEFORE WM(t=9)         │
│    │  • This is all a window needs to know!                                       │
│    │                                                                             │
│    ▼                                                                             │
│  .keyBy(event -> event.getUserId())                                              │
│    │                                                                             │
│    ▼                                                                             │
│  .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))                     │
│    │                                                                             │
│    │  Window [0:00, 1:00) collects all records with 0 ≤ t < 60000               │
│    │  Window fires when WM crosses 60000                                         │
│    │  It doesn't care what ORDER records arrived — only their TIMESTAMPS          │
│    │                                                                             │
│    ▼                                                                             │
│  .aggregate(countFunction)  →  correct result regardless of arrival order        │
└──────────────────────────────────────────────────────────────────────────────────┘
```

```java
// Full pipeline: Kafka → blacklist filter (unordered async) → event-time window

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 1. Source with event-time timestamps
DataStream<Event> events = env
    .fromSource(kafkaSource, WatermarkStrategy
        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event, ts) -> event.getTimestamp()),
        "kafka-source");

// 2. Blacklist filter — unorderedWait for maximum throughput
//    Records come out of order, but watermarks are preserved as barriers.
DataStream<Event> filtered = AsyncDataStream.unorderedWait(
    events,
    new BlacklistCheckAsyncFunction("redis://localhost:6379", "blacklist"),
    5000,                    // timeout: 5 seconds per request
    TimeUnit.MILLISECONDS,
    100                      // capacity: 100 concurrent in-flight requests
);

// 3. Event-time window — works correctly despite unordered input
//    The window collects records by their EVENT TIMESTAMPS, not arrival order.
//    Watermarks (preserved by unorderedWait) trigger window evaluation.
DataStream<Result> results = filtered
    .keyBy(Event::getUserId)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .aggregate(new CountAggregateFunction(), new WindowResultFunction());

results.print();

// WHY THIS WORKS:
// - unorderedWait may emit r3(t=5s) before r1(t=2s)
// - But both have timestamps in window [0:00, 1:00)
// - The window collects BOTH regardless of arrival order
// - Window fires when watermark crosses 1:00 (60000ms)
// - Result is the same as if records arrived in order
//
// WHEN IT WOULDN'T WORK:
// - CEP pattern matching (expects strict per-key order)
// - ProcessFunction that compares consecutive records
// - Writing to an ordered sink (e.g., append-only log where order matters)
// For these cases, use orderedWait instead.
```

## Failure, Restart, and Exactly-Once Guarantees

Async I/O provides **exactly-once** fault tolerance. Here's how it works:

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  ASYNC I/O CHECKPOINT AND RECOVERY                                               │
│                                                                                  │
│  TIME ──────────────────────────────────────────────────────────────────────►    │
│                                                                                  │
│  r1  r2  r3  r4  r5  │ CHECKPOINT N │  r6  r7  r8  ✗ CRASH                     │
│                       │              │                                           │
│  ┌────────────────────┤              │                                           │
│  │ In-flight requests:│              │                                           │
│  │  r3 → Redis ⏳     │              │  ← CP waits for all pending to complete   │
│  │  r4 → Redis ⏳     │              │                                           │
│  │  r5 → Redis ⏳     │              │                                           │
│  └────────────────────┤              │                                           │
│                       │ All done ✓   │                                           │
│                       │ Snapshot OK  │                                           │
│                       │──────────────│                                           │
│                                       r6  r7  r8  ✗ CRASH                       │
│                                                                                  │
│  RESTART from Checkpoint N:                                                      │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │ 1. Flink restores source offset from checkpoint N                       │    │
│  │ 2. r1-r5 results already committed — NOT replayed                       │    │
│  │ 3. r6, r7, r8 were NOT in checkpoint N → replayed from source           │    │
│  │ 4. Async operator re-triggers Redis lookups for r6, r7, r8             │    │
│  │ 5. No duplicates, no data loss → exactly-once                           │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│  KEY BEHAVIORS:                                                                  │
│                                                                                  │
│  • Checkpoint BLOCKS until all in-flight requests complete or timeout            │
│  • If a request hangs → checkpoint timeout → checkpoint failure                  │
│  • On recovery, requests are RE-TRIGGERED (Redis SISMEMBER is read-only,        │
│    so re-execution is safe/idempotent)                                           │
│  • The "capacity" parameter (max concurrent requests) limits how many            │
│    requests can be in-flight simultaneously — also limits checkpoint delay        │
│                                                                                  │
│  IMPORTANT SETTINGS:                                                             │
│  • timeout (in orderedWait/unorderedWait): max time per async request            │
│    → set this shorter than checkpoint interval to prevent CP failure              │
│  • capacity: max concurrent in-flight requests per operator instance             │
│    → higher = more throughput but more checkpoint state                           │
│  • AsyncRetryStrategy (FLIP-232): built-in retry with backoff                    │
│    → configure for transient Redis errors without failing the job                 │
└──────────────────────────────────────────────────────────────────────────────────┘
```

**Practical tips:**
- Set async `timeout` < checkpoint interval. If checkpoint interval is 60s, set timeout to 30s.
- If Redis is temporarily down, all in-flight requests will timeout → records fail → job may restart. Consider using `AsyncRetryStrategy` for transient failures.
- The `capacity` parameter (e.g., 100) controls backpressure: if 100 requests are pending, the operator stops consuming from upstream until slots free up.

## Pros and Cons

| Aspect | Detail |
|--------|--------|
| **Pros** | Simplest code; always reads latest blacklist (zero staleness); zero TM memory overhead; scales horizontally with Redis Cluster; works for ANY blacklist size; exactly-once via checkpointed in-flight requests |
| **Cons** | Redis on critical path — every record = 1 network round-trip (~1-5ms); Redis must handle QPS = stream throughput; network failure = job failure; connection pool sizing and tuning required; pending requests block checkpoints |
| **When to use** | Low-to-medium throughput (<50K records/sec), or when blacklist changes very frequently (sub-minute), or when blacklist is too large for any in-memory solution (>1B IDs) |

---

# Part 3: Solution B — TaskManager-Level In-Memory Cache

**Approach**: A static singleton `HashSet` is shared across all subtasks on the same TaskManager JVM. One background thread polls Redis periodically to refresh the set via atomic volatile swap.

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│  SOLUTION B: TaskManager-Level In-Memory Cache                                       │
│                                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐ │
│  │  TaskManager JVM                                                                │ │
│  │                                                                                 │ │
│  │  ┌───────────────────────────────────────────────────────────────────┐          │ │
│  │  │  BlacklistCache (static singleton)                                │          │ │
│  │  │                                                                   │          │ │
│  │  │  static volatile Set<String> blacklist = emptySet()               │          │ │
│  │  │  static volatile long lastRefreshTs = 0                           │          │ │
│  │  │  static final ReentrantLock refreshLock = new ReentrantLock()      │          │ │
│  │  │                                                                   │          │ │
│  │  │  refreshIfNeeded(intervalMs):                             ┌──────────────┐  │ │
│  │  │    if (now - lastRefreshTs > intervalMs):                  │ Redis / DB   │  │ │
│  │  │      if refreshLock.tryLock():                             │              │  │ │
│  │  │        newSet = HashSet()                                  │ SCAN cursor  │  │ │
│  │  │        redis.scan("blacklist") ──────────────────────────► │ (not SMEMBERS│  │ │
│  │  │        blacklist = newSet  ← atomic volatile swap          │  to avoid    │  │ │
│  │  │        lastRefreshTs = now                                 │  blocking)   │  │ │
│  │  │        refreshLock.unlock()                                └──────────────┘  │ │
│  │  │                                                                   │          │ │
│  │  │  contains(id): return blacklist.contains(id)  ← O(1) HashSet     │          │ │
│  │  └───────────────────────────────────────────────────────────────────┘          │ │
│  │       ▲            ▲            ▲            ▲                                  │ │
│  │       │            │            │            │   all share SAME JVM singleton    │ │
│  │  ┌────┴───┐   ┌────┴───┐   ┌────┴───┐   ┌────┴───┐                            │ │
│  │  │Subtask │   │Subtask │   │Subtask │   │Subtask │                             │ │
│  │  │  0     │   │  1     │   │  2     │   │  3     │                             │ │
│  │  │ filter │   │ filter │   │ filter │   │ filter │                             │ │
│  │  └────┬───┘   └────┬───┘   └────┬───┘   └────┬───┘                            │ │
│  └───────┼────────────┼────────────┼────────────┼────────────────────────────────┘ │
│          ▼            ▼            ▼            ▼                                   │
│  Only non-blacklisted records pass through                                          │
│                                                                                      │
│  KEY INSIGHT: 1 copy per TaskManager, NOT per subtask.                               │
│  If 4 subtasks run on 1 TM, they all read the same HashSet.                          │
│  This is 4× more memory-efficient than broadcast state.                              │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

## Why Volatile Reference Swap (Not ConcurrentHashMap Mutation)

```
Thread Safety with Volatile Reference Swap:

  Writer thread (refresh):                   Reader threads (subtasks):
  ┌──────────────────────────┐               ┌──────────────────────────┐
  │ 1. Build new HashSet     │               │ Set<String> s = blacklist│
  │    completely in private  │               │ s.contains(id)           │
  │    variable               │               │                          │
  │ 2. blacklist = newSet     │               │ Always sees either OLD   │
  │    (single volatile write)│               │ complete set or NEW      │
  │                           │               │ complete set — NEVER a   │
  │ Old set becomes garbage   │               │ partial/inconsistent     │
  │ collected when no readers │               │ state.                   │
  └──────────────────────────┘               └──────────────────────────┘

  vs. ConcurrentHashMap mutation (BAD):
  ┌──────────────────────────┐
  │ map.clear()  ← readers see empty set momentarily!
  │ map.putAll(newData)  ← readers see partial set during putAll!
  └──────────────────────────┘
```

## Code Example

```java
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFilterFunction;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Static singleton cache shared across all subtasks on the same TaskManager JVM.
 * Only ONE copy of the blacklist exists per TM, regardless of how many subtasks run.
 */
public class BlacklistCache {

    /** The current blacklist. Volatile ensures visibility across threads. */
    private static volatile Set<String> currentBlacklist = Collections.emptySet();
    private static volatile long lastRefreshTimestamp = 0;
    private static final ReentrantLock REFRESH_LOCK = new ReentrantLock();

    /** Default refresh interval: 1 hour. */
    private static final long DEFAULT_REFRESH_INTERVAL_MS = 3_600_000L;

    /**
     * Refreshes the blacklist from Redis if the interval has elapsed.
     * Uses tryLock() so only one thread refreshes; others use stale data
     * rather than blocking — no impact on processing latency.
     */
    public static void refreshIfNeeded(long refreshIntervalMs) {
        long now = System.currentTimeMillis();
        if (now - lastRefreshTimestamp < refreshIntervalMs) {
            return; // Still fresh
        }

        if (REFRESH_LOCK.tryLock()) {
            try {
                // Double-check after acquiring lock
                if (now - lastRefreshTimestamp < refreshIntervalMs) {
                    return;
                }

                // ---- Replace with real Redis SCAN ----
                // RedisClient client = RedisClient.create("redis://localhost:6379");
                // StatefulRedisConnection<String, String> conn = client.connect();
                // RedisCommands<String, String> commands = conn.sync();
                //
                // Set<String> newSet = new HashSet<>();
                // ScanArgs args = ScanArgs.Builder.limit(10000);
                // ScanCursor cursor = ScanCursor.INITIAL;
                // do {
                //     ValueScanCursor<String> result = commands.sscan("blacklist", cursor, args);
                //     newSet.addAll(result.getValues());
                //     cursor = result;
                // } while (!cursor.isFinished());
                //
                // conn.close();
                // client.shutdown();
                // ---- End Redis SCAN ----

                Set<String> newSet = new HashSet<>(); // placeholder

                // Atomic swap — readers see old or new, never partial
                currentBlacklist = Collections.unmodifiableSet(newSet);
                lastRefreshTimestamp = System.currentTimeMillis();

                System.out.printf("[BlacklistCache] Refreshed: %d IDs loaded%n", newSet.size());
            } finally {
                REFRESH_LOCK.unlock();
            }
        }
        // If tryLock() fails, another thread is refreshing — use current (slightly stale) data
    }

    public static boolean contains(String id) {
        return currentBlacklist.contains(id);
    }

    public static int size() {
        return currentBlacklist.size();
    }
}

/**
 * Filter function that drops blacklisted events.
 * Calls BlacklistCache.refreshIfNeeded() on each record to check if a refresh is due.
 * The actual refresh only happens once per interval per TM.
 */
public class BlacklistFilterFunction extends RichFilterFunction<Event> {

    private final long refreshIntervalMs;

    public BlacklistFilterFunction(long refreshIntervalMs) {
        this.refreshIntervalMs = refreshIntervalMs;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Eagerly load on startup so the first records are filtered correctly
        BlacklistCache.refreshIfNeeded(0);
    }

    @Override
    public boolean filter(Event event) throws Exception {
        BlacklistCache.refreshIfNeeded(refreshIntervalMs);
        return !BlacklistCache.contains(event.getId());
    }
}

// Wiring in main():
// DataStream<Event> filtered = inputStream
//     .filter(new BlacklistFilterFunction(3_600_000L))  // refresh every hour
//     .name("blacklist-filter");
```

## Why Redis Polling Instead of ZooKeeper

| Aspect | Redis Polling | ZooKeeper Watch |
|--------|---------------|-----------------|
| **Complexity** | Simple SCAN loop | Need ZK client, watcher registration, reconnect handling |
| **Scalability** | Redis Cluster scales horizontally | ZK is a coordination service, not a data store — struggles with large data |
| **Data storage** | Millions of IDs in a Redis SET is normal | ZK node size limit is 1MB by default — can't store millions of IDs |
| **Failure mode** | Graceful: use stale cache | ZK session expiry triggers complex reconnection logic |
| **Latency** | Up to 1 hour stale (configurable) | Near-real-time via watch notifications |
| **Recommendation** | **Use this** for hourly updates | Only if sub-second freshness is critical |

## Pros and Cons

| Aspect | Detail |
|--------|--------|
| **Pros** | O(1) HashSet lookup; no per-record network call; **1 copy per TM** (not per subtask); tolerates Redis downtime (stale cache); simple to understand |
| **Cons** | Not Flink-managed state (not checkpointed — on restart, re-loads from Redis); memory footprint on TM heap; static singleton is harder to unit test; refresh lag up to 1 hour |
| **When to use** | High throughput (>100K records/sec); blacklist fits in TM memory (up to ~10GB = ~100M IDs); hourly staleness is acceptable |

---

# Part 4: Solution C — Broadcast State (Most Flink-Native)

**Approach**: A custom source polls Redis periodically and emits the full blacklist as a broadcast stream. Each subtask receives a copy and stores it in Flink-managed broadcast state (checkpointed, fault-tolerant).

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│  SOLUTION C: Broadcast State                                                         │
│                                                                                      │
│  ┌──────────────────────────────┐                                                   │
│  │  BlacklistSource              │  (parallelism = 1, polls Redis every hour)        │
│  │                               │                                                   │
│  │  run():                       │                                                   │
│  │    while (running):           │      ┌──────────────────┐                        │
│  │      ids = redis.scan() ──────┼─────►│  Redis / DB      │                        │
│  │      emit BlacklistUpdate     │      └──────────────────┘                        │
│  │        { Set<String> ids }    │                                                   │
│  │      sleep(1 hour)            │                                                   │
│  └────────────┬──────────────────┘                                                   │
│               │                                                                      │
│               ▼                                                                      │
│        .broadcast(descriptor)    ← MapStateDescriptor<String, Set<String>>           │
│               │                                                                      │
│     ┌─────────┼─────────┬──────────────┐                                            │
│     ▼         ▼         ▼              ▼                                             │
│  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────────┐                                        │
│  │Sub 0 │  │Sub 1 │  │Sub 2 │  │Sub 3     │  each subtask gets FULL copy            │
│  │      │  │      │  │      │  │          │                                          │
│  │ Broadcast State (MapState)  │          │  processBroadcastElement():              │
│  │ key="bl" → Set<String>     │          │    clear old state                        │
│  │      │  │      │  │      │  │          │    put new set into broadcast state       │
│  │      │  │      │  │      │  │          │                                          │
│  │ processElement():           │          │  processElement():                        │
│  │   state = getBroadcastState │          │    if id NOT in state → collect(record)   │
│  │   if id NOT in state:       │          │                                          │
│  │     collect(record)         │          │                                          │
│  └──┬───┘  └──┬───┘  └──┬───┘  └──┬──────┘                                         │
│     ▼         ▼         ▼         ▼                                                  │
│  Non-blacklisted records continue downstream                                         │
│                                                                                      │
│  IMPORTANT: Broadcast state is HEAP-ONLY (no RocksDB backend).                       │
│  Checkpoint size = blacklist_size × parallelism (every subtask checkpoints its copy). │
│  With 10M IDs × 100 subtasks = 100GB+ checkpoint → UNACCEPTABLE.                     │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

## Code Example

See the standalone runnable example in:
`flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/BlacklistFilterBroadcastState.java`

Core wiring:

```java
// 1. Main data stream
DataStream<Event> events = env.addSource(kafkaSource);

// 2. Blacklist source (parallelism=1 — only one task polls Redis)
DataStream<BlacklistUpdate> blacklistStream = env
    .addSource(new BlacklistSource("redis://localhost:6379", 3_600_000L))
    .setParallelism(1)
    .name("blacklist-source");

// 3. Broadcast state descriptor
MapStateDescriptor<String, Set<String>> descriptor = new MapStateDescriptor<>(
    "blacklist-state",
    Types.STRING,
    TypeInformation.of(new TypeHint<Set<String>>() {})
);

// 4. Create broadcast stream and connect
BroadcastStream<BlacklistUpdate> broadcastBlacklist = blacklistStream.broadcast(descriptor);

DataStream<Event> filtered = events
    .connect(broadcastBlacklist)
    .process(new BlacklistFilterBroadcastFunction(descriptor))
    .name("blacklist-filter");

filtered.print();
```

## Fail-Open Behavior

Before the first broadcast arrives, the broadcast state is empty — all records pass through (fail-open). Mitigation: the `BlacklistSource` emits immediately on startup before sleeping, so the window of unfiltered records is minimal (just the time to SCAN Redis).

## Pros and Cons

| Aspect | Detail |
|--------|--------|
| **Pros** | Fully Flink-managed (checkpointed, restored on failover); no runtime Redis dependency (only source polls it); fault-tolerant; clean API |
| **Cons** | **N copies** of set (1 per subtask, NOT per TM); broadcast state is **heap-only** (no RocksDB); checkpoint size = data × parallelism; not designed for large datasets |
| **When to use** | Blacklist < 10M IDs; parallelism < 20; want native Flink fault tolerance; hourly refresh acceptable |

---

# Part 5: Solution D — Bloom Filter + Redis Fallback (For 100M+ Scale)

**Approach**: A Bloom filter pre-screens all records in-memory (~2 bits per ID). Records that test positive (possibly blacklisted) are verified against Redis. Records that test negative (definitely not blacklisted) pass through immediately. This reduces Redis calls by 99%+.

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│  SOLUTION D: Bloom Filter + Redis Fallback                                           │
│                                                                                      │
│  Kafka Source                                                                        │
│      │                                                                               │
│      ▼                                                                               │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐│
│  │  Stage 1: Bloom Filter Pre-Screen (in-memory, per TM)                            ││
│  │                                                                                  ││
│  │  record.getId() ──► bloomFilter.mightContain(id)                                 ││
│  │                         │                    │                                    ││
│  │                    FALSE (definite)      TRUE (maybe)                             ││
│  │                         │                    │                                    ││
│  │                         ▼                    ▼                                    ││
│  │              PASS THROUGH             ┌─────────────────────────────────────┐     ││
│  │              (99%+ of records)        │ Stage 2: Redis Verification         │     ││
│  │                                       │                                     │     ││
│  │                                       │ redis.sismember("blacklist", id)    │     ││
│  │                                       │      │                  │           │     ││
│  │                                       │   NOT MEMBER         MEMBER        │     ││
│  │                                       │   (false positive)   (blacklisted) │     ││
│  │                                       │      │                  │           │     ││
│  │                                       │   PASS THROUGH        DROP         │     ││
│  │                                       └─────────────────────────────────────┘     ││
│  └──────────────────────────────────────────────────────────────────────────────────┘│
│      │                                                                               │
│      ▼                                                                               │
│  Downstream operators                                                                │
│                                                                                      │
│  Memory footprint (Bloom filter with 1% false positive rate):                        │
│    100M IDs → ~120 MB        (vs. ~11 GB for HashSet)                                │
│    1B IDs   → ~1.2 GB        (vs. ~112 GB for HashSet — IMPOSSIBLE)                  │
│                                                                                      │
│  Redis QPS reduction:                                                                │
│    With 1% FPR: only 1% of non-blacklisted records hit Redis                         │
│    If 5% of traffic is blacklisted: Redis sees ~6% of total traffic (vs. 100%)       │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

## Code Example (Conceptual)

```java
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**
 * Two-stage blacklist filter:
 *   Stage 1: Bloom filter (in-memory, O(k) where k = number of hash functions)
 *            → FALSE = definitely NOT blacklisted → pass through immediately
 *   Stage 2: Redis verification (only for Bloom-positive hits)
 *            → removes false positives with authoritative check
 *
 * The Bloom filter is rebuilt from Redis periodically (e.g., hourly) using the
 * same static singleton pattern as Solution B (BlacklistCache).
 */
public class BloomFilterRedisFunction extends RichAsyncFunction<Event, Event> {

    private static volatile BloomFilter<CharSequence> bloomFilter;
    private static volatile long lastRebuildTs = 0;
    private static final long REBUILD_INTERVAL_MS = 3_600_000L;

    private transient RedisAsyncCommands<String, String> asyncCommands;

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Initialize Redis async client (same as Solution A)
        // ...

        // Build Bloom filter on startup
        rebuildBloomFilterIfNeeded();
    }

    private static synchronized void rebuildBloomFilterIfNeeded() {
        long now = System.currentTimeMillis();
        if (now - lastRebuildTs < REBUILD_INTERVAL_MS) return;

        // Scan Redis and build Bloom filter
        // long expectedInsertions = redis.scard("blacklist");
        long expectedInsertions = 100_000_000L; // placeholder
        double fpp = 0.01; // 1% false positive probability

        BloomFilter<CharSequence> newFilter = BloomFilter.create(
            Funnels.stringFunnel(StandardCharsets.UTF_8),
            expectedInsertions,
            fpp
        );

        // Scan and add all IDs
        // ScanCursor cursor = ScanCursor.INITIAL;
        // do {
        //     ValueScanCursor<String> result = commands.sscan("blacklist", cursor);
        //     result.getValues().forEach(newFilter::put);
        //     cursor = result;
        // } while (!cursor.isFinished());

        bloomFilter = newFilter; // atomic swap
        lastRebuildTs = now;
    }

    @Override
    public void asyncInvoke(Event event, ResultFuture<Event> resultFuture) {
        rebuildBloomFilterIfNeeded();

        // Stage 1: Bloom filter check (in-memory, nanoseconds)
        if (!bloomFilter.mightContain(event.getId())) {
            // Definitely NOT in blacklist — pass through immediately, no Redis call
            resultFuture.complete(Collections.singletonList(event));
            return;
        }

        // Stage 2: Bloom says "maybe" — verify against Redis
        asyncCommands.sismember("blacklist", event.getId())
            .whenComplete((isMember, error) -> {
                if (error != null) {
                    resultFuture.completeExceptionally(error);
                } else if (Boolean.TRUE.equals(isMember)) {
                    resultFuture.complete(Collections.emptyList()); // blacklisted
                } else {
                    // False positive from Bloom filter — not actually blacklisted
                    resultFuture.complete(Collections.singletonList(event));
                }
            });
    }
}
```

## Bloom Filter Sizing Formula

```
Optimal number of bits (m) = -n * ln(p) / (ln(2))^2
Optimal number of hash functions (k) = (m/n) * ln(2)

Where:
  n = expected number of elements
  p = desired false positive probability

Examples:
  n = 100M, p = 0.01  → m = 958M bits = ~120 MB,   k = 7
  n = 1B,   p = 0.01  → m = 9.58B bits = ~1.2 GB,   k = 7
  n = 1B,   p = 0.001 → m = 14.4B bits = ~1.8 GB,   k = 10
```

## Pros and Cons

| Aspect | Detail |
|--------|--------|
| **Pros** | Dramatically lower memory (~2 bits/ID vs ~112 bytes/ID in HashSet); reduces Redis QPS by 99%+; works at billion scale; combines fast local check with authoritative remote check |
| **Cons** | False positives (1% with default FPR) still hit Redis; cannot remove individual IDs without full rebuild; more complex code (two-stage logic); Bloom filter rebuild requires full Redis scan |
| **When to use** | Blacklist > 100M IDs; need to reduce Redis load while maintaining correctness; acceptable to have ~1% of clean traffic hit Redis |

---

# Part 6: RoaringBitmap Deep Dive (For Numeric IDs)

If your blacklist IDs are **numeric** (int or long user IDs), RoaringBitmap is dramatically more memory-efficient than HashSet and provides **zero false positives** (unlike Bloom filter). It's used in production by Apache Spark, Apache Pinot, Netflix Atlas, and Apache Druid.

## How RoaringBitmap Works

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  RoaringBitmap Internal Structure                                                │
│                                                                                  │
│  A 32-bit integer is split into two 16-bit halves:                               │
│                                                                                  │
│    integer = [upper 16 bits : chunk key] [lower 16 bits : container value]       │
│                                                                                  │
│  Top level: sorted array of (chunk_key, container) pairs                         │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │  key=0x0000  → Container for integers 0x00000000 – 0x0000FFFF (0–65535)   │ │
│  │  key=0x0001  → Container for integers 0x00010000 – 0x0001FFFF             │ │
│  │  key=0x0042  → Container for integers 0x00420000 – 0x0042FFFF             │ │
│  │  ...         (only populated chunks exist — sparse ranges use no memory)   │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                  │
│  Each container holds up to 65536 values and auto-selects the most compact       │
│  representation:                                                                 │
│                                                                                  │
│  ┌─────────────────────┐ ┌─────────────────────┐ ┌──────────────────────────┐   │
│  │  ARRAY CONTAINER     │ │  BITMAP CONTAINER    │ │  RUN CONTAINER           │   │
│  │  (sparse: < 4096)    │ │  (dense: ≥ 4096)     │ │  (consecutive ranges)    │   │
│  │                      │ │                      │ │                          │   │
│  │  Sorted array of     │ │  Uncompressed bit    │ │  List of [start, length] │   │
│  │  16-bit values       │ │  vector: 8 KB fixed  │ │  pairs                   │   │
│  │                      │ │                      │ │                          │   │
│  │  [3, 17, 42, 100]    │ │  [01101001...01101]  │ │  [3→17, 42→100, ...]     │   │
│  │                      │ │                      │ │                          │   │
│  │  Memory: 2 bytes     │ │  Memory: 8 KB        │ │  Memory: 4 bytes         │   │
│  │  per value            │ │  regardless of count │ │  per run                 │   │
│  │                      │ │                      │ │                          │   │
│  │  Best for: scattered │ │  Best for: many IDs  │ │  Best for: ranges like   │   │
│  │  individual IDs      │ │  in a 65K chunk      │ │  1000000–2000000         │   │
│  └─────────────────────┘ └─────────────────────┘ └──────────────────────────┘   │
│                                                                                  │
│  The container type is chosen AUTOMATICALLY based on cardinality.                │
│  Crossover point: 4096 values → switches from array to bitmap.                   │
│  Runs are detected via runOptimize() or addRange().                              │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## Java Code Example

```java
// Maven dependency:
//   <groupId>org.roaringbitmap</groupId>
//   <artifactId>RoaringBitmap</artifactId>
//   <version>1.0.1</version>

// =============================================
// 32-bit integer IDs (int user IDs, 0 to 2^31)
// =============================================
import org.roaringbitmap.RoaringBitmap;

RoaringBitmap blacklist = new RoaringBitmap();

// Add individual IDs
blacklist.add(12345);
blacklist.add(67890);

// Add a contiguous range [1_000_000, 2_000_000) — extremely compact as a RUN container
blacklist.addRange(1_000_000L, 2_000_000L);

// Membership check — O(log n) on chunks, O(1) or O(log n) within container
blacklist.contains(12345);       // true
blacklist.contains(99999);       // false
blacklist.contains(1_500_000);   // true (in range)

// Stats
blacklist.getLongCardinality();   // number of elements (1_000_002)
blacklist.getSizeInBytes();      // serialized size (~12 bytes for the range!)

// Set operations — SIMD-accelerated, much faster than HashSet
RoaringBitmap newBlacklist = RoaringBitmap.bitmapOf(12345, 99999, 3_000_000);
RoaringBitmap added   = RoaringBitmap.andNot(newBlacklist, blacklist); // IDs to add
RoaringBitmap removed = RoaringBitmap.andNot(blacklist, newBlacklist); // IDs to remove

// Optimize after bulk insertion (detect runs)
blacklist.runOptimize();

// =============================================
// 64-bit long IDs (long user IDs)
// =============================================
import org.roaringbitmap.longlong.Roaring64NavigableMap;

Roaring64NavigableMap blacklist64 = new Roaring64NavigableMap();
blacklist64.addLong(123_456_789_012L);
blacklist64.addLong(987_654_321_098L);
blacklist64.contains(123_456_789_012L);   // true

// Internally: splits 64-bit longs into upper-32 (red-black tree key)
// and lower-32 (standard RoaringBitmap per tree node)

// Serialize for checkpoint or network transfer
ByteArrayOutputStream baos = new ByteArrayOutputStream();
DataOutputStream dos = new DataOutputStream(baos);
blacklist64.serialize(dos);
byte[] bytes = baos.toByteArray();  // compact serialized form

// Deserialize
Roaring64NavigableMap restored = new Roaring64NavigableMap();
restored.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
```

## Using RoaringBitmap in Flink (TM-Level Cache Pattern)

```java
/**
 * Same pattern as Solution B (static singleton), but using RoaringBitmap
 * instead of HashSet for dramatically lower memory with numeric IDs.
 */
public class RoaringBlacklistCache {

    private static volatile Roaring64NavigableMap currentBlacklist = new Roaring64NavigableMap();
    private static volatile long lastRefreshTimestamp = 0;
    private static final ReentrantLock REFRESH_LOCK = new ReentrantLock();

    public static void refreshIfNeeded(long refreshIntervalMs) {
        long now = System.currentTimeMillis();
        if (now - lastRefreshTimestamp < refreshIntervalMs) return;

        if (REFRESH_LOCK.tryLock()) {
            try {
                if (now - lastRefreshTimestamp < refreshIntervalMs) return;

                Roaring64NavigableMap newBitmap = new Roaring64NavigableMap();

                // Load from Redis — IDs stored as strings, parsed to long
                // ScanCursor cursor = ScanCursor.INITIAL;
                // do {
                //     ValueScanCursor<String> result = commands.sscan("blacklist", cursor);
                //     for (String idStr : result.getValues()) {
                //         newBitmap.addLong(Long.parseLong(idStr));
                //     }
                //     cursor = result;
                // } while (!cursor.isFinished());

                newBitmap.runOptimize();  // Detect and compress runs

                currentBlacklist = newBitmap;  // Atomic volatile swap
                lastRefreshTimestamp = now;
            } finally {
                REFRESH_LOCK.unlock();
            }
        }
    }

    public static boolean contains(long id) {
        return currentBlacklist.contains(id);
    }
}
```

## Memory Comparison (Numeric IDs)

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  MEMORY FOOTPRINT: NUMERIC IDs (long)                                            │
│                                                                                  │
│  Count       HashSet<Long>    RoaringBitmap    Roaring64Nav    Bloom (1% FPR)    │
│                                (32-bit)         (64-bit)                          │
│  ─────────────────────────────────────────────────────────────────────────────── │
│  1M          ~48 MB           ~2-4 MB          ~4-8 MB         ~1.2 MB           │
│  10M         ~480 MB          ~20-40 MB        ~40-80 MB       ~12 MB            │
│  100M        ~4.8 GB          ~200-400 MB      ~400-800 MB     ~120 MB           │
│  1B          ~48 GB           ~2-4 GB          ~4-8 GB         ~1.2 GB           │
│              ❌ IMPOSSIBLE     ✅ FEASIBLE      ✅ FEASIBLE     ✅ FEASIBLE       │
│                                                                                  │
│  Notes:                                                                          │
│  • Exact RoaringBitmap sizes depend on data density and distribution             │
│  • Dense ranges (e.g., 1M–2M) compress to just a few bytes as RUN containers    │
│  • Randomly scattered IDs use ~2-3 bytes per value                              │
│  • Roaring64NavigableMap has ~2× overhead vs 32-bit due to red-black tree       │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## RoaringBitmap vs Bloom Filter vs HashSet

| Aspect | HashSet | Bloom Filter | RoaringBitmap |
|--------|---------|-------------|---------------|
| **False positives** | None | Yes (configurable FPR) | **None** |
| **False negatives** | None | None | None |
| **Remove individual IDs** | Yes | No (must rebuild) | **Yes** |
| **ID type constraint** | Any (String, Long, etc.) | Any (via Funnel) | **Integer/Long only** |
| **Memory (1B IDs)** | ~48-112 GB | ~1.2 GB | **~2-8 GB** |
| **Set operations (AND/OR/XOR)** | Slow (iteration) | Not supported | **Very fast (SIMD-accelerated)** |
| **Serialization** | Custom, large | Custom | **Built-in, compact** |
| **Lookup speed** | O(1) amortized | O(k) hash functions | O(log n) chunks + O(1) or O(log n) container |
| **Delta updates** | Add/remove freely | Must rebuild for removes | **Add/remove freely** |
| **Best for** | Small sets < 1M | Huge sets, FP acceptable | **Huge numeric sets, exact match needed** |

## When to Use RoaringBitmap

- **Hard constraint**: IDs must be numeric (int or long). String IDs cannot use RoaringBitmap.
- **Sweet spot**: 10M–1B numeric IDs where HashSet is too large but Bloom filter's false positives are unacceptable
- **Killer feature**: supports add/remove of individual IDs without rebuilding — perfect for delta updates
- **Bonus**: built-in serialization for checkpointing, SIMD-accelerated set operations for computing diffs between old and new blacklists

---

# Part 7: Memory Estimation

Assumptions: IDs are 20-character strings. In Java, each String object occupies ~80 bytes (12-byte object header + 4-byte hash + 4-byte coder + 16-byte char[]/byte[] reference + ~44 bytes padding/alignment). HashSet entry overhead: ~32 bytes (HashMap.Node: 12-byte header + 4 hash + 8 key ref + 8 next ref).

**Per ID in HashSet: ~112 bytes**

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                    MEMORY FOOTPRINT COMPARISON                                       │
│                                                                                      │
│  Blacklist    HashSet         Solution A     Solution B      Solution C        Sol D  │
│  Size         (per copy)      (Redis)        (per TM)        (per subtask)     Bloom  │
│  ─────────────────────────────────────────────────────────────────────────────────── │
│                                                                                      │
│  1M           112 MB          0 MB           112 MB          112 MB            1.2 MB │
│                                              × #TMs          × parallelism           │
│                                                                                      │
│  10M          1.1 GB          0 MB           1.1 GB          1.1 GB            12 MB  │
│                                              × #TMs          × parallelism           │
│                                                                                      │
│  100M         11 GB           0 MB           11 GB           11 GB             120 MB │
│               ⚠ LARGE                        × #TMs          × parallelism           │
│                                              (feasible       (IMPRACTICAL             │
│                                               with large      at high                 │
│                                               TM heap)        parallelism)            │
│                                                                                      │
│  1B           112 GB          0 MB           112 GB          112 GB            1.2 GB │
│               ❌ IMPOSSIBLE                   × #TMs          × parallelism           │
│                                              ❌ IMPOSSIBLE    ❌ IMPOSSIBLE            │
│                                                                                      │
└──────────────────────────────────────────────────────────────────────────────────────┘

Checkpoint size impact (Solution C only):
  Checkpoint = broadcast_state_size × parallelism (ALL subtasks checkpoint their copy)

  Example: 10M IDs, parallelism = 50
    Checkpoint size = 1.1 GB × 50 = 55 GB per checkpoint  ← VERY EXPENSIVE

  Example: 100M IDs, parallelism = 100
    Checkpoint size = 11 GB × 100 = 1.1 TB per checkpoint  ← UNACCEPTABLE

  This is the #1 limitation of broadcast state for large datasets.
```

---

# Part 8: Recommendation Matrix

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                    WHICH SOLUTION TO USE?                                             │
│                                                                                      │
│  Blacklist Size    Recommended          Reasoning                                    │
│  ──────────────────────────────────────────────────────────────────────────────────── │
│                                                                                      │
│  < 1M              C (Broadcast State)  Fits comfortably in memory. Flink-native,    │
│                                         checkpointed, simplest to operate.           │
│                                         Checkpoint overhead negligible.              │
│                                                                                      │
│  1M – 10M          B (TM Cache)         Avoids parallelism multiplication.           │
│                     or C (Broadcast)     Use C if parallelism < 20 and you want       │
│                                         Flink-managed fault tolerance.               │
│                                         Use B if parallelism > 20.                   │
│                                                                                      │
│  10M – 100M        B (TM Cache)         11 GB per TM is feasible with large heap     │
│                                         (e.g., TM with 16-32GB). Broadcast state     │
│                                         checkpoint multiplication is impractical.    │
│                                                                                      │
│  100M – 1B         D (Bloom + Redis)    HashSet at 112 GB won't fit in memory.       │
│                                         Bloom filter at 1.2 GB with 1% FPR.          │
│                                         99% of lookups resolved locally.              │
│                                         Remaining 1% verified against Redis.          │
│                                                                                      │
│                     or A (Async Redis)   If Redis cluster can handle the QPS          │
│                                         (millions/sec) and you want simplicity.      │
│                                                                                      │
│  > 1B              A (Async Redis)      No in-memory solution is feasible.            │
│                     with Redis Cluster   Use Redis Cluster with read replicas.        │
│                                         Or D (Bloom + Redis) if Redis QPS is         │
│                     or D (Bloom+Redis)   a concern — Bloom reduces calls by 99%.     │
│                                                                                      │
│  NUMERIC IDs       RoaringBitmap        If IDs are int/long, RoaringBitmap changes   │
│  (int/long)        + TM Cache (B)       the game at every scale:                     │
│                                                                                      │
│                                         100M ints: ~200-400 MB (vs 4.8 GB HashSet)   │
│                                         1B ints:   ~2-4 GB (vs 48 GB HashSet)        │
│                                         Zero false positives (unlike Bloom)          │
│                                         Supports add/remove without rebuild          │
│                                         Use Roaring64NavigableMap for long IDs       │
│                                                                                      │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

---

# Part 9: Full Comparison Table

| Dimension | A: Async Redis | B: TM Cache (HashSet) | B': TM Cache (Roaring) | C: Broadcast State | D: Bloom + Redis |
|-----------|---------------|----------------------|----------------------|--------------------|--------------------|
| **Lookup latency** | 1-5ms (network) | ~50ns (HashSet) | ~200ns (Roaring) | ~50ns (HashSet) | ~100ns (Bloom) + 1-5ms for positives |
| **Throughput impact** | High (bottleneck) | Negligible | Negligible | Negligible | Low (1% hits Redis) |
| **Memory per TM (1B IDs)** | 0 | ~112 GB (IMPOSSIBLE) | ~4-8 GB (FEASIBLE) | ~112 GB × subtasks (IMPOSSIBLE) | ~1.2 GB |
| **Memory per subtask** | 0 | 0 (shared static) | 0 (shared static) | Full copy | 0 (shared via static) |
| **Checkpoint size** | 0 | 0 (not managed) | 0 (not managed) | size × parallelism | 0 (not managed) |
| **Fault tolerance** | Exactly-once (checkpointed in-flight requests) | Re-loads from Redis on restart | Re-loads from Redis on restart | Full Flink checkpoint/restore | Re-builds Bloom from Redis on restart |
| **Staleness** | 0 (real-time) | Up to refresh interval | Up to refresh interval | Up to source poll interval | Up to rebuild interval |
| **Redis dependency** | Runtime (every record) | Startup + periodic | Startup + periodic | Source only (periodic) | Startup + periodic + ~1% of traffic |
| **Max blacklist size** | Unlimited | ~100M strings | ~1B numeric IDs | ~10M (checkpoint limit) | ~1B (Bloom fits in memory) |
| **False positives** | None | None | None | None | Yes (~1%) |
| **ID type** | Any | Any | Integer/Long only | Any | Any |
| **Code complexity** | Low | Medium | Medium | Medium | High |
| **External library** | Lettuce | Lettuce | Lettuce + RoaringBitmap | None at runtime | Guava or similar |

---

# Sources

- [Flink Async I/O documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/asyncio/)
- [Flink Broadcast State Pattern](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/broadcast_state/)
- [Broadcast State Pattern: 4 Important Considerations (Ververica)](https://www.ververica.com/blog/broadcast-state-pattern-flink-considerations)
- [Ten Flink Gotchas (Contentsquare)](https://engineering.contentsquare.com/2021/ten-flink-gotchas/)
- [Bloom Filters for Billion-Scale Blacklists](https://medium.com/@umeshcapg/blacklist-billion-dollar-problems-how-bloom-filters-save-the-day-and-gigabytes-881df0557df6)
- [RoaringBitmap](https://github.com/RoaringBitmap/RoaringBitmap)
- [Roaring64NavigableMap for 64-bit IDs](https://github.com/RoaringBitmap/RoaringBitmap/blob/master/roaringbitmap/src/main/java/org/roaringbitmap/longlong/Roaring64NavigableMap.java)
- [Introduction to Roaring Bitmap (Baeldung)](https://www.baeldung.com/java-roaring-bitmap-intro)
- [Memory Efficient HashSet in Java](https://intelligentjava.wordpress.com/2016/10/22/memory-efficient-hashset-implementation-for-java/)
- [Tuning Checkpoints and Large State](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/large_state_tuning/)
- [FLIP-232: Async I/O Retry Support](https://www.mail-archive.com/dev@flink.apache.org/msg57230.html)
