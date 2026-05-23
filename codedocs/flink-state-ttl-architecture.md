# Flink State TTL: Architecture, Cleanup Strategies, and Limitations

This document explains how Apache Flink's State TTL (Time-To-Live) works internally,
why it only applies to keyed state, how each cleanup strategy operates at the code level,
and the known limitations.

---

## Part 1: Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         FLINK STATE TTL ARCHITECTURE                                │
│                                                                                     │
│  User Code                                                                          │
│    │                                                                                │
│    │  state.value() / state.get(key) / state.add(...)                               │
│    ▼                                                                                │
│  ┌───────────────────────────────────────────────────────────────────────────────┐   │
│  │  TTL State Decorator (TtlValueState / TtlMapState / TtlListState / ...)     │   │
│  │                                                                              │   │
│  │  1. accessCallback.run()  ──► Incremental Cleanup (if configured)            │   │
│  │  2. Read TtlValue<V> from backend                                            │   │
│  │  3. Check: expired(lastAccessTs + ttl <= currentTs)?                          │   │
│  │     ├─ YES → stateClear.run()   (lazy delete)                                │   │
│  │     │        return null  (or expired value if ReturnExpiredIfNotCleanedUp)   │   │
│  │     └─ NO  → if updateTsOnRead: rewrap with new timestamp                    │   │
│  │              return userValue                                                 │   │
│  └──────────────────────────────┬────────────────────────────────────────────────┘   │
│                                 │                                                    │
│                                 ▼                                                    │
│  ┌───────────────────────────────────────────────────────────────────────────────┐   │
│  │  Original State (InternalValueState / InternalMapState / ...)                │   │
│  │                                                                              │   │
│  │  Stores TtlValue<V> = (userValue, lastAccessTimestamp)                       │   │
│  │  Serialized as: [8 bytes timestamp | user value bytes]                       │   │
│  │  Using TtlSerializer (CompositeSerializer<TtlValue<T>>)                      │   │
│  └──────────────────────────────┬────────────────────────────────────────────────┘   │
│                                 │                                                    │
│                                 ▼                                                    │
│  ┌───────────────────────────────────────────────────────────────────────────────┐   │
│  │  State Backend (Heap / RocksDB / ForSt)                                      │   │
│  │                                                                              │   │
│  │  Background Cleanup:                                                          │   │
│  │    ├─ Full Snapshot: filter expired during checkpoint serialization            │   │
│  │    ├─ Incremental:   scan N entries per state access                          │   │
│  │    └─ RocksDB Compaction Filter: drop expired during SST compaction           │   │
│  └───────────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Part 2: How TTL State is Stored — TtlValue Wrapping

Every state entry with TTL enabled is wrapped in a `TtlValue<T>`:

```java
// flink-runtime/.../ttl/TtlValue.java
public class TtlValue<T> implements Serializable {
    @Nullable private final T userValue;          // the actual state value
    private final long lastAccessTimestamp;        // milliseconds (processing time)
}
```

**Storage overhead**: 8 bytes per entry (one `long` timestamp).

### Serialization

`TtlSerializer` (a `CompositeSerializer`) serializes each entry as:

```
┌──────────────────────────────────────────┐
│  8 bytes: lastAccessTimestamp (long)      │
│  N bytes: user value (via user serializer)│
└──────────────────────────────────────────┘
```

### Factory Chain: How TTL Gets Wired In

When user code calls `getRuntimeContext().getState(descriptor)`, the backend calls:

```
TtlStateFactory.createStateAndWrapWithTtlIfEnabled()
  │
  ├─ descriptor.getTtlConfig().isEnabled()?
  │   ├─ NO  → return unwrapped state from backend
  │   └─ YES → wrap:
  │        1. Replace descriptor's value serializer with TtlSerializer
  │        2. Create inner state from backend (stores TtlValue<V>)
  │        3. Register snapshot transformer (for full snapshot cleanup)
  │        4. Create TtlIncrementalCleanup (if configured)
  │        5. Return TtlXxxState decorator wrapping the inner state
```

**Source**: `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlStateFactory.java`

### Supported State Types

| State Type | TTL Decorator | Extra Wrapping |
|---|---|---|
| ValueState | `TtlValueState` | — |
| ListState | `TtlListState` | `IteratorWithCleanup` for lazy iteration cleanup |
| MapState | `TtlMapState` | `EntriesIterator` for lazy iteration cleanup |
| ReducingState | `TtlReducingState` | `TtlReduceFunction` wraps the reduce function |
| AggregatingState | `TtlAggregatingState` | `TtlAggregateFunction` wraps the aggregate function |

---

## Part 3: StateTtlConfig Options

```java
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Duration.ofHours(1))                          // TTL duration
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanupInRocksdbCompactFilter(1000)                      // cleanup strategy
    .build();
```

### UpdateType

| Value | When Timestamp Refreshes | Use Case |
|---|---|---|
| `OnCreateAndWrite` | On first write and every subsequent write | Fixed expiry from last write |
| `OnReadAndWrite` | On every read AND write | "Sliding window" — active keys stay alive |

### StateVisibility

| Value | Behavior on Expired Read | Trade-off |
|---|---|---|
| `NeverReturnExpired` | Returns `null`, clears state | Correctness — user never sees stale data |
| `ReturnExpiredIfNotCleanedUp` | Returns expired value if still physically present | Performance — avoids read-then-clear overhead on every access |

### TtlTimeCharacteristic

Only **ProcessingTime** is supported. Event-time TTL is not available.

---

## Part 4: Lazy Delete — The On-Read/Write Cleanup

Lazy delete is the **foundation** of all TTL behavior. It happens on every state access,
regardless of which cleanup strategy is configured.

### How It Works

The core logic lives in `AbstractTtlDecorator.getWrappedWithTtlCheckAndUpdate()`:

```
state.value()  (user code)
  │
  ▼
TtlValueState.value()
  │
  ├─ 1. accessCallback.run()        ◄── triggers incremental cleanup (if configured)
  │
  ├─ 2. TtlValue<V> ttlValue = original.value()    ◄── read from backend
  │
  ├─ 3. ttlValue == null?
  │     └─ YES → return null
  │
  ├─ 4. TtlUtils.expired(ttlValue)?
  │     │   Formula: (lastAccessTimestamp + ttl) <= currentTimestamp
  │     │   Overflow-safe: ttl = Math.min(Long.MAX_VALUE - ts, ttl)
  │     │
  │     ├─ YES, expired:
  │     │     stateClear.run()              ◄── physically remove from backend
  │     │     if (NeverReturnExpired)
  │     │         return null
  │     │     else
  │     │         return ttlValue           ◄── ReturnExpiredIfNotCleanedUp
  │     │
  │     └─ NO, still valid:
  │           if (updateTsOnRead)           ◄── OnReadAndWrite mode
  │               updater.accept(rewrapWithNewTs(ttlValue))   ◄── refresh timestamp
  │           return ttlValue.getUserValue()
```

**Source**: `flink-runtime/.../ttl/AbstractTtlDecorator.java` lines 92-110

### The Expiration Check

```java
// flink-runtime/.../ttl/TtlUtils.java
public static boolean expired(long ts, long ttl, long currentTimestamp) {
    return getExpirationTimestamp(ts, ttl) <= currentTimestamp;
}

private static long getExpirationTimestamp(long ts, long ttl) {
    long ttlWithoutOverflow = ts > 0 ? Math.min(Long.MAX_VALUE - ts, ttl) : ttl;
    return ts + ttlWithoutOverflow;
}
```

### Critical Limitation of Lazy Delete

**Expired but never-accessed keys persist forever.** If a key is written once and never
read again, the lazy check never runs for that key. The entry stays in state, consuming
memory/disk. This is why background cleanup strategies exist.

---

## Part 5: Cleanup Strategy — Incremental Cleanup

Incrementally scans state entries on each state access, removing expired ones.

### How It Works

```
state.get(key)
  │
  ├─ accessCallback.run()
  │     │
  │     ▼
  │   TtlIncrementalCleanup.stateAccessed()
  │     │
  │     ├─ Initialize global StateIncrementalVisitor (if null or exhausted)
  │     │     iterator = ttlState.original.getStateIncrementalVisitor(cleanupSize)
  │     │
  │     └─ runCleanup():
  │           while (entryNum < cleanupSize && iterator.hasNext()):
  │               nextEntries = iterator.nextEntries()
  │               for each entry:
  │                   cleanState = ttlState.getUnexpiredOrNull(entry.getState())
  │                   if cleanState == null:
  │                       iterator.remove(entry)       ◄── fully expired → delete
  │                   else if cleanState != entry:
  │                       iterator.update(entry, clean) ◄── partially expired (list/map)
  │               entryNum += nextEntries.size()
  │
  ├─ (then proceed with the actual get/put operation)
```

### Configuration

```java
StateTtlConfig.newBuilder(Duration.ofHours(1))
    .cleanupIncrementally(
        5,      // cleanupSize: max entries to scan per state access
        true    // runCleanupForEveryRecord: true = every processElement,
    )           //                          false = only on state access
    .build();
```

### Characteristics

| Property | Value |
|---|---|
| **When it runs** | On every state access (or every record if configured) |
| **Entries scanned** | `cleanupSize` entries per access (default: 5) |
| **Backend support** | Heap backend (natural fit); RocksDB (works but less efficient) |
| **Overhead** | Adds latency to every `processElement()` — proportional to `cleanupSize` |
| **Completeness** | Eventually cleans all entries IF state is regularly accessed; abandoned keys stay if the operator stops receiving records |

**Source**: `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlIncrementalCleanup.java`

---

## Part 6: Cleanup Strategy — Full Snapshot

Filters expired entries during checkpoint serialization.

### How It Works

```
Checkpoint trigger
  │
  ▼
StateSnapshotTransformer.filterOrTransform(byte[] value)
  │
  ├─ Deserialize first 8 bytes → lastAccessTimestamp
  │
  ├─ TtlUtils.expired(ts, ttl, currentTimestamp)?
  │     ├─ YES → return null  (exclude from snapshot)
  │     └─ NO  → return value (include in snapshot)
```

The transformer is registered during state creation in `TtlStateFactory`:

```java
if (ttlConfig.getCleanupStrategies().inFullSnapshot()) {
    // Register TtlStateSnapshotTransformer.Factory
    // → filters expired entries during checkpoint serialization
}
```

### Characteristics

| Property | Value |
|---|---|
| **When it runs** | During checkpoint/savepoint creation |
| **Backend support** | Heap backend with full snapshots only |
| **Does NOT work with** | RocksDB incremental checkpoints (entries stay in SST files) |
| **Effect on local state** | None — expired entries remain in local state between checkpoints |
| **Effect on snapshot size** | Reduces snapshot size (expired entries excluded) |

**Source**: `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlStateSnapshotTransformer.java`

---

## Part 7: Cleanup Strategy — RocksDB Compaction Filter

The most efficient strategy for the RocksDB state backend. Drops expired entries
during RocksDB's native compaction process.

### How It Works

```
RocksDB background compaction thread (native C++)
  │
  ▼
FlinkCompactionFilter processes each entry in SST file
  │
  ├─ Read lastAccessTimestamp from value bytes
  │
  ├─ Need current time?
  │     Every queryTimeAfterNumEntries entries:
  │       JNI callback → TimeProviderWrapper.currentTimestamp()
  │                       → TtlTimeProvider.DEFAULT (System.currentTimeMillis())
  │     Otherwise: use cached timestamp
  │
  ├─ expired(lastAccessTs + ttl <= cachedTimestamp)?
  │     ├─ YES → drop entry (not written to output SST)
  │     └─ NO  → keep entry
```

### Configuration

```java
StateTtlConfig.newBuilder(Duration.ofHours(1))
    .cleanupInRocksdbCompactFilter(
        1000                        // queryTimeAfterNumEntries
    )
    // OR with periodic compaction:
    .cleanupInRocksdbCompactFilter(
        1000,                       // queryTimeAfterNumEntries
        Duration.ofDays(1)          // periodicCompactionTime
    )
    .build();
```

- **`queryTimeAfterNumEntries`**: The filter queries the JVM timestamp (via JNI) once
  every N entries and caches it. Lower = more accurate but slower compaction. Default: 1000.
- **`periodicCompactionTime`**: Forces RocksDB to compact SST files older than this duration,
  even if they haven't been touched. Helps clean expired entries in rarely-accessed state.
  Default: 30 days.

### Characteristics

| Property | Value |
|---|---|
| **When it runs** | During RocksDB background compaction (async) |
| **Backend support** | RocksDB / ForSt only |
| **Overhead** | JNI callback every N entries during compaction; minimal impact on record processing |
| **Completeness** | Comprehensive — all entries are eventually compacted |
| **Limitation** | Compaction timing is non-deterministic; rarely-accessed SST files may not compact without `periodicCompactionTime` |

### Source Code References

| Component | Location |
|---|---|
| Filter manager | `flink-state-backends/flink-statebackend-rocksdb/.../ttl/RocksDbTtlCompactFiltersManager.java` |
| JNI bridge | `TimeProviderWrapper.currentTimestamp()` (same file, inner class) |
| Native filter | `org.rocksdb.FlinkCompactionFilter` / `FlinkCompactionFilterFactory` (frocksdbjni library) |
| Expiration check | `flink-runtime/.../ttl/TtlUtils.expired()` |

See also: the class javadoc on `SaltedUrlDeduplication.java` for a detailed walkthrough
of `queryTimeAfterNumEntries` and JNI performance.

---

## Part 8: Cleanup Strategy Comparison

```
┌──────────────────────┬──────────────────────┬───────────────────┬──────────────────┬────────────────────┐
│                      │ Lazy Delete          │ Incremental       │ Full Snapshot    │ RocksDB Compact    │
│                      │ (always active)      │ Cleanup           │                  │ Filter             │
├──────────────────────┼──────────────────────┼───────────────────┼──────────────────┼────────────────────┤
│ When it runs         │ On state access      │ On state access   │ On checkpoint    │ During RocksDB     │
│                      │ (read/write)         │ (scans N entries) │ serialization    │ compaction (async)  │
├──────────────────────┼──────────────────────┼───────────────────┼──────────────────┼────────────────────┤
│ Heap backend         │ ✓                    │ ✓ (best fit)      │ ✓ (full only)    │ ✗                  │
├──────────────────────┼──────────────────────┼───────────────────┼──────────────────┼────────────────────┤
│ RocksDB backend      │ ✓                    │ ✓ (less ideal)    │ ✗ (incr. ckpt)   │ ✓ (best fit)       │
├──────────────────────┼──────────────────────┼───────────────────┼──────────────────┼────────────────────┤
│ Processing overhead  │ Per-access TTL check │ cleanupSize scans │ Checkpoint only  │ JNI calls during   │
│                      │ (minimal)            │ per access        │                  │ compaction          │
├──────────────────────┼──────────────────────┼───────────────────┼──────────────────┼────────────────────┤
│ Cleans abandoned     │ ✗ (never accessed    │ ✓ (if iterator    │ ✓ (on next       │ ✓ (on next         │
│ keys?                │   = never cleaned)   │    reaches them)  │    checkpoint)   │    compaction)     │
├──────────────────────┼──────────────────────┼───────────────────┼──────────────────┼────────────────────┤
│ Reduces local state? │ ✓ (on access)        │ ✓ (on access)     │ ✗ (snapshot      │ ✓ (physically      │
│                      │                      │                   │    size only)    │    removed)        │
└──────────────────────┴──────────────────────┴───────────────────┴──────────────────┴────────────────────┘
```

### Recommended Combinations

- **Heap backend**: Lazy delete + `cleanupIncrementally(5, true)` + `cleanupFullSnapshot()`
- **RocksDB backend**: Lazy delete + `cleanupInRocksdbCompactFilter(1000)`
- **RocksDB + thorough cleanup**: Add `periodicCompactionTime` for rarely-accessed state

---

## Part 9: Why TTL is Mandatory for Keyed State — And Why Operator State Doesn't Need It

### The Core Problem: Abandoned Keys

The primary reason keyed state **needs** TTL is that **keys can become permanently
abandoned** — once a key stops appearing in the input stream, its state is never
accessed again and never cleaned up. Without TTL, these dead entries accumulate
forever, eventually causing OOM.

This is fundamentally different from operator state, where the data structure is
managed by the operator itself and can be bounded by application logic.

```
Example: keyBy(date, user_id) for daily dedup

  Day 1 (2024-03-20):
    key=(2024-03-20, alice) → state: seen=true
    key=(2024-03-20, bob)   → state: seen=true
    key=(2024-03-20, carol) → state: seen=true
    ... 10M keys for this date

  Day 2 (2024-03-21):
    key=(2024-03-21, alice) → state: seen=true    ◄── new keys for today
    key=(2024-03-21, dave)  → state: seen=true
    ...

    key=(2024-03-20, alice) → NEVER ACCESSED AGAIN ◄── yesterday's keys are
    key=(2024-03-20, bob)   → NEVER ACCESSED AGAIN     permanently abandoned.
    key=(2024-03-20, carol) → NEVER ACCESSED AGAIN     No event will ever arrive
    ... 10M dead entries sitting in state               with date=2024-03-20 again.

  Day 3: another 10M dead entries from Day 2 accumulate
  Day 4: another 10M...
  Day N: OOM

  With TTL = 24h:
    Day 2: Day 1's entries expire → cleaned by compaction filter / incremental cleanup
    State size stays bounded at ~1 day's worth of keys
```

**This is not an edge case — it's the default behavior of most keyed state workloads.**
Any key space that includes time-varying components (date, session ID, request ID,
window boundaries) or naturally churning entities (user IDs that go inactive, devices
that go offline) produces abandoned keys. Without TTL, the state grows monotonically.

### Why Operator State Doesn't Have This Problem

Operator state is structurally different — it doesn't have the abandoned-key problem:

```
┌──────────────────────────────────────────┐    ┌──────────────────────────────────────────┐
│  KEYED STATE                             │    │  OPERATOR STATE                          │
│                                          │    │                                          │
│  Key space is UNBOUNDED and              │    │  Data structure is BOUNDED and            │
│  determined by input data:               │    │  managed by operator code:                │
│                                          │    │                                          │
│  ├─ New keys appear from input stream    │    │  ├─ Source offsets: 1 per partition       │
│  ├─ Old keys stop appearing but their    │    │  │   (naturally bounded, overwritten)     │
│  │   state remains forever               │    │  │                                       │
│  ├─ Operator has NO control over which   │    │  ├─ Union state: operator controls what   │
│  │   keys exist — input decides           │    │  │   goes in and what gets removed        │
│  ├─ State size = f(all keys ever seen)   │    │  │                                       │
│  │   → grows monotonically without TTL   │    │  ├─ List state: operator can prune at     │
│  │                                       │    │  │   checkpoint or in processElement()    │
│  └─ NEEDS TTL to bound state size        │    │  │                                       │
│                                          │    │  └─ NO abandoned-key problem              │
│  Examples of abandoned keys:             │    │     → TTL not needed                      │
│  ├─ (date, user_id) — yesterday's dates  │    │                                          │
│  ├─ session_id — closed sessions         │    │                                          │
│  ├─ request_id — completed requests      │    │                                          │
│  └─ device_id — offline devices          │    │                                          │
└──────────────────────────────────────────┘    └──────────────────────────────────────────┘
```

### Architectural Reasons (Why TTL is Only *Possible* for Keyed State)

Beyond the motivation, there are also structural reasons why TTL is only implemented
for keyed state:

- **Per-key indexing**: Keyed state naturally associates one `TtlValue(val, timestamp)`
  per key. Operator state is a flat list — there's no key to attach a per-entry timestamp to.
- **Backend support**: Keyed state uses RocksDB/ForSt which support compaction filters
  for background TTL cleanup. Operator state is always heap-based.
- **Rescaling**: Keyed state redistributes via key groups — TTL metadata travels with
  each key. Operator state uses list/union redistribution with no per-entry metadata.
- **Access pattern**: `getRuntimeContext().getState()` is scoped to `currentKey`,
  making per-key expiration checks natural. `getOperatorStateStore()` has no key context.

---

## Part 10: How Operator State Removes Unused Data

Since operator state has no built-in TTL, developers must manage cleanup manually.

### Common Patterns

**1. Timer-Based Cleanup**

```java
// Register a processing-time timer to periodically prune operator state
@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) {
    Iterator<MyEntry> it = operatorListState.get().iterator();
    List<MyEntry> retained = new ArrayList<>();
    while (it.hasNext()) {
        MyEntry entry = it.next();
        if (System.currentTimeMillis() - entry.createdAt < TTL_MS) {
            retained.add(entry);
        }
    }
    operatorListState.update(retained);
}
```

**2. Bounded Collections**

Use fixed-size data structures (e.g., `LinkedHashMap` with `removeEldestEntry`,
circular buffers) in operator state to cap memory usage.

**3. Application-Level Bookkeeping**

Track entry age or count within the state itself and prune during `processElement()`.

**4. Connectors: Naturally Bounded**

Source connectors (Kafka, Kinesis) store offsets — these are naturally bounded
(one offset per partition). Sink connectors with pending-write buffers flush
on checkpoint, so state is bounded by checkpoint interval.

---

## Part 11: Limitations and Caveats

### Processing Time Only

State TTL uses processing time (`System.currentTimeMillis()`). There is no event-time
TTL support. This means:
- Reprocessing historical data may expire entries unexpectedly (processing time advances
  even when event timestamps are in the past).
- TTL behavior is non-deterministic across restarts.

### Storage Overhead

Every TTL-enabled state entry adds **8 bytes** (one `long` timestamp). For state with
millions of small entries (e.g., `MapState<String, Boolean>` for dedup), this can be
significant.

### Session Window Merging

TTL interacts poorly with session windows:
- When windows merge, the reduce/aggregate function runs on `TtlValue`-wrapped accumulators.
- `TtlReduceFunction` and `TtlAggregateFunction` check expiration on both inputs during merge.
- If one side is expired, it's dropped — potentially losing valid data that should have
  been part of the merged window.

### State Migration (pre-2.2.0)

Before Flink 2.2.0, enabling or disabling TTL on existing state caused
`StateMigrationException` on restore. Since 2.2.0, full migration between TTL-enabled
and TTL-disabled state is supported across all backends.

**Caution**: Switching from a short TTL to a long TTL can surface entries that were
logically expired under the old TTL but never physically cleaned.

### State V2 (Async API) Restrictions

The async state API (`flink-runtime/.../state/v2/ttl/TtlStateFactory.java`) has
additional restrictions:
- Only `ROCKSDB_COMPACTION_FILTER` cleanup strategy is supported.
- `OnReadAndWrite` update type is NOT supported (throws `UnsupportedOperationException`).

### TTL Initialization Timing

`TtlTimeProvider.DEFAULT = System::currentTimeMillis` is called when state is accessed,
not when the event arrives. Under backpressure, the timestamp may be significantly later
than the event's ingestion time. In rare cases, data can expire in a downstream operator
but still be valid in the upstream operator due to timing differences.

### Null Values in MapState

`MapState` with TTL only supports `null` user values if the user-provided value serializer
can handle nulls. The `TtlSerializer` wraps the value — if the inner serializer throws on
`null`, the TTL layer will propagate the exception.

### No TTL for Broadcast State

Broadcast state does not support TTL. It uses `OperatorStateBackend` internally and has
no per-key semantics.

---

## Part 12: Source Code References

| Component | File Path |
|---|---|
| TtlValue (storage wrapper) | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlValue.java` |
| StateTtlConfig (config) | `flink-core/src/main/java/org/apache/flink/api/common/state/StateTtlConfig.java` |
| TtlUtils (expiration logic) | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlUtils.java` |
| AbstractTtlDecorator (lazy delete) | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/AbstractTtlDecorator.java` |
| AbstractTtlState (base state) | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/AbstractTtlState.java` |
| TtlValueState | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlValueState.java` |
| TtlMapState | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlMapState.java` |
| TtlListState | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlListState.java` |
| TtlReducingState | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlReducingState.java` |
| TtlAggregatingState | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlAggregatingState.java` |
| TtlReduceFunction | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlReduceFunction.java` |
| TtlAggregateFunction | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlAggregateFunction.java` |
| TtlStateFactory (wiring) | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlStateFactory.java` |
| TtlIncrementalCleanup | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlIncrementalCleanup.java` |
| TtlStateSnapshotTransformer | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlStateSnapshotTransformer.java` |
| RocksDB compact filter manager | `flink-state-backends/flink-statebackend-rocksdb/src/main/java/org/apache/flink/state/rocksdb/ttl/RocksDbTtlCompactFiltersManager.java` |
| TtlTimeProvider | `flink-runtime/src/main/java/org/apache/flink/runtime/state/ttl/TtlTimeProvider.java` |
| State V2 TTL factory | `flink-runtime/src/main/java/org/apache/flink/runtime/state/v2/ttl/TtlStateFactory.java` |
