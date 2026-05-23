# Flink Window Event Assignment and State Lifecycle: Tumbling vs Sliding

A code-level deep dive into how Flink assigns events to tumbling and sliding windows, the data structures and algorithms used, and how state is stored and removed from RocksDB.

---

## 1. Core Data Structure: TimeWindow

**File:** `TimeWindow.java:45-53`

```java
public class TimeWindow extends Window {
    private final long start;    // inclusive start timestamp (millis)
    private final long end;      // exclusive end timestamp (millis)

    public TimeWindow(long start, long end) {
        this.start = start;
        this.end = end;
    }
}
```

- **Immutable**: both `start` and `end` are `final`
- **Half-open interval**: `[start, end)` — start-inclusive, end-exclusive
- `maxTimestamp()` returns `end - 1` — the last millisecond that belongs to this window
- `equals()` compares both `start` and `end`; `hashCode()` uses `MathUtils.longToIntWithBitMixing(start + end)`

### TimeWindow Serialization (RocksDB Namespace)

**File:** `TimeWindow.java:130-198`

```java
public static class Serializer extends TypeSerializerSingleton<TimeWindow> {
    @Override
    public int getLength() {
        return Long.BYTES + Long.BYTES;  // fixed 16 bytes
    }

    @Override
    public void serialize(TimeWindow record, DataOutputView target) throws IOException {
        target.writeLong(record.start);   // bytes 0-7
        target.writeLong(record.end);     // bytes 8-15
    }

    @Override
    public TimeWindow deserialize(DataInputView source) throws IOException {
        long start = source.readLong();
        long end = source.readLong();
        return new TimeWindow(start, end);
    }
}
```

**Binary format: exactly 16 bytes** — 8 bytes for `start` (big-endian long) + 8 bytes for `end` (big-endian long). This serialized form becomes the **namespace** portion of the RocksDB composite key. Since it's fixed-length, no length prefix is needed.

### The Core Algorithm: getWindowStartWithOffset

**File:** `TimeWindow.java:264-272`

```java
public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
    final long remainder = (timestamp - offset) % windowSize;
    if (remainder < 0) {
        return timestamp - (remainder + windowSize);
    } else {
        return timestamp - remainder;
    }
}
```

**Algorithm:** Computes the start of the window that contains `timestamp`, given an alignment `offset` and window `windowSize`.

Mathematically: `floor((timestamp - offset) / windowSize) * windowSize + offset`

The `if (remainder < 0)` branch handles Java's behavior where `%` returns negative values for negative dividends.

**Example:**
```
timestamp=25000, offset=0, windowSize=60000
remainder = (25000 - 0) % 60000 = 25000
window_start = 25000 - 25000 = 0
→ window [0, 60000)
```

This function is the foundation for **both** tumbling and sliding window assignment — the difference is what `windowSize` value is passed.

---

## 2. Tumbling Window Assignment

### Algorithm

**File:** `TumblingEventTimeWindows.java:69-86`

```java
public Collection<TimeWindow> assignWindows(
        Object element, long timestamp, WindowAssignerContext context) {
    if (timestamp > Long.MIN_VALUE) {
        long start = TimeWindow.getWindowStartWithOffset(
                timestamp, (globalOffset + staggerOffset) % size, size);
        return Collections.singletonList(new TimeWindow(start, start + size));
    } else {
        throw new RuntimeException("Record has Long.MIN_VALUE timestamp...");
    }
}
```

**Key characteristics:**
- Calls `getWindowStartWithOffset(timestamp, offset, size)` — uses `size` as the window size
- Returns `Collections.singletonList()` — **always exactly 1 window per element**
- O(1) computation — no loops, no allocations beyond the singleton list and TimeWindow object

### Concrete Example

```
Configuration: TumblingEventTimeWindows.of(Duration.ofMinutes(1))
  size = 60000ms, offset = 0

Event timestamp = 25000ms:
  getWindowStartWithOffset(25000, 0, 60000)
  remainder = 25000 % 60000 = 25000
  start = 25000 - 25000 = 0
  → assigned to window [0, 60000)

Event timestamp = 75000ms:
  getWindowStartWithOffset(75000, 0, 60000)
  remainder = 75000 % 60000 = 15000
  start = 75000 - 15000 = 60000
  → assigned to window [60000, 120000)
```

Windows are non-overlapping: every event belongs to exactly one window. Adjacent windows tile perfectly: `[0, 60000)`, `[60000, 120000)`, `[120000, 180000)`, ...

### State Storage for Tumbling Windows

For each element, `WindowOperator.processElement()` runs this path once (lines 404-432):

```java
windowState.setCurrentNamespace(window);    // set namespace = the one window
windowState.add(element.getValue());        // one RocksDB write
registerCleanupTimer(window);               // one timer registered
```

**RocksDB entries per element: 1**
**Timers per element: 1** (cleanup timer at `window.maxTimestamp() + allowedLateness`)

RocksDB key:
```
[keyGroupPrefix][serialized_key][window_start_8bytes][window_end_8bytes]
```

---

## 3. Sliding Window Assignment

### Algorithm

**File:** `SlidingEventTimeWindows.java:77-91`

```java
public Collection<TimeWindow> assignWindows(
        Object element, long timestamp, WindowAssignerContext context) {
    if (timestamp > Long.MIN_VALUE) {
        List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
        long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
        for (long start = lastStart; start > timestamp - size; start -= slide) {
            windows.add(new TimeWindow(start, start + size));
        }
        return windows;
    } else {
        throw new RuntimeException("Record has Long.MIN_VALUE timestamp...");
    }
}
```

**Key characteristics:**
- Calls `getWindowStartWithOffset(timestamp, offset, slide)` — uses **`slide`** (not `size`) as the alignment interval
- Loops backwards from `lastStart`, decrementing by `slide`, until `start <= timestamp - size`
- Returns `size / slide` windows per element (pre-allocated: `new ArrayList<>((int)(size / slide))`)
- Validation in constructor (line 64): `size / slide <= MAX_WINDOW_NUM (10,000,000)`

**The algorithm finds all windows [start, start+size) where start <= timestamp < start+size.**

### Concrete Example

```
Configuration: SlidingEventTimeWindows.of(Duration.ofSeconds(60), Duration.ofSeconds(20))
  size = 60000ms, slide = 20000ms
  windows per element = 60000 / 20000 = 3

Event timestamp = 25000ms:
  lastStart = getWindowStartWithOffset(25000, 0, 20000)
            = 25000 - (25000 % 20000) = 25000 - 5000 = 20000

  Loop iteration 1: start=20000, 20000 > 25000-60000=-35000? YES
    → window [20000, 80000)

  Loop iteration 2: start=0, 0 > -35000? YES
    → window [0, 60000)

  Loop iteration 3: start=-20000, -20000 > -35000? YES
    → window [-20000, 40000)

  Loop iteration 4: start=-40000, -40000 > -35000? NO → STOP

  Result: 3 windows: [-20000, 40000), [0, 60000), [20000, 80000)
```

### State Storage for Sliding Windows

For each element, `WindowOperator.processElement()` runs the inner loop N times (lines 404-432):

```java
for (W window : elementWindows) {               // iterates over all N windows
    if (isWindowLate(window)) { continue; }

    windowState.setCurrentNamespace(window);     // set namespace = this window
    windowState.add(element.getValue());         // RocksDB write for THIS window
    // ... trigger check ...
    registerCleanupTimer(window);                // register timer for THIS window
}
```

**RocksDB entries per element: N** (one per assigned window)
**Timers per element: N** (one cleanup timer per window)

Each window gets its own independent RocksDB entry:

```
Window [-20000, 40000):
  RocksDB key: [KG][key][-20000_as_long][40000_as_long] → value

Window [0, 60000):
  RocksDB key: [KG][key][0_as_long][60000_as_long] → value

Window [20000, 80000):
  RocksDB key: [KG][key][20000_as_long][80000_as_long] → value
```

**No deduplication or cross-window optimization exists.** The same element is stored N times independently. This is by design — each window is an isolated processing unit with its own trigger, state, and cleanup lifecycle.

---

## 4. How State is Stored: The Write Path

### RocksDB Composite Key Structure

**File:** `SerializedCompositeKeyBuilder.java:120-128`

Every window state entry in RocksDB uses a composite key:

```
[keyGroupPrefix (1-2 bytes)] [serialized_key (variable)] [serialized_window (16 bytes)]
```

- **keyGroupPrefix**: determines which key group (and operator subtask) owns this key
- **serialized_key**: the user's key (e.g., `"group1"` serialized)
- **serialized_window**: `TimeWindow.Serializer` output — 16 bytes (start long + end long)

The window is the **namespace** in Flink's state abstraction. `windowState.setCurrentNamespace(window)` (line 413) stores the window reference, and subsequent operations serialize it into the composite key.

### ListState: Merge Operator (for `.process()` / `.apply()`)

**File:** `RocksDBListState.java:125-133`

```java
public void add(V value) throws IOException, RocksDBException {
    backend.db.merge(
            columnFamily,
            writeOptions,
            serializeCurrentKeyWithGroupAndNamespace(),  // composite key
            serializeValue(value, elementSerializer));    // serialized element
}
```

- Uses RocksDB's **merge()** operation with `StringAppendOperator`
- Elements are concatenated with a delimiter byte (`,` = 0x2C)
- No read needed — merge is append-only, O(1) per add
- On read (`get()`), the merged value is deserialized by splitting on the delimiter

### AggregatingState: Read-Modify-Write (for `.aggregate()`)

**File:** `RocksDBAggregatingState.java:100-105`

```java
public void add(T value) throws IOException, RocksDBException {
    byte[] key = getKeyBytes();                          // composite key bytes
    ACC accumulator = getInternal(key);                  // RocksDB GET
    accumulator = accumulator == null
        ? aggFunction.createAccumulator()
        : accumulator;
    updateInternal(key, aggFunction.add(value, accumulator));  // RocksDB PUT
}
```

- Performs a **GET → modify → PUT** cycle for each element
- `getInternal()` calls `backend.db.get(columnFamily, key)` — reads from RocksDB
- `updateInternal()` calls `backend.db.put(columnFamily, writeOptions, key, value)` — overwrites
- State size stays O(1) — only the accumulator is stored

### ReducingState: Read-Modify-Write (for `.reduce()`)

**File:** `RocksDBReducingState.java:93-98`

```java
public void add(V value) throws IOException, RocksDBException {
    byte[] key = getKeyBytes();
    V oldValue = getInternal(key);                       // RocksDB GET
    V newValue = oldValue == null ? value : reduceFunction.reduce(oldValue, value);
    updateInternal(key, newValue);                        // RocksDB PUT
}
```

Same read-modify-write pattern as AggregatingState.

### Write Options: WAL Disabled

**File:** `RocksDBResourceContainer.java:242-254`

```java
public WriteOptions getWriteOptions() {
    WriteOptions opt = new WriteOptions().setDisableWAL(true);
    // ...
    return opt;
}
```

- **Write-Ahead Log is disabled** — writes go directly to RocksDB's memtable
- Durability is guaranteed by Flink's checkpoint mechanism, not RocksDB WAL
- No sync to disk on each write — writes are batched in memtable until flush

### No Flink-Level Caching

Window state operations call `backend.db.get/put/merge/delete` directly. There is **no Flink-level read cache or write buffer** between `WindowOperator` and RocksDB. RocksDB's own internal block cache and memtable provide the caching.

---

## 5. How State is Removed: The Cleanup Path

### Step 1: Trigger Fires (Window Emits Result)

When the watermark passes `window.maxTimestamp()`, the timer fires and `WindowOperator.onEventTime()` runs (lines 450-488):

```java
TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());
if (triggerResult.isFire()) {
    ACC contents = windowState.get();      // RocksDB GET
    emitWindowContents(window, contents);  // emit downstream
}
if (triggerResult.isPurge()) {
    windowState.clear();                   // RocksDB DELETE (if purging trigger)
}
```

With default `EventTimeTrigger`, the result is `FIRE` (not purge) — state is read and emitted but **not deleted**.

### Step 2: Cleanup Timer Fires (State Deleted)

The cleanup timer fires at `window.maxTimestamp() + allowedLateness`. The same `onEventTime()` callback checks:

```java
if (isCleanupTime(triggerContext.window, timer.getTimestamp())) {
    clearAllState(triggerContext.window, windowState, mergingWindows);
}
```

`clearAllState()` (lines 560-571):

```java
private void clearAllState(W window, AppendingState<IN, ACC> windowState, ...) {
    windowState.clear();          // RocksDB DELETE — window contents
    triggerContext.clear();        // delete trigger's registered timer
    processContext.clear();        // user function cleanup callback
}
```

### Step 3: RocksDB Delete Operation

**File:** `AbstractRocksDBState.java:113-121`

```java
public void clear() {
    backend.db.delete(columnFamily, writeOptions,
        serializeCurrentKeyWithGroupAndNamespace());  // composite key
}
```

- Writes a **tombstone** (delete marker) to RocksDB's memtable
- The key becomes invisible to subsequent reads immediately
- The tombstone is flushed to SST files along with the memtable
- **Actual disk space reclamation** happens during RocksDB **compaction** — a background process that merges SST files and removes tombstoned entries

### RocksDB Internal Lifecycle

```
windowState.add(element)
  → data written to MEMTABLE (in-memory write buffer)
  → when memtable is full → FLUSHED to Level-0 SST file on disk
  → background COMPACTION merges SST files across levels

windowState.clear()
  → TOMBSTONE written to memtable
  → tombstone flushed to SST file
  → during compaction, tombstone meets original data → both REMOVED
  → disk space reclaimed
```

---

## 6. Tumbling vs Sliding: Complete Comparison

### Per-Element Cost

```
                        Tumbling                    Sliding (size=60s, slide=20s)
────────────────────    ────────────────────        ────────────────────────────
Windows assigned        1                           3 (= size/slide)
RocksDB writes          1                           3 (one per window)
Timers registered       1 cleanup timer             3 cleanup timers
State entries alive     1 per (key, window)         3 per (key, window-set)
```

### State Storage Layout in RocksDB

**Tumbling** — event at key="user1", timestamp=25000, window [0, 60000):

```
RocksDB Key                              Value
─────────────────────────────────────    ─────────────────
[KG|user1|0|60000]                       accumulated_state
```

**Sliding** — same event, 3 windows:

```
RocksDB Key                              Value
─────────────────────────────────────    ─────────────────
[KG|user1|-20000|40000]                  accumulated_state
[KG|user1|0|60000]                       accumulated_state
[KG|user1|20000|80000]                   accumulated_state
```

Each key is independent. The window bytes (16 bytes) in the key suffix are what distinguish them.

### Timer and Cleanup Timeline

**Tumbling** (allowedLateness=30s):
```
t=59999:  trigger timer fires → emit result, state kept
t=89999:  cleanup timer fires → clearAllState() → RocksDB DELETE
1 emit, 1 delete
```

**Sliding** (allowedLateness=30s, 3 windows):
```
t=39999:  cleanup for [-20000, 40000) → emit + clearAllState()
t=59999:  cleanup for [0, 60000)      → emit + clearAllState()
t=79999:  cleanup for [20000, 80000)  → emit + clearAllState()
3 separate emits, 3 separate deletes — each window independent
```

Each window fires and cleans up at its own `maxTimestamp + allowedLateness`. When window `[-20000, 40000)` is cleaned up, the other two windows **still have their state** in RocksDB. No cross-window interference.

### Memory and Checkpoint Impact

```
For K keys, M elements per key per window:

Tumbling:
  RocksDB entries = K × 1 = K             (one window per key at any time)
  Checkpoint size ∝ K entries

Sliding (size/slide = N):
  RocksDB entries = K × N                 (N overlapping windows per key)
  Checkpoint size ∝ K × N entries

With ListState (.process()):
  Tumbling: K × M elements total in state
  Sliding:  K × M × N elements total (same element stored N times)

With AggregatingState (.aggregate()):
  Tumbling: K × 1 accumulators in state
  Sliding:  K × N accumulators in state (N is manageable)
```

This is why `.aggregate()` is strongly preferred for sliding windows — state grows as O(K×N) accumulators instead of O(K×M×N) raw elements. For a 1-hour window with 1-second slide (N=3600), the difference is massive.

---

## 7. Algorithm Summary: getWindowStartWithOffset in Both Contexts

The same `getWindowStartWithOffset` function is used by both assigners, but with **different arguments**:

| | Tumbling | Sliding |
|---|---|---|
| Call | `getWindowStartWithOffset(timestamp, offset, size)` | `getWindowStartWithOffset(timestamp, offset, slide)` |
| Alignment unit | Window size | Slide interval |
| Result | Start of the one containing window | Start of the **latest** containing window |
| Post-processing | Return single window `[start, start+size)` | Loop backwards by `slide` to find all containing windows |

**Tumbling** uses window `size` as the alignment, producing a single aligned window. **Sliding** uses `slide` as the alignment to find the latest window start, then iterates backwards to enumerate all overlapping windows.

This is the fundamental algorithmic difference: tumbling is a single modulo operation; sliding is a modulo operation plus a bounded loop of `size/slide` iterations.

---

## 8. assignWindows vs Watermark: Separation of Concerns

### assignWindows Is a Pure Function — It Ignores the Watermark

`assignWindows()` is a **pure function of the element's timestamp**. It has no access to the watermark and doesn't know whether the element is late. It simply computes which window(s) contain this timestamp.

```java
// TumblingEventTimeWindows.java:69-86
public Collection<TimeWindow> assignWindows(
        Object element, long timestamp, WindowAssignerContext context) {
    long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, size);
    return Collections.singletonList(new TimeWindow(start, start + size));
}
```

The `WindowAssignerContext` only provides `getCurrentProcessingTime()` — no watermark access. A late event at `t=500` is always assigned to window `[0, 60000)`, regardless of whether the watermark is at 65000 or 200000.

### The Watermark Check Happens AFTER Assignment

In `WindowOperator.processElement()` (lines 293-447), there are three sequential stages:

```
Step 1 (line 294-296): assignWindows() — ALWAYS runs, IGNORES watermark
─────────────────────────────────────────────────────────────────────────
  elementWindows = windowAssigner.assignWindows(element, timestamp, ctx)

  Pure computation: timestamp → window(s). No filtering.

Step 2 (line 405-432): isWindowLate() — per-window watermark check
─────────────────────────────────────────────────────────────────────────
  for (W window : elementWindows) {
      if (isWindowLate(window)) {   // cleanupTime(window) <= watermark?
          continue;                 // SKIP — don't add to state
      }
      isSkippedElement = false;
      windowState.setCurrentNamespace(window);
      windowState.add(element.getValue());  // add to state
      // ... trigger, cleanup timer ...
  }

  The WINDOW (not the element) is checked:
    isWindowLate = (window.maxTimestamp() + allowedLateness) <= watermark

Step 3 (line 440-446): side output — if ALL windows were skipped
─────────────────────────────────────────────────────────────────────────
  if (isSkippedElement && isElementLate(element)) {
      if (lateDataOutputTag != null) sideOutput(element);
      else numLateRecordsDropped.inc();
  }
```

### Three Possible Outcomes for a Late Element

```
Event: (key="group1", timestamp=500), assigned to window [0, 60000)
Window: maxTimestamp=59999, allowedLateness=30s → cleanupTime=89999

─── Case 1: watermark=65000 (within allowedLateness) ─────────────────

  isWindowLate? 89999 <= 65000? NO
  → Element ADMITTED to state
  → Trigger sees watermark past window → immediate FIRE (re-fire)
  → State updated, corrected result emitted

─── Case 2: watermark=91000 (past allowedLateness) ───────────────────

  isWindowLate? 89999 <= 91000? YES
  → Window SKIPPED (state already cleaned up by cleanup timer)
  → isSkippedElement=true, isElementLate=true
  → Element goes to SIDE OUTPUT (if configured) or DROPPED

─── Case 3: watermark=61000, allowedLateness=0 ───────────────────────

  cleanupTime = 59999 + 0 = 59999
  isWindowLate? 59999 <= 61000? YES
  → Window SKIPPED
  → Element goes to SIDE OUTPUT or DROPPED
  (With allowedLateness=0, ANY late element is rejected)
```

### Why This Separation Matters

The `WindowAssigner` and watermark are **completely decoupled** by design:

1. **WindowAssigner** is a pure mapping: `timestamp → Collection<Window>`. It's simple, stateless, and testable.
2. **WindowOperator** owns the watermark logic: it decides whether to admit, re-fire, or discard based on `isWindowLate()`.
3. This means custom `WindowAssigner` implementations don't need to worry about lateness — the operator handles it uniformly.
4. The same assigner works identically regardless of `allowedLateness` configuration — only the operator's behavior changes.

---

## 9. Complete Window State Lifecycle: From assignWindows to Removal

### Who Calls assignWindows?

`WindowOperator.processElement()` (line 293) is the sole caller. It's invoked by the Flink runtime for every incoming `StreamRecord`:

```java
// WindowOperator.java:293-296
public void processElement(StreamRecord<IN> element) throws Exception {
    final Collection<W> elementWindows =
            windowAssigner.assignWindows(
                    element.getValue(), element.getTimestamp(), windowAssignerContext);
```

The runtime sets the current key in the keyed state backend before calling `processElement()`, so all state operations are automatically scoped to the element's key.

### What Is "Window State"?

Window state is **namespaced keyed state** stored in RocksDB (or heap). The comment on line 152 says it all:

```java
/** The state in which the window contents is stored. Each window is a namespace */
private transient InternalAppendingState<K, W, IN, ACC, ACC> windowState;
```

- **K** = user key type (e.g., `String`)
- **W** = window type (e.g., `TimeWindow`) — used as namespace
- **IN** = input element type
- **ACC** = accumulator type (the value stored per key+window)

The state is created in `open()` (line 247-251):
```java
windowState = (InternalAppendingState<K, W, IN, ACC, ACC>)
        getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
```

This creates a single RocksDB **column family** named `"window-contents"`. All (key, window) pairs share this column family. The window is encoded as the namespace in the composite key:

```
RocksDB column family: "window-contents"
Key format: [keyGroupPrefix][serialized_user_key][serialized_window (16 bytes)]
Value: serialized accumulator (ReducingState) or serialized element list (ListState)
```

There is **no separate "window registry"** — a window exists if and only if there is state for some (key, window) pair. The window is implicitly created on first `windowState.add()` and implicitly removed on `windowState.clear()`.

### After assignWindows: Two Paths

After getting the collection of windows, `processElement()` branches based on whether the assigner is a `MergingWindowAssigner` (line 303):

```
processElement(element)
    │
    ├─ assignWindows() → Collection<W>
    │
    ├─ if (windowAssigner instanceof MergingWindowAssigner)   ← line 303
    │   └─ MERGING PATH (session windows)                     ← lines 304-403
    │
    └─ else
        └─ NON-MERGING PATH (tumbling/sliding windows)       ← lines 404-432
```

### Non-Merging Path (Tumbling / Sliding)

**Lines 404-432.** This is the simpler path — each assigned window is processed independently.

```java
for (W window : elementWindows) {
    // Gate: skip if window's cleanup time has passed
    if (isWindowLate(window)) {
        continue;
    }
    isSkippedElement = false;

    // 1. Set namespace to this window
    windowState.setCurrentNamespace(window);
    // 2. Add element to this window's state in RocksDB
    windowState.add(element.getValue());

    // 3. Ask trigger what to do
    triggerContext.key = key;
    triggerContext.window = window;
    TriggerResult triggerResult = triggerContext.onElement(element);

    // 4. If trigger says fire → read state and emit
    if (triggerResult.isFire()) {
        ACC contents = windowState.get();
        if (contents != null) {
            emitWindowContents(window, contents);
        }
    }

    // 5. If trigger says purge → delete state
    if (triggerResult.isPurge()) {
        windowState.clear();
    }

    // 6. Register/update cleanup timer for this window
    registerCleanupTimer(window);
}
```

Key points:
- **No window merging** — the window from `assignWindows()` is used directly as the namespace
- **No merge metadata** — no `MergingWindowSet`, no state window indirection
- Each window is independent: own state, own trigger evaluation, own cleanup timer
- For sliding windows, the loop runs `size/slide` times per element

### Merging Path (Session Windows)

**Lines 303-403.** This path handles `MergingWindowAssigner` (e.g., `EventTimeSessionWindows`).

#### How Session Windows Work

`EventTimeSessionWindows.assignWindows()` creates a **preliminary window** `[timestamp, timestamp + gap)` for each element. These preliminary windows may overlap with existing windows and must be merged.

#### The MergingWindowSet

`MergingWindowSet` (line 304) maintains a `Map<W, W>` mapping from **actual window** → **state window**:

- **Actual window**: The logical merged window that grows as sessions merge (e.g., `[100, 250)`)
- **State window**: The RocksDB namespace where data is physically stored. Chosen once from one of the original windows (e.g., `[100, 150)`) and never changes, to avoid moving data

```java
MergingWindowSet<W> mergingWindows = getMergingWindowSet();
```

#### addWindow and Merge

```java
W actualWindow = mergingWindows.addWindow(window, mergeFunction);
```

`addWindow()` collects all existing windows + the new window, calls `assigner.mergeWindows()`, which uses `TimeWindow.mergeWindows()` to find overlapping windows. If the new window overlaps existing ones:

1. A merged window is computed via `cover()`: `[min(starts), max(ends))`
2. One existing state window is chosen as the physical namespace for the merged result
3. The `MergeFunction` callback (lines 314-365) is invoked:

```java
// line 353: notify trigger about merge
triggerContext.onMerge(mergedWindows);

// line 355-358: clear trigger state and timers for old windows
for (W m : mergedWindows) {
    triggerContext.window = m;
    triggerContext.clear();
    deleteCleanupTimer(m);
}

// line 363-364: merge state from old namespaces into the target namespace
windowMergingState.mergeNamespaces(stateWindowResult, mergedStateWindows);
```

`mergeNamespaces()` in RocksDB (e.g., `RocksDBReducingState:101-151`):
- For each source namespace: `GET` value, `DELETE` the key
- Reduce all source values together
- Merge with target value (if exists)
- `PUT` final result to target namespace

After merge, the element is added to state via the state window:

```java
W stateWindow = mergingWindows.getStateWindow(actualWindow);  // line 375
windowState.setCurrentNamespace(stateWindow);                  // line 381
windowState.add(element.getValue());                           // line 382
```

Finally, `mergingWindows.persist()` (line 403) saves the updated mapping to a `ListState<Tuple2<W,W>>`.

#### Key Difference: State Window Indirection

| | Non-Merging | Merging |
|---|---|---|
| Namespace used | The window itself | A "state window" (may differ from actual window) |
| Window identity | Fixed at assignment | Changes as sessions merge |
| Extra state | None | `mergingSetsState` — `ListState<Tuple2<W,W>>` mapping |
| State migration | Never needed | `mergeNamespaces()` moves data between namespaces |
| Cleanup | `clearAllState()` | `clearAllState()` + `mergingWindows.retireWindow()` |

### When Is Window State Removed?

Window state is removed in **two scenarios**:

#### (a) Purging Trigger — Immediate Removal After Fire

```java
// line 428-429 (non-merging), line 396-397 (merging)
if (triggerResult.isPurge()) {
    windowState.clear();  // RocksDB DELETE for this (key, window) entry
}
```

Only with `PurgingTrigger` or custom triggers that return `FIRE_AND_PURGE`.

#### (b) Cleanup Timer — Final Removal

The cleanup timer fires at `window.maxTimestamp() + allowedLateness` in `onEventTime()` (line 485-488):

```java
if (windowAssigner.isEventTime()
        && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
    clearAllState(triggerContext.window, windowState, mergingWindows);
}
```

`clearAllState()` (line 560-571):
```java
windowState.clear();          // RocksDB DELETE — window contents
triggerContext.clear();        // delete trigger's registered timer
processContext.clear();        // user function cleanup callback
if (mergingWindows != null) {
    mergingWindows.retireWindow(window);  // remove from merge mapping
    mergingWindows.persist();             // save updated mapping
}
```

This is the **definitive removal**. After this, the (key, window) entry is gone from RocksDB and any subsequent elements for this window are rejected by `isWindowLate()`.

### RocksDB Checkpoint Consistency: How Async Snapshots Stay Correct

Like the heap backend, RocksDB checkpoints are split into a **synchronous** and **asynchronous** phase. But RocksDB achieves snapshot isolation very differently — through **immutable SST files and hard links** rather than copy-on-write.

#### The Problem

Checkpointing must capture a consistent point-in-time view of all window state. But the checkpoint upload (to S3) is slow, so it runs asynchronously. Meanwhile, the processing thread keeps modifying state — adding elements to windows, reducing values, clearing expired windows. How does the snapshot stay consistent?

#### Phase 1: Synchronous — Native Checkpoint (Processing Paused)

**`RocksDBSnapshotStrategyBase.syncPrepareResources()`** (line 151-165):

```java
// 1. Prepare local snapshot directory
SnapshotDirectory snapshotDirectory = prepareLocalSnapshotDirectory(checkpointId);
// 2. Snapshot metadata
PreviousSnapshot previousSnapshot = snapshotMetaData(checkpointId, stateMetaInfoSnapshots);
// 3. Create RocksDB native checkpoint
takeDBNativeCheckpoint(snapshotDirectory);
```

**`takeDBNativeCheckpoint()`** (line 170-184):
```java
Checkpoint checkpoint = Checkpoint.create(db);
checkpoint.createCheckpoint(outputDirectory.getDirectory().toString());
```

This calls RocksDB's native `Checkpoint` API, which creates **hard links** to all current SST files. This is the critical consistency mechanism:

- **SST files are immutable** — once an SST file is written by RocksDB (via memtable flush or compaction), it is never modified. It can only be deleted (after compaction creates a replacement).
- **Hard links** create a second filesystem reference to the same inode. The hard-linked files in the snapshot directory point to the exact same data blocks on disk as the live SST files.
- Even if RocksDB later deletes the original SST file (e.g., after compaction), the hard link keeps the data alive. The OS only frees the disk blocks when all references (including hard links) are removed.

**This phase is fast** — creating hard links is O(number_of_files), not O(data_size). No data is copied. The processing thread is paused only for this brief operation.

#### Phase 2: Asynchronous — Upload (Processing Resumes)

After `syncPrepareResources()` returns, the processing thread **immediately resumes**. New writes go to RocksDB's memtable, which eventually flushes to **new** SST files. These new files are completely separate from the snapshot's hard-linked files.

Meanwhile, a background thread runs `asyncSnapshot()` → `RocksDBIncrementalSnapshotOperation.get()`:
1. Lists all files in the snapshot directory (the hard-linked SST files)
2. Compares against previously uploaded files (incremental checkpoint)
3. Uploads new SST files to S3

The snapshot thread reads from the hard-linked files. The processing thread writes to live RocksDB. They operate on **different file sets** — no conflict, no locks, no copy-on-write needed.

#### Why This Works: SST Immutability

```
Timeline:

[SYNC]  Processing paused
        RocksDB state: memtable + SST files {001.sst, 002.sst, 003.sst}
        Checkpoint.createCheckpoint() → hard links to {001.sst, 002.sst, 003.sst}
        Snapshot dir: {001.sst → inode_A, 002.sst → inode_B, 003.sst → inode_C}

[ASYNC] Processing resumes
        Processing thread writes: new data → memtable → flush → 004.sst (NEW file)
        Compaction: merges 001.sst + 002.sst → 005.sst, deletes 001.sst and 002.sst

        But: snapshot dir still has hard links to inode_A (was 001.sst) and inode_B (was 002.sst)
             These inodes are NOT freed because hard links still reference them

        Snapshot thread: reads {001.sst, 002.sst, 003.sst} from snapshot dir → consistent!
        Uploads to S3 → checkpoint complete

[DONE]  Snapshot dir cleaned up → hard links removed → inodes freed (if no other references)
```

#### Contrast: RocksDB vs Heap Backend

| | RocksDB | Heap (CopyOnWriteStateMap) |
|---|---|---|
| Snapshot isolation | SST file immutability + hard links | Version-based copy-on-write |
| Sync phase cost | O(num_files) hard link creation | O(array_length) pointer copy |
| Async phase safety | Separate file sets (hard links vs new SSTs) | Lazy deep copy on write |
| Data copying | None during snapshot | Copies entries/state on first write after snapshot |
| Memory overhead | Snapshot dir on disk (hard links, no extra space) | Copied entries/state objects in JVM heap |
| Processing pause | Brief (hard link creation) | Brief (array copy + version bump) |

Both approaches achieve the same goal — a consistent point-in-time snapshot that doesn't block processing — but through fundamentally different mechanisms. RocksDB leverages the filesystem's immutable-file semantics; the heap backend implements its own versioned copy-on-write in Java.

### SharedStateRegistry: How S3 SST Files Are Cleaned Up

After the async upload, SST files live on S3. They are cleaned up via **reference tracking** in the `SharedStateRegistry` on the JobManager.

#### Data Structure

**File:** `SharedStateRegistryImpl.java`

```java
// line 55: The registry — one entry per unique SST file
Map<SharedStateRegistryKey, SharedStateEntry> registeredStates;

// lines 300-344: Each entry tracks lifecycle
class SharedStateEntry {
    StreamStateHandle stateHandle;          // S3 file handle
    final long createdByCheckpointID;       // which checkpoint first uploaded this file
    long lastUsedCheckpointID;              // latest checkpoint still referencing this file
}
```

The key is derived from the S3 file path (UUID hash). The value tracks **which checkpoint range** needs this file.

#### Registration Flow: Checkpoint Acknowledged → SST Files Registered

When a TaskManager acknowledges a checkpoint, the JobManager registers the shared SST files:

```
CheckpointCoordinator (line 1235-1243)
  → IncrementalRemoteKeyedStateHandle.registerSharedStates(registry, checkpointID)
    → for each SST file handle:
        registry.registerReference(key, handle, checkpointID)
```

Inside `registerReference()` (lines 81-156):

```java
synchronized (registeredStates) {
    entry = registeredStates.get(registrationKey);

    if (entry == null) {
        // NEW file — create entry
        entry = new SharedStateEntry(newHandle, checkpointID);
        registeredStates.put(registrationKey, entry);
    } else {
        // EXISTING file reused by a newer checkpoint — update lastUsed
        entry.advanceLastUsingCheckpointID(checkpointID);
        // If caller sent a PlaceholderStreamStateHandle, return the REAL handle
        // (avoids duplicate S3 references)
    }
}
```

**PlaceholderStreamStateHandle:** When a TaskManager reuses an SST file from a previous checkpoint, it sends a lightweight `PlaceholderStreamStateHandle` instead of the full S3 handle. The registry resolves it to the actual handle already stored — avoiding duplicate tracking.

#### Cleanup Flow: Old Checkpoint Subsumed → Unreferenced SST Files Deleted

When a new checkpoint completes and old ones are subsumed:

```
DefaultCompletedCheckpointStore.addCheckpointAndSubsumeOldestOne() (lines 124-158):
  1. Add new checkpoint to deque
  2. Subsume old checkpoints exceeding retention limit
  3. Call: registry.unregisterUnusedState(lowestRetainedCheckpointID)
```

Inside `unregisterUnusedState()` (lines 159-195):

```java
synchronized (registeredStates) {
    Iterator<SharedStateEntry> it = registeredStates.values().iterator();
    while (it.hasNext()) {
        SharedStateEntry entry = it.next();
        if (entry.lastUsedCheckpointID < lowestCheckpointID) {
            // No retained checkpoint references this file anymore
            subsumed.add(entry.stateHandle);
            it.remove();  // remove from registry
        }
    }
}
// Delete files asynchronously
for (StreamStateHandle handle : subsumed) {
    scheduleAsyncDelete(handle);  // → handle.discardState() → S3 DELETE
}
```

The actual S3 deletion happens asynchronously via `handle.discardState()`, which issues an S3 DELETE operation for the file.

#### Concrete Example: SST File Lifecycle Across 3 Checkpoints

```
state.checkpoints.num-retained = 1  (default: keep only latest)

═══════════════════════════════════════════════════════════════════════
Checkpoint 1 completes:
═══════════════════════════════════════════════════════════════════════
  Uploaded to S3: 001.sst, 002.sst, 003.sst
  Registry:
    001.sst → {created=1, lastUsed=1}
    002.sst → {created=1, lastUsed=1}
    003.sst → {created=1, lastUsed=1}

═══════════════════════════════════════════════════════════════════════
Checkpoint 2 completes (RocksDB compacted 001.sst away, added 004.sst):
═══════════════════════════════════════════════════════════════════════
  SST files in this checkpoint: 002.sst (reuse), 003.sst (reuse), 004.sst (new)
  Registration:
    002.sst → {created=1, lastUsed=2}   ← lastUsed advanced from 1→2
    003.sst → {created=1, lastUsed=2}   ← lastUsed advanced from 1→2
    004.sst → {created=2, lastUsed=2}   ← new entry
    001.sst → {created=1, lastUsed=1}   ← NOT updated (not in checkpoint 2)

  Subsumption: checkpoint 1 subsumed, lowestRetained = 2
  unregisterUnusedState(2):
    001.sst: lastUsed(1) < 2? YES → DELETE from S3 ✓
    002.sst: lastUsed(2) < 2? NO  → keep
    003.sst: lastUsed(2) < 2? NO  → keep
    004.sst: lastUsed(2) < 2? NO  → keep

  S3 after cleanup: 002.sst, 003.sst, 004.sst

═══════════════════════════════════════════════════════════════════════
Checkpoint 3 completes (RocksDB compacted 002+003 → 005.sst):
═══════════════════════════════════════════════════════════════════════
  SST files in this checkpoint: 004.sst (reuse), 005.sst (new)
  Registration:
    004.sst → {created=2, lastUsed=3}   ← advanced
    005.sst → {created=3, lastUsed=3}   ← new entry
    002.sst → {created=1, lastUsed=2}   ← not updated
    003.sst → {created=1, lastUsed=2}   ← not updated

  Subsumption: checkpoint 2 subsumed, lowestRetained = 3
  unregisterUnusedState(3):
    002.sst: lastUsed(2) < 3? YES → DELETE from S3 ✓
    003.sst: lastUsed(2) < 3? YES → DELETE from S3 ✓
    004.sst: lastUsed(3) < 3? NO  → keep
    005.sst: lastUsed(3) < 3? NO  → keep

  S3 after cleanup: 004.sst, 005.sst
```

**Result:** At any point, S3 only holds SST files referenced by retained checkpoints. Old files are garbage-collected as soon as no retained checkpoint references them. With `num-retained=1`, each checkpoint completion triggers deletion of unreferenced files from the previous checkpoint.

---

## 10. Concrete Example: Sum-by-Key with a Tumbling Window

### Setup

```java
stream
    .keyBy(event -> event.key)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .allowedLateness(Duration.ofSeconds(30))
    .reduce((a, b) -> new Event(a.key, a.value + b.value))  // SUM
```

This creates:
- `windowAssigner` = `TumblingEventTimeWindows` (size=60000)
- `windowStateDescriptor` = `ReducingStateDescriptor` with sum reduce function
- `trigger` = `EventTimeTrigger` (default, returns `FIRE` not `FIRE_AND_PURGE`)
- `allowedLateness` = 30000ms

Window state in RocksDB uses `RocksDBReducingState`:
```java
// RocksDBReducingState.add() — line 93-98
public void add(V value) throws Exception {
    byte[] key = getKeyBytes();                              // composite key
    V oldValue = getInternal(key);                           // RocksDB GET
    V newValue = oldValue == null ? value : reduceFunction.reduce(oldValue, value);
    updateInternal(key, newValue);                            // RocksDB PUT
}
```

### Scenario Timeline

```
Current watermark: 30000 (30 seconds)
Key: "userA"
Window: [0, 60000) → maxTimestamp=59999, cleanupTime=89999
```

### Case 1: Element Arrives for an EXISTING Window

**Event:** `(key="userA", value=10, timestamp=25000)`

```
WindowOperator.processElement():

1. assignWindows(element, 25000, ctx)
   → getWindowStartWithOffset(25000, 0, 60000) = 0
   → returns [TimeWindow(0, 60000)]

2. for (window = [0, 60000)):
   isWindowLate? cleanupTime(89999) <= watermark(30000)? NO → proceed

3. windowState.setCurrentNamespace(TimeWindow[0, 60000])

4. windowState.add(Event("userA", 10))
   → RocksDBReducingState.add():
     key_bytes = [KG|"userA"|0|60000]
     oldValue = getInternal(key_bytes)
       → backend.db.get(columnFamily, key_bytes) → Event("userA", 50)  ← EXISTING STATE
     newValue = reduceFunction.reduce(Event("userA", 50), Event("userA", 10))
              = Event("userA", 60)
     updateInternal(key_bytes, Event("userA", 60))
       → backend.db.put(columnFamily, writeOptions, key_bytes, serialize(Event("userA", 60)))

   RocksDB state: [KG|"userA"|0|60000] → Event("userA", 60)

5. triggerContext.onElement(element)
   → EventTimeTrigger: 59999 <= 30000? NO → registerEventTimeTimer(59999)
   → returns CONTINUE

6. registerCleanupTimer([0, 60000))
   → registerEventTimeTimer(89999)
```

**Result:** State updated from 50 → 60 via reduce. No fire yet (watermark hasn't passed window).

### Case 2: Element Arrives for a NEW Window

**Event:** `(key="userA", value=7, timestamp=65000)`

```
WindowOperator.processElement():

1. assignWindows(element, 65000, ctx)
   → getWindowStartWithOffset(65000, 0, 60000) = 60000
   → returns [TimeWindow(60000, 120000)]    ← NEW window, never seen before

2. for (window = [60000, 120000)):
   isWindowLate? cleanupTime = 119999 + 30000 = 149999
                 149999 <= 30000? NO → proceed

3. windowState.setCurrentNamespace(TimeWindow[60000, 120000])

4. windowState.add(Event("userA", 7))
   → RocksDBReducingState.add():
     key_bytes = [KG|"userA"|60000|120000]
     oldValue = getInternal(key_bytes)
       → backend.db.get(columnFamily, key_bytes) → null  ← NO EXISTING STATE
     newValue = (oldValue == null) ? Event("userA", 7) : ...
              = Event("userA", 7)
     updateInternal(key_bytes, Event("userA", 7))
       → backend.db.put(columnFamily, writeOptions, key_bytes, serialize(Event("userA", 7)))

   RocksDB state: [KG|"userA"|60000|120000] → Event("userA", 7)  ← NEW ENTRY

5. triggerContext.onElement(element)
   → EventTimeTrigger: 119999 <= 30000? NO → registerEventTimeTimer(119999)
   → returns CONTINUE

6. registerCleanupTimer([60000, 120000))
   → registerEventTimeTimer(149999)
```

**Result:** New RocksDB entry created. The window "appears" simply because a key now exists for it — there is no explicit window creation step.

### Case 3: Element Arrives for a LATE Window

**Assume watermark has advanced to 95000.**

**Event:** `(key="userA", value=3, timestamp=5000)` — very late, falls in window [0, 60000)

```
WindowOperator.processElement():

1. assignWindows(element, 5000, ctx)
   → getWindowStartWithOffset(5000, 0, 60000) = 0
   → returns [TimeWindow(0, 60000)]    ← assignWindows ALWAYS assigns, ignores watermark

2. for (window = [0, 60000)):
   isWindowLate? cleanupTime = 59999 + 30000 = 89999
                 89999 <= 95000? YES → LATE
   → continue (skip this window)

   isSkippedElement remains true.

3. After the for loop (line 440-446):
   isSkippedElement=true, check isElementLate:
     5000 + 30000 = 35000 <= 95000? YES → element is late

   lateDataOutputTag != null?
     YES → output.collect(lateDataOutputTag, element)  → SIDE OUTPUT
     NO  → numLateRecordsDropped.inc()                 → SILENTLY DROPPED

   NO RocksDB operation at all. State is not touched.
```

**Result:** Element never reaches RocksDB. The window's state was already deleted by the cleanup timer at watermark=89999. The element goes to side output or is dropped.

### What Happened to Window [0, 60000) Between Case 1 and Case 3?

```
watermark=30000: Case 1 — element added, state = Event("userA", 60)

watermark advances to 59999:
  → Timer 59999 fires (trigger timer)
  → EventTimeTrigger.onEventTime(59999) → FIRE
  → windowState.get() → Event("userA", 60)
  → emitWindowContents → emit downstream: Event("userA", 60)
  → isPurge = false → state KEPT
  → isCleanupTime(59999)? cleanupTime=89999 ≠ 59999 → NO → state survives

watermark=60000..89998:
  → Late elements could arrive and be admitted (within allowedLateness)
  → Each would do: GET → reduce → PUT → immediate re-FIRE

watermark advances to 89999:
  → Timer 89999 fires (cleanup timer)
  → EventTimeTrigger.onEventTime(89999) → 89999 ≠ 59999 → CONTINUE (no emit)
  → isCleanupTime(89999)? YES → clearAllState():
      windowState.clear()
        → backend.db.delete(columnFamily, writeOptions, [KG|"userA"|0|60000])
      triggerContext.clear()
        → delete timer at 59999
  → Window [0, 60000) is GONE from RocksDB

watermark=95000: Case 3 — isWindowLate returns true, element rejected
```

### RocksDB State Snapshot Across the Timeline

```
watermark    RocksDB key                      Value
─────────    ─────────────────────────────    ──────────────────
  30000      [KG|"userA"|0|60000]             Event("userA", 60)
             [KG|"userA"|60000|120000]        Event("userA", 7)

  59999      [KG|"userA"|0|60000]             Event("userA", 60)  ← FIRED, state kept
             [KG|"userA"|60000|120000]        Event("userA", 7)

  89999      [KG|"userA"|60000|120000]        Event("userA", 7)
             (window [0,60000) DELETED by cleanup timer)

  95000      [KG|"userA"|60000|120000]        Event("userA", 7)
             (Case 3 element REJECTED — never touches RocksDB)
```

---

## 11. Heap State Backend: In-Memory Window State

Sections 1-10 describe the RocksDB state backend. When using the **heap (HashMapStateBackend)** state backend, window state lives entirely in JVM memory with a different data structure.

### Data Structure Hierarchy

```
HeapKeyedStateBackend
  └─ registeredKVStates: Map<String, StateTable<K, ?, ?>>
       └─ StateTable<K, N, S>  (one per state name, e.g., "window-contents")
            └─ keyGroupedStateMaps: StateMap<K, N, S>[]  (array indexed by key group)
                 └─ CopyOnWriteStateMap<K, N, S>  (one per key group)
                      └─ primaryTable: StateMapEntry<K, N, S>[]  (hash directory)
                           └─ StateMapEntry: { key, namespace, state, next, hash,
                                               entryVersion, stateVersion }
```

**Files:**
- `HeapKeyedStateBackend.java` — state factory and registration
- `StateTable.java` — abstract wrapper with key-group-indexed array
- `CopyOnWriteStateMap.java` — the actual hash map implementation

### CopyOnWriteStateMap: Flat Hash Map

**File:** `CopyOnWriteStateMap.java`

Unlike the alternative nested-map approach `(namespace → key → state)`, CopyOnWriteStateMap uses a **flat design** (line 63):

```
Flattened: (key, namespace) → state
```

All entries for all keys and all windows (namespaces) live in one flat hash table per key group.

**Hash function** (line 747-751):
```java
private static int compositeHash(Object key, Object namespace) {
    return MathUtils.bitMix(key.hashCode() ^ namespace.hashCode());
}
```

XORs key and namespace hash codes, then applies bit-mixing for better distribution. This is the **only hashing step** — no byte serialization like RocksDB.

**Lookup** (line 275-278): traverses the collision chain comparing with Java `.equals()`:
```java
if (e.hash == hash && key.equals(eKey) && namespace.equals(eNamespace))
```

For window state, `namespace` is a `TimeWindow` object. Lookup uses `TimeWindow.equals()` which compares `start` and `end` fields — no serialization overhead.

### StateMapEntry: The Entry Object

**Lines 817-878:**

```java
protected static class StateMapEntry<K, N, S> {
    @Nonnull final K key;            // user key (immutable)
    @Nonnull final N namespace;      // window (immutable)
    @Nullable S state;               // accumulator/list (mutable for COW)
    @Nullable StateMapEntry<K, N, S> next;  // collision chain
    final int hash;                  // cached composite hash

    int entryVersion;    // COW metadata: when was this entry struct last copied?
    int stateVersion;    // COW metadata: when was the state object last copied?
}
```

Collision resolution: **separate chaining** via the `next` pointer.

**Namespace dedup optimization** (line 577-582): if `namespace.equals(lastNamespace)`, reuse the same object reference to reduce memory — since `TimeWindow` objects for the same window are created repeatedly.

```java
if (namespace.equals(lastNamespace)) {
    namespace = lastNamespace;      // reuse existing object
} else {
    lastNamespace = namespace;
}
```

### Incremental Rehashing

When the table exceeds 75% load (`threshold = capacity * 3/4`), the map doubles capacity. But rather than rehashing everything at once (which would cause a latency spike), it uses **incremental rehashing**:

- Two table arrays: `primaryTable` (old) and `incrementalRehashTable` (new, 2x size)
- On each operation (`computeHashForOperationAndDoIncrementalRehash`), a few entries are migrated from primary to incremental table
- `rehashIndex` tracks progress through the primary table
- `selectActiveTable(hash)` routes lookups to the correct table based on whether the hash's bucket has been migrated
- When migration completes, `incrementalRehashTable` becomes the new `primaryTable`

This amortizes the rehash cost across regular operations.

### Copy-on-Write: How Checkpoints Work with a Single Thread

**Problem:** Flink checkpoints run **asynchronously** — a snapshot thread serializes state to the checkpoint while the processing thread continues modifying state. With heap state, both threads share the same in-memory objects. How to prevent corruption?

**Solution:** Version-based copy-on-write (COW). The key insight is that copies are made **lazily** — only when the processing thread actually touches an entry that a snapshot still needs.

#### Snapshot Creation (sync phase, processing thread)

```java
// CopyOnWriteStateMap.snapshotMapArrays() — lines 476-536
synchronized (snapshotVersions) {
    ++stateMapVersion;                                    // bump version
    highestRequiredSnapshotVersion = stateMapVersion;     // mark for COW
    snapshotVersions.add(stateMapVersion);                // register snapshot
}
// Copy the hash table ARRAY (O(array_length) pointer copy — NOT deep copy)
StateMapEntry<K, N, S>[] copy = new StateMapEntry[copiedArraySize];
System.arraycopy(primaryTable, 0, copy, 0, primaryTable.length);
```

This is **very fast**: only copies the array of pointers, not the entries themselves. The snapshot thread gets a frozen view of the hash table structure.

#### Processing Continues (processing thread modifies state)

When the processing thread reads or modifies an entry that was part of a snapshot:

**Reading state** (`get()`, lines 281-288):
```java
if (e.stateVersion < requiredVersion) {
    if (e.entryVersion < requiredVersion) {
        e = handleChainedEntryCopyOnWrite(tab, index, e);  // copy the entry struct
    }
    e.stateVersion = stateMapVersion;
    e.state = getStateSerializer().copy(e.state);           // deep copy the state object
}
return e.state;
```

**Modifying via transform** (`transform()`, lines 365-379):
```java
StateMapEntry<K, N, S> entry = putEntry(key, namespace);
entry.state = transformation.apply(
    (entry.stateVersion < highestRequiredSnapshotVersion)
        ? getStateSerializer().copy(entry.state)  // deep copy before modifying
        : entry.state,                             // no snapshot cares — modify in place
    value);
entry.stateVersion = stateMapVersion;
```

**Chain modification** (`putEntry()`, lines 394-397):
```java
if (e.entryVersion < highestRequiredSnapshotVersion) {
    e = handleChainedEntryCopyOnWrite(tab, index, e);  // copy chain entry
}
```

#### Two Levels of COW

| Level | Version Field | What's Copied | When |
|---|---|---|---|
| Entry structure | `entryVersion` | The `StateMapEntry` object (key, namespace, next pointer) | When modifying `next` chain pointers |
| State value | `stateVersion` | The state object (e.g., the accumulated value) via `stateSerializer.copy()` | When reading/modifying the state value |

This two-level scheme minimizes copying: if only the state value changes but the chain structure doesn't, only the state is copied (not the entire entry). If neither has been touched since the snapshot, both are copied on first access.

#### Snapshot Release

When the checkpoint completes:
```java
// releaseSnapshot() — lines 457-468
synchronized (snapshotVersions) {
    snapshotVersions.remove(snapshotVersion);
    highestRequiredSnapshotVersion =
        snapshotVersions.isEmpty() ? 0 : snapshotVersions.last();
}
```

Once `highestRequiredSnapshotVersion` drops to 0 (or below the entry's version), no more COW copies are needed — modifications are in-place again.

**The `synchronized` block** on `snapshotVersions` is the **only lock** in the entire system. It's used only during snapshot creation and release (both very fast operations). All data-path operations (get, put, transform) are **lock-free** — they just read `highestRequiredSnapshotVersion` which is a plain int field (stale reads are acceptable here, as noted in line 459).

### NOT Thread-Safe for Concurrent Writes

CopyOnWriteStateMap is **single-writer** by design. Flink guarantees that only one thread (the task's processing thread) modifies state at any time. The COW mechanism protects against concurrent *reads* by the snapshot thread, not concurrent writes. The `modCount` field (line 199) enables best-effort `ConcurrentModificationException` detection during iteration, but there are no write locks.

### Heap State Operations for Window State

**HeapReducingState.add()** (line 90-98):
```java
stateTable.transform(currentNamespace, value, reduceTransformation);
// → CopyOnWriteStateMap.transform() → putEntry() → apply reduceFunction
```
Single call to `transform()` — atomic get-reduce-put within the map.

**HeapListState.add()** (lines 92-105):
```java
StateMap<K, N, List<V>> map = stateTable.getMapForKeyGroup(...);
List<V> list = map.get(key, namespace);   // get existing list
if (list == null) {
    list = new ArrayList<>();
    map.put(key, namespace, list);
}
list.add(value);                          // append to ArrayList
```
Direct `ArrayList.add()` — O(1) amortized.

**HeapAggregatingState.add()** (lines 94-102):
```java
stateTable.transform(currentNamespace, value, aggregateTransformation);
// → applies aggFunction.add(value, accumulator) atomically
```
Same transform pattern as ReducingState.

### Comparison: Heap vs RocksDB State Backend

```
                          Heap (HashMapStateBackend)           RocksDB
─────────────────────     ─────────────────────────────        ─────────────────────────
Data structure            CopyOnWriteStateMap (flat HashMap)   LSM-tree (memtable + SST files)
Key format                Java objects (key, namespace)        Byte array: [KG|key|namespace]
Lookup                    Object.equals() + hashCode()         Byte comparison in sorted tree
Namespace (window)        TimeWindow object reference           16 bytes serialized (start+end)

Write path                HashMap put/transform (in-memory)    db.put/merge (memtable → SST)
Read path                 HashMap get (in-memory)              db.get (memtable/block cache/SST)
Delete path               HashMap remove (immediate)           db.delete (tombstone → compaction)

Serialization per access  None                                 Serialize key+namespace+value
State size limit           JVM heap                             Disk (unbounded)

Checkpoint mechanism      COW: array copy + lazy deep copy     Native checkpoint (hard link SST)
Checkpoint cost            O(entries) serialization             O(new SST files) upload
Snapshot isolation         Version-based copy-on-write          Immutable SST files

Threading                  Single-writer, COW for snapshot      Single-writer, native snapshot
Lock contention            snapshotVersions (rare)              RocksDB internal locks

Best for                   Small-to-medium state, low latency   Large state, spill to disk
```

---

## 12. Diagnostic Guide: Why RocksDB Local Disk and S3 Keep Growing

### Local Disk Growing

#### Cause 1: State itself is growing (most common)

The live RocksDB working directory grows because the actual state volume increases:

- **Sliding windows** store each element `size/slide` times. A 1-hour window with 1-second slide = 3600x duplication per element.
- **Large `allowedLateness`** keeps fired windows alive in RocksDB for longer.
- **Ever-growing key cardinality** — if new keys keep appearing (e.g., unique session IDs, device IDs), there are always more active (key, window) pairs even though individual windows get cleaned up.
- **`.process()` / `.apply()` with ListState** — stores ALL raw elements per (key, window). State per window grows as O(N) with element count. Use `.aggregate()` or `.reduce()` for O(1) state per window.

**Monitor:** RocksDB metrics `rocksdb.estimate-live-data-size`, `rocksdb.cur-size-all-mem-tables`.

#### Cause 2: RocksDB compaction lag + tombstones

When `windowState.clear()` is called, RocksDB writes a **tombstone** — actual disk space is only freed during compaction. Under heavy write load, compaction may lag behind, causing temporary disk growth.

**Monitor:** `rocksdb.num-running-compactions`, `rocksdb.estimate-pending-compaction-bytes`.

#### Cause 3: Local recovery enabled

```yaml
# Check flink-conf.yaml:
state.backend.local-recovery: true
```

If enabled, Flink keeps **permanent** copies of checkpoint data on local disk for fast recovery. These are cleaned up by `TaskLocalStateStoreImpl.confirmCheckpoint()`, which prunes all local state with checkpoint ID < confirmed checkpoint. So local recovery dirs should stay bounded at ~1 checkpoint.

**However**, if the job crashes before `confirmCheckpoint()` runs, or the TaskManager restarts with a different slot allocation ID, old local recovery dirs can be orphaned.

### S3 Growing

#### Cause 4: `num-retained` > 1

```yaml
# Default is 1:
state.checkpoints.num-retained: 1
```

With N retained checkpoints, N sets of exclusive files (MANIFEST, CURRENT, OPTIONS) exist on S3 plus all shared SST files referenced by any retained checkpoint. More retained checkpoints = more S3 storage.

#### Cause 5: Savepoints accumulating

Savepoints use `CheckpointedStateScope.EXCLUSIVE` — their files are **never shared** and **never auto-cleaned**. Every savepoint you trigger stays on S3 forever until you manually delete it.

#### Cause 6: Failed/aborted checkpoints leaving orphaned files

If a checkpoint is partially uploaded to S3 then aborted, the already-uploaded SST files may not be cleaned up. `RocksIncrementalSnapshotStrategy.notifyCheckpointAborted()` removes tracking entries but **files already on S3 may be orphaned** if the `SharedStateRegistry` never registered them (registration happens on JM acknowledgment, which doesn't happen for aborted checkpoints).

This was more prevalent in older Flink versions (before 1.15/1.16). Check JM logs for frequent checkpoint failures.

#### Cause 7: Async cleanup failure

`SharedStateRegistryImpl.scheduleAsyncDelete()` can silently fail if the executor pool is shutting down:
```java
} catch (RejectedExecutionException ex) {
    // TODO This is a temporary fix...
    if (!isDisposed) {
        LOG.warn("...", ex);
    }
}
```
Files that failed to delete remain on S3 as orphans.

### The Subtle Trap: Keyed State Outside Windows

If your pipeline uses `RichFunction` state alongside windows, that state is **NOT scoped to the window** and is never cleaned up by window lifecycle:

```java
// WRONG — this state lives forever per key, NOT per window
stream
    .keyBy(e -> e.userId)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .aggregate(new RichAggregateFunction<Event, Acc, Result>() {

        private ValueState<Long> globalCounter;  // VoidNamespace — never cleaned by window

        @Override
        public void open(Configuration config) {
            globalCounter = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("counter", Long.class));
        }

        @Override
        public Acc add(Event value, Acc acc) {
            Long count = globalCounter.value();
            globalCounter.update(count == null ? 1L : count + 1);  // grows forever
            return acc;
        }
    });
```

The difference:
- **Window state** (accumulator from `.aggregate()`/`.reduce()`) → uses window as namespace → cleaned up by `clearAllState()`
- **Rich function state** (`getRuntimeContext().getState()`) → uses `VoidNamespace` → **lives forever per key**, never cleaned by window lifecycle

Per-window state via `context.windowState()` in `ProcessWindowFunction` IS safe — it's cleaned up with the window.

### Diagnostic Checklist

```
1. Flink version? (older versions had SharedStateRegistry bugs)
2. Local recovery enabled? (state.backend.local-recovery)
3. num-retained setting? (state.checkpoints.num-retained)
4. Savepoints accumulating on S3?
5. Window type? Sliding windows multiply state by size/slide
6. allowedLateness value? Extends state lifetime
7. Key cardinality growing? New keys = new active windows
8. Using .process()/.apply()? Switch to .aggregate()/.reduce() for O(1) state
9. Using getRuntimeContext().getState() in window functions? → grows forever
10. Frequent checkpoint failures? Check JM logs for abort rate
11. RocksDB compaction healthy? Monitor pending-compaction-bytes
```

---

## 13. SharedStateRegistry Bug History by Flink Version

The `SharedStateRegistry` has had several bugs that caused S3 file leaks (files never cleaned up) or premature deletion. If you're on an older Flink version, upgrading may fix your S3 growth.

| JIRA | Issue | Impact | Fixed In |
|---|---|---|---|
| **FLINK-24611** | Shared state was registered at snapshot time (on TM), not at acknowledgment time (on JM). Race condition: if checkpoint N+1 completes before N's state is registered, N's SST files could be prematurely deleted or leaked. | S3 state corruption or leaked files | **1.15** (Nov 2021) |
| **FLINK-23949** | First incremental checkpoint after restoring from savepoint degenerates into a full checkpoint. | Unnecessarily large checkpoint spikes after savepoint restore | **1.14.1** (Sep 2021) |
| **FLINK-25395** | Unconfirmed SST files (uploaded but checkpoint not yet confirmed) could be lost if the next checkpoint doesn't reference them. | Checkpoint restore failure | **1.15** (Jan 2022) |
| **FLINK-26985** | Shared state of the initial restored checkpoint could be prematurely discarded during subsumption. | Data loss on restore — restored checkpoint's SST files deleted too early | **1.15.3, 1.16** (Apr 2022) |
| **FLINK-29913** | De-duplication logic in `registerReference()` could map different physical SST files to the same registry key if they had the same content hash. | S3 file leak — files tracked under wrong key, never cleaned up | **1.18** (Jul 2023) |
| **FLINK-35784** | File-merging directories not registered with SharedStateRegistry. | S3 file leak for file-merging feature | **1.20** (Jul 2024) |

**Version risk summary:**

- **Flink <= 1.13**: SharedStateRegistry had fundamental design issues — state registered too early, cleanup races common. **High risk of S3 leaks.**
- **Flink 1.14**: FLINK-23949 fixed, but FLINK-24611 still present. **Moderate risk.**
- **Flink 1.15 - 1.15.2**: FLINK-24611 major rewrite (moved registration to JM acknowledgment), FLINK-25395 fixed. **Much improved, but FLINK-26985 existed until 1.15.3.**
- **Flink 1.15.3+, 1.16, 1.17**: FLINK-26985 fixed. **Stable for most use cases.**
- **Flink 1.18+**: FLINK-29913 fixed registry key collision. **Best shared state cleanup.**
- **Flink 1.20+**: FLINK-35784 fixed file-merging leak. **Latest fix.**

**Recommendation:** If on Flink <= 1.14, upgrade to at least **1.15.3** for stable SharedStateRegistry. If on 1.15-1.17 and seeing slow S3 growth, upgrade to **1.18+** for the FLINK-29913 key collision fix.
