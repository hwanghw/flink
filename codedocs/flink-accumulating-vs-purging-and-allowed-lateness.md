# Accumulating vs. Fire & Purging, allowedLateness, and RocksDB State Impact

Analysis based on `TwoPhaseCountDeduplicatedEventTimeV2.java` and Flink window operator internals.

---

## Key Concepts

### window.maxTimestamp()

`TimeWindow` is defined as `[start, end)` — start-inclusive, end-exclusive. `maxTimestamp()` returns the **largest timestamp that still belongs to this window** (`TimeWindow.java:84-86`):

```java
public long maxTimestamp() {
    return end - 1;
}
```

For window `[0, 60000)`: `end` = 60000, `maxTimestamp()` = 59999.

This value is used throughout the window lifecycle:
- **Trigger timer**: `EventTimeTrigger.onElement()` registers a timer at `window.maxTimestamp()` (59999) — the window fires when the watermark passes this timestamp
- **Cleanup timer**: `cleanupTime = window.maxTimestamp() + allowedLateness` (59999 + 30000 = 89999) — state is deleted when the watermark passes this timestamp
- **Output timestamp**: `emitWindowContents()` sets the output record's timestamp to `window.maxTimestamp()` — downstream operators see this as the event time of the window result

### Watermark Generation (forBoundedOutOfOrderness)

**File:** `BoundedOutOfOrdernessWatermarks.java`

```java
// line 63-64: tracks the max event timestamp seen so far
public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
    maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
}

// line 68-69: periodically emits watermark (default every 200ms)
public void onPeriodicEmit(WatermarkOutput output) {
    output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
}
```

**Formula:** `watermark = max_event_timestamp_seen - outOfOrdernessMillis - 1`

The extra `-1` ensures the watermark is strictly below the boundary. For this code with `forBoundedOutOfOrderness(Duration.ofSeconds(5))`:

```
Events: t=1000, t=2000, ..., t=70000
maxTimestamp = 70000
watermark = 70000 - 5000 - 1 = 64999
```

Note: watermark is **64999**, not 65000. This means a window with `maxTimestamp()=64999` would fire, but one with `maxTimestamp()=65000` would not yet.

Watermarks are emitted **periodically** (not per-event). The `onEvent()` call only updates the tracked max; the actual watermark emission happens when the framework calls `onPeriodicEmit()` on a timer (default `pipeline.auto-watermark-interval = 200ms`).

### Timer Storage and Firing (InternalTimerServiceImpl)

**File:** `InternalTimerServiceImpl.java`

**Timer Storage: Heap-Based Priority Queues (NOT in RocksDB)**

```java
// line 52-58
KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue;
KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue;
```

Timers are stored in **heap-based priority queues** (min-heap ordered by timestamp), not in RocksDB. They are partitioned by key group for checkpoint/restore. Each timer stores `(timestamp, key, namespace)` where namespace = the window.

**registerEventTimeTimer:**

```java
// line 249-252
public void registerEventTimeTimer(N namespace, long time) {
    eventTimeTimersQueue.add(
        new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
}
```

When `EventTimeTrigger.onElement()` calls `ctx.registerEventTimeTimer(window.maxTimestamp())`, it creates a heap entry for `(59999, key, window)`. If a timer with the same `(time, key, namespace)` already exists, the queue deduplicates it.

**advanceWatermark — How Timers Fire:**

```java
// line 328-347
public boolean tryAdvanceWatermark(long time, ...) throws Exception {
    currentWatermark = time;
    InternalTimer<K, N> timer;
    while ((timer = eventTimeTimersQueue.peek()) != null
            && timer.getTimestamp() <= time) {
        keyContext.setCurrentKey(timer.getKey());
        eventTimeTimersQueue.poll();
        triggerTarget.onEventTime(timer);   // → WindowOperator.onEventTime()
    }
}
```

When watermark advances:
1. Sets `currentWatermark = new_watermark`
2. Polls the min-heap: fires ALL timers with `timestamp <= watermark` in ascending order
3. For each timer, sets the key context and calls `WindowOperator.onEventTime(timer)`
4. Multiple timers (e.g., trigger timer at 59999 and cleanup timer at 89999) fire in order as watermark passes each

**Timer Checkpointing:** Timers are checkpointed separately from RocksDB state via `snapshotTimersForKeyGroup()` (line 356). They are serialized into the checkpoint alongside the keyed state handles but are NOT stored in RocksDB SST files.

### When allowedLateness=0, Trigger and Cleanup Share the Same Timer

When `allowedLateness=0` (default), `cleanupTime = window.maxTimestamp() + 0 = window.maxTimestamp()`. This means the trigger timer (registered by `EventTimeTrigger` at `window.maxTimestamp()`) and the cleanup timer (registered by `registerCleanupTimer()` at `cleanupTime`) are the **same timestamp**.

In `onEventTime()` (WindowOperator.java:450-488), both execute in the same callback:

```java
// Step 1: Trigger fires
TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());
if (triggerResult.isFire()) {
    ACC contents = windowState.get();       // read state
    emitWindowContents(window, contents);   // emit result
}
if (triggerResult.isPurge()) {
    windowState.clear();                    // clear if purging
}

// Step 2: Cleanup check (SAME callback, runs immediately after)
if (isCleanupTime(window, timer.getTimestamp())) {  // true when allowedLateness=0
    clearAllState(window, windowState, mergingWindows);  // delete ALL state
}
```

**Execution order:** Fire first, then cleanup. The window emits its result, then `clearAllState()` deletes everything regardless of the trigger type. This means with `allowedLateness=0`, even `FIRE` (accumulating) effectively behaves like a final fire — state is cleaned up immediately after.

**With allowedLateness > 0:** The cleanup timer is at `maxTimestamp + allowedLateness`, which is a different timestamp than the trigger timer. They fire in separate `onEventTime()` callbacks. Between the two, late elements can arrive and trigger re-fires.

### AggregateFunction vs ProcessWindowFunction State Size

The choice of window function determines the **state descriptor type** and therefore how much data is stored per (key, window) in RocksDB.

**File:** `WindowOperatorBuilder.java`

| Pattern | State Descriptor | Stored Per (key, window) | RocksDB Size |
|---|---|---|---|
| `.aggregate(aggFunc)` | `AggregatingStateDescriptor` (line 285) | Single accumulator (ACC) | **Small** — O(1) per window |
| `.aggregate(aggFunc, processFunc)` | `AggregatingStateDescriptor` (line 343) | Single accumulator (ACC) | **Small** — O(1) per window |
| `.reduce(reduceFunc)` | `ReducingStateDescriptor` (line 178) | Single reduced value | **Small** — O(1) per window |
| `.process(processFunc)` | `ListStateDescriptor` (line 398) | **ALL raw elements** as a list | **Large** — O(N) per window |
| `.apply(windowFunc)` | `ListStateDescriptor` (line 398) | **ALL raw elements** as a list | **Large** — O(N) per window |

**Impact in Accumulating Mode with allowedLateness:**

With `ListStateDescriptor` (`.process()` / `.apply()`), **every element ever added stays in state** until cleanup. In accumulating mode with late re-fires, the list keeps growing. Each re-fire iterates the entire list.

With `AggregatingStateDescriptor` (`.aggregate()`), state stays O(1) regardless of how many elements arrive. Each `.add()` does a GET + aggregate + PUT of a single accumulator value.

**In This Example Code:**

**Local phase** (line 109): `.aggregate(new LocalCountAggregate())` → `AggregatingStateDescriptor`
- State per (key, window): a single `Tuple3<String, Integer, Long>` (groupKey, subtaskIndex, count)
- Adding an element: GET acc, increment count, PUT acc
- Re-fire cost: O(1) — just read the accumulator

**Global phase** (line 128): `.process(new GlobalDeduplicatedCountFunction())` → `ListStateDescriptor`
- State per (key, window): a list of ALL `Tuple3<String, Integer, Long>` elements ever received
- Adding an element: `db.merge()` appends to the list
- Re-fire cost: O(N) — iterates ALL accumulated elements to build the dedup map
- This is intentional: the dedup logic in lines 233-238 NEEDS all elements to correctly overwrite stale values with latest ones

---

## 1. Accumulating vs. Fire & Purging

### The TriggerResult Enum

The core distinction lives in `TriggerResult.java`:

```java
public enum TriggerResult {
    CONTINUE(false, false),      // do nothing
    FIRE_AND_PURGE(true, true),  // emit result AND delete state
    FIRE(true, false),           // emit result, KEEP state
    PURGE(false, true);          // delete state, don't emit
}
```

### How WindowOperator Uses TriggerResult

In `WindowOperator.onEventTime()` (line 474-483):

```java
TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

if (triggerResult.isFire()) {
    ACC contents = windowState.get();      // read from RocksDB
    if (contents != null) {
        emitWindowContents(window, contents);  // send result downstream
    }
}

if (triggerResult.isPurge()) {
    windowState.clear();                   // DELETE from RocksDB
}
```

And identically in `processElement()` (line 419-429):

```java
TriggerResult triggerResult = triggerContext.onElement(element);
if (triggerResult.isFire()) {
    ACC contents = windowState.get();
    if (contents != null) {
        emitWindowContents(window, contents);
    }
}
if (triggerResult.isPurge()) {
    windowState.clear();
}
```

### Accumulating Mode (Default - `EventTimeTrigger`)

`EventTimeTrigger.onEventTime()` returns **`FIRE`** (not `FIRE_AND_PURGE`):

```java
// EventTimeTrigger.java:50-51
public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
    return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
}
```

**What happens with `FIRE`:**
- `isFire()` = true → reads state from RocksDB and emits the result
- `isPurge()` = false → **state is NOT cleared from RocksDB**

This is **accumulating mode**: state survives after firing. If a late element arrives and triggers a re-fire, the window state still contains ALL previously accumulated elements plus the new one.

### Fire & Purging Mode (`PurgingTrigger`)

`PurgingTrigger` wraps any trigger and converts `FIRE` → `FIRE_AND_PURGE`:

```java
// PurgingTrigger.java:51-53
public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
    TriggerResult triggerResult = nestedTrigger.onEventTime(time, window, ctx);
    return triggerResult.isFire() ? TriggerResult.FIRE_AND_PURGE : triggerResult;
}
```

**What happens with `FIRE_AND_PURGE`:**
- `isFire()` = true → reads state and emits result
- `isPurge()` = true → **`windowState.clear()` is called → RocksDB `db.delete()`**

State is destroyed after every fire. If a late element arrives, it starts fresh with only itself — all previous elements are gone.

---

## 2. Concrete Walkthrough with TwoPhaseCountDeduplicatedEventTimeV2

### Setup

The code uses **accumulating mode** (default `EventTimeTrigger`) with `allowedLateness(Duration.ofSeconds(30))`. Line 106 shows `PurgingTrigger` is **commented out**:

```java
// Line 105-109
.window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
//  .trigger(PurgingTrigger.of(EventTimeTrigger.create()))  // COMMENTED OUT
.allowedLateness(Duration.ofSeconds(30))
.sideOutputLateData(LOCAL_LATE_TAG)
.aggregate(new LocalCountAggregate());
```

### Timeline: What Triggers State Updates and Cleanup

#### Batch 1: Normal Events (lines 274-286)

```
Events emitted:
  (group1, event, 1000), (group1, event, 2000), ..., (group1, event, 5000)  → 5 events
  (group2, event, 1000), (group2, event, 2000), (group2, event, 3000)       → 3 events
  (group1, event, 70000), (group2, event, 70000)                            → advance watermark

Watermark strategy: forBoundedOutOfOrderness(5s)
  → watermark = max_timestamp - 5000 = 70000 - 5000 = 65000
```

**LOCAL phase — For each event in window [0, 60000):**

`WindowOperator.processElement()` runs (line 404-432):

```java
// Line 413-414: For each element arriving to window [0, 60000)
windowState.setCurrentNamespace(TimeWindow[0, 60000]);  // window = namespace
windowState.add(element.getValue());                     // AggregatingState
```

Since this uses `AggregatingState` (`LocalCountAggregate`), each `.add()` call triggers in `RocksDBAggregatingState.add()`:
```java
// RocksDBAggregatingState.java:100-104
byte[] key = getKeyBytes();                    // [KG|"group1_event"|[0,60000]]
ACC accumulator = getInternal(key);            // RocksDB GET
accumulator = accumulator == null
    ? aggFunction.createAccumulator()          // (null, -1, 0)
    : accumulator;
updateInternal(key, aggFunction.add(value, accumulator));  // RocksDB PUT
```

After 5 events for group1: RocksDB key `[KG|"group1_event"|[0,60000]]` → value `("group1", 0, 5)`.

**When watermark reaches 65000 (> window.maxTimestamp() = 59999):**

`onEventTime(59999)` fires → `EventTimeTrigger` returns **`FIRE`**:

```
windowState.get()  →  RocksDB GET  →  ("group1", 0, 5)
emitWindowContents(window, ("group1", 0, 5))  →  emit downstream
// isPurge() = false  →  STATE IS KEPT IN ROCKSDB ✓
```

**Cleanup timer registered at:** `cleanupTime = window.maxTimestamp() + allowedLateness = 59999 + 30000 = 89999`

#### Thread.sleep(5000) then Batch 2: Late Events (lines 300-301)

```
Late event: (group1, event, 500)   → timestamp 500 falls in window [0, 60000)
```

**`processElement()` for the late event:**

```java
// Line 408: isWindowLate check
// cleanupTime(window) = 89999, watermark = 65000
// 89999 <= 65000? NO → window is NOT late, proceed

// Line 413-414:
windowState.setCurrentNamespace(TimeWindow[0, 60000]);
windowState.add(element.getValue());
// RocksDBAggregatingState: GET → ("group1", 0, 5), add → ("group1", 0, 6), PUT
```

**Then the trigger fires again** because `EventTimeTrigger.onElement()` sees watermark already past:

```java
// EventTimeTrigger.java:40-42
if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
    return TriggerResult.FIRE;  // watermark 65000 > 59999 → FIRE immediately
}
```

**RE-FIRE in accumulating mode:**

```
windowState.get()  →  RocksDB GET  →  ("group1", 0, 6)  ← includes ALL 6 elements
emitWindowContents(window, ("group1", 0, 6))  →  emit (group1, 0, 6) downstream
// isPurge() = false  →  STATE STILL KEPT
```

This is why the **global phase** uses a `ProcessWindowFunction` with deduplication (lines 225-238). In accumulating mode, the global window receives:
- Initial fire: `(group1, 0, 5)` — 5 events from subtask 0
- Re-fire: `(group1, 0, 6)` — updated count of 6

The `ProcessWindowFunction` iterates all accumulated elements and uses `Map.put(subtaskIndex, count)`, so the later value (6) overwrites the earlier value (5), yielding the correct total.

#### What If PurgingTrigger Were Enabled? (Line 106 Uncommented)

```java
.trigger(PurgingTrigger.of(EventTimeTrigger.create()))
```

**Initial fire at watermark 65000:**
```
PurgingTrigger converts FIRE → FIRE_AND_PURGE
windowState.get()  →  ("group1", 0, 5)
emitWindowContents(window, ("group1", 0, 5))  →  emit
isPurge() = true  →  windowState.clear()  →  RocksDB DELETE key  ← STATE DESTROYED
```

**Late event (group1, event, 500) arrives:**
```
windowState.setCurrentNamespace(TimeWindow[0, 60000])
windowState.add(element)
// RocksDBAggregatingState: GET → null (state was purged!), create new acc → (group1, 0, 1)
// PUT → ("group1", 0, 1)

// EventTimeTrigger.onElement → FIRE (watermark past window)
// PurgingTrigger converts → FIRE_AND_PURGE
windowState.get()  →  ("group1", 0, 1)   ← ONLY the late element, not 6!
emitWindowContents  →  emit (group1, 0, 1)
windowState.clear()  →  RocksDB DELETE
```

**Problem:** The global phase would receive `(group1, 0, 5)` then `(group1, 0, 1)`. Using `Map.put(0, 1)` overwrites 5 with 1, yielding total = 1 instead of 6. The deduplication logic **breaks** with purging triggers because the re-fire only contains the delta, not the full count.

---

## 3. allowedLateness() and Its Impact on RocksDB

### What allowedLateness Changes

`allowedLateness` controls **two things** in `WindowOperator`:

#### (a) Cleanup Timer Delay

```java
// WindowOperator.java:670-673
private long cleanupTime(W window) {
    if (windowAssigner.isEventTime()) {
        long cleanupTime = window.maxTimestamp() + allowedLateness;
        return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
    }
}
```

- **Without allowedLateness (default = 0):** cleanup timer = `59999 + 0 = 59999` → fires at the same time as the trigger → `clearAllState()` is called immediately after the initial fire.
- **With allowedLateness(30s):** cleanup timer = `59999 + 30000 = 89999` → state lives 30 more seconds after the window fires.

The cleanup is triggered in `onEventTime()` (line 485-488):

```java
if (windowAssigner.isEventTime()
        && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
    clearAllState(triggerContext.window, windowState, mergingWindows);
}
```

`clearAllState()` (line 560-571):
```java
windowState.clear();          // RocksDB DELETE for window contents
triggerContext.clear();        // delete trigger's event-time timer
processContext.clear();        // user function cleanup
```

#### (b) Late Element Admission Gate

```java
// WindowOperator.java:609-612
protected boolean isWindowLate(W window) {
    return (windowAssigner.isEventTime()
            && (cleanupTime(window) <= internalTimerService.currentWatermark()));
}

// WindowOperator.java:620-624
protected boolean isElementLate(StreamRecord<IN> element) {
    return (windowAssigner.isEventTime())
            && (element.getTimestamp() + allowedLateness
                    <= internalTimerService.currentWatermark());
}
```

Elements that arrive after `window.maxTimestamp() + allowedLateness` are either:
1. Dropped silently (if no side output tag), incrementing `numLateRecordsDropped` counter
2. Sent to side output (if `sideOutputLateData()` is configured)

### RocksDB Impact of allowedLateness

#### Without allowedLateness (default = 0)

```
Timeline for window [0, 60000):

t=59999 (watermark passes):
  Timer fires → TriggerResult.FIRE → emit result
  isCleanupTime(59999) = true → clearAllState()
    → RocksDB DELETE key=[KG|"group1_event"|[0,60000]]
    → Timer deleted

  State lifetime in RocksDB: from first element arrival until window fires

  Any element arriving after watermark passes 59999:
    → isWindowLate() = true (cleanupTime 59999 <= watermark)
    → DROPPED or side-output
    → NO state update in RocksDB
```

#### With allowedLateness(30s)

```
Timeline for window [0, 60000):

t=59999 (watermark passes):
  Timer fires → TriggerResult.FIRE → emit result
  isCleanupTime(59999) → cleanup time is 89999, timer is 59999 → NO, not cleanup time
    → STATE SURVIVES IN ROCKSDB ✓

t=60000..89998 (late elements can arrive):
  processElement → isWindowLate? cleanupTime(89999) <= watermark?
    Only if watermark >= 89999. Until then, window is NOT late.

  Each late element:
    → windowState.add(element)  →  RocksDB GET + aggregate + PUT
    → EventTimeTrigger.onElement → watermark past window → FIRE immediately
    → windowState.get() → emit RE-FIRE with ALL accumulated state
    → isPurge() = false → state kept

  State keeps GROWING in RocksDB with each late element.

t=89999 (cleanup timer fires):
  onEventTime(89999):
    EventTimeTrigger.onEventTime(89999) → 89999 != 59999 → CONTINUE (no fire)
    isCleanupTime(89999) = true → clearAllState()
      → RocksDB DELETE key=[KG|"group1_event"|[0,60000]]
      → All trigger timers deleted
      → State is GONE

t>89999:
  Any element for window [0,60000):
    → isWindowLate() = true → DROPPED
    → if sideOutputLateData configured → goes to side output
    → NO RocksDB state update
```

### RocksDB Storage Cost Summary

```
                          Without allowedLateness    With allowedLateness(30s)
─────────────────────     ──────────────────────     ────────────────────────
State lifetime            Until window fires          Until window.maxTimestamp + 30s
                          (window.maxTimestamp)        (cleanup timer fires)

Re-fires possible?        NO — state deleted          YES — state kept, late elements
                          immediately at fire          cause immediate re-fires

State size at cleanup     N/A (already gone)          Original + all late elements
                                                      (accumulating mode)

RocksDB entries alive     Only current windows        Current windows + recently-fired
at any moment                                         windows within lateness period

Extra timers              None                        One cleanup timer per (key, window)
                                                      at maxTimestamp + allowedLateness

Checkpoint size impact    Smaller — fewer live         Larger — fired-but-not-cleaned
                          entries in RocksDB           windows still have state in SST files
```

### Concrete RocksDB State Trace for This Code

```
Using allowedLateness(30s), AggregatingState, Accumulating mode:

═══════════════════════════════════════════════════════════════════════════
Events arrive: (group1, event, 1000..5000) — 5 events
═══════════════════════════════════════════════════════════════════════════

  RocksDB after each .add():
    key=[KG|"group1_event"|[0,60000]]
    PUT ("group1", 0, 1) → PUT ("group1", 0, 2) → ... → PUT ("group1", 0, 5)

  Timer registered: 59999 (trigger) + 89999 (cleanup)

═══════════════════════════════════════════════════════════════════════════
Watermark = 65000 → Timer 59999 fires (initial fire)
═══════════════════════════════════════════════════════════════════════════

  RocksDB GET → ("group1", 0, 5)
  Emit downstream: ("group1", 0, 5)
  isPurge = false → NO DELETE
  isCleanupTime(59999)? → cleanupTime=89999, timer=59999 → NO

  RocksDB state: key=[KG|"group1_event"|[0,60000]] → ("group1", 0, 5)  STILL HERE

═══════════════════════════════════════════════════════════════════════════
Late event arrives: (group1, event, 500) — timestamp 500, watermark ~65000
═══════════════════════════════════════════════════════════════════════════

  isWindowLate? cleanupTime=89999 <= 65000? NO → admitted

  RocksDB GET → ("group1", 0, 5)
  aggFunction.add(element, acc) → ("group1", 0, 6)
  RocksDB PUT → ("group1", 0, 6)

  EventTimeTrigger.onElement: 59999 <= 65000 → FIRE (immediate re-fire)
  RocksDB GET → ("group1", 0, 6)
  Emit downstream: ("group1", 0, 6)    ← corrected count with ALL elements
  isPurge = false → NO DELETE

  RocksDB state: key=[KG|"group1_event"|[0,60000]] → ("group1", 0, 6)  STILL HERE

═══════════════════════════════════════════════════════════════════════════
Watermark reaches 89999 → Timer 89999 fires (cleanup)
═══════════════════════════════════════════════════════════════════════════

  EventTimeTrigger.onEventTime(89999): 89999 != 59999 → CONTINUE (no emit)
  isCleanupTime(89999)? → YES
  clearAllState():
    windowState.clear() → RocksDB DELETE key=[KG|"group1_event"|[0,60000]]
    triggerContext.clear() → delete timer for 59999
    processContext.clear()

  RocksDB state: key=[KG|"group1_event"|[0,60000]] → GONE

═══════════════════════════════════════════════════════════════════════════
Any later event with timestamp in [0, 60000) after watermark > 89999:
═══════════════════════════════════════════════════════════════════════════

  isWindowLate? cleanupTime=89999 <= watermark(>89999)? YES → DROPPED
  → Goes to side output (LOCAL_LATE_TAG) if configured
  → No RocksDB write at all
```

### Why This Code Uses Accumulating + allowedLateness (Not Purging)

The two-phase count deduplication pattern **requires accumulating mode**:

1. **Local phase** uses `AggregateFunction` (accumulating). On re-fire, it emits the **total count** (e.g., 6), not the delta (1). This is correct because the accumulator was never purged.

2. **Global phase** uses `ProcessWindowFunction` which sees ALL accumulated elements. The deduplication `Map.put(subtaskIndex, count)` naturally picks the **latest** (largest) count per subtask, overwriting the stale initial-fire value.

3. If purging were used, re-fires would only contain the late delta (count=1 instead of count=6). The global deduplication map would overwrite the correct value (5) with the wrong delta (1), producing incorrect results.

The `allowedLateness(30s)` trades RocksDB storage cost (state lives 30s longer) for correctness (late elements within 30s are correctly incorporated and re-fired). Elements arriving after 30s are routed to side output for separate handling.

---

## 4. Why allowedLateness Instead of a Larger Watermark Delay?

### Option A: Increase Watermark Delay (no allowedLateness)

```java
// Change from 5s to 35s out-of-orderness
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(35))
```

The watermark is computed as `max_timestamp - 35000`. For the events in this code:

```
Events at t=1000..5000, then t=70000
Watermark = 70000 - 35000 = 35000   ← still BELOW 59999

Window [0, 60000) does NOT fire yet.
You'd need an event at t=95000+ to push watermark past 59999.
```

**The problem:** EVERY window across the entire pipeline waits 35 seconds longer before firing. Window `[0, 60000)` won't emit until watermark reaches 59999, which requires an event at timestamp >= 94999. This adds **35 seconds of end-to-end latency to ALL results**, even for the 99% of data that arrived on time.

### Option B: Small Watermark Delay + allowedLateness (What This Code Does)

```java
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))  // small delay
.allowedLateness(Duration.ofSeconds(30))                           // grace period
```

```
Events at t=1000..5000, then t=70000
Watermark = 70000 - 5000 = 65000   ← ABOVE 59999

Window [0, 60000) fires IMMEDIATELY with count=5    ← FAST initial result
State kept in RocksDB for 30 more seconds

Late event at t=500 arrives:
  → Admitted (within allowedLateness)
  → State updated: count=6
  → Re-fire: emit corrected count=6                 ← CORRECTED result

Cleanup at watermark=89999: state deleted
```

### Comparison

| | Watermark delay = 35s | Watermark delay = 5s + allowedLateness = 30s |
|---|---|---|
| First result emitted | After 35s delay | After 5s delay |
| Late data handling | Never fires late — waits for everything | Fires early, re-fires with corrections |
| Result count | **1** result per window | **1 + N** results (initial + re-fires) |
| Downstream impact | Simple — one result | Must handle updates/retractions |
| RocksDB state lifetime | Shorter (no extra retention) | Longer (state kept during lateness period) |
| Pipeline-wide impact | **ALL** operators delayed | Only windowed operators with allowedLateness affected |

### Why This Code Chose allowedLateness

1. **Low latency**: Initial result arrives after only 5s watermark delay, not 35s
2. **Self-correcting**: The global `ProcessWindowFunction` with `Map.put(subtaskIndex, count)` naturally deduplicates — re-fires overwrite stale counts with correct ones
3. **Bounded cost**: State is cleaned up after 30s; truly late data goes to side output
4. **Per-operator granularity**: Watermark delay is **global** — it affects every operator in the pipeline. `allowedLateness` is **per-window-operator**. In this code, both local and global phases independently set `allowedLateness(30s)` (lines 107 and 126). They could be tuned differently — e.g., 30s locally but 10s globally — which isn't possible with watermark delay alone.

The trade-off is that downstream consumers see **multiple results per window** (initial + corrections). This code handles that via the deduplication map. If your downstream can't handle updates, then a larger watermark delay is simpler — but you pay with latency on every single window.

---

## 5. sideOutputLateData(): The Three-Gate Filtering Logic

### How Late Elements Are Routed

There are **three sequential checks** an element must pass in `processElement()` (line 293-447). Side output is the **last resort** — it only catches elements that failed everything else.

```
Element arrives at processElement()
    │
    ▼
┌─ Gate 1: windowAssigner.assignWindows() ──────────────────────┐
│  Assigns element to window(s) based on its timestamp.         │
│  e.g., timestamp=500 → window [0, 60000)                     │
└───────────────────────────────────────────────────────────────┘
    │
    ▼  (for each assigned window)
┌─ Gate 2: isWindowLate(window) (line 408) ─────────────────────┐
│  cleanupTime(window) <= currentWatermark?                     │
│  i.e., window.maxTimestamp + allowedLateness <= watermark?     │
│                                                               │
│  YES → skip this window (continue), isSkippedElement remains  │
│        true. State has already been cleared by cleanup timer.  │
│  NO  → proceed: add to state, trigger, etc.                   │
│        isSkippedElement = false                                │
└───────────────────────────────────────────────────────────────┘
    │
    ▼  (after the for loop, line 440-446)
┌─ Gate 3: isSkippedElement && isElementLate(element) ──────────┐
│                                                               │
│  isSkippedElement = true means ALL assigned windows were late  │
│  (element was handled by ZERO windows)                        │
│                                                               │
│  isElementLate: element.timestamp + allowedLateness            │
│                 <= currentWatermark                            │
│                                                               │
│  Both true?                                                   │
│    → if lateDataOutputTag != null:                            │
│        output.collect(lateDataOutputTag, element)  ← SIDE OUT │
│    → else:                                                    │
│        numLateRecordsDropped.inc()                 ← DROPPED  │
└───────────────────────────────────────────────────────────────┘
```

### Concrete Trace

```
Window [0, 60000), allowedLateness = 30s → cleanupTime = 89999

─── Case 1: watermark = 65000 (within allowedLateness) ───────────

  Gate 2: isWindowLate? cleanupTime(89999) <= 65000? NO
    → Element ADMITTED to window state
    → isSkippedElement = false
  Gate 3: isSkippedElement is false → SKIP side output check entirely

  Result: Element goes into RocksDB, triggers re-fire. No side output.

─── Case 2: watermark = 91000 (past allowedLateness) ─────────────

  Gate 2: isWindowLate? cleanupTime(89999) <= 91000? YES
    → Window skipped (state already cleared by cleanup timer)
    → isSkippedElement stays true

  Gate 3: isSkippedElement=true, isElementLate?
    element.timestamp(500) + allowedLateness(30000) <= watermark(91000)?
    30500 <= 91000? YES

    lateDataOutputTag != null (LOCAL_LATE_TAG) → sideOutput(element)
    → output.collect(LOCAL_LATE_TAG, element)

  Result: Element goes to side output stream. No RocksDB write.
```

### What sideOutput() Does

```java
// WindowOperator.java:587-588
protected void sideOutput(StreamRecord<IN> element) {
    output.collect(lateDataOutputTag, element);
}
```

Routes the element to a **separate output stream** tagged with the `OutputTag`. Downstream, it's retrieved via `getSideOutput()`:

```java
// TwoPhaseCountDeduplicatedEventTimeV2.java:112-113
DataStream<Tuple3<String, Integer, String>> localLateEvents =
        localCounts.getSideOutput(LOCAL_LATE_TAG);
localLateEvents.print("LOCAL-LATE");   // line 138
```

Side output elements **never touch RocksDB**. They fail Gate 2 (`isWindowLate`), so `windowState.add()` is never called.

### The No-Side-Output Path

If `sideOutputLateData()` is **not** called (no `OutputTag` configured), then `lateDataOutputTag` is null:

```java
// Line 441-444
if (lateDataOutputTag != null) {
    sideOutput(element);              // tagged side output
} else {
    this.numLateRecordsDropped.inc(); // silently dropped, only a metric incremented
}
```

The element is silently dropped and the `numLateRecordsDropped` metric counter increments. You can monitor this counter in Flink's metrics system to detect data loss.

### Mathematical Proof: isElementLate Is Redundant for Standard Assigners

The `isElementLate` check in Gate 3 appears to add an extra filter beyond `isWindowLate`, but for standard window assigners (tumbling, sliding, session), **`isWindowLate` for all windows logically implies `isElementLate`**.

**Proof:**

For any window that contains an element with timestamp `T`:
- The element is inside the window: `window.start <= T < window.end`
- Therefore: `T <= window.end - 1 = window.maxTimestamp`
- Therefore: `T + allowedLateness <= window.maxTimestamp + allowedLateness = cleanupTime(window)`

```
If isWindowLate is true for a window:
    cleanupTime(window) <= watermark

Since T + allowedLateness <= cleanupTime(window):
    T + allowedLateness <= cleanupTime(window) <= watermark

Therefore isElementLate is true:
    T + allowedLateness <= watermark  ✓
```

This holds for **every** window the element is assigned to. If ALL windows are late (`isSkippedElement=true`), then `isElementLate` is guaranteed true — the check is a **redundant safety guard**.

The only way `isWindowLate=true` but `isElementLate=false` could occur is with a custom `WindowAssigner` that assigns elements to windows where `element.timestamp > window.maxTimestamp` (i.e., assigning an element to a window it doesn't temporally belong to), which would be a pathological implementation.
