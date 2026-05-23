# Flink Window State in RocksDB: State Storage, S3 Checkpointing, and Sliding Window Internals

A deep dive into the Flink source code covering three topics:
1. How window functions save state in RocksDB
2. How RocksDB uploads data files to S3 at each checkpoint
3. How sliding windows update state (with a concrete example)

---

## 1. How Window Functions Save State in RocksDB

### State Descriptor Creation

When you define a window operation (e.g., `.reduce()`, `.aggregate()`, `.apply()`), Flink creates a corresponding state descriptor in `WindowOperatorBuilder`:

- **ReduceFunction** -> `ReducingStateDescriptor` (stores a single pre-reduced value per window)
- **AggregateFunction** -> `AggregatingStateDescriptor` (stores an accumulator per window)
- **ProcessWindowFunction** -> `ListStateDescriptor` (stores all elements in a list per window)

All use the state name `"window-contents"`.

**Source:** `WindowOperatorBuilder.java:178-182` (reduce), `285-289` (aggregate), `398-399` (list)

### RocksDB Key Structure

Each state entry in RocksDB uses a **composite key** with the structure:

```
[keyGroupPrefix (variable bytes)] [serializedKey] [serializedNamespace(=Window)]
```

Built by `SerializedCompositeKeyBuilder.buildCompositeKeyNamespace()` (`SerializedCompositeKeyBuilder.java:120-128`).

- **keyGroupPrefix**: determines which key group (and thus which subtask) owns this key. Used for key-group-based partitioning and range scans during checkpoint.
- **serializedKey**: the user's key (e.g., a word in word-count)
- **serializedNamespace**: the **window** itself (e.g., `TimeWindow{start=1000, end=2000}`). The window IS the namespace.

So for key `"userA"` and window `[1000, 2000)`, the RocksDB key is roughly:
```
[0x0002]["userA"][1000, 2000]
```

### How State is Written to RocksDB

The `WindowOperator.processElement()` method (line 293-447) does this for each assigned window:

```java
// Line 413-414 (non-merging path)
windowState.setCurrentNamespace(window);  // sets the window as RocksDB key suffix
windowState.add(element.getValue());      // writes to RocksDB
```

The actual RocksDB write depends on state type:

| State Type | RocksDB Operation | What's Stored |
|---|---|---|
| `RocksDBListState.add()` | `db.merge(columnFamily, key, serializedValue)` | Appends element via StringAppendOperator (delimiter-separated bytes) |
| `RocksDBReducingState.add()` | `db.get()` then `reduceFunction.reduce(old, new)` then `db.put()` | Single pre-reduced value |
| `RocksDBAggregatingState.add()` | `db.get()` then `aggFunction.add(value, acc)` then `db.put()` | Single accumulator |

**Source:** `RocksDBListState.java:128`, `RocksDBReducingState.java:93-98`, `RocksDBAggregatingState.java:100-104`

Key detail: **All writes go directly to RocksDB's memtable** (no WAL by default in Flink). When the memtable is full, RocksDB flushes it to an SST file on the local disk.

### Window Firing and State Cleanup

When the trigger fires (e.g., watermark passes `window.maxTimestamp()`):
1. `windowState.get()` reads accumulated state from RocksDB
2. `emitWindowContents()` sends the result downstream
3. `windowState.clear()` calls `db.delete(columnFamily, compositeKey)` - removes the entry from RocksDB

**Source:** `WindowOperator.java:450-541` (onEventTime/onProcessingTime handlers)

---

## 2. How RocksDB Uploads Data Files to S3 at Each Checkpoint

### Overview: Two Phases (Sync + Async)

Flink's RocksDB checkpoint has a **synchronous preparation** phase (blocks the operator) and an **asynchronous upload** phase (runs in a background thread while the operator continues processing).

### Phase 1: Synchronous - Create Local Native Checkpoint

**`RocksDBSnapshotStrategyBase.syncPrepareResources()`** (line 151-165):

```java
// 1. Prepare local snapshot directory
SnapshotDirectory snapshotDirectory = prepareLocalSnapshotDirectory(checkpointId);
// 2. Snapshot metadata (state descriptors, etc.)
PreviousSnapshot previousSnapshot = snapshotMetaData(checkpointId, stateMetaInfoSnapshots);
// 3. Create RocksDB native checkpoint (hard links!)
takeDBNativeCheckpoint(snapshotDirectory);
```

**`takeDBNativeCheckpoint()`** (line 170-184):
```java
Checkpoint checkpoint = Checkpoint.create(db);
checkpoint.createCheckpoint(outputDirectory.getDirectory().toString());
```

This calls RocksDB's native `Checkpoint` API which creates **hard links** to the current live SST files. This is very fast (no data copy), and the hard links ensure the SST files survive even if RocksDB compacts and deletes the originals.

### Phase 2: Asynchronous - Upload SST Files to S3

**`RocksDBIncrementalSnapshotOperation.get()`** (line 252-333) runs in a background thread:

#### Step 1: Determine Which Files Need Uploading

**`createUploadFilePaths()`** (line 413-433):

```java
for (Path filePath : files) {
    String fileName = filePath.getFileName().toString();
    if (fileName.endsWith(".sst")) {
        Optional<StreamStateHandle> uploaded = previousSnapshot.getUploaded(fileName);
        if (uploaded.isPresent() && checkpointStreamFactory.couldReuseStateHandle(uploaded.get())) {
            sstFiles.add(HandleAndLocalPath.of(uploaded.get(), fileName));  // REUSE - skip upload
        } else {
            sstFilePaths.add(filePath);  // NEW - needs upload
        }
    } else {
        miscFilePaths.add(filePath);  // CURRENT, OPTIONS, MANIFEST etc - always upload
    }
}
```

**Key insight for incremental checkpoints:** SST files in RocksDB are **immutable** once written. If an SST file name (e.g., `000042.sst`) was already uploaded in a previous checkpoint, Flink reuses the existing S3 handle rather than re-uploading.

The `PreviousSnapshot` object tracks all SST files from the last completed checkpoint via the `uploadedSstFiles` TreeMap (`RocksIncrementalSnapshotStrategy.java:85`).

#### Step 2: Upload New Files in Parallel

**`RocksDBStateUploader.uploadFilesToCheckpointFs()`** (line 71-106):

```java
// Creates parallel upload futures - one per file
List<CompletableFuture<HandleAndLocalPath>> futures = createUploadFutures(files, ...);
FutureUtils.waitForAll(futures).get();  // Wait for all uploads
```

Each individual file upload (`uploadLocalFileToCheckpointFs()`, line 130-181):
```java
// 1. Open local file as InputStream
inputStream = Files.newInputStream(filePath);
// 2. Create output stream to checkpoint storage (S3)
outputStream = checkpointStreamFactory.createCheckpointStateOutputStream(stateScope);
// 3. Copy in 16KB chunks
while ((numBytes = inputStream.read(buffer)) != -1) {
    outputStream.write(buffer, 0, numBytes);
}
// 4. Close and get handle (S3 path reference)
StreamStateHandle result = outputStream.closeAndGetHandle();
return HandleAndLocalPath.of(result, filePath.getFileName().toString());
```

The `CheckpointStreamFactory` (typically `FsCheckpointStreamFactory`) creates streams that write to the configured checkpoint directory (e.g., `s3://bucket/checkpoints/chk-42/`).

**State scopes:**
- `SHARED`: SST files in incremental checkpoints - stored in a shared directory, can be referenced by multiple checkpoints
- `EXCLUSIVE`: Misc files (MANIFEST, CURRENT, OPTIONS) + all files in full checkpoints - deleted when checkpoint is discarded

#### Step 3: Build State Handle and Record Uploaded Files

```java
// Line 300-308: Create the state handle reported to JobManager
IncrementalRemoteKeyedStateHandle jmHandle = new IncrementalRemoteKeyedStateHandle(
    backendUID, keyGroupRange, checkpointId,
    sstFiles,    // shared state (SST files - reusable)
    miscFiles,   // private state (MANIFEST etc - exclusive)
    metaStateHandle, checkpointedSize);

// Line 393-398: Remember uploaded files for next checkpoint
uploadedSstFiles.put(checkpointId, Collections.unmodifiableList(sstFiles));
```

#### Step 4: Checkpoint Completion

When JobManager confirms the checkpoint (`notifyCheckpointComplete()`, line 167-180):
```java
// Remove records of checkpoints before the completed one
uploadedSstFiles.keySet().removeIf(id -> id < completedCheckpointId);
lastCompletedCheckpointId = completedCheckpointId;
```

### Visual Summary

```
Checkpoint N triggered
  |
  v
[SYNC] RocksDB Checkpoint.create() -> hard links SST files to local /tmp/chk-N/
  |
  v  (operator resumes processing)
  |
[ASYNC] Compare /tmp/chk-N/*.sst against previousSnapshot
  |
  |-- 000001.sst: in previousSnapshot -> REUSE (no upload)
  |-- 000002.sst: in previousSnapshot -> REUSE (no upload)
  |-- 000005.sst: NEW file            -> UPLOAD to s3://bucket/shared/000005.sst
  |-- 000007.sst: NEW file            -> UPLOAD to s3://bucket/shared/000007.sst
  |-- MANIFEST:   always              -> UPLOAD to s3://bucket/exclusive/MANIFEST
  |-- CURRENT:    always              -> UPLOAD to s3://bucket/exclusive/CURRENT
  |
  v
Report IncrementalRemoteKeyedStateHandle to JobManager
  |
  v
[JM confirms] -> notifyCheckpointComplete(N) -> update uploadedSstFiles tracking
```

---

## 3. Sliding Windows: How Flink Updates State (Concrete Example)

### How Sliding Window Assignment Works

**`SlidingEventTimeWindows.assignWindows()`** (`SlidingEventTimeWindows.java:77-91`):

```java
long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
for (long start = lastStart; start > timestamp - size; start -= slide) {
    windows.add(new TimeWindow(start, start + size));
}
```

Each element is assigned to `size / slide` windows (e.g., 60s/20s = 3 windows per element).

### Concrete Example

**Configuration:** Sliding window of **size=60s, slide=20s** over a `KeyedStream` keyed by `userId`, using a sum reduce function.

```java
stream
    .keyBy(event -> event.userId)
    .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(20)))
    .reduce((a, b) -> new Event(a.userId, a.value + b.value))
```

### Step-by-Step Timeline

```
═══════════════════════════════════════════════════════════════════════════
Event 1: (userA, value=3, timestamp=25s)
═══════════════════════════════════════════════════════════════════════════

  Window assignment:
    lastStart = 25000 - (25000 % 20000) = 20000
    Loop: start=20000 > 25000-60000=-35000 → [20s, 80s)
          start=0     > -35000             → [0s, 60s)
          start=-20000 > -35000            → [-20s, 40s)
          start=-40000 > -35000? NO → stop

  Assigned to 3 windows: [-20s, 40s), [0s, 60s), [20s, 80s)

  RocksDB writes (ReducingState: read old, reduce, write new):
    key=[KG|userA|[-20000,40000]]  PUT value=3     (new window, no old value)
    key=[KG|userA|[0,60000]]       PUT value=3     (new window)
    key=[KG|userA|[20000,80000]]   PUT value=3     (new window)

  Timers registered: 39999ms, 59999ms, 79999ms

═══════════════════════════════════════════════════════════════════════════
Event 2: (userA, value=5, timestamp=35s)
═══════════════════════════════════════════════════════════════════════════

  Window assignment:
    lastStart = 35000 - (35000 % 20000) = 20000
    Same 3 windows: [-20s, 40s), [0s, 60s), [20s, 80s)

  RocksDB writes (read-modify-write for each window):
    key=[KG|userA|[-20000,40000]]  GET→3, reduce(3,5)=8, PUT value=8
    key=[KG|userA|[0,60000]]       GET→3, reduce(3,5)=8, PUT value=8
    key=[KG|userA|[20000,80000]]   GET→3, reduce(3,5)=8, PUT value=8

═══════════════════════════════════════════════════════════════════════════
Event 3: (userA, value=2, timestamp=42s)
═══════════════════════════════════════════════════════════════════════════

  Window assignment:
    lastStart = 42000 - (42000 % 20000) = 40000
    Loop: start=40000 > 42000-60000=-18000 → [40s, 100s)
          start=20000 > -18000             → [20s, 80s)
          start=0     > -18000             → [0s, 60s)
          start=-20000 > -18000? NO → stop

  Assigned to 3 windows: [0s, 60s), [20s, 80s), [40s, 100s)

  Note: this element does NOT go into [-20s, 40s) because 42s >= 40s

  RocksDB writes:
    key=[KG|userA|[0,60000]]       GET→8, reduce(8,2)=10, PUT value=10
    key=[KG|userA|[20000,80000]]   GET→8, reduce(8,2)=10, PUT value=10
    key=[KG|userA|[40000,100000]]  PUT value=2           (new window)

  Timer registered: 99999ms

═══════════════════════════════════════════════════════════════════════════
Watermark advances past 39999ms → Timer fires for window [-20s, 40s)
═══════════════════════════════════════════════════════════════════════════

  EventTimeTrigger.onEventTime(39999) returns FIRE:
    → windowState.setCurrentNamespace([-20000, 40000])
    → windowState.get() → returns 8
    → emit downstream: (userA, 8) with timestamp 39999
    → cleanup: windowState.clear()
      → RocksDB DELETE key=[KG|userA|[-20000,40000]]
    → delete cleanup timer

═══════════════════════════════════════════════════════════════════════════
RocksDB state snapshot after all above:
═══════════════════════════════════════════════════════════════════════════

  key=[KG|userA|[0,60000]]        value=10    (timer at 59999)
  key=[KG|userA|[20000,80000]]    value=10    (timer at 79999)
  key=[KG|userA|[40000,100000]]   value=2     (timer at 99999)

  Window [-20s, 40s) has been fired and cleaned up — its state is gone.
```

### Key Observations for Sliding Windows

1. **Each element is written to `size/slide` windows independently.** For 60s/20s, that's 3 RocksDB writes per element. This is the main cost of sliding windows.

2. **Each (key, window) pair is a separate RocksDB entry.** The window is encoded as the namespace in the composite key. Windows don't "share" state.

3. **ReducingState/AggregatingState does a read-modify-write per window.** This is why `reduce()`/`aggregate()` is much more efficient than `apply()` for large windows — `apply()` uses `ListState` which stores every element.

4. **State is cleaned up independently per window.** When window `[-20s, 40s)` fires, only its entry is deleted. The overlapping windows `[0s, 60s)` and `[20s, 80s)` still retain their state.

5. **During a checkpoint**, all these RocksDB entries (across all keys and all windows) are snapshotted together as part of the RocksDB native checkpoint. There is no per-window checkpoint logic — it's all just key-value pairs in RocksDB.

6. **Sliding windows are NOT merging windows.** They take the simpler non-merging path in `WindowOperator.processElement()` (lines 404-432). Session windows are the only built-in merging window type.
