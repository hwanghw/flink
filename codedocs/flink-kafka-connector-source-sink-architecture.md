# Flink Kafka Connector: Source & Sink Architecture Deep Dive

This document provides a comprehensive architecture walkthrough of the Flink Kafka connector (`flink-connector-kafka`), covering the Source (with offset management and parallelism scaling) and the Sink (with exactly-once 2-phase commit at checkpoint).

All code references point to files under:
`flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/`

---

## Table of Contents

- [Part 1: Kafka Source Architecture](#part-1-kafka-source-architecture)
  - [1.1 High-Level Component Overview](#11-high-level-component-overview)
  - [1.2 The Split: KafkaPartitionSplit](#12-the-split-kafkapartitionsplit)
  - [1.3 KafkaSourceEnumerator: Partition Discovery and Assignment](#13-kafkasourceenumerator-partition-discovery-and-assignment)
    - [1.3.1 Split Lifecycle](#131-split-lifecycle-from-the-javadoc-in-kafkasourceenumeratorjava-lines-66-93)
    - [1.3.2 Startup](#132-startup)
    - [1.3.3 Partition Change Detection](#133-partition-change-detection)
    - [1.3.4 Offset Initialization](#134-offset-initialization)
    - [1.3.5 Partition-to-Reader Assignment (The Hash Algorithm)](#135-partition-to-reader-assignment-the-hash-algorithm)
    - [1.3.6 Assigning Splits to Readers](#136-assigning-splits-to-readers)
    - [1.3.7 Handling Failed Readers](#137-handling-failed-readers)
    - [1.3.8 Checkpoint](#138-checkpoint)
  - [1.4 Offset Management](#14-offset-management)
    - [1.4.1 Where Offsets Live](#141-where-offsets-live)
    - [1.4.2 Record Emission: Offset Tracking](#142-record-emission-offset-tracking)
    - [1.4.3 Snapshot State: Collecting Offsets for Checkpoint](#143-snapshot-state-collecting-offsets-for-checkpoint)
    - [1.4.4 Checkpoint Complete: Committing Offsets to Kafka](#144-checkpoint-complete-committing-offsets-to-kafka)
    - [1.4.5 Reader-Side: Fetch Loop and Stopping Logic](#145-reader-side-fetch-loop-and-stopping-logic)
    - [1.4.6 Reader-Side: Handling New Split Assignments](#146-reader-side-handling-new-split-assignments)
    - [1.4.7 Offset Commit on Checkpoint Complete](#147-offset-commit-on-checkpoint-complete)
  - [1.5 OffsetsInitializer Implementations](#15-offsetsinitializer-implementations)
  - [1.6 How Offsets Are Saved During Checkpoint (FLIP-27 State Framework)](#16-how-offsets-are-saved-during-checkpoint-flip-27-state-framework)
    - [1.6.1 The Two Checkpoint Paths](#161-the-two-checkpoint-paths)
    - [1.6.2 Reader-Side Checkpoint](#162-reader-side-checkpoint-sourceoperator--sourcereaderbase--kafkapartitionsplitstate)
    - [1.6.3 Coordinator-Side Checkpoint](#163-coordinator-side-checkpoint-sourcecoordinator--kafkasourceenumerator)
  - [1.7 Enumerator State Serialization Versions](#17-enumerator-state-serialization-versions)
  - [1.8 WatermarkStrategy Integration with Kafka Source](#18-watermarkstrategy-integration-with-kafka-source)
    - [1.8.1 How WatermarkStrategy Flows into the Source](#181-how-watermarkstrategy-flows-into-the-source)
    - [1.8.2 Per-Split Watermark Architecture](#182-per-split-watermark-architecture)
    - [1.8.3 Record Flow: From Kafka to Watermark](#183-record-flow-from-kafka-to-watermark)
    - [1.8.4 Periodic Watermark Emission](#184-periodic-watermark-emission)
    - [1.8.5 Combined Watermark = MIN(all splits)](#185-combined-watermark--minall-splits)
    - [1.8.6 Watermark Alignment (Advanced)](#186-watermark-alignment-advanced)
  - [1.9 assign() vs subscribe() — Why the Connector Uses assign()](#19-assign-vs-subscribe--why-the-connector-uses-assign)
  - [1.10 Kafka Lineage Integration](#110-kafka-lineage-integration)
- [Part 2: Kafka Sink Architecture](#part-2-kafka-sink-architecture)
  - [2.0 Delivery Guarantee Modes — NONE, AT_LEAST_ONCE, EXACTLY_ONCE](#20-delivery-guarantee-modes--none-at_least_once-exactly_once)
    - [2.0.1 Three Modes at a Glance](#201-three-modes-at-a-glance)
    - [2.0.2 Class Relationships](#202-class-relationships)
    - [2.0.3 KafkaWriter: The Non-Transactional Writer](#203-kafkawriter-the-non-transactional-writer-none-and-at_least_once)
    - [2.0.4 write() and the Async Callback](#204-write-and-the-async-callback)
    - [2.0.5 AT_LEAST_ONCE: Checkpoint Cycle](#205-at_least_once-checkpoint-cycle)
    - [2.0.6 Side-by-Side Comparison](#206-side-by-side-comparison)
  - [2.1 High-Level Component Overview (Exactly-Once)](#21-high-level-component-overview)
  - [2.2 FlinkKafkaInternalProducer: The Transaction State Machine](#22-flinkafkainternalproducer-the-transaction-state-machine)
  - [2.3 KafkaCommittable: What Gets Passed to the Committer](#23-kafkacommittable-what-gets-passed-to-the-committer)
  - [2.4 The Full 2-Phase Commit Protocol](#24-the-full-2-phase-commit-protocol)
  - [2.5 The Backchannel: Writer ↔ Committer Communication](#25-the-backchannel-writer--committer-communication)
  - [2.6 Transaction Naming Strategies](#26-transaction-naming-strategies)
  - [2.7 Recovery: Aborting Lingering Transactions](#27-recovery-aborting-lingering-transactions)
  - [2.8 Exactly-Once Guarantees: Why It Works](#28-exactly-once-guarantees-why-it-works)
  - [2.9 KafkaWriterState: What Gets Checkpointed](#29-kafkawriterstate-what-gets-checkpointed)
  - [2.10 Complete Timeline: One Checkpoint Cycle](#210-complete-timeline-one-checkpoint-cycle)

---

# Part 1: Kafka Source Architecture

## 1.1 High-Level Component Overview

The Kafka Source follows the FLIP-27 `Source` interface, splitting responsibilities between a coordinator-side **enumerator** and task-side **readers**.

```
┌─────────────────────────────────────────────────────────┐
│                     JobManager                          │
│                                                         │
│  KafkaSourceEnumerator (coordinator thread)             │
│    ├─ Discovers partitions via KafkaSubscriber          │
│    ├─ Initializes offsets via OffsetsInitializer        │
│    ├─ Assigns KafkaPartitionSplits to readers           │
│    └─ Checkpoints: assignedSplits + unassignedSplits    │
│                                                         │
└──────────────────┬──────────────────────────────────────┘
                   │ SplitsAssignment
                   ▼
┌─────────────────────────────────────────────────────────┐
│                   TaskManager (per subtask)              │
│                                                         │
│  KafkaSourceReader                                      │
│    ├─ KafkaSourceFetcherManager                         │
│    │     └─ KafkaPartitionSplitReader                   │
│    │           └─ KafkaConsumer (native Kafka client)    │
│    ├─ KafkaRecordEmitter (deserialize + track offset)   │
│    └─ Checkpoints: current offsets per partition         │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Entry point**: `KafkaSource.java` implements `Source<OUT, KafkaPartitionSplit, KafkaSourceEnumState>`.

```java
// KafkaSource.java — factory methods
public SourceReader<OUT, KafkaPartitionSplit> createReader(SourceReaderContext readerContext) {
    // Creates KafkaSourceReader wrapping KafkaSourceFetcherManager + KafkaRecordEmitter
}

public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> createEnumerator(
        SplitEnumeratorContext<KafkaPartitionSplit> enumContext) {
    return new KafkaSourceEnumerator(subscriber, startingOffsetsInitializer,
            stoppingOffsetsInitializer, props, enumContext, boundedness);
}

public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> restoreEnumerator(
        SplitEnumeratorContext<KafkaPartitionSplit> enumContext,
        KafkaSourceEnumState checkpoint) {
    return new KafkaSourceEnumerator(subscriber, startingOffsetsInitializer,
            stoppingOffsetsInitializer, props, enumContext, boundedness, checkpoint);
}
```

## 1.2 The Split: KafkaPartitionSplit

Each Kafka partition is represented as a **split** — the unit of work assigned to a reader.

```java
// source/split/KafkaPartitionSplit.java
public class KafkaPartitionSplit implements SourceSplit {
    // Special offset markers
    public static final long NO_STOPPING_OFFSET = Long.MIN_VALUE;  // Never stop (unbounded)
    public static final long EARLIEST_OFFSET = -2;                  // Seek to earliest
    public static final long COMMITTED_OFFSET = -3;                 // Use committed offset
    public static final long MIGRATED = Long.MIN_VALUE;             // Needs re-initialization

    private final TopicPartition tp;        // topic + partition id
    private final long startingOffset;       // where to begin reading
    private final long stoppingOffset;       // when to stop (bounded mode)
}
```

During reading, `KafkaPartitionSplitState` wraps the immutable split to track the **mutable current offset**:

```java
// source/split/KafkaPartitionSplitState.java
public class KafkaPartitionSplitState extends KafkaPartitionSplit {
    private long currentOffset;  // updated after each record is emitted

    public void setCurrentOffset(long currentOffset) {
        this.currentOffset = currentOffset;
    }

    // For checkpointing: converts back to immutable split using currentOffset as startingOffset
    public KafkaPartitionSplit toKafkaPartitionSplit() {
        return new KafkaPartitionSplit(
                getTopicPartition(),
                getCurrentOffset(),  // ← currentOffset becomes the new startingOffset
                getStoppingOffset().orElse(NO_STOPPING_OFFSET));
    }
}
```

## 1.3 KafkaSourceEnumerator: Partition Discovery and Assignment

The enumerator runs on the **coordinator thread** (JobManager) and manages the full lifecycle of splits.

### 1.3.1 Split Lifecycle (from the Javadoc in KafkaSourceEnumerator.java, lines 66-93)

```
1. getSubscribedTopicPartitions()     [worker thread]  → Fetches current partitions from Kafka broker
2. checkPartitionChanges()            [coordinator]    → Diffs against known splits
3. initializePartitionSplits()        [worker thread]  → Resolves starting/stopping offsets
4. handlePartitionSplitChanges()      [coordinator]    → Moves splits to unassigned + pending
5. assignPendingPartitionSplits()     [coordinator]    → Assigns to registered readers
6. snapshotState()                    [coordinator]    → Checkpoints assignedSplits + unassignedSplits
```

### 1.3.2 Startup

```java
// KafkaSourceEnumerator.java, lines 231-262
@Override
public void start() {
    adminClient = getKafkaAdminClient();

    // Find pre-initialized splits not yet assigned (e.g., from a restored checkpoint)
    final List<KafkaPartitionSplit> preinitializedSplits =
            unassignedSplits.values().stream()
                    .filter(split -> !split.isMigrated())
                    .collect(Collectors.toList());
    if (!preinitializedSplits.isEmpty()) {
        addPartitionSplitChangeToPendingAssignments(preinitializedSplits);
    }

    if (partitionDiscoveryIntervalMs > 0) {
        // Periodic discovery: runs getSubscribedTopicPartitions() → checkPartitionChanges()
        // on a recurring schedule
        context.callAsync(
                this::getSubscribedTopicPartitions,
                this::checkPartitionChanges,
                0,                              // initial delay
                partitionDiscoveryIntervalMs);  // repeat interval
    } else {
        // One-time discovery
        context.callAsync(this::getSubscribedTopicPartitions, this::checkPartitionChanges);
    }
}
```

### 1.3.3 Partition Change Detection

```java
// KafkaSourceEnumerator.java, lines 521-557
PartitionChange getPartitionChange(Set<TopicPartition> fetchedPartitions, boolean initialDiscovery) {
    final Set<TopicPartition> removedPartitions = new HashSet<>();
    Set<TopicPartition> newPartitions = new HashSet<>(fetchedPartitions);

    // Remove already-known partitions from the "new" set
    Consumer<TopicPartition> dedupOrMarkAsRemoved = (tp) -> {
        if (!newPartitions.remove(tp)) {
            removedPartitions.add(tp);
        }
    };
    assignedSplits.keySet().forEach(dedupOrMarkAsRemoved);
    pendingPartitionSplitAssignment.forEach(
            (reader, splits) -> splits.forEach(
                    split -> dedupOrMarkAsRemoved.accept(split.getTopicPartition())));

    Set<TopicPartition> initialPartitions = new HashSet<>();
    if (initialDiscovery) {
        // First discovery: ALL partitions are "initial" (use user-configured starting offsets)
        initialPartitions.addAll(newPartitions);
        newPartitions.clear();
    }

    // Migration path: splits without initialized offsets need re-initialization
    for (KafkaPartitionSplit split : unassignedSplits.values()) {
        if (split.isMigrated()) {
            initialPartitions.add(split.getTopicPartition());
        }
    }
    return new PartitionChange(initialPartitions, newPartitions, removedPartitions);
}
```

**Key distinction**: `initialPartitions` use the user-configured `startingOffsetsInitializer` (e.g., `earliest()`, `timestamp()`), while `newPartitions` (discovered after initial startup) always use `EARLIEST` to avoid skipping data.

### 1.3.4 Offset Initialization

```java
// KafkaSourceEnumerator.java, lines 365-397
private PartitionSplitChange initializePartitionSplits(PartitionChange partitionChange) {
    Set<TopicPartition> newPartitions = partitionChange.getNewPartitions();
    Set<TopicPartition> initialPartitions = partitionChange.getInitialPartitions();

    Map<TopicPartition, Long> startingOffsets = new LinkedHashMap<>();
    Map<TopicPartition, Long> stoppingOffsets = new LinkedHashMap<>();

    // New partitions (discovered later) always start from EARLIEST
    if (!newPartitions.isEmpty()) {
        initOffsets(newPartitions, newDiscoveryOffsetsInitializer, startingOffsets, stoppingOffsets);
    }
    // Initial partitions use user-configured OffsetsInitializer
    if (!initialPartitions.isEmpty()) {
        initOffsets(initialPartitions, startingOffsetInitializer, startingOffsets, stoppingOffsets);
    }

    // Create KafkaPartitionSplit for each partition with resolved offsets
    Set<KafkaPartitionSplit> partitionSplits = new HashSet<>();
    for (Entry<TopicPartition, Long> entry : startingOffsets.entrySet()) {
        TopicPartition tp = entry.getKey();
        long startingOffset = entry.getValue();
        long stoppingOffset = stoppingOffsets.getOrDefault(tp, KafkaPartitionSplit.NO_STOPPING_OFFSET);
        partitionSplits.add(new KafkaPartitionSplit(tp, startingOffset, stoppingOffset));
    }
    return new PartitionSplitChange(partitionSplits, partitionChange.getRemovedPartitions());
}
```

### 1.3.5 Partition-to-Reader Assignment (The Hash Algorithm)

```java
// KafkaSourceEnumerator.java, lines 599-606
static int getSplitOwner(TopicPartition tp, int numReaders) {
    int startIndex = ((tp.topic().hashCode() * 31) & 0x7FFFFFFF) % numReaders;

    // Assumption: Kafka partition IDs are ascending from 0,
    // so they can be used directly as the offset clockwise from the start index
    return (startIndex + tp.partition()) % numReaders;
}
```

**Properties of this distribution:**
- **Uniform**: for a single topic with N partitions and M subtasks, partitions are evenly distributed
- **Deterministic**: same topic+partition always maps to the same reader for a given parallelism
- **Round-robin per topic**: partitions 0,1,2,3... are assigned clockwise from a topic-specific starting index

### 1.3.6 Assigning Splits to Readers

```java
// KafkaSourceEnumerator.java, lines 441-466
private void addPartitionSplitChangeToPendingAssignments(
        Collection<KafkaPartitionSplit> newPartitionSplits) {
    int numReaders = context.currentParallelism();
    // Sort for deterministic assignment
    List<KafkaPartitionSplit> sortedSplits = new ArrayList<>(newPartitionSplits);
    sortedSplits.sort(Comparator.comparing(
                    (KafkaPartitionSplit split) -> split.getTopicPartition().topic())
            .thenComparingInt(split -> split.getTopicPartition().partition()));

    for (KafkaPartitionSplit split : sortedSplits) {
        int ownerReader = splitOwnerSelector.getSplitOwner(split, numReaders);
        pendingPartitionSplitAssignment
                .computeIfAbsent(ownerReader, r -> new HashSet<>())
                .add(split);
    }
}

// KafkaSourceEnumerator.java, lines 469-511
private void assignPendingPartitionSplits(Set<Integer> pendingReaders) {
    Map<Integer, List<KafkaPartitionSplit>> incrementalAssignment = new HashMap<>();

    for (int pendingReader : pendingReaders) {
        final Set<KafkaPartitionSplit> pendingAssignmentForReader =
                pendingPartitionSplitAssignment.remove(pendingReader);

        if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
            incrementalAssignment
                    .computeIfAbsent(pendingReader, (ignored) -> new ArrayList<>())
                    .addAll(pendingAssignmentForReader);

            // Track: move from unassigned → assigned
            pendingAssignmentForReader.forEach(split -> {
                assignedSplits.put(split.getTopicPartition(), split);
                unassignedSplits.remove(split.getTopicPartition());
            });
        }
    }

    if (!incrementalAssignment.isEmpty()) {
        context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
    }

    // Signal no more splits for bounded sources
    if (noMoreNewPartitionSplits && boundedness == Boundedness.BOUNDED) {
        pendingReaders.forEach(context::signalNoMoreSplits);
    }
}
```

### 1.3.7 Handling Failed Readers

```java
// KafkaSourceEnumerator.java, lines 270-281
@Override
public void addSplitsBack(List<KafkaPartitionSplit> splits, int subtaskId) {
    // Move splits back from assigned to unassigned
    for (KafkaPartitionSplit split : splits) {
        unassignedSplits.put(split.getTopicPartition(), split);
        assignedSplits.remove(split.getTopicPartition());
    }
    addPartitionSplitChangeToPendingAssignments(splits);

    // If the failed subtask has already restarted, assign immediately
    if (context.registeredReaders().containsKey(subtaskId)) {
        assignPendingPartitionSplits(Collections.singleton(subtaskId));
    }
}
```

### 1.3.8 Checkpoint

```java
// KafkaSourceEnumerator.java, lines 293-296
@Override
public KafkaSourceEnumState snapshotState(long checkpointId) throws Exception {
    return new KafkaSourceEnumState(
            assignedSplits.values(), unassignedSplits.values(), initialDiscoveryFinished);
}
```

The `KafkaSourceEnumState` captures:
- All assigned splits (with their current offsets)
- All unassigned splits (discovered but not yet sent to readers)
- Whether initial partition discovery has completed

---

## 1.4 Offset Management

### 1.4.1 Where Offsets Live

| Location | What's Stored | When Updated |
|----------|--------------|--------------|
| `KafkaPartitionSplitState.currentOffset` | Next offset to read | After each record emitted |
| `KafkaSourceReader.offsetsToCommit` | `SortedMap<checkpointId → Map<TP, Offset>>` | On `snapshotState()` |
| Kafka `__consumer_offsets` | Committed offsets | On `notifyCheckpointComplete()` |
| Flink checkpoint blob | Split offsets (via serializer) | Each checkpoint barrier |

### 1.4.2 Record Emission: Offset Tracking

```java
// source/reader/KafkaRecordEmitter.java, lines 45-58
@Override
public void emitRecord(
        ConsumerRecord<byte[], byte[]> consumerRecord,
        SourceOutput<T> output,
        KafkaPartitionSplitState splitState) throws Exception {
    try {
        sourceOutputWrapper.setSourceOutput(output);
        sourceOutputWrapper.setTimestamp(consumerRecord.timestamp());
        deserializationSchema.deserialize(consumerRecord, sourceOutputWrapper);

        // CRITICAL: advance the offset AFTER deserialization
        // offset + 1 = the NEXT record to read (not the current one)
        splitState.setCurrentOffset(consumerRecord.offset() + 1);
    } catch (Exception e) {
        throw new IOException("Failed to deserialize consumer record due to", e);
    }
}
```

### 1.4.3 Snapshot State: Collecting Offsets for Checkpoint

```java
// source/reader/KafkaSourceReader.java, lines 98-123
@Override
public List<KafkaPartitionSplit> snapshotState(long checkpointId) {
    // super.snapshotState() calls toSplitType() on each active split,
    // which converts KafkaPartitionSplitState → KafkaPartitionSplit using currentOffset
    List<KafkaPartitionSplit> splits = super.snapshotState(checkpointId);

    if (!commitOffsetsOnCheckpoint) {
        return splits;
    }

    if (splits.isEmpty() && offsetsOfFinishedSplits.isEmpty()) {
        offsetsToCommit.put(checkpointId, Collections.emptyMap());
    } else {
        Map<TopicPartition, OffsetAndMetadata> offsetsMap =
                offsetsToCommit.computeIfAbsent(checkpointId, id -> new HashMap<>());

        // Collect offsets from active splits
        for (KafkaPartitionSplit split : splits) {
            if (split.getStartingOffset() >= 0) {
                offsetsMap.put(
                        split.getTopicPartition(),
                        new OffsetAndMetadata(split.getStartingOffset()));
            }
        }
        // Include offsets from finished splits
        offsetsMap.putAll(offsetsOfFinishedSplits);
    }
    return splits;
}
```

### 1.4.4 Checkpoint Complete: Committing Offsets to Kafka

```java
// source/reader/KafkaSourceReader.java, lines 126-177
@Override
public void notifyCheckpointComplete(long checkpointId) throws Exception {
    if (!commitOffsetsOnCheckpoint) return;

    Map<TopicPartition, OffsetAndMetadata> committedPartitions = offsetsToCommit.get(checkpointId);
    if (committedPartitions == null || committedPartitions.isEmpty()) {
        removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
        return;
    }

    // Asynchronous commit via KafkaConsumer.commitAsync()
    ((KafkaSourceFetcherManager) splitFetcherManager)
            .commitOffsets(committedPartitions, (ignored, e) -> {
                if (e != null) {
                    // IMPORTANT: failure to commit does NOT fail the job
                    // Flink state is authoritative; Kafka offsets are for external monitoring
                    kafkaSourceReaderMetrics.recordFailedCommit();
                    LOG.warn("Failed to commit consumer offsets for checkpoint {}", checkpointId, e);
                } else {
                    kafkaSourceReaderMetrics.recordSucceededCommit();
                    committedPartitions.forEach((tp, offset) ->
                            kafkaSourceReaderMetrics.recordCommittedOffset(tp, offset.offset()));
                    // Clean up finished splits that have been committed
                    offsetsOfFinishedSplits.entrySet().removeIf(
                            entry -> committedPartitions.containsKey(entry.getKey()));
                    removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
                }
            });
}
```

### 1.4.5 Reader-Side: Fetch Loop and Stopping Logic

```java
// source/reader/KafkaPartitionSplitReader.java, lines 107-163
@Override
public RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> fetch() throws IOException {
    ConsumerRecords<byte[], byte[]> consumerRecords;
    try {
        consumerRecords = consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
    } catch (WakeupException | IllegalStateException e) {
        // All assigned partitions are invalid or empty
        KafkaPartitionSplitRecords recordsBySplits =
                new KafkaPartitionSplitRecords(ConsumerRecords.empty(), kafkaSourceReaderMetrics);
        markEmptySplitsAsFinished(recordsBySplits);
        return recordsBySplits;
    }

    KafkaPartitionSplitRecords recordsBySplits =
            new KafkaPartitionSplitRecords(consumerRecords, kafkaSourceReaderMetrics);
    List<TopicPartition> finishedPartitions = new ArrayList<>();

    for (TopicPartition tp : consumer.assignment()) {
        long stoppingOffset = getStoppingOffset(tp);
        long consumerPosition = getConsumerPosition(tp, "retrieving consumer position");

        // Stop when consumer position reaches or exceeds the stopping offset
        if (consumerPosition >= stoppingOffset) {
            recordsBySplits.setPartitionStoppingOffset(tp, stoppingOffset);
            finishSplitAtRecord(tp, stoppingOffset, consumerPosition,
                    finishedPartitions, recordsBySplits);
        }
    }

    // Unassign finished partitions from the consumer
    if (!finishedPartitions.isEmpty()) {
        finishedPartitions.forEach(kafkaSourceReaderMetrics::removeRecordsLagMetric);
        unassignPartitions(finishedPartitions);
    }

    return recordsBySplits;
}
```

### 1.4.6 Reader-Side: Handling New Split Assignments

```java
// source/reader/KafkaPartitionSplitReader.java, lines 175-227
@Override
public void handleSplitsChanges(SplitsChange<KafkaPartitionSplit> splitsChange) {
    // Parse starting offsets from each split
    List<TopicPartition> partitionsStartingFromEarliest = new ArrayList<>();
    List<TopicPartition> partitionsStartingFromLatest = new ArrayList<>();
    Map<TopicPartition, Long> partitionsStartingFromSpecifiedOffsets = new HashMap<>();

    splitsChange.splits().forEach(s -> {
        newPartitionAssignments.add(s.getTopicPartition());
        parseStartingOffsets(s,
                partitionsStartingFromEarliest,
                partitionsStartingFromLatest,
                partitionsStartingFromSpecifiedOffsets);
        parseStoppingOffsets(s, partitionsStoppingAtLatest, partitionsStoppingAtCommitted);
    });

    // Assign partitions to the native KafkaConsumer
    newPartitionAssignments.addAll(consumer.assignment());
    consumer.assign(newPartitionAssignments);

    // Seek to starting offsets:
    //   EARLIEST_OFFSET → consumer.seekToBeginning()
    //   LATEST_OFFSET   → consumer.seekToEnd()
    //   Concrete offset  → consumer.seek(tp, offset)
    seekToStartingOffsets(partitionsStartingFromEarliest,
            partitionsStartingFromLatest,
            partitionsStartingFromSpecifiedOffsets);

    // Resolve stopping offsets:
    //   LATEST_OFFSET    → consumer.endOffsets()
    //   COMMITTED_OFFSET → consumer.committed()
    acquireAndSetStoppingOffsets(partitionsStoppingAtLatest, partitionsStoppingAtCommitted);

    // Remove empty splits (startingOffset >= stoppingOffset)
    removeEmptySplits();
}
```

### 1.4.7 Offset Commit on Checkpoint Complete

```java
// source/reader/KafkaPartitionSplitReader.java, lines 255-259
public void notifyCheckpointComplete(
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
        OffsetCommitCallback offsetCommitCallback) {
    consumer.commitAsync(offsetsToCommit, offsetCommitCallback);
}
```

---

## 1.5 OffsetsInitializer Implementations

The `OffsetsInitializer` interface resolves starting/stopping offsets for partitions:

```java
// source/enumerator/initializer/OffsetsInitializer.java
public interface OffsetsInitializer {
    Map<TopicPartition, Long> getPartitionOffsets(
            Collection<TopicPartition> partitions,
            PartitionOffsetsRetriever partitionOffsetsRetriever);

    OffsetResetStrategy getAutoOffsetResetStrategy();
}
```

### Implementations

| Factory Method | Implementation Class | Resolution Strategy |
|---|---|---|
| `OffsetsInitializer.earliest()` | `ReaderHandledOffsetsInitializer` | Returns marker `EARLIEST_OFFSET = -2`. Reader calls `consumer.seekToBeginning()` |
| `OffsetsInitializer.latest()` | `LatestOffsetsInitializer` | Resolved in enumerator via `admin.listOffsets(OffsetSpec.latest())`. Returns concrete offsets. |
| `OffsetsInitializer.committedOffsets()` | `ReaderHandledOffsetsInitializer` | Returns marker `COMMITTED_OFFSET = -3`. Consumer auto-seeks to committed offset. |
| `OffsetsInitializer.timestamp(long)` | `TimestampOffsetsInitializer` | Resolved in enumerator via `admin.listOffsets(OffsetSpec.forTimestamp())`. Falls back to end offset. |
| `OffsetsInitializer.offsets(Map)` | `SpecifiedOffsetsInitializer` | User-provided map. Partitions not in map fall back to committed or reset strategy. |

**ReaderHandledOffsetsInitializer** — defers to the reader:
```java
// source/enumerator/initializer/ReaderHandledOffsetsInitializer.java
@Override
public Map<TopicPartition, Long> getPartitionOffsets(
        Collection<TopicPartition> partitions,
        PartitionOffsetsRetriever partitionOffsetsRetriever) {
    // Returns a map where every partition maps to the same marker (e.g., EARLIEST_OFFSET)
    // The actual seek happens in KafkaPartitionSplitReader.handleSplitsChanges()
    return partitions.stream().collect(Collectors.toMap(tp -> tp, tp -> startingOffset));
}
```

**TimestampOffsetsInitializer** — queries broker:
```java
// source/enumerator/initializer/TimestampOffsetsInitializer.java
@Override
public Map<TopicPartition, Long> getPartitionOffsets(
        Collection<TopicPartition> partitions,
        PartitionOffsetsRetriever partitionOffsetsRetriever) {
    Map<TopicPartition, Long> timestampToSearch =
            partitions.stream().collect(Collectors.toMap(tp -> tp, tp -> timestamp));
    Map<TopicPartition, Long> offsets = new HashMap<>();
    // Query Kafka broker for offsets matching the timestamp
    partitionOffsetsRetriever.offsetsForTimes(timestampToSearch)
            .forEach((tp, offsetAndTimestamp) -> offsets.put(tp, offsetAndTimestamp.offset()));
    // Partitions with no matching timestamp: fall back to end offset
    Map<TopicPartition, Long> endOffsets = partitionOffsetsRetriever.endOffsets(
            partitions.stream().filter(tp -> !offsets.containsKey(tp)).collect(Collectors.toList()));
    offsets.putAll(endOffsets);
    return offsets;
}
```

---

## 1.6 How Offsets Are Saved During Checkpoint (FLIP-27 State Framework)

A common misconception is that the Kafka Source uses `UnionListState` for offset storage. In fact, it uses **regular `ListState<byte[]>`** wrapped by `SimpleVersionedListState`. The checkpoint involves **two parallel paths**: reader-side (per subtask) and coordinator-side (single instance).

### 1.6.1 The Two Checkpoint Paths

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                        CHECKPOINT BARRIER                                      │
│                              │                                                 │
│     ┌────────────────────────┼─────────────────────────────┐                   │
│     │                        │                             │                   │
│     ▼                        ▼                             ▼                   │
│  Subtask 0               Subtask 1          ...        Subtask 9              │
│  SourceOperator          SourceOperator                SourceOperator          │
│  .snapshotState()        .snapshotState()              .snapshotState()        │
│     │                        │                             │                   │
│     ▼                        ▼                             ▼                   │
│  reader.snapshotState()  reader.snapshotState()        reader.snapshotState()  │
│  → [P0@100, P10@300,    → [P1@200, P11@350,           → [P9@150, P19@275,    │
│     P20@400]                P21@250]                      P29@500]             │
│     │                        │                             │                   │
│     ▼                        ▼                             ▼                   │
│  ListState<byte[]>       ListState<byte[]>             ListState<byte[]>       │
│  (own splits only)       (own splits only)             (own splits only)       │
│                                                                                │
│  ═══════════════════════════════════════════════════════════════════            │
│                                                                                │
│                    SourceCoordinator (JobManager)                               │
│                    .checkpointCoordinator()                                     │
│                              │                                                 │
│                              ▼                                                 │
│                    enumerator.snapshotState()                                   │
│                    → KafkaSourceEnumState {                                     │
│                        ALL 30 assignedSplits with offsets                       │
│                        unassignedSplits                                         │
│                        initialDiscoveryFinished                                 │
│                      }                                                          │
│                              │                                                 │
│                              ▼                                                 │
│                    Coordinator State (single copy)                              │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 1.6.2 Reader-Side Checkpoint: SourceOperator → SourceReaderBase → KafkaPartitionSplitState

**Step 1**: `SourceOperator.snapshotState()` triggers the reader snapshot:

```java
// SourceOperator.java (flink-runtime), lines 650-660
@Override
public void snapshotState(StateSnapshotContext context) throws Exception {
    long checkpointId = context.getCheckpointId();
    LOG.debug("Taking a snapshot for checkpoint {}", checkpointId);
    readerState.update(sourceReader.snapshotState(checkpointId));
}
```

**Step 2**: `SourceReaderBase.snapshotState()` collects current splits from the mutable state map:

```java
// SourceReaderBase.java (flink-connector-base), lines 348-352
@Override
public List<SplitT> snapshotState(long checkpointId) {
    List<SplitT> splits = new ArrayList<>();
    splitStates.forEach((id, context) -> splits.add(toSplitType(id, context.state)));
    return splits;
}
```

`splitStates` is a `Map<String, SplitContext<T, SplitStateT>>` holding only **this subtask's** active splits. Each split's mutable `KafkaPartitionSplitState` is converted back to an immutable `KafkaPartitionSplit`:

```java
// KafkaSourceReader.java, lines 191-193
@Override
protected KafkaPartitionSplit toSplitType(String splitId, KafkaPartitionSplitState splitState) {
    return splitState.toKafkaPartitionSplit();
}

// KafkaPartitionSplitState.java, lines 50-55
public KafkaPartitionSplit toKafkaPartitionSplit() {
    return new KafkaPartitionSplit(
            getTopicPartition(),
            getCurrentOffset(),      // ← currentOffset becomes the new startingOffset!
            getStoppingOffset().orElse(NO_STOPPING_OFFSET));
}
```

**Step 3**: `SimpleVersionedListState.update()` serializes each split and stores in state backend:

```java
// SimpleVersionedListState.java (flink-runtime), lines 70-72
@Override
public void update(@Nullable List<T> values) throws Exception {
    rawState.update(serializeAll(values));
}

private byte[] serialize(T value) throws IOException {
    return SimpleVersionedSerialization.writeVersionAndSerialize(serializer, value);
    // serializer = KafkaPartitionSplitSerializer (provided by KafkaSource.getSplitSerializer())
}
```

**The state descriptor** (NOT union list state):

```java
// SourceOperator.java, lines 114-115
static final ListStateDescriptor<byte[]> SPLITS_STATE_DESC =
        new ListStateDescriptor<>("SourceReaderState", BytePrimitiveArraySerializer.INSTANCE);

// SourceOperator.initializeState(), lines 684-688
@Override
public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    final ListState<byte[]> rawState =
            context.getOperatorStateStore().getListState(SPLITS_STATE_DESC);  // Regular ListState!
    readerState = new SimpleVersionedListState<>(rawState, splitSerializer);
}
```

### 1.6.3 Coordinator-Side Checkpoint: SourceCoordinator → KafkaSourceEnumerator

The coordinator runs on the JobManager and checkpoints the **full enumerator state** (all partitions across all subtasks):

```java
// SourceCoordinator.resetToCheckpoint(), lines 531-536
final EnumChkT enumeratorCheckpoint = deserializeCheckpoint(checkpointData);
enumerator = source.restoreEnumerator(context, enumeratorCheckpoint);

// KafkaSource.restoreEnumerator() → new KafkaSourceEnumerator(..., kafkaSourceEnumState)
// KafkaSourceEnumerator constructor, lines 214-215
this.assignedSplits = indexByPartition(kafkaSourceEnumState.assignedSplits());
this.unassignedSplits = indexByPartition(kafkaSourceEnumState.unassignedSplits());
```

The enumerator now has **all 30 splits** with their checkpoint offsets. This is the source of truth.

**Step 2: Reader ListState redistributed to 20 subtasks**

Flink's state redistribution takes the 10 `ListState<byte[]>` copies and redistributes entries round-robin across 20 new subtasks:

```
Old 10 subtasks (30 splits total) → Flink round-robin redistribution → 20 new subtasks

  New Subtask 0:  may get [P0@15000, P1@23000, P2@8000]  (from old subtask 0)
  New Subtask 1:  may get [P3@45000, P4@12000, P5@31000]  (from old subtask 1)
  ...
  New Subtask 9:  may get [P27@9000, P28@55000, P29@7000] (from old subtask 9)
  New Subtask 10: empty (no old state)
  New Subtask 11: empty
  ...
  New Subtask 19: empty
```

Note: The redistribution is non-deterministic and subtasks may get splits they shouldn't own.

**Step 3: Each reader restores splits locally**

```java
// SourceOperator.open(), lines 439-447
final List<SplitT> splits = CollectionUtil.iterableToList(readerState.get());
if (!splits.isEmpty() && !supportsSplitReassignmentOnRecovery) {
    // KafkaSource does NOT implement SupportsSplitReassignmentOnRecovery
    // So supportsSplitReassignmentOnRecovery = false
    sourceReader.addSplits(splits);  // Reader adds whatever splits it got
}

// Register with coordinator, passing EMPTY list (not reassignment mode)
registerReader(supportsSplitReassignmentOnRecovery ? splits : Collections.emptyList());
```

**Step 4: Enumerator redistributes splits correctly**

When each reader registers via `addReader(subtaskId)`, the enumerator assigns splits based on the **new parallelism**:

```java
// KafkaSourceEnumerator.start() queues unassigned splits for reassignment
// Then addReader() triggers:
@Override
public void addReader(int subtaskId) {
    assignPendingPartitionSplits(Collections.singleton(subtaskId));
}

// getSplitOwner() with numReaders=20:
int startIndex = ((tp.topic().hashCode() * 31) & 0x7FFFFFFF) % numReaders;
return (startIndex + tp.partition()) % numReaders;
```

**Concrete example** (assuming `"orders".hashCode() * 31 & 0x7FFFFFFF = 1234567`):

```
With numReaders=10:  startIndex = 1234567 % 10 = 7
  P0 → (7+0)%10 = 7,  P1 → (7+1)%10 = 8,  P2 → (7+2)%10 = 9
  P3 → (7+3)%10 = 0,  P4 → (7+4)%10 = 1,  P5 → (7+5)%10 = 2
  ...each subtask gets exactly 3 partitions

With numReaders=20:  startIndex = 1234567 % 20 = 7
  P0  → 7,   P1  → 8,   P2  → 9,   P3  → 10,  P4  → 11
  P5  → 12,  P6  → 13,  P7  → 14,  P8  → 15,  P9  → 16
  P10 → 17,  P11 → 18,  P12 → 19,  P13 → 0,   P14 → 1
  P15 → 2,   P16 → 3,   P17 → 4,   P18 → 5,   P19 → 6
  P20 → 7,   P21 → 8,   P22 → 9,   P23 → 10,  P24 → 11
  P25 → 12,  P26 → 13,  P27 → 14,  P28 → 15,  P29 → 16

  Subtasks 0-6: 1 partition each, Subtasks 7-16: 2 partitions each, Subtasks 17-19: 1 partition each
```

### Summary: Who Owns the Truth?

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                   STATE OWNERSHIP DURING RECOVERY                            │
│                                                                              │
│  Coordinator (enumerator):                                                   │
│    ✓ Has ALL 30 splits with checkpoint offsets                               │
│    ✓ Knows the correct partition→subtask mapping (via getSplitOwner)         │
│    ✓ SOURCE OF TRUTH for split assignment                                    │
│                                                                              │
│  Reader (per subtask):                                                       │
│    △ Has SOME splits from ListState redistribution (may be wrong subtask)    │
│    △ These are used as a LOCAL OPTIMIZATION — if a split happens to land     │
│      on its correct new owner, the reader already has it                     │
│    △ The enumerator will send the correct assignments regardless             │
│                                                                              │
│  Net result: Enumerator is authoritative. Reader state is best-effort.       │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Key Guarantees

1. **No data loss**: The enumerator's `KafkaSourceEnumState` contains all 30 splits with their exact checkpoint offsets. These offsets survive parallelism changes.
2. **No duplicates**: Flink checkpoint state (not Kafka committed offsets) is the source of truth. Each split resumes from its checkpoint offset via `consumer.seek(tp, offset)`.
3. **Correct redistribution**: The enumerator's `getSplitOwner()` deterministically computes the new mapping and assigns splits to the right subtasks.

---

## 1.7 Enumerator State Serialization Versions

The `KafkaSourceEnumStateSerializer` has evolved through 4 versions:

| Version | Format | Notes |
|---------|--------|-------|
| V0 | `Map<subtaskId, List<KafkaPartitionSplit>>` | Legacy: tied splits to subtask IDs |
| V1 | `Set<KafkaPartitionSplit>` (assigned only) | All splits marked as `MIGRATED` on restore |
| V2 | `Set<(TopicPartition, AssignmentStatus)>` + `initialDiscoveryFinished` | No offset stored in splits |
| V3 (current) | Full `KafkaPartitionSplit` with offsets + `AssignmentStatus` + `initialDiscoveryFinished` | Complete state preservation |

When restoring from V1/V2, splits have `startingOffset = MIGRATED`. The enumerator detects this in `getPartitionChange()` and re-injects them into the offset initialization pipeline.

---

## 1.8 WatermarkStrategy Integration with Kafka Source

The Kafka Source integrates with Flink's WatermarkStrategy through the FLIP-27 framework, providing **per-split watermark tracking** — each Kafka partition gets its own WatermarkGenerator instance.

### 1.8.1 How WatermarkStrategy Flows into the Source

```
User Code:
  env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "kafka")
       │
       ▼
  DataStreamSource → SourceTransformation(source, watermarkStrategy)
       │
       ▼
  SourceOperatorFactory.createStreamOperator()
       │
       ▼
  new SourceOperator(... watermarkStrategy ...)
       │
       ▼
  SourceOperator.open():
    eventTimeLogic = TimestampsAndWatermarks.createProgressiveEventTimeLogic(
        watermarkStrategy,               // user's WatermarkStrategy
        sourceMetricGroup,
        getProcessingTimeService(),
        getExecutionConfig().getAutoWatermarkInterval(),  // default 200ms
        mainInputActivityClock,
        getProcessingTimeService().getClock(),
        taskIOMetricGroup);

    eventTimeLogic.startPeriodicWatermarkEmits();  // schedules periodic watermark emission
```

### 1.8.2 Per-Split Watermark Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  SourceOperator (Subtask 0, assigned partitions P0, P5, P10)                │
│                                                                             │
│  ProgressiveTimestampsAndWatermarks                                         │
│    │                                                                        │
│    ├─ StreamingReaderOutput (main output)                                    │
│    │     └─ WatermarkGenerator (main, for records not assigned to a split)  │
│    │                                                                        │
│    └─ SplitLocalOutputs                                                     │
│         │                                                                   │
│         ├─ Split "orders-0" (P0):                                           │
│         │    ├─ SourceOutputWithWatermarks                                  │
│         │    │    ├─ TimestampAssigner (extracts timestamp from record)      │
│         │    │    └─ WatermarkGenerator (per-split instance)                │
│         │    └─ WatermarkOutputMultiplexer.ImmediateOutput[P0]              │
│         │                                                                   │
│         ├─ Split "orders-5" (P5):                                           │
│         │    ├─ SourceOutputWithWatermarks                                  │
│         │    │    ├─ TimestampAssigner                                      │
│         │    │    └─ WatermarkGenerator (separate instance)                 │
│         │    └─ WatermarkOutputMultiplexer.ImmediateOutput[P5]              │
│         │                                                                   │
│         └─ Split "orders-10" (P10):                                         │
│              ├─ SourceOutputWithWatermarks                                  │
│              │    ├─ TimestampAssigner                                      │
│              │    └─ WatermarkGenerator (separate instance)                 │
│              └─ WatermarkOutputMultiplexer.ImmediateOutput[P10]             │
│                                                                             │
│         WatermarkOutputMultiplexer                                          │
│           Combined Watermark = MIN(P0_wm, P5_wm, P10_wm)                   │
│                     │                                                       │
│                     ▼                                                       │
│           WatermarkToDataOutput → emitWatermark() → downstream              │
└─────────────────────────────────────────────────────────────────────────────┘
```

Each split gets its own `WatermarkGenerator` created from the user's `WatermarkStrategy`:

```java
// ProgressiveTimestampsAndWatermarks.SplitLocalOutputs, lines 266-303
SourceOutput<T> createOutputForSplit(String splitId) {
    // Register with multiplexer for combined watermark tracking
    watermarkMultiplexer.registerNewOutput(splitId, new WatermarkUpdateListener() {
        @Override
        public void onWatermarkUpdate(long watermark) {
            watermarkUpdateListener.updateCurrentSplitWatermark(splitId, watermark);
        }
        @Override
        public void onIdleUpdate(boolean idle) {
            watermarkUpdateListener.updateCurrentSplitIdle(splitId, idle);
        }
    });

    final WatermarkOutput onEventOutput = watermarkMultiplexer.getImmediateOutput(splitId);
    final WatermarkOutput periodicOutput = watermarkMultiplexer.getDeferredOutput(splitId);

    // Create a SEPARATE WatermarkGenerator for this split
    final WatermarkGenerator<T> watermarks =
            watermarksFactory.createWatermarkGenerator(
                    watermarksContextProvider.create(inputActivityClock));

    final SourceOutputWithWatermarks<T> localOutput =
            SourceOutputWithWatermarks.createWithSeparateOutputs(
                    recordOutput, onEventOutput, periodicOutput,
                    timestampAssigner, watermarks);

    localOutputs.put(splitId, localOutput);
    return localOutput;
}
```

### 1.8.3 Record Flow: From Kafka to Watermark

```
Kafka ConsumerRecord (partition P0, offset 42, timestamp 1703000000000)
       │
       ▼
KafkaRecordEmitter.emitRecord()
       │
       ├─ sourceOutputWrapper.setTimestamp(consumerRecord.timestamp())  // 1703000000000
       ├─ deserializationSchema.deserialize(consumerRecord, sourceOutputWrapper)
       │     └─ sourceOutputWrapper.collect(record)  // calls output.collect(record, 1703000000000)
       └─ splitState.setCurrentOffset(consumerRecord.offset() + 1)    // 43
       │
       ▼
SourceOutputWithWatermarks.collect(record, timestamp=1703000000000)
       │
       ├─ long assignedTimestamp = timestampAssigner.extractTimestamp(record, 1703000000000)
       │    // Default RecordTimestampAssigner: returns 1703000000000 (Kafka's timestamp)
       │    // Custom: user can override to extract from record fields
       │
       ├─ recordsOutput.emitRecord(StreamRecord(record, assignedTimestamp))
       │    → downstream operator receives record with event timestamp
       │
       └─ watermarkGenerator.onEvent(record, assignedTimestamp, onEventWatermarkOutput)
            // e.g., BoundedOutOfOrdernessWatermarks tracks maxTimestamp per split
            // and emits watermark = maxTimestamp - outOfOrderness on periodic emit
```

### 1.8.4 Periodic Watermark Emission

```java
// Triggered by: timeService.scheduleWithFixedDelay(autoWatermarkInterval)
// Default interval: 200ms (ExecutionConfig.getAutoWatermarkInterval())

// ProgressiveTimestampsAndWatermarks, line 164
periodicEmitHandle = timeService.scheduleWithFixedDelay(
        this::emitImmediateWatermark,
        periodicWatermarkInterval,
        periodicWatermarkInterval);
```

```
Every 200ms:
       │
       ▼
ProgressiveTimestampsAndWatermarks.emitImmediateWatermark()
       │
       ├──► SplitLocalOutputs.emitPeriodicWatermark()
       │      │
       │      │  For each split:
       │      │    SourceOutputWithWatermarks.emitPeriodicWatermark()
       │      │      → watermarkGenerator.onPeriodicEmit(deferredOutput)
       │      │      → records candidate watermark in deferred output
       │      │
       │      └─ watermarkMultiplexer.onPeriodicEmit()
       │           → merges all deferred candidate watermarks
       │           → computes combined = MIN(all split watermarks)
       │           → if combined advanced: emitWatermark(combined)
       │
       └──► StreamingReaderOutput.emitPeriodicWatermark()
              → main watermarkGenerator.onPeriodicEmit()
```

```
Combined Watermark = MIN(all splits)

The multiplexer holds a "candidate" watermark for each split. On periodic emit:
  → merges all candidate watermarks
  → computes combined = MIN(all active splits' watermarks)
  → if combined advanced:
      emitWatermark(combined) to downstream
```

### 1.8.5 Combined Watermark = MIN(all splits)

The `WatermarkOutputMultiplexer` holds a `PartialWatermark` per split. The combined watermark is the **minimum** across all active (non-idle) splits:

- If P0 watermark = 1000, P5 watermark = 950, P10 watermark = 1100 → **combined = 950**
- If P5 becomes idle (no records for a while, `markIdle()` called) → **combined = MIN(1000, 1100) = 1000**
- Idle splits don't hold back the watermark

### 1.8.6 Watermark Alignment (Advanced)

When multiple source subtasks have significantly different watermarks (e.g., one subtask's splits are much faster than another's), watermark alignment can **pause fast splits** to let slow ones catch up:

```java
// SourceOperator implements WatermarkUpdateListener, lines 770-808
@Override
public void updateCurrentSplitWatermark(String splitId, long watermark) {
    WatermarkSampler splitWatermarkSampler = sampledSplitWatermarks.get(splitId);
    splitWatermarkSampler.addLatest(watermark);
    // If this split is too far ahead of the max desired watermark:
    //   → pause the split via eventTimeLogic.pauseOrResumeSplits()
    //   → which pauses the PausableRelativeClock for that split
}
```

This is configured via `WatermarkStrategy.withWatermarkAlignment(groupName, maxAllowedWatermarkDrift)`.

---

## 1.9 assign() vs subscribe() — Why the Connector Uses assign()

### The fundamental difference

```
 subscribe("my-topic")                    assign([TopicPartition("my-topic", 0),
                                                   TopicPartition("my-topic", 1)])
 ┌─────────────────────────────┐           ┌─────────────────────────────┐
 │  Kafka Group Coordinator    │           │  No group coordinator       │
 │  decides which partitions   │           │  Caller decides exactly     │
 │  each consumer gets.        │           │  which partitions to read.  │
 │                             │           │                             │
 │  Rebalance triggered on:    │           │  No rebalance. Ever.        │
 │  - new consumer joins       │           │                             │
 │  - consumer leaves/dies     │           │  Flink enumerator owns all  │
 │  - new partition added      │           │  partition assignment logic. │
 └─────────────────────────────┘           └─────────────────────────────┘
         Kafka controls assignment                Flink controls assignment
```

The Flink connector uses **`assign()`** because:
1. **FLIP-27 source framework** — `KafkaSourceEnumerator` (running in `SourceCoordinator` on the
   JobManager) already manages which `KafkaPartitionSplit` goes to which reader. It tracks
   `assignedSplits` and `unassignedSplits`, handles reader failures, and checkpoints the full
   partition map. Letting Kafka also manage assignment via `subscribe()` would create a conflict.
2. **No rebalance storms** — `subscribe()` triggers a group-wide rebalance whenever a Flink task
   restarts. With `assign()`, a restarted task simply calls `assign()` again with the splits from
   its restored checkpoint — no other consumers are disturbed.
3. **Deterministic partition ownership** — the enumerator assigns partitions via a consistent hash
   (topic-specific round-robin). This is deterministic and reproducible from a checkpoint.

### Exact code path: how the connector assigns partitions

**Step 1 — Enumerator sends splits to readers (JobManager side)**

```
KafkaSourceEnumerator.assignPendingPartitionSplits()
    │
    │  for each pendingReader:
    │      incrementalAssignment.put(readerIndex, splitsForReader)
    │
    context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
    │  moves split: unassignedSplits → assignedSplits
    │
    ▼
SourceCoordinator  ──[RPC]──►  SourceOperator (TaskManager)
                                    │
                                    addSplits(List<KafkaPartitionSplit>)
```

**Step 2 — handleSplitsChanges(): parse offsets then call assign()**

```java
// KafkaPartitionSplitReader.java — handleSplitsChanges()

// 1. Collect the new TopicPartitions from the incoming splits
List<TopicPartition> newPartitionAssignments = new ArrayList<>();
splitsChange.splits().forEach(s -> {
    newPartitionAssignments.add(s.getTopicPartition());
    parseStartingOffsets(s, ...);   // EARLIEST / LATEST / specific offset
    parseStoppingOffsets(s, ...);   // for bounded reads
});

// 2. Merge with existing assignment (never drop already-assigned partitions)
newPartitionAssignments.addAll(consumer.assignment());  // ← existing TPs preserved
consumer.assign(newPartitionAssignments);               // ← THE assign() call

// 3. Seek each new partition to its starting offset
seekToStartingOffsets(
    partitionsStartingFromEarliest,          // consumer.seekToBeginning(tps)
    partitionsStartingFromLatest,            // consumer.seekToEnd(tps)
    partitionsStartingFromSpecifiedOffsets); // consumer.seek(tp, offset)

// 4. Resolve stopping offsets (bounded sources)
acquireAndSetStoppingOffsets(partitionsStoppingAtLatest, partitionsStoppingAtCommitted);
```

**Key invariant**: `assign()` is always called with the **full** list (new + existing). Passing only
new partitions would silently drop the currently-reading ones.

**Partition unassignment** (split finished or empty range):

```java
private void unassignPartitions(Collection<TopicPartition> partitionsToUnassign) {
    Collection<TopicPartition> newAssignment = new HashSet<>(consumer.assignment());
    newAssignment.removeAll(partitionsToUnassign);
    consumer.assign(newAssignment);   // re-assign minus finished partitions
}
```

### Full assignment lifecycle

```
  JobManager (SourceCoordinator)          TaskManager (SplitFetcher I/O thread)
  ─────────────────────────────           ──────────────────────────────────────

  KafkaSourceEnumerator
  │  discoverPartitions()
  │  → new KafkaPartitionSplit(TopicPartition("orders", 2), startOffset=COMMITTED)
  │
  │  assignPendingPartitionSplits()
  │  → context.assignSplits(...)
  │                                  ──RPC──►  KafkaSourceReader.addSplits()
  │                                                 │
  │                                            SplitFetcher enqueues AddSplitsTask
  │                                                 │
  │                                            handleSplitsChanges()
  │                                                 │
  │                                            newAssignment = [orders-2]
  │                                              + existing [orders-0, orders-1]
  │                                                 │
  │                                            consumer.assign([orders-0, orders-1, orders-2])
  │                                            consumer.seek(orders-2, committedOffset)
  │                                                 │
  │  assignedSplits.put(orders-2, split)       consumer.poll() loops...
  │  unassignedSplits.remove(orders-2)
  │
  │  snapshotState() → checkpoint [orders-0, orders-1, orders-2]
```

### What happens when the Kafka broker fails

**Layer 1 — Kafka client internal retry (transparent to Flink)**

The `KafkaConsumer` handles transient failures internally before Flink sees anything:

| Config | Default | Effect |
|--------|---------|--------|
| `retry.backoff.ms` | 100 ms | Wait between retries on retriable errors |
| `retry.backoff.max.ms` | 1 000 ms | Max backoff (exponential) |
| `request.timeout.ms` | 30 000 ms | Total time before request is declared failed |
| `reconnect.backoff.ms` | 50 ms | Backoff before reconnecting to a broker |
| `reconnect.backoff.max.ms` | 1 000 ms | Max reconnect backoff |

For **partition leader failover** (most common HA case):
```
  Broker 1: leader of orders-2  →  Broker 2: new leader of orders-2

  KafkaConsumer detects LEADER_NOT_AVAILABLE / NOT_LEADER_OR_FOLLOWER
  → refreshes cluster metadata → reconnects to Broker 2
  → resumes fetch from same offset  (assign() is unaffected — no rebalance)
```

**Layer 2 — WakeupException (Flink-triggered interrupt, not a broker failure)**

```java
// KafkaPartitionSplitReader.fetch()
try {
    consumerRecords = consumer.poll(Duration.ofMillis(10_000));
} catch (WakeupException | IllegalStateException e) {
    return empty records;   // new splits arriving; fetch thread unblocked safely
}

// retryOnWakeup() handles the race: wakeup stored between two consumer calls
private <V> V retryOnWakeup(Supplier<V> consumerCall, String description) {
    try { return consumerCall.get(); }
    catch (WakeupException we) { return consumerCall.get(); }  // retry once
}
```

**Layer 3 — Unrecoverable exception → Flink task restart**

```
consumer.poll() throws (broker down > request.timeout.ms)
    → not caught by fetch()
    → SplitFetcher stores error, signals queue
    → KafkaSourceReader throws to task thread
    → StreamTask marks FAILED → RestartStrategy
    → restore from checkpoint:
         consumer.assign([checkpointed partitions])
         consumer.seek(tp, checkpointedOffset)
```

---

## 1.10 Kafka Lineage Integration

### What lineage is

Flink's lineage API tracks **which datasets a job reads from and writes to**, and the edges
between them. This graph is built at job submission time (before execution) from the
`Transformation` tree, and is available to external tools via `JobCreatedEvent`.

```
  LineageGraph
  ┌──────────────────────────────────────────────────────────┐
  │                                                          │
  │  sources: [SourceLineageVertex]                          │
  │    └─ datasets: [LineageDataset]                         │
  │         ├─ name:      "orders,payments"  ← topic list   │
  │         ├─ namespace: "kafka://broker:9092"              │
  │         └─ facets:                                       │
  │              ├─ "kafka": KafkaDatasetFacet               │
  │              │     ├─ topicIdentifier (topics/pattern)   │
  │              │     └─ properties (bootstrap.servers, …)  │
  │              └─ "type":  TypeDatasetFacet                │
  │                    └─ TypeInformation (schema)            │
  │                                                          │
  │  sinks:   [LineageVertex]                                │
  │    └─ datasets: [LineageDataset]  (same structure)       │
  │                                                          │
  │  relations: [LineageEdge]                                │
  │    └─ source ──► sink  (one edge per source→sink pair)   │
  └──────────────────────────────────────────────────────────┘
```

### How it is built — code path

**Step 1: Source advertises its datasets via `LineageVertexProvider`**

```java
// KafkaSource.java — implements LineageVertexProvider
@Override
public SourceLineageVertex getLineageVertex() {
    // subscriber must implement KafkaDatasetIdentifierProvider
    Optional<DefaultKafkaDatasetIdentifier> topicsIdentifier =
            ((KafkaDatasetIdentifierProvider) subscriber).getDatasetIdentifier();

    DefaultKafkaDatasetFacet kafkaDatasetFacet =
            new DefaultKafkaDatasetFacet(topicsIdentifier.get(), props);

    String namespace = LineageUtil.namespaceOf(props);
    // namespace = "kafka://broker1:9092"  (first bootstrap server)

    return LineageUtil.sourceLineageVertexOf(
            Collections.singletonList(
                    LineageUtil.datasetOf(
                            namespace,
                            kafkaDatasetFacet,
                            new DefaultTypeDatasetFacet(getProducedType()))));
}
```

Each subscriber implementation provides a `KafkaDatasetIdentifier`:

```java
// TopicListSubscriber.getDatasetIdentifier()
return Optional.of(DefaultKafkaDatasetIdentifier.ofTopics(topics));
// → name = "orders,payments"   (comma-joined topic list)

// TopicPatternSubscriber.getDatasetIdentifier()
return Optional.of(DefaultKafkaDatasetIdentifier.ofPattern(topicPattern));
// → name = "order.*"           (regex pattern string)

// PartitionSetSubscriber.getDatasetIdentifier()
return Optional.of(DefaultKafkaDatasetIdentifier.ofTopics(
        partitions.stream().map(TopicPartition::topic).distinct().collect(...)));
// → name = "orders"            (distinct topics from explicit partition set)
```

**Step 2: `SourceTransformation` captures the vertex at graph build time**

```java
// SourceTransformation.java
if (source instanceof LineageVertexProvider) {
    setLineageVertex(((LineageVertexProvider) source).getLineageVertex());
}
```

Sink transformations do the same:
```java
// DataStreamSink.java
if (sink instanceof LineageVertexProvider) {
    transformation.setLineageVertex(((LineageVertexProvider) sink).getLineageVertex());
}
```

**Step 3: `StreamGraphGenerator` assembles the full `LineageGraph`**

```java
// StreamGraphGenerator.java (called during env.execute())
LineageGraph lineageGraph = LineageGraphUtils.convertToLineageGraph(transformations);
streamGraph.setLineageGraph(lineageGraph);
```

`LineageGraphUtils.convertToLineageGraph()` walks all transformations:
- For each sink transformation that has a lineage vertex, it walks transitive predecessors
  to find source transformations with lineage vertices.
- Each source→sink pair becomes a `LineageEdge`.
- Sources with no matching sink are added as standalone `SourceLineageVertex` nodes.

```
  SourceTransformation("orders")     SourceTransformation("payments")
         │                                    │
         └───────────────┬───────────────────┘
                         │  (transitive predecessors)
                         ▼
                  SinkTransformation("results-topic")
                         │
         LineageGraphUtils creates:
           edge(orders-source → results-sink)
           edge(payments-source → results-sink)
```

**Step 4: `LineageGraph` is published via `JobCreatedEvent`**

```java
// DefaultJobCreatedEvent carries the lineage graph
public interface JobCreatedEvent extends JobStatusChangedEvent {
    JobID jobId();
    LineageGraph lineageGraph();
}
```

External integrations register a `JobStatusChangedListener` to receive this event when a
Flink job is submitted, before execution starts.

### Dataset identity: name + namespace

```
  name      = KafkaDatasetIdentifier.toLineageName()
            = "orders,payments"          // topic list
            = "order.*"                  // pattern
  namespace = LineageUtil.namespaceOf(properties)
            = "kafka://" + first bootstrap server
            = "kafka://broker1:9092"
```

Together `(namespace, name)` uniquely identifies a Kafka dataset across the lineage graph,
matching the OpenLineage convention used by tools like Apache Atlas, DataHub, and Marquez.

### Facets: Kafka-specific metadata

A `LineageDataset` carries **facets** — typed metadata blobs. The Kafka connector provides two:

| Facet key | Interface | Contains |
|-----------|-----------|---------|
| `"kafka"` | `KafkaDatasetFacet` | `topicIdentifier` (topics/pattern), `properties` (producer/consumer config) |
| `"type"` | `TypeDatasetFacet` | `TypeInformation` — Flink's schema (field names + types) |

```java
// DefaultKafkaDatasetFacet
public class DefaultKafkaDatasetFacet implements KafkaDatasetFacet {
    public static final String KAFKA_FACET_NAME = "kafka";   // facet map key
    private Properties properties;                            // bootstrap.servers etc.
    private final KafkaDatasetIdentifier topicIdentifier;    // topics or pattern

    @Override public String name() { return KAFKA_FACET_NAME; }
}
```

External tools access facets via `dataset.facets().get("kafka")` and cast to `KafkaDatasetFacet`.

### How to implement lineage in a custom serialization schema

If your `KafkaRecordSerializationSchema` knows the target topic at construction time, implement
`KafkaDatasetFacetProvider` (for sink) or `KafkaDatasetIdentifierProvider` (for source subscriber)
to contribute lineage:

```java
// Custom sink serializer contributing lineage
public class MySerializer
        implements KafkaRecordSerializationSchema<MyEvent>,
                   KafkaDatasetFacetProvider {   // ← opt-in interface

    private final String topic;

    @Override
    public Optional<KafkaDatasetFacet> getKafkaDatasetFacet() {
        KafkaDatasetIdentifier id =
                DefaultKafkaDatasetIdentifier.ofTopics(List.of(topic));
        return Optional.of(new DefaultKafkaDatasetFacet(id));
        // KafkaSink.getLineageVertex() will call this and attach the facet
    }
}
```

`KafkaSink.getLineageVertex()` checks `recordSerializer instanceof KafkaDatasetFacetProvider`
and calls `getKafkaDatasetFacet()`. If not implemented, lineage for that sink is omitted.

### External integrations

The `LineageGraph` produced at job submission is the integration point for data catalog and
governance tools. The typical integration pattern is:

```
  Flink Job submitted
       │
       ▼
  StreamGraphGenerator.generate()
       │  LineageGraphUtils.convertToLineageGraph(transformations)
       ▼
  StreamGraph (holds LineageGraph)
       │
       ▼
  JobCreatedEvent fired  →  JobStatusChangedListener implementations
       │
       ├─ Apache Atlas listener
       │     POST /api/atlas/v2/entity/bulk
       │     { "entities": [ { "typeName": "kafka_topic",
       │                       "attributes": { "name": "orders",
       │                                       "qualifiedName": "kafka://broker:9092/orders",
       │                                       "bootstrapServers": "broker:9092" } } ] }
       │
       ├─ DataHub listener  (via OpenLineage facets)
       │     POST /api/v2/lineage
       │     { "upstreamLineage": { "upstreams": [
       │         { "dataset": { "platform": "kafka",
       │                        "name": "orders",
       │                        "env": "PROD" },
       │           "type": "TRANSFORMED" } ] } }
       │
       └─ Marquez / OpenLineage listener
             POST /api/v1/lineage
             { "run": { "runId": "<flink-job-id>" },
               "job": { "name": "my-flink-job" },
               "inputs":  [ { "namespace": "kafka://broker:9092",
                               "name": "orders",
                               "facets": { "kafka": { "topics": ["orders"],
                                                      "bootstrapServers": "broker:9092" },
                                           "schema": { "fields": [...] } } } ],
               "outputs": [ { "namespace": "kafka://broker:9092",
                               "name": "enriched-orders" } ] }
```

### What lineage does NOT capture

- Runtime partition assignment (which reader owns which partition) — that is FLIP-27 state, not lineage.
- Actual offsets consumed — those are in checkpoint state and Kafka `__consumer_offsets`.
- Field-level transformations (column lineage) — Flink's lineage is dataset-level only.
- Dynamic topic routing (topics chosen at record time) — only topics known at construction time
  are captured. Records routed dynamically via `TopicSelector` appear in lineage only if the
  serializer implements `KafkaDatasetFacetProvider` and returns the correct facet.

---

# Part 2: Kafka Sink Architecture

## 2.0 Delivery Guarantee Modes — NONE, AT_LEAST_ONCE, EXACTLY_ONCE

### 2.0.1 Three Modes at a Glance

`KafkaSink` supports three delivery guarantees controlled by `DeliveryGuarantee`:

```
KafkaSinkBuilder
    .setDeliveryGuarantee(DeliveryGuarantee.NONE)          // at-most-once
    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // default, no transactions
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)  // Kafka transactions + 2PC
```

The mode drives two decisions in `KafkaSink.java`:

```java
// KafkaSink.restoreWriter() — which writer class to create
if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
    writer = new ExactlyOnceKafkaWriter<>(...);   // transactions, ProducerPool
} else {
    writer = new KafkaWriter<>(...);              // no transactions, single producer
}

// KafkaSink.createCommitter() — which committer to create
if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
    return new KafkaCommitter(...);   // calls producer.commitTransaction()
}
return new NoopCommitter();           // does nothing — commit is already done by producer.flush()
```

### 2.0.2 Class Relationships

```
                KafkaSink (DeliveryGuarantee field)
                     │
         ┌───────────┴───────────┐
         │                       │
   NONE / AT_LEAST_ONCE    EXACTLY_ONCE
         │                       │
    KafkaWriter            ExactlyOnceKafkaWriter
    (base class)             (extends KafkaWriter)
         │
    NoopCommitter          KafkaCommitter
    (does nothing)         (commitTransaction)
```

### 2.0.3 KafkaWriter: The Non-Transactional Writer (NONE and AT_LEAST_ONCE)

Both non-transactional modes share the same `KafkaWriter` class. The difference is
entirely in `flush()`:

```java
// KafkaWriter.flush()
@Override
public void flush(boolean endOfInput) throws IOException, InterruptedException {
    if (deliveryGuarantee != DeliveryGuarantee.NONE || endOfInput) {
        currentProducer.flush();   // block until Kafka acks all in-flight records
    }
    checkAsyncException();         // surface any async send failure
}
```

| Condition | NONE | AT_LEAST_ONCE |
|-----------|------|---------------|
| `flush()` on checkpoint | No — skipped | Yes — blocks until all records acked |
| `flush()` on `endOfInput` | Yes | Yes |
| `prepareCommit()` returns | empty list | empty list |
| `snapshotState()` returns | empty list | empty list |
| Committer | `NoopCommitter` | `NoopCommitter` |

**NONE**: Records are fire-and-forget. Kafka's internal producer buffer may hold unsent records when
the job fails. Those records are lost → **at-most-once**.

**AT_LEAST_ONCE**: On every checkpoint, Flink calls `flush()` which blocks until all in-flight
`producer.send()` callbacks have been ack'd by Kafka brokers. The checkpoint only completes after
all records up to that point are durably written to Kafka. On restart from checkpoint, the source
replays from the last checkpointed offset, and the sink re-sends any records that were in-flight at
the time of failure → **at-most-once delivery to Kafka is impossible; duplicates are possible** →
**at-least-once**.

### 2.0.4 write() and the Async Callback

```java
// KafkaWriter.write()
@Override
public void write(@Nullable IN element, Context context) throws IOException {
    checkAsyncException();                          // ① fail fast on prior error
    final ProducerRecord<byte[], byte[]> record =
            recordSerializer.serialize(element, kafkaSinkContext, context.timestamp());
    if (record != null) {
        currentProducer.send(record, deliveryCallback);   // ② async send
        numRecordsOutCounter.inc();
    }
}
```

#### What happens when `send()` throws synchronously?

`KafkaProducer.send()` is normally asynchronous — it appends to an internal buffer and returns
immediately. However it **can throw synchronously** in several cases, all happening inside
`KafkaProducer.doSend()` before the record reaches the buffer:

| Exception thrown by `send()` | Cause |
|---|---|
| `TimeoutException` | `buffer.memory` exhausted and `max.block.ms` elapsed without space being freed (see buffer-full section below) |
| `InterruptException` | Caller thread interrupted while blocked waiting for buffer memory |
| `SerializationException` | Key or value serializer threw a `ClassCastException` |
| `RecordTooLargeException` | Serialized record exceeds `max.request.size` or is larger than `buffer.memory` |
| `KafkaException` (ApiException subclass) | Producer closed, or topic authorization failure caught before append |
| Any other `KafkaException` | Unchecked error inside `doSend()` |

For `ApiException` subclasses (e.g., authorization failures detected pre-append), `doSend()`
**additionally** calls `callback.onCompletion(nullMetadata, e)` before throwing, so the callback
fires AND the exception propagates:

```java
// KafkaProducer.doSend() — KafkaProducer.java:1074
} catch (ApiException e) {
    if (callback != null) {
        callback.onCompletion(nullMetadata, e);    // ← callback fired synchronously
    }
    return new FutureFailure(e);                   // ← future also carries the error
```

For all other synchronous exceptions, the callback is **not** called — only the exception propagates.

**In `KafkaWriter.write()`, synchronous exceptions from `send()` are not caught.** They propagate
directly as unchecked exceptions out of `write()`, bypassing the async error path entirely. Flink's
task runner catches them and triggers a task restart/failure via its normal exception handling.

#### What happens when the Kafka callback fires with an error (async path)?

Broker-side errors (retries exhausted, topic deleted, auth revoked mid-flight, etc.) reach
`KafkaWriter` through `WriterCallback.onCompletion()`, which runs on the **Kafka producer I/O
thread** — not the Flink task thread:

```java
// WriterCallback.onCompletion() — KafkaWriter.java:310
@Override
public void onCompletion(RecordMetadata metadata, Exception exception) {
    if (exception != null) {
        if (closed) {
            LOG.debug("Completed exceptionally, but shutdown was already initiated", exception);
            return;
        }
        if (asyncProducerException == null) {        // store first exception only
            asyncProducerException = decorateException(metadata, exception, producer);
            mailboxExecutor.execute(
                () -> checkAsyncException(),         // schedule re-throw on task thread
                "Update error metric");
        }
    } else {
        numRecordsSent++;
    }
}
```

The exception is stored in `volatile asyncProducerException`. On the next call to `write()` or
`flush()` on the Flink task thread, `checkAsyncException()` rethrows it as `IOException`:

```java
// KafkaWriter.checkAsyncException() — KafkaWriter.java:286
private void checkAsyncException() throws IOException {
    Exception e = asyncProducerException;
    if (e != null) {
        asyncProducerException = null;
        numRecordsOutErrorsCounter.inc();
        throw new IOException("One or more Kafka Producer send requests have encountered exception", e);
    }
}
```

This converts the async I/O-thread error into a synchronous task-thread failure that Flink's
restart strategy handles normally.

**Note**: `flush()` also calls `checkAsyncException()` after `producer.flush()` returns. Since
`flush()` blocks until all in-flight sends complete (all callbacks have fired), the async exception
is guaranteed to be visible by the time `flush()` checks — making checkpoint barriers reliable.

#### What happens when the producer's batch buffer is full?

`KafkaProducer.send()` calls `RecordAccumulator.append()`, which calls `BufferPool.allocate()` to
get memory for a new batch when the current batch is full. The buffer pool behavior:

```
buffer.memory (default 32 MB) = pool of ByteBuffers for all in-flight batches
```

**If buffer memory is available**: `allocate()` returns immediately and `send()` returns in
microseconds.

**If buffer memory is exhausted** (`BufferPool.allocate()` — `BufferPool.java:111`):

```java
public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
    // ...
    } else {
        // we are out of memory and will have to block
        long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
        this.waiters.addLast(moreMemory);           // ← register as waiter
        while (accumulated < size) {
            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
            // ...
            if (waitingTimeElapsed) {
                throw new BufferExhaustedException(  // ← throws after max.block.ms
                    "Failed to allocate " + size + " bytes within the configured max blocking time "
                    + maxTimeToBlockMs + " ms. ...");
            }
            // otherwise: memory was freed by the Sender thread, loop and accumulate
        }
    }
}
```

**The buffer-full behavior is: `send()` BLOCKS the calling thread** (the Flink task thread) for up
to `max.block.ms` (default 60,000 ms = 60 seconds), waiting for the Sender background thread to
drain batches and free memory. **The callback is NOT called** — the record has not been appended
yet, so there is nothing to call back on.

If memory is freed within `max.block.ms`: `send()` unblocks and returns normally. No exception,
no callback invocation.

If `max.block.ms` elapses without memory being freed: `BufferExhaustedException` is thrown
synchronously from `send()` — this propagates out of `KafkaWriter.write()` as an unchecked
exception directly to Flink's task runner (not via the async callback path).

```
send() call
  │
  └─► RecordAccumulator.append()
        └─► BufferPool.allocate(size, remainingWaitMs)
              │
              ├─ memory available → return immediately → callback called later (async)
              │
              └─ memory exhausted → BLOCK on Condition.await(remainingTimeToBlockNs)
                    │
                    ├─ memory freed within max.block.ms
                    │     → send() returns → callback called later (async) when ack arrives
                    │
                    └─ max.block.ms elapsed
                          → throw BufferExhaustedException  ← synchronous, no callback
                            propagates out of write() → Flink task failure
```

`remainingWaitMs` passed to `allocate()` is `max.block.ms` minus the time already spent waiting
for topic metadata (from `waitOnMetadata()`), so the total blocking in `send()` across metadata
wait and buffer wait is bounded by a single `max.block.ms`.

**Summary — two completely separate error paths in `KafkaWriter.write()`**:

| Path | When | Exception delivery | Flink impact |
|---|---|---|---|
| Synchronous throw from `send()` | Serialization error, record too large, buffer full after `max.block.ms`, producer closed | Exception propagates directly from `write()` | Task fails immediately |
| Async callback with exception | Broker rejected after retries exhausted, delivery timeout, auth revoked mid-flight | Stored in `asyncProducerException`, rethrown on next `write()` or `flush()` | Task fails on next write or at checkpoint |

### 2.0.5 AT_LEAST_ONCE: Checkpoint Cycle

```
  Normal record flow (between checkpoints):
  ──────────────────────────────────────────
  Flink task thread                     Kafka broker
      │                                      │
      │  write(r1) → producer.send(r1) ─────►│ ack (async callback)
      │  write(r2) → producer.send(r2) ─────►│ ack (async callback)
      │  write(r3) → producer.send(r3) ─────►│ ack (async callback)
      │  ...                                  │
      │  (records visible to consumers immediately after ack)
      │
  Checkpoint N triggered:
  ───────────────────────
      │  flush(endOfInput=false)
      │      producer.flush()  ◄── blocks here
      │      (waits for all in-flight send callbacks to complete)
      │      all records acked ──────────────►│
      │  flush() returns
      │  prepareCommit() → [] (empty, nothing to commit)
      │  checkpoint barrier passes downstream
      │  checkpoint N complete
      │
  Failure after checkpoint N, before checkpoint N+1:
  ───────────────────────────────────────────────────
      │  (job restarts, source replays from checkpoint N offset)
      │  records r1..r3 re-sent → duplicates on Kafka topic
      │  (consumer may see r1..r3 twice)
```

### 2.0.6 Side-by-Side Comparison

```
 ┌──────────────────┬─────────────────────────────┬─────────────────────────────┬──────────────────────────────┐
 │                  │ NONE                        │ AT_LEAST_ONCE               │ EXACTLY_ONCE                 │
 ├──────────────────┼─────────────────────────────┼─────────────────────────────┼──────────────────────────────┤
 │ Writer class     │ KafkaWriter                 │ KafkaWriter                 │ ExactlyOnceKafkaWriter       │
 │ Committer        │ NoopCommitter               │ NoopCommitter               │ KafkaCommitter               │
 │ Transactions     │ No                          │ No                          │ Yes (beginTransaction /      │
 │                  │                             │                             │  commitTransaction)          │
 │ flush() on ckpt  │ No                          │ Yes (blocks until acked)    │ Implicit in prepareCommit()  │
 │ Records visible  │ Immediately after send      │ Immediately after ack       │ Only after commit (post-ckpt)│
 │ On failure       │ Records in buffer lost      │ Records replayed (dupes ok) │ Transaction aborted, no dupe │
 │ Latency impact   │ None                        │ Checkpoint delay            │ Full checkpoint delay        │
 │ Kafka config req │ None                        │ None                        │ transactional.id,            │
 │                  │                             │                             │ isolation.level=read_committed│
 │ State size       │ No writer state             │ No writer state             │ KafkaWriterState             │
 │                  │                             │                             │ (transactional IDs + offsets)│
 └──────────────────┴─────────────────────────────┴─────────────────────────────┴──────────────────────────────┘
```

**Key insight**: AT_LEAST_ONCE and EXACTLY_ONCE look similar from a checkpoint-coupling
standpoint (both block the checkpoint until Kafka has the data), but they differ fundamentally
on **what "the data is in Kafka" means**:
- AT_LEAST_ONCE: records are in Kafka and **immediately visible** to consumers with any
  `isolation.level`. A restart re-sends them → consumers see duplicates.
- EXACTLY_ONCE: records are in Kafka inside an **open transaction** — invisible to
  `read_committed` consumers until `commitTransaction()` fires after the checkpoint succeeds.
  A restart aborts the transaction → no duplicates.

---

## 2.1 High-Level Component Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        TaskManager (per subtask)                    │
│                                                                     │
│  ExactlyOnceKafkaWriter                                             │
│    ├─ FlinkKafkaInternalProducer (wrapped KafkaProducer)            │
│    │     └─ TransactionState: NOT_IN_TXN → IN_TXN → DATA → PRECOMMITTED │
│    ├─ ProducerPool (reuses producers across checkpoints)            │
│    ├─ TransactionNamingStrategy (INCREMENTING or POOLING)           │
│    └─ ReadableBackchannel ← receives commit confirmations           │
│                                                                     │
│  KafkaCommitter (colocated with writer)                             │
│    ├─ Calls producer.commitTransaction()                            │
│    └─ WritableBackchannel → sends TransactionFinished to writer     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Entry point**: `KafkaSink.java` implements `TwoPhaseCommittingStatefulSink<IN, KafkaWriterState, KafkaCommittable>`.

The sink uses Flink's `TwoPhaseCommittingStatefulSink` framework:
- **Writer** produces records within Kafka transactions
- **Committer** finalizes transactions after checkpoint success
- Writer and committer are **colocated** on the same TaskManager (via `addPostCommitTopology`)

## 2.2 FlinkKafkaInternalProducer: The Transaction State Machine

```java
// sink/internal/FlinkKafkaInternalProducer.java, lines 403-408
enum TransactionState {
    NOT_IN_TRANSACTION,     // No active transaction
    IN_TRANSACTION,         // beginTransaction() called, no data yet
    DATA_IN_TRANSACTION,    // At least one record sent
    PRECOMMITTED,           // prepareCommit() called, waiting for commit
}
```

```
  NOT_IN_TRANSACTION ──beginTransaction()──► IN_TRANSACTION
                                                  │
                                              send(record)
                                                  │
                                                  ▼
                                          DATA_IN_TRANSACTION
                                                  │
                                          precommitTransaction()
                                                  │
                                                  ▼
                                            PRECOMMITTED
                                                  │
                              ┌────────────────────┴────────────────────┐
                       commitTransaction()                       abortTransaction()
                              │                                         │
                              ▼                                         ▼
                      NOT_IN_TRANSACTION                        NOT_IN_TRANSACTION
                      (data visible)                            (data discarded)
```

### Key Methods

```java
// sink/internal/FlinkKafkaInternalProducer.java

// Track when data is written to the transaction
@Override
public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    if (isInTransaction()) {
        transactionState = TransactionState.DATA_IN_TRANSACTION;  // line 82
    }
    return super.send(record, callback);
}

// Flush also registers new partitions added during writes
@Override
public void flush() {
    super.flush();
    if (isInTransaction()) {
        flushNewPartitions();  // line 91 — adds newly discovered partitions to the transaction
    }
}

// Begin: start a new Kafka transaction
@Override
public void beginTransaction() throws ProducerFencedException {
    super.beginTransaction();
    transactionState = TransactionState.IN_TRANSACTION;  // line 99
}

// Flink-specific: mark as pre-committed (no Kafka protocol operation)
public void precommitTransaction() {
    checkState(hasRecordsInTransaction(), "Transaction was not started");
    transactionState = TransactionState.PRECOMMITTED;  // line 132
}

// Final commit: data becomes visible to consumers with isolation.level=read_committed
@Override
public void commitTransaction() throws ProducerFencedException {
    checkState(isInTransaction(), "Transaction was not started");
    transactionState = TransactionState.NOT_IN_TRANSACTION;  // line 114
    super.commitTransaction();
}

// Query helpers
public boolean hasRecordsInTransaction() {
    return transactionState == TransactionState.DATA_IN_TRANSACTION;  // line 123
}
```

### Resume Transaction (for the Committer)

The committer needs to commit a transaction that was started by the writer. It uses reflection to set the `producerId` and `epoch` on a new producer:

```java
// sink/internal/FlinkKafkaInternalProducer.java, lines 300-340
public void resumeTransaction(long producerId, short epoch) {
    checkState(!isInTransaction(), "Already in transaction %s", transactionalId);
    checkState(producerId >= 0 && epoch >= 0, "Incorrect values");

    Object transactionManager = getTransactionManager();
    synchronized (transactionManager) {
        Object txnPartitionMap = getField(transactionManager, "txnPartitionMap");

        // Walk through Kafka's internal state machine via reflection:
        transitionTransactionManagerStateTo(transactionManager, "INITIALIZING");
        invoke(txnPartitionMap, "reset");

        // Set the producerId and epoch to match the writer's transaction
        setField(transactionManager, PRODUCER_ID_AND_EPOCH_FIELD_NAME,
                createProducerIdAndEpoch(producerId, epoch));

        transitionTransactionManagerStateTo(transactionManager, "READY");
        transitionTransactionManagerStateTo(transactionManager, "IN_TRANSACTION");

        // Ensure EndTxnRequest will be sent on commit
        // (Kafka only sends it if transactionStarted = true)
        setField(transactionManager, "transactionStarted", true);
    }
    this.transactionState = TransactionState.PRECOMMITTED;
}
```

## 2.3 KafkaCommittable: What Gets Passed to the Committer

```java
// sink/KafkaCommittable.java
public class KafkaCommittable {
    private final long producerId;         // Kafka producer ID (part of transaction identity)
    private final short epoch;             // Producer epoch (fencing mechanism)
    private final String transactionalId;  // e.g., "my-app-0-42"
    @Nullable private FlinkKafkaInternalProducer<?, ?> producer;  // Optional: if writer & committer are chained

    public static <K, V> KafkaCommittable of(FlinkKafkaInternalProducer<K, V> producer) {
        return new KafkaCommittable(
                producer.getProducerId(),
                producer.getEpoch(),
                producer.getTransactionalId(),
                producer);  // Pass producer reference for in-JVM commit
    }
}
```

## 2.4 The Full 2-Phase Commit Protocol

### Phase 0: Initialization (on startup or recovery)

```java
// sink/ExactlyOnceKafkaWriter.java, lines 191-205
@Override
public void initialize() {
    try {
        // STEP 1: Abort any lingering transactions from previous execution
        abortLingeringTransactions(recoveredStates, restoredCheckpointId + 1);

        // STEP 2: Start a new transaction for the first checkpoint
        this.currentProducer = startTransaction(restoredCheckpointId + 1);
    } catch (Throwable t) {
        try { close(); } catch (Exception e) { t.addSuppressed(e); }
        throw t;
    }
}

// sink/ExactlyOnceKafkaWriter.java, lines 207-218
private FlinkKafkaInternalProducer<byte[], byte[]> startTransaction(long checkpointId) {
    namingContext.setNextCheckpointId(checkpointId);
    namingContext.setOngoingTransactions(
            producerPool.getOngoingTransactions().stream()
                    .map(CheckpointTransaction::getTransactionalId)
                    .collect(Collectors.toSet()));

    // Get or create a producer with the appropriate transactional ID
    FlinkKafkaInternalProducer<byte[], byte[]> producer =
            transactionNamingStrategy.getTransactionalProducer(namingContext);

    namingContext.setLastCheckpointId(checkpointId);
    producer.beginTransaction();  // ← Kafka beginTransaction()
    return producer;
}
```

### Phase 1: Normal Writing

```java
// sink/KafkaWriter.java (base class), lines 170-178
@Override
public void write(@Nullable IN element, Context context) throws IOException {
    checkAsyncException();
    final ProducerRecord<byte[], byte[]> record =
            recordSerializer.serialize(element, kafkaSinkContext, context.timestamp());
    if (record != null) {
        currentProducer.send(record, deliveryCallback);
        // send() sets transactionState → DATA_IN_TRANSACTION
        numRecordsOutCounter.inc();
    }
}
```

### Phase 2: Pre-Commit (checkpoint barrier arrives)

When the checkpoint barrier reaches the sink operator, Flink calls `prepareCommit()`:

```java
// sink/ExactlyOnceKafkaWriter.java, lines 221-233
@Override
public Collection<KafkaCommittable> prepareCommit() {
    if (currentProducer.hasRecordsInTransaction()) {
        // Create committable capturing (producerId, epoch, transactionalId)
        KafkaCommittable committable = KafkaCommittable.of(currentProducer);
        LOG.debug("Prepare {}.", committable);

        // Mark the producer as pre-committed
        // Data is in Kafka's transaction log but NOT visible to consumers yet
        currentProducer.precommitTransaction();

        return Collections.singletonList(committable);
    }

    // No data in this checkpoint interval → recycle the producer
    producerPool.recycle(currentProducer);
    return Collections.emptyList();
}
```

**At this point:**
- Records are written to Kafka partitions but **invisible** to consumers with `isolation.level=read_committed`
- The `KafkaCommittable(producerId, epoch, transactionalId)` is serialized into checkpoint state
- The producer is in `PRECOMMITTED` state

### Phase 3: Snapshot State + Start Next Transaction

Immediately after `prepareCommit()`, Flink calls `snapshotState()`:

```java
// sink/ExactlyOnceKafkaWriter.java, lines 236-248
@Override
public List<KafkaWriterState> snapshotState(long checkpointId) throws IOException {
    // 1. Poll backchannel for commit confirmations from PREVIOUS checkpoints
    TransactionFinished finishedTransaction;
    while ((finishedTransaction = backchannel.poll()) != null) {
        producerPool.recycleByTransactionId(
                finishedTransaction.getTransactionId(),
                finishedTransaction.isSuccess());
    }

    // 2. Persist the ongoing (precommitted) transactions into state
    //    These will NOT be aborted on restart
    Collection<CheckpointTransaction> ongoingTransactions =
            producerPool.getOngoingTransactions();

    // 3. Start a NEW transaction for the NEXT checkpoint
    //    (the current one is frozen in PRECOMMITTED state)
    currentProducer = startTransaction(checkpointId + 1);

    // 4. Return state snapshots for each owned subtask
    return createSnapshots(ongoingTransactions);
}
```

### Phase 4: Commit (after checkpoint completes successfully)

The `KafkaCommitter` receives the `KafkaCommittable` and performs the final commit:

```java
// sink/internal/KafkaCommitter.java, lines 93-151
@Override
public void commit(Collection<CommitRequest<KafkaCommittable>> requests)
        throws IOException, InterruptedException {
    for (CommitRequest<KafkaCommittable> request : requests) {
        final KafkaCommittable committable = request.getCommittable();
        final String transactionalId = committable.getTransactionalId();

        Optional<FlinkKafkaInternalProducer<?, ?>> writerProducer = committable.getProducer();
        FlinkKafkaInternalProducer<?, ?> producer = null;
        try {
            // Use the writer's producer if available (in-JVM), or create one to resume
            producer = writerProducer.orElseGet(() -> getProducer(committable));

            // ═══════════════════════════════════════════════════════════
            // FINAL COMMIT: data becomes visible to Kafka consumers!
            // ═══════════════════════════════════════════════════════════
            producer.commitTransaction();

            // Signal the writer that this transaction is done
            backchannel.send(TransactionFinished.successful(committable.getTransactionalId()));

        } catch (RetriableException e) {
            // Kafka retriable error → try again later
            request.retryLater();
        } catch (ProducerFencedException e) {
            // Another producer with same transactionalId has initialized → data loss risk
            handleFailedTransaction(producer);
            request.signalFailedWithKnownReason(e);
        } catch (InvalidTxnStateException e) {
            // Transaction was already aborted (e.g., by Kafka timeout)
            handleFailedTransaction(producer);
            request.signalFailedWithKnownReason(e);
        } catch (UnknownProducerIdException e) {
            // Kafka broker bug (KAFKA-9310) — upgrade to Kafka 2.5+
            handleFailedTransaction(producer);
            request.signalFailedWithKnownReason(e);
        } catch (Exception e) {
            closeCommitterProducer(producer);
            request.signalFailedWithUnknownReason(e);  // Triggers failover
        }
    }
}
```

### The Committer's getProducer: Resuming a Transaction

When the committer is not chained with the writer (or after recovery), it creates a new producer and resumes the transaction:

```java
// sink/internal/KafkaCommitter.java, lines 215-224
private FlinkKafkaInternalProducer<?, ?> getProducer(KafkaCommittable committable) {
    if (committingProducer == null) {
        // First commit: create a new producer with the transactionalId
        committingProducer = producerFactory.apply(
                kafkaProducerConfig, committable.getTransactionalId());
    } else {
        // Reuse: switch to the committable's transactionalId
        committingProducer.setTransactionId(committable.getTransactionalId());
    }
    // Resume the transaction using the writer's producerId and epoch
    committingProducer.resumeTransaction(committable.getProducerId(), committable.getEpoch());
    return committingProducer;
}
```

## 2.5 The Backchannel: Writer ↔ Committer Communication

Since writer and committer are colocated on the same TaskManager, they communicate through an in-memory channel:

```java
// sink/internal/Backchannel.java
public interface WritableBackchannel<T> extends Closeable {
    void send(T message);
}

public interface ReadableBackchannel<T> extends Closeable {
    @Nullable T poll();
}
```

The implementation uses `ConcurrentLinkedDeque` for thread safety. The `BackchannelFactory` ensures a single channel per `(subtaskId, attemptNumber, transactionalIdPrefix)` tuple.

**Flow:**
1. **Committer** → `backchannel.send(TransactionFinished.successful(txnId))` after `commitTransaction()`
2. **Writer** → `backchannel.poll()` during `snapshotState()` to learn which producers can be recycled

This avoids creating new Kafka producers for every checkpoint (expensive: each producer opens connections, allocates buffers, and registers with the transaction coordinator).

## 2.6 Transaction Naming Strategies

```java
// sink/TransactionNamingStrategy.java
public enum TransactionNamingStrategy {
    DEFAULT,   // Uses INCREMENTING internally
    POOLING,   // Reuses transaction IDs from a fixed-size pool
}
```

### INCREMENTING (Default)

```
Transaction ID format: {prefix}-{subtaskId}-{checkpointId}
Example: "my-sink-0-1", "my-sink-0-2", "my-sink-0-3", ...
```

- New unique ID for every checkpoint
- Simple but creates many transaction IDs on the Kafka broker (metadata lives 7 days)
- Uses **PROBING** abort strategy on recovery: iterates `(prefix, subtaskId, checkpointId++)` until `initTransactions()` returns epoch=0 (no existing transaction)

### POOLING (Resource-friendly)

```
Transaction ID format: {prefix}-{subtaskId}-{poolOffset}
Example: "my-sink-0-0", "my-sink-0-1", "my-sink-0-2", "my-sink-0-0" (wraps around)
```

- Reuses IDs from a fixed pool, cycling through them
- Requires Kafka 3.0+ and topic read permissions
- Uses **LISTING** abort strategy: queries broker's `listTransactions()` API to find open transactions, then aborts them
- Better for long-running jobs with frequent checkpoints

## 2.7 Recovery: Aborting Lingering Transactions

On restart, `ExactlyOnceKafkaWriter.initialize()` calls `abortLingeringTransactions()`:

```java
// sink/ExactlyOnceKafkaWriter.java, lines 304-331
private void abortLingeringTransactions(
        Collection<KafkaWriterState> recoveredStates, long startCheckpointId) {
    List<String> prefixesToAbort = new ArrayList<>();
    prefixesToAbort.add(transactionalIdPrefix);

    // Handle prefix changes between executions
    final Optional<KafkaWriterState> lastStateOpt = recoveredStates.stream().findFirst();
    if (lastStateOpt.isPresent()) {
        KafkaWriterState lastState = lastStateOpt.get();
        if (!lastState.getTransactionalIdPrefix().equals(transactionalIdPrefix)) {
            prefixesToAbort.add(lastState.getTransactionalIdPrefix());
        }
    }

    TransactionAbortStrategyContextImpl context =
            getTransactionAbortStrategyContext(startCheckpointId, prefixesToAbort);
    transactionAbortStrategy.abortTransactions(context);
}
```

**PROBING strategy** (for INCREMENTING):
```
For each subtask in [0, parallelism):
    For checkpoint from startCheckpointId:
        txnId = prefix + "-" + subtask + "-" + checkpoint
        epoch = initTransactions(txnId)  // Fences old producer
        if epoch == 0: break             // No existing transaction → stop probing
```

**LISTING strategy** (for POOLING):
```
openTxns = admin.listTransactions()          // Query broker
Filter by: known prefix + owned subtask IDs
Skip: transactions in precommitted list (will be committed by committer)
Abort all remaining open transactions
```

## 2.8 Exactly-Once Guarantees: Why It Works

The 2PC protocol provides exactly-once because of these interlocking mechanisms:

### 1. Kafka Transactions = Atomicity

Records within a transaction are either **all visible** or **none visible** to consumers with `isolation.level=read_committed`. There's no partial visibility.

### 2. ProducerId + Epoch = Fencing

```java
// When initTransactions() is called with the same transactionalId:
// - The broker increments the epoch
// - Any producer with an older epoch for that transactionalId is FENCED
// - Fenced producers cannot commit or send records
```

This prevents zombie producers from committing stale data after a failover.

### 3. Checkpoint Coupling = Commit Safety

```
Timeline:
─────────────────────────────────────────────────────────────────
  write records    │ prepareCommit()  │  checkpoint  │ commit()
  into txn         │ (precommit)      │  persisted   │ (final)
─────────────────────────────────────────────────────────────────
                   ▲                  ▲              ▲
              barrier arrives    state saved    checkpoint complete
```

- `commitTransaction()` only runs **after** the checkpoint succeeds
- If the job fails before checkpoint completion → transaction is never committed
- On recovery → lingering transactions are aborted

### 4. Consumer Isolation = Read-Committed

Downstream Kafka consumers with `isolation.level=read_committed` only see records from **committed** transactions. Precommitted (but not yet committed) records are invisible.

### 5. Idempotent Producer Writes

Kafka's idempotent producer (enabled by default with transactions) assigns sequence numbers to records. If a producer retries a send, Kafka deduplicates based on `(producerId, partition, sequenceNumber)`.

## 2.9 KafkaWriterState: What Gets Checkpointed

```java
// sink/KafkaWriterState.java
public class KafkaWriterState {
    private final String transactionalIdPrefix;
    private final int ownedSubtaskId;
    private final int totalNumberOfOwnedSubtasks;
    private final TransactionOwnership transactionOwnership;
    private final Collection<CheckpointTransaction> precommittedTransactionalIds;
    // CheckpointTransaction = (transactionalId, checkpointId)
}
```

This state allows recovery to:
1. Know which transactions are in-flight (precommitted but not yet committed)
2. Skip aborting those transactions (the committer will commit them)
3. Handle parallelism changes via the ownership model

## 2.10 Complete Timeline: One Checkpoint Cycle

```
TIME ──────────────────────────────────────────────────────────────────►

Writer (Subtask 0):
  │ beginTransaction("my-sink-0-5")
  │ send(record1) → state: DATA_IN_TRANSACTION
  │ send(record2)
  │ send(record3)
  │                         ┌─ Checkpoint barrier arrives
  │                         │
  │ prepareCommit()         │  → KafkaCommittable(producerId=42, epoch=3, "my-sink-0-5")
  │   precommitTransaction()│  → state: PRECOMMITTED
  │                         │
  │ snapshotState(5)        │  → poll backchannel (recycle committed producers)
  │                         │  → persist ongoingTransactions to state
  │                         │  → startTransaction("my-sink-0-6")
  │                         │  → beginTransaction() → state: IN_TRANSACTION
  │ send(record4)           │  → new records go to checkpoint 6's transaction
  │                         │
  │                         │  Checkpoint 5 persisted to storage ✓
  │                         │
Committer (colocated):      │
  │ commit(KafkaCommittable)│
  │   getProducer()         │  → resumeTransaction(producerId=42, epoch=3)
  │   commitTransaction()   │  ← DATA BECOMES VISIBLE TO CONSUMERS
  │   backchannel.send(     │
  │     TransactionFinished │
  │     .successful("my-sink-0-5"))
  │                         │
  │   (next snapshotState() │  will poll this and recycle the producer)
```
