# `flink-dstl`

Parent Maven aggregator pom for Flink's **Durable Short-term Log (DSTL)** —
the storage layer behind the Changelog State Backend (FLIP-158). DSTL is
where every keyed-state mutation gets appended *between* materializations,
so checkpoints can be taken in O(delta) time without waiting for the
underlying state backend (RocksDB, heap) to flush a full snapshot.

DSTL is sometimes spelled out as "Durable Short-term Log" in the FLIP and as
"Distributed State Transition Log" in some external docs — the code uses
DSTL as the artifact prefix without expanding the acronym.

## Purpose

- Aggregator-only (`<packaging>pom</packaging>`) — produces no jar.
- Defines the SPI for changelog storage via `StateChangelogStorageFactory`
  (the interface itself lives in `flink-runtime`) and ships the default
  filesystem-backed implementation.
- Decouples checkpoint latency from the working-state backend: the
  Changelog Backend (in `flink-state-backends/flink-statebackend-changelog`)
  *writes* state changes here continuously and *reads* from a periodic
  materialization of the underlying backend.

## Where it fits (FLIP-158, simplified)

```
operator state op (put / append / clear)
        |
        v
ChangelogKeyedStateBackend                  (flink-statebackend-changelog)
   |                          \
   |  apply to working state   \  log change
   v                            v
delegated backend (rocksdb / heap)   StateChangelogWriter
                                          |
                                          v
                                     DSTL (this module)
                                       FsStateChangelogStorage
                                       BatchingStateChangeUploadScheduler
                                       StateChangeFsUploader
                                          |
                                          v
                                     DFS (S3 / HDFS / local fs)

checkpoint barrier
        |
        v
StateChangelogWriter.persist(sqn) -> upload task -> StreamStateHandle
        |
        v
ChangelogStateBackendHandle (a small handle holding "materialized state up to T"
                             + "changelog after T")

PeriodicMaterializationManager (in flink-statebackend-common) periodically
   asks the wrapped backend to materialize, then `ChangelogTruncateHelper`
   prunes the now-redundant changelog suffix.
```

So DSTL is the storage *underneath* the changelog backend; the backend
itself is in `flink-state-backends`.

## Maven coordinates

- Group: `org.apache.flink`
- Artifact: `flink-dstl`
- Version: `2.3-SNAPSHOT`
- Parent: `flink-parent`
- Packaging: `pom`

### Sub-modules

| Sub-module | Purpose |
| ---------- | ------- |
| `flink-dstl-dfs` | The only current DSTL implementation: appends batched state changes to files in a DFS (S3, HDFS, GCS, file://). Registered as `state.changelog.storage = filesystem` via a `META-INF/services` SPI entry. |

The aggregator declares no dependencies; everything is in the sub-module.

## Dependencies

### Direct (flink-dstl-dfs)

- `flink-core` — `Path`, `FileSystem`, `JobID`, config options.
- `flink-runtime` — `StateChangelogStorage`, `StateChangelogWriter`,
  `StateChange`, `SequenceNumber`, `ChangelogStateHandleStreamImpl`,
  `LocalChangelogRegistry`, `TaskManagerJobMetricGroup`.
- `flink-streaming-java` — only for `CheckpointingOptions.CHECKPOINTING_TIMEOUT`.
- Test scope: `flink-test-utils-junit`, plus test-jars of `flink-core`,
  `flink-runtime`, `flink-streaming-java`.

### Used by

- `flink-state-backends/flink-statebackend-changelog` — the changelog
  backend instantiates `StateChangelogStorage` via
  `TaskStateManager.getStateChangelogStorage()`; the implementation is
  loaded by SPI.
- `flink-dist` — packages `flink-dstl-dfs` so the changelog backend can
  find it.
- `flink-tests` and `flink-test-utils-parent/flink-test-utils` — ITCases.
- `flink-docs` — config option generator picks up `FsStateChangelogOptions`
  for the website docs.

## Source layout

All in `org.apache.flink.changelog.fs` (`flink-dstl-dfs`):

```
flink-dstl/
  flink-dstl-dfs/
    META-INF/services/
      org.apache.flink.runtime.state.changelog.StateChangelogStorageFactory
                                              # -> FsStateChangelogStorageFactory

    FsStateChangelogStorageFactory.java       # SPI entry; identifier "filesystem"
    FsStateChangelogStorage.java              # StateChangelogStorage impl
    FsStateChangelogStorageForRecovery.java   # read-only variant used on restore
    FsStateChangelogWriter.java               # one writer per (operator, keyGroupRange)

    StateChangeUploader.java                  # SPI: upload(Collection<UploadTask>)
    StateChangeUploadScheduler.java           # SPI: scheduler interface (direct vs batching)
    BatchingStateChangeUploadScheduler.java   # default; delays + size-thresholds for batching
    SchedulerFactory.java                     # builds scheduler from config

    AbstractStateChangeFsUploader.java        # base for writing changes to a DFS Path
    StateChangeFsUploader.java                # default uploader: one DFS file per upload
    DuplicatingStateChangeFsUploader.java     # writes to a primary + secondary path (local recovery)

    StateChangeFormat.java                    # binary encoding of state changes
    StateChangeSet.java, StateChangeIteratorImpl.java
    OutputStreamWithPos.java, DuplicatingOutputStreamWithPos.java
    ChangelogStreamWrapper.java               # optional compression layer
    ChangelogStreamHandleReader.java          # read path on restore
    ChangelogStreamHandleReaderWithCache.java # caches recently-read change files

    ChangelogStorageMetricGroup.java          # ProxyMetricGroup with upload counters/histograms
    UploadResult.java, UploadThrottle.java    # backpressure when in-flight bytes exceed limit
    TaskChangelogRegistry.java                # per-task registry; drives discard of unused uploads
    TaskChangelogRegistryImpl.java
    RetryPolicy.java, RetryingExecutor.java   # retry with backoff for failed uploads
    FsStateChangelogOptions.java              # all state.changelog.dstl.* options
```

## Architecture & key concepts

- **`StateChangelogStorage` SPI.** The interface (in `flink-runtime`) is
  loaded reflectively from
  `META-INF/services/org.apache.flink.runtime.state.changelog.StateChangelogStorageFactory`.
  This module's `FsStateChangelogStorageFactory` registers identifier
  `"filesystem"` and is selected by setting `state.changelog.storage = filesystem`.
- **Per-writer log id.** `FsStateChangelogStorage.createWriter(operatorId,
  keyGroupRange, mailboxExecutor)` returns one `FsStateChangelogWriter`
  per `(operator, keyGroupRange)`. The writer holds an in-memory list of
  un-uploaded `StateChangeSet`s tagged with a monotonic `SequenceNumber`.
- **Append vs persist.**
  - `append(keyGroup, bytes)` is the per-mutation call from the changelog
    backend. It stores changes in memory only (no synchronization).
  - `persist(from, checkpointId)` is invoked at checkpoint barrier; it
    submits an `UploadTask` to the scheduler and returns a
    `CompletableFuture<ChangelogStateHandleStreamImpl>` that completes
    once the upload finishes.
- **Pre-emptive persist.** When the in-memory buffer exceeds
  `state.changelog.dstl.dfs.preemptive-persist-threshold` (default 5 MB) the
  writer triggers an upload *without* waiting for a checkpoint. This is the
  key trick that keeps checkpoint async-phase cost flat.
- **Batching scheduler.** `BatchingStateChangeUploadScheduler` waits up to
  `state.changelog.dstl.dfs.batch.persist-delay` (default 10 ms) or until
  `state.changelog.dstl.dfs.batch.persist-size-threshold` (default 10 MB)
  is reached, then hands a batch of `UploadTask`s to the underlying
  `StateChangeUploader`. Batching coalesces many small operator writes
  into a single DFS file → fewer S3 PUT requests → shorter checkpoint
  async phase.
- **DFS uploader.** `StateChangeFsUploader` creates one DFS file per
  batch under `<base-path>/dstl/<job-uuid>/<random-uuid>`, returning a
  `FileStateHandle` plus per-`StateChangeSet` offsets. The
  `DuplicatingStateChangeFsUploader` writes the same bytes to a primary
  DFS path *and* a local path for fast local recovery.
- **Backpressure.** `UploadThrottle` enforces
  `state.changelog.dstl.dfs.upload.max-in-flight` (default 100 MB). When
  exceeded, snapshotting blocks; normal processing only blocks if the
  pre-emptive threshold is configured.
- **Retries.** `RetryingExecutor` + `RetryPolicy` re-attempt failed uploads
  with exponential backoff up to `state.changelog.dstl.dfs.upload.max-attempts`
  (default 3). Per-attempt and total attempts are histogrammed.
- **TaskChangelogRegistry.** Tracks which uploaded change files are still
  referenced by some live writer. Once all writers have advanced past a
  file's sequence range (truncation), the registry discards the file via
  a small thread pool (`state.changelog.dstl.dfs.discard.num-threads`).
- **Recovery view.** `FsStateChangelogStorageForRecovery` is a read-only
  `StateChangelogStorageView` used on restore: it opens
  `ChangelogStreamHandleReaderWithCache` (LRU cache of decompressed bytes)
  to replay change files. Returned as `createStorageView()` from the
  factory.
- **Encoding.** `StateChangeFormat` writes a length-prefixed record stream;
  `ChangelogStreamWrapper` optionally wraps it in a compressing stream when
  `state.changelog.dstl.dfs.compression.enabled = true`.

## Important public APIs

User-facing: there isn't one. DSTL is selected indirectly by setting
`state.backend.changelog.enabled = true` and
`state.changelog.storage = filesystem`. All other knobs are in
`FsStateChangelogOptions`:

- `state.changelog.dstl.dfs.base-path` — required; DFS path for changelog
  files.
- `state.changelog.dstl.dfs.preemptive-persist-threshold` (5 MB) — per-op
  buffer size that triggers eager upload.
- `state.changelog.dstl.dfs.batch.persist-delay` (10 ms) —
  scheduler debounce.
- `state.changelog.dstl.dfs.batch.persist-size-threshold` (10 MB) —
  scheduler flush size.
- `state.changelog.dstl.dfs.upload.buffer-size` (1 MB).
- `state.changelog.dstl.dfs.upload.num-threads` (5).
- `state.changelog.dstl.dfs.upload.max-in-flight` (100 MB) — backpressure.
- `state.changelog.dstl.dfs.discard.num-threads` (1).
- `state.changelog.dstl.dfs.compression.enabled` (false).
- `state.changelog.dstl.dfs.retry.*` — attempts, backoff, timeout.

Most options have legacy `dstl.dfs.*` aliases retained via
`withDeprecatedKeys`.

## Internal flows

- **Normal write path.**
  1. Operator mutates state → `Changelog*State` (in
     `flink-statebackend-changelog`) calls `KvStateChangeLoggerImpl.valueUpdated(...)`.
  2. Logger serializes to bytes and calls `FsStateChangelogWriter.append(keyGroup, bytes)`.
  3. Bytes accumulate in `notUploaded`. If size > `preemptive-persist-threshold`,
     `persistInternal(...)` is called eagerly.
- **Checkpoint async phase.**
  1. Checkpoint coordinator triggers per-operator `snapshotState` →
     `FsStateChangelogWriter.persist(lastConfirmedSqn, checkpointId)`.
  2. Writer builds an `UploadTask` (sqn range, bytes, completion callback)
     and submits to `BatchingStateChangeUploadScheduler`.
  3. Scheduler either fires immediately (size threshold hit) or after
     `persist-delay`. It coalesces tasks from all writers in the slot and
     hands them to `StateChangeFsUploader`.
  4. Uploader opens one DFS file, writes all batched change sets, computes
     per-`StateChangeSet` `(startOffset, endOffset)` tuples, returns an
     `UploadTasksResult`.
  5. Per task, `complete(List<UploadResult>)` resolves the writer's
     `CompletableFuture` with a `ChangelogStateHandleStreamImpl(handle, offsets)`.
- **Restore path.**
  1. `FsStateChangelogStorageFactory.createStorageView` returns a
     `FsStateChangelogStorageForRecovery` with a
     `ChangelogStreamHandleReaderWithCache`.
  2. The changelog backend iterates `ChangelogStateHandleStreamImpl`s; for
     each, the reader opens the DFS file once, caches the bytes, and
     emits `StateChange` records via `StateChangeIteratorImpl`.
  3. The changelog backend applies them through
     `ChangelogBackendLogApplier` to the underlying restored backend.
- **Truncation.** After a successful checkpoint and a materialization,
  `ChangelogTruncateHelper` calls `truncate(sqn)` on the writer; the
  writer notifies `TaskChangelogRegistry`, which decrements ref counts
  and may schedule a delete on the discard executor.

## Tests

- `FsStateChangelogStorageTest`, `FsStateChangelogWriterTest`,
  `FsStateChangelogWriterSqnTest` — end-to-end writer behavior including
  sequence numbering and persist semantics.
- `BatchingStateChangeUploadSchedulerTest`,
  `TestingBatchingUploadScheduler`, `TestingStateChangeUploader` — exercise
  batching, size threshold, persist-delay, throttling.
- `StateChangeFsUploaderTest` — DFS write format and offset accounting.
- `ChangelogStreamHandleReaderWithCacheTest` — restore-side caching.
- `RetryingExecutorTest` — backoff behavior.
- `TaskChangelogRegistryImplTest` — ref counting / discard.
- `ChangelogStorageMetricsTest` — that counters/histograms move as
  expected.
- Cross-module integration tests live in
  `flink-state-backends/flink-statebackend-changelog/src/test` and
  `flink-tests`.

## Pitfalls & gotchas

- **Required DFS path.** `state.changelog.dstl.dfs.base-path` has no
  default; forgetting it crashes the JM with an `IllegalConfigurationException`.
- **Cleanup churn on S3.** Pre-emptive persisting + small batches mean lots
  of small S3 objects. Tune `persist-size-threshold` and `persist-delay`
  upward if S3 PUT/DELETE costs dominate.
- **Backpressure can stall snapshots.** When `max-in-flight` is exceeded,
  the snapshot path blocks; check the `dstl.upload.queue.size` and
  `dstl.upload.latencies` metrics if checkpoint times grow.
- **DFS lifecycle.** Changelog files are discarded by `TaskChangelogRegistry`
  on truncation. If a JM crashes mid-checkpoint, orphan files can linger in
  the DFS path — they will not be cleaned automatically. Use lifecycle
  policies on the bucket.
- **DuplicatingUploader == double bandwidth.** With local recovery enabled
  every change is written to *both* the DFS path and the local path.
  Disable local recovery if your network is the bottleneck.
- **Compression is opt-in.** It defaults off because of CPU cost; for
  highly compressible payloads (long strings, repeated keys), turning it on
  can shrink DFS bytes 5–10x.
- **Don't share a base path across jobs without isolation.** Files live
  under `<base-path>/dstl/<jobUUID>/...` but bucket-level access is on you.
- **Materialization is *not* in this module.** The periodic
  materialization that allows changelog truncation lives in
  `flink-statebackend-common`'s `PeriodicMaterializationManager` —
  forgetting to run materialization causes unbounded changelog growth.
- **Sequence numbers are per-writer.** Don't compare `SequenceNumber`s
  across operators; ranges only make sense within one
  `FsStateChangelogWriter`.

## Related modules / codedocs

- `flink-state-backends/flink-statebackend-changelog` —
  `ChangelogKeyedStateBackend`, `KvStateChangeLogger*`, state wrappers
  (`Changelog{Value,List,Map,Reducing,Aggregating}State`). The consumer of
  this module.
- `flink-state-backends/flink-statebackend-common` —
  `PeriodicMaterializationManager` and `ChangelogMaterializationMetricGroup`
  drive the materialization side of FLIP-158.
- `flink-runtime/.../state/changelog/` —
  `StateChangelogStorage`, `StateChangelogStorageFactory`,
  `StateChangelogStorageView`, `StateChangelogWriter`,
  `StateChange`, `SequenceNumber`, `LocalChangelogRegistry`,
  `ChangelogStateHandleStreamImpl`.
- `flink-core/.../configuration/StateChangelogOptions.java` — the
  `state.changelog.*` options (the `STATE_CHANGE_LOG_STORAGE` identifier
  selector lives here).
- `codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md` —
  for context on the RocksDB snapshot path that the changelog backend
  *avoids* doing per-checkpoint.
- `codedocs/flink-exactly-once-checkpointing-deep-dive.md` — how
  changelog handles fit into checkpoint metadata.
- `codedocs/flink-state-ttl-architecture.md` — TTL applies to the
  delegated backend's materialized state; the changelog itself is
  truncated by sequence number, not by TTL.
- FLIP-158: "Generalized incremental checkpoints" (Apache Flink FLIPs
  wiki) — the design doc that motivated this module.
