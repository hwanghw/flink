# `flink-filesystems`

## Purpose

Aggregator that ships **pluggable `FileSystem` implementations** for
every supported object store: HDFS-compatible (any Hadoop FS), AWS S3
(two flavors), Aliyun OSS, Azure Blob / ADLS Gen2, and Google Cloud
Storage. Each sub-module is a self-contained jar that is **loaded as
a Flink plugin** via a dedicated `URLClassLoader`, so that the
underlying SDKs (`aws-java-sdk`, `hadoop-aws`, `presto-hive`,
`google-cloud-storage`, `aliyun-oss-sdk`, `azure-storage`) never
collide with each other or with the user's job classpath.

Beyond plain read/write, every implementation supports Flink's
**`RecoverableWriter`** abstraction (used by the streaming `FileSink`,
the bucketing sinks, RocksDB incremental checkpoints, Hive sinks,
Iceberg / Paimon connectors that delegate to FS, etc.).

## Where it fits

The contract — `FileSystem`, `FileSystemFactory`, `RecoverableWriter`,
`Path`, `FSDataInputStream`, `FSDataOutputStream` — lives in
**`flink-core`** under `org.apache.flink.core.fs`. This module
contains the **implementations** of those contracts. Lookup happens
through `FileSystem.initialize(Configuration, PluginManager)` which
uses Java `ServiceLoader` to discover `FileSystemFactory` SPIs;
each sub-module advertises its factory in
`META-INF/services/org.apache.flink.core.fs.FileSystemFactory`.

## Maven coordinates

Aggregator POM: `<artifactId>flink-filesystems</artifactId>`,
packaging `pom`. Inherited property `fs.hadoopshaded.version` =
**3.3.4** (the Hadoop version that backs the shaded jar). Dependency
convergence is intentionally disabled for the children because each
shades its own world.

### Sub-modules

| Sub-module                | Purpose                                                                                                                    | Key factory(ies)                                                                                          |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `flink-fs-hadoop-shaded`  | Shaded `hadoop-common` + transitive deps under a Flink-private package. All hadoop-flavored FS modules depend on this.     | n/a (a build helper)                                                                                      |
| `flink-hadoop-fs`         | Generic `HadoopFileSystem` wrapper that adapts any Hadoop `FileSystem` to Flink. Provides HDFS and any Hadoop-compat URL. | `HadoopFsFactory` (schemes: `hdfs`, `viewfs`, `webhdfs`, ...)                                             |
| `flink-s3-fs-base`        | Shared S3 plumbing: `AbstractS3FileSystemFactory`, `FlinkS3FileSystem`, `S3RecoverableWriter`, multipart-upload helpers.   | abstract                                                                                                  |
| `flink-s3-fs-hadoop`      | S3 via Hadoop's S3A connector. Recommended default.                                                                        | `S3FileSystemFactory`, `S3AFileSystemFactory` (schemes: `s3://`, `s3a://`)                                |
| `flink-s3-fs-presto`      | S3 via Presto's S3 connector. Lower-latency for many small reads / RocksDB checkpoint downloads. Bundles `S5Cmd` support.  | `S3FileSystemFactory`, `S3PFileSystemFactory` (schemes: `s3://`, `s3p://`)                                |
| `flink-oss-fs-hadoop`     | Aliyun OSS via Hadoop's `hadoop-aliyun`.                                                                                   | `OSSFileSystemFactory` (`oss://`)                                                                         |
| `flink-azure-fs-hadoop`   | Azure Blob (`wasb[s]://`) and ADLS Gen2 (`abfs[s]://`) via `hadoop-azure`. Secure / insecure variants.                     | `AzureBlobStorageFSFactory`, `SecureAzureBlobStorageFSFactory`, `AzureDataLakeStoreGen2FSFactory`, ...    |
| `flink-gs-fs-hadoop`      | Google Cloud Storage. Uses Google's official `google-cloud-storage` Java SDK directly (not Hadoop's GCS connector).        | `GSFileSystemFactory` (`gs://`)                                                                           |

Note: there is no `flink-mapr-fs` in this tree — MapR support was
removed in Flink 1.x.

## Dependencies

Common to every child (declared in the aggregator pom):

- `org.slf4j:slf4j-api` (provided)
- `com.google.code.findbugs:jsr305` (provided)
- `flink-test-utils-junit` (test)
- `flink-core` (provided, brings `FileSystem` and `Path`)

### Direct (per sub-module highlights)

- **`flink-fs-hadoop-shaded`** — shades `org.apache.hadoop:hadoop-
  common` 3.3.4 (plus transitive `commons-*`, `jersey-*`, etc.)
  under `org.apache.flink.fs.shaded.hadoop3.*`.
- **`flink-hadoop-fs`** — depends on the shaded jar plus
  `org.apache.hadoop:hadoop-common` (provided / shaded).
- **`flink-s3-fs-base`** — pulls Hadoop's S3A interfaces and provides
  the abstract S3 factory all S3 sub-modules extend.
- **`flink-s3-fs-hadoop`** — `flink-s3-fs-base`, `hadoop-aws`,
  `aws-java-sdk-*` (S3A).
- **`flink-s3-fs-presto`** — `flink-s3-fs-base`, Presto's S3 SDK
  shaded; also bundles support for the `s5cmd` external binary for
  fast bulk copies (`s3.s5cmd.path`).
- **`flink-oss-fs-hadoop`** — `hadoop-aliyun`, `aliyun-sdk-oss`.
- **`flink-azure-fs-hadoop`** — `hadoop-azure`, MS `azure` SDK,
  `jetty-util-ajax`.
- **`flink-gs-fs-hadoop`** — `com.google.cloud:google-cloud-storage`,
  Google auth libraries.

Every sub-module uses `maven-shade-plugin` to relocate its
third-party deps under `org.apache.flink.fs.shaded.*`, producing an
opaque single jar suitable for the plugin loader.

### Used by

External dependents (only those that explicitly name an FS artifact):

- `flink-runtime` — depends on `flink-hadoop-fs` for the
  built-in HDFS RecoverableWriter integration tests / archive
  fetcher.
- `flink-formats/flink-parquet` — uses `flink-hadoop-fs` for the
  Hadoop input format compatibility layer.
- `flink-dist` — bundles every FS jar (mostly at `provided` scope,
  copied into `lib/` or `plugins/` of the distribution).

**Everyone else** (state backends, sinks, connectors) interacts only
with the `org.apache.flink.core.fs.FileSystem` API in `flink-core`.
The choice of implementation is made at runtime by URL scheme.

## Source layout (representative classes)

```
flink-fs-hadoop-shaded/         # no source — pure shading project

flink-hadoop-fs/src/main/java/org/apache/flink/runtime/
├── fs/hdfs/
│   ├── HadoopFsFactory.java                  # FileSystemFactory entry
│   ├── HadoopFileSystem.java                 # wraps any Hadoop FS
│   ├── HadoopDataInputStream / OutputStream
│   ├── HadoopFileStatus / LocatedHadoopFileStatus / BlockLocation
│   ├── HadoopRecoverableWriter.java          # checkpoint-safe writes
│   ├── HadoopRecoverableFsDataOutputStream.java
│   ├── BaseHadoopFsRecoverableFsDataOutputStream.java
│   ├── HadoopFsRecoverable.java              # persistent state
│   └── HadoopRecoverableSerializer.java
└── util/HadoopUtils.java, HadoopConfigLoader.java

flink-s3-fs-base/src/main/java/org/apache/flink/fs/s3/common/
├── AbstractS3FileSystemFactory.java          # base; reads s3.access-key,
│                                              s3.secret-key, s3.endpoint,
│                                              s3.s5cmd.path, ...
├── FlinkS3FileSystem.java                    # extends HadoopFileSystem
├── token/
│   ├── AbstractS3DelegationTokenProvider.java
│   ├── AbstractS3DelegationTokenReceiver.java
│   └── DynamicTemporaryAWSCredentialsProvider.java
└── writer/                                    # S3 RecoverableWriter
    ├── S3RecoverableWriter.java
    ├── S3RecoverableFsDataOutputStream.java
    ├── S3RecoverableMultipartUploadFactory.java
    ├── RecoverableMultiPartUpload(Impl).java  # MPU state machine
    ├── S3Committer.java                       # finalises a checkpoint
    ├── S3Recoverable / S3RecoverableSerializer
    ├── MultiPartUploadInfo.java
    └── S3AccessHelper.java                    # SDK-agnostic facade

flink-s3-fs-hadoop/.../org/apache/flink/fs/s3hadoop/
├── S3FileSystemFactory.java                   # scheme s3://
├── S3AFileSystemFactory.java                  # scheme s3a://
├── HadoopS3AccessHelper.java                  # S3A SDK glue
└── token/  (delegation token provider/receiver)

flink-s3-fs-presto/.../org/apache/flink/fs/s3presto/
├── S3FileSystemFactory.java                   # scheme s3://
├── S3PFileSystemFactory.java                  # scheme s3p://
├── FlinkS3PrestoFileSystem.java
└── token/  (delegation token provider/receiver)

flink-oss-fs-hadoop/.../org/apache/flink/fs/osshadoop/
├── OSSFileSystemFactory.java                  # scheme oss://
├── FlinkOSSFileSystem.java
├── OSSAccessor.java
└── writer/  (OSSRecoverableWriter + serializer + committer)

flink-azure-fs-hadoop/.../org/apache/flink/fs/azurefs/
├── AbstractAzureFSFactory.java
├── AzureBlobStorageFSFactory.java             # wasb://
├── SecureAzureBlobStorageFSFactory.java       # wasbs://
├── AzureDataLakeStoreGen2FSFactory.java       # abfs://
├── SecureAzureDataLakeStoreGen2FSFactory.java # abfss://
├── AzureBlobFileSystem.java
├── AzureBlobRecoverableWriter.java
├── AzureBlobFsRecoverableDataOutputStream.java
└── EnvironmentVariableKeyProvider.java

flink-gs-fs-hadoop/.../org/apache/flink/fs/gs/
├── GSFileSystemFactory.java                   # scheme gs://
├── GSFileSystem.java
├── GSFileSystemOptions.java
├── storage/  (GSBlobStorage + impl + GSBlobIdentifier)
├── utils/    (BlobUtils, ChecksumUtils, ConfigUtils)
└── writer/   (GSRecoverableWriter, GSCommitRecoverable,
             GSResumeRecoverable, GSChecksumWriteChannel,
             GSRecoverableWriterCommitter, ...)
```

## Architecture & key concepts

### `FileSystem` (`flink-core`)

Flink's filesystem abstraction. Sub-classes implement `open()`,
`create()`, `getFileStatus()`, `listStatus()`, `delete()`, `rename()`,
and (optionally) `createRecoverableWriter()`. Streams returned must
expose `getPos()` and `sync()`.

### `FileSystemFactory` (SPI)

Each plugin declares its factories in `META-INF/services/`. The
`FileSystem.get(URI)` lookup matches by URI scheme; multiple plugins
may register the **same** scheme — last one initialized wins, which
is why ordering inside `plugins/` matters when both
`flink-s3-fs-hadoop` and `flink-s3-fs-presto` are installed.

### `RecoverableWriter`

The contract used by streaming file sinks for **exactly-once** writes:

1. `open(path)` returns a `RecoverableFsDataOutputStream`.
2. On checkpoint, `persist()` records intermediate state (offset +
   in-flight multipart upload id for S3; tmp file path + length for
   HDFS).
3. The `RecoverableWriter.ResumeRecoverable` is serialized into the
   checkpoint via a `SimpleVersionedSerializer`.
4. On restore, `recover(resumeable)` rebuilds the output stream.
5. On commit (end of input or aligned barrier), `commit()` finalises
   the file (HDFS rename, S3 CompleteMultipartUpload, GCS compose,
   etc.).

### Plugin classloader isolation

Critical design point. Each FS jar lives under
`$FLINK_HOME/plugins/<name>/<name>.jar`. At startup,
`PluginManager` (in `flink-core`) creates **one `URLClassLoader` per
plugin directory** whose parent is restricted via a parent-first /
child-first filter. The user's job classloader and the system
classloader cannot see the FS plugin's classes (or its shaded
SDKs); they can only see the `org.apache.flink.core.fs.*` API. This
prevents version conflicts between (say) the user's `aws-java-sdk`
1.11 and the plugin's 1.12.

This isolation is the reason every sub-module **shades everything**.
If you depend on `org.apache.flink:flink-s3-fs-hadoop` from a user
project, classpath layout will break — always use the plugin
mechanism instead, by dropping the jar into
`$FLINK_HOME/plugins/s3-fs-hadoop/`.

## Important public APIs

- `org.apache.flink.core.fs.FileSystem` — what every user touches.
- `FileSystemFactory` — the SPI implemented by each child.
- `RecoverableWriter` / `RecoverableFsDataOutputStream` /
  `ResumeRecoverable` / `CommitRecoverable` — sink-side contract.
- `AbstractS3FileSystemFactory` — extension point for S3-compatible
  stores (custom endpoints, in-house Ceph deployments, MinIO, etc.).
- `S3AccessHelper` — SDK-agnostic facade so the writer code in
  `flink-s3-fs-base` can target either the S3A SDK
  (`flink-s3-fs-hadoop`) or Presto's
  (`flink-s3-fs-presto`).
- Config keys: every implementation registers `ConfigOption`s; see
  `AbstractS3FileSystemFactory`, `GSFileSystemOptions`, and the
  hadoop-style `*-site.xml` keys forwarded by `HadoopConfigLoader`.

## Internal flows

### URL → FileSystem lookup

```
user code: FileSystem.get(new Path("s3://bucket/key"))
   │
   ▼
FileSystem#getUnguardedFileSystem(uri)
   │  (cached by authority+scheme)
   ▼
FileSystem#getFileSystemFactory(scheme)
   │  ServiceLoader scan across plugin classloaders
   ▼
S3FileSystemFactory.create(uri)
   │  (loads Hadoop config, builds S3A FileSystem instance)
   ▼
FlinkS3FileSystem (extends HadoopFileSystem)
```

### Streaming sink commit on S3

```
StreamingFileSink/FileSink writes via S3RecoverableFsDataOutputStream
   ├── data buffered to local temp file
   ├── once >= s3.multipart.min-part-size or sink hint → UploadPart
   └── checkpoint barrier:
         persist() → snapshots {uploadId, part list, last part bytes}
         serializes as S3Recoverable in checkpoint
   commit():
         CompleteMultipartUpload(uploadId, parts)
         → object visible at final key
```

If recovery happens between persist() and commit(), the
`uploadId` survives in the checkpoint, so the in-flight multipart
upload is **resumed and committed**, not retried from scratch —
this is the heart of exactly-once on S3.

## Tests

- HDFS / Hadoop:
  `flink-hadoop-fs/.../HadoopFileSystemTest`,
  `HadoopRecoverableWriterTest`.
- S3 (require live credentials, gated by an env-driven flag):
  - `S3FileSystemBehaviorITCase`,
  - `S3RecoverableWriterITCase`,
  - `S5CmdOnHadoopS3FileSystemITCase` /
    `S5CmdOnPrestoS3FileSystemITCase`,
  - `HAJobRunOnHadoopS3FileSystemITCase` /
    `HAJobRunOnPrestoS3FileSystemITCase` — end-to-end HA test
    that runs an actual job whose checkpoints land on S3.
- OSS: `HadoopOSSFileSystemITCase`,
  `HadoopOSSRecoverableWriterITCase`,
  `HadoopOSSRecoverableWriterExceptionITCase`.
- GCS: `GSRecoverableWriterTest`,
  `GSCommitRecoverableSerializerTest`, etc. (mostly local; use a
  fake `GSBlobStorage`).
- Azure: behaviour tests in `flink-azure-fs-hadoop/src/test/...`.

Most `ITCase` tests require external credentials and are skipped by
default.

## Pitfalls & gotchas

- **Always install as a plugin.** Putting a filesystem jar on the
  system classpath (`$FLINK_HOME/lib/`) defeats the classloader
  isolation and frequently causes `NoSuchMethodError` between two
  different SDK versions. The correct location is
  `$FLINK_HOME/plugins/<fs-name>/<fs-jar>`.
- **`flink-s3-fs-hadoop` vs `flink-s3-fs-presto`.** Both register the
  `s3://` scheme. If both jars are dropped into `plugins/`, the
  outcome is implementation-defined ("last one wins"). Use
  `s3a://` to force Hadoop and `s3p://` to force Presto explicitly.
  Presto is typically faster for many small reads (RocksDB
  incremental checkpoint download); Hadoop is more feature-complete.
- **`s5cmd` integration is opt-in.** Setting `s3.s5cmd.path` makes
  Flink shell out to the `s5cmd` binary for bulk copies during
  RocksDB incremental state recovery. The binary must exist at that
  path on every TaskManager.
- The shaded `hadoop-common` 3.3.4 is **the** Hadoop version Flink
  embeds. User jobs that bring their own Hadoop SDK at a different
  major version will see method-resolution errors **unless the
  plugin mechanism is used**.
- HDFS `HadoopRecoverableWriter` uses the underlying Hadoop
  `truncate()`+rename machinery; on filesystems that do not support
  truncate (e.g. some HDFS-compatible block stores) it falls back to
  a copy-and-rename, which is significantly slower.
- The Hadoop-azure connector requires JDK system property
  `fs.azure.enable.flush=true` for ABFS streaming sinks — the
  Azure SDK does not flush on close by default.
- **GCS does not natively support multipart upload** in the same way
  as S3. `GSRecoverableWriter` instead uses **object composition**:
  parts are written as separate temp objects, then the commit step
  issues a `compose()` to produce the final object. Different state
  shape (`GSResumeRecoverable` vs `S3Recoverable`).
- The `flink-fs-hadoop-shaded` jar is **not** meant to be referenced
  from user code; it exists only to break the otherwise cyclic
  Hadoop dependency graph during the shaded build.

## Related modules / docs

- `flink-core/.../org/apache/flink/core/fs/` — the abstractions
  implemented here.
- `flink-streaming-java`'s `FileSink` / `StreamingFileSink` — the
  largest consumer of `RecoverableWriter`.
- `flink-state-backends/flink-statebackend-rocksdb` — uses these
  filesystems for incremental checkpoint upload/download; the
  `s5cmd` integration above is for it.
- `codedocs/flink-exactly-once-checkpointing-deep-dive.md` — explains
  the persist/commit dance from the sink side.
- `codedocs/flink-iceberg-connector-deep-dive.md` — references
  `s3://bucket/db/table/...` paths whose backing FS is provided
  here.
- `codedocs/flink-rocksdb-state-backend-tuning.md` — RocksDB
  incremental checkpoints flow through `S3RecoverableWriter` (and
  optionally `s5cmd`).
- `codedocs/flink-sink-patterns-comparison.md` — discusses the
  `RecoverableWriter`-based sinks vs. the unified Sink V2 API.
