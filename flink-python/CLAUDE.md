# `flink-python` (PyFlink)

## Purpose
The Python API for Flink — DataStream and Table API exposed to Python users, plus
the execution infrastructure to run Python UDFs from Java-based JobManager /
TaskManager processes. This single Maven module produces both a JVM artifact
(`flink-python-<version>.jar`, shaded with Beam/Arrow/Py4J/Pemja) and the
`apache-flink` Python wheel published to PyPI.

## Where it fits
PyFlink users write Python programs against `pyflink.datastream` or `pyflink.table`.
At program-construction time the Python code drives Java via Py4J: every Python
API call translates into a JVM call that builds the same `Transformation` /
`Table` graph the Java API builds. At runtime, Python UDFs are dispatched from
the JVM operator into a Python worker — either an external worker subprocess
(default) managed via Apache Beam's portability framework over gRPC, or an
embedded interpreter inside the JVM via PemJa (thread mode).

## Maven coordinates
- Java side (this module): `org.apache.flink:flink-python` (jar)
- Python side: published as `apache-flink` on PyPI; built from `setup.py` /
  `pyproject.toml` in this directory. Companion package `apache-flink-libraries`
  is shipped from `apache-flink-libraries/`.

## Dependencies
### Direct (Java, `provided` or shaded)
- `flink-core`, `flink-clients`, `flink-streaming-java`, `flink-table-common`,
  `flink-table-runtime`, `flink-connector-files`, `flink-csv`, `flink-avro`,
  `flink-json` (all `provided`).
- `org.apache.beam:beam-runners-java-fn-execution` + `beam-runners-core-java` —
  the Beam portability framework that drives the Python worker over gRPC.
- `com.alibaba:pemja` 0.5.6 — Python interpreter embedded into the JVM, used
  for `python.execution-mode = thread`.
- `net.sf.py4j:py4j` + `net.razorvine:pyrolite` — Py4J gateway for the
  client-side bridge; pyrolite for pickle protocol.
- `com.google.protobuf:protobuf-java` — protocol for `flink-fn-execution.proto`.
- `org.apache.arrow:arrow-vector` + `arrow-memory-netty` 13.0.0 — Arrow batches
  for vectorized (Pandas) UDFs.

All bundled deps are relocated under `org.apache.flink.api.python.shaded.*` to
avoid clashes with user-provided versions. The Beam SDKs include their own
vendored gRPC at `org.apache.beam.vendor.grpc.v1p60p1`.

### Python (`setup.py` `install_requires`)
- `py4j==0.10.9.7`, `apache-beam>=2.54.0,<=2.61.0`, `protobuf>=3.19.0`,
  `cloudpickle>=2.2.0`, `pyarrow>=5.0.0,<21.0.0`,
  `pandas>=1.3.0,<2.3`, `numpy>=1.22.4`, `avro`, `fastavro`,
  `pemja>=0.5.6,<0.5.7` (non-Windows), `ruamel.yaml`,
  `apache-flink-libraries==<same-version>`.
- Python `>=3.9` (3.9 / 3.10 / 3.11 / 3.12 are CI-tested via tox).
- Beam version is tightly pinned — Beam's SDK harness is what the worker
  process actually runs.

### Used by
`flink-dist` bundles `pyflink.zip` (the Python sources, see `pom.xml`
`generate-resources`) and the shaded jar.

## Source layout
- `src/main/java/org/apache/flink/python/...` — config, env managers,
  metric containers, dependency utils
  - `env/process/` (`ProcessPythonEnvironmentManager`) — manages external
    Python worker subprocesses
  - `env/embedded/` (`EmbeddedPythonEnvironmentManager`) — manages PemJa
    embedded interpreters in thread mode
  - `metric/process/` and `metric/embedded/` — corresponding metric bridges
- `src/main/java/org/apache/flink/streaming/api/operators/python/...` —
  DataStream-side Python operators
  - `process/Abstract*ExternalPythonFunctionOperator` (default, Beam-based)
  - `embedded/Abstract*EmbeddedPythonFunctionOperator` (PemJa-based)
  - one operator per fan-in/keyed/broadcast combination (Co, Keyed,
    BatchKeyedCoBroadcast, Window…)
- `src/main/java/org/apache/flink/streaming/api/runners/python/beam/` —
  `BeamPythonFunctionRunner` (the bridge into Beam's `JobBundleFactory` /
  `StageBundleFactory` / `RemoteBundle`), `BeamDataStreamPythonFunctionRunner`,
  `PythonSharedResources`, `state/Beam*StateHandler` (forwards Beam state
  requests to Flink keyed/operator state)
- `src/main/java/org/apache/flink/table/runtime/operators/python/...` —
  Table-side Python operators
  - `scalar/PythonScalarFunctionOperator`, `scalar/arrow/...` (Pandas UDFs),
    `scalar/async/...` (async scalar UDFs), `scalar/EmbeddedPythonScalarFunctionOperator`
  - `table/PythonTableFunctionOperator` (UDTFs)
  - `aggregate/PythonStreamGroupAggregateOperator`,
    `PythonStreamGroupWindowAggregateOperator`,
    `PythonStreamGroupTableAggregateOperator`, `aggregate/arrow/...`
- `src/main/java/org/apache/flink/table/runtime/arrow/` —
  `ArrowReader` / `ArrowWriter` / `ArrowUtils` and per-type writers/vectors
  bridging Flink `RowData` ↔ Arrow batches.
- `src/main/java/org/apache/flink/table/runtime/typeutils/serializers/python/`
  — serializers used on the wire between Java and Python (Row, BigDec,
  String, Date/Time, etc.).
- `src/main/java/org/apache/flink/client/python/` — `PythonDriver`
  (Java entry point for `flink run -py`), `PythonGatewayServer` (Py4J),
  `PythonEnvUtils`, `PythonFunctionFactoryImpl` (loads a Python function
  by name from Java SQL).
- `src/main/java/org/apache/beam/...` — copies/patches of Beam classes
  (e.g. `beam/runners/fnexecution/control/...`, `beam/sdk/fn/server/...`,
  vendored gRPC compat shims under `org.apache.beam.vendor.grpc.v1p60p1`)
  that PyFlink needs to override for its runner semantics.
- `pyflink/proto/flink-fn-execution.proto` — Flink-specific FnAPI extensions
  (UDF spec, coder spec). Compiled to Java (`org.apache.flink.fnexecution.v1`)
  via `protoc-jar-maven-plugin`, and to Python (`flink_fn_execution_pb2.py`).

- `pyflink/` — the Python package
  - `pyflink/common/` — `Configuration`, `Row`, `Types`/`TypeInformation`,
    `WatermarkStrategy`, `Time`, serializers
  - `pyflink/datastream/` — DataStream API: `StreamExecutionEnvironment`,
    `DataStream`, `KeyedStream`, `ConnectedStreams`, `WindowedStream`,
    `functions.py` (Map/FlatMap/KeyedProcess/Window/Broadcast functions),
    `state.py`, `checkpoint_config.py`, `connectors/` (Kafka, Pulsar,
    Kinesis, JDBC, Cassandra, RabbitMQ, Elasticsearch, Files), `formats/`
  - `pyflink/table/` — Table API: `TableEnvironment`, `Table`,
    `expressions.py`, `udf.py` (`@udf`, `@udtf`, `@udaf`, `@udtaf`,
    `AsyncScalarFunction`), `catalog.py`, `descriptors.py`, `types.py`,
    `compiled_plan.py`, `statement_set.py`
  - `pyflink/fn_execution/` — Python worker / Beam portability glue
    - `beam/` — Beam SDK harness extensions: `beam_boot.py` (worker bootstrap,
      invoked by `pyflink-udf-runner.sh`), `beam_sdk_worker_main.py`
      (entry point that registers PyFlink coders/operations into the Beam
      SDK harness and patches `sdk_worker_main`), `beam_worker_pool_service.py`
      (loopback worker pool used by `MiniCluster` for local execution),
      `beam_coders.py`, `beam_operations.py`, `beam_coder_impl_*` /
      `beam_stream_fast.pyx` (Cython hot-path)
    - `embedded/` — PemJa-side execution: `operations.py`, `converters.py`,
      `state_impl.py`, `java_utils.py`
    - `datastream/` — Python implementations of DataStream operators
      (`process/`, `embedded/`, `window/`, `timerservice.py`)
    - `table/` — Python implementations of Table operators:
      `aggregate_*` (slow + Cython fast), `window_aggregate_*`,
      `window_assigner.py`, `window_trigger.py`, `state_data_view.py`,
      `async_function/`
    - `coder_impl_fast.pyx` / `coder_impl_slow.py`, `stream_fast.pyx` /
      `stream_slow.py`, `pickle.py`, `internal_state.py`,
      `flink_fn_execution_pb2.py` (generated)
    - `metrics/` — `MetricGroup` shims to Java metrics over Beam Fn API
  - `pyflink/java_gateway.py` — bootstraps a Py4J `JavaGateway` to a JVM
    that runs `PythonGatewayServer`, importing Flink classes for use from
    Python.
  - `pyflink/pyflink_gateway_server.py` — launches the JVM with Flink's
    classpath (for local / `flink run`-less execution).
  - `pyflink/pyflink_callback_server.py` — JVM-to-Python callback channel.
  - `pyflink/find_flink_home.py`, `pyflink/version.py`, `pyflink/shell.py`,
    `pyflink/serializers.py` (pickle), `pyflink/metrics/`, `pyflink/util/`,
    `pyflink/testing/`, `pyflink/examples/`.
  - `pyflink/bin/pyflink-udf-runner.{sh,bat}` — the script Beam invokes to
    spawn a Python SDK worker: simply `python -m pyflink.fn_execution.beam.beam_boot $@`.

## Architecture & key concepts

### Beam portability (process mode)
PyFlink reuses Apache Beam's portability framework as the wire protocol
between Java operators and Python workers. `BeamPythonFunctionRunner` builds
an `ExecutableStage` containing exactly one PTransform (the Python UDF) and
hands it to Beam's `DefaultJobBundleFactory`, which:
1. Asks `ProcessPythonEnvironmentManager` for a `PythonEnvironment` — this
   stages user files, sets `PYTHONPATH`, then invokes
   `pyflink-udf-runner.sh` (which launches `python -m
   pyflink.fn_execution.beam.beam_boot`).
2. The Python worker connects back to Beam-managed gRPC services (control,
   data, state, logging, provision) hosted by the JVM operator.
3. For each input bundle, the Java operator pushes serialized rows onto
   the data channel; the Python worker invokes the UDF and writes results
   back. Bundle boundaries are driven by `python.fn-execution.bundle.size`
   (default 1000) and `python.fn-execution.bundle.time` (1000 ms).

`PythonSharedResources` allows multiple operators in the same TaskManager
slot to share a single Python worker process when they belong to the same
chain — controlled by `python.operator-chaining.enabled`.

### Embedded mode (`python.execution-mode = thread`)
`EmbeddedPythonEnvironmentManager` uses PemJa to load a CPython interpreter
into the JVM. UDF invocation skips serialization and gRPC, calling directly
via JNI — much lower latency, but loses process isolation and is not
supported for every operator (it falls back to process mode in unsupported
cases).

### Java operator side
- `AbstractPythonFunctionOperator` is the common base. It manages bundle
  lifecycle: `processElement` accumulates rows, `invokeFinishBundle()` flushes
  on size/time, on checkpoint barriers, on watermarks, and on shutdown.
- DataStream operators live under
  `streaming/api/operators/python/{process,embedded}/` — one class per
  variant (Process / KeyedProcess / CoProcess / KeyedCoProcess /
  BatchCoBroadcast / BatchKeyedCoBroadcast / Window).
- Table operators live under
  `table/runtime/operators/python/{scalar,table,aggregate}/`; the `aggregate`
  ones own keyed state and forward state ops over the Beam state channel
  via `BeamStateRequestHandler` + `BeamKeyedStateStore`.

### Type system bridging
- DataStream: Python objects are pickled via cloudpickle (`pyflink/serializers.py`).
- Table: column-wise conversion. Each Flink `LogicalType` has a Python
  serializer in `table/runtime/typeutils/serializers/python/` and a
  Python coder in `pyflink/fn_execution/coder_impl_*` (Cython fast path
  plus pure-Python slow fallback). The Cython `.pyx` files are compiled at
  wheel-build time; `.c` files shipped in the sdist are used when Cython
  is unavailable.
- Vectorized (Pandas) UDFs: rows are batched into Arrow `RecordBatch`es
  via `ArrowWriter`, sent as bytes, decoded into Pandas DataFrames on the
  Python side, processed, and the result encoded back.

### Python program submission
- `flink run -py user.py` invokes `PythonDriver` (Java main), which starts a
  Py4J `GatewayServer`, spawns the user's Python interpreter, and waits while
  the Python script builds and executes the job via the gateway.
- Inside Python, `pyflink.java_gateway.get_gateway()` returns the Py4J
  `JavaGateway`; if `PYFLINK_GATEWAY_PORT` is not set (i.e. running outside
  `flink run`, e.g. `python user.py`), `launch_gateway()` spins up the JVM
  itself via `pyflink_gateway_server.py`.

## How to build PyFlink
- **Java side**: `mvn install -pl flink-python -am` builds and shades the jar.
  The `generate-resources` phase zips the `pyflink/` directory into
  `lib/pyflink.zip` for `flink-dist`. The `protoc-jar-maven-plugin` step
  compiles `pyflink/proto/flink-fn-execution.proto`.
- **Python wheel**: `cd flink-python && python setup.py sdist bdist_wheel`
  (or use `dev/build-wheels.sh`). `setup.py` detects whether it is running
  inside a Flink source tree (`in_flink_source`); if so it pulls
  `flink-dist`'s `bin/` and `conf/` for inclusion in the wheel. Cython
  extensions are built when Cython is available.
- **Lint / test**: `dev/lint-python.sh` (flake8, mypy, sphinx checks),
  `dev/integration_test.sh`, `tox` for matrix testing across Python versions.

## Important public APIs
- `pyflink.datastream.StreamExecutionEnvironment` — `get_execution_environment()`,
  `add_source` / `from_source`, `add_sink` / `sink_to`, `execute()`,
  `enable_checkpointing()`, `set_state_backend()`.
- `pyflink.datastream.DataStream` / `KeyedStream` / `WindowedStream` —
  `map`, `flat_map`, `filter`, `key_by`, `process`, `window`, `reduce`,
  `connect`, `broadcast`, `union`, `co_process`.
- `pyflink.datastream.functions` — `MapFunction`, `FlatMapFunction`,
  `FilterFunction`, `KeyedProcessFunction`, `CoProcessFunction`,
  `BroadcastProcessFunction`, `WindowFunction`, `AggregateFunction`,
  `ReduceFunction`, `SourceFunction`/`SinkFunction`.
- `pyflink.table.TableEnvironment` — `create(EnvironmentSettings)`,
  `from_path`, `sql_query`, `execute_sql`, `from_pandas`, `to_pandas`,
  `create_temporary_function`, `register_function`.
- `pyflink.table.udf` — `@udf`, `@udtf`, `@udaf`, `@udtaf` decorators;
  `ScalarFunction`, `TableFunction`, `AggregateFunction`,
  `TableAggregateFunction`, `AsyncScalarFunction` base classes.
- Pandas-vectorized variant: pass `func_type='pandas'` to `@udf` /
  `@udaf` for Arrow-batched execution.

## Internal flow: Python UDF invoked from a Java operator
1. JM compiles the job; Python UDFs are wrapped in `DataStreamPythonFunctionInfo`
   / `PythonFunctionInfo` and shipped to TMs as serialized blobs (cloudpickle
   payload + UDF spec proto).
2. TM instantiates the Java operator (`ExternalPythonProcessOperator` etc.);
   `open()` constructs a `BeamPythonFunctionRunner`.
3. `BeamPythonFunctionRunner.open()` builds the Beam `ExecutableStage`,
   asks `ProcessPythonEnvironmentManager.createEnvironment()` which
   materialises staging dirs and forms the Beam `PythonEnvironment` URL
   for the runner script.
4. Beam launches the worker (via `pyflink-udf-runner.sh` →
   `pyflink.fn_execution.beam.beam_boot` → Beam's `sdk_worker_main`).
5. Per bundle: operator buffers up to `bundle.size` records, calls
   `runner.process(bytes)` which writes to the data channel; the Python
   side decodes via the registered coder, invokes the UDF, encodes the
   result, sends it back. The Java side `pollResult()` drains and emits.
6. On checkpoint/finish: `flush()` blocks until the current bundle completes
   so state mutations land before the checkpoint barrier passes.

## Tests
- Java: standard `mvn test` (Surefire). Many integration tests use Python
  via `pyflink_gateway_server` — see `flink-sql-connector-*` deps in
  `pom.xml` listed as `Indirectly accessed in pyflink_gateway_server`.
- Python: `pytest` under `pyflink/{common,datastream,table,fn_execution}/tests/`.
  `tox.ini` drives multi-Python-version runs. `dev/integration_test.sh` runs
  the end-to-end matrix.

## Pitfalls & gotchas
- **Python version range is narrow**: 3.9-3.12. Don't assume newer features.
- **Beam version is tightly pinned** (`>=2.54.0,<=2.61.0`). Both the Java
  Beam jars and the Python `apache-beam` install must match — bumping one
  without the other will break the FnAPI wire protocol.
- **pyarrow upper bound** (`<21.0.0`) and **pandas upper bound** (`<2.3`)
  are real: Arrow batches are sized and laid out per the bundled Arrow 13;
  pandas 2.3 dropped cp39 wheels (see FLINK-38513).
- **Cython compile step**: `coder_impl_fast`, `stream_fast`,
  `beam_coder_impl_fast`, `beam_operations_fast`, table `aggregate_fast`
  and `window_aggregate_fast` are all `.pyx`. Missing C extensions fall
  back to slow pure-Python paths — perf will tank silently.
- **Process-mode UDFs are slow** vs Java: every record crosses a serialization
  + gRPC boundary. Use Pandas UDFs (Arrow-vectorized) or `thread` execution
  mode for hot paths.
- **Thread / embedded mode is not universal**: not all operators implement
  the embedded variant; PyFlink falls back to process mode silently if the
  required `Embedded*` operator class doesn't exist.
- **Py4J callback server**: when running under `flink run -py`, the JVM is
  the long-lived side; when running `python user.py` directly, the Python
  process is. The two paths differ in lifecycle and `PYFLINK_GATEWAY_DISABLED`
  / `PYFLINK_GATEWAY_PORT` env vars switch behavior.
- **`pyflink.zip` packaging**: the `pom.xml` `generate-resources` step zips
  `pyflink/` into `lib/pyflink.zip`. If you edit Python files without
  rebuilding, distributed jobs will see stale code (the script
  `pyflink-udf-runner.sh` has a `FLINK_TESTING=1` short-circuit that
  prepends source `pyflink/` to `PYTHONPATH`).
- **Shaded Beam classes under `org.apache.beam.*` inside this module**:
  PyFlink ships modified copies of some Beam internals (e.g.
  `beam/runners/fnexecution/control/`, vendored gRPC under
  `org.apache.beam.vendor.grpc.v1p60p1/`). Don't replace these without
  understanding the deltas vs upstream Beam.
- **Memory accounting**: `python.fn-execution.memory.managed` (default
  `true`) charges the Python worker against Flink managed memory. If `false`,
  you must size task off-heap memory yourself.

## Related modules / docs
- `flink-table-api-python` integration uses this module's
  `PythonFunctionFactoryImpl` to load Python functions from Java SQL
  (`CREATE FUNCTION ... LANGUAGE PYTHON`).
- `flink-streaming-java` provides the operator base classes
  (`AbstractStreamOperator`) and `Transformation` infra that the Python
  operators specialise.
- `flink-table-runtime` provides `RowData`, the in-memory format Python
  Table operators convert to/from.
- `flink-dist` bundles `pyflink.zip` and the shaded jar so they're picked
  up automatically by `flink run -py`.
- `apache-flink-libraries/` (sibling subdirectory) — the companion PyPI
  package that ships the Flink JARs needed at runtime; PyFlink's wheel
  depends on it via a version-locked install_requires entry.
