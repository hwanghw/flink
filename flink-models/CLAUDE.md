# `flink-models`

## Purpose

A **multi-module umbrella for Flink's built-in remote ML model connectors**.
Each child module implements the Flink SQL "model" SPI
(`org.apache.flink.table.factories.ModelProviderFactory`) for a specific
inference backend, exposing an `AsyncPredictFunction` that Flink SQL can
invoke via `CREATE MODEL` / `ML_PREDICT(...)`.

Today there are two child modules:

- **`flink-model-openai`** — OpenAI-compatible HTTP endpoints (chat
  completions + embeddings; usable with any OpenAI-API-compatible server).
- **`flink-model-triton`** — NVIDIA Triton Inference Server (KServe v2 REST
  protocol).

This is **not** related to PyFlink, FLIP-409 / DataStream V2, or the legacy
`flink-ml` library. It targets *Flink SQL's table/ML SPIs* in
`flink-table-common`.

## Where it fits — relationship to `flink-streaming-java` and the rest of the stack

This module sits entirely on top of the Flink **Table/SQL** stack — it has
no relationship to `flink-streaming-java`, `flink-datastream-api`, or
`flink-datastream`. The relevant integration surface is:

```
  Flink SQL job (CREATE MODEL ... WITH ('provider'='openai', ...);
                 SELECT ML_PREDICT(my_model, ...) FROM t;)
                          │
                          ▼
  flink-table-common
      ├── ModelProviderFactory     ◄── this module's *ModelProviderFactory implements
      ├── AsyncPredictFunction     ◄── this module's *ModelFunction extends
      └── AsyncPredictRuntimeProvider / ModelProvider
                          │
                          ▼
  flink-models (THIS MODULE)
      ├── flink-model-openai       → openai-java client
      └── flink-model-triton       → OkHttp + Jackson, Triton KServe v2 REST
                          │
                          ▼
              External HTTP inference server
```

The child modules are discovered at runtime through Java's `ServiceLoader`
via files in `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`,
matched by `factoryIdentifier()`: `"openai"` or `"triton"`.

## Maven coordinates

Parent (aggregator, `<packaging>pom</packaging>`):

```xml
<groupId>org.apache.flink</groupId>
<artifactId>flink-models</artifactId>
<version>2.3-SNAPSHOT</version>
```

Children (each `<packaging>jar</packaging>`, shaded fat jar):

- `org.apache.flink:flink-model-openai:2.3-SNAPSHOT`
- `org.apache.flink:flink-model-triton:2.3-SNAPSHOT`

## Dependencies

### Parent

- `org.slf4j:slf4j-api` (provided) — only thing the parent declares.

### `flink-model-openai`

- `com.openai:openai-java:1.6.1` (bundled, optional) — async REST client.
- `com.knuddels:jtokkit:1.1.0` (bundled, optional) — tokenizer for context
  truncation when `max-context-size` is set.
- `flink-core`, `flink-table-api-java`, `flink-table-common` — *provided*
  (assumed present in the Flink runtime/classpath).
- Test: `flink-table-planner_${scala.binary.version}`, `mockwebserver`,
  `flink-table-api-java-bridge`, `flink-clients`, `flink-test-utils-junit`.
- Shade plugin: relocates `com.fasterxml.jackson` →
  `org.apache.flink.model.openai.com.fasterxml.jackson`,
  `org.apache.httpcomponents` → `...openai.org.apache.httpcomponents`,
  `com.squareup` → `...openai.com.squareup` to avoid clashes with Flink's
  bundled Jackson and downstream HTTP libraries.

### `flink-model-triton`

- `com.squareup.okhttp3:okhttp:4.12.0` (bundled, optional).
- `com.fasterxml.jackson.core:jackson-{core,databind,annotations}:2.15.2`
  (bundled, optional).
- `flink-core`, `flink-table-api-java`, `flink-table-common` — *provided*.
- Test: same set as openai plus `gson`.
- Shade plugin: relocates `com.fasterxml.jackson` →
  `org.apache.flink.model.triton.com.fasterxml.jackson`,
  `com.squareup` → `...triton.com.squareup`.

### Used by

Flink SQL jobs / Table API users that reference these factory identifiers
when defining a `MODEL` in a catalog. No other Flink module depends on
them at compile time.

## Source layout

```
flink-models/
├── pom.xml                                  aggregator (modules = openai, triton)
├── flink-model-openai/
│   ├── pom.xml                              shaded; relocates jackson/squareup/httpcomponents
│   └── src/main/java/org/apache/flink/model/openai/
│       ├── OpenAIModelProviderFactory       SPI entry; factoryIdentifier() = "openai"
│       ├── AbstractOpenAIModelFunction      shared base (retry / errorHandling / contextOverflow)
│       ├── OpenAIChatModelFunction          ENDPOINT_SUFFIX = "chat/completions"
│       ├── OpenAIEmbeddingModelFunction     ENDPOINT_SUFFIX = "embeddings"
│       ├── OpenAIOptions                    ConfigOption catalog (endpoint, api-key, model, ...)
│       ├── ContextOverflowAction            TRUNCATED_TAIL / TRUNCATED_HEAD / ...
│       └── OpenAIUtils                      misc helpers
│   ├── src/main/resources/META-INF/services/
│   │     org.apache.flink.table.factories.Factory   → OpenAIModelProviderFactory
│   └── src/test/java/...                    OpenAIChatModelTest, OpenAIEmbeddingModelTest,
│                                            ContextOverflowActionTest,
│                                            ModelFunctionErrorHandlingStrategyTest (uses MockWebServer)
└── flink-model-triton/
    ├── pom.xml                              shaded; relocates jackson/squareup
    └── src/main/java/org/apache/flink/model/triton/
        ├── TritonModelProviderFactory       SPI entry; factoryIdentifier() = "triton"
        ├── TritonInferenceModelFunction     KServe v2 REST predict
        ├── AbstractTritonModelFunction      shared base
        ├── TritonOptions                    ConfigOption catalog (endpoint, model-name, version, ...)
        ├── TritonDataType, TritonTypeMapper Flink LogicalType <-> Triton dtype mapping
        ├── TritonUtils
        └── exception/
            ├── TritonException              base
            ├── TritonClientException, TritonNetworkException
            ├── TritonSchemaException, TritonServerException
    ├── src/main/resources/META-INF/services/
    │     org.apache.flink.table.factories.Factory   → TritonModelProviderFactory
    └── src/test/java/...                    TritonModelProviderFactoryTest, TritonTypeMapperTest
```

## Architecture & key concepts

- **SPI shape.** Each child registers a `ModelProviderFactory` via
  `ServiceLoader`. `createModelProvider(context)` reads
  `context.getCatalogModel()` (input/output schema + options), validates
  options through `FactoryUtil.createModelProviderFactoryHelper`, and
  returns an `AsyncPredictRuntimeProvider` wrapping a per-model
  `AsyncPredictFunction`.
- **AsyncPredictFunction.** `AsyncPredictFunction` (in
  `flink-table-common`) extends `AsyncTableFunction<RowData>`. The child
  modules implement `predict(RowData) → CompletableFuture<Collection<RowData>>`,
  so inference is **non-blocking** at the Flink operator level.
- **OpenAI endpoint dispatch.** `OpenAIModelProviderFactory` inspects the
  configured `endpoint` URL: a `/embeddings`-suffixed endpoint
  instantiates `OpenAIEmbeddingModelFunction`, a
  `/chat/completions`-suffixed endpoint instantiates
  `OpenAIChatModelFunction`. Anything else throws.
- **Retry, error handling, context overflow (OpenAI).**
  `AbstractOpenAIModelFunction` centralizes:
  - `error-handling-strategy` ∈ {RETRY, FAIL, ...}
  - `retry-num` (default 100 — rate-limit defensive)
  - `retry-fallback-strategy`
  - `max-context-size` + `context-overflow-action` (TRUNCATED_TAIL by
    default; uses jtokkit to count tokens).
- **Triton typed payload.** `TritonTypeMapper` maps Flink `LogicalType`s
  (including `ArrayType`) to Triton dtype strings. Optional
  `flatten-batch-dim`, gzip `compression`, sequence-batch fields
  (`sequence-id` / `sequence-start` / `sequence-end`), `priority`,
  `auth-token`, and `custom-headers` are exposed via `TritonOptions`.
- **Shading.** Both modules build *shaded fat jars*: dependencies are
  bundled (`<optional>true</optional>` on direct deps lets users skip them
  when not using these connectors) and their internal Jackson / OkHttp /
  Apache HttpComponents packages are relocated under
  `org.apache.flink.model.{openai|triton}.*` to prevent collisions with
  Flink's own bundled Jackson and with user code.

## Important public APIs

The only public API surface is the **option set** consumed by SQL DDL.
Java-level entry points are factories used by `ServiceLoader`:

- `OpenAIModelProviderFactory.IDENTIFIER = "openai"`
  - Required: `OpenAIOptions.ENDPOINT`, `API_KEY`, `MODEL`.
  - Optional: `MAX_CONTEXT_SIZE`, `CONTEXT_OVERFLOW_ACTION`,
    `ERROR_HANDLING_STRATEGY`, `RETRY_NUM`, `RETRY_FALLBACK_STRATEGY`,
    `SYSTEM_PROMPT`, `TEMPERATURE`, `TOP_P`, `STOP`, `MAX_TOKENS`,
    `PRESENCE_PENALTY`, `N`, `SEED`, `RESPONSE_FORMAT`, `DIMENSION`.
- `TritonModelProviderFactory.IDENTIFIER = "triton"`
  - Required: `TritonOptions.ENDPOINT`, `MODEL_NAME`.
  - Optional: `MODEL_VERSION`, `TIMEOUT`, `FLATTEN_BATCH_DIM`, `PRIORITY`,
    `SEQUENCE_ID`, `SEQUENCE_START`, `SEQUENCE_END`, `COMPRESSION`,
    `AUTH_TOKEN`, `CUSTOM_HEADERS`.

Typical SQL usage (illustrative):

```sql
CREATE MODEL gpt
  INPUT (prompt STRING)
  OUTPUT (response STRING)
  WITH ('provider'='openai',
        'endpoint'='https://api.openai.com/v1/chat/completions',
        'api-key'='sk-...',
        'model'='gpt-4o-mini');

SELECT ML_PREDICT(MODEL gpt, prompt_col) FROM tbl;
```

## Internal flows

1. **Discovery.** Flink Table planner loads
   `org.apache.flink.table.factories.Factory` via `ServiceLoader`, finds
   `OpenAIModelProviderFactory` / `TritonModelProviderFactory` by their
   `factoryIdentifier()`.
2. **Provider construction.** `createModelProvider(context)` validates
   options, instantiates the right `*ModelFunction`, wraps it in a
   `Provider` (`AsyncPredictRuntimeProvider`) and returns it.
3. **Function open.** At task startup, Flink calls
   `AsyncPredictFunction.open(FunctionContext)`. The OpenAI base builds an
   `OpenAIClientAsync`; the Triton base builds an OkHttp `Call.Factory`.
4. **Per-record predict.** For each input row, `predict(RowData)` builds
   the request (Chat / Embedding / Triton tensor), submits asynchronously,
   and returns a `CompletableFuture<Collection<RowData>>`. The async
   runtime in `flink-table-runtime` orders / buffers results.
5. **Error handling (OpenAI).** Failures pass through
   `ErrorHandlingStrategy.RETRY` (with `retry-num`) or `FAIL`; on retry
   exhaustion, the `RetryFallbackStrategy` decides whether to emit an
   error row or fail the task.

## Tests

- **`flink-model-openai`:** `OpenAIChatModelTest`,
  `OpenAIEmbeddingModelTest`, `ContextOverflowActionTest`,
  `ModelFunctionErrorHandlingStrategyTest`. All use `okhttp3.mockwebserver`
  to stub the OpenAI HTTP endpoint and `flink-table-planner` to drive
  end-to-end SQL.
- **`flink-model-triton`:** `TritonModelProviderFactoryTest`,
  `TritonTypeMapperTest`. Schema-mapping and factory wiring are
  exhaustively tested; full network integration is left to manual /
  external testing.

## Pitfalls & gotchas

- **Not related to DataStream V2 or PyFlink.** Despite the *"new module
  in Flink 2.x"* framing, `flink-models` lives entirely in the
  Table/SQL/ML SPI world. It does not import anything from
  `flink-streaming-java`, `flink-datastream-api`, or `flink-datastream`.
- **`<packaging>pom</packaging>` on the parent.** The aggregator has no
  classes — depending on `flink-models` directly does nothing. Depend on
  `flink-model-openai` or `flink-model-triton` explicitly.
- **Provided Flink dependencies.** All `flink-core` / `flink-table-*`
  deps are `<scope>provided</scope>`. A fat-jar build that strips
  "provided" scope will produce a jar that fails to load Flink classes at
  runtime — only the third-party deps are bundled (and relocated).
- **Bundled HTTP/JSON libraries are shaded.** Don't try to cast a
  `com.fasterxml.jackson.databind.JsonNode` produced inside this module
  to one from your own classpath; the package is
  `org.apache.flink.model.{openai|triton}.com.fasterxml.jackson.databind.JsonNode`.
- **OpenAI compatibility, not OpenAI-only.** `flink-model-openai` works
  against any OpenAI-API-compatible server (Azure OpenAI, vLLM, Ollama in
  OpenAI mode, etc.) — the `endpoint` URL drives chat-vs-embeddings
  dispatch by suffix match.
- **`forced-central` repository.** `flink-model-openai`'s pom forces
  `https://repo1.maven.org/maven2` because `com.openai:openai-java` was
  not yet mirrored to RedHat/Google mirrors at the time of writing. Air-
  gapped builds need that artifact pre-fetched.
- **`AsyncPredictFunction` requires async-capable runtime.** The function
  emits `CompletableFuture<Collection<RowData>>` — wiring it into a job
  that disables async I/O will not work; the Flink ML SPI assumes the
  async runtime provider path.
- **Single physical output column.** `OpenAIChatModelFunction` enforces
  one and only one physical output column (the rest must be metadata
  columns such as `error-message`). Violations throw at construction.

## Related modules / docs

- `flink-table/flink-table-common` — defines `ModelProviderFactory`,
  `AsyncPredictFunction`, `ModelProvider`, `AsyncPredictRuntimeProvider`.
- `flink-table/flink-table-api-java`, `flink-table/flink-table-planner` —
  required at compile time (provided) / runtime; planner is used as a
  test dependency.
- Flink SQL `CREATE MODEL` / `ML_PREDICT` documentation — primary user-
  facing surface for this module.
- OpenAI Java SDK (`com.openai:openai-java`), KServe v2 REST predict
  protocol for Triton — external references for the request/response
  shapes.
