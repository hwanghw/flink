# `flink-queryable-state`

**Status: deprecated since Flink 1.18; scheduled for removal in a future
major release.** Don't build new functionality on this — use a sink to an
external KV store (HBase, Cassandra, RocksDB-as-a-service, …) instead.

Parent Maven aggregator pom for the legacy "Queryable State" feature that
lets external clients ask a TaskManager for the current value of a keyed
state slot, without going through a sink.

## Purpose

- Aggregator-only (`<packaging>pom</packaging>`) — produces no jar of its own.
- Provides a small Netty-based RPC stack (client / proxy / server) plus
  immutable read-only views of `ValueState`, `ListState`, `MapState`,
  `ReducingState`, `AggregatingState`.
- Lets a user mark a state descriptor as queryable
  (`StateDescriptor#setQueryable(name)`) and then read it from outside the
  cluster through `QueryableStateClient`.
- Loaded by `flink-runtime` reflectively (see `QueryableStateUtils`), so the
  feature is optional: drop the jars in `lib/` to enable, leave them in
  `opt/` to disable.

## Where it fits

```
QueryableStateClient (user JVM, outside the cluster)
        |  Netty TCP, KvStateRequest / KvStateResponse
        v
KvStateClientProxy  (one per TaskManager; port = queryable-state.proxy.ports, default 9069)
        |  resolves KvStateLocation via JobManager (KvStateLocationOracle)
        |  Netty TCP, KvStateInternalRequest / KvStateResponse
        v
KvStateServer       (one per TaskManager; port = queryable-state.server.ports, default 9067)
        |  KvStateRegistry.getKvState(id) -> InternalKvState
        v
RocksDB / Heap keyed state backend
```

Two Netty servers run on every TaskManager when the feature is enabled
(`queryable-state.enable = true`):

- the **client proxy** is the user-facing entry point;
- the **state server** is the TM-internal endpoint that reads from
  `KvStateRegistry`.

Both extend `AbstractServerBase` from `flink-queryable-state-client-java`.

## Maven coordinates

- Group: `org.apache.flink`
- Artifact: `flink-queryable-state`
- Version: `2.3-SNAPSHOT`
- Parent: `flink-parent`
- Packaging: `pom`

### Sub-modules

| Sub-module | Purpose |
| ---------- | ------- |
| `flink-queryable-state-client-java` | The client library: `QueryableStateClient`, immutable state views, Netty plumbing (`Client`, `ClientHandler`, `AbstractServerBase`, `AbstractServerHandler`), message types, request stats. Used both by external users *and* — surprisingly — by the runtime (the proxy/server extend its abstract servers). |
| `flink-queryable-state-runtime` | The TM-side implementations: `KvStateServerImpl` (handles internal requests), `KvStateClientProxyImpl` (handles external requests, talks to JM + KvStateServer). Loaded by reflection from `flink-runtime/QueryableStateUtils`. |

A `flink-state-client-scala` module is referenced in the parent pom but
commented out — Scala support has been dropped.

## Dependencies

### Direct

- `flink-queryable-state-client-java`: `flink-core` (for serializers,
  `JobID`, `ExecutionConfig`), Netty (transitive via `flink-runtime` in
  tests; in production the client bundles its own), `slf4j-api`,
  `findbugs/jsr305`.
- `flink-queryable-state-runtime`: `flink-core` (`provided`),
  `flink-runtime` (`provided`), `flink-queryable-state-client-java`
  (`provided`). Tests pull in `flink-statebackend-rocksdb` and
  `curator-test` for end-to-end ITCases.

### Used by

- `flink-runtime` declares a `provided` dep on
  `flink-queryable-state-client-java` so `KvStateRegistry`,
  `KvStateClientProxy`, `KvStateServer`, `QueryableStateUtils` (in
  `org.apache.flink.runtime.query`) can reference its abstractions
  (`KvStateID`, `KvStateRequestStats`, `MessageSerializer`).
- `flink-dist` packages both sub-modules into `opt/` (move them to `lib/`
  to enable the feature).
- `flink-end-to-end-tests/flink-queryable-state-test` exercises the round
  trip in an actual cluster.

## Source layout

```
flink-queryable-state/
  flink-queryable-state-client-java/
    org.apache.flink.queryablestate
      KvStateID.java                                          # UUID identifying a registered state
      client/
        QueryableStateClient.java                             # @Deprecated public entry point
        VoidNamespace*.java                                   # placeholder namespace for non-windowed state
        state/Immutable{Value,List,Map,Reducing,Aggregating}State.java  # read-only views
        state/serialization/KvStateSerializer.java
      exceptions/Unknown{KvStateId,KvStateKeyGroupLocation,Location,KeyOrNamespace}Exception.java
      messages/KvStateRequest.java                            # external request type
      messages/KvStateResponse.java
      network/
        AbstractServerBase.java                               # Netty bootstrap shared by proxy + server
        AbstractServerHandler.java                            # ChannelInboundHandler base
        Client.java, ClientHandler.java, ClientHandlerCallback.java
        ServerConnection.java, ChunkedByteBuf.java, NettyBufferPool.java
        messages/MessageBody.java, MessageType.java,
                 MessageSerializer.java, MessageDeserializer.java, RequestFailure.java
        stats/KvStateRequestStats.java, AtomicKvStateRequestStats.java, DisabledKvStateRequestStats.java
  flink-queryable-state-runtime/
    org.apache.flink.queryablestate
      messages/KvStateInternalRequest.java                    # proxy -> server request type
      client/proxy/
        KvStateClientProxyImpl.java                           # external Netty server (the "proxy")
        KvStateClientProxyHandler.java                        # resolves location + forwards to server
      server/
        KvStateServerImpl.java                                # internal Netty server (the "server")
        KvStateServerHandler.java                             # reads InternalKvState via KvStateRegistry
```

Counterpart classes in `flink-runtime/.../runtime/query/` (not in this
module but tightly coupled):
`KvStateRegistry`, `KvStateLocationRegistry`, `KvStateLocation`,
`KvStateEntry`, `KvStateInfo`, `KvStateRegistryListener`, `TaskKvStateRegistry`,
`KvStateClientProxy` (interface), `KvStateServer` (interface),
`UnknownKvStateLocation`, `QueryableStateUtils`.

## Architecture & key concepts

- **Two-tier Netty server.** `AbstractServerBase` is a Netty-based
  request/response server bound to a port range, used by both
  `KvStateClientProxyImpl` and `KvStateServerImpl`. Each instance has its
  own event-loop thread pool (`queryable-state.{proxy,server}.network-threads`)
  and query thread pool (`queryable-state.{proxy,server}.query-threads`).
- **Message framing.** `MessageSerializer` / `MessageDeserializer` produce a
  4-byte length prefix, a `MessageType` byte, and a `MessageBody`-serialized
  payload. `KvStateRequest`/`Response` (external) and
  `KvStateInternalRequest`/`Response` (internal) differ in what they carry;
  the proxy translates between them.
- **Location resolution.** On the first request for a `(jobId, registrationName)`
  the proxy asks the JobManager via `KvStateLocationOracle` for a
  `KvStateLocation` (which maps key-groups → `(KvStateID, host, port)`).
  Locations are cached; lookup failures throw `UnknownLocationException`
  family of exceptions.
- **State registration.** When a keyed state backend creates an `InternalKvState`
  for a descriptor marked queryable, it calls
  `TaskKvStateRegistry.registerKvState(...)` which assigns a `KvStateID` and
  notifies `KvStateRegistryListener` (`RpcKvStateRegistryListener` in the
  TM forwards the registration to the JM).
- **Read path.** `KvStateServerHandler.handleRequest` looks up the
  `KvStateEntry` by `KvStateID`, sets the key + namespace on the
  `InternalKvState`, and calls its
  `getSerializedValue(serializedKeyAndNamespace, ...)` — reusing the same
  serializer the backend uses for snapshots.
- **Immutable views on the client.** Responses are deserialized into
  `Immutable{Value,List,Map,Reducing,Aggregating}State` so the caller gets
  the familiar `State` API even though there is no real backend.
- **Reflective loading.** `flink-runtime/.../query/QueryableStateUtils`
  reflectively loads `KvStateServerImpl` and `KvStateClientProxyImpl` by
  fully-qualified class name; if `flink-queryable-state-runtime` is not on
  the classpath the methods log a hint ("move from opt to lib") and return
  `null`.

## Important public APIs

- `org.apache.flink.queryablestate.client.QueryableStateClient` —
  `@Deprecated @PublicEvolving`. Construct with the proxy host/port and call
  `getKvState(jobId, queryableStateName, key, keyTypeInfo, stateDescriptor)`.
- `org.apache.flink.api.common.state.StateDescriptor#setQueryable(String)` —
  marks a descriptor as queryable under the given name (lives in
  `flink-core`).
- `org.apache.flink.configuration.QueryableStateOptions` (in `flink-core`,
  also `@Deprecated`): `queryable-state.enable`,
  `queryable-state.{proxy,server}.ports`,
  `queryable-state.{proxy,server}.{network,query}-threads`,
  `queryable-state.client.network-threads`.

## Internal flows

- **Request path (happy case).**
  1. Client calls `getKvState(jobId, name, key, …)`.
  2. Client serializes `(key, namespace)`, builds a `KvStateRequest`,
     sends it to the proxy via `Client` (Netty).
  3. Proxy (`KvStateClientProxyHandler`) resolves
     `KvStateLocation` via JM, computes the responsible key group, picks the
     `(host, port, kvStateID)`, and forwards a `KvStateInternalRequest`
     to that TM's `KvStateServer`.
  4. Server (`KvStateServerHandler`) looks up the `KvStateEntry`, calls
     `InternalKvState.getSerializedValue(...)` on the backend, and replies
     with a `KvStateResponse` carrying the bytes.
  5. Proxy forwards the response back to the client; the client
     deserializes into an `Immutable*State` of the matching kind.
- **Failure paths.** `UnknownKvStateIdException`,
  `UnknownKvStateKeyGroupLocationException`,
  `UnknownKeyOrNamespaceException`, `UnknownLocationException`,
  `BadRequestException`. They all serialize as `RequestFailure` messages.

## Tests

- Client-side serializer tests: `VoidNamespaceTypeInfoTest`,
  `KvStateRequestSerializerTest`, `MessageSerializerTest`.
- Netty plumbing tests: `AbstractServerTest`, `KvStateServerTest`,
  `ClientTest`, `KvStateClientHandlerTest`, `KvStateServerHandlerTest`.
- RocksDB-specific serializer round-trip: `KVStateRequestSerializerRocksDBTest`.
- End-to-end ITCases on top of a mini-cluster:
  `AbstractQueryableStateTestBase` (subclassed per backend in
  `flink-end-to-end-tests/flink-queryable-state-test`).

## Pitfalls & gotchas

- **Deprecated.** Both `QueryableStateClient` and `QueryableStateOptions`
  carry `@Deprecated`. The maintainer guidance is to migrate to external
  sinks; expect this whole module to be deleted in a future major.
- **Disabled by default.** `queryable-state.enable = false` is the
  default. Even with the jars in `lib/`, the proxy/server won't start
  unless the option is set.
- **Jars must be in `lib/`, not `plugins/`.** `QueryableStateUtils` loads
  the runtime classes with the same classloader as `flink-runtime` (via
  `Class.forName`), not via the plugin manager. They must be on the main
  classpath.
- **No security / no auth.** The Netty servers expose anyone on the network
  to TM-internal state. Run behind a network policy.
- **Read-your-writes is not guaranteed.** The query reads whatever the
  backend has *right now* — possibly mid-update, possibly stale relative to
  a downstream sink. There is no checkpoint coordination.
- **Serializer compatibility is the user's problem.** The client must
  construct the same `TypeSerializer` as the operator. Schema evolution on
  the server side without matching client changes returns garbage or
  exceptions.
- **Port ranges and many TMs.** `queryable-state.proxy.ports` /
  `server.ports` accept ranges; large clusters benefit from a range to
  avoid bind collisions.
- **RocksDB sees a fresh `RocksIterator` per query.** Map state queries
  iterate the map; large maps under load are expensive and can stall the
  query thread pool.
- **`AbstractServerBase` lives in the *client* jar.** A bit of a layering
  oddity: the TM-internal servers extend a class from
  `flink-queryable-state-client-java`. This is why the client jar is
  `provided` on `flink-queryable-state-runtime`.

## Related modules / docs

- `flink-runtime/.../runtime/query/` — registry + interfaces consumed by
  this module (`KvStateRegistry`, `KvStateServer`, `KvStateClientProxy`,
  `QueryableStateUtils`).
- `flink-runtime/.../taskexecutor/{KvStateService,QueryableStateConfiguration}.java`
  — TaskExecutor wiring that starts the proxy + server.
- `flink-runtime/.../taskexecutor/rpc/RpcKvStateRegistryListener.java` —
  bridges TM registrations to the JM.
- `flink-core/.../configuration/QueryableStateOptions.java` — all
  `queryable-state.*` options.
- `flink-end-to-end-tests/flink-queryable-state-test` — full cluster ITs.
- The state backend deep-dives
  (`codedocs/flink-window-state-rocksdb-checkpoint-deep-dive.md`,
  `codedocs/flink-state-ttl-architecture.md`) describe the keyed state
  surface that `getSerializedValue` reads from.
