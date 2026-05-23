# `flink-rpc`

## Purpose

Provides the **RPC framework** that Flink uses internally for all
inter-component communication. Every long-lived Flink daemon
(JobManager, ResourceManager, Dispatcher, TaskManager, JobMaster) is an
**RpcEndpoint** that exposes a typed **RpcGateway** other components can
call asynchronously. The framework hides the transport (today an
Apache Pekko / former Akka actor system) behind a small set of Flink-
native interfaces so the production code never imports Pekko/Akka
classes directly.

## Where it fits

`flink-rpc` sits underneath `flink-runtime`. It does **not** depend on
runtime; runtime depends on it. The RPC system is the
"motherboard" that wires together leader election, slot allocation,
checkpointing coordination, task deployment, and metric reporting.
The Kubernetes / ZooKeeper HA deep-dive (`codedocs/flink-kubernetes-
ha-deep-dive.md`) shows how leader pointers stored in ConfigMaps
encode `akka.tcp://...` URLs that are produced by this module.

## Maven coordinates

Aggregator POM. `<artifactId>flink-rpc</artifactId>`, packaging `pom`.

### Sub-modules

| Sub-module               | Packaging | Purpose                                                                                                                              |
| ------------------------ | --------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `flink-rpc-core`         | jar       | Pure-Java interfaces and POJOs (`RpcGateway`, `RpcEndpoint`, `RpcService`, `RpcSystem`, message envelopes). No Pekko/Scala leaks.    |
| `flink-rpc-akka`         | jar       | The Pekko-backed `RpcSystem` implementation. Shaded into a fat jar; contains all Pekko, Scala, Aeron-excluded Pekko-Remote, Netty.  |
| `flink-rpc-akka-loader`  | jar       | Wraps the shaded `flink-rpc-akka.jar` as a classpath resource and loads it through a `SubmoduleClassLoader` at runtime.              |

The name `akka` is kept for git/compat reasons — the implementation
has been switched to Apache **Pekko 1.4.0** (after Lightbend
relicensed Akka). All new classes use the `pekko` package suffix,
e.g. `PekkoRpcService`, `PekkoRpcSystem`, `PekkoInvocationHandler`.

## Dependencies

### Direct (flink-rpc-core)

- `flink-core` (provided) — for `Configuration`, `AutoCloseableAsync`,
  `FlinkException`.
- `flink-test-utils-junit` (test).
- **No third-party transport** dependency.

### Direct (flink-rpc-akka)

- `flink-core` + `flink-rpc-core` (both provided).
- `org.apache.pekko:pekko-actor_2.12` 1.4.0.
- `org.apache.pekko:pekko-remote_2.12` 1.4.0 (Aeron UDP transport
  excluded — TCP only).
- `org.apache.pekko:pekko-slf4j_2.12`.
- `flink-shaded-netty` and `io.netty:netty-handler`, `netty-transport`.
- Pekko + Scala + Netty are **shaded** into the final artifact;
  `io.netty` is relocated to `org.apache.flink.shaded.netty4.io.netty`
  to avoid clashing with the runtime's own Netty.

### Direct (flink-rpc-akka-loader)

- `flink-core`, `flink-rpc-core`.
- `flink-rpc-akka` at **runtime + optional** scope — the maven-shade
  plugin copies its jar into the loader artifact at `prepare-package`
  with the fixed name `flink-rpc-akka.jar`. **No production code may
  reference flink-rpc-akka directly** (enforced via maven-enforcer in
  the parent pom, exempted only inside the loader itself).

### Used by

- `flink-runtime` — depends on `flink-rpc-core` and `flink-rpc-akka-
  loader`. Every gateway interface (`JobMasterGateway`,
  `TaskExecutorGateway`, `ResourceManagerGateway`,
  `DispatcherGateway`, `CheckpointCoordinatorGateway`) extends
  `RpcGateway`.
- `flink-test-utils-parent/flink-test-utils` — depends on the loader to
  bring up a local RPC system in integration tests.
- `flink-dist` transitively bundles `flink-rpc-akka.jar` so it ends up
  inside the assembled distribution.

## Source layout

```
flink-rpc-core/src/main/java/org/apache/flink/runtime/
├── concurrent/
│   ├── ClassLoadingUtils.java        # Wraps callbacks to set context CL
│   ├── ComponentMainThreadExecutor.java
│   └── ScheduledFutureAdapter.java
└── rpc/
    ├── RpcEndpoint.java              # Base class for every daemon
    ├── RpcGateway.java               # Marker for typed remote interfaces
    ├── RpcService.java               # Factory: connect, startServer, stop
    ├── RpcServer.java                # Per-endpoint message pump
    ├── RpcSystem.java                # Top-level: local/remote builder
    ├── RpcSystemLoader.java          # ServiceLoader SPI
    ├── FencedRpcEndpoint.java        # Adds leader fencing tokens
    ├── FencedRpcGateway.java
    ├── MainThreadExecutable.java     # runAsync / callAsync contract
    ├── exceptions/                   # FencingTokenException, etc.
    └── messages/
        ├── RpcInvocation.java         # Method-call envelope (interface)
        ├── LocalRpcInvocation.java    # In-JVM (no serialization)
        ├── RemoteRpcInvocation.java   # Wire-format (serializable args)
        ├── FencedMessage.java         # Wraps invocation + token
        └── RemoteHandshakeMessage.java

flink-rpc-akka/src/main/java/org/apache/flink/runtime/
├── concurrent/pekko/
│   ├── ActorSystemScheduledExecutorAdapter.java
│   └── ScalaFutureUtils.java
└── rpc/pekko/
    ├── PekkoRpcSystem.java            # Implements RpcSystem (SPI entry)
    ├── PekkoRpcService.java           # Implements RpcService
    ├── PekkoRpcServiceUtils.java      # Address parsing, name gen
    ├── PekkoRpcActor.java             # The actor wrapping an RpcEndpoint
    ├── FencedPekkoRpcActor.java       # Subclass that checks fencing tokens
    ├── PekkoInvocationHandler.java    # JDK Proxy → ask/tell on actor
    ├── FencedPekkoInvocationHandler.java
    ├── SupervisorActor.java           # Top-level supervisor with escalation
    ├── RobustActorSystem.java         # ActorSystem that escalates uncaught
    ├── DeadLettersActor.java          # Logs unreachable recipients
    ├── EscalatingSupervisorStrategy.java
    ├── PekkoUtils.java                # ActorSystem config / SSL hookup
    └── RpcSerializedValue.java        # Pre-serialized wire payload

flink-rpc-akka-loader/src/main/java/org/apache/flink/runtime/rpc/pekko/
└── PekkoRpcSystemLoader.java          # Extracts jar from classpath +
                                       # loads it in SubmoduleClassLoader
```

## Architecture & key concepts

### Gateway / Endpoint pair

A Flink component declares a typed gateway interface that extends
`RpcGateway` (e.g. `JobMasterGateway extends FencedRpcGateway<JobMasterId>`).
The endpoint class extends `(Fenced)RpcEndpoint` and implements the
gateway methods. Every method must return `CompletableFuture<T>`,
`void`, or be annotated `@Local` if it must run in-JVM only.

When `rpcService.startServer(endpoint)` is called, the framework spawns
a Pekko actor (a `PekkoRpcActor`), wires the endpoint instance into
it, and hands back an `RpcServer` proxy. Clients later call
`rpcService.connect(address, GatewayClass)` which returns a JDK
dynamic proxy implementing the gateway — every call is translated into
a `RemoteRpcInvocation` and sent via `ask`/`tell` to the remote actor.

### Single-thread main-thread invariant

All endpoint methods execute serially on the endpoint's **main
thread** (the Pekko dispatcher). `RpcEndpoint.runAsync(Runnable)` and
`callAsync(Callable, Duration)` schedule work back onto that thread.
`MainThreadValidatorUtil.isRunningInExpectedThread()` enforces it.
Most fields in an endpoint are therefore single-threaded — no locks
required.

### Fencing tokens

`FencedRpcEndpoint<F>` tags its incoming invocations with a fencing
token `F` (typically a `UUID` representing leadership epoch). Messages
arriving with a stale token are rejected with `FencingTokenException`,
which is how leader changes prevent split-brain RPCs. This is the
mechanism the HA layer relies on (see `codedocs/flink-kubernetes-ha-
deep-dive.md`).

### Classloader isolation via the loader

`PekkoRpcSystemLoader` extracts `flink-rpc-akka.jar` from the
classpath into a temp dir and wraps it in a
`SubmoduleClassLoader` (parent = Flink's CL but with restricted
visibility to Pekko classes). The result is a `CleanupOnCloseRpcSystem`
whose `close()` deletes the temp jar and unloads the classloader. This
keeps Pekko/Scala completely invisible to user code and avoids
classpath collisions with applications that ship their own Akka.

## Important public APIs

- `org.apache.flink.runtime.rpc.RpcSystem` — `load()` /
  `load(Configuration)` is the **only** public entry point. Returns
  an `RpcSystem` with `localServiceBuilder` / `remoteServiceBuilder`.
- `RpcSystem.RpcServiceBuilder` — fluent builder
  (`withBindAddress`, `withBindPort`, `withExecutorConfiguration`,
  `createAndStart()`).
- `RpcService` — `startServer(endpoint)`, `connect(address, clazz)`,
  `stopService()`.
- `RpcEndpoint` — `start()`, `onStart()`, `onStop()`, `getSelfGateway`,
  `getMainThreadExecutor()`.
- `RpcGateway`, `FencedRpcGateway<F>` — extension points for user
  gateway interfaces.
- `@RpcTimeout`, `@Local` — annotations on gateway methods.
- `RpcServiceUtils` — endpoint id / address helpers.

## Internal flows

1. **Service load** — at process start, `RpcSystem.load(config)`
   runs `ServiceLoader<RpcSystemLoader>`, sorts by `getLoadPriority()`,
   and tries each in order. The default `PekkoRpcSystemLoader`
   (priority 0) extracts the bundled jar and instantiates
   `PekkoRpcSystem` via the submodule classloader.
2. **Endpoint registration** — daemon constructs an
   `RpcEndpoint`, calling `rpcService.startServer(this)` (called from
   the endpoint's own constructor via `RpcEndpoint`). Pekko spawns a
   `PekkoRpcActor`; a `RemoteHandshakeMessage` is exchanged so clients
   know the actor speaks the right gateway.
3. **Remote call** — `rpcService.connect(addr, GatewayClass)` returns
   a JDK proxy whose `invoke` method builds a `RemoteRpcInvocation`,
   serializes args with `RpcSerializedValue`, and `ask`s the remote
   actor. The future is completed on the calling thread's executor
   (with classloader restored by `ClassLoadingUtils`).
4. **Shutdown** — `endpoint.closeAsync()` runs `onStop()` in the
   main thread, drains remaining messages, then poisons the actor.

## Tests

- `flink-rpc-core` has minimal unit tests:
  `RpcSystemTest`, `ClassLoadingUtilsTest`, `ScheduledFutureAdapterTest`.
- `flink-rpc-akka` carries the heavy suite:
  `PekkoRpcActorTest`, `PekkoRpcServiceTest`, `RemotePekkoRpcActorTest`,
  `PekkoRpcActorHandshakeTest`, `MessageSerializationTest`,
  `PekkoRpcActorOversizedResponseMessageTest` (covers the maximum-
  framesize behaviour), `RobustActorSystemTest`,
  `SupervisorActorTest`, `TimeoutCallStackTest`.
- `flink-rpc-akka-loader` has `PekkoRpcSystemLoaderITCase` plus a
  `FallbackPekkoRpcSystemLoader` used to verify priority ordering.

## Pitfalls & gotchas

- **Do not import Pekko classes** from `flink-rpc-akka` outside this
  module. The maven-enforcer rule
  `forbid-direct-akka-rpc-dependencies` will fail the build. Use the
  `flink-rpc-core` interfaces.
- The `akka` substring in artifact names is **misleading** — there is
  no Lightbend Akka anywhere; the implementation is Pekko 1.4.0. The
  module description in `flink-rpc-akka/pom.xml` explicitly notes
  "For compatibility/git reasons not all mentions of Akka have been
  replaced."
- `RemoteRpcInvocation` arguments must be **Java-serializable**;
  `LocalRpcInvocation` is used for in-JVM calls and skips
  serialization. Failing to make an argument serializable will surface
  only when the gateway is invoked remotely.
- All gateway methods that return values must return
  `CompletableFuture<T>` — declaring a synchronous return type
  throws at startup.
- `@RpcTimeout` on a `Duration` parameter lets the caller override the
  default timeout per call.
- Tests that load the RPC system in the IDE need the
  `flink-rpc-akka.jar` resource on the classpath. The hint in
  `PekkoRpcSystemLoader.HINT_USAGE` (`mvn clean package -pl
  flink-rpc/flink-rpc-akka,flink-rpc/flink-rpc-akka-loader -DskipTests`)
  is what to run when an IDE-only test fails with
  `RpcLoaderException`.

## Related modules / docs

- `flink-runtime` — the only direct consumer; defines all gateway
  interfaces.
- `flink-clients`, `flink-kubernetes` — use `RpcService` to look up
  the dispatcher.
- `codedocs/flink-kubernetes-ha-deep-dive.md` — shows the
  `akka.tcp://flink@host:6123/user/rpc/...` addresses produced by
  `PekkoRpcServiceUtils` being stored in K8s ConfigMaps for leader
  recovery.
- `codedocs/flink-exactly-once-checkpointing-deep-dive.md` — the
  CheckpointCoordinator → TaskExecutor calls in that flow are all RPC.
