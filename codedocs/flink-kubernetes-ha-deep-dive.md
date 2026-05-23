# Flink Kubernetes High Availability Deep Dive

How Flink achieves High Availability on Kubernetes — leader election via ConfigMaps,
metadata persistence, TaskManager failover, and the end-to-end flow when one JobManager
dies and a standby takes over.

Based on the
[HA overview](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/ha/overview/),
[Kubernetes HA docs](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/ha/kubernetes_ha/),
[FLIP-144](https://cwiki.apache.org/confluence/display/FLINK/FLIP-144:+Native+Kubernetes+HA+for+Flink),
and the source code in `flink-kubernetes` and `flink-runtime`.

---

# Part 1: Overview

## The Problem

By default, Flink runs a single JobManager. If it dies, every running job fails with no
automatic recovery — the JM is a single point of failure.

## The HA Solution

Run **one active** + **N standby** JobManagers. When the active dies, a standby takes over
and recovers all jobs from the last checkpoint. Flink needs three capabilities:

| Capability | What It Does |
|-----------|-------------|
| **Leader election** | Choose exactly one active JM from the pool |
| **Service discovery** | Let TaskManagers and clients find the current leader |
| **State persistence** | Store JM metadata so the successor can recover jobs |

## Why Kubernetes-Native HA?

Before FLIP-144, Flink required an external ZooKeeper quorum for HA. On Kubernetes this
meant deploying and operating a separate stateful ZK cluster just for leader election.

Kubernetes-native HA replaces ZooKeeper entirely:

```
┌──────────────────────────────────────────────────────────────────┐
│              ZooKeeper HA vs Kubernetes-Native HA                  │
│                                                                    │
│  ZooKeeper HA:                                                     │
│  ┌──────┐  ┌──────┐  ┌──────┐                                    │
│  │ ZK-1 │  │ ZK-2 │  │ ZK-3 │   ← Extra stateful cluster        │
│  └──┬───┘  └──┬───┘  └──┬───┘     to deploy and maintain         │
│     └─────────┼─────────┘                                          │
│               │                                                    │
│        ┌──────┴──────┐                                             │
│        │  Flink JMs  │                                             │
│        └─────────────┘                                             │
│                                                                    │
│  Kubernetes-Native HA:                                             │
│        ┌─────────────┐                                             │
│        │  Flink JMs  │──── ConfigMaps ← Already part of K8s       │
│        └─────────────┘     (no extra                               │
│                             infrastructure)                        │
└──────────────────────────────────────────────────────────────────┘
```

---

# Part 2: HA Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    FLINK KUBERNETES HA ARCHITECTURE                      │
│                                                                          │
│  ┌──────────────────────── Kubernetes Cluster ──────────────────────┐   │
│  │                                                                   │   │
│  │  ┌─────────────┐         ┌─────────────┐                        │   │
│  │  │ JM Pod      │         │ JM Pod      │                        │   │
│  │  │ (ACTIVE)    │         │ (STANDBY)   │                        │   │
│  │  │             │         │             │                        │   │
│  │  │ Dispatcher  │         │ Dispatcher  │                        │   │
│  │  │ ResourceMgr │         │ ResourceMgr │  ← Contending for      │   │
│  │  │ JobMaster   │         │ JobMaster   │    leadership           │   │
│  │  │ REST Server │         │ REST Server │                        │   │
│  │  └──────┬──────┘         └──────┬──────┘                        │   │
│  │         │                       │                                │   │
│  │         │  Leader election      │  Watches for                   │   │
│  │         │  (write lock)         │  leader changes                │   │
│  │         ▼                       ▼                                │   │
│  │  ┌─────────────────────────────────────┐                        │   │
│  │  │         Kubernetes ConfigMaps        │                        │   │
│  │  │                                      │                        │   │
│  │  │  {clusterId}-cluster-config-map      │  ← Leader addresses    │   │
│  │  │  {clusterId}-{jobId}-config-map      │  ← Checkpoint pointers │   │
│  │  │                                      │                        │   │
│  │  │  annotation: control-plane.alpha.    │                        │   │
│  │  │  kubernetes.io/leader (lease info)   │                        │   │
│  │  └──────────────┬──────────────────────┘                        │   │
│  │                 │                                                │   │
│  │                 │  Pointers to actual state                      │   │
│  │                 ▼                                                │   │
│  │  ┌─────────────────────────────────────┐                        │   │
│  │  │    Distributed File Storage          │                        │   │
│  │  │    (S3 / HDFS / GCS)                │                        │   │
│  │  │                                      │                        │   │
│  │  │    /ha-storage/                      │                        │   │
│  │  │      ├── checkpoints/                │  ← Actual checkpoint   │   │
│  │  │      ├── executionplans/             │    state data (can be  │   │
│  │  │      └── jobresults/                 │    multiple GBs)       │   │
│  │  └─────────────────────────────────────┘                        │   │
│  │                                                                   │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │   │
│  │  │ TM Pod      │  │ TM Pod      │  │ TM Pod      │             │   │
│  │  │             │  │             │  │             │             │   │
│  │  │ Watches     │  │ Watches     │  │ Watches     │             │   │
│  │  │ ConfigMap   │  │ ConfigMap   │  │ ConfigMap   │             │   │
│  │  │ for leader  │  │ for leader  │  │ for leader  │             │   │
│  │  │ address     │  │ address     │  │ address     │             │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘             │   │
│  └───────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

# Part 3: HA Service Components

Flink abstracts HA behind `HighAvailabilityServices`. On Kubernetes, the implementation
is `KubernetesLeaderElectionHaServices`, created by `KubernetesHaServicesFactory`.

| Service | Interface | K8s Implementation | Purpose |
|---------|-----------|-------------------|---------|
| Leader Election | `LeaderElectionDriver` | `KubernetesLeaderElectionDriver` | JMs contend for leadership via ConfigMap lock |
| Leader Retrieval | `LeaderRetrievalDriver` | `KubernetesLeaderRetrievalDriver` | TMs/clients watch ConfigMap for current leader |
| Checkpoint Recovery | `CheckpointRecoveryFactory` | `KubernetesCheckpointRecoveryFactory` | Recover completed checkpoints + ID counter |
| Execution Plan Store | `ExecutionPlanStore` | via `KubernetesStateHandleStore` | Persist/recover JobGraphs across failover |
| Job Result Store | `JobResultStore` | Filesystem-based | Archive terminal job results to prevent re-execution |

Each component (Dispatcher, ResourceManager, JobMaster, REST server) runs its own
independent leader election:

```
┌──────────────────────────────────────────────────────────┐
│           FOUR INDEPENDENT LEADER ELECTIONS               │
│                                                          │
│  Component        ConfigMap Key                          │
│  ─────────        ────────────                           │
│  Dispatcher       org.apache.flink.k8s.leader.dispatcher │
│  ResourceManager  org.apache.flink.k8s.leader.resourcemanager │
│  REST Server      org.apache.flink.k8s.leader.restserver │
│  JobMaster(job1)  org.apache.flink.k8s.leader.job-{id}   │
│                                                          │
│  All stored in the same cluster ConfigMap, but each      │
│  component elects its leader independently.              │
└──────────────────────────────────────────────────────────┘
```

Source: `KubernetesLeaderElectionHaServices.java`, `HighAvailabilityServices.java`

---

# Part 4: Leader Election on Kubernetes

## Why ConfigMap? Why Not etcd Directly?

Kubernetes stores all cluster state in **etcd**, but applications never talk to etcd
directly. Instead, they go through the **Kubernetes API Server**, which provides:

```
┌──────────────────────────────────────────────────────────────────┐
│          WHY CONFIGMAP INSTEAD OF DIRECT ETCD                     │
│                                                                   │
│  Direct etcd access:                                              │
│    ✗ Requires etcd client credentials (security risk)            │
│    ✗ Bypasses K8s RBAC — no permission model                    │
│    ✗ etcd API is internal, changes across K8s versions           │
│    ✗ etcd may not be reachable from application pods             │
│    ✗ Tight coupling to infrastructure layer                      │
│                                                                   │
│  ConfigMap via API Server:                                        │
│    ✓ Standard K8s RBAC for access control                        │
│    ✓ Stable API — won't break across K8s versions               │
│    ✓ kubectl-visible — operators can inspect leader state        │
│    ✓ resourceVersion provides optimistic concurrency for free    │
│    ✓ K8s Watch API provides push-based change notifications      │
│    ✓ Same ConfigMap serves dual purpose: lock + data storage     │
│                                                                   │
│  etcd DOES back the concurrency guarantees — but you access      │
│  them through the API Server's resource versioning, not by       │
│  talking to etcd directly.                                        │
│                                                                   │
│  ┌─────────┐     ┌──────────────┐     ┌──────┐                  │
│  │ Flink   │────▶│  K8s API     │────▶│ etcd │                  │
│  │ JM Pods │     │  Server      │     │      │                  │
│  └─────────┘     │              │     │      │                  │
│    ConfigMap     │ resourceVer. │     │ MVCC │                  │
│    CRUD via      │ concurrency  │     │ txn  │                  │
│    REST API      │ control      │     │      │                  │
│                  └──────────────┘     └──────┘                  │
└──────────────────────────────────────────────────────────────────┘
```

Note: Modern Kubernetes also offers a `Lease` resource purpose-built for leader election
(lighter weight than ConfigMap). Flink uses `ConfigMap` because it also stores HA metadata
(leader addresses, checkpoint pointers) in the same resource — one ConfigMap serves as
both the lock and the data store.

## How the Lock Works — fabric8 `ConfigMapLock`

Flink wraps fabric8's `LeaderElector` in its own `KubernetesLeaderElector` class.
The lock is a **ConfigMap annotation** — the `LeaderElectionRecord`.

### The LeaderElectionRecord

Every leader election stores a JSON record in the ConfigMap annotation
`control-plane.alpha.kubernetes.io/leader`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-cluster-cluster-config-map
  namespace: flink
  resourceVersion: "1847293"        ← Changes on every update (optimistic lock)
  annotations:
    control-plane.alpha.kubernetes.io/leader: |
      {
        "holderIdentity": "623e39fb-70c3-44f1-811f-561ec4a28d75",
        "leaseDuration": 15.000000000,
        "acquireTime": "2024-01-15T10:30:00.000Z",
        "renewTime": "2024-01-15T10:30:45.843Z",
        "leaderTransitions": 42
      }
data:
  org.apache.flink.k8s.leader.dispatcher: "d4e5f6-...,akka.tcp://..."
  org.apache.flink.k8s.leader.resourcemanager: "a1b2c3-...,akka.tcp://..."
```

| Field | Meaning |
|-------|---------|
| `holderIdentity` | UUID of the JM pod that currently holds the lock |
| `leaseDuration` | How long the lock is valid (15s) |
| `acquireTime` | When this leader first acquired the lock |
| `renewTime` | Last time the leader renewed its lease |
| `leaderTransitions` | Total number of leadership changes (monotonically increasing) |

### Is the ConfigMap Created Dynamically?

**Yes.** The ConfigMap is created automatically by fabric8's `LeaderElector.run()`:

```
┌──────────────────────────────────────────────────────────────────┐
│  CONFIGMAP LIFECYCLE                                              │
│                                                                   │
│  1. KubernetesLeaderElectionDriver constructor calls              │
│     leaderElector.run()                                           │
│                                                                   │
│  2. fabric8 LeaderElector.start() internally:                     │
│     ├── Try GET ConfigMap                                         │
│     │    ├── NOT FOUND → CREATE it with this JM's lock record    │
│     │    │                (first JM to start creates it)          │
│     │    └── EXISTS → check if lease expired                     │
│     │         ├── Expired → UPDATE with new holderIdentity       │
│     │         └── Not expired → wait and retry                   │
│     └── ConfigMap now exists with leader annotation               │
│                                                                   │
│  3. For per-job ConfigMaps (checkpoint metadata):                 │
│     Flink pre-creates them via                                    │
│     KubernetesUtils.createConfigMapIfItDoesNotExist()             │
│                                                                   │
│  4. On job completion/cancellation:                               │
│     ConfigMap is explicitly DELETED                               │
│                                                                   │
│  5. On cluster stop (graceful shutdown):                          │
│     ConfigMap is KEPT — survives for recovery                     │
│     (no owner reference to JM Deployment)                         │
└──────────────────────────────────────────────────────────────────┘
```

Source: `KubernetesLeaderElector.java` line 53: *"LeaderElector#run() is responsible for
creating the leader ConfigMap and continuously update the annotation."*

## fabric8 LeaderElector Internals — The Acquire/Renew Loop

The fabric8 `LeaderElector` implements the same algorithm as the official Kubernetes
client-go leader election library. Here's how it works:

```
┌──────────────────────────────────────────────────────────────────┐
│          fabric8 LeaderElector.start() — THE MAIN LOOP           │
│                                                                   │
│  ┌─────────────────────────────────────────────────────┐         │
│  │  PHASE 1: tryAcquireOrRenew()                       │         │
│  │                                                      │         │
│  │  1. GET ConfigMap from K8s API                       │         │
│  │     → receives current resourceVersion (e.g., "42") │         │
│  │                                                      │         │
│  │  2. Read annotation: parse LeaderElectionRecord      │         │
│  │                                                      │         │
│  │  3. DECIDE:                                          │         │
│  │     ┌─────────────────────────────────────────┐      │         │
│  │     │ ConfigMap not found?                    │      │         │
│  │     │   → CREATE ConfigMap with my record     │      │         │
│  │     │     (I become leader)                   │      │         │
│  │     ├─────────────────────────────────────────┤      │         │
│  │     │ Record exists, I am the holder?         │      │         │
│  │     │   → UPDATE: refresh renewTime           │      │         │
│  │     │     (renew my lease)                    │      │         │
│  │     ├─────────────────────────────────────────┤      │         │
│  │     │ Record exists, someone else is holder,  │      │         │
│  │     │ lease NOT expired?                      │      │         │
│  │     │   → DO NOTHING, wait, retry later       │      │         │
│  │     ├─────────────────────────────────────────┤      │         │
│  │     │ Record exists, someone else is holder,  │      │         │
│  │     │ lease EXPIRED?                          │      │         │
│  │     │   (now > renewTime + leaseDuration)     │      │         │
│  │     │   → UPDATE: overwrite holderIdentity    │      │         │
│  │     │     with mine (I become leader)         │      │         │
│  │     └─────────────────────────────────────────┘      │         │
│  │                                                      │         │
│  │  4. UPDATE uses resourceVersion for optimistic lock: │         │
│  │     "Update this ConfigMap, but ONLY if its version  │         │
│  │      is still '42'. If someone else modified it      │         │
│  │      since I read it, REJECT my update."             │         │
│  │                                                      │         │
│  └──────────────────────────────────────────────────────┘         │
│                                                                   │
│  ┌─────────────────────────────────────────────────────┐         │
│  │  PHASE 2: LOOP                                      │         │
│  │                                                      │         │
│  │  If I AM leader:                                     │         │
│  │    sleep(renewDeadline × jitter)                     │         │
│  │    call tryAcquireOrRenew() again to refresh lease   │         │
│  │    If renewal fails → call notLeader() callback      │         │
│  │                                                      │         │
│  │  If I am NOT leader:                                 │         │
│  │    sleep(retryPeriod × jitter)                       │         │
│  │    call tryAcquireOrRenew() again to try acquiring   │         │
│  │                                                      │         │
│  │  Loop continues indefinitely until cancelled.        │         │
│  └──────────────────────────────────────────────────────┘         │
└──────────────────────────────────────────────────────────────────┘
```

### Optimistic Concurrency — How Two JMs Can't Both Win

The key safety mechanism is Kubernetes' `resourceVersion`:

```
┌──────────────────────────────────────────────────────────────────┐
│  CONCURRENT ELECTION — ONLY ONE WINNER                            │
│                                                                   │
│  JM-1                          JM-2                               │
│   │                             │                                 │
│   ├── GET ConfigMap             ├── GET ConfigMap                  │
│   │   version="42"              │   version="42"                  │
│   │                             │                                 │
│   ├── Lease expired!            ├── Lease expired!                │
│   │   Prepare update:           │   Prepare update:               │
│   │   holder=JM-1               │   holder=JM-2                   │
│   │                             │                                 │
│   ├── UPDATE ConfigMap          │                                 │
│   │   "if version==42"          │                                 │
│   │   → SUCCESS ✓               │                                 │
│   │   version now "43"          │                                 │
│   │                             │                                 │
│   │                             ├── UPDATE ConfigMap              │
│   │                             │   "if version==42"              │
│   │                             │   → 409 CONFLICT ✗             │
│   │                             │   (version is now 43!)          │
│   │                             │                                 │
│   │  I am leader!               │   Retry next cycle...           │
│   │                             │                                 │
│   │  This is how etcd's MVCC    │                                 │
│   │  provides safety through    │                                 │
│   │  the API Server.            │                                 │
└──────────────────────────────────────────────────────────────────┘
```

In Flink's code, the optimistic lock is applied by `Fabric8FlinkKubeClient`:

```java
// Fabric8FlinkKubeClient.attemptCheckAndUpdateConfigMap()
internalClient
    .resource(maybeUpdate.get().getInternalResource())
    .lockResourceVersion()  // ← "fail if version changed since I read it"
    .update();              // ← K8s API returns 409 Conflict on version mismatch
```

If the update fails with a conflict, `checkAndUpdateConfigMap()` retries with
`FutureUtils.retry()` up to `maxRetryAttempts` times. On retry, it re-reads the ConfigMap
(getting the new version), re-evaluates the update function, and tries again.

Source: `Fabric8FlinkKubeClient.java` lines 302-351

## The Full Election Protocol in Flink

```
┌──────────────────────────────────────────────────────────────────┐
│              LEADER ELECTION PROTOCOL (COMPLETE)                  │
│                                                                   │
│  JM-1 (lockId: aaa)              JM-2 (lockId: bbb)             │
│       │                                │                          │
│  T0:  ├── Try create/update ConfigMap  │                          │
│       │   with lockId=aaa              ├── Try create/update      │
│       │                                │   with lockId=bbb        │
│       │                                │                          │
│  T1:  ├── SUCCESS ✓                    ├── CONFLICT ✗             │
│       │   isLeader() callback fires    │   (aaa holds the lock)   │
│       │   → onGrantLeadership(UUID)    │                          │
│       │   → publish address to data:   │                          │
│       │     key=k8s.leader.dispatcher  │                          │
│       │     val="{uuid},{rpc-addr}"    │                          │
│       │                                │                          │
│  T2:  ├── Renew every ~10s             ├── Retry every ~2s        │
│       │   Update renewTime in          │   Read annotation,       │
│       │   annotation                   │   check if expired       │
│       │                                │                          │
│  T3:  ├── CRASH ✗                      │                          │
│       │   (stops renewing)             │                          │
│       │                                │                          │
│  T4:  │   (up to 15s passes)           ├── Reads annotation:      │
│       │                                │   now > renewTime + 15s  │
│       │                                │   Lease EXPIRED!         │
│       │                                │                          │
│  T5:  │                                ├── UPDATE ConfigMap:      │
│       │                                │   holderIdentity=bbb     │
│       │                                │   renewTime=now          │
│       │                                │   leaderTransitions++    │
│       │                                │                          │
│  T6:  │                                ├── SUCCESS ✓              │
│       │                                │   isLeader() fires       │
│       │                                │   → onGrantLeadership()  │
│       │                                │   → publish new address  │
│       │                                │                          │
│       │                                │   ConfigMap watch fires  │
│       │                                │   on all TMs → they      │
│       │                                │   reconnect to JM-2      │
└──────────────────────────────────────────────────────────────────┘
```

## Timing Parameters

| Parameter | Default | Meaning |
|-----------|---------|---------|
| Lease duration | 15 seconds | How long the lock is valid without renewal |
| Renew deadline | 10 seconds | Leader must renew within this window |
| Retry period | 2 seconds | How often standbys check for expired lease |

**Maximum failover detection time**: lease duration (15s) + retry period (2s) = **~17 seconds**
before a standby can detect the leader is gone and win the election.

## How Flink Wires It Together — From Driver to fabric8

```
KubernetesLeaderElectionDriver (Flink's LeaderElectionDriver impl)
  │
  │  Constructor:
  │  ├── kubeClient.createLeaderElector(config, callbackHandler)
  │  │     → creates KubernetesLeaderElector
  │  │
  │  ├── configMapSharedWatcher.watch(configMapName, ...)
  │  │     → watches ConfigMap for changes (used by this leader
  │  │       AND by LeaderRetrievalDrivers on TMs/standbys)
  │  │
  │  └── leaderElector.run()
  │        → starts fabric8 LeaderElector on a background thread
  │
  KubernetesLeaderElector (Flink's wrapper around fabric8)
  │
  │  Constructor:
  │  ├── Builds LeaderElectionConfig:
  │  │     .withLock(new ConfigMapLock(
  │  │         namespace, configMapName, lockIdentity))
  │  │     .withLeaseDuration(15s)
  │  │     .withRenewDeadline(10s)
  │  │     .withRetryPeriod(2s)
  │  │     .withReleaseOnCancel(true)
  │  │     .withLeaderCallbacks(isLeader, notLeader, onNewLeader)
  │  │
  │  run():
  │  └── new LeaderElector(kubernetesClient, config, executor).start()
  │        → fabric8 runs tryAcquireOrRenew() loop on executor thread
  │
  fabric8 LeaderElector (library code)
  │
  │  start():
  │  └── Blocking loop:
  │        ├── ConfigMapLock.get()     → GET ConfigMap from K8s API
  │        ├── Check LeaderElectionRecord in annotation
  │        ├── ConfigMapLock.create()  → CREATE ConfigMap (if not exists)
  │        │   or ConfigMapLock.update() → UPDATE annotation
  │        │   (uses resourceVersion for optimistic concurrency)
  │        ├── On success: call isLeader() → Flink callback
  │        ├── On failure: sleep(retryPeriod), try again
  │        └── On lost: call notLeader() → Flink restarts election
  │
  ConfigMapLock (fabric8 Lock implementation)
  │
  └── Maps LeaderElectionRecord to/from ConfigMap annotation:
        key:   "control-plane.alpha.kubernetes.io/leader"
        value: JSON { holderIdentity, leaseDuration, renewTime, ... }
```

### What `releaseOnCancel: true` Does

When Flink calls `leaderElector.stop()` during graceful shutdown:

1. fabric8 cancels the election loop
2. Because `releaseOnCancel=true`, it **clears the holderIdentity** in the annotation
3. This immediately signals to standbys that the lock is free
4. A standby can acquire leadership without waiting for lease expiry
5. This reduces failover time during **planned** shutdowns to near-zero

Without this flag, standbys would have to wait the full lease duration (15s) even during
graceful shutdowns.

Source: `KubernetesLeaderElector.java`, `KubernetesLeaderElectionDriver.java`,
`Fabric8FlinkKubeClient.java`

---

# Part 5: ConfigMap Structure

## ConfigMap Naming

| ConfigMap | Purpose |
|-----------|---------|
| `{clusterId}-cluster-config-map` | Cluster-wide leader info (Dispatcher, ResourceManager, REST server) |
| `{clusterId}-{jobId}-config-map` | Per-job leader info (JobMaster) + checkpoint metadata |

## Data Keys Inside ConfigMaps

```
┌──────────────────────────────────────────────────────────────────┐
│  CLUSTER CONFIGMAP: my-cluster-cluster-config-map                │
│                                                                   │
│  data:                                                            │
│    org.apache.flink.k8s.leader.dispatcher:                       │
│      "d4e5f6-...,akka.tcp://flink@jm-pod:6123/..."              │
│                                                                   │
│    org.apache.flink.k8s.leader.resourcemanager:                  │
│      "a1b2c3-...,akka.tcp://flink@jm-pod:6123/..."              │
│                                                                   │
│    org.apache.flink.k8s.leader.restserver:                       │
│      "e7f8g9-...,http://jm-pod:8081"                             │
│                                                                   │
│    executionPlan-{jobId}:                                         │
│      <Base64-encoded RetrievableStateHandle>                     │
│      → points to s3://ha-storage/executionplans/{jobId}          │
│                                                                   │
│  metadata:                                                        │
│    annotations:                                                   │
│      control-plane.alpha.kubernetes.io/leader: {lease JSON}       │
│    resourceVersion: "1847293"    ← Used for optimistic locking   │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│  JOB CONFIGMAP: my-cluster-{jobId}-config-map                    │
│                                                                   │
│  data:                                                            │
│    org.apache.flink.k8s.leader.job-{jobId}:                      │
│      "x1y2z3-...,akka.tcp://flink@jm-pod:6123/..."              │
│                                                                   │
│    counter: "42"                                                  │
│      → Current checkpoint ID counter                             │
│                                                                   │
│    checkpointID-38:                                               │
│      <Base64-encoded RetrievableStateHandle>                     │
│      → points to s3://ha-storage/checkpoints/38/...              │
│                                                                   │
│    checkpointID-39:                                               │
│      <Base64-encoded RetrievableStateHandle>                     │
│                                                                   │
│    checkpointID-40:                                               │
│      <Base64-encoded RetrievableStateHandle>                     │
└──────────────────────────────────────────────────────────────────┘
```

Key design choice: **ConfigMaps store only pointers** (serialized `RetrievableStateHandle`
references), not the actual state data. The real checkpoint data, job graphs, etc. live in
distributed file storage (S3/HDFS/GCS). This keeps ConfigMap size well under the
Kubernetes 1 MB limit.

Source: `KubernetesStateHandleStore.java`, `KubernetesCheckpointStoreUtil.java`,
`KubernetesExecutionPlanStoreUtil.java`

---

# Part 6: JM Metadata That Must Survive Failover

When the active JM dies, the new leader must recover everything needed to resume job
execution. Here is what is persisted and where:

```
┌──────────────────────────────────────────────────────────────────┐
│              METADATA PERSISTENCE ARCHITECTURE                    │
│                                                                   │
│           ConfigMap (pointers)          DFS (actual data)         │
│           ─────────────────            ──────────────────         │
│                                                                   │
│  ┌──────────────────────┐     ┌────────────────────────────┐     │
│  │ executionPlan-{jobId}│────▶│ s3://ha/executionplans/     │     │
│  │ (Base64 handle)      │     │   {jobId}/plan.bin          │     │
│  └──────────────────────┘     │                             │     │
│                                │ Contains: JobGraph with all │     │
│                                │ operator chains, configs,   │     │
│                                │ parallelism settings        │     │
│                                └────────────────────────────┘     │
│                                                                   │
│  ┌──────────────────────┐     ┌────────────────────────────┐     │
│  │ checkpointID-40      │────▶│ s3://ha/checkpoints/40/     │     │
│  │ (Base64 handle)      │     │   _metadata                 │     │
│  └──────────────────────┘     │                             │     │
│                                │ Contains: operator states,  │     │
│                                │ keyed state handles, coord- │     │
│                                │ inator state (Kafka offsets) │     │
│                                └────────────────────────────┘     │
│                                                                   │
│  ┌──────────────────────┐                                        │
│  │ counter: "42"        │     Checkpoint ID counter — next       │
│  │ (plain integer)      │     checkpoint will be ID 43           │
│  └──────────────────────┘                                        │
│                                                                   │
│  ┌──────────────────────┐     ┌────────────────────────────┐     │
│  │ JobResultStore       │────▶│ s3://ha/jobresults/         │     │
│  │ (filesystem-based)   │     │   {jobId}/result.bin        │     │
│  └──────────────────────┘     │                             │     │
│                                │ Contains: terminal status   │     │
│                                │ (FINISHED/CANCELLED/FAILED) │     │
│                                │ Prevents re-execution of    │     │
│                                │ already-completed jobs       │     │
│                                └────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────┘
```

### What Each Piece Contains

| Metadata | Contents | Why It's Needed |
|----------|----------|-----------------|
| **ExecutionPlan (JobGraph)** | Operator topology, edge connections, parallelism, user JARs | New JM must know what job to run |
| **Completed Checkpoints** | Per-operator state snapshots, keyed state handles, coordinator state (e.g., Kafka offsets) | Restore exactly-once consistent state |
| **Checkpoint ID Counter** | Monotonically increasing long | Prevent duplicate checkpoint IDs after failover |
| **Job Result Store** | Terminal job status | Prevent re-executing already-finished jobs |
| **Leader Info** | Session ID + RPC address | Service discovery for TMs and clients |

Source: `KubernetesCheckpointRecoveryFactory.java`, `KubernetesCheckpointIDCounter.java`,
`KubernetesExecutionPlanStoreUtil.java`

---

# Part 7: TaskManager Failover Behavior

## Do TaskManagers Restart?

**No.** TaskManager processes (pods) stay alive during JM failover. They are not killed
or restarted. However, the **tasks running inside them are killed and redeployed** by
the new JobMaster.

```
┌──────────────────────────────────────────────────────────────────┐
│          TASKMANAGER BEHAVIOR DURING JM FAILOVER                  │
│                                                                   │
│  Before failover:                                                 │
│  ┌─────────────────────────────────────────────────┐             │
│  │ TaskManager Pod (ALIVE)                          │             │
│  │                                                   │             │
│  │  Slot 1: [Map task - processing records] ✓       │             │
│  │  Slot 2: [Reduce task - aggregating]     ✓       │             │
│  │  Slot 3: [Sink task - writing to Kafka]  ✓       │             │
│  │                                                   │             │
│  │  RPC connection → JM-1 (active)                  │             │
│  └─────────────────────────────────────────────────┘             │
│                                                                   │
│  During failover:                                                 │
│  ┌─────────────────────────────────────────────────┐             │
│  │ TaskManager Pod (ALIVE — pod not restarted)      │             │
│  │                                                   │             │
│  │  Slot 1: [EMPTY — task killed]           ✗       │             │
│  │  Slot 2: [EMPTY — task killed]           ✗       │             │
│  │  Slot 3: [EMPTY — task killed]           ✗       │             │
│  │                                                   │             │
│  │  RPC connection → (reconnecting...)              │             │
│  │  Watching ConfigMap for new leader address        │             │
│  └─────────────────────────────────────────────────┘             │
│                                                                   │
│  After failover:                                                  │
│  ┌─────────────────────────────────────────────────┐             │
│  │ TaskManager Pod (ALIVE)                          │             │
│  │                                                   │             │
│  │  Slot 1: [Map task - restoring from ckpt] ✓     │             │
│  │  Slot 2: [Reduce task - restoring]        ✓     │             │
│  │  Slot 3: [Sink task - restoring]          ✓     │             │
│  │                                                   │             │
│  │  RPC connection → JM-2 (new active)              │             │
│  └─────────────────────────────────────────────────┘             │
└──────────────────────────────────────────────────────────────────┘
```

## How TaskManagers Discover the New Leader

TMs use `DefaultJobLeaderService` which watches ConfigMaps via
`KubernetesLeaderRetrievalDriver`:

```
DefaultJobLeaderService
  │
  ├── For each job the TM is involved with:
  │     └── LeaderRetrievalService.start(listener)
  │           └── KubernetesLeaderRetrievalDriver
  │                 └── configMapSharedWatcher.watch(configMapName)
  │
  ├── ConfigMap changes detected:
  │     notifyLeaderAddress(newAddress, newSessionID)
  │       │
  │       ├── Close old RPC connection to dead JM
  │       ├── Open new RPC connection to new JM
  │       │     openRpcConnectionTo(newAddress, newJobMasterId)
  │       └── Re-register with new JM
  │             JobManagerRetryingRegistration (with exponential backoff)
  │
  └── On successful registration:
        jobLeaderListener.jobManagerGainedLeadership()
          → TM offers its slots to the new JM
          → New JM deploys tasks into available slots
```

Key point: TaskManagers don't maintain any persistent job state. Everything the new JM
needs is recovered from ConfigMaps + DFS. The TM simply provides compute slots.

Source: `DefaultJobLeaderService.java`, `KubernetesLeaderRetrievalDriver.java`

---

# Part 8: End-to-End Failover Flow

## The Complete Sequence

```
┌──────────────────────────────────────────────────────────────────────┐
│  T0: ACTIVE JM DIES                                                  │
│  ════════════════════                                                │
│  JM-1 pod crashes (OOM, node failure, process kill)                  │
│  Lease renewal stops → lease will expire in ≤15 seconds              │
│                                                                       │
│  T1: STANDBY WINS ELECTION (~17s after T0)                           │
│  ═══════════════════════════════════════════                          │
│  JM-2 detects expired lease on retry (every 2s)                      │
│  JM-2 updates ConfigMap with its own lockIdentity                    │
│  onGrantLeadership(newSessionID) fires                               │
│    │                                                                  │
│    └── JobMasterServiceLeadershipRunner                              │
│          .grantLeadership(newSessionID)                               │
│            → Creates new JobMasterServiceProcess                     │
│                                                                       │
│  T2: DISPATCHER RECOVERS JOB GRAPHS                                  │
│  ═══════════════════════════════════                                  │
│  SessionDispatcherLeaderProcess:                                     │
│    ├── Read ExecutionPlanStore (ConfigMap: executionPlan-{jobId})     │
│    ├── Deserialize handles → load JobGraphs from DFS                 │
│    ├── Check JobResultStore — skip already-finished jobs             │
│    └── Submit recovered jobs to new Dispatcher                       │
│                                                                       │
│  T3: NEW JM RECOVERS CHECKPOINTS                                     │
│  ════════════════════════════════                                     │
│  CheckpointRecoveryFactory:                                          │
│    ├── Read ConfigMap keys: checkpointID-38, checkpointID-39, ...    │
│    ├── Deserialize handles → load checkpoint metadata from DFS       │
│    ├── Select latest completed checkpoint (e.g., ID 40)              │
│    └── Read checkpoint ID counter for next checkpoint number         │
│                                                                       │
│  T4: TASKMANAGERS DETECT LEADER CHANGE                               │
│  ═════════════════════════════════════                                │
│  ConfigMap watcher fires on all TMs:                                 │
│    KubernetesLeaderRetrievalDriver                                   │
│      → notifyLeaderAddress(jm2-address, newSessionID)                │
│    DefaultJobLeaderService:                                          │
│      ├── Close RPC connection to dead JM-1                           │
│      ├── Open RPC connection to JM-2                                 │
│      └── Register with retrying logic (exponential backoff)          │
│                                                                       │
│  T5: NEW JM REDEPLOYS TASKS                                          │
│  ═══════════════════════════                                          │
│  JM-2 receives TM registrations:                                     │
│    ├── Collects available slots from all TaskManagers                 │
│    ├── Builds new ExecutionGraph from recovered JobGraph              │
│    ├── Assigns tasks to slots                                        │
│    └── Deploys tasks with checkpoint state:                          │
│          JobManagerTaskRestore {                                      │
│            checkpointId: 40,                                         │
│            taskStateSnapshot: <operator states, keyed state>         │
│          }                                                            │
│                                                                       │
│  T6: TASKS RESUME FROM CHECKPOINT                                     │
│  ════════════════════════════════                                      │
│  Each task:                                                           │
│    ├── Loads operator state via initializeState()                     │
│    ├── Restores keyed state from state backend                       │
│    ├── Source operators: reset offsets to checkpoint position         │
│    │   (e.g., Kafka offsets from coordinator state)                   │
│    ├── Calls open() to initialize operator logic                     │
│    └── Begins processing from checkpoint boundary                    │
│                                                                       │
│  TOTAL DOWNTIME: ~17s (election) + recovery time (depends on         │
│                  state size, number of tasks, DFS latency)            │
└──────────────────────────────────────────────────────────────────────┘
```

Source: `JobMasterServiceLeadershipRunner.java`, `SessionDispatcherLeaderProcess.java`,
`DefaultJobLeaderService.java`

---

# Part 9: Does the Job Restart?

**Yes.** The job gets a new execution attempt. All running tasks are killed and
redeployed from the last completed checkpoint.

```
┌──────────────────────────────────────────────────────────────────┐
│  WHAT IS PRESERVED vs LOST DURING FAILOVER                        │
│                                                                   │
│  PRESERVED (from checkpoint):                                     │
│    ✓ Operator state (e.g., window contents, aggregations)        │
│    ✓ Keyed state (e.g., per-key counters, RocksDB state)         │
│    ✓ Source offsets (e.g., Kafka consumer positions)              │
│    ✓ Sink coordinator state (for two-phase commit)               │
│    ✓ Exactly-once semantics                                      │
│                                                                   │
│  LOST (in-flight data between checkpoints):                       │
│    ✗ Records currently being processed in operator buffers       │
│    ✗ Network buffers between operators                           │
│    ✗ Records consumed from source but not yet checkpointed       │
│    ✗ Pending sink transactions not yet committed                 │
│                                                                   │
│  These records are RE-PROCESSED from the checkpoint boundary:     │
│                                                                   │
│  Source stream:                                                    │
│  ───[E1][E2][E3][E4]──║──[E5][E6][E7][E8]──║──[E9][E10]───▶     │
│                        CP-39               CP-40                  │
│                                              ▲                    │
│                                              │                    │
│                                        JM dies here               │
│                                                                   │
│  After recovery, processing resumes from CP-40:                   │
│  Source resets to offset at CP-40, re-reads E9, E10, ...          │
│  Exactly-once: duplicate writes prevented by 2PC or idempotency   │
└──────────────────────────────────────────────────────────────────┘
```

### Why Not "Resume Without Restart"?

The new JM has no knowledge of the old JM's in-flight execution state — network buffers,
partial operator state, pending timers. Only checkpointed state is durable. Therefore:

1. All tasks must be stopped (old execution attempt canceled)
2. New execution attempt created with fresh `ExecutionAttemptID`
3. Tasks deployed with `JobManagerTaskRestore` containing checkpoint state
4. Operators call `initializeState()` to restore from checkpoint
5. Processing begins from the checkpoint boundary

This is identical to what happens during a normal job restart from a savepoint/checkpoint.

---

# Part 10: Transactional Safety

## Optimistic Concurrency via Resource Versions

Kubernetes ConfigMaps have a `resourceVersion` that changes on every update.
The autoscaler uses this for **compare-and-swap** operations:

```
┌──────────────────────────────────────────────────────────────────┐
│  OPTIMISTIC CONCURRENCY CONTROL                                  │
│                                                                   │
│  1. Leader reads ConfigMap                                        │
│     → gets data + resourceVersion=1847293                        │
│                                                                   │
│  2. Leader modifies data client-side                              │
│     (e.g., adds new checkpoint handle)                           │
│                                                                   │
│  3. Leader writes back with resourceVersion=1847293              │
│     → K8s API checks: is current version still 1847293?          │
│                                                                   │
│     YES → Update succeeds, version becomes 1847294               │
│     NO  → 409 Conflict → retry from step 1                      │
│                                                                   │
│  This prevents a stale leader (whose lease just expired but      │
│  doesn't know yet) from overwriting the new leader's data.       │
└──────────────────────────────────────────────────────────────────┘
```

In code: `kubeClient.checkAndUpdateConfigMap()` — a single atomic check-and-update
operation. Only the current leader can successfully update because staleness is detected
via resource version mismatch.

## State Handle Deletion Safety

`StateHandleWithDeleteMarker` wraps state handles with an `isMarkedForDeletion` flag,
enabling idempotent deletion. If a delete fails mid-way, the handle is already marked
and will be cleaned up on the next attempt.

Source: `KubernetesStateHandleStore.java`

---

# Part 11: ConfigMap Lifecycle & Cleanup

## When Are HA ConfigMaps Created/Deleted?

| Event | ConfigMaps | Cluster Resources (pods, services) |
|-------|-----------|-----------------------------------|
| Job submitted | Created | Created |
| Job running | Updated (checkpoints, leader info) | Running |
| Job finishes (FINISHED/CANCELLED/FAILED) | **Deleted** | Cleaned up |
| `flink cancel` | **Deleted** (all HA data removed) | Cleaned up |
| Cluster `stop` | **Kept** (survive for recovery) | Cleaned up |
| K8s Deployment deleted | **Kept** (no owner reference) | **Deleted** via owner references |

Key design decision: HA ConfigMaps intentionally do **not** have Kubernetes owner
references pointing to the JM Deployment. This means:

- When the JM Deployment is deleted (e.g., node failure), K8s garbage collection
  removes the pods but **leaves the ConfigMaps intact**
- A new JM Deployment can find the existing ConfigMaps and recover
- Manual cleanup is required if you truly want to discard HA state

```
┌──────────────────────────────────────────────────────────────────┐
│  OWNER REFERENCE STRATEGY                                         │
│                                                                   │
│  JM Deployment                                                    │
│    │                                                              │
│    ├── owns → JM Pod          (deleted when Deployment deleted)   │
│    ├── owns → TM Pods         (deleted when Deployment deleted)   │
│    ├── owns → Services        (deleted when Deployment deleted)   │
│    │                                                              │
│    └── does NOT own →  HA ConfigMaps  (SURVIVE deletion)         │
│                                                                   │
│  This is intentional: HA state must outlive the cluster itself    │
│  to enable recovery after total cluster destruction.              │
└──────────────────────────────────────────────────────────────────┘
```

---

# Part 12: Configuration Reference

## Required Settings

| Config Key | Example | Description |
|-----------|---------|-------------|
| `high-availability.type` | `kubernetes` | Enable K8s-native HA |
| `high-availability.storageDir` | `s3://bucket/flink-ha/` | DFS path for actual state data |
| `kubernetes.cluster-id` | `my-flink-cluster` | Unique cluster identifier (used in ConfigMap names) |

## Leader Election Tuning

| Config Key | Default | Description |
|-----------|---------|-------------|
| `kubernetes.leader-election.lease-duration` | `15s` | How long lease is valid |
| `kubernetes.leader-election.renew-deadline` | `15s` | Max time leader has to renew |
| `kubernetes.leader-election.retry-period` | `5s` | How often standbys check for expired lease |

## Checkpoint HA

| Config Key | Default | Description |
|-----------|---------|-------------|
| `state.checkpoints.num-retained` | `1` | Number of completed checkpoints to retain in HA store |

## Job Recovery

| Config Key | Default | Description |
|-----------|---------|-------------|
| `high-availability.jobmanager.port` | `6123` | RPC port for JM (must match across standby JMs) |
| `restart-strategy.type` | `fixed-delay` | How tasks restart after failover |
| `restart-strategy.fixed-delay.attempts` | `Integer.MAX_VALUE` | Effectively unlimited for HA |
| `restart-strategy.fixed-delay.delay` | `10s` | Delay between restart attempts |

Source: `KubernetesLeaderElectionConfiguration.java`, `HighAvailabilityOptions.java`

---

# Part 13: Key Classes Reference

## Kubernetes HA Module (`flink-kubernetes`)

| Class | File | Role |
|-------|------|------|
| `KubernetesHaServicesFactory` | `kubernetes/highavailability/` | Creates all K8s HA service instances |
| `KubernetesLeaderElectionHaServices` | `kubernetes/highavailability/` | Orchestrates leader election + retrieval for all components |
| `KubernetesLeaderElectionDriver` | `kubernetes/highavailability/` | ConfigMap-based leader election using fabric8 `KubernetesLeaderElector` |
| `KubernetesLeaderRetrievalDriver` | `kubernetes/highavailability/` | Watches ConfigMap for leader address changes |
| `KubernetesStateHandleStore` | `kubernetes/highavailability/` | Generic key-value store for serialized state handles in ConfigMap |
| `KubernetesCheckpointRecoveryFactory` | `kubernetes/highavailability/` | Creates CompletedCheckpointStore + CheckpointIDCounter for a job |
| `KubernetesCheckpointIDCounter` | `kubernetes/highavailability/` | Distributed monotonic counter stored in ConfigMap |
| `KubernetesExecutionPlanStoreUtil` | `kubernetes/highavailability/` | Helpers for persisting/recovering JobGraphs |
| `KubernetesLeaderElectionConfiguration` | `kubernetes/configuration/` | Lease/renew/retry timing parameters |

## Runtime HA Abstractions (`flink-runtime`)

| Class | File | Role |
|-------|------|------|
| `HighAvailabilityServices` | `runtime/highavailability/` | Interface: all HA services a JM/TM needs |
| `LeaderElectionDriver` | `runtime/leaderelection/` | Interface: pluggable leader election backend |
| `LeaderContender` | `runtime/leaderelection/` | Interface: component that wants to be leader |
| `DefaultLeaderElectionService` | `runtime/leaderelection/` | Manages contender lifecycle + leader confirmation |
| `LeaderRetrievalService` | `runtime/leaderretrieval/` | Interface: discover current leader address |
| `DefaultLeaderRetrievalService` | `runtime/leaderretrieval/` | Notifies listeners of leader changes |
| `CompletedCheckpointStore` | `runtime/checkpoint/` | Interface: persist/recover completed checkpoints |
| `CheckpointRecoveryFactory` | `runtime/checkpoint/` | Interface: create checkpoint store + ID counter |

## Failover Flow Classes (`flink-runtime`)

| Class | File | Role |
|-------|------|------|
| `JobMasterServiceLeadershipRunner` | `runtime/jobmaster/` | Creates new JobMasterServiceProcess on leadership grant |
| `DefaultDispatcherRunner` | `runtime/dispatcher/runner/` | Manages Dispatcher lifecycle across leadership changes |
| `SessionDispatcherLeaderProcess` | `runtime/dispatcher/runner/` | Recovers JobGraphs + creates Dispatcher when becoming leader |
| `ResourceManagerServiceImpl` | `runtime/resourcemanager/` | ResourceManager HA — re-registers TMs on leadership |
| `DefaultJobLeaderService` | `runtime/taskexecutor/` | TM-side: watches for JM leader changes, manages RPC reconnection |
| `JobManagerRetryingRegistration` | `runtime/taskexecutor/` | TM-side: retrying registration with new JM (exponential backoff) |
