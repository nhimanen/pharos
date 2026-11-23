# Pharos Kubernetes operator: design (v1)

Date: 2026-11-22. Status: draft, awaiting approval.

## Intent

Pharos scales beyond one daemon (multi-node Phases 0–5) and should be easy to run on Kubernetes. The operator:

- sets up a complete multi-node pharos from one resource: writers, readers, the router, their config, the cluster token and the storage backend;
- keeps it running;
- scales reader and writer counts from the proposals pharos already publishes on `/cluster/status`.

Tests run on minikube.

## Goals (v1)

1. **One custom resource sets up the whole cluster.** A `PharosCluster` resource creates the writers, readers, router, config, token and storage.
2. **Storage backend.** Either:
   - a MinIO the operator deploys and manages; or
   - an external S3 or S3-compatible bucket (AWS S3, Ceph, R2, a MinIO elsewhere), given as an endpoint plus credentials in a Kubernetes Secret.
3. **Autoscaling** of reader and writer replicas from pharos's proposals (`add-readers`, `remove-readers`, `add-writers`, `remove-writers`), within min/max bounds the user sets. Scale-down goes through pharos's drain and lease handoff, never a hard kill.
4. **A pharos container image** that the operator deploys.
5. **Tests on minikube:** an end-to-end test that installs, indexes, searches, scales up under load and scales down.

## Non-goals (v1)

- **Applying reshard proposals.** That is the next version, after Phase 6. v1 only surfaces them in its own status.
- The peer-pull backend (no bucket). v1 always uses a bucket.
- Multi-cluster deployments and cross-region replication.
- Managing the embedding model lifecycle beyond selecting the model.
- Installing ingress or TLS certificates for the public endpoint. v1 exposes a Service; ingress is left to the user.

## Anti-goals (a failure even if it works)

- **No disruptive scale-down.** The operator must never cause failed queries or a lost or corrupt generation when scaling down. Pod deletion must respect `cluster.drainSeconds` and writer lease release.
- **No flapping.** The operator must not oscillate. Pharos's proposals already carry hysteresis, so the operator must not add a second, conflicting loop.
- **No secrets in the spec.** Credentials never appear in the custom resource, in status, or in logs.
- **No divergence.** The operator must not reimplement pharos's scaling logic. Pharos decides what is needed; the operator decides only whether and how to apply it, within bounds.

## Constraints

- Java 25 and Maven, as in the main project, so the operator reuses pharos's own `ClusterConfig` and status record classes.
- The operator only uses the object-store backend. Writers use the Phase 4 work queue (leases, takeover).
- Every writer mounts its work dir at the same path, `/data/work`, until the relative-paths todo lands. Without that, takeovers fall back to a full re-index.
- Must run on minikube with a modest footprint for tests, roughly 8–10 GB of RAM for a small cluster.

## Approaches (decided)

| Approach | Trade-off | Decision |
|---|---|---|
| **Java operator, Java Operator SDK (JOSDK), module in this repo** | Reuses pharos's config and status classes, so nothing drifts; one language and build; smaller community than Go's | **Chosen** (user, 2026-11-22) |
| Go operator with kubebuilder | Kubernetes standard and the best tooling; a second language; status and config types duplicated by hand | Rejected |
| Helm chart plus the built-in Horizontal Pod Autoscaler | Least new code; needs a metrics adapter to read proposals; scale-down skips drain and lease handoff | Rejected |
| KEDA for the scaling triggers | KEDA's `metrics-api` scaler could read `/cluster/status` fields, and it has cron triggers. But it means one more cluster-wide component to install, and it brings its own cooldown and scale-down logic, which would compete with pharos's proposal hysteresis and the operator's drain-aware steps (anti-goals: flapping, divergence) | Not in v1 (controller's call). Triggers are set in the custom resource and evaluated by pharos; a simple built-in cron covers the schedule case. Generating an optional KEDA `ScaledObject` for users who already run KEDA can come later |

## Design

### Repository layout
- `operator/` is a separate Maven module. It depends on the pharos artifact for `ClusterConfig` and the status records. If a multi-module change to the main pom is too invasive, it can depend on a thin `pharos-api` artifact instead; that is a builder's call.
- The operator ships as its own image, `pharos-operator`.
- `deploy/` holds:
  - the CRD YAML, generated from the Java classes by JOSDK's CRD generator;
  - RBAC;
  - the operator Deployment;
  - an example `PharosCluster`.

### Images
- **`pharos`:** JDK 25 runtime (Temurin JRE), the fat jar and the extractor runtimes (python3, node).
  - The entrypoint renders `config.json` from env and a mounted file, then runs `pharos web` with `cluster.role` set.
  - The embedding model is downloaded on first start into a PVC-backed cache. With `embeddings: none`, no model is downloaded.
- **`pharos-operator`:** the JOSDK operator jar.
- Images are published to GHCR from the release workflow. Local tests load them straight into minikube with `minikube image load`.

### Custom resource `PharosCluster` (namespaced)

```yaml
apiVersion: pharos.dev/v1alpha1
kind: PharosCluster
metadata: { name: demo }
spec:
  version: "1.6.0"                  # pharos image tag
  storage:
    minio:                          # either this...
      storage: 20Gi
    # s3:                           # ...or this
    #   endpoint: https://s3.eu-north-1.amazonaws.com
    #   bucket: my-pharos
    #   region: eu-north-1
    #   pathStyle: false
    #   credentialsSecret: pharos-s3   # keys: accessKey, secretKey
  embeddings: { model: granite-r2 }    # or "none"; same choices as `pharos embeddings enable`
  projects:
    - name: lucene
      git: { url: https://github.com/apache/lucene.git, ref: main }
      embed: false
    - name: mypal
      git: { url: git@github.com:org/mypal.git, ref: main, credentialsSecret: mypal-deploy-key }
  writers:
    min: 1
    max: 4
    resources: {...}
    workVolume: 50Gi
  readers:
    min: 2
    max: 8
    mode: lazy                      # lazy | full
    cacheVolume: 20Gi
    resources: {...}
  router: { replicas: 2, service: { type: ClusterIP } }
  autoscaling:
    enabled: true
    cooldownSeconds: 300
    maxStepUp: 2
    maxStepDown: 1
    triggers:                       # rendered into pharos's cluster.status.* config; pharos raises the proposals
      readers:
        scaleUp:   { warmP95Millis: 500, qpsPerReader: 50, forSeconds: 300 }
        scaleDown: { loadBelowPercent: 20, forSeconds: 1800 }
      writers:
        scaleUp:   { backlogShards: 1, oldestPendingSeconds: 300, forSeconds: 120 }
        scaleDown: { idleForSeconds: 1800 }
    schedule:                       # optional: raise the floors on a timetable (cron, timezone)
      - { cron: "0 8 * * 1-5", timezone: Europe/Helsinki, readers: { min: 4 } }
      - { cron: "0 18 * * 1-5", timezone: Europe/Helsinki, readers: { min: 2 } }
  pharosConfig: {}                  # escape hatch: extra keys merged into cluster.* (validated by ClusterConfig)
status:
  phase: Ready                      # Pending | Provisioning | Ready | Degraded
  writers:  { desired: 2, ready: 2 }
  readers:  { desired: 3, ready: 3 }
  storage:  { kind: minio, ready: true }
  projects: [{ name: lucene, generation: 14, docCount: 65295, pending: false }]
  proposals: [{ id: ..., kind: add-readers, applied: true, since: ... }]
  reshardProposals: [...]           # surfaced only in v1
  conditions: [...]
```

### What the operator creates

| Object | Purpose |
|---|---|
| Secret `<name>-token` | Generated cluster token; never logged or shown in status |
| ConfigMap `<name>-config` | Rendered `cluster.*` config per role; validated with `ClusterConfig.validate()` before it's applied |
| MinIO StatefulSet, Service and PVC (minio mode) | The bucket. Credentials are generated into a Secret, and the bucket is created by a Job |
| Writer StatefulSet | Stable node ids (pod names) and the work-dir PVC at `/data/work`. Runs `pharos web`, role writer, workQueue on |
| Reader StatefulSet | Stable node ids and the cache PVC. Role reader |
| Router Deployment and Service | `pharos router`. This is the public endpoint |
| Headless Services | Writer and reader pod DNS, used for `advertiseUrl` |
| PodDisruptionBudgets | Readers `minAvailable: min`; writers `maxUnavailable: 1` |

**Registering projects.** Projects are registered in the catalog with their git source by a one-shot Job running `pharos index --source-git <url> --ref <ref> [--no-embed]` against a writer config. This reuses Task 1 of Phase 4. The work queue then indexes them. Removing a project from the spec runs `pharos remove` through the same kind of Job.

### Probes and lifecycle
- **Readiness:** `/health` returns 200. It is 503 while warming or draining.
- **Liveness:** `/health` answers at all, with a generous timeout.
- **Shutdown:**
  - `terminationGracePeriodSeconds` = `drainSeconds` + 30.
  - pharos's SIGTERM handling does the drain. Writers release their leases first, then the work loop stops.
  - No preStop sleep hacks beyond what the README already prescribes for readers: k8s grace ≥ `drainSeconds` + 15 s plus a margin.

### Scaling ranges and triggers
- **Ranges:** `writers.min/max` and `readers.min/max` are hard bounds. A `schedule` entry raises or lowers the floor (`min`) on a cron timetable, and never goes above `max`.
- **Triggers:** each value in `autoscaling.triggers` maps 1:1 onto pharos's proposal thresholds and windows (`cluster.status.targets.*`, `cluster.status.windows.*`). The operator renders them into the ConfigMap, so pharos raises proposals under exactly the conditions the user set. There is one place where scaling logic lives (anti-goal: divergence).
  - Pharos keys that don't exist yet, for example a separate scale-down threshold per role, are added to pharos's status config as part of this work.
- **Validation:** a trigger set that can't make sense is rejected in the resource's status before anything is applied. Examples: a scale-down load at or above the scale-up target, or `min > max`.

### Autoscaling loop
- **Watching:** the operator watches `/cluster/events` (SSE) through the router Service, with the cluster token. Every 60 s it also re-reads `/cluster/status`, so a missed event can't leave the state stale.
- **Applying proposals:**
  - `add-readers` / `remove-readers` and `add-writers` / `remove-writers` change the StatefulSet `replicas`.
  - Bounds are min/max. Each step is capped at `maxStepUp` / `maxStepDown`.
  - At most one change per role per `cooldownSeconds`.
  - The status records each application, so the same proposal id is never applied twice.
- **Scale-down:** only through the StatefulSet `replicas` count. Kubernetes removes the highest ordinal, and pharos drains it on SIGTERM. The operator waits until the removed pod's heartbeat is gone before allowing the next step down.
- **writer-disk** proposals raise a status condition and an Event. v1 does not resize PVCs automatically.
- **reshard** proposals are copied into `status.reshardProposals` and raised as an Event. The operator never applies them in v1.

### Upgrades
- **Changing `spec.version`:** readers are rolled first, then writers, then the router. This follows the N-1 index-format rule in UPGRADING.md: readers accept the new format before any writer writes it.
- **Rollout:** a StatefulSet rolling update with `maxUnavailable: 1`. The operator waits for the cluster status `ready` count between pods.

### Security
- The token is a generated Secret. It is passed to pods only through env (`cluster.tokenEnv` or equivalent), never in the ConfigMap.
- S3 credentials come only from the referenced Secret, through `accessKeyEnv` / `secretKeyEnv`.
- The example manifests include a NetworkPolicy that limits pod-to-pod traffic on the replication and internal ports to pods of the same cluster.
- RBAC follows least privilege. The operator touches only its own namespace's objects.

### Testing
- **Unit tests:** JOSDK's mock-server tests for reconcile logic, covering:
  - rendering the desired objects;
  - applying proposals with bounds, step caps and cooldown;
  - idempotency by proposal id;
  - the upgrade order.
- **End-to-end on minikube:** a script (`scripts/operator-e2e.sh`) that:
  1. installs the CRD and the operator;
  2. creates a small `PharosCluster` with MinIO and two small projects, `--no-embed` plus one with embeddings;
  3. waits for Ready;
  4. checks search results against a single-node reference;
  5. drives query load until `add-readers` appears and is applied, and checks there were 0 failed queries;
  6. removes the load and checks scale-down with 0 failed queries and no lost generation;
  7. runs a writer pod `delete --force` to check takeover;
  8. tears down.
- The end-to-end run may use the build server (16 cores) if minikube doesn't fit locally. The user approved that pattern for gates.

## Resolved details (from the builder check)

1. **Proposal episodes.** A proposal id is stable for each kind and target. The operator re-applies an active proposal once per `cooldownSeconds` while it stays in force, so it ramps toward max. It records `(id, since, appliedAt, newReplicas)` in its status. If the proposal clears and later reappears with a new `since`, that is a new episode. `clearGraceSeconds` belongs to pharos; the operator just reads the proposal state.
2. **Step size.**
   - Pharos change: each scaling proposal gets a machine-readable `delta` (a signed int) next to the prose `action`.
   - The operator steps `clamp(delta, -maxStepDown, maxStepUp)`, then clamps the result to the current min/max range.
   - Without a `delta` (an older pharos), the step is 1.
3. **Where the operator reads status.**
   - It talks to the pods directly, with the token, to get the full view:
     - first the readers' headless Service;
     - if no reader is ready, a writer's headless Service, because every role serves `/cluster/status` and `/cluster/events`.
   - It never goes through the router, which strips the token.
   - The `min` floors still apply when no node answers, so a cold start always brings readers up to `min`.
4. **Pharos-side changes are in scope, as the first task of the plan:**
   - `delta` on proposals;
   - configurable thresholds for today's hardcoded constants: `removeReadersLoad`, the writer backlog in shards and its age, and the idle window per role;
   - `cluster.tokenEnv`, so the token is read from env;
   - docs and validation for all of it.

   `spec.version` must be a release that includes these changes; the operator checks it with a pre-flight Job (below). Schedule floors are enforced by the operator itself, because they are bounds the user set, not scaling logic.
5. **Build and dependency.**
   - The operator image must not contain the pharos fat jar. Add a small `pharos-api` Maven module, compiled from the same source tree, that contains only `ClusterConfig` (plus its nested config classes) and the status records. Both pharos and the operator depend on it.
   - If that split turns out to be too invasive, the fallback is: the operator keeps its own minimal Jackson records, and a contract test parses pharos's `status-golden.json`; config is validated by a pre-flight Job that runs `pharos` with `--validate-config`, a pharos change in task 1.
6. **Project sources.**
   - Writers poll git, as in Phase 4, so every writer mounts each project's credentials Secret, if one is set, at a fixed path. A `GIT_SSH_COMMAND` or credential helper points at it.
   - The registration Job runs with the writer config: bucket plus token. It works with no writer running, because it only writes the catalog entry.
   - A change of `ref` or `url` in the spec re-runs registration, which updates the catalog source.
   - The flags `--source-git`, `--ref`, `--source-path` and `--no-embed` exist on `pharos index` and `pharos publish` (Phase 4 Task 1).
7. **Scale-down safety.**
   - Removing the highest writer ordinal is safe: on SIGTERM it finishes or aborts at a safe point, releases its leases, and other writers take over.
   - The operator removes one writer at a time and waits until that pod's heartbeat is gone and its leases are released before the next step.
   - Pod disruption budgets: readers use `maxUnavailable: 1`, not `minAvailable: min`, so a node drain is never blocked; writers also use `maxUnavailable: 1`.
8. **Upgrades.**
   - The StatefulSet keeps each ordinal's PVC across a rolling update, so writers keep their work dir and index, with no full re-index.
   - Before a version change, a pre-flight Job runs the new image with `pharos upgrade-indexes --check` against the bucket. If any shard would be refused (≥ N-2), the operator puts `UpgradeBlocked` in its status and applies nothing.
9. **Phase and conditions.**
   - **Pending:** the resource is accepted but nothing is created yet.
   - **Provisioning:** objects are created, and storage or nodes are not ready yet.
   - **Ready:**
     - storage is ready;
     - at least `min` readers are ready (`/health` 200);
     - the writers report heartbeats;
     - every project has a `LATEST`.
   - **Degraded:** any of these:
     - storage is unreachable;
     - fewer than `min` readers are ready for longer than 5 min;
     - a registration Job has failed;
     - a reader reports degraded shards;
     - `UpgradeBlocked` is set.
   - Each of these is also a Kubernetes condition with a reason.

## Decided by the user
- Java operator built on JOSDK (approach 1).
- Scaling ranges and scale triggers are configurable in the custom resource (2026-11-22).
- Scaling only in v1; resharding is the next step.
- The operator sets up the whole multi-node pharos: config plus the storage backend, either MinIO or a configured S3-compatible bucket.

## Controller's calls (correct any)
- Only the object-store backend; no peer-pull.
- StatefulSets for writers and readers. Pod names are the node ids, and every writer mounts its work dir at `/data/work`.
- Projects come from git sources declared in the custom resource, registered through Jobs.
- The proposal stream is read through the router Service.
- Upgrade order: readers, writers, router.
- writer-disk proposals produce a condition only, with no PVC resize.
- No KEDA in v1: triggers live in the custom resource and are evaluated by pharos; a built-in cron schedule handles time-based floors.

## Left to the builder
- Whether `operator/` becomes a Maven sub-module or depends on a published `pharos-api` artifact.
- The exact CRD group name, the image base and the entrypoint details.
- JOSDK version and whether to use plain JOSDK or its Quarkus extension. Plain JOSDK is preferred, for size.
