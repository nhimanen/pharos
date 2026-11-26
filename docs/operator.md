# Kubernetes operator

`pharos-operator` runs a whole [multi-node pharos cluster](cluster.md) from one `PharosCluster`
resource (`pharos.dev/v1alpha1`). From it the operator creates:

- the writers and readers as StatefulSets with volume claims and headless Services;
- the router as a Deployment with its Service;
- PodDisruptionBudgets (`maxUnavailable: 1`) and a NetworkPolicy that admits only the cluster's own pods
  and the operator to the writer and reader port;
- the rendered `config.json` of each role (validated by pharos's own config check before it is applied);
- a generated cluster token;
- with `storage.minio`, a MinIO StatefulSet and a Job that creates the bucket;
- a Job per project that registers its git source (the writers' work queue then indexes it).

Secrets reach the pods only as environment variables, never in a ConfigMap, the resource or its status.

The operator is namespaced: it watches `PharosCluster` resources in its own namespace only, and runs as
one replica.

## Install

The images are published on GitHub Container Registry for every release from 1.11.1 on:
`ghcr.io/nhimanen/pharos:<version>` and `ghcr.io/nhimanen/pharos-operator:<version>`.

```bash
kubectl apply -f deploy/crd/ -f deploy/rbac.yaml       # the CRD, the namespace "pharos", RBAC
sed -e 's/OWNER/nhimanen/g' -e 's/VERSION/1.12.0/g' deploy/operator.yaml | kubectl apply -f -
kubectl apply -f deploy/examples/minio.yaml            # or s3.yaml, after creating its Secret
kubectl -n pharos get pharoscluster demo -w            # phase, conditions, projects
```

- `deploy/rbac.yaml` creates the `pharos` namespace, the `pharos-operator` ServiceAccount, a namespaced
  Role for everything the operator manages, and a ClusterRole that can only read the operator's own CRD.
- `deploy/operator.yaml` is the operator Deployment. `PHAROS_IMAGE` (default in the file:
  `ghcr.io/OWNER/pharos`) is the pharos image repository; each cluster's `spec.version` is the tag.
- To try it on minikube with locally built images, `minikube image load` them and set the images in
  `deploy/operator.yaml` instead.

## The `PharosCluster` resource

`deploy/examples/minio.yaml` shows every field. Required: `version`, `storage`, `writers`, `readers`.

```yaml
apiVersion: pharos.dev/v1alpha1
kind: PharosCluster
metadata:
  name: demo
  namespace: pharos
spec:
  version: "1.12.0"
  storage:
    minio: { storage: 20Gi }
  embeddings: { model: granite-r2 }
  projects:
    - name: lucene
      git: { url: https://github.com/apache/lucene.git, ref: main }
      embed: false
  writers: { min: 1, max: 4, workVolume: 50Gi }
  readers: { min: 2, max: 8, mode: lazy, cacheVolume: 20Gi }
  router:  { replicas: 2, service: { type: ClusterIP } }
  autoscaling: { enabled: true }
```

| Field | Default | Meaning |
|---|---|---|
| `version` | — (required) | the pharos image tag, e.g. `1.12.0`. Changing it is an [upgrade](#upgrades-and-downgrades) |
| `storage.minio.storage` | `20Gi` | the operator deploys MinIO with a PVC of this size |
| `storage.s3.endpoint`, `.bucket`, `.region`, `.pathStyle`, `.credentialsSecret` | — | an existing S3-compatible bucket; see [Storage](#storage-minio-or-s3) |
| `embeddings.model` | `none` | `granite-r2`, `jina-v2-code` or `none` (keyword search only): the choices of `pharos embeddings enable` |
| `projects[].name` | — | letters, digits, `.`, `_` or `-`, starting with a letter or digit |
| `projects[].git.url`, `.git.ref` | — (both required) | the source; the URL must not carry credentials |
| `projects[].git.credentialsSecret` | — | a Secret with git credentials, see [Projects](#projects-from-git) |
| `projects[].embed` | `true` | `false`: index this project without embeddings |
| `writers.min` / `readers.min` | 1 | at least 1 |
| `writers.max` / `readers.max` | the role's `min` | |
| `writers.workVolume` | `20Gi` | each writer's PVC (work dir and index) |
| `writers.resources` / `readers.resources` | — | the pods' resource requests and limits |
| `readers.mode` | `lazy` | `lazy` or `full` ([reader modes](cluster.md#readers-lazy-or-full)) |
| `readers.cacheVolume` | `10Gi` | each reader's cache PVC; the block cache may fill 75 % of it |
| `router.replicas` | 2 | at least 1 |
| `router.service.type` | `ClusterIP` | `ClusterIP`, `NodePort` or `LoadBalancer` |
| `autoscaling.*` | | see [Autoscaling](#autoscaling) |
| `pharosConfig` | `{}` | extra `cluster.*` keys, see [Extra pharos settings](#extra-pharos-settings) |

`mode` and `cacheVolume` apply to readers only, `workVolume` to writers only. The operator does not resize
volumes: a changed size on an existing claim is reported as the condition `VolumeChangeNotApplied` (see
[Deleting a cluster](#deleting-a-cluster) for how to change it).

### Status

`kubectl -n pharos get pharoscluster demo -o yaml` shows:

- `phase`: `Pending`, `Provisioning`, `Ready` or `Degraded` (each is also a condition with a reason);
- conditions `StorageReady`, `ReadersReady`, `WritersReady`, `ProjectsPublished`, `UpgradeBlocked`,
  `ClusterStatusAvailable`, `WriterLowDisk`, `SpecInvalid` (with the field paths at fault) and
  `ApplyFailed`;
- `writers` and `readers` (desired and ready), `storage`, `upgrade` (while one runs);
- `projects[]`: registered, published generation, document count, pending (a split project once, over
  its active shards);
- `proposals[]` (the scaling proposals and whether they were applied) and `reshardProposals[]`.

## Storage: MinIO or S3

Set exactly one of `storage.minio` and `storage.s3`.

**MinIO** (`storage.minio`): the operator deploys MinIO with a PVC of `storage` and creates the bucket
with `mc`. MinIO no longer publishes images (`quay.io/minio/*` and `docker.io/minio/*` cannot be pulled
anymore), so the defaults are community builds of the upstream MinIO sources (AGPL), pinned to release
tags:

- `docker.io/pgsty/minio:RELEASE.2026-08-04T00-00-00Z`
- `docker.io/pgsty/mc:RELEASE.2026-11-12T00-00-00Z`

To use your own build or a mirror, set `MINIO_IMAGE` and `MC_IMAGE` on the operator Deployment
(`deploy/operator.yaml`); there is no field in the `PharosCluster` spec for it:

```yaml
env:
  - { name: MINIO_IMAGE, value: "registry.example.com/mirror/minio:RELEASE.2026-08-04T00-00-00Z" }
  - { name: MC_IMAGE, value: "registry.example.com/mirror/mc:RELEASE.2026-11-12T00-00-00Z" }
```

A changed image applies to the MinIO StatefulSet at the next reconcile; the bucket Job picks it up the
next time it is created.

**S3** (`storage.s3`): an existing S3-compatible bucket. Create the credentials Secret first; its keys
are fixed, `accessKey` and `secretKey`:

```bash
kubectl -n pharos create secret generic pharos-s3 \
  --from-literal=accessKey=... --from-literal=secretKey=...
```

```yaml
storage:
  s3:
    endpoint: https://s3.eu-north-1.amazonaws.com    # http:// or https://
    bucket: my-pharos
    region: eu-north-1
    pathStyle: false                                  # AWS virtual-hosted URLs
    credentialsSecret: pharos-s3
```

The operator always uses the key prefix `pharos` in the bucket. To move an existing hand-run bucket
cluster under the operator, point `storage.s3` at the same bucket (this works only if that cluster kept
the default prefix) and stop the hand-run writers first, so only the operator's writers take leases.

## Projects from git

Each entry in `spec.projects` becomes a registration Job that runs `pharos index --project <name>
--source-git <url> --ref <ref>` (plus `--no-embed` when `embed: false`). It only records the source in
the catalog; the writers' work queue clones and indexes it. Changing the URL, ref or `embed` runs a new
Job. Removing a project runs `pharos remove-index --from-catalog <name>`: writers release it and readers
stop serving it, and its generations stay in the bucket.

For a private repository, name a Secret in `git.credentialsSecret`. Every writer mounts it read-only at
`/etc/pharos/git/<project>`, and only that project's git calls use it. Use the Kubernetes Secret types:

```bash
# SSH deploy key (known_hosts is optional; without it the host key is pinned on first contact)
kubectl -n pharos create secret generic mypal-deploy-key --type=kubernetes.io/ssh-auth \
  --from-file=ssh-privatekey=./id_ed25519 --from-file=known_hosts=./known_hosts

# HTTPS (a token works as the password)
kubectl -n pharos create secret generic mypal-https --type=kubernetes.io/basic-auth \
  --from-literal=username=git --from-literal=password=<token>
```

```yaml
projects:
  - name: mypal
    git: { url: git@github.com:org/mypal.git, ref: main, credentialsSecret: mypal-deploy-key }
```

## Autoscaling

Replicas always stay within each role's `[min, max]`. With autoscaling on (the default; set
`autoscaling.enabled: false` to turn it off), the operator applies pharos's own
[scaling proposals](cluster.md#scaling-proposals) from `/cluster/status` within that range.

| Field | Default | Meaning |
|---|---|---|
| `autoscaling.enabled` | `true` | |
| `autoscaling.cooldownSeconds` | 300 | minimum time between two scale steps of a role (kept on the StatefulSet, so it survives an operator restart) |
| `autoscaling.maxStepUp` | 2 | most replicas added in one step |
| `autoscaling.maxStepDown` | 1 | most replicas removed in one step |

A writer is removed only once its heartbeat and its leases are gone, so its shards are handed over first.

### Triggers

`autoscaling.triggers` sets the thresholds at which pharos raises the proposals. Each field maps to one
`cluster.status` key:

| Field | pharos key | pharos default |
|---|---|---|
| `readers.scaleUp.warmP95Millis` | `targets.warmP95Millis` | 500 |
| `readers.scaleUp.qpsPerReader` | `targets.qpsPerReader` | 50 |
| `readers.scaleUp.forSeconds` | `windows.addReadersSeconds` | 300 |
| `readers.scaleDown.loadBelowPercent` | `removeReadersLoad` (as a fraction) | 20 (%), 1–99 |
| `readers.scaleDown.forSeconds` | `windows.removeReadersSeconds` | 1800 |
| `writers.scaleUp.backlogShards` | `writers.backlogShards` | 1 |
| `writers.scaleUp.oldestPendingSeconds` | `writers.oldestPendingSeconds` | 300 |
| `writers.scaleUp.forSeconds` | `windows.addWritersSeconds` | 0 |
| `writers.scaleDown.idleForSeconds` | `writers.idleSeconds` | 1800 |

### Schedules

`autoscaling.schedule` raises the floors on a timetable. Each entry has a 5-field `cron`, an optional
`timezone` (default UTC), and `readers.min` and/or `writers.min`. An entry holds from its last firing
until another entry for the same role fires; the floor never goes below the role's `min` nor above its
`max`.

```yaml
autoscaling:
  schedule:
    - { cron: "0 8 * * 1-5", timezone: Europe/Helsinki, readers: { min: 4 } }
    - { cron: "0 18 * * 1-5", timezone: Europe/Helsinki, readers: { min: 2 } }
```

## Extra pharos settings

`spec.pharosConfig` merges extra `cluster.*` keys into every role's rendered config. Dotted keys and
nesting are the same:

```yaml
pharosConfig:
  writer.reshardSyncTimeoutSeconds: 600
  reader.previousGenerationGraceSeconds: 45
  drainSeconds: 30
```

The merged config is validated as pharos validates it at startup; an unknown key or a wrong type is
reported under `SpecInvalid`. The keys the operator owns cannot be set here: `role`, `backend`,
`objectStore`, `token`, `tokenEnv`, `nodeId`, `advertiseUrl`, `cacheDir`, `writer.workQueue`,
`writer.workDir`, `writer.gitCredentials`, `reader.mode`, and the `status.*` keys that
`autoscaling.triggers` sets. `cluster.sharding` is validated but not applied (see
[Sharding](cluster.md#sharding-and-live-resharding)).

The operator sets each pod's `terminationGracePeriodSeconds` to `drainSeconds` + 30, so a raised
`drainSeconds` keeps its stop bound.

## Running pharos commands in the cluster

The pharos image's entrypoint runs one pharos command when given arguments, with the pod's config and
environment. For example, to reshard a large project:

```bash
kubectl -n pharos exec demo-writers-0 -c pharos -- pharos-entrypoint reshard lucene --spec "[project, ring(path, shards=4)]"
kubectl -n pharos exec demo-writers-0 -c pharos -- pharos-entrypoint reshard lucene --status
```

`--status` shows the requested spec version, then the job's progress, and `flipped` once readers route
the new version. (The writer logs Lucene's `Using incubator modules: jdk.incubator.vector` warning on
every command; that is expected.) A one-off Job with the same image and a writer's config works too.

The cluster's router Service is `<name>-router` (port 7080), the headless Services are
`<name>-writers-hl` and `<name>-readers-hl`, and the writer and reader pods listen on port 7070. To try
a search from your machine: `kubectl -n pharos port-forward svc/demo-router 7080:7080`, then
`curl "localhost:7080/api/search?q=parse%20options&type=keyword"`. The image has no `curl` or `wget`.

## Upgrades and downgrades

Change `spec.version`. The operator then:

1. **Pre-flight:** runs a Job with the new image, `pharos upgrade-indexes --check` against the bucket. If
   a published shard would be refused by the new release (older than N-1, or newer), the upgrade is
   blocked with the condition `UpgradeBlocked` (reason `PreflightRefused`, or `PreflightFailed` for other
   errors) and nothing is applied. The Job is retried with a backoff (60 s, doubling to 15 min) while the
   upgrade stays blocked, since the shards may have been re-indexed meanwhile.
2. Rolls the **readers**, then the **writers**, then the **router**, each once the previous role is
   rolled out and its pods heartbeat on the new version. Readers go first because they serve both the
   new format and the previous one ([N-1 rule](cluster.md#index-formats-and-upgrades)); work-queue writers
   re-index and re-publish shards whose format is out of date by themselves.

A **downgrade** (a lower `spec.version`) rolls the writers first and checks the bucket again before the
readers and the router move. Read [UPGRADING.md](../UPGRADING.md) for what each release changes; after a
reshard, do not roll readers back to a release without sharding (before 1.11.0).

### Upgrading the operator to 1.12.0: one pod roll

The 1.12.0 operator sets a new environment variable, `PHAROS_ARITHMETIC`, on every writer, reader and
router pod and on the project Jobs (`deterministic` on readers, see
[CPU-deterministic readers](cluster.md#cpu-deterministic-readers)). That changes every pod template, so
the first reconcile after the operator upgrade **restarts every pharos pod once**, even when
`spec.version` pins an older pharos image (which ignores the variable). Schedule the operator upgrade as
you would a release rollout; readers drain one at a time under their PodDisruptionBudget.

## Deleting a cluster

`kubectl -n pharos delete pharoscluster demo` removes the workloads, Services, ConfigMaps and Jobs, but
**keeps the data**:

- The MinIO PVC and its credentials Secret have no owner reference, so applying a cluster with the same
  name again adopts them, indexes and keys intact.
- Writer and reader volume claims outlive their StatefulSets.
- With `storage.s3` the operator never touches the bucket's contents.

The claims left behind are `<name>-minio`, `work-<name>-writers-<n>` and `cache-<name>-readers-<n>`;
the kept Secret is `<name>-minio`. Full teardown, data included:

```bash
kubectl -n pharos delete pharoscluster demo
kubectl -n pharos delete pvc,secret -l pharos.dev/cluster=demo
```

Deleting the cluster and applying it again under the same name is also how you change a volume size or
switch the storage kind, since the operator does not change those in place.

To remove the operator itself, delete its Deployment (`kubectl delete -f deploy/operator.yaml`) and then
the CRD (`kubectl delete -f deploy/crd/`, which deletes every remaining `PharosCluster`). Note that
`deploy/rbac.yaml` also creates the `pharos` namespace: deleting that file's objects deletes the
namespace and everything still in it, the kept volumes and Secrets included.
