# Pharos Kubernetes Operator (v1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One `PharosCluster` resource sets up a complete multi-node pharos on Kubernetes: writers, readers, router, config, token, and either an operator-managed MinIO or an external S3-compatible bucket. The operator then scales readers and writers from pharos's own proposals, within user-set ranges, triggers and schedules.

**Architecture:**
- **Module and runtime:** a Java Operator SDK (JOSDK, plain, no Quarkus) operator in a new `operator/` Maven module. It shares a thin `pharos-api` module, holding `ClusterConfig` and the status records, with pharos itself.
- **Desired state:** pharos runs as StatefulSets for writers and readers, plus a Deployment for the router. The operator renders their config from the resource.
- **Scaling decisions:** pharos decides what scaling is needed (proposals carry a machine-readable `delta`). The operator applies proposals with bounds, step caps, cooldown and drain-aware scale-down.

**Tech Stack:** Java 25, Maven, Java Operator SDK 5.x with the fabric8 Kubernetes client (pick the newest release at least two weeks old and record it), JUnit 5, fabric8 mock server, Docker image built from a `Dockerfile`, minikube for end-to-end tests.

**Spec:** `docs/superpowers/specs/2026-11-22-k8s-operator-design.md` (approved 2026-11-22). Read it whole before Task 1. Its "Resolved details" section is binding.

**Base:** branch `feat/k8s-operator`, forked from `feat/cluster-phase-5`, which has Phases 0–5. Resharding (Phase 6) is a non-goal for v1. The operator only surfaces reshard proposals.

## Global Constraints

- **Storage:** object-store backend only. `storage.minio` (operator-managed) or `storage.s3` (endpoint, bucket, region, pathStyle, `credentialsSecret` with keys `accessKey` and `secretKey`). No peer-pull.
- **Secrets:** they never appear in the custom resource, its status, a ConfigMap, Events or logs. The token reaches pods only through env (`cluster.tokenEnv`). S3 keys only through `accessKeyEnv` and `secretKeyEnv`.
- **Scaling:** pharos's proposals decide. The operator only applies them:
  - step = `clamp(delta, -maxStepDown, maxStepUp)`, then clamped to `[min, max]`;
  - at most one change per role per `cooldownSeconds`;
  - re-applied once per cooldown while the proposal stays in force.

  The defaults are `cooldownSeconds: 300`, `maxStepUp: 2` and `maxStepDown: 1`. A proposal without `delta`, from an older pharos, steps by 1.
- **Scale-down:** only by lowering StatefulSet `replicas`. One writer at a time: wait until the removed pod's heartbeat is gone and its leases are released before the next step down. `terminationGracePeriodSeconds` = `drainSeconds` + 30.
- **Min floors:** they hold even when no pharos node answers, so a cold start always reaches `min`. `schedule` entries (cron plus timezone) move the `min` floor, never above `max`.
- **Status source:** the operator reads `/cluster/status` and `/cluster/events` directly from pods with the token: first the readers' headless Service, then a writer's. Never through the router, which strips the token.
- **Writers:** `workQueue: true`. The work dir is mounted at `/data/work` on every writer. Project git credentials Secrets are mounted on every writer.
- **Upgrades:**
  - Roll in the order readers, then writers, then the router.
  - Before that, a pre-flight Job runs the new image with `pharos upgrade-indexes --check` against the bucket. Any refused shard sets `UpgradeBlocked`, and nothing is applied.
- **Pod disruption budgets:** `maxUnavailable: 1` for both readers and writers.
- **Status phases:** Pending, Provisioning, Ready and Degraded, defined exactly as in spec Resolved detail 9. Each is also a Kubernetes condition with a reason.
- **Images:**
  - `pharos` (Temurin 25 JRE, fat jar, python3, node);
  - `pharos-operator` (no pharos fat jar inside).
- **Isolation:** tests never touch `~/.pharos` or :7171, and `./pharos` stays in sync with any CLI change.
- **Process (user):** only Critical and Important review findings are fixed. Minor findings are skipped.

## Review Focus

1. **A proposal that stays in force across a cooldown, then clears and reappears.** The operator ramps once per cooldown up to `max`. A reappearance with a new `since` starts a new episode, and the operator never applies twice inside one cooldown, even across an operator restart. Its state comes from `status`. Test owned by Task 7.
2. **Scale-down while a writer holds leases mid-index.** The operator lowers `replicas` by one, waits for that pod's heartbeat to vanish and its leases to clear, and only then allows another step. No failed queries and no lost generation. Covered by Task 7's unit test and Task 9's end-to-end run.
3. **No pharos node answers** (cold start, a rollout, or every reader at 503). The operator still enforces the `min` floors, applies no proposals, and keeps its status honest: Provisioning or Degraded. Test owned by Task 7.
4. **A secret could leak.** The token and S3 keys must never appear in any rendered ConfigMap, in the resource's status, in Events or in operator logs. Test owned by Task 5: a scan of every rendered object and of captured logs.
5. **An upgrade to a version that would refuse existing indexes (≥ N-2).** The pre-flight Job fails, the operator sets `UpgradeBlocked` and changes no workload. Test owned by Task 8.

---

### Task 1: Pharos-side support for the operator

**Files:**
- Modify:
  - `src/main/java/com/pharos/cluster/status/Proposals.java`: the `delta` field and the configurable thresholds;
  - `ClusterStatus.java`: the record field;
  - `config/ClusterConfig.java`: `tokenEnv`, the new `status.*` keys and validation;
  - `cli/` + `Main.java`: a new `pharos config validate <file>` command;
  - `./pharos`;
  - README;
  - CHANGELOG.
- Test:
  - `ProposalsTest`;
  - `ClusterConfigTest`;
  - a new `ConfigValidateCommandTest`;
  - `GoldenStatusTest`, with the fixtures regenerated.

**Interfaces:**
- **Produces:**
  - each scaling proposal JSON gains `"delta": int`, with a sign: +N for add, −N for remove; the reshard and writer-disk proposals carry none;
  - config keys `cluster.status.removeReadersLoad` (default 0.2), `cluster.status.writers.backlogShards` (default 1), `cluster.status.writers.oldestPendingSeconds` (default 300) and `cluster.status.writers.idleSeconds` (default 1800);
  - `cluster.tokenEnv`, which reads the token from that env var. It is mutually exclusive with `token`, and validation fails if both are set;
  - CLI `pharos config validate <file>`: exit 0 and "ok", or exit 2 with the validation message.

- [ ] Write the failing tests:
  - `addReadersCarriesPositiveDelta`;
  - `removeReadersCarriesNegativeDelta`;
  - `removeReadersLoadIsConfigurable`;
  - `writerBacklogTriggersUseConfiguredShardsAndAge`;
  - `tokenFromEnv`;
  - `tokenAndTokenEnvTogetherIsInvalid`;
  - `configValidateReportsErrorsWithExit2`.
- [ ] Run them and confirm they fail.
- [ ] Implement. The delta is computed from the same numbers the proposal's `action` prose already uses, so it stays consistent with the prose.
- [ ] Run the focused tests, then the full `mvn test`, on the build server if local memory is under 5 GB.
- [ ] Commit `status+config: proposal delta, configurable scale thresholds, tokenEnv, config validate`.

### Task 2: `pharos-api` module

**Files:**
- Modify: root `pom.xml`, which becomes a parent with the modules `pharos-api`, `pharos` (the current code, moved or kept as the root module) and `operator` (empty for now).
- Create: `pharos-api/pom.xml`.
- Move into `pharos-api`: `ClusterConfig` with its nested config classes, the status records (`ClusterStatus` and the records it references), and nothing that pulls in Lucene, ONNX or Javalin.
- Test: the existing tests keep passing. Add `ApiModuleHasNoHeavyDepsTest`, which asserts the `pharos-api` dependency tree has no `org.apache.lucene`, `ai.djl`, `com.microsoft.onnxruntime` or `io.javalin`.

**Interfaces:**
- **Produces:** artifact `com.pharos:pharos-api` with packages `com.pharos.config` (config classes) and `com.pharos.cluster.status` (records only).
- **Fallback:** if a class that must move drags heavy deps along and can't be cut cleanly, stop and record the reason in the report. The operator then uses its own Jackson records, plus a contract test against `src/test/resources/cluster/status-golden.json`, and validates config through Task 1's `pharos config validate` in a Job.

- [ ] Write `ApiModuleHasNoHeavyDepsTest`.
- [ ] Split the modules. The fat jar output path and name (`target/pharos-*.jar`), the release workflow and `setup.sh` must be unchanged, or updated in the same commit.
- [ ] Run `mvn -B clean package`, then the full suite on the build server.
- [ ] Commit `build: pharos-api module for config and status records`.

### Task 3: The `pharos` container image

**Files:**
- Create:
  - `docker/pharos/Dockerfile`;
  - `docker/pharos/entrypoint.sh`;
  - `scripts/image-smoke.sh`;
  - a README section.

**Interfaces:**
- **Produces:**
  - image `pharos:<version>`;
  - entrypoint contract:
    - `PHAROS_ROLE` = `writer` | `reader` | `router`;
    - `/etc/pharos/config.json` is mounted from a ConfigMap;
    - the env vars named by the config hold the secrets;
    - `PHAROS_HOME=/data/pharos`, the model cache, on the PVC;
    - it `exec`s `pharos web` (writer or reader) or `pharos router`, so SIGTERM reaches the JVM directly;
  - exposed port 7070, plus the `cluster.tls.localPort` + 1 convention only if TLS is set, which v1 doesn't use;
  - `HEALTHCHECK` is off, because Kubernetes probes are used instead.

- [ ] Write the Dockerfile:
  - multi-stage: Maven build, then `eclipse-temurin:25-jre` with python3 and node;
  - runs as a non-root user with uid 10001.
- [ ] Write `scripts/image-smoke.sh`:
  - it builds the image and runs a single node (`role: all`) with a tiny project volume;
  - it waits for `/health` 200;
  - it searches once.
  - Exit 0 means pass.
  - It needs a Docker daemon. If none is local, run it on a host that has one and record which. The local machine has no Docker daemon running.
- [ ] Run the smoke script.
- [ ] Extend `.github/workflows/release.yml` so a release tag publishes `ghcr.io/<owner>/pharos:<version>`, and, once Task 4 exists, `pharos-operator:<version>`. Leave the existing jar and asset steps unchanged.
- [ ] Commit `docker: pharos image`.

### Task 4: Operator skeleton, the CRD and spec validation

**Files:**
- Create:
  - `operator/pom.xml`;
  - `operator/src/main/java/com/pharos/operator/OperatorMain.java`;
  - `api/PharosCluster.java`, `api/PharosClusterSpec.java` with nested `Storage`, `Minio`, `S3`, `Project`, `Git`, `RoleSpec`, `RouterSpec`, `Autoscaling`, `Triggers` and `ScheduleEntry`, and `api/PharosClusterStatus.java`;
  - `validate/SpecValidator.java`;
  - `deploy/crd/` (generated);
  - `deploy/rbac.yaml`;
  - `deploy/operator.yaml`;
  - `deploy/examples/minio.yaml`;
  - `deploy/examples/s3.yaml`;
  - `docker/operator/Dockerfile`.
- Test: `SpecValidatorTest`, `CrdGenerationTest`.

**Interfaces:**
- **Produces:**
  - the CRD group `pharos.dev`, version `v1alpha1`, kind `PharosCluster`, namespaced, with the status subresource;
  - the field names exactly as in the spec's YAML example;
  - `SpecValidator.validate(PharosClusterSpec) → List<String> errors`.

- [ ] Write the failing tests:
  - `minAboveMaxRejected`;
  - `scaleDownLoadAtOrAboveScaleUpRejected`, meaning `readers.scaleDown.loadBelowPercent` must be below the load that `scaleUp.qpsPerReader` implies; compare as fractions of the target;
  - `exactlyOneStorageKind`;
  - `scheduleMinAboveMaxRejected`;
  - `badCronRejected`;
  - `duplicateProjectNamesRejected`;
  - `crdGeneratedMatchesCommitted`.
- [ ] Implement:
  - the CRD is generated by JOSDK's CRD generator during the build;
  - an invalid spec sets the status `phase: Pending` with a `SpecInvalid` condition listing the errors;
  - it creates nothing.
- [ ] Run the tests.
- [ ] Commit `operator: PharosCluster CRD, spec validation, deploy manifests`.

### Task 5: Desired-state rendering and reconcile of the workloads

**Files:**
- Create in `operator/src/main/java/com/pharos/operator/`:
  - `render/ConfigRenderer.java`: renders `cluster.*` per role from the spec. It maps the triggers onto `cluster.status.*` and its windows, and validates with `ClusterConfig.validate()`;
  - `render/WorkloadRenderer.java`: renders the StatefulSets, Deployment, Services, PDBs and NetworkPolicy;
  - `render/StorageRenderer.java`: renders the MinIO StatefulSet, Service, PVC, credentials Secret and bucket Job, or the references to the S3 Secret;
  - `reconcile/PharosClusterReconciler.java`: JOSDK dependent resources.
- Test:
  - `ConfigRendererTest`;
  - `WorkloadRendererTest`;
  - `StorageRendererTest`;
  - `ReconcilerMockServerTest`, using the fabric8 mock server;
  - `NoSecretsRenderedTest` (Review Focus 4).

**Interfaces:**
- **Consumes:** Task 1's config keys, Task 2's `ClusterConfig`, Task 3's image contract and Task 4's spec classes.
- **Produces:**
  - object names: `<name>-token`, `<name>-config-writer`, `<name>-config-reader`, `<name>-config-router`, `<name>-minio`, `<name>-writers`, `<name>-readers`, `<name>-router`;
  - headless Services `<name>-writers-hl` and `<name>-readers-hl`;
  - `advertiseUrl` = `http://$(POD_NAME).<name>-<role>s-hl.<ns>.svc:7070`, with `nodeId` = the pod name;
  - the writer PVC template `work`, mounted at `/data/work`; the reader PVC template `cache`.

- [ ] Write the failing tests:
  - `triggersMapOneToOneOntoStatusConfig`;
  - `writerConfigHasWorkQueueAndSharedWorkDir`;
  - `readerConfigHasModeAndCacheCap`;
  - `minioModeCreatesBucketJobAndCredentials`;
  - `s3ModeReferencesSecretOnly`;
  - `gracePeriodIsDrainPlus30`;
  - `pdbsAreMaxUnavailableOne`;
  - `networkPolicyLimitsReplicationAndInternalPorts`;
  - `reconcileCreatesEverythingIdempotently`, where a second reconcile changes nothing;
  - `noSecretAppearsInAnyRenderedObject`, scanning every rendered object's YAML for the token and keys.
- [ ] Implement.
- [ ] Run the tests.
- [ ] Commit `operator: render and reconcile pharos workloads and storage`.

### Task 6: Projects and git sources

**Files:**
- Create:
  - `operator/.../reconcile/ProjectReconciler.java`: registration and removal Jobs;
  - `render/ProjectJobRenderer.java`.
- Modify: `WorkloadRenderer`, which mounts each project's `credentialsSecret` on every writer under `/etc/pharos/git/<project>/` and sets `GIT_SSH_COMMAND` or a credential helper per project.
- Test: `ProjectReconcilerTest`.

**Interfaces:**
- **Produces:**
  - registration Job `<name>-register-<project>-<hash(spec)>`, which runs `pharos index --source-git <url> --ref <ref> [--no-embed] --project <name>` with the writer config, the token and the bucket;
  - removal Job `<name>-remove-<project>`, which runs `pharos remove`;
  - project state in the status: `{name, registered, generation, docCount, pending}`. The status part comes from `/cluster/status`.

- [ ] Write the failing tests:
  - `newProjectRunsRegistrationJobOnce`;
  - `refChangeReRegisters`, with a new hash and a new Job;
  - `removedProjectRunsRemovalJob`;
  - `failedJobSetsDegradedWithReason`;
  - `credentialsMountedOnEveryWriter`.
- [ ] Implement.
- [ ] Run the tests.
- [ ] Commit `operator: register projects from git sources`.

### Task 7: Status client and autoscaler

**Files:**
- Create in `operator/src/main/java/com/pharos/operator/`:
  - `status/ClusterStatusClient.java`: GETs `/cluster/status` and subscribes to `/cluster/events` with the token. It targets the readers' headless Service, then falls back to a writer's. It re-reads every 60 s;
  - `scale/Autoscaler.java`;
  - `scale/ScheduleFloors.java`: cron plus timezone, using a small cron parser library or a vetted minimal one;
  - `scale/ScaleDownGate.java`.
- Test: `AutoscalerTest`, `ScheduleFloorsTest`, `ScaleDownGateTest`, `StatusClientTest`.

**Interfaces:**
- **Consumes:** Task 1's `delta`, and Task 5's names and status.
- **Produces:**
  - `Autoscaler.decide(Role, int current, Bounds, List<Proposal>, AppliedHistory, Instant now) → Decision{target, reason, proposalId}`;
  - `status.proposals[{id, kind, since, applied, appliedAt, fromReplicas, toReplicas}]`;
  - `status.reshardProposals`, copied as they are.

- [ ] Write the failing tests:
  - `rampsOncePerCooldownWhileProposalInForce` (Review Focus 1);
  - `newSinceIsNewEpisode`;
  - `noDoubleApplyAcrossOperatorRestart`, with the state read from the status;
  - `stepClampedByMaxStepAndBounds`;
  - `missingDeltaStepsByOne`;
  - `scheduleRaisesFloorNeverAboveMax`;
  - `minFloorsEnforcedWhenNoNodeAnswers` (Review Focus 3);
  - `writerScaleDownWaitsForHeartbeatAndLeaseRelease` (Review Focus 2);
  - `readerScaleDownOneStepPerCooldown`;
  - `reshardProposalSurfacedNeverApplied`;
  - `writerDiskRaisesConditionOnly`;
  - `statusClientFallsBackToWriter`.
- [ ] Implement.
- [ ] Run the tests.
- [ ] Commit `operator: proposal-driven autoscaling with ranges, triggers and schedules`.

### Task 8: Status phases and upgrades

**Files:**
- Create in `operator/src/main/java/com/pharos/operator/`:
  - `status/PhaseEvaluator.java`;
  - `upgrade/UpgradeCoordinator.java`;
  - `render/PreflightJobRenderer.java`.
- Test: `PhaseEvaluatorTest`, `UpgradeCoordinatorTest`.

**Interfaces:**
- **Produces:**
  - phases and conditions exactly as in spec Resolved detail 9, with condition types `StorageReady`, `ReadersReady`, `WritersReady`, `ProjectsPublished`, `UpgradeBlocked` and `SpecInvalid`;
  - the upgrade flow: run the pre-flight Job `<name>-preflight-<version>` (`pharos upgrade-indexes --check`), then roll readers, then writers, then the router. Each step waits until `/cluster/status` shows the rolled pods ready.

- [ ] Write the failing tests:
  - `readyRequiresStorageReadersWritersAndLatest`;
  - `degradedWhenReadersBelowMinFor5Min`;
  - `upgradeBlockedWhenPreflightRefuses` (Review Focus 5);
  - `rollOrderReadersWritersRouter`;
  - `noWorkloadChangeWhileBlocked`.
- [ ] Implement.
- [ ] Run the tests.
- [ ] Commit `operator: phases, conditions and ordered upgrades`.

### Task 9: End-to-end on minikube

**Files:**
- Create:
  - `scripts/operator-e2e.sh`;
  - `scripts/operator_e2e.py`;
  - `docs/superpowers/gates/<date>-operator.md`.

**Procedure:** spec "Testing", end-to-end steps 1–8.
1. Install the CRD and the operator.
2. Create a small `PharosCluster` with MinIO and two small projects: one `--no-embed`, one with embeddings.
3. Wait for Ready.
4. Search, and compare against a single-node reference: the top-10 must be identical.
5. Drive query load until `add-readers` appears and is applied.
6. Remove the load, and check that the cluster scales down.
7. `kubectl delete pod --force` on a writer that holds a lease, and check takeover.
8. Tear down.

**Pass criteria:**
- 0 failed queries throughout steps 5–7;
- no lost or duplicate generation, checked from the manifests' `parentGeneration`;
- scaling stays within min/max and step caps;
- no secret in `kubectl get -o yaml` of any object, or in operator logs.

- [ ] **Prerequisites need the user's go:** installing minikube and a container runtime. The local Docker daemon isn't running, and the build server has no Docker. Ask before installing anything, and record which host and driver are used.
- [ ] Write the scripts.
- [ ] Run them and record the results.
- [ ] Commit `operator: minikube end-to-end gate`.
