package com.pharos.operator.status;

import com.pharos.cluster.status.ClusterStatus;
import com.pharos.operator.api.PharosClusterStatus.Phase;
import com.pharos.operator.status.PhaseEvaluator.Cond;
import com.pharos.operator.status.PhaseEvaluator.Input;
import com.pharos.operator.status.PhaseEvaluator.Result;
import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ConditionBuilder;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PhaseEvaluatorTest {

    private static final Instant T0 = Instant.parse("2026-11-22T10:00:00Z");

    /** A healthy cluster's observations: min 1 writer, min 2 readers, all ready, one published project. */
    private static final class Obs {
        boolean storageReady = true;
        String storageReason = "Ready";
        boolean bucketFailed = false;
        int writersReady = 1;
        int readersReady = 2;
        int routerReady = 2;
        ClusterStatus status = status(List.of("demo-writers-0"), List.of());
        Map<String, Long> projects = new LinkedHashMap<>(Map.of("lucene", 7L));
        List<String> projectFailures = List.of();
        boolean blocked = false;
        List<Condition> previous = null;
        Instant now = T0;

        Input input() {
            return new Input(storageReady, storageReason, "storage message", bucketFailed, true, 1, writersReady, 2,
                    readersReady, routerReady, status, projects, projectFailures,
                    projectFailures.isEmpty() ? null : "RegistrationJobFailed", blocked,
                    blocked ? "PreflightRefused" : "UpToDate", blocked ? "Upgrade 1.6.0 -> 1.7.0: refused" : "up to date",
                    previous, now);
        }

        Result eval() {
            Result r = PhaseEvaluator.evaluate(input());
            previous = conditions(r, previous, now); // as the reconciler keeps transition times
            return r;
        }
    }

    static ClusterStatus status(List<String> writers, List<String> degradedReaders) {
        List<ClusterStatus.Reader> readers = new ArrayList<>();
        for (String r : degradedReaders) {
            readers.add(new ClusterStatus.Reader(r, null, List.of("p"), "degraded", List.of("p"), Map.of(), 0, 0, 0, null,
                    null, 1));
        }
        return new ClusterStatus("cluster", T0.toString(), new ClusterStatus.Layout(null, List.of(),
                writers.stream().map(w -> new ClusterStatus.Writer(w, true, List.of(), List.of(), null, 0, false, null, 1))
                        .toList(), readers), null, List.of(), List.of());
    }

    /** The conditions as StatusConditions would build them: a transition time kept while the status stays. */
    static List<Condition> conditions(Result r, List<Condition> previous, Instant now) {
        List<Condition> out = new ArrayList<>();
        for (Cond c : r.conditions()) {
            String s = c.status() ? "True" : "False";
            String since = previous == null ? now.toString() : previous.stream()
                    .filter(p -> p.getType().equals(c.type()) && p.getStatus().equals(s))
                    .map(Condition::getLastTransitionTime).findFirst().orElse(now.toString());
            out.add(new ConditionBuilder().withType(c.type()).withStatus(s).withReason(c.reason())
                    .withMessage(c.message()).withLastTransitionTime(since).build());
        }
        return out;
    }

    private static Cond cond(Result r, String type) {
        return r.conditions().stream().filter(c -> c.type().equals(type)).findFirst().orElseThrow();
    }

    @Test
    void readyRequiresStorageReadersWritersAndLatest() {
        Obs o = new Obs();
        Result ready = o.eval();
        assertThat(ready.phase()).isEqualTo(Phase.Ready);
        for (String t : List.of("Ready", "StorageReady", "ReadersReady", "WritersReady", "ProjectsPublished")) {
            assertThat(cond(ready, t).status()).as(t).isTrue();
        }
        for (String t : List.of("Pending", "Provisioning", "Degraded", "UpgradeBlocked")) {
            assertThat(cond(ready, t).status()).as(t).isFalse();
        }
        assertThat(cond(ready, "ProjectsPublished").reason()).isEqualTo("Published");

        // each requirement on its own keeps the cluster Provisioning, with a condition saying which
        Obs storage = new Obs();
        storage.storageReady = false;
        storage.storageReason = "BucketPending";
        Result r = storage.eval();
        assertThat(r.phase()).isEqualTo(Phase.Provisioning);
        assertThat(cond(r, "StorageReady").reason()).isEqualTo("BucketPending");
        assertThat(cond(r, "Provisioning").message()).isEqualTo("Waiting for the bucket.");

        Obs readers = new Obs();
        readers.readersReady = 1;
        r = readers.eval();
        assertThat(r.phase()).isEqualTo(Phase.Provisioning);
        assertThat(cond(r, "ReadersReady").reason()).isEqualTo("Waiting");
        assertThat(cond(r, "ReadersReady").message()).isEqualTo("1/2 readers ready (/health 200).");

        Obs noHeartbeat = new Obs();
        noHeartbeat.status = status(List.of(), List.of());
        r = noHeartbeat.eval();
        assertThat(r.phase()).isEqualTo(Phase.Provisioning);
        assertThat(cond(r, "WritersReady").reason()).isEqualTo("NoHeartbeats");
        Obs noStatus = new Obs();
        noStatus.status = null;
        r = noStatus.eval();
        assertThat(r.phase()).isEqualTo(Phase.Provisioning);
        assertThat(cond(r, "WritersReady").reason()).isEqualTo("NoClusterStatus");
        Obs writerPods = new Obs();
        writerPods.writersReady = 0;
        r = writerPods.eval();
        assertThat(cond(r, "WritersReady").reason()).isEqualTo("Waiting");
        assertThat(cond(r, "Provisioning").message()).isEqualTo("Waiting for writers (0/1 ready).");

        Obs latest = new Obs();
        latest.projects.put("kafka", null); // registered, no LATEST yet
        r = latest.eval();
        assertThat(r.phase()).isEqualTo(Phase.Provisioning);
        assertThat(cond(r, "ProjectsPublished").status()).isFalse();
        assertThat(cond(r, "ProjectsPublished").message()).contains("kafka").doesNotContain("lucene");
        Obs none = new Obs();
        none.projects.clear();
        assertThat(cond(none.eval(), "ProjectsPublished").reason()).isEqualTo("NoProjects");

        Obs router = new Obs();
        router.routerReady = 0;
        assertThat(router.eval().phase()).isEqualTo(Phase.Provisioning);
    }

    @Test
    void degradedWhenReadersBelowMinFor5Min() {
        // a cold start that takes longer than 5 min stays Provisioning: the readers were never ready
        Obs cold = new Obs();
        cold.readersReady = 0;
        cold.eval();
        cold.now = T0.plus(Duration.ofMinutes(30));
        Result stillCold = cold.eval();
        assertThat(stillCold.phase()).isEqualTo(Phase.Provisioning);
        assertThat(stillCold.recheck()).isNull();

        // ready, then a reader lost: Provisioning for 5 min (with a recheck), then Degraded
        Obs o = new Obs();
        assertThat(o.eval().phase()).isEqualTo(Phase.Ready);
        o.readersReady = 1;
        o.now = T0.plusSeconds(60);
        Result lost = o.eval();
        assertThat(lost.phase()).isEqualTo(Phase.Provisioning);
        assertThat(cond(lost, "ReadersReady").reason()).isEqualTo("BelowMin");
        assertThat(lost.recheck()).isEqualTo(Duration.ofMinutes(5));
        o.now = T0.plusSeconds(60 + 299);
        Result almost = o.eval();
        assertThat(almost.phase()).isEqualTo(Phase.Provisioning);
        assertThat(almost.recheck()).isEqualTo(Duration.ofSeconds(1));
        o.now = T0.plusSeconds(60 + 300);
        Result degraded = o.eval();
        assertThat(degraded.phase()).isEqualTo(Phase.Degraded);
        assertThat(cond(degraded, "Degraded").reason()).isEqualTo("ReadersBelowMin");
        assertThat(cond(degraded, "Ready").reason()).isEqualTo("Degraded");
        // and back
        o.readersReady = 2;
        o.now = T0.plusSeconds(1000);
        assertThat(o.eval().phase()).isEqualTo(Phase.Ready);
    }

    @Test
    void otherDegradedCauses() {
        Obs blocked = new Obs();
        blocked.blocked = true;
        Result r = blocked.eval();
        assertThat(r.phase()).isEqualTo(Phase.Degraded);
        assertThat(cond(r, "Degraded").reason()).isEqualTo("UpgradeBlocked");
        assertThat(cond(r, "UpgradeBlocked").status()).isTrue();
        assertThat(cond(r, "UpgradeBlocked").reason()).isEqualTo("PreflightRefused");

        Obs shards = new Obs();
        shards.status = status(List.of("demo-writers-0"), List.of("demo-readers-1"));
        r = shards.eval();
        assertThat(r.phase()).isEqualTo(Phase.Degraded);
        assertThat(cond(r, "Degraded").reason()).isEqualTo("ReaderDegradedShards");
        assertThat(cond(r, "Degraded").message()).contains("demo-readers-1").contains("p");

        Obs job = new Obs();
        job.projectFailures = List.of("project lucene: Job x failed");
        r = job.eval();
        assertThat(cond(r, "Degraded").reason()).isEqualTo("RegistrationJobFailed");

        Obs bucket = new Obs();
        bucket.storageReady = false;
        bucket.storageReason = "BucketJobFailed";
        bucket.bucketFailed = true;
        r = bucket.eval();
        assertThat(r.phase()).isEqualTo(Phase.Degraded);
        assertThat(cond(r, "Degraded").reason()).isEqualTo("BucketJobFailed");

        // storage that was ready and is lost is unreachable: Degraded at once (MinIO not starting yet is not)
        Obs lost = new Obs();
        lost.eval();
        lost.storageReady = false;
        lost.storageReason = "MinioNotReady";
        r = lost.eval();
        assertThat(r.phase()).isEqualTo(Phase.Degraded);
        assertThat(cond(r, "Degraded").reason()).isEqualTo("StorageUnreachable");
        assertThat(cond(r, "StorageReady").reason()).isEqualTo("Unreachable");
        assertThat(cond(lost.eval(), "StorageReady").reason()).isEqualTo("Unreachable"); // remembered while False
        Obs starting = new Obs();
        starting.storageReady = false;
        starting.storageReason = "MinioNotReady";
        assertThat(starting.eval().phase()).isEqualTo(Phase.Provisioning);

        // several causes: UpgradeBlocked first, every message kept
        Obs many = new Obs();
        many.blocked = true;
        many.projectFailures = List.of("project lucene: Job x failed");
        r = many.eval();
        assertThat(cond(r, "Degraded").reason()).isEqualTo("UpgradeBlocked");
        assertThat(cond(r, "Degraded").message()).isEqualTo("Upgrade 1.6.0 -> 1.7.0: refused; project lucene: Job x failed");
    }

    @Test
    void anUnchangedClusterGivesTheSameConditions() {
        Obs o = new Obs();
        Result a = o.eval();
        o.now = T0.plusSeconds(3600);
        assertThat(o.eval().conditions()).isEqualTo(a.conditions());
    }
}
