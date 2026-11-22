package com.pharos.operator.upgrade;

import com.pharos.cluster.status.ClusterStatus;
import com.pharos.operator.api.PharosClusterStatus.UpgradeStatus;
import com.pharos.operator.render.DesiredState;
import com.pharos.operator.upgrade.UpgradeCoordinator.Input;
import com.pharos.operator.upgrade.UpgradeCoordinator.Plan;
import com.pharos.operator.upgrade.UpgradeCoordinator.Preflight;
import com.pharos.operator.upgrade.UpgradeCoordinator.PreflightState;
import com.pharos.operator.upgrade.UpgradeCoordinator.VersionSince;
import com.pharos.operator.upgrade.UpgradeCoordinator.Workload;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class UpgradeCoordinatorTest {

    private static final Instant T0 = Instant.parse("2026-11-22T10:00:00Z");

    private static Workload on(String version) {
        return new Workload(version, true, 2, null);
    }

    private static Input input(Workload readers, Workload writers, Workload router, Preflight preflight,
                               ClusterStatus status, UpgradeStatus previous, Instant now) {
        return new Input("demo", "1.7.0", readers, writers, router, preflight, List.of(), true, status, previous, now);
    }

    private static Preflight preflight(PreflightState state) {
        return new Preflight(state, state == PreflightState.REFUSED || state == PreflightState.FAILED ? T0 : null,
                state == PreflightState.REFUSED ? "PodFailurePolicy" : state == PreflightState.FAILED
                        ? "BackoffLimitExceeded" : null);
    }

    /** {@code /cluster/status} with live reader and writer heartbeats started at {@code startedAt}. */
    private static ClusterStatus heartbeats(Instant startedAt, List<String> readers, List<String> writers) {
        long ms = startedAt.toEpochMilli();
        return new ClusterStatus("cluster", T0.toString(), new ClusterStatus.Layout(null, List.of(),
                writers.stream().map(n -> new ClusterStatus.Writer(n, true, List.of(), List.of(), null, ms, false, null, 1))
                        .toList(),
                readers.stream().map(n -> new ClusterStatus.Reader(n, null, List.of(), "ready", List.of(), Map.of(), 0, 0,
                        ms, null, null, 1)).toList()),
                null, List.of(), List.of());
    }

    private static final List<String> READER_PODS = List.of("demo-readers-0", "demo-readers-1");
    private static final List<String> WRITER_PODS = List.of("demo-writers-0", "demo-writers-1");

    @Test
    void steadyClusterAndFreshClusterRenderTheSpecVersion() {
        Plan fresh = UpgradeCoordinator.plan(input(Workload.ABSENT, Workload.ABSENT, Workload.ABSENT, Preflight.ABSENT,
                null, null, T0));
        assertThat(fresh.versions()).isEqualTo(new DesiredState.Versions("1.7.0", "1.7.0", "1.7.0", null, null, null));
        assertThat(fresh.createPreflight()).isFalse(); // a new cluster is no version change
        assertThat(fresh.upgrade()).isNull();
        assertThat(fresh.blocked()).isFalse();
        assertThat(fresh.reason()).isEqualTo(UpgradeCoordinator.REASON_UP_TO_DATE);

        // up to date: annotations carried, every pre-flight Job (other versions, the spec's finished one) deleted
        Workload readers = new Workload("1.7.0", true, 2, "1.7.0@2026-11-21T09:00:00Z");
        Plan done = UpgradeCoordinator.plan(new Input("demo", "1.7.0", readers, on("1.7.0"), on("1.7.0"),
                preflight(PreflightState.SUCCEEDED), List.of("demo-preflight-1.6.1"), true, null, null, T0));
        assertThat(done.versions().readersSince()).isEqualTo("1.7.0@2026-11-21T09:00:00Z");
        assertThat(done.deleteJobs()).containsExactly("demo-preflight-1.6.1", "demo-preflight-1.7.0");
        assertThat(done.upgrade()).isNull();
    }

    /** Review Focus 5: a pre-flight check that refuses a shard blocks the upgrade and moves nothing. */
    @Test
    void upgradeBlockedWhenPreflightRefuses() {
        Plan refused = UpgradeCoordinator.plan(input(on("1.6.0"), on("1.6.0"), on("1.6.0"),
                preflight(PreflightState.REFUSED), null, new UpgradeStatus("1.6.0", "1.7.0", "Preflight", null),
                T0.plusSeconds(10)));
        assertThat(refused.blocked()).isTrue();
        assertThat(refused.reason()).isEqualTo(UpgradeCoordinator.REASON_PREFLIGHT_REFUSED);
        assertThat(refused.message()).contains("demo-preflight-1.7.0").contains("would refuse")
                .contains("No workload is changed").contains("set spec.version back to 1.6.0").contains("after 60 s");
        assertThat(refused.versions()).isEqualTo(new DesiredState.Versions("1.6.0", "1.6.0", "1.6.0", null, null, null));
        assertThat(refused.createPreflight()).isFalse();
        assertThat(refused.deleteJobs()).isEmpty(); // within the backoff
        assertThat(refused.recheck()).isEqualTo(Duration.ofSeconds(50));
        assertThat(refused.upgrade()).isEqualTo(new UpgradeStatus("1.6.0", "1.7.0", "Preflight", null));

        // after the backoff: the Job is deleted (re-created next time), the retry counted, still blocked
        Plan retry = UpgradeCoordinator.plan(input(on("1.6.0"), on("1.6.0"), on("1.6.0"),
                preflight(PreflightState.REFUSED), null, refused.upgrade(), T0.plusSeconds(60)));
        assertThat(retry.deleteJobs()).containsExactly("demo-preflight-1.7.0");
        assertThat(retry.upgrade().preflightRetries()).isEqualTo(1);
        assertThat(retry.blocked()).isTrue();
        assertThat(retry.versions().readers()).isEqualTo("1.6.0");

        Plan rerun = UpgradeCoordinator.plan(input(on("1.6.0"), on("1.6.0"), on("1.6.0"), Preflight.ABSENT, null,
                retry.upgrade(), T0.plusSeconds(61)));
        assertThat(rerun.createPreflight()).isTrue();
        assertThat(rerun.blocked()).isTrue(); // blocked until a run passes
        assertThat(rerun.reason()).isEqualTo(UpgradeCoordinator.REASON_PREFLIGHT_RETRYING);
        Plan failedAgain = UpgradeCoordinator.plan(input(on("1.6.0"), on("1.6.0"), on("1.6.0"),
                new Preflight(PreflightState.REFUSED, T0.plusSeconds(90), "PodFailurePolicy"), null, rerun.upgrade(),
                T0.plusSeconds(100)));
        assertThat(failedAgain.recheck()).isEqualTo(Duration.ofSeconds(110)); // 120 s backoff after one retry
        assertThat(failedAgain.message()).contains("retries so far: 1");

        // a failure that is no refusal (bucket unreachable, an image without --check) blocks too
        Plan failed = UpgradeCoordinator.plan(input(on("1.6.0"), on("1.6.0"), on("1.6.0"),
                preflight(PreflightState.FAILED), null, null, T0.plusSeconds(1)));
        assertThat(failed.blocked()).isTrue();
        assertThat(failed.reason()).isEqualTo(UpgradeCoordinator.REASON_PREFLIGHT_FAILED);
        assertThat(failed.message()).contains("BackoffLimitExceeded");
    }

    @Test
    void noWorkloadChangeWhileBlocked() {
        // mid-upgrade (readers already moved), then a version whose check refuses: nothing moves either way
        Workload readers = new Workload("1.7.0", true, 2, "1.7.0@2026-11-22T09:00:00Z");
        Workload writers = new Workload("1.6.0", true, 2, null);
        Workload router = new Workload("1.6.0", true, 2, null);
        for (PreflightState state : List.of(PreflightState.ABSENT, PreflightState.RUNNING, PreflightState.DELETING,
                PreflightState.REFUSED, PreflightState.FAILED)) {
            Plan p = UpgradeCoordinator.plan(input(readers, writers, router, preflight(state),
                    heartbeats(T0, READER_PODS, WRITER_PODS), null, T0));
            assertThat(p.versions()).as(state.name())
                    .isEqualTo(new DesiredState.Versions("1.6.0", "1.7.0", "1.6.0", null, "1.7.0@2026-11-22T09:00:00Z", null));
            assertThat(p.upgrade().step()).isEqualTo("Preflight");
        }
        // the storage not ready: no pre-flight Job, nothing moves
        Plan noStorage = UpgradeCoordinator.plan(new Input("demo", "1.7.0", on("1.6.0"), on("1.6.0"), on("1.6.0"),
                Preflight.ABSENT, List.of(), false, null, null, T0));
        assertThat(noStorage.createPreflight()).isFalse();
        assertThat(noStorage.reason()).isEqualTo(UpgradeCoordinator.REASON_WAITING_FOR_STORAGE);
        assertThat(noStorage.versions().writers()).isEqualTo("1.6.0");
        // a workload that does not exist yet keeps the version being replaced
        Plan missing = UpgradeCoordinator.plan(input(on("1.6.0"), on("1.6.0"), Workload.ABSENT, Preflight.ABSENT,
                null, null, T0));
        assertThat(missing.createPreflight()).isTrue();
        assertThat(missing.versions().router()).isEqualTo("1.6.0");
    }

    @Test
    void rollOrderReadersWritersRouter() {
        Preflight passed = preflight(PreflightState.SUCCEEDED);
        // 1. the check passed: readers take the new version, with the annotation of when
        Plan readersStep = UpgradeCoordinator.plan(input(on("1.6.0"), on("1.6.0"), on("1.6.0"), passed, null, null, T0));
        String since = new VersionSince("1.7.0", T0).value();
        assertThat(readersStep.versions()).isEqualTo(new DesiredState.Versions("1.6.0", "1.7.0", "1.6.0", null, since, null));
        assertThat(readersStep.upgrade()).isEqualTo(new UpgradeStatus("1.6.0", "1.7.0", "Readers", null));
        assertThat(readersStep.blocked()).isFalse();

        // 2. readers on 1.7.0 but not rolled out, or rolled out with old heartbeats, or none: writers wait
        Workload rolling = new Workload("1.7.0", false, 2, since);
        Workload readers = new Workload("1.7.0", true, 2, since);
        Instant later = T0.plusSeconds(120);
        for (Plan p : List.of(
                UpgradeCoordinator.plan(input(rolling, on("1.6.0"), on("1.6.0"), passed,
                        heartbeats(later, READER_PODS, WRITER_PODS), null, later)),
                UpgradeCoordinator.plan(input(readers, on("1.6.0"), on("1.6.0"), passed,
                        heartbeats(T0.minusSeconds(3600), READER_PODS, WRITER_PODS), null, later)),
                UpgradeCoordinator.plan(input(readers, on("1.6.0"), on("1.6.0"), passed,
                        heartbeats(later, List.of("demo-readers-0"), WRITER_PODS), null, later)),
                UpgradeCoordinator.plan(input(readers, on("1.6.0"), on("1.6.0"), passed, null, null, later)))) {
            assertThat(p.versions().writers()).isEqualTo("1.6.0");
            assertThat(p.versions().router()).isEqualTo("1.6.0");
            assertThat(p.versions().readersSince()).isEqualTo(since); // carried
            assertThat(p.upgrade().step()).isEqualTo("Readers");
        }

        // 3. every reader pod heartbeats on the new version: writers move, the router waits
        ClusterStatus readersUp = heartbeats(later, READER_PODS, List.of());
        Plan writersStep = UpgradeCoordinator.plan(input(readers, on("1.6.0"), on("1.6.0"), passed, readersUp, null, later));
        assertThat(writersStep.versions().writers()).isEqualTo("1.7.0");
        assertThat(writersStep.versions().writersSince()).isEqualTo(new VersionSince("1.7.0", later).value());
        assertThat(writersStep.versions().router()).isEqualTo("1.6.0");
        assertThat(writersStep.upgrade().step()).isEqualTo("Writers");
        assertThat(writersStep.message()).contains("rolling the writers").contains("waiting for the writers to take 1.7.0");

        // 4. writers rolled and heartbeating since their switch: the router moves last
        Instant end = later.plusSeconds(120);
        Workload writers = new Workload("1.7.0", true, 2, new VersionSince("1.7.0", later).value());
        Plan routerStep = UpgradeCoordinator.plan(input(readers, writers, on("1.6.0"), passed,
                heartbeats(end, READER_PODS, WRITER_PODS), null, end));
        assertThat(routerStep.versions()).isEqualTo(new DesiredState.Versions("1.7.0", "1.7.0", "1.7.0",
                writers.since(), since, new VersionSince("1.7.0", end).value()));
        assertThat(routerStep.upgrade().step()).isEqualTo("Router");

        // a stopping writer, or a reader still warming, does not count
        ClusterStatus stopping = new ClusterStatus("cluster", T0.toString(), new ClusterStatus.Layout(null, List.of(),
                List.of(new ClusterStatus.Writer("demo-writers-0", true, List.of(), List.of(), null, end.toEpochMilli(),
                        true, null, 1), new ClusterStatus.Writer("demo-writers-1", true, List.of(), List.of(), null,
                        end.toEpochMilli(), false, null, 1)),
                List.of(new ClusterStatus.Reader("demo-readers-0", null, List.of(), "warming", List.of(), Map.of(), 0, 0,
                        end.toEpochMilli(), null, null, 1), new ClusterStatus.Reader("demo-readers-1", null, List.of(),
                        "degraded", List.of("p"), Map.of(), 0, 0, end.toEpochMilli(), null, null, 1))),
                null, List.of(), List.of());
        Plan held = UpgradeCoordinator.plan(input(readers, writers, on("1.6.0"), passed, stopping, null, end));
        assertThat(held.versions().writers()).isEqualTo("1.7.0"); // the writers already run it: kept
        assertThat(held.versions().router()).isEqualTo("1.6.0");
        assertThat(held.message()).contains("heartbeats of demo-readers-0 on").doesNotContain("demo-readers-1");

        ClusterStatus writerStopping = new ClusterStatus("cluster", T0.toString(), new ClusterStatus.Layout(null,
                List.of(), stopping.layout().writers(), heartbeats(end, READER_PODS, List.of()).layout().readers()),
                null, List.of(), List.of());
        Plan heldByWriter = UpgradeCoordinator.plan(input(readers, writers, on("1.6.0"), passed, writerStopping, null, end));
        assertThat(heldByWriter.versions().router()).isEqualTo("1.6.0");
        assertThat(heldByWriter.message()).contains("heartbeats of demo-writers-0 on");
    }

    @Test
    void versionChangedAgainMidUpgradeStartsOverWithItsOwnCheck() {
        Workload readers = new Workload("1.7.0", true, 2, "1.7.0@2026-11-22T09:00:00Z");
        Plan p = UpgradeCoordinator.plan(new Input("demo", "1.8.0", readers, on("1.6.0"), on("1.6.0"),
                Preflight.ABSENT, List.of("demo-preflight-1.7.0"), true, null,
                new UpgradeStatus("1.6.0", "1.7.0", "Writers", 2), T0));
        assertThat(p.deleteJobs()).containsExactly("demo-preflight-1.7.0");
        assertThat(p.createPreflight()).isTrue();
        assertThat(p.upgrade()).isEqualTo(new UpgradeStatus("1.6.0", "1.8.0", "Preflight", null)); // retries are per version
        assertThat(p.versions().readers()).isEqualTo("1.7.0");
        assertThat(p.blocked()).isFalse();
    }

    @Test
    void versionSinceAnnotationRoundTrips() {
        VersionSince v = new VersionSince("1.7.0-rc.1", T0);
        assertThat(v.value()).isEqualTo("1.7.0-rc.1@2026-11-22T10:00:00Z");
        assertThat(VersionSince.parse(v.value())).isEqualTo(v);
        assertThat(VersionSince.parse(null)).isNull();
        assertThat(VersionSince.parse("garbage")).isNull();
        assertThat(VersionSince.parse("1.7.0@yesterday")).isNull();
    }

    /**
     * Ruling OP-R6: a revert mid-roll (readers and writers on 1.7.0, the router still rolling) is a
     * downgrade. Writers go first, so no 1.7.0 writer is left to publish a format 1.6.0 readers refuse;
     * the bucket is checked again before the readers move, and a refusal keeps them on 1.7.0.
     */
    @Test
    void midRollRevertRollsWritersFirstAndChecksTheBucketAgain() {
        Instant t = T0;
        Workload readers = new Workload("1.7.0", true, 2, "1.7.0@2026-11-22T08:00:00Z");
        Workload writers = new Workload("1.7.0", true, 2, "1.7.0@2026-11-22T09:00:00Z");
        Workload router = new Workload("1.7.0", false, 2, "1.7.0@2026-11-22T09:30:00Z");
        UpgradeStatus wasHeadingTo170 = new UpgradeStatus("1.6.0", "1.7.0", "Router", null);
        ClusterStatus old = heartbeats(T0.minusSeconds(7200), READER_PODS, WRITER_PODS);

        // 1. the pre-flight check of 1.6.0 first; nothing moves
        Plan p1 = UpgradeCoordinator.plan(new Input("demo", "1.6.0", readers, writers, router, Preflight.ABSENT,
                List.of("demo-preflight-1.7.0"), true, old, wasHeadingTo170, t));
        assertThat(p1.createPreflight()).isTrue();
        assertThat(p1.deleteJobs()).containsExactly("demo-preflight-1.7.0");
        assertThat(p1.versions()).isEqualTo(new DesiredState.Versions("1.7.0", "1.7.0", "1.7.0", writers.since(),
                readers.since(), router.since()));
        assertThat(p1.upgrade()).isEqualTo(new UpgradeStatus("1.7.0", "1.6.0", "Preflight", null, true));
        assertThat(p1.message()).startsWith("Downgrade 1.7.0 -> 1.6.0");

        // 2. it passes: the WRITERS move first; readers and router stay
        Preflight passed = preflight(PreflightState.SUCCEEDED);
        Plan p2 = UpgradeCoordinator.plan(new Input("demo", "1.6.0", readers, writers, router, passed, Preflight.ABSENT,
                List.of(), true, old, p1.upgrade(), t));
        String writersSince = new VersionSince("1.6.0", t).value();
        assertThat(p2.versions()).isEqualTo(new DesiredState.Versions("1.6.0", "1.7.0", "1.7.0", writersSince,
                readers.since(), router.since()));
        assertThat(p2.upgrade().step()).isEqualTo("Writers");
        assertThat(p2.createPostCheck()).isFalse();

        // 3. the writers run 1.6.0 everywhere: the post-check runs, the readers still wait
        Instant later = t.plusSeconds(300);
        Workload writers160 = new Workload("1.6.0", true, 2, writersSince);
        ClusterStatus newWriters = heartbeats(later, READER_PODS, WRITER_PODS);
        Plan p3 = UpgradeCoordinator.plan(new Input("demo", "1.6.0", readers, writers160, router, passed,
                Preflight.ABSENT, List.of(), true, newWriters, p2.upgrade(), later));
        assertThat(p3.createPostCheck()).isTrue();
        assertThat(p3.createPreflight()).isFalse();
        assertThat(p3.upgrade()).isEqualTo(new UpgradeStatus("1.7.0", "1.6.0", "PostCheck", null, true));
        assertThat(p3.versions().readers()).isEqualTo("1.7.0");
        assertThat(p3.versions().router()).isEqualTo("1.7.0");
        assertThat(p3.blocked()).isFalse();
        assertThat(p3.message()).contains("demo-preflight-1.6.0-post").contains("before the readers move");

        // 4. it refuses (the 1.7.0 writers published a format 1.6.0 cannot read): blocked, readers stay on 1.7.0
        Preflight refused = new Preflight(PreflightState.REFUSED, later.plusSeconds(10), "PodFailurePolicy");
        Plan p4 = UpgradeCoordinator.plan(new Input("demo", "1.6.0", readers, writers160, router, passed, refused,
                List.of(), true, newWriters, p3.upgrade(), later.plusSeconds(20)));
        assertThat(p4.blocked()).isTrue();
        assertThat(p4.reason()).isEqualTo(UpgradeCoordinator.REASON_PREFLIGHT_REFUSED);
        assertThat(p4.message()).contains("cannot read").contains("The readers stay on 1.7.0")
                .contains("Roll forward (set spec.version back to 1.7.0)").contains("re-index");
        assertThat(p4.versions()).isEqualTo(new DesiredState.Versions("1.6.0", "1.7.0", "1.7.0", writersSince,
                readers.since(), router.since()));
        assertThat(p4.recheck()).isEqualTo(Duration.ofSeconds(50));
        Plan p4retry = UpgradeCoordinator.plan(new Input("demo", "1.6.0", readers, writers160, router, passed, refused,
                List.of(), true, newWriters, p4.upgrade(), later.plusSeconds(80)));
        assertThat(p4retry.deleteJobs()).containsExactly("demo-preflight-1.6.0-post");
        assertThat(p4retry.upgrade().preflightRetries()).isEqualTo(1);
        assertThat(p4retry.versions().readers()).isEqualTo("1.7.0");

        // 5. a passing post-check moves the readers, then the router
        Plan p5 = UpgradeCoordinator.plan(new Input("demo", "1.6.0", readers, writers160, router, passed, passed,
                List.of(), true, newWriters, p4retry.upgrade(), later.plusSeconds(400)));
        assertThat(p5.versions().readers()).isEqualTo("1.6.0");
        assertThat(p5.versions().router()).isEqualTo("1.7.0");
        assertThat(p5.upgrade().step()).isEqualTo("Readers");
        Instant end = later.plusSeconds(800);
        Workload readers160 = new Workload("1.6.0", true, 2, new VersionSince("1.6.0", later.plusSeconds(400)).value());
        Plan p6 = UpgradeCoordinator.plan(new Input("demo", "1.6.0", readers160, writers160, router, passed, passed,
                List.of(), true, heartbeats(end, READER_PODS, WRITER_PODS), p5.upgrade(), end));
        assertThat(p6.versions().router()).isEqualTo("1.6.0");
        assertThat(p6.upgrade().step()).isEqualTo("Router");

        // done: both check Jobs go
        Plan done = UpgradeCoordinator.plan(new Input("demo", "1.6.0", readers160, writers160,
                new Workload("1.6.0", true, 2, null), passed, passed, List.of(), true, null, p6.upgrade(), end));
        assertThat(done.upgrade()).isNull();
        assertThat(done.deleteJobs()).containsExactly("demo-preflight-1.6.0", "demo-preflight-1.6.0-post");
    }

    @Test
    void forwardUpgradesKeepReadersFirstAndOneCheck() {
        assertThat(UpgradeCoordinator.writersFirst("1.6.0", "1.7.0", null)).isFalse();
        assertThat(UpgradeCoordinator.writersFirst("1.7.0", "1.6.0", null)).isTrue();
        assertThat(UpgradeCoordinator.writersFirst("1.10.0", "1.9.3", null)).isTrue();
        assertThat(UpgradeCoordinator.writersFirst("1.7.0-rc1", "1.7.0", null)).isFalse();
        // tags that are no releases: a downgrade only when reverting the upgrade in progress; then kept
        assertThat(UpgradeCoordinator.writersFirst("main", "stable", null)).isFalse();
        assertThat(UpgradeCoordinator.writersFirst("main", "stable",
                new UpgradeStatus("stable", "main", "Router", null))).isTrue();
        assertThat(UpgradeCoordinator.writersFirst("main", "stable",
                new UpgradeStatus("main", "stable", "Writers", null, true))).isTrue();
        // a forward roll never creates the post-check
        Plan p = UpgradeCoordinator.plan(input(new Workload("1.7.0", true, 2, null), on("1.6.0"), on("1.6.0"),
                preflight(PreflightState.SUCCEEDED), heartbeats(T0, READER_PODS, WRITER_PODS), null, T0));
        assertThat(p.createPostCheck()).isFalse();
        assertThat(p.versions().writers()).isEqualTo("1.7.0");
        assertThat(p.upgrade().writersFirst()).isFalse();
    }
}
