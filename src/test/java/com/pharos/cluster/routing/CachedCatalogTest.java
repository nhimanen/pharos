package com.pharos.cluster.routing;

import com.pharos.cluster.ShardCatalog;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.internal.ShardClient;
import com.pharos.cluster.membership.ReaderHealth;
import com.pharos.search.distributed.DistributedSearch;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** I5: the coordinator's catalog never blocks queries on the store, backs off, and has a maximum age. */
class CachedCatalogTest {

    private static final long S = 1_000_000_000L;

    private final AtomicLong now = new AtomicLong(1_000 * S);
    private final AtomicInteger reads = new AtomicInteger();
    private volatile boolean failing;
    private final List<Runnable> refreshes = new ArrayList<>();
    private final ShardCatalog store = () -> {
        reads.incrementAndGet();
        if (failing) throw new IOException("bucket unreachable");
        return List.of(new ShardId("a"), new ShardId("b"));
    };

    private CachedCatalog catalog() {
        return new CachedCatalog(store, Duration.ofSeconds(5), Duration.ofSeconds(600), now::get, refreshes::add);
    }

    @Test
    void neverReadFailsFastAndBacksOff() throws Exception {
        failing = true;
        CachedCatalog c = catalog();
        assertThatThrownBy(c::shards).isInstanceOf(CatalogNeverReadException.class).hasMessageContaining("unreachable");
        assertThatThrownBy(c::shards).isInstanceOf(CatalogNeverReadException.class);
        assertThat(reads).as("no second store read within the back-off").hasValue(1);
        assertThat(c.ageSeconds()).isEqualTo(-1);
        now.addAndGet(5 * S);
        failing = false;
        assertThat(c.shards() == null).isFalse();
        assertThat(reads).hasValue(2);
    }

    @Test
    void refreshRunsInTheBackgroundAndAFailureKeepsTheLastCatalogWithBackOff() throws Exception {
        CachedCatalog c = catalog();
        assertThat(c.shards()).hasSize(2);
        now.addAndGet(6 * S);
        failing = true;
        assertThat(c.shards()).as("answered at once from the cache").hasSize(2);
        assertThat(reads).as("the store is not read on the query's thread").hasValue(1);
        assertThat(refreshes).hasSize(1);
        refreshes.remove(0).run(); // the background refresh fails
        assertThat(reads).hasValue(2);
        assertThat(c.shards()).hasSize(2);
        assertThat(refreshes).as("backing off: no new refresh within 5 s of the last attempt").isEmpty();
        now.addAndGet(5 * S);
        c.shards();
        assertThat(refreshes).hasSize(1);
        assertThat(c.ageSeconds()).isEqualTo(11);
    }

    @Test
    void overMaxAgeUnscopedRoutingIsRefused() throws Exception {
        CachedCatalog c = catalog();
        c.shards();
        failing = true;
        now.addAndGet(601 * S);
        c.shards(); // starts a background read, which fails
        refreshes.remove(0).run();
        assertThatThrownBy(c::shards).isInstanceOf(CatalogUnavailableException.class)
                .isNotInstanceOf(CatalogNeverReadException.class)
                .hasMessageContaining("cluster.maxCatalogAgeSeconds");
    }

    @Test
    void anOldCatalogIsNamedInTheQueryWarnings() throws Exception {
        CachedCatalog c = catalog();
        ReaderHealth health = new ReaderHealth(30);
        ShardRouter router = new ShardRouter(List::of, c, health, id -> false);
        DistributedSearch ds = new DistributedSearch(router,
                new ShardClient(health, "t", Duration.ofMillis(100), Duration.ofMillis(100)), null, null, null,
                Duration.ofSeconds(5), c::ageSeconds);
        try {
            assertThat(ds.open(List.of()).warnings()).noneMatch(w -> w.startsWith("routing with a catalog"));
            now.addAndGet(61 * S);
            failing = true;
            c.shards(); // starts a background read, which fails
            refreshes.remove(0).run();
            assertThat(ds.open(List.of()).warnings()).contains("routing with a catalog 61 s old");
            assertThat(ds.open(List.of("a")).warnings()).as("a scoped query does not use the catalog")
                    .noneMatch(w -> w.startsWith("routing with a catalog"));
        } finally {
            ds.close();
        }
    }

    /** Fix round 2: a background read that hangs is given up after hungAfter; a late finish of it cannot clear the new one. */
    @Test
    void hungRefreshIsGivenUpAndALateFinishDoesNotClearTheNewOne() throws Exception {
        CachedCatalog c = new CachedCatalog(store, Duration.ofSeconds(5), Duration.ofSeconds(600), now::get,
                refreshes::add, Duration.ofSeconds(10));
        c.shards();
        now.addAndGet(6 * S);
        c.shards();
        assertThat(refreshes).hasSize(1); // started, never finishes
        now.addAndGet(5 * S);
        c.shards();
        assertThat(refreshes).as("still within hungAfter: no second read").hasSize(1);
        now.addAndGet(6 * S);
        c.shards();
        assertThat(refreshes).as("hung for 11 s: given up, a new read started").hasSize(2);
        refreshes.remove(0).run(); // the hung one finally returns
        now.addAndGet(5 * S);
        c.shards();
        assertThat(refreshes).as("the new read still counts as running").hasSize(1);
        refreshes.remove(0).run();
        assertThat(c.ageSeconds()).isZero();
    }

    // ---------------------------------------------------------------- Task 5 carried minors

    /** An abandoned (hung) read is capped at one: while it still runs, the watchdog abandons no other. */
    @Test
    void atMostOneAbandonedReadIsInFlight() throws Exception {
        CachedCatalog c = new CachedCatalog(store, Duration.ofSeconds(5), Duration.ofSeconds(600), now::get,
                refreshes::add, Duration.ofSeconds(10));
        c.shards();
        now.addAndGet(6 * S);
        c.shards();
        assertThat(refreshes).hasSize(1);
        now.addAndGet(11 * S);
        c.shards();
        assertThat(refreshes).as("the first hung read is abandoned, a second starts").hasSize(2);
        now.addAndGet(11 * S);
        c.shards();
        assertThat(refreshes).as("the second hangs too, but one abandoned read still runs: no third")
                .hasSize(2);
        refreshes.remove(0).run(); // the abandoned read finally returns
        now.addAndGet(1 * S);
        c.shards();
        assertThat(refreshes).as("no abandoned read in flight any more: the hung second is given up")
                .hasSize(2);
    }

    /** A late finish of an abandoned read never overwrites what its replacement stored. */
    @Test
    void anAbandonedReadsLateResultIsDiscarded() throws Exception {
        List<List<ShardId>> answers = new ArrayList<>(List.of(
                List.of(new ShardId("a")),          // the first, blocking read
                List.of(new ShardId("fresh")),      // the replacement, which finishes first
                List.of(new ShardId("stale"))));    // the abandoned read, which finishes last
        ShardCatalog scripted = () -> answers.remove(0);
        CachedCatalog c = new CachedCatalog(scripted, Duration.ofSeconds(5), Duration.ofSeconds(600), now::get,
                refreshes::add, Duration.ofSeconds(10));
        c.shards();
        now.addAndGet(6 * S);
        c.shards();
        Runnable hung = refreshes.remove(0);
        now.addAndGet(11 * S);
        c.shards(); // the hung read is abandoned; its replacement starts
        refreshes.remove(0).run();
        assertThat(c.shards()).containsExactly(new ShardId("fresh"));
        now.addAndGet(1 * S);
        hung.run();
        assertThat(c.shards()).as("the abandoned read's result is not stored").containsExactly(new ShardId("fresh"));
        assertThat(c.ageSeconds()).as("nor its read time").isEqualTo(1);
    }

    // ---------------------------------------------------------------- final review I1

    /** A refresher that runs each read on its own thread, 50 ms later (a real, not instant, store). */
    private static void later(Runnable r) {
        Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                return;
            }
            r.run();
        });
    }

    /** I1: idle past maxAge with a healthy store, the first query briefly waits for the fresh read. */
    @Test
    void idlePastMaxAgeWithAHealthyStoreTheFirstQueryRoutesNormally() throws Exception {
        CachedCatalog c = new CachedCatalog(store, Duration.ofSeconds(5), Duration.ofSeconds(600), now::get,
                CachedCatalogTest::later, Duration.ofSeconds(60), Duration.ofMillis(2_000));
        c.shards();
        now.addAndGet(700 * S);
        assertThat(c.shards()).as("no CatalogTooOldException: the store is healthy").hasSize(2);
        assertThat(c.ageSeconds()).as("the query waited for the read it started").isZero();
        assertThat(reads).hasValue(2);
    }

    /** I1: the same through the coordinator: no "routing with a catalog N s old" warning, no fallback. */
    @Test
    void idlePastMaxAgeWithAHealthyStoreGivesNoWarning() {
        CachedCatalog c = new CachedCatalog(store, Duration.ofSeconds(5), Duration.ofSeconds(600), now::get,
                CachedCatalogTest::later, Duration.ofSeconds(60), Duration.ofMillis(2_000));
        ReaderHealth health = new ReaderHealth(30);
        ShardRouter router = new ShardRouter(List::of, c, health, id -> false);
        DistributedSearch ds = new DistributedSearch(router,
                new ShardClient(health, "t", Duration.ofMillis(100), Duration.ofMillis(100)), null, null, null,
                Duration.ofSeconds(5), c::staleSeconds);
        try {
            assertThat(ds.open(List.of()).warnings()).noneMatch(w -> w.startsWith("routing with a catalog"));
            now.addAndGet(700 * S);
            assertThat(ds.open(List.of()).warnings()).as("idle is not stale: routed (no CatalogTooOldException),"
                    + " no age warning").noneMatch(w -> w.contains("catalog"));
        } finally {
            ds.close();
        }
    }

    /** I1: a healthy read slower than the wait leaves the held catalog in use, without warning or refusal. */
    @Test
    void aSlowHealthyReadIsNotTakenForAFailure() throws Exception {
        CachedCatalog c = new CachedCatalog(store, Duration.ofSeconds(5), Duration.ofSeconds(600), now::get,
                refreshes::add, Duration.ofSeconds(60), Duration.ofMillis(20));
        c.shards();
        now.addAndGet(700 * S);
        assertThat(c.shards()).as("the read has not finished, but nothing failed").hasSize(2);
        assertThat(c.staleSeconds()).isZero();
        refreshes.remove(0).run();
        assertThat(c.ageSeconds()).isZero();
    }

    /** I1: the background timer's tick re-reads the catalog with no query asking. */
    @Test
    void tickRefreshesWithoutAQuery() throws Exception {
        CachedCatalog c = catalog();
        c.shards();
        now.addAndGet(6 * S);
        c.tick();
        assertThat(refreshes).hasSize(1);
        refreshes.remove(0).run();
        assertThat(reads).hasValue(2);
        c.tick();
        assertThat(refreshes).as("not due again yet").isEmpty();
        now.addAndGet(700 * S);
        c.tick();
        refreshes.remove(0).run();
        assertThat(c.ageSeconds()).isZero();
    }

    /** I1: before any read a tick does nothing (the first read stays a query's, with its back-off). */
    @Test
    void tickBeforeTheFirstReadDoesNothing() {
        CachedCatalog c = catalog();
        c.tick();
        assertThat(refreshes).isEmpty();
        assertThat(reads).hasValue(0);
    }

    /** I1: with reads failing, staleSeconds reports the age (the warning stays). */
    @Test
    void staleSecondsReportsTheAgeOnlyWhileReadsFail() throws Exception {
        CachedCatalog c = catalog();
        c.shards();
        now.addAndGet(61 * S);
        assertThat(c.staleSeconds()).isZero();
        failing = true;
        c.tick();
        refreshes.remove(0).run();
        assertThat(c.staleSeconds()).isEqualTo(61);
        failing = false;
        now.addAndGet(5 * S);
        c.tick();
        refreshes.remove(0).run();
        assertThat(c.staleSeconds()).isZero();
    }
}
