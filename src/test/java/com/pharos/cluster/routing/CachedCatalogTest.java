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
        assertThatThrownBy(c::shards).isInstanceOf(CatalogUnavailableException.class)
                .isNotInstanceOf(CatalogNeverReadException.class)
                .hasMessageContaining("cluster.maxCatalogAgeSeconds");
    }

    @Test
    void anOldCatalogIsNamedInTheQueryWarnings() {
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
            assertThat(ds.open(List.of()).warnings()).contains("routing with a catalog 61 s old");
            assertThat(ds.open(List.of("a")).warnings()).as("a scoped query does not use the catalog")
                    .noneMatch(w -> w.startsWith("routing with a catalog"));
        } finally {
            ds.close();
        }
    }
}
