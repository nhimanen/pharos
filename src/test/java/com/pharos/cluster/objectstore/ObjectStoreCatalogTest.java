package com.pharos.cluster.objectstore;

import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.ShardId;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObjectStoreCatalogTest {

    private final ObjectStoreLayout layout = new ObjectStoreLayout("pharos");
    private final List<Long> sleeps = new ArrayList<>();

    /** Fails the first {@code failures} conditional catalog puts as if another writer won. */
    private static final class ContendedStore extends InMemoryObjectStore {
        int failures;
        int conditionalPuts;

        ContendedStore(int failures) { this.failures = failures; }

        @Override
        public Optional<String> putIfAbsent(String key, byte[] data) {
            conditionalPuts++;
            if (failures > 0) {
                failures--;
                return Optional.empty();
            }
            return super.putIfAbsent(key, data);
        }
    }

    @Test
    void invalidShardIdsAreSkipped() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        store.put(layout.catalogKey(), ("{\"shards\":[{\"id\":\"good\"},{\"id\":\"bad id\"},{\"id\":\"../x\"},"
                + "{\"nope\":1},{\"id\":\"also-good\"}]}").getBytes(StandardCharsets.UTF_8));
        assertThat(new ObjectStoreCatalog(store, layout).shards())
                .containsExactly(new ShardId("good"), new ShardId("also-good"));
    }

    @Test
    void registerRetriesWithGrowingJitteredBackoff() throws Exception {
        ContendedStore store = new ContendedStore(4);
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout, sleeps::add);
        catalog.register(new ShardId("s"));
        assertThat(catalog.shards()).containsExactly(new ShardId("s"));
        assertThat(store.conditionalPuts).isEqualTo(5);
        assertThat(sleeps).hasSize(4);
        for (int i = 0; i < sleeps.size(); i++) {
            long d = ObjectStoreCatalog.BASE_BACKOFF_MS << i;
            assertThat(sleeps.get(i)).as("sleep %d", i).isBetween(d / 2, d);
        }
    }

    @Test
    void registerGivesUpAfterMaxAttemptsWithBoundedTotalBackoff() {
        ContendedStore store = new ContendedStore(Integer.MAX_VALUE);
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout, sleeps::add);
        assertThatThrownBy(() -> catalog.register(new ShardId("s")))
                .isInstanceOf(PublishConflictException.class).hasMessageContaining("kept changing");
        assertThat(store.conditionalPuts).isEqualTo(ObjectStoreCatalog.MAX_ATTEMPTS);
        assertThat(sleeps.stream().mapToLong(Long::longValue).sum()).isLessThanOrEqualTo(2000);
    }

    @Test
    void registerIsIdempotent() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout, sleeps::add);
        catalog.register(new ShardId("s"));
        String etag = store.get(layout.catalogKey()).orElseThrow().etag();
        catalog.register(new ShardId("s"));
        assertThat(store.get(layout.catalogKey()).orElseThrow().etag()).isEqualTo(etag);
        assertThat(sleeps).isEmpty();
    }
}
