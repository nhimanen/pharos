package com.pharos.cluster.objectstore;

import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.ShardEntry;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.ShardSource;
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

    @Test
    void unregisterRemovesOnlyThatEntryAndIsIdempotent() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout, sleeps::add);
        assertThat(catalog.unregister(new ShardId("s"))).as("no catalog yet").isFalse();
        ShardSource src = ShardSource.git("https://example.org/r.git", "main");
        catalog.register(new ShardId("s"), Optional.of(src));
        catalog.register(new ShardId("t"), Optional.of(src));

        assertThat(catalog.unregister(new ShardId("s"))).isTrue();
        assertThat(catalog.entries()).containsExactly(new ShardEntry(new ShardId("t"), Optional.of(src)));
        String etag = store.get(layout.catalogKey()).orElseThrow().etag();
        assertThat(catalog.unregister(new ShardId("s"))).isFalse();
        assertThat(store.get(layout.catalogKey()).orElseThrow().etag()).as("absent: no write").isEqualTo(etag);
        assertThat(sleeps).isEmpty();
    }

    @Test
    void registerRecordsAndUpdatesSourceIdempotently() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        ShardSource first = ShardSource.git("https://example.org/r.git", "main");
        catalog.register(new ShardId("s"), Optional.of(first));
        String etag = store.get(layout.catalogKey()).orElseThrow().etag();
        catalog.register(new ShardId("s"), Optional.of(first));
        assertThat(store.get(layout.catalogKey()).orElseThrow().etag()).as("unchanged: no write").isEqualTo(etag);

        ShardSource moved = ShardSource.path("/shared/s");
        catalog.register(new ShardId("s"), Optional.of(moved));
        assertThat(catalog.entries()).containsExactly(new ShardEntry(new ShardId("s"), Optional.of(moved)));
        assertThat(catalog.shards()).containsExactly(new ShardId("s"));
    }

    @Test
    void noEmbedSourceIsRecordedAndReadBack() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        ShardSource plain = ShardSource.git("https://example.org/r.git", "main");
        catalog.register(new ShardId("s"), Optional.of(plain));
        assertThat(new String(store.get(layout.catalogKey()).orElseThrow().data(), StandardCharsets.UTF_8))
                .as("embed defaults to true and is not written").doesNotContain("embed");
        ShardSource noEmbed = plain.withEmbed(false);
        catalog.register(new ShardId("s"), Optional.of(noEmbed));
        assertThat(new String(store.get(layout.catalogKey()).orElseThrow().data(), StandardCharsets.UTF_8))
                .contains("\"embed\" : false");
        assertThat(catalog.entries().get(0).source()).contains(noEmbed);
        assertThat(catalog.entries().get(0).source().orElseThrow().embed()).isFalse();
        catalog.register(new ShardId("s"), Optional.of(plain));
        assertThat(catalog.entries().get(0).source().orElseThrow().embed()).isTrue();
    }

    @Test
    void olderWritersRegisterKeepsExistingSource() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        ShardSource src = ShardSource.git("https://example.org/r.git", "v1");
        catalog.register(new ShardId("s"), Optional.of(src));
        catalog.register(new ShardId("s"));                       // a writer that knows no sources
        catalog.register(new ShardId("s"), Optional.empty());
        assertThat(catalog.entries().get(0).source()).contains(src);
    }

    @Test
    void noSourceShardIsPinnedToItsWriter() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        catalog.register(new ShardId("plain"));
        catalog.register(new ShardId("shared"), Optional.of(ShardSource.path("/srv/x")));
        assertThat(catalog.entries()).extracting(ShardEntry::source)
                .containsExactly(Optional.empty(), Optional.of(ShardSource.path("/srv/x")));
        assertThat(new String(store.get(layout.catalogKey()).orElseThrow().data(), StandardCharsets.UTF_8))
                .doesNotContain("\"source\":{\"kind\":\"git\"");
    }

    @Test
    void credentialsNeverStoredInCatalog() throws Exception {
        for (String url : new String[] {"https://user:s3cret@example.org/r.git", "https://ghp_token@example.org/r.git"}) {
            assertThatThrownBy(() -> ShardSource.git(url, "main"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("credentials")
                    .hasMessageNotContaining("s3cret").hasMessageNotContaining("ghp_token");
        }
        assertThatThrownBy(() -> ShardSource.git("https://bad host:s3cret@x/\\u0000", "main"))
                .hasMessageNotContaining("s3cret");
        // ssh user names and scp-style urls carry no secret
        InMemoryObjectStore store = new InMemoryObjectStore();
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        catalog.register(new ShardId("a"), Optional.of(ShardSource.git("ssh://git@example.org/r.git", "main")));
        catalog.register(new ShardId("b"), Optional.of(ShardSource.git("git@example.org:r.git", "main")));
        assertThat(catalog.entries()).hasSize(2);
    }
}
