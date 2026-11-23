package com.pharos.cluster.objectstore;

import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.ShardEntry;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.ShardSource;
import com.pharos.cluster.sharding.ProjectShards;
import com.pharos.cluster.sharding.ShardSpec;
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

    /**
     * Operator final review I2, kept for future id shapes (integration item 7): an id this build cannot
     * parse (here a separator no release uses yet) after a downgrade. Phase 6's keyed ids are parsed now
     * and keep the catalog authoritative.
     */
    @Test
    void anUnparseableEntryMakesTheCatalogNotAuthoritative() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        store.put(layout.catalogKey(), "{\"shards\":[{\"id\":\"proj~k:v\"}]}".getBytes(StandardCharsets.UTF_8));
        assertThat(catalog.shards()).isEmpty();
        assertThat(catalog.unparseableEntries()).containsExactly("proj~k:v");
        assertThat(catalog.authoritativeShards()).isEmpty(); // readers keep what they serve
        assertThat(catalog.mappingRead().authoritative()).isFalse();

        store.put(layout.catalogKey(), "{\"shards\":[{\"id\":\"good\"},{\"id\":\"proj~k:v\"}]}"
                .getBytes(StandardCharsets.UTF_8));
        assertThat(catalog.authoritativeShards()).isEmpty();
        assertThat(catalog.mappingRead().map().listed()).containsExactly(new ShardId("good")); // still synced

        // a split project's keyed ids (Phase 6) are this build's own: authoritative
        store.put(layout.catalogKey(), "{\"shards\":[{\"id\":\"good\"},{\"id\":\"proj~ring=s01\"}]}"
                .getBytes(StandardCharsets.UTF_8));
        assertThat(catalog.unparseableEntries()).isEmpty();
        assertThat(catalog.authoritativeShards()).contains(List.of(new ShardId("good"), new ShardId("proj~ring=s01")));

        store.put(layout.catalogKey(), "{\"shards\":[]}".getBytes(StandardCharsets.UTF_8));
        assertThat(catalog.unparseableEntries()).isEmpty();
        assertThat(catalog.mappingRead().authoritative()).isTrue();
        assertThat(catalog.authoritativeShards()).contains(List.of()); // really empty: authoritative
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

    // ── Phase 6: spec versions and the project ↔ shards mapping ─────────────────────────────

    @Test
    void phase5CatalogReadsAsTheProjectIdentity() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        assertThat(catalog.projectShards().projects()).isEmpty();
        catalog.register(new ShardId("pharos"), Optional.of(ShardSource.path("/srv/pharos")));
        catalog.register(new ShardId("lucene"));
        ProjectShards ps = catalog.projectShards();
        assertThat(ps.projects()).containsExactly("lucene", "pharos");
        assertThat(ps.shardsOf("pharos")).containsExactly(ShardId.ofProject("pharos"));
        assertThat(ps.specOf("pharos")).isEqualTo(ShardSpec.PROJECT);
        assertThat(new String(store.get(layout.catalogKey()).orElseThrow().data(), StandardCharsets.UTF_8))
                .as("nothing Phase 6 is written for [project]").doesNotContain("specs").doesNotContain("projects");
    }

    @Test
    void specVersionsAreAppendedNeverEdited() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        catalog.register(new ShardId("pharos"));
        assertThat(catalog.addSpec(Optional.empty(), ShardSpec.PROJECT)).isEqualTo(1);
        ShardSpec ring4 = ShardSpec.parse("[project, ring(path, shards=4)]");
        assertThat(catalog.addSpec(Optional.of("lucene"), ring4)).isEqualTo(2);
        String etag = store.get(layout.catalogKey()).orElseThrow().etag();
        assertThat(catalog.addSpec(Optional.of("lucene"), ShardSpec.parse("[project,ring(path,shards=4,vnodes=512)]")))
                .as("same as the newest of its scope: no new version").isEqualTo(2);
        assertThat(store.get(layout.catalogKey()).orElseThrow().etag()).isEqualTo(etag);
        assertThat(catalog.addSpec(Optional.of("lucene"), ShardSpec.parse("[project, ring(path, shards=5)]"))).isEqualTo(3);
        assertThat(catalog.addSpec(Optional.of("lucene"), ring4)).as("going back is a new version").isEqualTo(4);
        assertThat(catalog.projectShards().specs()).extracting(v -> v.version() + " " + v.project().orElse("-") + " " + v.spec())
                .containsExactly("1 - [project]", "2 lucene [project, ring(path, shards=4, vnodes=512)]",
                        "3 lucene [project, ring(path, shards=5, vnodes=512)]",
                        "4 lucene [project, ring(path, shards=4, vnodes=512)]");
        catalog.setActiveVersion(1);
        assertThat(catalog.projectShards().activeVersion()).hasValue(1);
        assertThatThrownBy(() -> catalog.setActiveVersion(2)).hasMessageContaining("belongs to project 'lucene'");
        assertThatThrownBy(() -> catalog.setActiveVersion(9)).hasMessageContaining("no spec version 9");
        assertThatThrownBy(() -> catalog.addSpec(Optional.of("a~b=c"), ring4)).hasMessageContaining("not a valid");
        assertThat(catalog.shards()).as("register's list untouched").containsExactly(new ShardId("pharos"));
    }

    @Test
    void putProjectMapsShardsAndListsThem() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        catalog.register(new ShardId("lucene"), Optional.of(ShardSource.git("https://example.org/l.git", "main")));
        int v = catalog.addSpec(Optional.of("lucene"), ShardSpec.parse("[project, ring(path, shards=2)]"));
        List<ShardId> ring = List.of(new ShardId("lucene~ring=s01"), new ShardId("lucene~ring=s02"));
        catalog.putProject("lucene", v, ring);
        String etag = store.get(layout.catalogKey()).orElseThrow().etag();
        catalog.putProject("lucene", v, ring);
        assertThat(store.get(layout.catalogKey()).orElseThrow().etag()).as("idempotent").isEqualTo(etag);

        ProjectShards ps = catalog.projectShards();
        assertThat(ps.shardsOf("lucene")).containsExactlyElementsOf(ring);
        assertThat(ps.projectOf(new ShardId("lucene~ring=s02"))).contains("lucene");
        assertThat(ps.specVersionOf("lucene")).hasValue(v);
        assertThat(catalog.shards()).containsExactly(new ShardId("lucene"), ring.get(0), ring.get(1));
        assertThat(catalog.entries().get(0).source()).as("existing entries and sources are kept").isPresent();

        // a reshard flip replaces the entry in one CAS
        int v2 = catalog.addSpec(Optional.of("lucene"), ShardSpec.parse("[project, ring(path, shards=3)]"));
        List<ShardId> three = List.of(ring.get(0), ring.get(1), new ShardId("lucene~ring=s03"));
        catalog.putProject("lucene", v2, three);
        assertThat(catalog.projectShards().shardsOf("lucene")).containsExactlyElementsOf(three);
        assertThat(catalog.projectShards().specVersionOf("lucene")).hasValue(v2);

        assertThatThrownBy(() -> catalog.putProject("lucene", v2, List.of(new ShardId("lucene~ring=s09"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not a shard id of project");
        assertThatThrownBy(() -> catalog.putProject("other", v2, List.of(new ShardId("other~ring=s01"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("belongs to project 'lucene'");
        assertThatThrownBy(() -> catalog.putProject("lucene", 42, three))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no spec version 42");
    }

    @Test
    void phase6FieldsSurviveOlderWritersRegister() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        int v = catalog.addSpec(Optional.of("lucene"), ShardSpec.parse("[project, module]"));
        catalog.putProject("lucene", v, List.of(new ShardId("lucene~module=core")));
        catalog.register(new ShardId("pharos")); // the Phase 5 write path
        ProjectShards ps = catalog.projectShards();
        assertThat(ps.projects()).containsExactly("lucene", "pharos");
        assertThat(ps.shardsOf("lucene")).containsExactly(new ShardId("lucene~module=core"));
    }

    @Test
    void phase6WritesRetryUnderContention() throws Exception {
        ContendedStore store = new ContendedStore(2);
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout, sleeps::add);
        assertThat(catalog.addSpec(Optional.empty(), ShardSpec.PROJECT)).isEqualTo(1);
        assertThat(store.conditionalPuts).isEqualTo(3);
        assertThat(sleeps).hasSize(2);
    }
}
