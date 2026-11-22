package com.pharos.cluster.routing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.ShardCatalog;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.membership.ReaderHealth;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.sharding.ProjectShards;
import com.pharos.cluster.sharding.ShardSpec;
import com.pharos.search.distributed.DistributedSearch;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 6 Task 3: the router resolves projects to shards through one read of the catalog mapping,
 * prunes split projects' shards against their spec, and a query keeps the catalog version it was
 * routed with.
 */
class SpecRoutingTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String RING3 = "[project, ring(path, shards=3)]";
    private static final String BY_MODULE = "[project, module]";

    private final AtomicLong nanos = new AtomicLong(1_000_000_000L);
    private final ReaderHealth health = new ReaderHealth(30, nanos::get);
    private List<ReaderHeartbeat> live = new ArrayList<>();
    private final AtomicReference<ProjectShards> mapping = new AtomicReference<>();
    private final AtomicInteger reads = new AtomicInteger();
    private final ShardCatalog catalog = new ShardCatalog() {
        @Override
        public List<ShardId> shards() throws IOException {
            return projectShards().listed();
        }

        @Override
        public ProjectShards projectShards() {
            reads.incrementAndGet();
            return mapping.get();
        }
    };

    /** A catalog document: plain shards, the split project {@code lib} (ring of 3), {@code mod} by module. */
    static ProjectShards catalog(String extraProjects) throws IOException {
        String doc = """
                {"shards": [{"id": "app"}, {"id": "lib~ring=s01"}, {"id": "lib~ring=s02"}, {"id": "lib~ring=s03"},
                            {"id": "mod~module=core"}, {"id": "mod~module=web"}, {"id": "mod~module=_"},
                            {"id": "stray~ring=s01"}, {"id": "lib"}, {"id": "lib~ring=s04"}],
                 "specs": [{"version": 1, "spec": "[project]"},
                           {"version": 2, "project": "lib", "spec": "%s"},
                           {"version": 3, "project": "mod", "spec": "%s"},
                           {"version": 4, "project": "lib", "spec": "[project, ring(path, shards=4)]"}],
                 "projects": [{"name": "lib", "spec": 2, "shards": ["lib~ring=s01", "lib~ring=s02", "lib~ring=s03"]
                               %s},
                              {"name": "mod", "spec": 3, "shards": ["mod~module=core", "mod~module=web", "mod~module=_"]}]}
                """.formatted(RING3, BY_MODULE, extraProjects);
        JsonNode root = JSON.readTree(doc);
        List<ShardId> ids = new ArrayList<>();
        for (JsonNode s : root.path("shards")) ids.add(new ShardId(s.path("id").asText()));
        return ProjectShards.fromCatalog(root, ids, w -> { throw new AssertionError(w); });
    }

    private static ReaderHeartbeat hb(String node, int inFlight, String... shards) {
        return new ReaderHeartbeat(node, "http://" + node, List.of(shards), false, Map.of(), inFlight, 0, 0L);
    }

    private ShardRouter router() {
        return new ShardRouter(() -> live, catalog, health, s -> false);
    }

    private static List<String> shardNames(ShardRouter.RoutePlan plan) {
        return plan.shards().stream().map(s -> s.shard().value()).toList();
    }

    @Test
    void splitProjectRoutesToItsShardsFromOneCatalogRead() throws IOException {
        mapping.set(catalog(""));
        ShardRouter.RoutePlan plan = router().plan(List.of("lib", "app"), ShardFilter.NONE);
        assertThat(reads).hasValue(1);
        assertThat(shardNames(plan)).as("requested order, catalog order within a project")
                .containsExactly("lib~ring=s01", "lib~ring=s02", "lib~ring=s03", "app");
        assertThat(plan.shards()).extracting(ShardRouter.PlannedShard::project)
                .containsExactly("lib", "lib", "lib", "app");
        assertThat(plan.shards()).extracting(ShardRouter.PlannedShard::split).containsExactly(true, true, true, false);
        assertThat(plan.catalog()).isSameAs(mapping.get());
        assertThat(plan.notes()).isEmpty();
    }

    @Test
    void unscopedRoutesEveryProjectsActiveShardsButNoOrphanOrStagedShard() throws IOException {
        mapping.set(catalog(", \"staged\": {\"spec\": 4, \"shards\": [\"lib~ring=s01\", \"lib~ring=s02\","
                + " \"lib~ring=s03\", \"lib~ring=s04\"]}"));
        assertThat(mapping.get().orphans()).extracting(ShardId::value).containsExactly("stray~ring=s01", "lib");
        ShardRouter.RoutePlan plan = router().plan(List.of(), ShardFilter.NONE);
        assertThat(shardNames(plan)).containsExactly("app", "lib~ring=s01", "lib~ring=s02", "lib~ring=s03",
                "mod~module=core", "mod~module=web", "mod~module=_");
        assertThat(mapping.get().syncShards()).as("the reader syncs the staged shard ahead")
                .extracting(ShardId::value).contains("lib~ring=s04").doesNotContain("lib", "stray~ring=s01");
        // a project the catalog does not know is its [project] shard, as before Phase 6
        assertThat(shardNames(router().plan(List.of("other"), ShardFilter.NONE))).containsExactly("other");
    }

    @Test
    void scopedQueryWithoutACatalogRoutesTheProjectShard() {
        ShardCatalog never = new ShardCatalog() {
            @Override public List<ShardId> shards() { throw new CatalogNeverReadException("not read yet", null); }
            @Override public ProjectShards projectShards() { throw new CatalogNeverReadException("not read yet", null); }
        };
        live = List.of(hb("r1", 0, "app"));
        ShardRouter r = new ShardRouter(() -> live, never, health, s -> false);
        ShardRouter.RoutePlan plan = r.plan(List.of("app"), ShardFilter.NONE);
        assertThat(shardNames(plan)).containsExactly("app");
        assertThat(plan.shards().getFirst().route().candidates()).extracting(ReaderHeartbeat::node).containsExactly("r1");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> r.plan(List.of(), ShardFilter.NONE))
                .isInstanceOf(CatalogUnavailableException.class);
    }

    @Test
    void moduleFilterPrunesAModuleKeyedProject() throws IOException {
        mapping.set(catalog(""));
        ShardRouter.RoutePlan plan = router().plan(List.of("mod", "app"), ShardFilter.module("core"));
        assertThat(shardNames(plan)).containsExactly("mod~module=core", "app");
        assertThat(plan.notes()).containsExactly("project mod: module 'core' selects 1 of 3 shards (mod~module=core)");
        // a module key value is compared in its id-safe form
        assertThat(shardNames(router().plan(List.of("mod"), ShardFilter.module("Core")))).containsExactly("mod~module=core");
        assertThat(shardNames(router().plan(List.of("mod"), ShardFilter.module("none")))).isEmpty();
    }

    @Test
    void filtersWithoutAMatchingSpecKeyFanOutAndSayWhy() throws IOException {
        mapping.set(catalog(""));
        ShardRouter.RoutePlan plan = router().plan(List.of("lib"), ShardFilter.pathPrefix("src/main"));
        assertThat(shardNames(plan)).containsExactly("lib~ring=s01", "lib~ring=s02", "lib~ring=s03");
        assertThat(plan.notes()).singleElement().asString()
                .contains("project lib", "path prefix 'src/main' cannot prune a ring", "fans out to all 3 shards");
        ShardRouter.RoutePlan byModule = router().plan(List.of("lib"), ShardFilter.module("core"));
        assertThat(byModule.shards()).hasSize(3);
        assertThat(byModule.notes()).singleElement().asString().contains("the spec has no module key");
    }

    @Test
    void prefixLanguageAndKindPruneThroughTheirKeys() {
        ShardSpec byPrefix = ShardSpec.parse("[project, prefix(path, depth=2)]");
        List<ShardId> shards = List.of(new ShardId("p~prefix=src-main"), new ShardId("p~prefix=src-test"),
                new ShardId("p~prefix=docs"), new ShardId("p~prefix=_"));
        assertThat(ShardFilter.pathPrefix("src/main/java").prune("p", byPrefix, shards).kept())
                .extracting(ShardId::value).containsExactly("p~prefix=src-main");
        assertThat(ShardFilter.pathPrefix("src").prune("p", byPrefix, shards).kept())
                .extracting(ShardId::value).containsExactly("p~prefix=src-main", "p~prefix=src-test");
        assertThat(ShardFilter.pathPrefix("docs").prune("p", byPrefix, shards).kept())
                .extracting(ShardId::value).containsExactly("p~prefix=docs");

        ShardSpec byLanguage = ShardSpec.parse("[language, kind]");
        List<ShardId> lk = List.of(new ShardId("p~language=java~kind=code"), new ShardId("p~language=md~kind=doc"),
                new ShardId("p~language=py~kind=code"));
        assertThat(new ShardFilter(null, null, "Java", null).prune("p", byLanguage, lk).kept())
                .extracting(ShardId::value).containsExactly("p~language=java~kind=code");
        assertThat(new ShardFilter(null, null, null, "code").prune("p", byLanguage, lk).kept())
                .extracting(ShardId::value).containsExactly("p~language=java~kind=code", "p~language=py~kind=code");
        // a [project] spec has nothing to prune and says nothing
        ShardFilter.Pruned plain = ShardFilter.module("x").prune("app", ShardSpec.PROJECT, List.of(new ShardId("app")));
        assertThat(plain.kept()).hasSize(1);
        assertThat(plain.notes()).isEmpty();
    }

    /**
     * P6-R12: a reader that does not advertise key filter support would ignore the filter and answer
     * unfiltered, so a filtered query never routes to it; unfiltered queries still do.
     */
    @Test
    void aFilteredQueryNeverRoutesToAReaderWithoutFilterSupport() throws IOException {
        mapping.set(catalog(""));
        ReaderHeartbeat old = hb("old", 0, "app", "mod~module=core");
        ReaderHeartbeat current = new ReaderHeartbeat("new", "http://new", List.of("mod~module=core"), false, Map.of(),
                5, 0, 0L, null, null, null, null, Boolean.TRUE);
        live = List.of(old, current);
        ShardRouter.RoutePlan filtered = router().plan(List.of("mod", "app"), ShardFilter.module("core"));
        assertThat(filtered.shards()).extracting(s -> s.shard().value()).containsExactly("mod~module=core", "app");
        assertThat(filtered.shards().get(0).route().candidates()).extracting(ReaderHeartbeat::node).containsExactly("new");
        assertThat(filtered.shards().get(0).route().noFilters()).containsExactly("old");
        assertThat(filtered.shards().get(1).route().candidates()).as("app's only reader lacks filters").isEmpty();
        assertThat(filtered.shards().get(1).route().noFilters()).containsExactly("old");
        ShardRouter.RoutePlan plain = router().plan(List.of("app"), ShardFilter.NONE);
        assertThat(plain.shards().get(0).route().candidates()).extracting(ReaderHeartbeat::node).containsExactly("old");

        DistributedSearch ds = new DistributedSearch(router(), new com.pharos.cluster.internal.ShardClient(health, "t",
                Duration.ofMillis(100), Duration.ofMillis(100)), null, null, null, Duration.ofSeconds(5), () -> -1L);
        try (DistributedSearch.Query q = ds.open(List.of("app"), new ShardFilter(null, null, "java", null))) {
            assertThat(q.warnings()).anySatisfy(w -> assertThat(w).contains("old", "without key filter support"))
                    .anySatisfy(w -> assertThat(w).contains("shard app unavailable",
                            "every live reader serving it lacks key filter support (old)"));
        } finally {
            ds.close();
        }
    }

    @Test
    void theShardsOfASplitProjectPreferTheSameReader() throws IOException {
        mapping.set(catalog(""));
        live = List.of(hb("r1", 0, "lib~ring=s01", "lib~ring=s02", "lib~ring=s03"),
                hb("r2", 0, "lib~ring=s01", "lib~ring=s02", "lib~ring=s03"));
        ShardRouter r = router();
        for (int i = 0; i < 6; i++) {
            ShardRouter.RoutePlan plan = r.plan(List.of("lib"), ShardFilter.NONE);
            List<String> first = plan.shards().stream()
                    .map(s -> s.route().candidates().getFirst().node()).distinct().toList();
            assertThat(first).as("query %s: one first choice for the whole project", i).hasSize(1);
        }
    }

    /**
     * Review Focus 2: a query keeps the catalog version it was routed with. The catalog flips (a reshard
     * from three ring shards to four) after the query was opened: the open query still has exactly the
     * three shards of its version, the next query has the four, and each open read the catalog once.
     */
    @Test
    void queryUsesOneSpecVersion() throws IOException {
        mapping.set(catalog(""));
        live = List.of(hb("r1", 0, "lib~ring=s01", "lib~ring=s02", "lib~ring=s03", "lib~ring=s04"));
        ShardRouter r = router();
        DistributedSearch ds = new DistributedSearch(r, new com.pharos.cluster.internal.ShardClient(health, "t",
                Duration.ofMillis(100), Duration.ofMillis(100)), null, null, null, Duration.ofSeconds(5), () -> -1L);
        try (DistributedSearch.Query before = ds.open(List.of("lib"))) {
            assertThat(reads).hasValue(1);
            ProjectShards version = before.catalog();
            String flipped = """
                    {"shards": [{"id": "lib~ring=s01"}, {"id": "lib~ring=s02"}, {"id": "lib~ring=s03"}, {"id": "lib~ring=s04"}],
                     "specs": [{"version": 4, "project": "lib", "spec": "[project, ring(path, shards=4)]"}],
                     "projects": [{"name": "lib", "spec": 4,
                                   "shards": ["lib~ring=s01", "lib~ring=s02", "lib~ring=s03", "lib~ring=s04"]}]}
                    """;
            JsonNode root = JSON.readTree(flipped);
            List<ShardId> ids = new ArrayList<>();
            for (JsonNode s : root.path("shards")) ids.add(new ShardId(s.path("id").asText()));
            mapping.set(ProjectShards.fromCatalog(root, ids, w -> { }));

            try (DistributedSearch.Query after = ds.open(List.of("lib"))) {
                assertThat(reads).hasValue(2);
                assertThat(after.shardNames()).hasSize(4);
                assertThat(after.catalog().specOf("lib").ring().orElseThrow().names()).hasSize(4);
            }
            assertThat(before.shardNames()).as("the open query keeps its version's shards")
                    .containsExactly("lib~ring=s01", "lib~ring=s02", "lib~ring=s03");
            assertThat(before.catalog()).isSameAs(version);
            assertThat(before.catalog().specOf("lib")).isEqualTo(ShardSpec.parse(RING3));
            assertThat(before.fansOut()).as("a split project always runs through the coordinator").isTrue();
        } finally {
            ds.close();
        }
    }
}
