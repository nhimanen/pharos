package com.pharos.cluster.sharding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.ShardId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.*;

class ProjectShardsTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private final List<String> warnings = new ArrayList<>();

    private ProjectShards read(String json) throws Exception {
        JsonNode root = JSON.readTree(json);
        List<ShardId> ids = new ArrayList<>();
        for (JsonNode s : root.path("shards")) ids.add(new ShardId(s.path("id").asText()));
        return ProjectShards.fromCatalog(root, ids, warnings::add);
    }

    private static ShardId id(String s) { return new ShardId(s); }

    @Test
    void catalogWithoutProjectsIsTheIdentity() throws Exception {
        ProjectShards ps = read("{\"shards\":[{\"id\":\"pharos\"},{\"id\":\"lucene\",\"source\":{\"type\":\"path\"}}]}");
        assertThat(ps.projects()).containsExactly("lucene", "pharos");
        assertThat(ps.shardsOf("pharos")).containsExactly(ShardId.ofProject("pharos"));
        assertThat(ps.projectOf(ShardId.ofProject("lucene"))).contains("lucene");
        assertThat(ps.specOf("lucene")).isEqualTo(ShardSpec.PROJECT);
        assertThat(ps.specVersionOf("lucene")).isEmpty();
        assertThat(ps.activeVersion()).isEmpty();
        assertThat(ps.specs()).isEmpty();
        assertThat(ps.shardsOf("unknown")).isEmpty();
        assertThat(ps.projectOf(id("unknown"))).isEmpty();
        assertThat(ps.orphans()).isEmpty();
        assertThat(warnings).isEmpty();
    }

    @Test
    void identityOfShardIds() {
        ProjectShards ps = ProjectShards.identity(List.of(id("a"), id("b"), id("a"), id("x~ring=s01")));
        assertThat(ps.projects()).containsExactly("a", "b");
        assertThat(ps.shardsOf("a")).containsExactly(id("a"));
        assertThat(ps.projectOf(id("x~ring=s01"))).isEmpty();
        assertThat(ps.orphans()).containsExactly(id("x~ring=s01"));
    }

    @Test
    void readsProjectsSpecsAndActiveVersion() throws Exception {
        ProjectShards ps = read("""
                {"shards":[{"id":"pharos"},{"id":"lucene"},{"id":"lucene~ring=s01"},{"id":"lucene~ring=s02"}],
                 "specs":[{"version":1,"spec":"[project]"},
                          {"version":2,"project":"lucene","spec":"[project, ring(path, shards=2)]"}],
                 "activeVersion":1,
                 "projects":[{"name":"lucene","spec":2,"shards":["lucene~ring=s01","lucene~ring=s02"]}]}
                """);
        assertThat(warnings).isEmpty();
        assertThat(ps.projects()).containsExactly("lucene", "pharos");
        assertThat(ps.shardsOf("lucene")).containsExactly(id("lucene~ring=s01"), id("lucene~ring=s02"));
        assertThat(ps.projectOf(id("lucene~ring=s02"))).contains("lucene");
        assertThat(ps.specOf("lucene")).isEqualTo(ShardSpec.parse("[project, ring(path, shards=2)]"));
        assertThat(ps.specVersionOf("lucene")).isEqualTo(OptionalInt.of(2));
        assertThat(ps.shardsOf("pharos")).containsExactly(id("pharos"));
        assertThat(ps.activeVersion()).isEqualTo(OptionalInt.of(1));
        assertThat(ps.specs()).containsExactly(new VersionedSpec(1, Optional.empty(), ShardSpec.PROJECT),
                new VersionedSpec(2, Optional.of("lucene"), ShardSpec.parse("[project, ring(path, shards=2)]")));
        // the pre-split shard "lucene" is no longer the project's: an orphan, not a second project
        assertThat(ps.projectOf(id("lucene"))).isEmpty();
        assertThat(ps.orphans()).containsExactly(id("lucene"));
        assertThat(ps.shards()).containsExactlyInAnyOrder(id("lucene~ring=s01"), id("lucene~ring=s02"), id("pharos"));
    }

    @Test
    void keyedShardsWithoutAProjectEntryAreOrphans() throws Exception {
        ProjectShards ps = read("{\"shards\":[{\"id\":\"lucene~ring=s01\"},{\"id\":\"pharos\"}]}");
        assertThat(ps.projects()).containsExactly("pharos");
        assertThat(ps.projectOf(id("lucene~ring=s01"))).isEmpty();
        assertThat(ps.orphans()).containsExactly(id("lucene~ring=s01"));
    }

    @Test
    void brokenEntriesAreSkippedWithAWarning() throws Exception {
        ProjectShards ps = read("""
                {"shards":[{"id":"a"}],
                 "specs":[{"version":1,"spec":"[project]"},{"version":1,"spec":"[project, module]"},
                          {"version":2,"project":"b","spec":"[project, ring(path, shards=2)]"},
                          {"version":3,"spec":"[bogus]"},{"spec":"[project]"},
                          {"version":4,"project":"c","spec":"[project, module]"}],
                 "activeVersion":2,
                 "projects":[{"name":"x","spec":9,"shards":["x~ring=s01"]},
                             {"name":"c","spec":2,"shards":["c~ring=s01"]},
                             {"name":"b","spec":2,"shards":["c~ring=s01"]},
                             {"name":"b","spec":2,"shards":["b~ring=s03"]},
                             {"name":"b","spec":2,"shards":["b~module=x"]},
                             {"name":"d","shards":["d~module=x"]},
                             {"name":"c","spec":4,"shards":["c~module=core","c~module=core"]},
                             {"name":"b","spec":2,"shards":["b~ring=s01","b~ring=s02"]},
                             {"name":"e","spec":2,"shards":["b~ring=s01"]},
                             {"name":"c","spec":4,"shards":["c~module=core"]},
                             {"name":"c","spec":4,"shards":["c~module=api"]},
                             {"name":"bad name","shards":["a"]}]}
                """);
        assertThat(ps.specs()).extracting(VersionedSpec::version).containsExactly(1, 2, 4);
        assertThat(ps.activeVersion()).as("version 2 belongs to project b").isEmpty();
        assertThat(ps.shardsOf("b")).containsExactly(id("b~ring=s01"), id("b~ring=s02"));
        assertThat(ps.shardsOf("c")).containsExactly(id("c~module=core"));
        assertThat(ps.projects()).containsExactly("a", "b", "c");
        assertThat(warnings).hasSize(14);
    }

    @Test
    void checkProjectEnforcesTheModel() {
        VersionedSpec ring = new VersionedSpec(2, Optional.of("lucene"), ShardSpec.parse("[project, ring(path, shards=2)]"));
        ProjectShards.checkProject("lucene", ring, List.of(id("lucene~ring=s01"), id("lucene~ring=s02")));
        ProjectShards.checkProject("lucene", null, List.of(id("lucene")));
        ProjectShards.checkProject("lucene", new VersionedSpec(1, Optional.empty(), ShardSpec.PROJECT), List.of(id("lucene")));
        assertThatThrownBy(() -> ProjectShards.checkProject("lucene", ring, List.of(id("lucene"))))
                .hasMessageContaining("not a shard id of project 'lucene'");
        assertThatThrownBy(() -> ProjectShards.checkProject("lucene", ring, List.of(id("lucene~ring=s03"))))
                .hasMessageContaining("not a shard id of project 'lucene'");
        assertThatThrownBy(() -> ProjectShards.checkProject("lucene", ring, List.of(id("lucenex~ring=s01"))))
                .hasMessageContaining("not a shard id");
        assertThatThrownBy(() -> ProjectShards.checkProject("other", ring, List.of(id("other~ring=s01"))))
                .hasMessageContaining("belongs to project 'lucene'");
        assertThatThrownBy(() -> ProjectShards.checkProject("lucene", ring, List.of()))
                .hasMessageContaining("no shards");
        assertThatThrownBy(() -> ProjectShards.checkProject("lucene", null, List.of(id("lucene"), id("lucene"))))
                .hasMessageContaining("twice");
        assertThatThrownBy(() -> ProjectShards.checkProject("lucene", null, List.of(id("lucene~ring=s01"))))
                .hasMessageContaining("exactly one shard");
        assertThatThrownBy(() -> ProjectShards.checkProject("a~b=c", null, List.of()))
                .hasMessageContaining("not a valid cluster project name");
    }
}
