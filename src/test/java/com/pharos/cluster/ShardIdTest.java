package com.pharos.cluster;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class ShardIdTest {

    @Test
    void acceptsValidNames() {
        assertThat(ShardId.ofProject("pharos").value()).isEqualTo("pharos");
        assertThat(new ShardId("my-own-gpt").value()).isEqualTo("my-own-gpt");
        assertThat(new ShardId("a.b_c").value()).isEqualTo("a.b_c");
    }

    @Test
    void rejectsInvalidNames() {
        for (String bad : new String[]{"", "my project", "a/b", "..", "x..y", "a".repeat(129)}) {
            assertThatThrownBy(() -> new ShardId(bad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("project name '" + bad
                            + "' cannot be a shard id: use letters, digits, '.', '_' or '-'");
        }
    }

    @Test
    void acceptsKeyedIds() {
        for (String ok : new String[]{"lucene~ring=s03", "lucene~module=core", "pharos~module=pharos-core~ring=s01",
                "a.b_c~prefix=src-main", "p~module=_", "p~language=c.sharp"}) {
            assertThat(new ShardId(ok).value()).isEqualTo(ok);
            assertThat(ShardId.isValid(ok)).isTrue();
        }
        assertThat(ShardId.keyed("lucene", new java.util.LinkedHashMap<>(java.util.Map.of("ring", "s03"))))
                .isEqualTo(new ShardId("lucene~ring=s03"));
        assertThat(ShardId.keyed("lucene", new java.util.LinkedHashMap<>())).isEqualTo(ShardId.ofProject("lucene"));
    }

    @Test
    void rejectsMalformedKeyedIds() {
        for (String bad : new String[]{"~ring=s01", "lucene~", "lucene~ring", "lucene~=s01", "lucene~ring=",
                "lucene~Ring=s01", "lucene~ring=s0=1", "lucene~ring=s01~", "lucene=x", "lucene~ring=a..b",
                "lu cene~ring=s01", "lucene~ring=s 1", "lucene~ring=s01~~module=x", "-x~ring=s01",
                "p~module=" + "m".repeat(125)}) {
            assertThatThrownBy(() -> new ShardId(bad)).as(bad)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("shard id '" + bad + "' is invalid");
            assertThat(ShardId.isValid(bad)).isFalse();
        }
    }

    @Test
    void projectNamesStayStrict() {
        for (String bad : new String[]{"lucene~ring=s01", "a=b", "a~b"}) {
            assertThatThrownBy(() -> ShardId.ofProject(bad)).as(bad)
                    .hasMessage("project name '" + bad + "' cannot be a shard id: use letters, digits, '.', '_' or '-'");
            assertThat(ShardId.isProjectName(bad)).isFalse();
        }
    }

    @Test
    void encodingRoundTrips() {
        // project ids encode to themselves, byte for byte (existing buckets and caches keep working)
        for (String p : new String[]{"pharos", "my-own-gpt", "a.b_c", "A1", "x".repeat(128)}) {
            assertThat(ShardId.ofProject(p).pathSegment()).isEqualTo(p);
            assertThat(ShardId.fromPathSegment(p)).contains(ShardId.ofProject(p));
        }
        assertThat(new ShardId("lucene~ring=s03").pathSegment()).isEqualTo("lucene~ring%3Ds03");
        assertThat(new ShardId("pharos~module=core~ring=s01").pathSegment()).isEqualTo("pharos~module%3Dcore~ring%3Ds01");
        for (String id : new String[]{"lucene~ring=s03", "pharos~module=core~ring=s01", "p~prefix=_"}) {
            ShardId shard = new ShardId(id);
            String seg = shard.pathSegment();
            assertThat(seg).doesNotContain("=").doesNotContain("/");
            assertThat(seg).matches("[A-Za-z0-9._~%-]+");
            assertThat(ShardId.fromPathSegment(seg)).contains(shard);
        }
    }

    @Test
    void onlyCanonicalSegmentsDecode() {
        for (String bad : new String[]{"lucene~ring=s03", "lucene~ring%3ds03", "lucene%7Ering%3Ds03", "%70haros",
                "lucene~ring%3", "lucene~ring%ZZs03", ".blocks", ".blocks-private", "registry.json.tmp~x", "ü", "", ".."}) {
            assertThat(ShardId.fromPathSegment(bad)).as(bad).isEmpty();
        }
        assertThat(ShardId.fromPathSegment(null)).isEmpty();
        assertThat(ShardId.pathSegmentOf("lucene~ring=s03")).isEqualTo("lucene~ring%3Ds03");
        assertThat(ShardId.pathSegmentOf("my project")).isEqualTo("my project"); // not a shard id: unchanged
    }

    @Test
    void generationOrdering() {
        assertThat(Generation.FIRST.next()).isEqualTo(new Generation(2));
        assertThat(Generation.FIRST).isLessThan(Generation.FIRST.next());
    }
}
