package com.pharos.cluster.sharding;

import com.pharos.cluster.ShardId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.*;

class ShardSpecTest {

    private static final int PATHS = 50_000;

    // ── parse and validation ─────────────────────────────────────────────────────────────

    @Test
    void parsesEverySpecExample() {
        assertThat(ShardSpec.parse("[project]")).isEqualTo(ShardSpec.PROJECT);
        assertThat(ShardSpec.parse("[project]").isDefault()).isTrue();
        assertThat(ShardSpec.parse("[project, module]").toString()).isEqualTo("[project, module]");
        assertThat(ShardSpec.parse("[project, ring(path, shards=8)]").toString())
                .isEqualTo("[project, ring(path, shards=8, vnodes=512)]");
        assertThat(ShardSpec.parse("[project, ring(path, shards=8)]").ring().orElseThrow().vnodes()).isEqualTo(512);
        // the project is always the first key (P6-R1): [language] means [project, language]
        assertThat(ShardSpec.parse("[language]")).isEqualTo(ShardSpec.parse("[project, language]"));
        assertThat(ShardSpec.parse("[language]").toString()).isEqualTo("[project, language]");
        assertThat(ShardSpec.parse("[project, kind]").toString()).isEqualTo("[project, kind]");
        assertThat(ShardSpec.parse("[project, prefix(path)]").toString()).isEqualTo("[project, prefix(path)]");
        assertThat(ShardSpec.parse("[project, prefix(path, depth=2)]").toString()).isEqualTo("[project, prefix(path, depth=2)]");
        assertThat(ShardSpec.parse("[project, module, ring(path, shards=4, vnodes=64)]").toString())
                .isEqualTo("[project, module, ring(path, shards=4, vnodes=64)]");
    }

    @Test
    void canonicalFormIsWhitespaceAndDefaultInsensitive() {
        ShardSpec a = ShardSpec.parse("  [ project ,ring( path,shards = 3 , vnodes=512 ) ] ");
        assertThat(a.toString()).isEqualTo("[project, ring(path, shards=3, vnodes=512)]");
        assertThat(ShardSpec.parse(a.toString())).isEqualTo(a);
        assertThat(ShardSpec.parse("[project, ring(path, shards=[s01, s02, s03])]")).isEqualTo(a);
        assertThat(ShardSpec.parse("[project, ring(path, shards=[s03,s01,s02])]")).isEqualTo(a);
        assertThat(ShardSpec.parse("[project, ring(path, shards=[s01, s02, s04])]").toString())
                .isEqualTo("[project, ring(path, shards=[s01, s02, s04], vnodes=512)]");
    }

    /** I2: the canonical form (what the catalog stores) always names vnodes, so it never depends on the default. */
    @Test
    void canonicalFormNeverDependsOnTheDefaultVnodes() {
        for (String spec : List.of("[project, ring(path, shards=8)]", "[project, ring(path, shards=8, vnodes=128)]",
                "[project, module, ring(path, shards=[s01, s03], vnodes=64)]")) {
            ShardSpec s = ShardSpec.parse(spec);
            String canonical = s.toString();
            assertThat(canonical).as(spec).contains("vnodes=" + s.ring().orElseThrow().vnodes());
            assertThat(ShardSpec.parse(canonical)).isEqualTo(s);
            assertThat(ShardSpec.parse(canonical).ring().orElseThrow().vnodes()).isEqualTo(s.ring().orElseThrow().vnodes());
            // what parsing the canonical form means is fixed by the text alone
            assertThat(ShardSpec.parse(canonical.replace("vnodes=", "vnodes= ")).toString()).isEqualTo(canonical);
        }
        assertThat(ShardSpec.parse("[project, ring(path, shards=8, vnodes=128)]"))
                .isNotEqualTo(ShardSpec.parse("[project, ring(path, shards=8)]"));
    }

    @Test
    void rejectsInvalidSpecs() {
        String[] bad = {
                "", "project", "[]", "[project", "[project]]", "[project] x", "[Project]", "[owner]",
                "[module, project]", "[project, project]", "[project, module, module]",
                "[project, ring(path, shards=8), module]", "[project, ring(path)]", "[project, ring(name, shards=2)]",
                "[project, ring(path, shards=0)]", "[project, ring(path, shards=1000)]", "[project, ring(path, shards=2, vnodes=0)]",
                "[project, ring(path, shards=2, vnodes=2000)]", "[project, ring(path, shards=2, size=3)]",
                "[project, ring(path, shards=[s1])]", "[project, ring(path, shards=[s00])]", "[project, ring(path, shards=[])]",
                "[project, ring(path, shards=[s01, s01])]", "[project, ring(path, shards=2, shards=3)]",
                "[project, ring(path, shards=2), ring(path, shards=3)]", "[project, module(x)]",
                "[project, prefix(path, depth=0)]", "[project, prefix(path, depth=17)]", "[project, prefix(path, size=1)]",
                "[project, hash(path) % 8]",
        };
        for (String s : bad) {
            assertThatThrownBy(() -> ShardSpec.parse(s)).as(s)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("invalid shard spec '" + s + "': ");
        }
        assertThatThrownBy(() -> ShardSpec.parse(null)).isInstanceOf(IllegalArgumentException.class);
    }

    // ── assignment ───────────────────────────────────────────────────────────────────────

    @Test
    void defaultSpecGivesProjectIds() {
        ShardSpec spec = ShardSpec.parse("[project]");
        for (String p : List.of("pharos", "lucene", "my-own-gpt", "a.b_c")) {
            assertThat(spec.assign(new DocKeys(p, "src/A.java", "core", "java", "code"))).isEqualTo(ShardId.ofProject(p));
            assertThat(spec.assign(DocKeys.of(p, "x/y/z.md")).value()).isEqualTo(p);
        }
        assertThatThrownBy(() -> spec.assign(DocKeys.of("my project", "a"))).hasMessageContaining("cannot be a shard id");
    }

    @Test
    void keyFieldsGiveKeyedIds() {
        DocKeys k = new DocKeys("pharos", "pharos-core/src/main/java/A.java", "pharos-core", "java", "code");
        assertThat(ShardSpec.parse("[project, module]").assign(k).value()).isEqualTo("pharos~module=pharos-core");
        assertThat(ShardSpec.parse("[language]").assign(k).value()).isEqualTo("pharos~language=java");
        assertThat(ShardSpec.parse("[project, kind]").assign(k).value()).isEqualTo("pharos~kind=code");
        assertThat(ShardSpec.parse("[project, prefix(path)]").assign(k).value()).isEqualTo("pharos~prefix=pharos-core");
        assertThat(ShardSpec.parse("[project, prefix(path, depth=3)]").assign(k).value())
                .isEqualTo("pharos~prefix=pharos-core-src-main");
        assertThat(ShardSpec.parse("[project, module, language]").assign(k).value())
                .isEqualTo("pharos~module=pharos-core~language=java");
        // missing values and files in the project root
        assertThat(ShardSpec.parse("[project, module]").assign(DocKeys.of("pharos", "pom.xml")).value())
                .isEqualTo("pharos~module=_");
        assertThat(ShardSpec.parse("[project, prefix(path)]").assign(DocKeys.of("pharos", "pom.xml")).value())
                .isEqualTo("pharos~prefix=_");
    }

    @Test
    void keyValuesAreMadeIdSafe() {
        assertThat(ShardSpec.idValue("com.example:core")).isEqualTo("com.example-core");
        assertThat(ShardSpec.idValue("Docs")).isEqualTo("docs");
        assertThat(ShardSpec.idValue("CamelCase-Module_X")).isEqualTo("camelcase-module_x");
        assertThat(ShardSpec.idValue("a..b/./c")).isEqualTo("a.b-.-c");
        assertThat(ShardSpec.idValue("x~y=z")).isEqualTo("x-y-z");
        assertThat(ShardSpec.idValue("ünï code")).isEqualTo("-n--code");
        assertThat(ShardSpec.idValue(null)).isEqualTo("_");
        String longValue = ShardSpec.idValue("m".repeat(200));
        assertThat(longValue).hasSize(64).startsWith("m".repeat(55) + "-");
        assertThat(ShardSpec.idValue("M".repeat(200))).as("hash of the original, not the lowered form").isNotEqualTo(longValue);
        assertThat(ShardSpec.idValue("m".repeat(201))).isNotEqualTo(longValue).hasSize(64);
        ShardSpec spec = ShardSpec.parse("[project, module]");
        assertThat(ShardId.isValid(spec.assign(new DocKeys("p", "a", "..weird//=~name..", null, null)).value())).isTrue();
    }

    /** I3: key values are lower-cased (case-insensitive file systems); ring names and the project are not. */
    @Test
    void keyValuesAreLowerCasedButProjectAndRingNamesAreNot() {
        ShardSpec prefix = ShardSpec.parse("[project, prefix(path)]");
        ShardId upper = prefix.assign(DocKeys.of("MyProj", "Docs/a.md"));
        ShardId lower = prefix.assign(DocKeys.of("MyProj", "docs/b.md"));
        assertThat(upper).isEqualTo(lower);
        assertThat(upper.value()).isEqualTo("MyProj~prefix=docs");
        assertThat(upper.pathSegment()).isEqualTo("MyProj~prefix%3Ddocs");
        assertThat(ShardSpec.parse("[project, module]").assign(new DocKeys("Lucene", "a", "Core", null, null)).value())
                .isEqualTo("Lucene~module=core");
        assertThat(ShardSpec.parse("[project, ring(path, shards=3)]").assign(DocKeys.of("MyProj", "X/Y.java")).value())
                .matches("MyProj~ring=s0[123]");
        assertThat(ShardSpec.PROJECT.assign(DocKeys.of("MyProj", "a")).value()).isEqualTo("MyProj");
    }

    @Test
    void tooLongIdIsAClearError() {
        String project = "p".repeat(120);
        assertThatThrownBy(() -> ShardSpec.parse("[project, module]").assign(new DocKeys(project, "a", "core", null, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gives project '" + project + "' an invalid shard id");
    }

    @Test
    void pathHashStableAcrossPlatforms() {
        // golden values: the first 64 bits of MurmurHash3_x64_128 (seed 0) of the UTF-8 bytes, checked
        // against Guava's Hashing.murmur3_128().hashString(s, UTF_8).asLong()
        assertThat(Murmur3.hash64("")).isEqualTo(0L);
        assertThat(Murmur3.hash64("a")).isEqualTo(-8839064797231613815L);
        assertThat(Murmur3.hash64("README.md")).isEqualTo(-6294286134569877059L);
        assertThat(Murmur3.hash64("pom.xml")).isEqualTo(4299733125303115014L);
        assertThat(Murmur3.hash64("src/main/java/com/pharos/Main.java")).isEqualTo(4246426027785246476L);
        assertThat(Murmur3.hash64("core/src/java/org/apache/lucene/index/IndexWriter.java")).isEqualTo(9014181037907508529L);
        assertThat(Murmur3.hash64("docs/a b/ü.md")).isEqualTo(-3914081944222933218L);
        assertThat(Murmur3.hash64("päth/ünïcode/文件.go")).isEqualTo(-7290290765291457366L);
        assertThat(Murmur3.hash64("0123456789abcdef")).isEqualTo(5467490433528156583L); // exactly one block
        assertThat(Murmur3.hash64("0123456789abcdefg")).isEqualTo(-8200385122730116642L); // block + 1-byte tail
        assertThat(Murmur3.hash64("abcdefghijklmno")).isEqualTo(-8449275918290243589L); // 15-byte tail
        assertThat(Murmur3.hash64("lucene~ring=s01#0")).isEqualTo(-8130210811261702475L); // a ring point
        assertThat(Murmur3.hash64("s01#0")).isEqualTo(3404105333452629188L);

        // a Windows-style or ./-prefixed path is the same file, with the same position and shard
        ShardSpec spec = ShardSpec.parse("[project, ring(path, shards=8)]");
        for (String variant : List.of("src/main/java/com/pharos/Main.java", "src\\main\\java\\com\\pharos\\Main.java",
                "./src/main/java/com/pharos/Main.java", "src//main/./java/com/pharos/Main.java")) {
            assertThat(spec.assign(DocKeys.of("lucene", variant)).value()).as(variant).isEqualTo("lucene~ring=s04");
        }
        // golden assignments pin the ring layout (512 points per shard at murmur3_64(shardId + "#" + i))
        assertThat(spec.assign(DocKeys.of("lucene", "README.md")).value()).isEqualTo("lucene~ring=s02");
        assertThat(spec.assign(DocKeys.of("lucene", "core/src/java/org/apache/lucene/index/IndexWriter.java")).value())
                .isEqualTo("lucene~ring=s04");
        assertThat(spec.assign(DocKeys.of("lucene", "docs/a b/ü.md")).value()).isEqualTo("lucene~ring=s03");
        assertThat(spec.assign(DocKeys.of("lucene", "pom.xml")).value()).isEqualTo("lucene~ring=s02");
    }

    @Test
    void sameFileSameShard() {
        ShardSpec spec = ShardSpec.parse("[project, module, ring(path, shards=8)]");
        ShardSpec again = ShardSpec.parse("[project, module, ring(path, shards=8)]");
        for (int i = 0; i < 2_000; i++) {
            String path = syntheticPath(i);
            // class, method and chunk documents of a file carry the same keys
            ShardId first = spec.assign(new DocKeys("lucene", path, "core", "java", "code"));
            assertThat(spec.assign(new DocKeys("lucene", path, "core", "java", "code"))).isEqualTo(first);
            assertThat(again.assign(new DocKeys("lucene", "./" + path, "core", "java", "code"))).isEqualTo(first);
        }
    }

    /** Ruling P6-R10: with the default 512 vnodes every shard is within ±15% of the mean, for every project. */
    @Test
    void ringBalance() {
        ShardSpec spec = ShardSpec.parse("[project, ring(path, shards=8)]");
        assertThat(spec.ring().orElseThrow().vnodes()).isEqualTo(512);
        double mean = PATHS / 8.0;
        for (String project : List.of("lucene", "pharos", "kafka", "demo", "spring-boot", "linux", "a", "b", "c", "h")) {
            Map<ShardId, Integer> counts = new HashMap<>();
            for (int i = 0; i < PATHS; i++) counts.merge(spec.assign(DocKeys.of(project, syntheticPath(i))), 1, Integer::sum);
            assertThat(counts).as(project).hasSize(8);
            counts.forEach((shard, n) -> assertThat(n).as(shard.value())
                    .isBetween((int) (mean * 0.85), (int) Math.ceil(mean * 1.15)));
        }
    }

    @Test
    void addingAShardMovesOnlyItsShare() {
        ShardSpec eight = ShardSpec.parse("[project, ring(path, shards=8)]");
        ShardSpec nine = ShardSpec.parse("[project, ring(path, shards=9)]");
        ShardId s09 = new ShardId("lucene~ring=s09");
        int moved = 0;
        for (int i = 0; i < PATHS; i++) {
            DocKeys k = DocKeys.of("lucene", syntheticPath(i));
            ShardId before = eight.assign(k);
            ShardId after = nine.assign(k);
            if (!before.equals(after)) {
                moved++;
                assertThat(after).as("8 → 9: every moved file goes to the new shard").isEqualTo(s09);
            }
            // 9 → 8 moves only the removed shard's files, each back to where it was
            if (!after.equals(s09)) assertThat(before).isEqualTo(after);
        }
        double share = moved / (double) PATHS;
        assertThat(share).isBetween(0.08, 0.14);
    }

    @Test
    void shardNamesAreStable() {
        ShardSpec all = ShardSpec.parse("[project, ring(path, shards=8)]");
        ShardSpec without3 = ShardSpec.parse("[project, ring(path, shards=[s01, s02, s04, s05, s06, s07, s08])]");
        assertThat(without3.ring().orElseThrow().names()).containsExactly("s01", "s02", "s04", "s05", "s06", "s07", "s08");
        ShardId s03 = new ShardId("lucene~ring=s03");
        Map<String, Integer> s03Went = new TreeMap<>();
        for (int i = 0; i < PATHS; i++) {
            DocKeys k = DocKeys.of("lucene", syntheticPath(i));
            ShardId before = all.assign(k);
            ShardId after = without3.assign(k);
            assertThat(after).isNotEqualTo(s03);
            if (before.equals(s03)) {
                s03Went.merge(after.value(), 1, Integer::sum);
            } else {
                assertThat(after).as("s04 and the others keep their names and their files").isEqualTo(before);
            }
        }
        assertThat(s03Went).isNotEmpty();
        // the names never shift: no file of the shrunk ring lands on a shard named after its position
        assertThat(without3.assign(DocKeys.of("lucene", "pom.xml"))).isEqualTo(all.assign(DocKeys.of("lucene", "pom.xml")));
    }

    @Test
    void ringIsPerKeyGroup() {
        ShardSpec spec = ShardSpec.parse("[project, module, ring(path, shards=2)]");
        assertThat(spec.assign(new DocKeys("lucene", "a/B.java", "core", null, null)).value())
                .matches("lucene~module=core~ring=s0[12]");
        assertThat(spec.assign(new DocKeys("lucene", "a/B.java", "analysis", null, null)).value())
                .matches("lucene~module=analysis~ring=s0[12]");
    }

    @Test
    void singleShardRingTakesEverything() {
        ShardSpec spec = ShardSpec.parse("[project, ring(path, shards=1, vnodes=1)]");
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 100; i++) ids.add(spec.assign(DocKeys.of("p", syntheticPath(i))).value());
        assertThat(ids).containsOnly("p~ring=s01");
    }

    /** Paths shaped like a real source tree: a few modules, many packages, unique file names. */
    static String syntheticPath(int i) {
        return "module" + (i % 7) + "/src/main/java/org/example/pkg" + (i % 113) + "/sub" + (i % 17)
                + "/File" + i + ".java";
    }
}
