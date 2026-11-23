package com.pharos.graph;

import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.model.ParsedProject;
import com.pharos.parser.model.ParsedRelationships;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompactGraphParityTest {

    @TempDir
    static Path tempDir;

    private static CallGraph arcade;
    private static CompactGraph compact;
    private static Path compactFile;

    @BeforeAll
    static void build() throws Exception {
        Path root = Paths.get(CompactGraphParityTest.class.getClassLoader()
                .getResource("test-projects/sample-app").toURI());
        JavaCodeParser parser = new JavaCodeParser();
        ParsedProject project = parser.parseProject(root, "sample-app");
        ParsedRelationships rel = parser.buildRelationships(project);

        arcade = CallGraph.open(tempDir.resolve("callgraph.arcadedb"));
        arcade.clear(); // like ProjectIndexManager: a full index clears first (dedupes vertices)
        new CallGraphBuilder().build(arcade, project);
        new KnowledgeGraphBuilder().build(arcade, rel);
        arcade.flush();
        arcade.flushKnowledge();
        for (String type : CallGraph.EDGE_TYPES) {
            int[] n = {0};
            arcade.forEachEdge(type, (a, b) -> n[0]++);
            sampleAppEdges.put(type, n[0]);
        }
        addInheritanceFixture();

        compactFile = tempDir.resolve(GraphViews.COMPACT_FILE);
        CompactGraphWriter.export(arcade, compactFile);
        compact = CompactGraph.read(compactFile);
    }

    /** Edge counts of the sample-app graph alone, before {@link #addInheritanceFixture}. */
    private static final Map<String, Integer> sampleAppEdges = new LinkedHashMap<>();

    /**
     * sample-app has no inheritance, so hand-add a hierarchy (diamond + chain, with one cycle-free
     * interface) to exercise inherits / implements_iface / allSubclasses.
     */
    private static void addInheritanceFixture() {
        for (String t : List.of("syn.Base", "syn.Mid", "syn.Leaf", "syn.Iface", "syn.Other")) {
            arcade.addCodeType(t, t);
        }
        arcade.addCodeField("syn.Base#zeta", "syn.Base", "int", "private");
        arcade.addCodeField("syn.Base#alpha", "syn.Base", "String", "public");
        arcade.addCodeField("syn.Base#mid", "syn.Base", "long", null);
        arcade.addKgEdge("declares_field", "syn.Base", "CodeType", "syn.Base#zeta", "CodeField");
        arcade.addKgEdge("declares_field", "syn.Base", "CodeType", "syn.Base#alpha", "CodeField");
        arcade.addKgEdge("declares_field", "syn.Base", "CodeType", "syn.Base#mid", "CodeField");
        arcade.addKgEdge("inherits", "syn.Mid", "CodeType", "syn.Base", "CodeType");
        arcade.addKgEdge("inherits", "syn.Leaf", "CodeType", "syn.Mid", "CodeType");
        arcade.addKgEdge("inherits", "syn.Other", "CodeType", "syn.Base", "CodeType");
        arcade.addKgEdge("implements_iface", "syn.Mid", "CodeType", "syn.Iface", "CodeType");
        arcade.addKgEdge("implements_iface", "syn.Leaf", "CodeType", "syn.Iface", "CodeType");
        arcade.addKgEdge("implements_iface", "syn.Base", "CodeType", "syn.Iface", "CodeType");
        // sample-app has no annotations either: a method and a class annotated by syn.Marker
        arcade.addCodeType("syn.Marker", "syn.Marker");
        arcade.addKgEdge("annotated_by", "syn.Mid", "CodeType", "syn.Marker", "CodeType");
        arcade.addKgEdge("annotated_by", "com.example.Calculator#abs(int)", "Method", "syn.Marker", "CodeType");
        arcade.flushKnowledge();
    }

    @AfterAll
    static void close() {
        if (arcade != null) arcade.close();
    }

    @Test
    void fixtureHasEdgesOfEveryType() {
        for (String type : CallGraph.EDGE_TYPES) {
            int[] n = {0};
            arcade.forEachEdge(type, (a, b) -> n[0]++);
            assertThat(n[0]).as("edges of type " + type).isGreaterThan(0);
        }
    }

    @Test
    void everyMethodMatchesArcade() {
        // CallGraph.allFqns() reads the non-unique Method[classPrefix] index, which lists a method
        // once more for every time a knowledge-graph edge re-saved its vertex (stale index entries,
        // see "Index hygiene" in CallGraph). The compact graph lists each method once.
        List<String> all = arcade.allFqns().distinct().sorted().toList();
        assertThat(all).isNotEmpty();
        assertThat(compact.allFqns().sorted().toList()).isEqualTo(all);
        assertThat(compact.methodCount()).isEqualTo(arcade.methodCount());
        assertThat(compact.callCount()).isEqualTo(arcade.callCount());
        assertThat(compact.calledTargets(new HashSet<>(all))).isEqualTo(arcade.calledTargets(new HashSet<>(all)));

        // single-argument queries over every name in the graph, including class, field and unknown names
        Set<String> names = new LinkedHashSet<>(all);
        arcade.forEachVertex(v -> {
            names.add(v.fqn());
            if (v.ownerFqn() != null) names.add(v.ownerFqn());
            if (v.classPrefix() != null) names.add(v.classPrefix());
        });
        names.add("com.example.DoesNotExist#nope()");
        assertThat(names.size()).isGreaterThan(all.size());

        List<Function<GraphView, Function<String, Object>>> setQueries = List.of(
                g -> g::callers, g -> g::callees, g -> g::directSubclasses, g -> g::directSuperTypes,
                g -> g::fieldReaders, g -> g::fieldWriters, g -> g::declaredFields, g -> g::annotatedWith,
                g -> g::methodsReturning, g -> g::methodsTaking);
        int nonEmpty = 0;
        for (String n : names) {
            for (var q : setQueries) {
                Object expected = q.apply(arcade).apply(n);
                assertThat(q.apply(compact).apply(n)).as(n).isEqualTo(expected);
                if (!((Set<?>) expected).isEmpty()) nonEmpty++;
            }
            // Same classes, each once, start excluded. The BFS discovery order depends on how ArcadeDB
            // interleaves a vertex's inherits / implements_iface in-edges (global insertion order),
            // which the per-type compact file does not record, so it is not compared.
            List<String> subs = compact.allSubclasses(n);
            assertThat(subs).as("allSubclasses " + n).doesNotHaveDuplicates().doesNotContain(n)
                    .containsExactlyInAnyOrderElementsOf(arcade.allSubclasses(n));
            // Row order of CallGraph.getFieldsOf is the order of its ownerFqn index entries, which
            // changes whenever a knowledge-graph edge re-saves a field vertex; compare contents.
            assertThat(compact.getFieldsOf(n)).as("getFieldsOf " + n)
                    .containsExactlyInAnyOrderElementsOf(arcade.getFieldsOf(n));
        }
        assertThat(nonEmpty).isGreaterThan(20);
        // transitive subclass queries really traverse something
        assertThat(names.stream().anyMatch(n -> !arcade.allSubclasses(n).isEmpty())).isTrue();
        assertThat(names.stream().anyMatch(n -> !arcade.getFieldsOf(n).isEmpty())).isTrue();
    }

    @Test
    void shortestPathMatches() {
        List<String> all = arcade.allFqns().distinct().sorted().toList();
        Random rnd = new Random(42);
        int connected = 0;
        for (int i = 0; i < 50; i++) {
            String a = all.get(rnd.nextInt(all.size()));
            String b = i < 10 ? a : all.get(rnd.nextInt(all.size())); // include from == to
            List<String> expected = arcade.shortestPath(a, b);
            List<String> actual = compact.shortestPath(a, b);
            assertThat(actual).hasSameSizeAs(expected);
            if (!actual.isEmpty()) {
                connected++;
                assertThat(actual.get(0)).isEqualTo(a);
                assertThat(actual.get(actual.size() - 1)).isEqualTo(b);
                for (int k = 0; k + 1 < actual.size(); k++) {
                    assertThat(arcade.callees(actual.get(k))).contains(actual.get(k + 1));
                }
            }
        }
        assertThat(connected).isGreaterThan(0);
        // directly connected pairs
        for (String a : all) {
            for (String b : arcade.callees(a)) {
                assertThat(compact.shortestPath(a, b)).hasSameSizeAs(arcade.shortestPath(a, b));
            }
        }
    }

    @Test
    void emptyGraphRoundTrips() throws Exception {
        Path file = tempDir.resolve("empty").resolve("g.cgf.gz");
        try (CallGraph empty = CallGraph.open(tempDir.resolve("empty.arcadedb"))) {
            CompactGraphWriter.export(empty, file);
        }
        CompactGraph g = CompactGraph.read(file);
        assertThat(g.methodCount()).isZero();
        assertThat(g.callCount()).isZero();
        assertThat(g.allFqns().toList()).isEmpty();
        assertThat(g.callers("x")).isEmpty();
        assertThat(g.allSubclasses("x")).isEmpty();
        assertThat(g.shortestPath("a", "b")).isEmpty();
    }

    @Test
    void rejectsWrongMagic() throws Exception {
        Path garbage = tempDir.resolve("garbage.gz");
        Files.writeString(garbage, "this is not a graph at all");
        assertThatThrownBy(() -> CompactGraph.read(garbage))
                .isInstanceOf(IOException.class).hasMessageContaining("not a pharos compact graph");

        Path gzipped = tempDir.resolve("wrongmagic.gz");
        try (var out = new java.util.zip.GZIPOutputStream(Files.newOutputStream(gzipped))) {
            out.write("NOPE0000000000".getBytes());
        }
        assertThatThrownBy(() -> CompactGraph.read(gzipped))
                .isInstanceOf(IOException.class).hasMessageContaining("not a pharos compact graph");
    }

    @Test
    void corruptCountsGiveIOExceptionNotError() throws Exception {
        for (String what : List.of("vertexCount", "stringLength", "edgeCount")) {
            Path f = tempDir.resolve("corrupt-" + what + ".gz");
            try (var out = new java.io.DataOutputStream(new java.util.zip.GZIPOutputStream(Files.newOutputStream(f)))) {
                out.write("PHGR".getBytes());
                out.writeInt(1);
                switch (what) {
                    case "vertexCount" -> out.writeInt(Integer.MAX_VALUE);
                    case "stringLength" -> { out.writeInt(1); out.writeByte(0); out.writeInt(Integer.MAX_VALUE); }
                    default -> { // zero vertices, then a huge calls edge count with no data
                        out.writeInt(0);
                        out.writeInt(Integer.MAX_VALUE);
                    }
                }
            }
            assertThatThrownBy(() -> CompactGraph.read(f)).as(what).isInstanceOf(IOException.class);
        }
    }

    @Test
    void graphViewsOpensCompactFileAndSurfacesCorruption() throws Exception {
        Path shard = Files.createDirectory(tempDir.resolve("shard-compact"));
        Files.copy(compactFile, shard.resolve(GraphViews.COMPACT_FILE));
        try (GraphView g = GraphViews.open(shard)) {
            assertThat(g).isInstanceOf(CompactGraph.class);
            assertThat(g.methodCount()).isEqualTo(arcade.methodCount());
        }
        Path bad = Files.createDirectory(tempDir.resolve("shard-bad"));
        Files.writeString(bad.resolve(GraphViews.COMPACT_FILE), "garbage");
        assertThatThrownBy(() -> GraphViews.open(bad))
                .isInstanceOf(java.io.UncheckedIOException.class)
                .hasMessageContaining(GraphViews.COMPACT_FILE);
    }
}
