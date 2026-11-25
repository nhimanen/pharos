package com.pharos.graph;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Graphs written before 1.10.0 can carry a stale ArcadeDB property dictionary (see
 * {@link CallGraphCreateFlushTest}); pharos always added the names in a fixed order, so the damage
 * shows as a name with another id.
 */
class CallGraphDictionaryTest {

    @TempDir Path tmp;

    private static Map<String, Integer> canonical(List<String> names) {
        Map<String, Integer> m = new LinkedHashMap<>();
        for (int i = 0; i < names.size(); i++) m.put(names.get(i), i);
        return m;
    }

    @Test
    void freshGraphHasTheFixedOrderAndWritesAddNoNames() throws Exception {
        Path dir = tmp.resolve("p").resolve("callgraph.arcadedb");
        try (CallGraph g = CallGraph.open(dir)) {
            assertThat(g.dictionaryDamage()).isNull();
            assertThat(g.dictionaryMap()).isEqualTo(canonical(CallGraph.DICTIONARY_ORDER));
            // every kind of record the builders write, stubs included
            g.addMethod("p.A#run()", "p.A");
            g.addCall("p.A#run()", "ext.X#go()");                                    // Method stub
            g.addCodeType("p.A", "p.A");
            g.addCodeField("p.A#n", "p.A", "int", "private");
            g.addKgEdge("declares_field", "p.A", "CodeType", "p.A#n", "CodeField");
            g.addKgEdge("reads_field", "p.A#run()", "Method", "p.B#m", "CodeField");  // CodeField stub
            g.addKgEdge("returns_type", "p.A#run()", "Method", "p.T", "CodeType");    // CodeType stub
            g.addKgEdge("annotated_by", "p.A", "CodeType", "p.Ann", "CodeType");
            g.flush();
            g.flushKnowledge();
            g.evictClasses(List.of("p.A"));
            g.addMethod("p.A#run()", "p.A");
            g.completeEviction();
            assertThat(g.dictionaryMap()).as("no name added after initSchema")
                    .isEqualTo(canonical(CallGraph.DICTIONARY_ORDER));
        }
        try (CallGraph g = CallGraph.openExisting(dir)) {
            assertThat(g.dictionaryDamage()).isNull();
            assertThat(g.dictionaryMap()).isEqualTo(canonical(CallGraph.DICTIONARY_ORDER));
        }
        assertThat(CallGraph.dictionaryProblem(dir)).isNull();
    }

    @Test
    void damagedSampleIsDetectedWarnedOnceAndNotExported() throws Exception {
        Path dir = tmp.resolve("sample-app").resolve("callgraph.arcadedb");
        DamagedGraphFixture.unpackCallGraph(dir);
        assertThat(CallGraph.dictionaryProblem(dir)).contains("fqn has id 2, not 0");

        List<ILoggingEvent> warnings = captureWarnings(() -> {
            for (int i = 0; i < 2; i++) {
                try (CallGraph g = CallGraph.openExisting(dir)) {
                    assertThat(g.dictionaryDamage()).contains("damaged property dictionary")
                            .contains("fqn has id 2, not 0");
                    Path out = tmp.resolve("out.cgf.gz");
                    assertThatThrownBy(() -> CompactGraphWriter.export(g, out))
                            .hasMessageContaining("damaged property dictionary")
                            .hasMessageContaining("pharos index --full sample-app");
                    assertThat(out).doesNotExist();
                }
            }
            return null;
        });
        assertThat(warnings).hasSize(1); // a reader opens per query: warned once per JVM
        assertThat(warnings.get(0).getFormattedMessage())
                .contains("project 'sample-app'").contains("pharos index --full sample-app");
    }

    @Test
    void outOfOrderDictionaryIsDetectedOnOpen() {
        Path dir = tmp.resolve("q").resolve("callgraph-1.arcadedb");
        DamagedGraphFixture.createWithDictionary(dir, List.of("classPrefix", "fqn"));
        try (CallGraph g = CallGraph.openExisting(dir)) {
            assertThat(g.dictionaryDamage()).contains("fqn has id 1, not 0").contains("classPrefix has id 0, not 1");
        }
        // a prefix in the fixed order (a graph from before the knowledge graph) is not damage
        Path old = tmp.resolve("r").resolve("callgraph.arcadedb");
        DamagedGraphFixture.createWithDictionary(old, List.of("fqn", "classPrefix"));
        try (CallGraph g = CallGraph.openExisting(old)) {
            assertThat(g.dictionaryDamage()).isNull();
            assertThat(g.dictionaryMap()).isEqualTo(canonical(CallGraph.DICTIONARY_ORDER));
        }
    }

    /**
     * The race's raw on-disk state, the header-only dictionary page (names refilled out of order
     * only by the next open), is damage too: the schema declares names the file lacks.
     */
    @Test
    void headerOnlyDictionaryIsDamage() throws Exception {
        Path sample = tmp.resolve("sample-app").resolve("callgraph.arcadedb");
        DamagedGraphFixture.unpackCallGraph(sample);
        DamagedGraphFixture.cutDictionaryToHeader(sample);
        Path fresh = tmp.resolve("p").resolve("callgraph-1.arcadedb");
        try (CallGraph g = CallGraph.open(fresh)) {
            g.addMethod("p.A#run()", "p.A");
        }
        assertThat(CallGraph.dictionaryProblem(fresh)).as("healthy before the cut").isNull();
        DamagedGraphFixture.cutDictionaryToHeader(fresh);
        for (Path dir : List.of(sample, fresh)) {
            assertThat(CallGraph.readDictionary(dir)).as(dir.toString()).isEmpty();
            assertThat(CallGraph.dictionaryProblem(dir)).as(dir.toString())
                    .contains("damaged property dictionary (no ").contains("fqn").contains("which the schema declares");
        }
        // unreadable schema: fail-safe, no claim of damage
        Files.writeString(fresh.resolve("schema.json"), "{not json");
        assertThat(CallGraph.dictionaryProblem(fresh)).isNull();
    }

    /** The file reader used without opening a database agrees with ArcadeDB's own dictionary. */
    @Test
    void dictionaryFileReaderMatchesArcadeDb() throws Exception {
        Path fresh = tmp.resolve("f").resolve("callgraph.arcadedb");
        CallGraph.open(fresh).close();
        Path damaged = tmp.resolve("d").resolve("callgraph.arcadedb");
        DamagedGraphFixture.unpackCallGraph(damaged);
        Path odd = tmp.resolve("o.arcadedb");
        DamagedGraphFixture.createWithDictionary(odd, List.of("zeta", "fqn", "äö-name"));
        for (Path dir : List.of(fresh, damaged, odd)) {
            var db = new com.arcadedb.database.DatabaseFactory(dir.toString()).open();
            Map<String, Integer> arcade;
            try {
                arcade = Map.copyOf(((com.arcadedb.database.DatabaseInternal) db).getSchema().getEmbedded()
                        .getDictionary().getDictionaryMap());
            } finally {
                db.close();
            }
            assertThat(CallGraph.readDictionary(dir)).as(dir.toString()).isEqualTo(arcade).isNotEmpty();
        }
        assertThat(CallGraph.readDictionary(tmp.resolve("none"))).isNull();
    }

    @Test
    void crossProjectGraphAdviceIsLink() {
        assertThat(CallGraph.repairAdvice(tmp.resolve("cross-project.arcadedb"))).contains("pharos link");
        assertThat(CallGraph.repairAdvice(tmp.resolve("proj").resolve("callgraph-12.arcadedb")))
                .isEqualTo("run: pharos index --full proj");
    }

    @Test
    void damagedModuleGraphIsRebuiltWithAWarning() {
        Path dir = tmp.resolve("module-graph.arcadedb");
        DamagedGraphFixture.createWithDictionary(dir, List.of("status", "moduleKey"));
        List<ILoggingEvent> warnings = captureWarnings(ModuleGraph.class, () -> {
            try (ModuleGraph g = ModuleGraph.open(dir)) {
                g.addOrUpdate("g:a", "g", "a", "1", "INDEXED", "a");
            }
            return null;
        });
        assertThat(warnings).singleElement().satisfies(w -> assertThat(w.getFormattedMessage())
                .contains("rebuilt empty").contains("moduleKey has id 1, not 0"));
        var db = new com.arcadedb.database.DatabaseFactory(dir.toString()).open();
        try {
            assertThat(CallGraph.dictionaryProblem(db, ModuleGraph.DICTIONARY_ORDER)).isNull();
        } finally {
            db.close();
        }
        // a healthy module graph is kept as it is
        List<ILoggingEvent> again = captureWarnings(ModuleGraph.class, () -> {
            try (ModuleGraph g = ModuleGraph.open(dir)) {
                assertThat(g.moduleCount()).isEqualTo(1);
            }
            return null;
        });
        assertThat(again).isEmpty();
        assertThat(Files.isDirectory(dir)).isTrue();
    }

    private static List<ILoggingEvent> captureWarnings(Supplier<Void> body) {
        return captureWarnings(CallGraph.class, body);
    }

    private static List<ILoggingEvent> captureWarnings(Class<?> logger, Supplier<Void> body) {
        Logger l = (Logger) LoggerFactory.getLogger(logger);
        ListAppender<ILoggingEvent> a = new ListAppender<>();
        a.start();
        l.addAppender(a);
        try {
            body.get();
        } finally {
            l.detachAppender(a);
        }
        return a.list.stream().filter(e -> e.getLevel() == Level.WARN).toList();
    }
}
