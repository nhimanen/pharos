package com.pharos.graph;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class GraphViewsTest {

    @TempDir Path tmp;

    @Test
    void missingGraphIsEmptyAndCreatesNothing() throws Exception {
        try (GraphView v = GraphViews.open(tmp)) {
            assertTrue(v.callers("x").isEmpty());
            assertEquals(0, v.methodCount());
        }
        try (Stream<Path> s = Files.list(tmp)) {
            assertEquals(0, s.count());
        }
    }

    @Test
    void arcadeGraphIsUsedWhenPresent() {
        try (CallGraph g = CallGraph.open(tmp.resolve(GraphViews.ARCADE_DIR), true)) {
            g.addMethod("a.A#caller()", "a.A");
            g.addMethod("a.A#callee()", "a.A");
            g.addCall("a.A#caller()", "a.A#callee()");
            g.flush();
        }
        try (GraphView v = GraphViews.open(tmp)) {
            assertEquals(Set.of("a.A#caller()"), v.callers("a.A#callee()"));
        }
    }

    /** A reader generation dir holding only the compact graph exported from a tiny arcade graph. */
    private Path generationDir(String name, String callee) throws Exception {
        Path arcade = tmp.resolve("arcade-" + name);
        try (CallGraph g = CallGraph.open(arcade, true)) {
            g.addMethod("a.A#caller()", "a.A");
            g.addMethod(callee, "a.A");
            g.addCall("a.A#caller()", callee);
            g.flush();
            Path dir = Files.createDirectories(tmp.resolve("cache").resolve(name));
            CompactGraphWriter.export(g, dir.resolve(GraphViews.COMPACT_FILE));
            return dir;
        }
    }

    @Test
    void compactGraphIsParsedOncePerGenerationDir() throws Exception {
        Path gen1 = generationDir("gen-1", "a.A#callee()");
        long reads = GraphViews.compactReads.get();
        GraphView first = GraphViews.open(gen1);
        first.close(); // no-op: the cached instance stays usable
        GraphView second = GraphViews.open(gen1.resolve(".").resolve("..").resolve("gen-1"));
        assertSame(first, second);
        assertEquals(reads + 1, GraphViews.compactReads.get(), "file read once");
        assertEquals(Set.of("a.A#caller()"), second.callers("a.A#callee()"));
        GraphViews.evict(gen1);
    }

    @Test
    void activatingAnotherGenerationEvictsThePrevious() throws Exception {
        Path gen1 = generationDir("gen-1", "a.A#callee()");
        Path gen2 = generationDir("gen-2", "a.A#other()");
        com.pharos.cluster.ReaderLocalCache cache = new com.pharos.cluster.ReaderLocalCache(tmp.resolve("cache"));
        cache.activate("p", gen1);
        GraphView g1 = GraphViews.open(gen1);
        int cached = GraphViews.cachedCount();
        long reads = GraphViews.compactReads.get();

        cache.activate("p", gen2);
        assertEquals(cached - 1, GraphViews.cachedCount(), "gen-1 evicted on activation of gen-2");
        assertEquals(Set.of("a.A#caller()"), GraphViews.open(gen2).callers("a.A#other()"));
        assertNotSame(g1, GraphViews.open(gen1), "re-read after eviction");
        assertEquals(reads + 2, GraphViews.compactReads.get());
        GraphViews.evict(gen1);
        GraphViews.evict(gen2);
    }

    @Test
    void rewrittenFileIsReRead() throws Exception {
        Path gen = generationDir("gen-7", "a.A#callee()");
        GraphView before = GraphViews.open(gen);
        Files.setLastModifiedTime(gen.resolve(GraphViews.COMPACT_FILE),
                java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 60_000));
        assertNotSame(before, GraphViews.open(gen));
        GraphViews.evict(gen);
    }
}
