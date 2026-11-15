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
}
