package com.pharos.graph;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A vertex with more than ArcadeDB 26's default super-node threshold (4,096 edges in one direction)
 * keeps the plain edge-list layout ({@link ArcadeDbSettings}): no striped buckets and no bloom
 * filters that ArcadeDB 25.1.1 (pharos 1.11.x) cannot read, every edge comes back, and the exact
 * newest-first order that {@link CallGraph#load} relies on survives an export and import.
 */
class CallGraphSupernodeTest {

    private static final int EDGES = 20_000; // ArcadeDB estimates degree from bytes (~8 per edge): small RIDs need more than 4,096

    @TempDir Path tmp;

    @Test
    void busyVerticesStayInThePlainLayoutAndKeepTheirOrder() throws Exception {
        Path dir = tmp.resolve("callgraph.arcadedb");
        List<String> calleesInOrder = new ArrayList<>();
        try (CallGraph g = CallGraph.open(dir)) {
            g.addMethod("p.Hub#run()", "p.Hub");
            g.addMethod("p.Fan#out()", "p.Fan");
            for (int i = 0; i < EDGES; i++) {
                String m = "p.C" + i + "#m()";
                g.addMethod(m, "p.C" + i);
                g.addCall(m, "p.Hub#run()");          // well past the 4,096 threshold, in-edges
                g.addCall("p.Fan#out()", m);          // and out-edges
            }
            g.flush();
            calleesInOrder.addAll(g.callees("p.Fan#out()"));
        }
        try (Stream<Path> files = Files.list(dir)) {
            List<String> names = files.map(p -> p.getFileName().toString()).toList();
            assertThat(names).as("no striped super-node buckets").noneMatch(n -> n.contains("_sn_stripe"));
            assertThat(names).as("no bloom filters").noneMatch(n -> n.endsWith(".bfidx"));
        }

        try (CallGraph g = CallGraph.openExisting(dir)) {
            assertThat(g.callers("p.Hub#run()")).hasSize(EDGES);
            List<String> callees = new ArrayList<>(g.callees("p.Fan#out()"));
            assertThat(callees).hasSize(EDGES).isEqualTo(calleesInOrder);
            // the plain layout lists out-edges exactly newest first
            assertThat(callees.get(0)).isEqualTo("p.C" + (EDGES - 1) + "#m()");
            assertThat(callees.get(EDGES - 1)).isEqualTo("p.C0#m()");

            Path cgf = tmp.resolve("g.cgf.gz");
            CompactGraphWriter.export(g, cgf);
            Path imported = tmp.resolve("imported.arcadedb");
            CompactGraphImport.importInto(cgf, imported);
            try (CallGraph i = CallGraph.openExisting(imported)) {
                assertThat(new ArrayList<>(i.callees("p.Fan#out()"))).isEqualTo(callees);
                Path again = tmp.resolve("again.cgf.gz");
                CompactGraphWriter.export(i, again);
                assertThat(gunzip(again)).isEqualTo(gunzip(cgf));
            }
        }
    }

    private static byte[] gunzip(Path f) throws java.io.IOException {
        try (var in = new java.util.zip.GZIPInputStream(Files.newInputStream(f))) {
            return in.readAllBytes();
        }
    }
}
