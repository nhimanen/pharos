package com.pharos.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A graph created by {@link CallGraph#open} reads back intact even when ArcadeDB's asynchronous
 * flush thread writes the new database's dictionary page late: ArcadeDB
 * <a href="https://github.com/ArcadeData/arcadedb/issues/13441">#13441</a>.
 *
 * <p>{@code DatabaseFactory.create()} commits the property dictionary's header page through the
 * asynchronous flush queue; {@code setAsyncFlush(false)} only applies after that. The schema then
 * rewrites the same page synchronously (it adds the property names). When the queued, older copy
 * reaches the disk after that synchronous write, the file keeps the empty dictionary and every later
 * open decodes the records' property names wrongly, with no error.
 *
 * <p>Deterministic: ArcadeDB's test hook {@link PageManager#setPageWriteFaultInjector} holds the
 * flush thread's write of dictionary page 0 (file 0) of the databases under this test's temp dir.
 */
class CallGraphCreateFlushTest {

    @TempDir Path tmp;

    @AfterEach
    void removeInjector() {
        PageManager.setPageWriteFaultInjector(null);
    }

    /**
     * Holds the async flush thread's write of dictionary page 0 for databases under {@code root}
     * until {@link #release} or {@code maxHoldMs}.
     */
    private static final class HeldDictionaryWrite implements PageManager.PageWriteFaultInjector {
        private final String root;
        private final long maxHoldMs;
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger held = new AtomicInteger();
        final AtomicInteger written = new AtomicInteger();

        HeldDictionaryWrite(Path root, long maxHoldMs) {
            this.root = root.toAbsolutePath().normalize().toString();
            this.maxHoldMs = maxHoldMs;
        }

        @Override
        public void onPageWrite(PageId id) {
            if (id.getFileId() != 0 || id.getPageNumber() != 0) return;
            if (!Thread.currentThread().getName().contains("AsyncFlush")) return;
            if (!Path.of(id.getDatabase().getDatabasePath()).toAbsolutePath().normalize().toString().startsWith(root)) return;
            held.incrementAndGet();
            try {
                release.await(maxHoldMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            written.incrementAndGet();
        }
    }

    /**
     * The tripwire: ArcadeDB's plain create, then synchronous schema writes, under the same hold,
     * leaves the header-only dictionary on disk. If this fails, ArcadeDB fixed #13441 (or the hook
     * no longer reaches the write) and {@link CallGraph#createSettled} can be revisited.
     */
    @Test
    void plainCreateLosesTheDictionaryUnderALateFlush() throws Exception {
        Path dir = tmp.resolve("plain.arcadedb");
        HeldDictionaryWrite hold = new HeldDictionaryWrite(tmp, 10_000);
        PageManager.setPageWriteFaultInjector(hold);
        Database db = new DatabaseFactory(dir.toString()).create();
        try {
            db.setAsyncFlush(false);
            var method = db.getSchema().createVertexType("Method");
            method.createProperty("fqn", Type.STRING);
            method.createProperty("classPrefix", Type.STRING);
            assertThat(hold.held.get()).as("the create's dictionary write was held").isEqualTo(1);
        } finally {
            hold.release.countDown(); // the older queued copy lands after the synchronous rewrite
            db.close();
        }
        assertThat(CallGraph.readDictionary(dir)).as("header-only dictionary on disk").isEmpty();
        assertThat(CallGraph.dictionaryProblem(dir)).contains("damaged property dictionary (no ").contains("fqn").contains("classPrefix");
    }

    @Test
    void createdGraphSurvivesALateAsyncFlushOfItsFirstPage() throws Exception {
        Path dir = tmp.resolve("callgraph.arcadedb");
        // Not released by the test: createSettled's close has to wait the hold out.
        HeldDictionaryWrite hold = new HeldDictionaryWrite(tmp, 1_500);
        PageManager.setPageWriteFaultInjector(hold);
        try (CallGraph g = CallGraph.open(dir)) {
            assertThat(hold.held.get()).as("the create's dictionary write was held").isEqualTo(1);
            assertThat(hold.written.get()).as("open returned only after that write").isEqualTo(1);
            g.addMethod("p.A#run()", "p.A");
            g.addMethod("p.B#go()", "p.B");
            g.addCall("p.A#run()", "p.B#go()");
            g.addCodeType("p.A", "p.A");
            g.addCodeField("p.A#count", "p.A", "int", "private");
            g.addKgEdge("declares_field", "p.A", "CodeType", "p.A#count", "CodeField");
            g.flush();
            g.flushKnowledge();
        } finally {
            PageManager.setPageWriteFaultInjector(null);
            hold.release.countDown();
        }
        assertThat(CallGraph.dictionaryProblem(dir)).isNull();
        assertThat(CallGraph.readDictionary(dir)).containsOnlyKeys(CallGraph.DICTIONARY_ORDER);

        List<String> dump = new ArrayList<>();
        try (CallGraph g = CallGraph.openExisting(dir)) {
            assertThat(g.dictionaryDamage()).isNull();
            g.forEachVertex(v -> dump.add("V " + v));
            for (String type : CallGraph.EDGE_TYPES) g.forEachEdgeRecord(type, e -> dump.add("E " + type + " " + e));
        }
        assertThat(dump).containsExactlyInAnyOrder(
                "V " + new CallGraph.VertexRecord("Method", "p.A#run()", "p.A", null, null, null),
                "V " + new CallGraph.VertexRecord("Method", "p.B#go()", "p.B", null, null, null),
                "V " + new CallGraph.VertexRecord("CodeType", "p.A", "p.A", null, null, null),
                "V " + new CallGraph.VertexRecord("CodeField", "p.A#count", null, "p.A", "int", "private"),
                "E calls " + new CallGraph.EdgeRecord("Method", "p.A#run()", "Method", "p.B#go()"),
                "E declares_field " + new CallGraph.EdgeRecord("CodeType", "p.A", "CodeField", "p.A#count"));
    }

    @Test
    void createdModuleGraphSurvivesALateAsyncFlushOfItsFirstPage() throws Exception {
        Path dir = tmp.resolve("module-graph.arcadedb");
        HeldDictionaryWrite hold = new HeldDictionaryWrite(tmp, 1_500);
        PageManager.setPageWriteFaultInjector(hold);
        try (ModuleGraph g = ModuleGraph.open(dir)) {
            assertThat(hold.held.get()).isEqualTo(1);
            g.addOrUpdate("g:a", "g", "a", "1", "INDEXED", "a");
        } finally {
            PageManager.setPageWriteFaultInjector(null);
            hold.release.countDown();
        }
        assertThat(CallGraph.readDictionary(dir)).containsOnlyKeys(ModuleGraph.DICTIONARY_ORDER);
        try (ModuleGraph g = ModuleGraph.open(dir)) {
            assertThat(g.moduleCount()).isEqualTo(1);
        }
    }
}
