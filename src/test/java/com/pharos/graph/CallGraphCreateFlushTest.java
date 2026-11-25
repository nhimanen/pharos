package com.pharos.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PageManagerFlushThread;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A graph created by {@link CallGraph#open} reads back intact even when ArcadeDB's asynchronous
 * flush thread writes the new database's first page late.
 *
 * <p>ArcadeDB's {@code DatabaseFactory.create()} commits the property dictionary's first page
 * through the JVM-wide asynchronous flush queue; {@code setAsyncFlush(false)} only applies after
 * that. {@code initSchema} then rewrites the same page synchronously (it adds the property names).
 * When the queued, older copy reaches the disk after that synchronous write, the file keeps the
 * stale dictionary and every later open decodes the records' property names wrongly: vertices whose
 * {@code fqn} reads as null vanish from every scan, others come back with shuffled properties.
 * Seen as a restored writer graph "missing one CodeField vertex" (two CodeFields read with the same
 * shuffled fqn, deduplicated by the export).
 *
 * <p>The test parks the flush thread on a decoy batch so the late write is certain, instead of
 * depending on page-cache eviction or scheduling to open the window.
 */
class CallGraphCreateFlushTest {

    @TempDir Path tmp;

    @Test
    void createdGraphSurvivesALateAsyncFlushOfItsFirstPage() throws Exception {
        Database decoy = new DatabaseFactory(tmp.resolve("decoy.arcadedb").toString()).create();
        PageManagerFlushThread flusher = flushThread(); // started with the first database
        try {
            awaitDrained(flusher);
            List<MutablePage> decoyPages = new ArrayList<>(List.of(new MutablePage(new PageId(decoy, 0, 0), 4096)));
            PageManagerFlushThread.PagesToFlush parkOn = new PageManagerFlushThread.PagesToFlush(decoyPages);
            CountDownLatch parked = new CountDownLatch(1), release = new CountDownLatch(1);
            Thread holder = new Thread(() -> {
                synchronized (decoyPages) { // the flush thread blocks on this monitor after polling the batch
                    flusher.queue.add(parkOn);
                    while (flusher.queue.contains(parkOn)) Thread.onSpinWait();
                    parked.countDown();
                    try {
                        // a fixed open() waits for the queue: it gets the thread back after the timeout
                        release.await(2, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }, "flush-parker");
            holder.start();
            assertThat(parked.await(10, TimeUnit.SECONDS)).isTrue();

            Path dir = tmp.resolve("callgraph.arcadedb");
            CallGraph g = CallGraph.open(dir);
            g.addMethod("p.A#run()", "p.A");
            g.addMethod("p.B#go()", "p.B");
            g.addCall("p.A#run()", "p.B#go()");
            g.addCodeType("p.A", "p.A");
            g.addCodeField("p.A#count", "p.A", "int", "private");
            g.addKgEdge("declares_field", "p.A", "CodeType", "p.A#count", "CodeField");
            g.flush();
            g.flushKnowledge();
            release.countDown();
            holder.join();
            // FIFO: once this marker is taken, every page queued before it has been written
            List<MutablePage> markerPages = new ArrayList<>(List.of(new MutablePage(new PageId(decoy, 0, 0), 4096)));
            PageManagerFlushThread.PagesToFlush marker = new PageManagerFlushThread.PagesToFlush(markerPages);
            flusher.queue.add(marker);
            while (flusher.queue.contains(marker)) Thread.onSpinWait();
            synchronized (markerPages) { } // and the marker itself is done
            g.close();
        } finally {
            decoy.close();
        }

        List<String> dump = new ArrayList<>();
        try (CallGraph g = CallGraph.openExisting(tmp.resolve("callgraph.arcadedb"))) {
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

    private static PageManagerFlushThread flushThread() throws ReflectiveOperationException {
        Field f = PageManager.class.getDeclaredField("flushThread");
        f.setAccessible(true);
        return (PageManagerFlushThread) f.get(PageManager.INSTANCE);
    }

    private static void awaitDrained(PageManagerFlushThread flusher) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!flusher.queue.isEmpty() && System.nanoTime() < deadline) Thread.onSpinWait();
    }
}
