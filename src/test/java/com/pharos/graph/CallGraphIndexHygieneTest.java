package com.pharos.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ArcadeDB LSM indexes kept the entries of deleted records: TRUNCATE (full re-index) let them
 * resurface on the next insert and DELETE VERTEX + re-insert (incremental patch) added more each
 * time. A real pharos graph had 1,909,818 Method[classPrefix] entries for 3,968 methods; every
 * lookup skipped the stale ones with a WARN (96 % of a 246 MB daemon.log) and was ~30–45×
 * slower. The graph must keep its indexes close to the number of live records.
 */
class CallGraphIndexHygieneTest {

    @TempDir Path tmp;

    private static void addClass(CallGraph g, String cls, int methods) {
        for (int m = 0; m < methods; m++) g.addMethod(cls + "#m" + m + "()", cls);
    }

    /** Opens the raw database and counts entries of the given index. */
    private static long indexEntries(Path dbDir, String index) {
        try (Database db = new DatabaseFactory(dbDir.toString()).open()) {
            var it = ((TypeIndex) db.getSchema().getIndexByName(index)).iterator(true);
            long n = 0;
            while (it.hasNext()) { it.next(); n++; }
            return n;
        }
    }

    @Test
    void repeatedFullRebuilds_keepIndexesExact() {
        Path dbDir = tmp.resolve("cg");
        for (int round = 0; round < 3; round++) {
            try (CallGraph g = CallGraph.open(dbDir)) {
                g.clear();
                for (int c = 0; c < 50; c++) addClass(g, "C" + c, 10);
                g.flush();
            }
        }
        assertThat(indexEntries(dbDir, "Method[fqn]")).isEqualTo(500);
        assertThat(indexEntries(dbDir, "Method[classPrefix]")).isLessThanOrEqualTo(520);
    }

    @Test
    void repeatedIncrementalPatches_keepIndexesBounded() {
        Path dbDir = tmp.resolve("cg");
        try (CallGraph g = CallGraph.open(dbDir)) {
            for (int c = 0; c < 50; c++) addClass(g, "C" + c, 10);
            g.flush();
        }
        for (int cycle = 0; cycle < 300; cycle++) {           // 300 × 10 = 3,000 deletions > 2,000 threshold
            try (CallGraph g = CallGraph.open(dbDir)) {
                g.evictClasses(List.of("C7"));
                addClass(g, "C7", 10);
                g.flush();
                assertThat(g.callers("C7#m0()")).isNotNull();
            }
        }
        // Without rebuilds this grows superlinearly (2,437 classPrefix entries after only 5 cycles).
        assertThat(indexEntries(dbDir, "Method[fqn]")).isLessThan(2 * 500 + 2_000);
        assertThat(indexEntries(dbDir, "Method[classPrefix]")).isLessThan(10 * 500);
    }

    @Test
    void graphCorruptedByEarlierVersions_isRepairedOnceOnOpen() throws Exception {
        Path dbDir = tmp.resolve("cg");
        try (CallGraph g = CallGraph.open(dbDir)) {
            for (int c = 0; c < 50; c++) addClass(g, "C" + c, 10);
            g.flush();
        }
        // Reproduce what earlier versions did: TRUNCATE without rebuild, then re-insert.
        try (Database db = new DatabaseFactory(dbDir.toString()).open()) {
            db.begin();
            db.command("sql", "TRUNCATE TYPE Method UNSAFE");
            db.commit();
            db.begin();
            for (int c = 0; c < 50; c++) for (int m = 0; m < 10; m++)
                db.newVertex("Method").set("fqn", "C" + c + "#m" + m + "()").set("classPrefix", "C" + c).save();
            db.commit();
        }
        Files.deleteIfExists(dbDir.resolveSibling(dbDir.getFileName() + ".index-v2"));
        assertThat(indexEntries(dbDir, "Method[fqn]")).isGreaterThan(500);    // corrupted

        try (CallGraph g = CallGraph.open(dbDir)) {
            assertThat(g.methodCount()).isEqualTo(500);
        }

        assertThat(indexEntries(dbDir, "Method[fqn]")).isEqualTo(500);
        assertThat(dbDir.resolveSibling(dbDir.getFileName() + ".index-v2")).exists();
    }
}
