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
    void repeatedIncrementalPatches_keepIndexesBounded() throws Exception {
        Path dbDir = tmp.resolve("cg");
        try (CallGraph g = CallGraph.open(dbDir)) {
            for (int c = 0; c < 50; c++) addClass(g, "C" + c, 10);
            g.flush();
        }
        Path churn = dbDir.resolveSibling(dbDir.getFileName() + ".index-churn");
        boolean rebuilt = false;
        for (int cycle = 0; cycle < 300; cycle++) {
            try (CallGraph g = CallGraph.open(dbDir, true)) {
                g.evictClasses(List.of("C7"));
                // every edit renames C7's methods, so the old ones are really deleted
                for (int m = 0; m < 10; m++) g.addMethod("C7#v" + cycle + "_" + m + "()", "C7");
                g.flush();
                g.completeEviction();
            }
            long c = Long.parseLong(Files.readString(churn).trim());
            if (cycle > 0 && c < 10) rebuilt = true;          // counter reset → rebuild ran
        }
        assertThat(rebuilt).as("churn past the threshold triggered a rebuild").isTrue();
        // 3,000 real deletions; without rebuilds stale entries grow superlinearly
        assertThat(indexEntries(dbDir, "Method[fqn]")).isLessThan(500 + 2_000 + 500);
        assertThat(indexEntries(dbDir, "Method[classPrefix]")).isLessThan(10 * 500);
    }

    @Test
    void graphCorruptedByEarlierVersions_isRepairedOnceOnOpen() throws Exception {
        Path dbDir = tmp.resolve("cg");
        try (CallGraph g = CallGraph.open(dbDir)) {
            for (int c = 0; c < 50; c++) addClass(g, "C" + c, 10);
            g.flush();
        }
        // What earlier versions did: TRUNCATE without rebuild, then re-insert. ArcadeDB 25.1.1 kept
        // the truncated records' index entries; 26.x rebuilds the index on TRUNCATE, so the graph
        // is only corrupted when an earlier ArcadeDB wrote it. The once-only repair of a graph
        // without the marker is what is checked either way.
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
        long before = indexEntries(dbDir, "Method[fqn]");
        assertThat(before).isGreaterThanOrEqualTo(500);    // > 500 when written by ArcadeDB 25.1.1

        try (CallGraph reader = CallGraph.open(dbDir)) {
            assertThat(reader.methodCount()).isEqualTo(500);
        }
        assertThat(indexEntries(dbDir, "Method[fqn]")).as("readers never rebuild").isEqualTo(before);
        assertThat(dbDir.resolveSibling(dbDir.getFileName() + ".index-v2")).as("nor mark").doesNotExist();

        try (CallGraph writer = CallGraph.open(dbDir, true)) {
            assertThat(writer.methodCount()).isEqualTo(500);
        }

        assertThat(indexEntries(dbDir, "Method[fqn]")).isEqualTo(500);
        assertThat(dbDir.resolveSibling(dbDir.getFileName() + ".index-v2")).exists();
    }
}
