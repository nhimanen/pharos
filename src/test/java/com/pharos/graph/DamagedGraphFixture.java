package com.pharos.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/** Graph databases whose ArcadeDB property dictionary is out of pharos' fixed order. */
public final class DamagedGraphFixture {

    private DamagedGraphFixture() {}

    /**
     * Unpacks {@code graph/damaged-dictionary.zip} into {@code dbDir} (which must not exist): the
     * sample-app call graph as the pre-1.10.0 dictionary race left it, kept from a reproduction
     * (dictionary {@code accessMod=0, ownerFqn=1, fqn=2, fieldType=3, classPrefix=4}; a scan returns
     * two CodeFields with shuffled properties and nothing else).
     */
    public static void unpackCallGraph(Path dbDir) throws IOException {
        Files.createDirectories(dbDir);
        try (InputStream raw = DamagedGraphFixture.class.getClassLoader()
                .getResourceAsStream("graph/damaged-dictionary.zip");
             ZipInputStream zip = new ZipInputStream(raw)) {
            ZipEntry e;
            while ((e = zip.getNextEntry()) != null) {
                if (e.isDirectory()) continue;
                String name = e.getName().substring(e.getName().indexOf('/') + 1);
                Files.copy(zip, dbDir.resolve(name));
            }
        }
    }

    /**
     * Cuts the dictionary of the (closed) database at {@code dbDir} back to its header-only page:
     * what the race leaves on disk before an ArcadeDB open refills the names in another order.
     */
    public static void cutDictionaryToHeader(Path dbDir) throws IOException {
        Path dict;
        try (var s = Files.list(dbDir)) {
            dict = s.filter(p -> p.getFileName().toString().matches("dictionary\\..*\\.dict")).findFirst().orElseThrow();
        }
        try (var ch = java.nio.channels.FileChannel.open(dict, java.nio.file.StandardOpenOption.READ,
                java.nio.file.StandardOpenOption.WRITE)) {
            java.nio.ByteBuffer head = java.nio.ByteBuffer.allocate(8);
            ch.read(head, 0);
            int used = head.getInt(4);
            // used size = page header (8) + the 4-byte legacy counter; the names are zeroed
            ch.write(java.nio.ByteBuffer.allocate(4).putInt(0, 12), 4);
            ch.write(java.nio.ByteBuffer.allocate(used - 12), 12);
        }
    }

    /**
     * Creates an empty ArcadeDB database at {@code dbDir} whose dictionary holds {@code names} in
     * this order, as a stale dictionary re-filled in another order would.
     */
    public static void createWithDictionary(Path dbDir, List<String> names) {
        Database db = new DatabaseFactory(dbDir.toString()).create();
        try {
            CallGraph.awaitDictionaryOnDisk(db);
            db.setAsyncFlush(false);
            var dictionary = ((DatabaseInternal) db).getSchema().getEmbedded().getDictionary();
            for (String n : names) dictionary.getIdByName(n, true);
        } finally {
            db.close();
        }
    }
}
