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
