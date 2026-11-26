package com.pharos.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

/**
 * ArcadeDB-backed method call graph.
 *
 * Nodes (vertex type "Method"):
 *   fqn          — fully qualified method name, e.g. "com.example.MyClass#myMethod(String,int)"
 *   classPrefix  — qualified class name, e.g. "com.example.MyClass"
 *                  Empty string ("") for external stub vertices (callees from other projects).
 *
 * Edges (edge type "calls"):
 *   Directed: caller → callee.
 *
 * Indexes:
 *   UNIQUE   on Method.fqn         → O(1) lookup by FQN
 *   NON-UNIQUE on Method.classPrefix → O(log N) class eviction
 *
 * Usage:
 *   try (CallGraph g = CallGraph.open(dbDir)) {
 *       g.addMethod(fqn, classPrefix);
 *       g.addCall(callerFqn, calleeFqn);
 *       g.flush();  // commit pending batch
 *   }
 */
public class CallGraph implements GraphView {

    private static final Logger log = LoggerFactory.getLogger(CallGraph.class);

    /** Commit vertex/edge batches every N pending operations to bound transaction size. */
    private static final int BATCH_SIZE = 500;

    private final Database db;

    /** Pending method vertices not yet committed: [fqn, classPrefix]. */
    private final List<String[]> methodBatch = new ArrayList<>(BATCH_SIZE);

    /** Pending call edges not yet committed: [callerFqn, calleeFqn]. */
    private final List<String[]> callBatch = new ArrayList<>(BATCH_SIZE);

    /**
     * When true the database was just cleared and is guaranteed empty — skip the
     * "SELECT @rid ... WHERE fqn = ?" existence check in flush methods, turning
     * O(N × query_cost) into O(N × HashSet.contains) batch inserts.
     * Cleared and reset by {@link #clear()}.
     */
    private boolean freshStart = false;

    /**
     * FQNs inserted during the current session (populated by flush methods).
     * Replaces the per-entry {@code SELECT @rid} existence check when
     * {@link #freshStart} is true — handles both intra- and inter-batch
     * duplicates without any DB queries.
     * Cleared by {@link #clear()}.
     */
    private final Set<String> insertedMethodFqns   = new LinkedHashSet<>();
    private final Set<String> insertedCodeTypeFqns = new LinkedHashSet<>();
    private final Set<String> insertedCodeFieldFqns = new LinkedHashSet<>();

    /** Database directory; null for graphs not opened from disk. */
    private final Path dbDir;

    private CallGraph(Database db, Path dbDir) {
        this.db = db;
        this.dbDir = dbDir;
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /**
     * Open (or create) an ArcadeDB call graph at the given directory.
     * Schema is auto-created on first open.
     */
    public static CallGraph open(Path dbDir) {
        return open(dbDir, false);
    }

    /**
     * @param repairIndexes when true (indexing / linking — the writers), a graph written by an
     *                      earlier pharos version gets its indexes rebuilt once. Readers pass
     *                      false so a search or lookup never pays for, or blocks on, a rebuild.
     */
    public static CallGraph open(Path dbDir, boolean repairIndexes) {
        return open(dbDir, repairIndexes, true);
    }

    /**
     * Opens an existing graph for reading; never creates one. Readers use this so a lookup on a
     * project without a graph (or one being swapped) cannot leave an empty database behind.
     *
     * @throws GraphNotFoundException when no database exists at {@code dbDir}
     */
    public static CallGraph openExisting(Path dbDir) {
        return open(dbDir, false, false);
    }

    private static CallGraph open(Path dbDir, boolean repairIndexes, boolean allowCreate) {
        DatabaseFactory factory = new DatabaseFactory(dbDir.toString());
        boolean created = !factory.exists();
        if (created && !allowCreate) throw new GraphNotFoundException(dbDir);
        Database db = created ? createSettled(dbDir) : factory.open();
        // Checked before initSchema, which may add names to an older graph's dictionary.
        String damage = created ? null : dictionaryProblem(db, DICTIONARY_ORDER);
        if (damage != null) warnDamaged(dbDir, damage);
        // Database-wide since ArcadeDB 26 (#8352): every commit on this instance, from any thread,
        // writes its pages synchronously, so close() returns with them on disk.
        db.setAsyncFlush(false);
        initSchema(db);
        CallGraph graph = new CallGraph(db, dbDir);
        graph.dictionaryDamage = damage;
        if (created) graph.markFreshDatabase();
        else if (repairIndexes) graph.repairIndexesOnce();
        return graph;
    }

    // -------------------------------------------------------------------------
    // Damaged property dictionary (graphs written before 1.10.0, see createSettled)
    // -------------------------------------------------------------------------

    /**
     * The order in which pharos has always added this graph's property names to ArcadeDB's
     * dictionary: {@link #initSchema} creates them in this order (Method's two since the ArcadeDB
     * backend, CodeField's three since the knowledge graph), and no record sets any other name.
     * A dictionary that maps one of them to another id was written stale, and its records decode
     * with the wrong names.
     */
    static final List<String> DICTIONARY_ORDER = List.of("fqn", "classPrefix", "ownerFqn", "fieldType", "accessMod");

    /** Dirs already warned about in this JVM: a reader opens the graph on every query. */
    private static final Set<Path> WARNED_DAMAGED = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** Null, or what is wrong with this graph's property dictionary (checked when it was opened). */
    private String dictionaryDamage;

    /**
     * Null, or why this graph's records decode with the wrong property names: its dictionary does
     * not map {@link #DICTIONARY_ORDER}{@code [i]} to {@code i}. Such a graph must not be published
     * or exported; {@code pharos index --full} rebuilds it.
     */
    public String dictionaryDamage() {
        return dictionaryDamage;
    }

    /** Null, or the names of {@code canonical} that {@code db}'s dictionary maps to another id. */
    static String dictionaryProblem(Database db, List<String> canonical) {
        return dictionaryProblem(((com.arcadedb.database.DatabaseInternal) db).getSchema().getEmbedded()
                .getDictionary().getDictionaryMap(), canonical);
    }

    static String dictionaryProblem(Map<String, Integer> ids, List<String> canonical) {
        List<String> wrong = new ArrayList<>();
        for (int i = 0; i < canonical.size(); i++) {
            Integer id = ids.get(canonical.get(i));
            if (id != null && id != i) wrong.add(canonical.get(i) + " has id " + id + ", not " + i);
        }
        return wrong.isEmpty() ? null : "damaged property dictionary (" + String.join(", ", wrong) + ")";
    }

    /**
     * {@link #dictionaryDamage()} of the graph database at {@code dbDir}, read from its dictionary
     * file without opening the database (an open, even read-only, rewrites ArcadeDB's
     * {@code cached-count.json}, and fails while another instance holds the database). Null when
     * there is no database or its dictionary cannot be read, so a caller never reports damage it
     * did not see.
     */
    public static String dictionaryProblem(Path dbDir) {
        try {
            Map<String, Integer> ids = readDictionary(dbDir);
            if (ids == null) return null;
            // The race's raw state on disk is the header-only page: the names come back, out of
            // order, only on the next ArcadeDB open. Every open adds each property the schema
            // declares, so a declared name missing from the file is that state, never a healthy one.
            List<String> missing = new ArrayList<>();
            for (String name : declaredProperties(dbDir)) if (!ids.containsKey(name)) missing.add(name);
            if (!missing.isEmpty()) {
                return "damaged property dictionary (no " + String.join(", ", missing)
                        + ", which the schema declares)";
            }
            return dictionaryProblem(ids, DICTIONARY_ORDER);
        } catch (java.io.IOException | RuntimeException e) {
            log.debug("Could not read the dictionary of {}: {}", dbDir, e.getMessage());
            return null;
        }
    }

    /** The property names {@code schema.json} declares on any type, in file order. */
    static Set<String> declaredProperties(Path dbDir) throws java.io.IOException {
        Path schema = dbDir.resolve("schema.json");
        Set<String> names = new LinkedHashSet<>();
        if (!Files.isRegularFile(schema)) return names;
        com.fasterxml.jackson.databind.JsonNode types =
                new com.fasterxml.jackson.databind.ObjectMapper().readTree(schema.toFile()).path("types");
        types.forEach(t -> t.path("properties").fieldNames().forEachRemaining(names::add));
        return names;
    }

    /** ArcadeDB's dictionary file name: {@code dictionary.<fileId>.<pageSize>.v<version>.dict}. */
    private static final java.util.regex.Pattern DICTIONARY_FILE =
            java.util.regex.Pattern.compile("dictionary\\.\\d+\\.(\\d+)\\.v\\d+\\.dict");

    /** Page header (int version, int used size) plus the dictionary's 4-byte legacy counter. */
    private static final int DICTIONARY_PAGE_START = 12;

    /** {@code dbDir}'s dictionary file, or null when it has none. */
    private static Path dictionaryFile(Path dbDir) throws java.io.IOException {
        if (!Files.isDirectory(dbDir)) return null;
        try (Stream<Path> s = Files.list(dbDir)) {
            return s.filter(p -> DICTIONARY_FILE.matcher(p.getFileName().toString()).matches()).findFirst().orElse(null);
        }
    }

    /**
     * Name to id from ArcadeDB's dictionary file, as {@code Dictionary.reload} reads it. The file is
     * a run of pages of the size its name carries; each page holds an int version and an int used
     * size (header included, big-endian), a 4-byte legacy counter, then names, each an unsigned
     * LEB128 byte length plus UTF-8. Ids are the names' ordinals across the pages in file order.
     * ArcadeDB 25.1.1 wrote exactly one such page (327,680 bytes); 26.x starts new databases at
     * 65,536-byte pages and rolls over to further pages (ArcadeDB #5560), and reads a 25.1.1 file
     * as its one-page case, so this one reader covers both. Null when {@code dbDir} has no
     * dictionary file or the file is empty.
     *
     * @throws java.io.IOException on a page whose header does not fit the file: callers treat
     *                             an unreadable dictionary as not damaged
     */
    static Map<String, Integer> readDictionary(Path dbDir) throws java.io.IOException {
        Path file = dictionaryFile(dbDir);
        if (file == null) return null;
        var name = DICTIONARY_FILE.matcher(file.getFileName().toString());
        if (!name.matches()) return null;
        int pageSize = Integer.parseInt(name.group(1));
        if (pageSize < DICTIONARY_PAGE_START) throw new java.io.IOException("dictionary page size " + pageSize);
        try (var ch = java.nio.channels.FileChannel.open(file, java.nio.file.StandardOpenOption.READ)) {
            long size = ch.size();
            if (size < 8) return null;
            // As ArcadeDB counts them: whole pages, and page 0 even when the file is shorter.
            long pages = Math.max(1, size / pageSize);
            Map<String, Integer> ids = new LinkedHashMap<>();
            int id = 0;
            for (long p = 0; p < pages; p++) {
                long offset = p * pageSize;
                java.nio.ByteBuffer head = java.nio.ByteBuffer.allocate(8);
                while (head.hasRemaining() && ch.read(head, offset + head.position()) > 0) { }
                if (head.hasRemaining()) throw new java.io.IOException("dictionary page " + p + " is cut short");
                int used = head.getInt(4);
                if (used < DICTIONARY_PAGE_START || used > pageSize || offset + used > size) {
                    throw new java.io.IOException("dictionary page " + p + " used size " + used);
                }
                java.nio.ByteBuffer page = java.nio.ByteBuffer.allocate(used);
                while (page.hasRemaining() && ch.read(page, offset + page.position()) > 0) { }
                int pos = DICTIONARY_PAGE_START;
                for (; pos < used; id++) {
                    int len = 0, shift = 0, b;
                    do {
                        b = page.get(pos++) & 0xFF;
                        len |= (b & 0x7F) << shift;
                        shift += 7;
                    } while ((b & 0x80) != 0 && shift < 32 && pos < used);
                    if ((b & 0x80) != 0 || len < 0 || pos + len > used) {
                        throw new java.io.IOException("corrupt dictionary entry in page " + p + " at " + pos);
                    }
                    ids.putIfAbsent(new String(page.array(), pos, len, java.nio.charset.StandardCharsets.UTF_8), id);
                    pos += len;
                }
            }
            return ids;
        }
    }

    /** What repairs the graph at {@code dbDir}: a project's full re-index, or the cross-project link. */
    static String repairAdvice(Path dbDir) {
        Path name = dbDir.getFileName();
        Path parent = dbDir.toAbsolutePath().normalize().getParent();
        if (name != null && GRAPH_ENTRY.matcher(name.toString()).matches() && parent != null
                && parent.getFileName() != null) {
            return "run: pharos index --full " + parent.getFileName();
        }
        return "re-run: pharos link (it recreates the cross-project graph)";
    }

    private static void warnDamaged(Path dbDir, String damage) {
        if (!WARNED_DAMAGED.add(dbDir.toAbsolutePath().normalize())) return;
        Path parent = dbDir.toAbsolutePath().normalize().getParent();
        String what = GRAPH_ENTRY.matcher(String.valueOf(dbDir.getFileName())).matches() && parent != null
                ? "Call graph of project '" + parent.getFileName() + "'" : "Call graph " + dbDir;
        log.warn("{} has a {}, written by pharos before 1.10.0: callers, callees, usages and paths "
                + "from it are wrong and it will not be published; {}", what, damage, repairAdvice(dbDir));
    }

    /** The database directory; null for graphs not opened from disk. */
    Path dbDir() {
        return dbDir;
    }

    /** This graph's dictionary, name to id; for tests. */
    Map<String, Integer> dictionaryMap() {
        return ((com.arcadedb.database.DatabaseInternal) db).getSchema().getEmbedded().getDictionary().getDictionaryMap();
    }

    /**
     * Creates the ArcadeDB database at {@code dbDir} and returns it opened again, with nothing of
     * its creation still in ArcadeDB's asynchronous flush queue. A workaround for ArcadeDB
     * <a href="https://github.com/ArcadeData/arcadedb/issues/13441">#13441</a> (open in 26.11.1).
     *
     * <p>{@code DatabaseFactory.create()} commits the property dictionary's header page through
     * the asynchronous flush queue: the commit runs before {@code setAsyncFlush(false)} can apply.
     * {@link #initSchema} then rewrites the same page synchronously (it adds the property names),
     * and nothing in ArcadeDB orders the two writes. When the queued, older copy reached the disk
     * last, the file kept the empty dictionary and every later open decoded the records' property
     * names wrongly: vertices whose fqn read as null vanished from every scan, others came back with
     * shuffled properties, and nothing failed.
     *
     * <p>Closing waits for the database's queued and in-flight pages on ArcadeDB 26 (on 25.1.1 it
     * could miss a batch the flush thread had polled, which is why pharos up to 1.11 polled the
     * dictionary file and used the internal {@code waitAllPagesOfDatabaseAreFlushed}). The file
     * check below is a cheap guard on that promise: a dictionary file without its header page
     * after close means the write was lost, and the reopen would queue the header again.
     */
    static Database createSettled(Path dbDir) {
        new DatabaseFactory(dbDir.toString()).create().close();
        if (!hasDictionaryPage(dbDir)) {
            throw new IllegalStateException("ArcadeDB closed the new graph " + dbDir
                    + " without writing its dictionary header page (ArcadeDB #13441)");
        }
        return new DatabaseFactory(dbDir.toString()).open();
    }

    /** True when {@code dbDir}'s dictionary file holds at least its page header. */
    private static boolean hasDictionaryPage(Path dbDir) {
        try {
            Path file = dictionaryFile(dbDir);
            return file != null && Files.size(file) >= 12;
        } catch (java.io.IOException e) {
            return false;
        }
    }

    // -------------------------------------------------------------------------
    // Per-project graph layout
    // -------------------------------------------------------------------------
    //
    // A full re-index builds a new generation dir (callgraph-<gen>.arcadedb) and then publishes
    // it by atomically replacing the pointer file callgraph.current. The live graph is never
    // renamed: ArcadeDB writes cached-count.json and deletes its WAL and lock files by the path
    // captured at open, so a reader closing after a rename would write into, or delete from,
    // whatever graph then sits at that path. Old generations are deleted instead, which is safe
    // on POSIX for readers that still have them open (they keep their file handles, and
    // ArcadeDB skips cached-count.json when its dir is gone). Indexes without a pointer (format 0,
    // and anything indexed before the pointer existed) use the legacy callgraph.arcadedb.

    /** Legacy per-project graph dir, used while there is no {@link #POINTER_FILE}. */
    public static final String LEGACY_DIR = "callgraph.arcadedb";
    /** Names the live generation dir of a project's graph. */
    public static final String POINTER_FILE = "callgraph.current";
    /** At most 18 digits, so the generation number always fits a long. */
    private static final java.util.regex.Pattern GENERATION =
            java.util.regex.Pattern.compile("callgraph-(\\d{1,18})\\.arcadedb");
    /** A graph dir name (legacy or generation) followed by an optional sidecar suffix. */
    private static final java.util.regex.Pattern GRAPH_ENTRY =
            java.util.regex.Pattern.compile("(callgraph(?:-\\d+)?\\.arcadedb)(\\..*)?");

    /** The live graph dir of the project whose index dir is {@code projectIndexDir}. */
    public static Path projectGraphDir(Path projectIndexDir) {
        String target = readPointer(projectIndexDir);
        return projectIndexDir.resolve(target != null ? target : LEGACY_DIR);
    }

    /**
     * The live graph dir for a writer (incremental patch). Unlike {@link #projectGraphDir}, a
     * pointer that exists but is malformed, unreadable, or names a generation dir that is gone is
     * an error: falling back to the legacy dir would create and patch an empty legacy graph.
     *
     * @throws java.io.IOException when the pointer is present but unusable
     */
    public static Path writerGraphDir(Path projectIndexDir) throws java.io.IOException {
        String problem = pointerProblem(projectIndexDir);
        if (problem != null) throw new java.io.IOException(problem);
        return projectGraphDir(projectIndexDir);
    }

    /**
     * Why the project's {@link #POINTER_FILE} cannot be used, or null when it is fine or absent
     * (absent = legacy layout).
     */
    public static String pointerProblem(Path projectIndexDir) {
        Path f = projectIndexDir.resolve(POINTER_FILE);
        if (!Files.exists(f, java.nio.file.LinkOption.NOFOLLOW_LINKS)) return null;
        String name;
        try {
            name = Files.readString(f).trim();
        } catch (java.io.IOException e) {
            return "Call graph pointer " + f + " is unreadable: " + e.getMessage();
        }
        if (!GENERATION.matcher(name).matches()) {
            return "Call graph pointer " + f + " is malformed: '" + name + "'";
        }
        if (!Files.isDirectory(projectIndexDir.resolve(name))) {
            return "Call graph pointer " + f + " names " + name + ", which does not exist";
        }
        return null;
    }

    private static String readPointer(Path projectIndexDir) {
        Path f = projectIndexDir.resolve(POINTER_FILE);
        if (!Files.isRegularFile(f)) return null;
        try {
            String name = Files.readString(f).trim();
            if (GENERATION.matcher(name).matches()) return name;
            log.warn("Ignoring malformed {}: '{}'", f, name);
        } catch (java.io.IOException e) {
            log.warn("Could not read {}: {}", f, e.getMessage());
        }
        return null;
    }

    /**
     * A fresh, not yet existing generation dir for a full rebuild: newer than every generation
     * present, so a name is never reused while an old reader may still hold it.
     */
    public static Path newGenerationDir(Path projectIndexDir) throws java.io.IOException {
        long gen = System.currentTimeMillis();
        try (var s = Files.list(projectIndexDir)) {
            for (Path p : s.toList()) {
                var m = GRAPH_ENTRY.matcher(p.getFileName().toString());
                if (!m.matches()) continue;
                var g = GENERATION.matcher(m.group(1));
                if (g.matches()) gen = Math.max(gen, Long.parseLong(g.group(1)) + 1);
            }
        }
        String current = readPointer(projectIndexDir);
        if (current != null) {
            var g = GENERATION.matcher(current);
            if (g.matches()) gen = Math.max(gen, Long.parseLong(g.group(1)) + 1);
        }
        return projectIndexDir.resolve("callgraph-" + gen + ".arcadedb");
    }

    /** Makes {@code generationDir} the project's live graph (atomic pointer replace). */
    public static void publish(Path projectIndexDir, Path generationDir) throws java.io.IOException {
        String name = generationDir.getFileName().toString();
        if (!GENERATION.matcher(name).matches()) throw new IllegalArgumentException(name);
        Path tmp = Files.createTempFile(projectIndexDir, POINTER_FILE + ".", ".tmp");
        try {
            // Durable before it becomes visible: after a crash the pointer names a complete
            // generation, never an empty or truncated file.
            try (var ch = java.nio.channels.FileChannel.open(tmp,
                    java.nio.file.StandardOpenOption.WRITE,
                    java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)) {
                ch.write(java.nio.ByteBuffer.wrap((name + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                ch.force(true);
            }
            Files.move(tmp, projectIndexDir.resolve(POINTER_FILE),
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            fsyncDirectory(projectIndexDir);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    /** Best effort: persists a rename in {@code dir}. Not every platform can open a directory. */
    private static void fsyncDirectory(Path dir) {
        try (var ch = java.nio.channels.FileChannel.open(dir, java.nio.file.StandardOpenOption.READ)) {
            ch.force(true);
        } catch (java.io.IOException | UnsupportedOperationException e) {
            log.debug("Could not fsync {}: {}", dir, e.getMessage());
        }
    }

    /**
     * Deletes every graph dir (and sidecar) of the project except the live one, plus stray
     * pointer temp files. Only call while holding the project's Lucene write lock: then no other
     * full run can have a generation in progress.
     */
    public static void removeUnusedGraphs(Path projectIndexDir) {
        if (!Files.isDirectory(projectIndexDir)) return;
        String live = projectGraphDir(projectIndexDir).getFileName().toString();
        try (var s = Files.list(projectIndexDir)) {
            for (Path p : s.toList()) {
                String n = p.getFileName().toString();
                var m = GRAPH_ENTRY.matcher(n);
                boolean unusedGraph = m.matches() && !m.group(1).equals(live);
                boolean strayTmp = n.startsWith(POINTER_FILE + ".") && n.endsWith(".tmp");
                if (unusedGraph || strayTmp) deleteRecursively(p);
            }
        } catch (java.io.IOException e) {
            log.warn("Could not remove old call graphs in {}: {}", projectIndexDir, e.getMessage());
        }
    }

    static void deleteRecursively(Path p) throws java.io.IOException {
        if (!Files.exists(p, java.nio.file.LinkOption.NOFOLLOW_LINKS)) return;
        try (var w = Files.walk(p)) {
            for (Path q : w.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(q);
        }
    }

    // -------------------------------------------------------------------------
    // Index hygiene
    // -------------------------------------------------------------------------
    //
    // ArcadeDB's LSM-tree indexes keep the entries of deleted records: after TRUNCATE TYPE …
    // UNSAFE they reappear on the next insert, and DELETE VERTEX + re-insert of the same keys
    // adds more stale entries each time (no compaction happens at pharos' sizes). Every full
    // index (clear) and incremental patch (evictClasses + re-insert) grew them; a real pharos
    // graph had 1,909,818 Method[classPrefix] entries for 3,968 methods. Lookups skip each stale
    // entry with a WARN "Record #… not found" — 96% of a 246 MB daemon.log — and a class
    // lookup took 150 ms instead of 3–5 ms. REBUILD INDEX restores exact indexes.

    /** Marker written next to the database once its indexes have been rebuilt by this version. */
    private static final String INDEX_REPAIRED_SUFFIX = ".index-v2";
    /** Sidecar holding the number of vertices deleted since the last index rebuild. */
    private static final String CHURN_SUFFIX = ".index-churn";
    /** Rebuild once deletions since the last rebuild exceed this share of the graph … */
    private static final double CHURN_REBUILD_RATIO = 0.10;
    /** … but not for fewer than this many deletions. */
    private static final long CHURN_REBUILD_MIN = 2_000;

    /** Rebuilds every index of the graph so it holds exactly the live records. */
    public void rebuildIndexes() {
        long t0 = System.nanoTime();
        db.command("sql", "REBUILD INDEX *");
        writeChurn(0);
        log.debug("Rebuilt call-graph indexes in {} ms", (System.nanoTime() - t0) / 1_000_000);
    }

    /** Rebuilds the non-unique indexes of {@code type} (Method[classPrefix], CodeField[ownerFqn]). */
    private void rebuildNonUniqueIndexes(String type) {
        if (!db.getSchema().existsType(type)) return;
        // Collect names first: REBUILD drops and recreates the index in the type's collection.
        List<String> names = new ArrayList<>();
        for (var ix : db.getSchema().getType(type).getAllIndexes(false)) {
            if (!ix.isUnique()) names.add(ix.getName());
        }
        for (String name : names) {
            try {
                db.command("sql", "REBUILD INDEX `" + name + "`");
            } catch (Exception e) {
                log.warn("Could not rebuild index {} of {}: {}", name, dbDir, e.getMessage());
            }
        }
    }

    /** One-time repair of graphs written by earlier versions (which grew stale entries). */
    private void repairIndexesOnce() {
        if (dbDir == null) return;
        Path marker = sibling(INDEX_REPAIRED_SUFFIX);
        if (Files.exists(marker)) return;
        long t0 = System.nanoTime();
        try {
            rebuildIndexes();
        } catch (Exception e) {
            log.warn("Could not rebuild call-graph indexes of {}: {}", dbDir, e.getMessage());
            return;
        }
        log.info("Repaired call-graph indexes of {} ({} ms)", dbDir.getFileName(),
                (System.nanoTime() - t0) / 1_000_000);
        writeMarker(marker);
    }

    /** A database created by this version has clean indexes: mark it and reset any old churn. */
    private void markFreshDatabase() {
        if (dbDir == null) return;
        writeChurn(0);
        writeMarker(sibling(INDEX_REPAIRED_SUFFIX));
    }

    private void writeMarker(Path marker) {
        try {
            Files.writeString(marker, "rebuilt\n");
        } catch (Exception e) {
            log.warn("Could not write {} (the index repair will run again next time): {}",
                    marker, e.getMessage());
        }
    }

    private Path sibling(String suffix) {
        return dbDir.resolveSibling(dbDir.getFileName() + suffix);
    }

    private long readChurn() {
        if (dbDir == null) return 0;
        try {
            Path f = sibling(CHURN_SUFFIX);
            return Files.exists(f) ? Long.parseLong(Files.readString(f).trim()) : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    private void writeChurn(long value) {
        if (dbDir == null) return;
        try {
            Files.writeString(sibling(CHURN_SUFFIX), Long.toString(value));
        } catch (Exception e) {
            log.debug("Could not record index churn for {}: {}", dbDir, e.getMessage());
        }
    }

    /** Records {@code deleted} vertex deletions and rebuilds the indexes once churn is large. */
    private void recordDeletions(long deleted) {
        if (deleted <= 0) return;
        long churn = readChurn() + deleted;
        long live = db.countType("Method", false);
        if (churn > Math.max(CHURN_REBUILD_MIN, (long) (live * CHURN_REBUILD_RATIO))) {
            try {
                rebuildIndexes();
            } catch (Exception e) {
                // the patch itself succeeded; retry the rebuild on a later run
                log.warn("Could not rebuild call-graph indexes of {}: {}", dbDir, e.getMessage());
                writeChurn(churn);
            }
        } else {
            writeChurn(churn);
        }
    }

    private static void initSchema(Database db) {
        if (!db.getSchema().existsType("Method")) {
            var methodType = db.getSchema().createVertexType("Method");
            methodType.createProperty("fqn",        Type.STRING);
            methodType.createProperty("classPrefix", Type.STRING);
            db.getSchema().createEdgeType("calls");
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true,  "Method", "fqn");
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Method", "classPrefix");
        }
        // Knowledge-graph vertex/edge types (additive — safe on existing databases)
        if (!db.getSchema().existsType("CodeType")) {
            var t = db.getSchema().createVertexType("CodeType");
            t.createProperty("fqn",         Type.STRING);
            t.createProperty("classPrefix", Type.STRING);
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "CodeType", "fqn");
        }
        if (!db.getSchema().existsType("CodeField")) {
            var f = db.getSchema().createVertexType("CodeField");
            f.createProperty("fqn",         Type.STRING);
            f.createProperty("ownerFqn",    Type.STRING);
            f.createProperty("fieldType",   Type.STRING);
            f.createProperty("accessMod",   Type.STRING);
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true,  "CodeField", "fqn");
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "CodeField", "ownerFqn");
        }
        for (String edge : new String[]{"inherits", "implements_iface", "declares_field",
                "reads_field", "writes_field", "returns_type", "takes_type", "annotated_by"}) {
            if (!db.getSchema().existsType(edge)) db.getSchema().createEdgeType(edge);
        }
    }

    // -------------------------------------------------------------------------
    // Mutations (buffered)
    // -------------------------------------------------------------------------

    /**
     * Add a project-owned method vertex.
     * @param classPrefix the qualified class name; must be non-empty for project-owned methods.
     */
    public void addMethod(String fqn, String classPrefix) {
        if (!pendingEviction.isEmpty()) readdedSinceEviction.add(fqn);
        methodBatch.add(new String[]{fqn, classPrefix != null ? classPrefix : ""});
        if (methodBatch.size() >= BATCH_SIZE) flushMethods();
    }

    /**
     * Add a directed CALLS edge.  If either endpoint does not exist yet it is added
     * as an external stub (empty classPrefix).
     */
    public void addCall(String callerFqn, String calleeFqn) {
        callBatch.add(new String[]{callerFqn, calleeFqn});
        if (callBatch.size() >= BATCH_SIZE) flushCalls();
    }

    /** Commit all pending method and call batches to the database. */
    public void flush() {
        flushMethods();
        flushCalls();
    }

    // -------------------------------------------------------------------------
    // Knowledge-graph mutations
    // -------------------------------------------------------------------------

    /** Batch buffers for knowledge-graph vertices and edges. */
    private final List<String[]>  codeTypeBatch  = new ArrayList<>(BATCH_SIZE); // [fqn, classPrefix]
    private final List<String[]>  codeFieldBatch = new ArrayList<>(BATCH_SIZE); // [fqn, ownerFqn, type, access]
    private final List<String[]>  kgEdgeBatch    = new ArrayList<>(BATCH_SIZE); // [edgeType, fromFqn, fromVType, toFqn, toVType]

    public void addCodeType(String fqn, String classPrefix) {
        if (!pendingEviction.isEmpty()) readdedSinceEviction.add(fqn);
        codeTypeBatch.add(new String[]{fqn, classPrefix != null ? classPrefix : ""});
        if (codeTypeBatch.size() >= BATCH_SIZE) flushCodeTypes();
    }

    public void addCodeField(String fieldFqn, String ownerFqn, String fieldType, String accessMod) {
        if (!pendingEviction.isEmpty()) readdedSinceEviction.add(fieldFqn);
        codeFieldBatch.add(new String[]{fieldFqn, ownerFqn, fieldType, accessMod});
        if (codeFieldBatch.size() >= BATCH_SIZE) flushCodeFields();
    }

    /** Add a typed knowledge-graph edge. {@code fromType}/{@code toType}: "Method","CodeType","CodeField". */
    public void addKgEdge(String edgeType, String fromFqn, String fromVType, String toFqn, String toVType) {
        kgEdgeBatch.add(new String[]{edgeType, fromFqn, fromVType, toFqn, toVType});
        if (kgEdgeBatch.size() >= BATCH_SIZE) flushKgEdges();
    }

    /** Commit all knowledge-graph pending batches. */
    public void flushKnowledge() {
        flushCodeTypes();
        flushCodeFields();
        flushKgEdges();
    }

    /**
     * Delete all vertices and edges (call graph + knowledge graph).
     * Uses TRUNCATE TYPE (O(1)) rather than DELETE VERTEX (O(M) full scan).
     */
    public void clear() {
        methodBatch.clear();
        callBatch.clear();
        codeTypeBatch.clear();
        codeFieldBatch.clear();
        kgEdgeBatch.clear();
        freshStart = true; // DB is about to be empty — use session sets instead of DB queries
        insertedMethodFqns.clear();
        insertedCodeTypeFqns.clear();
        insertedCodeFieldFqns.clear();
        db.begin();
        for (String edge : new String[]{"calls", "inherits", "implements_iface", "declares_field",
                "reads_field", "writes_field", "returns_type", "takes_type", "annotated_by"}) {
            if (db.getSchema().existsType(edge))
                db.command("sql", "TRUNCATE TYPE " + edge + " UNSAFE");
        }
        db.command("sql", "TRUNCATE TYPE Method UNSAFE");
        if (db.getSchema().existsType("CodeType"))  db.command("sql", "TRUNCATE TYPE CodeType UNSAFE");
        if (db.getSchema().existsType("CodeField")) db.command("sql", "TRUNCATE TYPE CodeField UNSAFE");
        db.commit();
        // TRUNCATE leaves the old index entries behind (they resurface on the next insert);
        // rebuilding the now-empty indexes is instant and drops them.
        rebuildIndexes();
    }

    // -------------------------------------------------------------------------
    // Eviction
    // -------------------------------------------------------------------------

    /** Vertices of evicted classes, by type → fqns; deleted by {@link #completeEviction} unless re-added. */
    private final Map<String, Set<String>> pendingEviction = new LinkedHashMap<>();
    /** Fqns (re-)added since {@link #evictClasses} — these vertices are kept. */
    private final Set<String> readdedSinceEviction = new HashSet<>();

    /**
     * Starts replacing the given classes (an incremental patch of their files).
     *
     * <p>Their vertices are kept and only their <em>outgoing</em> edges (calls they make, types
     * they extend, fields they read, …) are removed; the caller then re-adds the classes'
     * methods, types, fields and edges and calls {@link #completeEviction()}, which deletes the
     * vertices that were not re-added (members removed from the source).
     *
     * <p>Deleting the vertices up front — as before — also deleted every edge from
     * <em>unchanged</em> files into them, which the patch never re-created: editing B.java lost
     * all of B's callers in other files until the next full index. It also re-inserted the same
     * index keys on every edit, which grows ArcadeDB's LSM indexes quadratically.
     */
    public void evictClasses(Collection<String> classPrefixes) {
        if (classPrefixes == null || classPrefixes.isEmpty()) return;
        flush();
        flushKnowledge();
        db.begin();
        for (String cls : classPrefixes) {
            if (cls == null || cls.isEmpty()) continue;
            detachOutgoing("Method", "classPrefix", cls);
            if (db.getSchema().existsType("CodeType")) detachOutgoing("CodeType", "classPrefix", cls);
            if (db.getSchema().existsType("CodeField")) detachOutgoing("CodeField", "ownerFqn", cls);
        }
        db.commit();
        log.debug("Detached {} class(es) for patching", classPrefixes.size());
    }

    /** Removes the outgoing edges of every {@code type} vertex with {@code property = value}. */
    private void detachOutgoing(String type, String property, String value) {
        Set<String> pending = pendingEviction.computeIfAbsent(type, k -> new LinkedHashSet<>());
        try (ResultSet rs = db.query("sql", "SELECT FROM " + type + " WHERE " + property + " = ?", value)) {
            while (rs.hasNext()) {
                Vertex v = rs.next().getVertex().orElse(null);
                if (v == null) continue;
                Object fqn = v.get("fqn");
                if (fqn != null) pending.add(fqn.toString());
                List<com.arcadedb.graph.Edge> out = new ArrayList<>();
                v.getEdges(Vertex.DIRECTION.OUT).forEach(out::add);
                for (com.arcadedb.graph.Edge e : out) e.delete();
            }
        }
    }

    /**
     * Marks vertices as still present in the source during a patch, so {@link #completeEviction}
     * keeps them even when the re-parse added nothing for them — e.g. an edited interface's own
     * CodeType, whose implementers in unchanged files point at it.
     */
    public void keep(Collection<String> fqns) {
        if (!pendingEviction.isEmpty()) readdedSinceEviction.addAll(fqns);
    }

    /**
     * Finishes an incremental patch started by {@link #evictClasses}: deletes the evicted
     * classes' vertices that were not re-added (and with them any edges into them).
     */
    public void completeEviction() {
        if (pendingEviction.isEmpty()) return;
        flush();
        flushKnowledge();
        long deleted = 0;
        Set<String> typesWithDeletions = new LinkedHashSet<>();
        db.begin();
        for (Map.Entry<String, Set<String>> e : pendingEviction.entrySet()) {
            for (String fqn : e.getValue()) {
                if (readdedSinceEviction.contains(fqn)) continue;
                long n = deletedCount(db.command("sql", "DELETE VERTEX FROM " + e.getKey() + " WHERE fqn = ?", fqn));
                if (n > 0) typesWithDeletions.add(e.getKey());
                deleted += n;
            }
        }
        db.commit();
        pendingEviction.clear();
        readdedSinceEviction.clear();
        // Non-unique indexes accumulate stale entries per key quadratically (one class whose
        // methods keep changing grew Method[classPrefix] to 540k entries in ~100 edits), so a
        // count threshold can't bound them: rebuild them whenever members were really removed.
        for (String type : typesWithDeletions) rebuildNonUniqueIndexes(type);
        recordDeletions(deleted);
        log.debug("Patch complete: removed {} vertex(es) no longer in the source", deleted);
    }

    private static long deletedCount(com.arcadedb.query.sql.executor.ResultSet rs) {
        try (rs) {
            if (!rs.hasNext()) return 0;
            Object n = rs.next().getProperty("count");
            return n instanceof Number num ? num.longValue() : 0;
        }
    }

    // -------------------------------------------------------------------------
    // Queries
    // -------------------------------------------------------------------------

    /** Returns all method FQNs that directly call {@code fqn} (in-edges). */
    public Set<String> callers(String fqn) {
        Vertex v = lookupVertex(fqn);
        if (v == null) return Set.of();
        Set<String> result = new LinkedHashSet<>();
        for (Vertex caller : v.getVertices(Vertex.DIRECTION.IN, "calls")) {
            String f = caller.getString("fqn");
            if (f != null) result.add(f);
        }
        return result;
    }

    /** Returns all method FQNs called by {@code fqn} (out-edges). */
    public Set<String> callees(String fqn) {
        Vertex v = lookupVertex(fqn);
        if (v == null) return Set.of();
        Set<String> result = new LinkedHashSet<>();
        for (Vertex callee : v.getVertices(Vertex.DIRECTION.OUT, "calls")) {
            String f = callee.getString("fqn");
            if (f != null) result.add(f);
        }
        return result;
    }

    /**
     * Returns the subset of {@code candidateFqns} that appear as callees of any
     * edge in this graph. One O(M+E) graph scan instead of O(N) per-vertex lookups.
     * Used by ModuleBoundaryAnalyzer to find entry points without N×M queries.
     */
    public Set<String> calledTargets(Set<String> candidateFqns) {
        if (candidateFqns.isEmpty()) return Set.of();
        Set<String> found = new LinkedHashSet<>();
        ResultSet rs = db.query("sql", "SELECT FROM Method");
        while (rs.hasNext() && found.size() < candidateFqns.size()) {
            Vertex v = rs.next().getVertex().orElse(null);
            if (v == null) continue;
            for (Vertex callee : v.getVertices(Vertex.DIRECTION.OUT, "calls")) {
                String fqn = callee.getString("fqn");
                if (fqn != null && candidateFqns.contains(fqn)) found.add(fqn);
            }
        }
        return found;
    }

    /**
     * BFS shortest call path from {@code fromFqn} to {@code toFqn}.
     * Returns an empty list if no path exists.
     */
    public List<String> shortestPath(String fromFqn, String toFqn) {
        if (lookupVertex(fromFqn) == null || lookupVertex(toFqn) == null) return List.of();
        Map<String, String> prev = new LinkedHashMap<>(); // fqn → caller fqn (null for start)
        Deque<String> queue = new ArrayDeque<>();
        prev.put(fromFqn, null);
        queue.add(fromFqn);
        outer:
        while (!queue.isEmpty()) {
            String current = queue.poll();
            if (current.equals(toFqn)) break;
            Vertex v = lookupVertex(current);
            if (v == null) continue;
            for (Vertex callee : v.getVertices(Vertex.DIRECTION.OUT, "calls")) {
                String calleeFqn = callee.getString("fqn");
                if (calleeFqn != null && !prev.containsKey(calleeFqn)) {
                    prev.put(calleeFqn, current);
                    if (calleeFqn.equals(toFqn)) break outer;
                    queue.add(calleeFqn);
                }
            }
        }
        if (!prev.containsKey(toFqn)) return List.of();
        List<String> path = new ArrayList<>();
        for (String cur = toFqn; cur != null; cur = prev.get(cur)) {
            path.add(0, cur);
        }
        return path;
    }

    /**
     * Returns all project-owned method FQNs (classPrefix != "").
     * External stubs (added implicitly for unresolved callees) are excluded.
     */
    public Stream<String> allFqns() {
        // Use range predicate (classPrefix > '') rather than not-equal (<> '')
        // so ArcadeDB can satisfy the query via the LSM_TREE index on classPrefix
        // instead of doing a full O(M) type scan.  Stubs have classPrefix='' which
        // sorts below any real package string, so they are correctly excluded.
        ResultSet rs = db.query("sql", "SELECT fqn FROM Method WHERE classPrefix > ''");
        List<String> fqns = new ArrayList<>();
        while (rs.hasNext()) {
            Object v = rs.next().getProperty("fqn");
            if (v != null) fqns.add(v.toString());
        }
        return fqns.stream();
    }

    public long methodCount() {
        return db.countType("Method", false);
    }

    public long callCount() {
        return db.countType("calls", false);
    }

    // -------------------------------------------------------------------------
    // Knowledge-graph queries
    // -------------------------------------------------------------------------

    /** Direct subclasses of {@code classFqn} (one hop via inherits or implements_iface, in-edges). */
    public Set<String> directSubclasses(String classFqn) {
        return kgNeighbors(classFqn, "CodeType", Vertex.DIRECTION.IN,
                new String[]{"inherits", "implements_iface"});
    }

    /** Direct superclasses / implemented interfaces of {@code classFqn} (out-edges). */
    public Set<String> directSuperTypes(String classFqn) {
        return kgNeighbors(classFqn, "CodeType", Vertex.DIRECTION.OUT,
                new String[]{"inherits", "implements_iface"});
    }

    /** BFS over inherits + implements_iface to collect all transitive subclasses. */
    public List<String> allSubclasses(String classFqn) {
        return kgBfs(classFqn, "CodeType", Vertex.DIRECTION.IN,
                new String[]{"inherits", "implements_iface"});
    }

    /** All methods that read a field (reads_field in-edges on CodeField vertex). */
    public Set<String> fieldReaders(String fieldFqn) {
        return kgNeighbors(fieldFqn, "CodeField", Vertex.DIRECTION.IN, new String[]{"reads_field"});
    }

    /** All methods that write a field. */
    public Set<String> fieldWriters(String fieldFqn) {
        return kgNeighbors(fieldFqn, "CodeField", Vertex.DIRECTION.IN, new String[]{"writes_field"});
    }

    /** All fields declared by a class (FQNs only). */
    public Set<String> declaredFields(String classFqn) {
        return kgNeighbors(classFqn, "CodeType", Vertex.DIRECTION.OUT, new String[]{"declares_field"});
    }

    /** Rich field info for all fields declared by a class. */
    public List<FieldInfo> getFieldsOf(String classFqn) {
        if (!db.getSchema().existsType("CodeField")) return List.of();
        ResultSet rs = db.query("sql",
                "SELECT fqn, fieldType, accessMod FROM CodeField WHERE ownerFqn = ?", classFqn);
        List<FieldInfo> result = new ArrayList<>();
        while (rs.hasNext()) {
            var rec = rs.next();
            String fqn   = rec.getProperty("fqn");
            String type  = rec.getProperty("fieldType");
            String access = rec.getProperty("accessMod");
            if (fqn != null) result.add(new FieldInfo(fqn, type, access));
        }
        return result;
    }

    /** Field declaration with type and access modifier. */
    public record FieldInfo(String fqn, String fieldType, String accessMod) {
        public String fieldName() {
            int hash = fqn.indexOf('#');
            return hash >= 0 ? fqn.substring(hash + 1) : fqn;
        }
    }

    /** All methods/classes annotated with the given annotation FQN or simple name. */
    public Set<String> annotatedWith(String annotationFqn) {
        return kgNeighbors(annotationFqn, "CodeType", Vertex.DIRECTION.IN, new String[]{"annotated_by"});
    }

    /** All methods that return the given type. */
    public Set<String> methodsReturning(String typeFqn) {
        return kgNeighbors(typeFqn, "CodeType", Vertex.DIRECTION.IN, new String[]{"returns_type"});
    }

    /** All methods that take the given type as a parameter. */
    public Set<String> methodsTaking(String typeFqn) {
        return kgNeighbors(typeFqn, "CodeType", Vertex.DIRECTION.IN, new String[]{"takes_type"});
    }

    private Set<String> kgNeighbors(String fqn, String vType, Vertex.DIRECTION dir, String[] edgeTypes) {
        Vertex v = lookupKgVertex(fqn, vType);
        if (v == null) return Set.of();
        Set<String> result = new LinkedHashSet<>();
        for (Vertex neighbor : v.getVertices(dir, edgeTypes)) {
            String f = neighbor.getString("fqn");
            if (f != null) result.add(f);
        }
        return result;
    }

    private List<String> kgBfs(String startFqn, String vType, Vertex.DIRECTION dir, String[] edgeTypes) {
        Set<String> visited = new LinkedHashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        queue.add(startFqn);
        visited.add(startFqn);
        while (!queue.isEmpty()) {
            String current = queue.poll();
            Vertex v = lookupKgVertex(current, vType);
            if (v == null) continue;
            for (Vertex n : v.getVertices(dir, edgeTypes)) {
                String f = n.getString("fqn");
                if (f != null && visited.add(f)) queue.add(f);
            }
        }
        visited.remove(startFqn);
        return new ArrayList<>(visited);
    }

    // -------------------------------------------------------------------------
    // Export (compact graph file)
    // -------------------------------------------------------------------------

    /** Edge types of the graph, in the order they are written to the compact file. */
    public static final List<String> EDGE_TYPES = List.of("calls", "inherits", "implements_iface",
            "declares_field", "reads_field", "writes_field", "returns_type", "takes_type", "annotated_by");

    /** One vertex of the graph. {@code kind} is Method, CodeType or CodeField; unused fields are null. */
    public record VertexRecord(String kind, String fqn, String classPrefix, String ownerFqn,
                               String fieldType, String accessMod) {}

    /** One edge with the vertex type of each endpoint (an fqn is only unique within its type). */
    public record EdgeRecord(String fromKind, String fromFqn, String toKind, String toFqn) {}

    /** Visits every vertex of every type. Pending batches must be flushed by the caller. */
    public void forEachVertex(java.util.function.Consumer<VertexRecord> visitor) {
        for (String type : new String[]{"Method", "CodeType", "CodeField"}) {
            if (!db.getSchema().existsType(type)) continue;
            var it = db.iterateType(type, false);
            while (it.hasNext()) {
                Vertex v = it.next().asVertex();
                String fqn = v.getString("fqn");
                if (fqn == null) continue;
                visitor.accept(new VertexRecord(type, fqn, v.getString("classPrefix"),
                        v.getString("ownerFqn"), v.getString("fieldType"), v.getString("accessMod")));
            }
        }
    }

    /** Visits every edge of {@code edgeType} as (from fqn, to fqn). */
    public void forEachEdge(String edgeType, java.util.function.BiConsumer<String, String> fromTo) {
        forEachEdgeRecord(edgeType, e -> fromTo.accept(e.fromFqn(), e.toFqn()));
    }

    /** Visits every edge of {@code edgeType}, including the vertex type of both endpoints. */
    public void forEachEdgeRecord(String edgeType, java.util.function.Consumer<EdgeRecord> visitor) {
        if (!db.getSchema().existsType(edgeType)) return;
        for (String type : new String[]{"Method", "CodeType", "CodeField"}) {
            if (!db.getSchema().existsType(type)) continue;
            var it = db.iterateType(type, false);
            while (it.hasNext()) {
                Vertex from = it.next().asVertex();
                String fromFqn = from.getString("fqn");
                if (fromFqn == null) continue;
                for (Vertex to : from.getVertices(Vertex.DIRECTION.OUT, edgeType)) {
                    String toFqn = to.getString("fqn");
                    if (toFqn != null) visitor.accept(new EdgeRecord(type, fromFqn, to.getTypeName(), toFqn));
                }
            }
        }
    }

    /**
     * Bulk-loads {@code vertices} and, per {@link #EDGE_TYPES} index {@code t}, the edges
     * {@code from[t][i] → to[t][i]} (vertex indexes into {@code vertices}) into this graph, which
     * must be empty. Only the non-null properties of each record are set, exactly as the builders
     * set them. Used by {@link CompactGraphImport}.
     */
    void load(List<VertexRecord> vertices, int[][] from, int[][] to) {
        for (String type : new String[]{"Method", "CodeType", "CodeField"}) {
            if (db.countType(type, false) != 0) throw new IllegalStateException("load needs an empty graph: " + dbDir);
        }
        com.arcadedb.database.RID[] rids = new com.arcadedb.database.RID[vertices.size()];
        for (int start = 0; start < vertices.size(); start += BATCH_SIZE) {
            db.begin();
            for (int i = start; i < Math.min(vertices.size(), start + BATCH_SIZE); i++) {
                VertexRecord r = vertices.get(i);
                MutableVertex v = db.newVertex(r.kind());
                v.set("fqn", r.fqn());
                if (r.classPrefix() != null) v.set("classPrefix", r.classPrefix());
                if (r.ownerFqn() != null) v.set("ownerFqn", r.ownerFqn());
                if (r.fieldType() != null) v.set("fieldType", r.fieldType());
                if (r.accessMod() != null) v.set("accessMod", r.accessMod());
                v.save();
                rids[i] = v.getIdentity();
            }
            db.commit();
        }
        for (int t = 0; t < EDGE_TYPES.size(); t++) {
            String type = EDGE_TYPES.get(t);
            int[] f = from[t], tt = to[t];
            if (f == null) continue;
            // ArcadeDB lists a vertex's out-edges newest first: inserting in reverse file order
            // gives back the exported order.
            for (int end = f.length; end > 0; end -= BATCH_SIZE) {
                db.begin();
                for (int i = end - 1; i >= Math.max(0, end - BATCH_SIZE); i--) {
                    rids[f[i]].asVertex().modify().newEdge(type, rids[tt[i]]);
                }
                db.commit();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void flushMethods() {
        if (methodBatch.isEmpty()) return;
        db.begin();
        for (String[] m : methodBatch) {
            if (freshStart) {
                // Fast path: use in-memory set — O(1), handles intra- and inter-batch duplicates.
                if (!insertedMethodFqns.add(m[0])) continue;
            } else {
                ResultSet existing = db.query("sql", "SELECT FROM Method WHERE fqn = ?", m[0]);
                if (existing.hasNext()) {
                    // A call from another file of the same patch may have created this method as
                    // an external stub (empty classPrefix): give it its class, or it stays out of
                    // allFqns() and eviction, and its outgoing edges duplicate on later patches.
                    Vertex v = existing.next().getVertex().orElse(null);
                    Object prefix = v != null ? v.get("classPrefix") : null;
                    if (v != null && !m[1].isEmpty() && (prefix == null || prefix.toString().isEmpty())) {
                        v.modify().set("classPrefix", m[1]).save();
                    }
                    continue;
                }
            }
            MutableVertex v = db.newVertex("Method");
            v.set("fqn", m[0]);
            v.set("classPrefix", m[1]);
            v.save();
        }
        db.commit();
        methodBatch.clear();
    }

    private void flushCalls() {
        if (callBatch.isEmpty()) return;

        // Single transaction: look up pre-committed vertices via index (they're visible),
        // create stub vertices directly (no index needed), then wire edges via object refs.
        // This avoids SQL subqueries in CREATE EDGE, which can't see uncommitted vertices.
        db.begin();
        Map<String, MutableVertex> vMap = new HashMap<>();
        for (String[] c : callBatch) {
            for (String fqn : new String[]{c[0], c[1]}) {
                if (!vMap.containsKey(fqn)) {
                    vMap.put(fqn, ensureVertexInTx(fqn));
                }
            }
            MutableVertex from = vMap.get(c[0]);
            MutableVertex to   = vMap.get(c[1]);
            if (from != null && to != null) {
                try {
                    from.newEdge("calls", to);
                } catch (Exception e) {
                    log.debug("Could not create call edge {} → {}: {}", c[0], c[1], e.getMessage());
                }
            }
        }
        db.commit();
        callBatch.clear();
    }

    /**
     * Within an open transaction: return a MutableVertex for {@code fqn}.
     * For pre-committed vertices (from flushMethods), the index finds them.
     * For new stubs, creates and saves them in the current transaction.
     */
    private MutableVertex ensureVertexInTx(String fqn) {
        ResultSet rs = db.query("sql", "SELECT FROM Method WHERE fqn = ?", fqn);
        if (rs.hasNext()) {
            Vertex v = rs.next().getVertex().orElse(null);
            if (v != null) return v.modify();
        }
        MutableVertex v = db.newVertex("Method");
        v.set("fqn", fqn);
        v.set("classPrefix", "");
        v.save();
        return v;
    }

    private Vertex lookupVertex(String fqn) {
        ResultSet rs = db.query("sql", "SELECT FROM Method WHERE fqn = ?", fqn);
        if (!rs.hasNext()) return null;
        return rs.next().getVertex().orElse(null);
    }

    private Vertex lookupKgVertex(String fqn, String vType) {
        ResultSet rs = db.query("sql", "SELECT FROM " + vType + " WHERE fqn = ?", fqn);
        if (!rs.hasNext()) return null;
        return rs.next().getVertex().orElse(null);
    }

    private void flushCodeTypes() {
        if (codeTypeBatch.isEmpty()) return;
        db.begin();
        for (String[] m : codeTypeBatch) {
            if (freshStart) {
                if (!insertedCodeTypeFqns.add(m[0])) continue;
            } else {
                if (db.query("sql", "SELECT @rid FROM CodeType WHERE fqn = ?", m[0]).hasNext()) continue;
            }
            MutableVertex v = db.newVertex("CodeType");
            v.set("fqn", m[0]);
            v.set("classPrefix", m[1]);
            v.save();
        }
        db.commit();
        codeTypeBatch.clear();
    }

    private void flushCodeFields() {
        if (codeFieldBatch.isEmpty()) return;
        db.begin();
        for (String[] f : codeFieldBatch) {
            if (freshStart) {
                if (!insertedCodeFieldFqns.add(f[0])) continue;
            } else {
                if (db.query("sql", "SELECT @rid FROM CodeField WHERE fqn = ?", f[0]).hasNext()) continue;
            }
            MutableVertex v = db.newVertex("CodeField");
            v.set("fqn",       f[0]);
            v.set("ownerFqn",  f[1]);
            v.set("fieldType", f[2]);
            v.set("accessMod", f[3]);
            v.save();
        }
        db.commit();
        codeFieldBatch.clear();
    }

    private void flushKgEdges() {
        if (kgEdgeBatch.isEmpty()) return;
        db.begin();
        for (String[] e : kgEdgeBatch) {
            String edgeType = e[0], fromFqn = e[1], fromVType = e[2], toFqn = e[3], toVType = e[4];
            try {
                MutableVertex from = ensureKgVertex(fromFqn, fromVType);
                MutableVertex to   = ensureKgVertex(toFqn,   toVType);
                if (from != null && to != null) from.newEdge(edgeType, to);
            } catch (Exception ex) {
                log.debug("KG edge {} → {} ({}): {}", fromFqn, toFqn, edgeType, ex.getMessage());
            }
        }
        db.commit();
        kgEdgeBatch.clear();
    }

    private MutableVertex ensureKgVertex(String fqn, String vType) {
        String sql = "SELECT FROM " + vType + " WHERE fqn = ?";
        ResultSet rs = db.query("sql", sql, fqn);
        if (rs.hasNext()) {
            Vertex v = rs.next().getVertex().orElse(null);
            if (v != null) return v.modify();
        }
        // Fall back to Method vertex for method FQNs in relational edges
        if ("Method".equals(vType)) {
            rs = db.query("sql", "SELECT FROM Method WHERE fqn = ?", fqn);
            if (rs.hasNext()) {
                Vertex v = rs.next().getVertex().orElse(null);
                if (v != null) return v.modify();
            }
        }
        MutableVertex v = db.newVertex(vType);
        v.set("fqn", fqn);
        v.set("classPrefix", "");
        v.save();
        return v;
    }

    @Override
    public void close() {
        if (!db.isOpen()) return;
        try {
            flush();
            flushKnowledge();
        } finally {
            // A patch that wasn't completed explicitly (e.g. it failed half-way) is abandoned,
            // not completed: stale vertices are safer than deleting members — and edges from
            // unchanged files — that the failed patch never got to re-add.
            pendingEviction.clear();
            readdedSinceEviction.clear();
            db.close();
        }
    }
}
