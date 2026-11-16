package com.pharos.indexer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.PharosVersion;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-project index format stamp ({@code <project index dir>/index-format.json}) and the
 * compatibility check against {@link IndexVersions#INDEX_FORMAT}.
 *
 * <p>N-1 policy: this build serves indexes of its own format and of the previous one
 * ({@link #MIN_READABLE_FORMAT}), so a live system keeps answering while projects are re-indexed.
 * Older or newer formats are refused.
 */
public final class IndexFormat {

    public enum Status {
        /** Written in this build's format. */
        OK("ok"),
        /** One format behind (N-1): still served; the next {@code pharos index} rebuilds it. */
        UPGRADE_AVAILABLE("upgrade-available"),
        /** Older than N-1: refused until re-indexed. */
        OUTDATED("outdated"),
        /** Written by a newer pharos: refused. */
        NEWER("newer"),
        /** No index dir. */
        MISSING("missing");

        private final String wireName;

        Status(String wireName) { this.wireName = wireName; }

        /** The status as shown in JSON and the CLI ({@code upgrade-available}, …). */
        public String wireName() { return wireName; }
    }

    /** The oldest format this build still reads and serves (N-1). */
    public static final int MIN_READABLE_FORMAT = IndexVersions.INDEX_FORMAT - 1;

    /** Whether a project with this status may be searched and its graph read. */
    public static boolean servable(Status s) {
        return s == Status.OK || s == Status.UPGRADE_AVAILABLE;
    }

    public record Stamp(int format, String pharosVersion, Instant writtenAt) {}

    static final String FILE_NAME = "index-format.json";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private record Cached(long mtime, boolean overwritten, Status status, Stamp stamp) {}

    /**
     * True when a pharos older than the pointer layout wrote to this index after this format's
     * stamp: such a build only knows the legacy {@code callgraph.arcadedb}, which a stamped full
     * run removes, so a legacy dir next to a stamp and a pointer means the stamp no longer
     * describes the index (an old build re-indexed it, or a reader of it created an empty graph).
     */
    public static boolean overwrittenByOlderPharos(Path projectIndexDir) {
        return Files.isRegularFile(projectIndexDir.resolve(FILE_NAME))
                && Files.exists(projectIndexDir.resolve(com.pharos.graph.CallGraph.POINTER_FILE))
                && Files.isDirectory(projectIndexDir.resolve(com.pharos.graph.CallGraph.LEGACY_DIR));
    }

    private static final Map<Path, Cached> CACHE = new ConcurrentHashMap<>();

    private IndexFormat() {}

    /**
     * The index dir recorded in a registry entry, or null when it has none (a damaged or
     * hand-edited registry). Every method here treats a null dir as {@link Status#MISSING}.
     */
    public static Path dir(String indexPath) {
        return indexPath == null || indexPath.isBlank() ? null : Path.of(indexPath);
    }

    public static Stamp read(Path projectIndexDir) {
        if (projectIndexDir == null) return new Stamp(0, "unknown", null);
        Path f = projectIndexDir.resolve(FILE_NAME);
        if (!Files.isRegularFile(f)) return new Stamp(0, "unknown", null);
        try {
            JsonNode n = MAPPER.readTree(f.toFile());
            int format = n.path("format").asInt(0);
            String v = n.path("pharosVersion").asText("unknown");
            String w = n.path("writtenAt").asText(null);
            return new Stamp(format, v, w == null ? null : Instant.parse(w));
        } catch (Exception e) {
            // Unreadable stamp: treat as unstamped so the user is told to re-index.
            return new Stamp(0, "unknown", null);
        }
    }

    public static void write(Path projectIndexDir) {
        try {
            Files.createDirectories(projectIndexDir);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("format", IndexVersions.INDEX_FORMAT);
            m.put("pharosVersion", PharosVersion.get());
            m.put("writtenAt", Instant.now().toString());
            Path tmp = Files.createTempFile(projectIndexDir, FILE_NAME, ".tmp");
            try {
                MAPPER.writeValue(tmp.toFile(), m);
                Files.move(tmp, projectIndexDir.resolve(FILE_NAME),
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } finally {
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Undoes the layout an older pharos left behind (see {@link #overwrittenByOlderPharos}):
     * drops the stale stamp, then the stale pointer, so the index reads as the unstamped
     * legacy layout it really is (the old build's Lucene index and its callgraph.arcadedb).
     * The stamp goes first so the index is never reported current in between.
     */
    public static void forgetStaleLayout(Path projectIndexDir) throws IOException {
        Files.deleteIfExists(projectIndexDir.resolve(FILE_NAME));
        Files.deleteIfExists(projectIndexDir.resolve(com.pharos.graph.CallGraph.POINTER_FILE));
    }

    public static Status status(Path projectIndexDir) {
        return cached(projectIndexDir).status();
    }

    public static void check(String project, Path projectIndexDir) {
        Cached c = cached(projectIndexDir);
        if (servable(c.status()) || c.status() == Status.MISSING) return;
        String me = PharosVersion.get();
        Stamp s = c.stamp();
        String msg;
        if (c.overwritten()) {
            msg = "Project '" + project + "' was re-indexed by a pharos older than " + s.pharosVersion()
                    + " after this pharos stamped it (index format " + s.format()
                    + "), so its index cannot be trusted. Run: pharos index --full " + project
                    + "   (or upgrade all: ./setup.sh --upgrade-indexes)";
        } else if (c.status() == Status.OUTDATED) {
            msg = "Project '" + project + "' was indexed by pharos " + s.pharosVersion()
                    + " (index format " + s.format() + "); this pharos (" + me
                    + ") reads index formats " + MIN_READABLE_FORMAT + "–" + IndexVersions.INDEX_FORMAT
                    + ". Run: pharos index --full " + project
                    + "   (or upgrade all: ./setup.sh --upgrade-indexes)";
        } else {
            msg = "Project '" + project + "' was indexed by a newer pharos " + s.pharosVersion()
                    + " (index format " + s.format() + "); this pharos (" + me
                    + ") reads index formats " + MIN_READABLE_FORMAT + "–" + IndexVersions.INDEX_FORMAT
                    + ". Re-index with this version (pharos index --full " + project
                    + ") or reinstall the newer pharos.";
        }
        throw new IncompatibleIndexException(project, c.status(), s, msg);
    }

    private static Cached cached(Path dir) {
        if (dir == null || !Files.isDirectory(dir)) {
            return new Cached(-1, false, Status.MISSING, new Stamp(0, "unknown", null));
        }
        long mtime = -1;
        try {
            Path f = dir.resolve(FILE_NAME);
            if (Files.exists(f)) mtime = Files.getLastModifiedTime(f).toMillis();
        } catch (IOException ignored) {
            // fall through with -1
        }
        long key = mtime;
        boolean overwritten = overwrittenByOlderPharos(dir);
        Path k = dir.toAbsolutePath().normalize();
        Cached c = CACHE.get(k);
        if (c != null && c.mtime() == key && c.overwritten() == overwritten) return c;
        Stamp s = read(dir);
        Status st = overwritten ? Status.OUTDATED
                : s.format() == IndexVersions.INDEX_FORMAT ? Status.OK
                : s.format() > IndexVersions.INDEX_FORMAT ? Status.NEWER
                : s.format() >= MIN_READABLE_FORMAT ? Status.UPGRADE_AVAILABLE : Status.OUTDATED;
        Cached n = new Cached(key, overwritten, st, s);
        CACHE.put(k, n);
        return n;
    }
}
