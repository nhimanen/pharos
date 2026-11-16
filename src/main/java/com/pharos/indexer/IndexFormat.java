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
        MISSING("missing"),
        /** Registered under a name that is not a usable project name (e.g. "."): never touched. */
        INVALID("invalid");

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

    /** The stamp's file name inside a project index dir (or a reader's generation dir). */
    public static final String FILE_NAME = "index-format.json";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private record Cached(long mtime, Status status, Stamp stamp) {}

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
        write(projectIndexDir, new Stamp(IndexVersions.INDEX_FORMAT, PharosVersion.get(), Instant.now()));
    }

    /**
     * Writes {@code stamp} as given (atomic replace). A reader uses this to carry the writer's
     * stamp from a published manifest into the generation dir it materialises.
     */
    public static void write(Path projectIndexDir, Stamp stamp) {
        try {
            Files.createDirectories(projectIndexDir);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("format", stamp.format());
            m.put("pharosVersion", stamp.pharosVersion());
            m.put("writtenAt", stamp.writtenAt() == null ? null : stamp.writtenAt().toString());
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
     * The status an index of {@code format} has for this build (never {@code MISSING} or
     * {@code INVALID}). An unstamped index is format 0.
     */
    public static Status statusOf(int format) {
        return format == IndexVersions.INDEX_FORMAT ? Status.OK
                : format > IndexVersions.INDEX_FORMAT ? Status.NEWER
                : format >= MIN_READABLE_FORMAT ? Status.UPGRADE_AVAILABLE : Status.OUTDATED;
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
        if (isReaderGeneration(projectIndexDir)) {
            // A reader never re-indexes: the fix is on this reader (newer) or on the writer (outdated).
            msg = "Project '" + project + "' generation " + projectIndexDir.getFileName()
                    + " was published by pharos " + s.pharosVersion() + " (index format " + s.format()
                    + "); this reader (pharos " + me + ") reads index formats " + MIN_READABLE_FORMAT + "–"
                    + IndexVersions.INDEX_FORMAT + ". "
                    + (c.status() == Status.NEWER
                        ? "Upgrade pharos on this reader."
                        : "Re-publish it from an upgraded writer (on the writer: pharos index --full "
                          + project + "); this reader picks it up on its next sync.");
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

    private static final java.util.regex.Pattern READER_GEN_DIR = java.util.regex.Pattern.compile("gen-\\d{1,18}");

    /**
     * True for a reader node's materialised generation dir ({@code <cacheDir>/<shard>/gen-N} holding
     * the published {@code manifest.json}); writer index dirs never contain a manifest.
     */
    static boolean isReaderGeneration(Path dir) {
        return dir != null && dir.getFileName() != null
                && READER_GEN_DIR.matcher(dir.getFileName().toString()).matches()
                && Files.isRegularFile(dir.resolve("manifest.json"));
    }

    private static Cached cached(Path dir) {
        if (dir == null || !Files.isDirectory(dir)) {
            return new Cached(-1, Status.MISSING, new Stamp(0, "unknown", null));
        }
        long mtime = -1;
        try {
            Path f = dir.resolve(FILE_NAME);
            if (Files.exists(f)) mtime = Files.getLastModifiedTime(f).toMillis();
        } catch (IOException ignored) {
            // fall through with -1
        }
        long key = mtime;
        Path k = dir.toAbsolutePath().normalize();
        Cached c = CACHE.get(k);
        if (c != null && c.mtime() == key) return c;
        Stamp s = read(dir);
        Cached n = new Cached(key, statusOf(s.format()), s);
        CACHE.put(k, n);
        return n;
    }
}
