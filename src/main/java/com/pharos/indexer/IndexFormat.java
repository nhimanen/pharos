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
 */
public final class IndexFormat {

    public enum Status { OK, OUTDATED, NEWER, MISSING }

    public record Stamp(int format, String pharosVersion, Instant writtenAt) {}

    static final String FILE_NAME = "index-format.json";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private record Cached(long mtime, Status status, Stamp stamp) {}

    private static final Map<Path, Cached> CACHE = new ConcurrentHashMap<>();

    private IndexFormat() {}

    public static Stamp read(Path projectIndexDir) {
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

    public static Status status(Path projectIndexDir) {
        return cached(projectIndexDir).status();
    }

    public static void check(String project, Path projectIndexDir) {
        Cached c = cached(projectIndexDir);
        if (c.status() != Status.OUTDATED && c.status() != Status.NEWER) return;
        String me = PharosVersion.get();
        Stamp s = c.stamp();
        String msg;
        if (c.status() == Status.OUTDATED) {
            msg = "Project '" + project + "' was indexed by pharos " + s.pharosVersion()
                    + " (index format " + s.format() + "); this pharos (" + me
                    + ") needs index format " + IndexVersions.INDEX_FORMAT
                    + ". Run: pharos index --full " + project
                    + "   (or upgrade all: ./setup.sh --upgrade-indexes)";
        } else {
            msg = "Project '" + project + "' was indexed by a newer pharos " + s.pharosVersion()
                    + " (index format " + s.format() + "); this pharos (" + me
                    + ") only supports index format " + IndexVersions.INDEX_FORMAT
                    + ". Re-index with this version (pharos index --full " + project
                    + ") or reinstall the newer pharos.";
        }
        throw new IncompatibleIndexException(project, c.status(), s, msg);
    }

    private static Cached cached(Path dir) {
        if (!Files.isDirectory(dir)) {
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
        Status st = s.format() == IndexVersions.INDEX_FORMAT ? Status.OK
                : s.format() < IndexVersions.INDEX_FORMAT ? Status.OUTDATED : Status.NEWER;
        Cached n = new Cached(key, st, s);
        CACHE.put(k, n);
        return n;
    }
}
