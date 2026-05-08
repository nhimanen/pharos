package com.pharos.indexer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks file modification state for incremental indexing.
 * Stored as {projectIndexDir}/file-state.json.
 *
 * Uses both mtime (fast check) and SHA-256 (accurate check) to detect changes.
 */
public class FileStateTracker {

    private static final Logger log = LoggerFactory.getLogger(FileStateTracker.class);

    private final Path stateFile;
    private final ObjectMapper mapper = new ObjectMapper();
    /** Thread-safe map for concurrent access during parallel indexing */
    private final ConcurrentHashMap<String, FileState> state;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FileState {
        public long lastModifiedMs;
        public String sha256;
        /** Qualified class names declared in this file — used to patch the call graph incrementally. */
        public List<String> classNames;

        public FileState() {}
        public FileState(long lastModifiedMs, String sha256) {
            this.lastModifiedMs = lastModifiedMs;
            this.sha256 = sha256;
        }
        public FileState(long lastModifiedMs, String sha256, List<String> classNames) {
            this.lastModifiedMs = lastModifiedMs;
            this.sha256 = sha256;
            this.classNames = classNames;
        }
    }

    public FileStateTracker(Path projectIndexDir) {
        this.stateFile = projectIndexDir.resolve("file-state.json");
        this.state = load();
    }

    /** Returns true if the file has changed since last tracking (or is new). */
    public boolean isDirty(Path file) {
        String key = file.toAbsolutePath().toString();
        FileState tracked = state.get(key);
        if (tracked == null) return true;

        try {
            long mtime = Files.getLastModifiedTime(file).toMillis();
            if (mtime != tracked.lastModifiedMs) {
                // mtime changed — do full hash check
                String hash = sha256(file);
                return !hash.equals(tracked.sha256);
            }
            return false;
        } catch (IOException e) {
            return true;
        }
    }

    /** Records the current state of a file. */
    public void track(Path file) {
        track(file, null);
    }

    /** Records the current state of a file, along with its declared class names (for graph patching). */
    public void track(Path file, List<String> classNames) {
        try {
            String key = file.toAbsolutePath().toString();
            long mtime = Files.getLastModifiedTime(file).toMillis();
            String hash = sha256(file);
            state.put(key, new FileState(mtime, hash, classNames));
        } catch (IOException e) {
            log.debug("Cannot track file {}: {}", file, e.getMessage());
        }
    }

    /** Returns the class names tracked for a file, or empty list if unknown. */
    public List<String> getTrackedClassNames(Path file) {
        FileState s = state.get(file.toAbsolutePath().toString());
        if (s == null || s.classNames == null) return List.of();
        return s.classNames;
    }

    /** Removes a file from tracking (file was deleted). */
    public void remove(Path file) {
        state.remove(file.toAbsolutePath().toString());
    }

    /**
     * Returns the set of all absolute file paths currently tracked.
     * Used to detect files that have been deleted since the last index run.
     */
    public Set<Path> trackedFiles() {
        Set<Path> paths = new java.util.HashSet<>();
        for (String key : state.keySet()) {
            paths.add(Path.of(key));
        }
        return paths;
    }

    /** Clears all tracked state (for full re-index). */
    public void clear() {
        state.clear();
    }

    /** Persists state to disk atomically. */
    public void save() {
        try {
            Files.createDirectories(stateFile.getParent());
            Path tmp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), state);
            Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.warn("Failed to save file state: {}", e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<String, FileState> load() {
        if (!Files.exists(stateFile)) return new ConcurrentHashMap<>();
        try {
            Map<String, FileState> loaded = mapper.readValue(stateFile.toFile(),
                    mapper.getTypeFactory().constructMapType(HashMap.class, String.class, FileState.class));
            return loaded != null ? new ConcurrentHashMap<>(loaded) : new ConcurrentHashMap<>();
        } catch (IOException e) {
            log.warn("Could not read file state {}: {}", stateFile, e.getMessage());
            return new ConcurrentHashMap<>();
        }
    }

    private String sha256(Path file) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] bytes = Files.readAllBytes(file);
            byte[] digest = md.digest(bytes);
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
}
