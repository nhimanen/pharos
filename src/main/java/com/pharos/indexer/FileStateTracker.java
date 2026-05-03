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
    private Map<String, FileState> state;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FileState {
        public long lastModifiedMs;
        public String sha256;

        public FileState() {}
        public FileState(long lastModifiedMs, String sha256) {
            this.lastModifiedMs = lastModifiedMs;
            this.sha256 = sha256;
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
        try {
            String key = file.toAbsolutePath().toString();
            long mtime = Files.getLastModifiedTime(file).toMillis();
            String hash = sha256(file);
            state.put(key, new FileState(mtime, hash));
        } catch (IOException e) {
            log.debug("Cannot track file {}: {}", file, e.getMessage());
        }
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
    private Map<String, FileState> load() {
        if (!Files.exists(stateFile)) return new HashMap<>();
        try {
            Map<String, FileState> loaded = mapper.readValue(stateFile.toFile(),
                    mapper.getTypeFactory().constructMapType(HashMap.class, String.class, FileState.class));
            return loaded != null ? loaded : new HashMap<>();
        } catch (IOException e) {
            log.warn("Could not read file state {}: {}", stateFile, e.getMessage());
            return new HashMap<>();
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
