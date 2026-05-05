package com.example.files;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Blob storage abstraction for uploading, retrieving, and managing files.
 */
public class FileStorageService {

    private final Map<String, byte[]> blobs     = new HashMap<>();
    private final Map<String, Long>   createdAt = new HashMap<>();

    /**
     * Uploads a file and stores it under a generated identifier.
     *
     * @param name    the original file name (used as a hint for MIME type detection)
     * @param content the raw file bytes to persist
     * @return the assigned file identifier
     */
    public String uploadFile(String name, byte[] content) {
        String id = UUID.randomUUID() + "-" + name;
        blobs.put(id, content);
        createdAt.put(id, System.currentTimeMillis());
        return id;
    }

    /**
     * Downloads the binary contents of a stored file by its identifier.
     * Returns null if the file does not exist.
     *
     * @param fileId the identifier returned by {@link #uploadFile}
     * @return the raw file bytes, or null if not found
     */
    public byte[] downloadFile(String fileId) {
        return blobs.get(fileId);
    }

    /**
     * Permanently deletes a file from the store.
     * Has no effect if the identifier does not exist.
     *
     * @param fileId the file to remove
     */
    public void deleteFile(String fileId) {
        blobs.remove(fileId);
        createdAt.remove(fileId);
    }

    /**
     * Lists the identifiers of all files stored under a given directory prefix.
     *
     * @param directory the directory prefix to enumerate
     * @return sorted list of file identifiers whose keys start with this prefix
     */
    public List<String> listFiles(String directory) {
        return blobs.keySet().stream()
                .filter(k -> k.startsWith(directory + "/"))
                .sorted()
                .toList();
    }

    /**
     * Returns metadata about a stored file: its size in bytes and creation timestamp.
     * Returns null if the file does not exist.
     *
     * @param fileId the file identifier
     * @return a metadata record with size and creation time, or null
     */
    public FileMetadata getFileMetadata(String fileId) {
        byte[] data = blobs.get(fileId);
        if (data == null) return null;
        return new FileMetadata(fileId, data.length, createdAt.getOrDefault(fileId, 0L));
    }

    /** Size and creation timestamp for a stored file. */
    public record FileMetadata(String id, long sizeBytes, long createdAtMs) {}
}
