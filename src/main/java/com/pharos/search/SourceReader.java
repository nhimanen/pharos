package com.pharos.search;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Reads a 1-based, inclusive line range from a source file. Disabled on reader nodes
 * ({@link #setEnabled}), where callers fall back to the bodies stored in the index.
 */
public final class SourceReader {

    private static volatile boolean enabled = true;

    private SourceReader() {}

    /** JVM-wide switch; when disabled, {@link #readRange} returns null without touching the disk. */
    public static void setEnabled(boolean enabled) {
        SourceReader.enabled = enabled;
    }

    /**
     * Returns the lines [startLine, endLine] (1-based, inclusive) from {@code filePath}
     * joined with {@code \n}. Returns null if the file does not exist or cannot be read,
     * or if the line range is invalid, and always when reading is disabled.
     */
    public static String readRange(String filePath, int startLine, int endLine) {
        if (!enabled) return null;
        if (filePath == null || filePath.isBlank()) return null;
        if (startLine < 1 || endLine < startLine) return null;
        Path p = Path.of(filePath);
        if (!Files.isRegularFile(p)) return null;
        try {
            List<String> lines = Files.readAllLines(p);
            int from = Math.min(startLine - 1, lines.size());
            int to   = Math.min(endLine, lines.size());
            if (from >= to) return null;
            return String.join("\n", lines.subList(from, to));
        } catch (IOException e) {
            return null;
        }
    }
}
