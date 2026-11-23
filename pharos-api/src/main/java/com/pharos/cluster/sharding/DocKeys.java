package com.pharos.cluster.sharding;

import java.nio.file.Path;
import java.util.Objects;

/**
 * The shard-key values of one source file, which {@link ShardSpec#assign} maps to a shard. Every
 * document of a file (its class, method and chunk documents) has the same keys, so a file is never
 * split across shards.
 *
 * @param project      the project (cluster project name)
 * @param relativePath the file's path relative to the project root, normalised by
 *                     {@link #normalizePath(String)} (the constructor normalises it)
 * @param module       the build module the file belongs to, or null when none
 * @param language     the file's language (for example {@code java}), or null when unknown
 * @param fileKind     the kind of file (for example {@code code} or {@code doc}), or null when unknown
 */
public record DocKeys(String project, String relativePath, String module, String language, String fileKind) {

    public DocKeys {
        Objects.requireNonNull(project, "project");
        relativePath = normalizePath(relativePath);
    }

    /** Keys with only the project and path known. */
    public static DocKeys of(String project, String relativePath) {
        return new DocKeys(project, relativePath, null, null, null);
    }

    /**
     * The normalised relative path the ring hashes, so a file has the same position on every platform
     * (ruling P6-R7):
     * <ul>
     *   <li>forward slashes ({@code '\'} becomes {@code '/'});</li>
     *   <li>relative to the project root: no leading {@code '/'} or drive letter;</li>
     *   <li>no leading {@code ./}, no empty or {@code .} segments ({@code a//b}, {@code a/./b} become
     *       {@code a/b}), no trailing {@code '/'};</li>
     *   <li>case preserved, and no Unicode normalisation: the name's characters as given.</li>
     * </ul>
     * <b>Reuse this normaliser</b> for the planned "relative paths in indexes" work (file state and the
     * {@code filePath} field), so the stored path and the ring position always agree.
     *
     * @throws IllegalArgumentException for an empty or absolute path, or one with a {@code ..} segment
     */
    public static String normalizePath(String path) {
        if (path == null) throw new IllegalArgumentException("relative path is missing");
        String p = path.replace('\\', '/');
        boolean drive = p.length() >= 2 && p.charAt(1) == ':' && Character.isLetter(p.charAt(0))
                && (p.length() == 2 || p.charAt(2) == '/');
        if (p.startsWith("/") || drive) {
            throw new IllegalArgumentException("path '" + path + "' is not relative to the project root");
        }
        StringBuilder sb = new StringBuilder(p.length());
        for (String seg : p.split("/", -1)) {
            if (seg.isEmpty() || seg.equals(".")) continue;
            if (seg.equals("..")) {
                throw new IllegalArgumentException("path '" + path + "' leaves the project root ('..')");
            }
            if (!sb.isEmpty()) sb.append('/');
            sb.append(seg);
        }
        if (sb.isEmpty()) throw new IllegalArgumentException("path '" + path + "' names no file");
        return sb.toString();
    }

    /** {@link #normalizePath(String)} of {@code file} relative to {@code projectRoot}. */
    public static String normalizePath(Path projectRoot, Path file) {
        Path rel = projectRoot.toAbsolutePath().normalize().relativize(file.toAbsolutePath().normalize());
        StringBuilder sb = new StringBuilder();
        for (Path name : rel) {
            if (!sb.isEmpty()) sb.append('/');
            sb.append(name);
        }
        return normalizePath(sb.toString());
    }
}
