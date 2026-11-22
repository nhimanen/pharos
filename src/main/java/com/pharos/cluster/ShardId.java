package com.pharos.cluster;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedMap;
import java.util.regex.Pattern;

/**
 * Identifier of an index shard.
 *
 * <p><b>Format.</b> Under the default shard spec {@code [project]} the id is the project name, as in
 * Phases 1–5. A project split by another spec has ids {@code <project>~<key>=<value>[~<key>=<value>…]},
 * for example {@code lucene~ring=s03} or {@code lucene~module=core} ({@link #keyed}). The project
 * part follows the project rule ({@link #ofProject}); keys are lower-case letters; values use
 * letters, digits, {@code '.'}, {@code '_'} and {@code '-'}. At most 128 characters, never {@code ".."}.
 *
 * <p><b>Never derive the project from the id string.</b> Which project a shard belongs to is recorded
 * in the catalog and answered by {@link com.pharos.cluster.sharding.ProjectShards}; a guard test fails
 * when code elsewhere splits ids on {@code '~'}.
 *
 * <p><b>Object keys and file paths</b> embed the id through {@link #pathSegment()}, one URL-safe
 * encoding: the unreserved characters {@code [A-Za-z0-9._~-]} (RFC 3986) pass through and every other
 * byte is percent-encoded with upper-case hex ({@code '='} becomes {@code %3D}). A project id
 * therefore maps to itself, byte for byte, so existing buckets and cache dirs keep working.
 */
public record ShardId(String value) {

    /** A project usable as a shard id: the Phase 1–5 rule. */
    private static final Pattern PROJECT = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$");
    private static final Pattern KEY = Pattern.compile("^[a-z]{1,32}$");
    private static final Pattern VALUE = Pattern.compile("^[A-Za-z0-9._-]{1,128}$");
    public static final int MAX_LENGTH = 128;

    public ShardId {
        String problem = problem(value);
        if (problem != null) throw new IllegalArgumentException(problem);
    }

    /** The id of {@code project} under spec {@code [project]}; the project name must follow the shard-id rule. */
    public static ShardId ofProject(String project) {
        if (!isProjectName(project)) throw new IllegalArgumentException(projectMessage(project));
        return new ShardId(project);
    }

    /** True when {@code project} can be a cluster project (and so the id of its {@code [project]} shard). */
    public static boolean isProjectName(String project) {
        return project != null && PROJECT.matcher(project).matches() && !project.contains("..");
    }

    /**
     * The id of one shard of {@code project}: {@code <project>~<key>=<value>…} in the map's order; an
     * empty map gives {@link #ofProject}.
     *
     * @throws IllegalArgumentException when a key or value breaks the format or the id gets too long
     */
    public static ShardId keyed(String project, SequencedMap<String, String> keyValues) {
        ShardId base = ofProject(project);
        if (keyValues.isEmpty()) return base;
        StringBuilder sb = new StringBuilder(base.value());
        for (Map.Entry<String, String> e : keyValues.entrySet()) {
            sb.append('~').append(e.getKey()).append('=').append(e.getValue());
        }
        return new ShardId(sb.toString());
    }

    /**
     * The key values of this id in order ({@code lucene~module=core~ring=s03} gives
     * {@code module=core, ring=s03}); empty for a {@code [project]} id. The values are the id's
     * ({@code ShardSpec}'s id-safe form), so routing compares them with values made id-safe the same way.
     * This is <b>not</b> how to find the project of a shard: that is
     * {@link com.pharos.cluster.sharding.ProjectShards#projectOf}.
     */
    public SequencedMap<String, String> keyValues() {
        java.util.LinkedHashMap<String, String> out = new java.util.LinkedHashMap<>();
        if (value.indexOf('~') < 0) return out;
        String[] parts = value.split("~", -1);
        for (int i = 1; i < parts.length; i++) {
            int eq = parts[i].indexOf('=');
            out.put(parts[i].substring(0, eq), parts[i].substring(eq + 1));
        }
        return out;
    }

    /**
     * The project of a reader-pin wildcard ({@code cluster.reader.shards}: {@code lucene~*} means every
     * shard of project {@code lucene}, P6-R5), or empty when {@code pin} is not one.
     */
    public static Optional<String> projectWildcard(String pin) {
        if (pin == null || !pin.endsWith("~*")) return Optional.empty();
        String project = pin.substring(0, pin.length() - 2);
        return isProjectName(project) ? Optional.of(project) : Optional.empty();
    }

    /** How a reader-pin wildcard is written, for messages. */
    public static final String WILDCARD_HINT = "a project wildcard <project>~* (every shard of the project)";

    /** The reader-pin wildcard of {@code project}: every shard of it ({@link #projectWildcard}). */
    public static String wildcardOf(String project) {
        return ofProject(project).value() + "~*";
    }

    /** True when {@code value} is a valid shard id. */
    public static boolean isValid(String value) {
        return problem(value) == null;
    }

    /** This id as one object-key or file-name segment: the URL-safe encoding (see the class comment). */
    public String pathSegment() {
        return encode(value);
    }

    /**
     * The local path segment of a name that should be a shard id: its {@link #pathSegment()}, or the
     * name unchanged when it is not a valid id (registry names that are not shard ids keep their
     * Phase 1–5 behaviour).
     */
    public static String pathSegmentOf(String name) {
        return isValid(name) ? encode(name) : name;
    }

    /**
     * The id a {@link #pathSegment()} came from, or empty when {@code segment} is not one: only the
     * canonical encoding of a valid id decodes (a lower-case or needless escape does not).
     */
    public static Optional<ShardId> fromPathSegment(String segment) {
        if (segment == null) return Optional.empty();
        ByteArrayOutputStream out = new ByteArrayOutputStream(segment.length());
        for (int i = 0; i < segment.length(); i++) {
            char c = segment.charAt(i);
            if (c == '%') {
                if (i + 2 >= segment.length()) return Optional.empty();
                int hi = Character.digit(segment.charAt(i + 1), 16);
                int lo = Character.digit(segment.charAt(i + 2), 16);
                if (hi < 0 || lo < 0) return Optional.empty();
                out.write(hi << 4 | lo);
                i += 2;
            } else if (c < 0x80) {
                out.write(c);
            } else {
                return Optional.empty();
            }
        }
        String decoded = out.toString(StandardCharsets.UTF_8);
        if (!isValid(decoded) || !encode(decoded).equals(segment)) return Optional.empty();
        return Optional.of(new ShardId(decoded));
    }

    private static String encode(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder(bytes.length + 8);
        for (byte value : bytes) {
            int b = value & 0xff;
            boolean unreserved = (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
                    || b == '.' || b == '_' || b == '-' || b == '~';
            if (unreserved) {
                sb.append((char) b);
            } else {
                sb.append('%').append(Character.toUpperCase(Character.forDigit(b >> 4, 16)))
                        .append(Character.toUpperCase(Character.forDigit(b & 0xf, 16)));
            }
        }
        return sb.toString();
    }

    private static String problem(String value) {
        if (value == null || (value.indexOf('~') < 0 && value.indexOf('=') < 0)) {
            return isProjectName(value) ? null : projectMessage(value);
        }
        String shape = "shard id '" + value + "' is invalid: expected <project>~<key>=<value>[~<key>=<value>...]"
                + " (project: letters, digits, '.', '_' or '-'; key: lower-case letters; value: letters, digits,"
                + " '.', '_' or '-'; at most " + MAX_LENGTH + " characters, no '..')";
        if (value.length() > MAX_LENGTH || value.contains("..")) return shape;
        String[] parts = value.split("~", -1);
        if (parts.length < 2 || !isProjectName(parts[0])) return shape;
        for (int i = 1; i < parts.length; i++) {
            int eq = parts[i].indexOf('=');
            if (eq < 0) return shape;
            if (!KEY.matcher(parts[i].substring(0, eq)).matches()) return shape;
            if (!VALUE.matcher(parts[i].substring(eq + 1)).matches()) return shape;
        }
        return null;
    }

    private static String projectMessage(String value) {
        return "project name '" + value + "' cannot be a shard id: use letters, digits, '.', '_' or '-'";
    }

    @Override
    public String toString() {
        return value;
    }
}
