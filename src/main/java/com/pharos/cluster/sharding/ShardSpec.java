package com.pharos.cluster.sharding;

import com.pharos.cluster.ShardId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * A shard spec: how a project's files are divided into shards. An ordered list of key fields plus an
 * optional consistent-hash ring, written like {@code [project]} (the default), {@code [project, module]},
 * {@code [project, ring(path, shards=8)]} or {@code [language]}.
 *
 * <p><b>Fields.</b>
 * <ul>
 *   <li>{@code project}: the project. Always the first key, even when not written (ruling P6-R1: a
 *       shard never spans projects), so {@code [language]} means {@code [project, language]};</li>
 *   <li>{@code module}, {@code language}, {@code kind}: the file's {@link DocKeys} value;</li>
 *   <li>{@code prefix(path, depth=D)}: the first {@code D} directories of the normalised path (default 1;
 *       a file in the project root has the value {@code _});</li>
 *   <li>{@code ring(path, shards=N, vnodes=V)}: a consistent-hash ring over the normalised path, with
 *       shards {@code s01 … sNN}, or an explicit set {@code shards=[s01, s02, s04]} once a shard has been
 *       removed (names are stable and never renumbered). {@code vnodes} defaults to {@value #DEFAULT_VNODES}.
 *       At most one ring, and it is the last field.</li>
 * </ul>
 *
 * <p><b>Shard ids</b> ({@link #assign}): the project, then {@code ~<key>=<value>} per field after it, in
 * spec order: {@code lucene~module=core~ring=s03}. Under {@code [project]} the id is the project name,
 * as in Phases 1–5. A key value is made id-safe: characters outside {@code [A-Za-z0-9._-]} become
 * {@code -}, a run of dots becomes one dot, a missing value is {@code _}, and a value longer than
 * {@value #MAX_VALUE_LENGTH} characters is cut and suffixed with a hash of the original. Two values that
 * map to the same id-safe form share a shard; pruning (Task 3) applies the same mapping, so this is
 * always correct, just coarser.
 *
 * <p><b>The ring.</b> A file's position is {@code murmur3_64(normalizedRelativePath)}
 * ({@link Murmur3#hash64(String)}, {@link DocKeys#normalizePath(String)}); shard {@code s} owns the
 * points {@code murmur3_64(shardId + "#" + i)} for {@code i} in {@code [0, vnodes)}, where
 * {@code shardId} is that ring shard's full id ({@code lucene~ring=s03}); a file belongs to the shard
 * owning the first point clockwise from its position (positions compare as unsigned 64-bit values,
 * wrapping around; two shards on one point: the lower name wins). Adding a shard moves only the files
 * whose first point is now one of its points; removing one hands its arcs to its ring neighbours.
 *
 * <p><b>Versions.</b> A spec is immutable and compares by its canonical form ({@link #toString()});
 * changing a project's spec creates a new {@link VersionedSpec} in the catalog, never an edit.
 */
public final class ShardSpec {

    public static final int DEFAULT_VNODES = 128;
    public static final int MAX_VNODES = 1024;
    public static final int MAX_RING_SHARDS = 999;
    public static final int MAX_PREFIX_DEPTH = 16;
    static final int MAX_VALUE_LENGTH = 64;
    /** Key value of a file that has none (no module, a file in the root for {@code prefix}). */
    public static final String NO_VALUE = "_";

    /** The default spec: one shard per project, the shard id is the project name. */
    public static final ShardSpec PROJECT = parse("[project]");

    /** One field of a spec. */
    public sealed interface Field permits KeyField, PrefixField, RingField {
        /** The key name the field puts in shard ids ({@code module=…}). */
        String key();
    }

    /** {@code project}, {@code module}, {@code language} or {@code kind}. */
    public record KeyField(String key) implements Field {
        static final Set<String> NAMES = Set.of("project", "module", "language", "kind");

        @Override
        public String toString() { return key; }
    }

    /** {@code prefix(path, depth=D)}. */
    public record PrefixField(int depth) implements Field {
        @Override
        public String key() { return "prefix"; }

        @Override
        public String toString() { return depth == 1 ? "prefix(path)" : "prefix(path, depth=" + depth + ")"; }
    }

    /** {@code ring(path, shards=…, vnodes=V)}: the ring shard numbers ({@code 3} is {@code s03}). */
    public record RingField(SortedSet<Integer> shards, int vnodes) implements Field {
        public RingField {
            shards = Collections.unmodifiableSortedSet(new TreeSet<>(shards));
        }

        @Override
        public String key() { return "ring"; }

        /** The ring shard names, in order: {@code s01, s02, …}. */
        public List<String> names() {
            return shards.stream().map(ShardSpec::ringName).toList();
        }

        @Override
        public String toString() {
            boolean contiguous = shards.first() == 1 && shards.last() == shards.size();
            String s = contiguous ? String.valueOf(shards.size()) : "[" + String.join(", ", names()) + "]";
            return "ring(path, shards=" + s + (vnodes == DEFAULT_VNODES ? "" : ", vnodes=" + vnodes) + ")";
        }
    }

    private static final Pattern RING_NAME = Pattern.compile("^s(0[1-9]|[1-9][0-9]{1,2})$");

    private final List<Field> fields;
    private final String canonical;
    /** Rings by the id prefix before {@code ~ring=}: one per combination of the preceding key values. */
    private final Map<String, Ring> rings = new ConcurrentHashMap<>();

    private ShardSpec(List<Field> fields) {
        this.fields = List.copyOf(fields);
        StringBuilder sb = new StringBuilder("[");
        for (Field f : this.fields) {
            if (sb.length() > 1) sb.append(", ");
            sb.append(f);
        }
        this.canonical = sb.append(']').toString();
    }

    /** The name of ring shard {@code n}: {@code s01} … {@code s99}, {@code s100} …. */
    public static String ringName(int n) {
        return String.format("s%02d", n);
    }

    /** The fields in order; the first is always {@code project}. */
    public List<Field> fields() { return fields; }

    /** The ring, when the spec has one (always its last field). */
    public Optional<RingField> ring() {
        return fields.getLast() instanceof RingField r ? Optional.of(r) : Optional.empty();
    }

    /** True for {@code [project]}: shard id = project name, the Phases 1–5 layout. */
    public boolean isDefault() { return fields.size() == 1; }

    /**
     * The shard of a file.
     *
     * @throws IllegalArgumentException when the project is not a valid cluster project name, or the id
     *         would exceed {@value ShardId#MAX_LENGTH} characters
     */
    public ShardId assign(DocKeys keys) {
        ShardId projectId = ShardId.ofProject(keys.project());
        if (isDefault()) return projectId;
        LinkedHashMap<String, String> kv = new LinkedHashMap<>();
        for (int i = 1; i < fields.size(); i++) {
            Field f = fields.get(i);
            switch (f) {
                case KeyField k -> kv.put(k.key(), idValue(switch (k.key()) {
                    case "module" -> keys.module();
                    case "language" -> keys.language();
                    case "kind" -> keys.fileKind();
                    default -> throw new IllegalStateException("unexpected key " + k.key());
                }));
                case PrefixField p -> kv.put(p.key(), idValue(dirPrefix(keys.relativePath(), p.depth())));
                case RingField r -> kv.put(r.key(), ring(keys.project(), kv, r).owner(Murmur3.hash64(keys.relativePath())));
            }
        }
        try {
            return ShardId.keyed(keys.project(), kv);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("spec " + canonical + " gives project '" + keys.project()
                    + "' an invalid shard id: " + e.getMessage(), e);
        }
    }

    private Ring ring(String project, LinkedHashMap<String, String> before, RingField r) {
        String group = ShardId.keyed(project, before).value();
        return rings.computeIfAbsent(group, g -> {
            List<String> names = r.names();
            String[] ids = new String[names.size()];
            for (int i = 0; i < ids.length; i++) {
                LinkedHashMap<String, String> kv = new LinkedHashMap<>(before);
                kv.put(r.key(), names.get(i));
                ids[i] = ShardId.keyed(project, kv).value();
            }
            return new Ring(names, ids, r.vnodes());
        });
    }

    /** The first {@code depth} directories of {@code path} joined by '/', or null for a file in the root. */
    private static String dirPrefix(String path, int depth) {
        String[] segs = path.split("/");
        int dirs = Math.min(depth, segs.length - 1);
        if (dirs == 0) return null;
        return String.join("/", Arrays.asList(segs).subList(0, dirs));
    }

    /** A key value as it appears in a shard id; see the class comment. */
    static String idValue(String raw) {
        if (raw == null || raw.isEmpty()) return NO_VALUE;
        StringBuilder sb = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            boolean ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
            if (c == '.') {
                if (sb.isEmpty() || sb.charAt(sb.length() - 1) != '.') sb.append('.');
            } else {
                sb.append(ok ? c : '-');
            }
        }
        if (sb.length() > MAX_VALUE_LENGTH) {
            String hash = String.format("%016x", Murmur3.hash64(raw)).substring(0, 8);
            sb.setLength(MAX_VALUE_LENGTH - 9);
            if (sb.charAt(sb.length() - 1) == '.') sb.setCharAt(sb.length() - 1, '-');
            sb.append('-').append(hash);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ShardSpec s && s.canonical.equals(canonical);
    }

    @Override
    public int hashCode() { return canonical.hashCode(); }

    /** The canonical form, which {@link #parse} reads back to an equal spec. */
    @Override
    public String toString() { return canonical; }

    // ---------------------------------------------------------------------------------------- ring

    /** The points of one ring, as unsigned positions (sign bit flipped so signed order is ring order). */
    private static final class Ring {
        private final long[] points;
        private final String[] owners;

        Ring(List<String> names, String[] ids, int vnodes) {
            int n = names.size() * vnodes;
            long[][] pts = new long[n][];
            int k = 0;
            for (int s = 0; s < names.size(); s++) {
                for (int i = 0; i < vnodes; i++) {
                    pts[k++] = new long[]{Murmur3.hash64(ids[s] + "#" + i) ^ Long.MIN_VALUE, s};
                }
            }
            // names are in ring-number order, so on a tie the lower name wins
            Arrays.sort(pts, (a, b) -> a[0] != b[0] ? Long.compare(a[0], b[0]) : Long.compare(a[1], b[1]));
            points = new long[n];
            owners = new String[n];
            for (int i = 0; i < n; i++) {
                points[i] = pts[i][0];
                owners[i] = names.get((int) pts[i][1]);
            }
        }

        /** The owner of the first point at or clockwise after {@code position}. */
        String owner(long position) {
            long p = position ^ Long.MIN_VALUE;
            int i = Arrays.binarySearch(points, p);
            if (i < 0) {
                i = -i - 1;
            } else {
                while (i > 0 && points[i - 1] == p) i--; // the lowest-name owner of a shared point
            }
            return owners[i == points.length ? 0 : i];
        }
    }

    // ------------------------------------------------------------------------------------- parsing

    /**
     * Parses and validates a spec.
     *
     * @throws IllegalArgumentException naming the input and the problem
     */
    public static ShardSpec parse(String text) {
        if (text == null) throw new IllegalArgumentException("shard spec is missing");
        try {
            return new Parser(text).spec();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid shard spec '" + text + "': " + e.getMessage()
                    + " (examples: [project], [project, module], [project, ring(path, shards=8)], [language])", e);
        }
    }

    private static final class Parser {
        private final String s;
        private int pos;

        Parser(String s) { this.s = s; }

        ShardSpec spec() {
            expect('[');
            List<Field> fields = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            do {
                Field f = field();
                if (!seen.add(f.key())) throw new IllegalArgumentException("'" + f.key() + "' appears twice");
                if (!fields.isEmpty() && fields.getLast() instanceof RingField) {
                    throw new IllegalArgumentException("ring(...) must be the last field");
                }
                if (f.key().equals("project") && !fields.isEmpty()) {
                    throw new IllegalArgumentException("'project' must be the first field");
                }
                fields.add(f);
            } while (accept(','));
            expect(']');
            skipWs();
            if (pos != s.length()) throw new IllegalArgumentException("unexpected text after ']'");
            if (!fields.getFirst().key().equals("project")) fields.addFirst(new KeyField("project"));
            return new ShardSpec(fields);
        }

        private Field field() {
            String name = ident();
            if (KeyField.NAMES.contains(name)) {
                if (peek('(')) throw new IllegalArgumentException("'" + name + "' takes no arguments");
                return new KeyField(name);
            }
            if (!name.equals("prefix") && !name.equals("ring")) {
                throw new IllegalArgumentException("unknown field '" + name
                        + "' (allowed: project, module, language, kind, prefix(path, depth=D), ring(path, shards=N))");
            }
            expect('(');
            String first = ident();
            if (!first.equals("path")) throw new IllegalArgumentException(name + "(...) takes 'path' first, got '" + first + "'");
            Map<String, Object> args = new LinkedHashMap<>();
            while (accept(',')) {
                String arg = ident();
                expect('=');
                Object value = peek('[') ? nameList() : integer(name + "(" + arg + "=...)");
                if (args.put(arg, value) != null) throw new IllegalArgumentException(name + "(...): '" + arg + "' appears twice");
            }
            expect(')');
            return name.equals("prefix") ? prefix(args) : ring(args);
        }

        private PrefixField prefix(Map<String, Object> args) {
            int depth = 1;
            for (Map.Entry<String, Object> e : args.entrySet()) {
                if (!e.getKey().equals("depth") || !(e.getValue() instanceof Integer d)) {
                    throw new IllegalArgumentException("prefix(...) takes only depth=D, got '" + e.getKey() + "'");
                }
                depth = d;
            }
            if (depth < 1 || depth > MAX_PREFIX_DEPTH) {
                throw new IllegalArgumentException("prefix depth must be between 1 and " + MAX_PREFIX_DEPTH + ", got " + depth);
            }
            return new PrefixField(depth);
        }

        private RingField ring(Map<String, Object> args) {
            SortedSet<Integer> shards = null;
            int vnodes = DEFAULT_VNODES;
            for (Map.Entry<String, Object> e : args.entrySet()) {
                switch (e.getKey()) {
                    case "shards" -> {
                        shards = new TreeSet<>();
                        if (e.getValue() instanceof Integer n) {
                            if (n < 1 || n > MAX_RING_SHARDS) {
                                throw new IllegalArgumentException("ring shards must be between 1 and " + MAX_RING_SHARDS + ", got " + n);
                            }
                            for (int i = 1; i <= n; i++) shards.add(i);
                        } else {
                            @SuppressWarnings("unchecked") List<String> names = (List<String>) e.getValue();
                            for (String nm : names) {
                                if (!RING_NAME.matcher(nm).matches()) {
                                    throw new IllegalArgumentException("ring shard name '" + nm
                                            + "' is invalid (s01, s02, …, s" + MAX_RING_SHARDS + ")");
                                }
                                if (!shards.add(Integer.parseInt(nm.substring(1)))) {
                                    throw new IllegalArgumentException("ring shard '" + nm + "' appears twice");
                                }
                            }
                            if (shards.isEmpty()) throw new IllegalArgumentException("ring needs at least one shard");
                        }
                    }
                    case "vnodes" -> {
                        if (!(e.getValue() instanceof Integer v) || v < 1 || v > MAX_VNODES) {
                            throw new IllegalArgumentException("ring vnodes must be between 1 and " + MAX_VNODES
                                    + ", got " + e.getValue());
                        }
                        vnodes = v;
                    }
                    default -> throw new IllegalArgumentException("ring(...) takes shards=N and vnodes=V, got '" + e.getKey() + "'");
                }
            }
            if (shards == null) throw new IllegalArgumentException("ring(...) needs shards=N");
            return new RingField(shards, vnodes);
        }

        private List<String> nameList() {
            expect('[');
            List<String> out = new ArrayList<>();
            if (!peek(']')) {
                do {
                    out.add(ident());
                } while (accept(','));
            }
            expect(']');
            return out;
        }

        private int integer(String what) {
            skipWs();
            int start = pos;
            while (pos < s.length() && Character.isDigit(s.charAt(pos))) pos++;
            if (start == pos) throw new IllegalArgumentException(what + " needs a number");
            if (pos - start > 6) throw new IllegalArgumentException(what + ": number too large");
            return Integer.parseInt(s.substring(start, pos));
        }

        private String ident() {
            skipWs();
            int start = pos;
            while (pos < s.length() && (Character.isLetterOrDigit(s.charAt(pos)) || s.charAt(pos) == '_')) pos++;
            if (start == pos) {
                throw new IllegalArgumentException(pos < s.length()
                        ? "unexpected '" + s.charAt(pos) + "' at position " + pos
                        : "unexpected end");
            }
            return s.substring(start, pos);
        }

        private boolean peek(char c) {
            skipWs();
            return pos < s.length() && s.charAt(pos) == c;
        }

        private boolean accept(char c) {
            if (!peek(c)) return false;
            pos++;
            return true;
        }

        private void expect(char c) {
            if (!accept(c)) {
                throw new IllegalArgumentException("expected '" + c + "' at position " + pos
                        + (pos < s.length() ? ", got '" + s.charAt(pos) + "'" : ""));
            }
        }

        private void skipWs() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) pos++;
        }
    }
}
