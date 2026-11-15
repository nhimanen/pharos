package com.pharos.graph;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/**
 * Immutable in-memory {@link GraphView} loaded from a compact graph file (see
 * {@link CompactGraphWriter}). Every method returns what {@link CallGraph} returns for the same
 * graph: vertices live in three namespaces (Method, CodeType, CodeField), each query only looks up
 * its start vertex in the namespace {@link CallGraph} uses, and neighbours are collected in a
 * {@link LinkedHashSet} (deduplicated, in edge order).
 *
 * <p>Memory: one {@code String[]} of fqns, per edge type a forward and a reverse CSR
 * (offsets + targets as {@code int[]}), and one fqn to id map per vertex kind.
 */
public final class CompactGraph implements GraphView {
    private static final int METHOD = 0, CODE_TYPE = 1, CODE_FIELD = 2;
    private static final int CALLS = 0, INHERITS = 1, IMPLEMENTS = 2, DECLARES = 3, READS = 4,
            WRITES = 5, RETURNS = 6, TAKES = 7, ANNOTATED = 8;

    /** Sanity limits: counts / string sizes above these mean a corrupt file, not a big graph. */
    private static final int MAX_COUNT = 50_000_000;
    private static final int MAX_STRING_BYTES = 64 * 1024;
    private static final int INITIAL_CAPACITY = 1 << 16;

    private final String[] fqns;
    private final byte[] kinds;
    /** Per vertex: classPrefix is non-empty (only meaningful for methods). */
    private final boolean[] owned;
    private final List<Map<String, Integer>> idByKind;
    /** Field vertices grouped by ownerFqn, in vertex order. */
    private final Map<String, int[]> fieldsByOwner;
    /** Indexed by vertex id; null for non-fields. */
    private final String[] fieldType;
    private final String[] accessMod;
    /** [edgeType][0=forward,1=reverse] offsets / targets; offsets null when the type has no edges. */
    private final int[][][] offsets;
    private final int[][][] targets;
    private final long methodCount;
    private final long[] edgeCount;

    private CompactGraph(String[] fqns, byte[] kinds, boolean[] owned, List<Map<String, Integer>> idByKind,
                         Map<String, int[]> fieldsByOwner, String[] fieldType, String[] accessMod,
                         int[][][] offsets, int[][][] targets, long[] edgeCount) {
        this.fqns = fqns;
        this.kinds = kinds;
        this.owned = owned;
        this.idByKind = idByKind;
        this.fieldsByOwner = fieldsByOwner;
        this.fieldType = fieldType;
        this.accessMod = accessMod;
        this.offsets = offsets;
        this.targets = targets;
        this.edgeCount = edgeCount;
        this.methodCount = idByKind.get(METHOD).size();
    }

    public static CompactGraph read(Path file) throws IOException {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(
                new GZIPInputStream(Files.newInputStream(file), 1 << 16), 1 << 16))) {
            return read(in);
        } catch (ZipException e) {
            throw new IOException("not a pharos compact graph: " + file, e);
        }
    }

    private static CompactGraph read(DataInputStream in) throws IOException {
        byte[] magic = new byte[4];
        try {
            in.readFully(magic);
        } catch (EOFException e) {
            throw new IOException("not a pharos compact graph: file too short", e);
        }
        if (!new String(magic, StandardCharsets.US_ASCII).equals(CompactGraphWriter.MAGIC)) {
            throw new IOException("not a pharos compact graph: bad magic");
        }
        int version = in.readInt();
        if (version != CompactGraphWriter.VERSION) {
            throw new IOException("unsupported compact graph version " + version);
        }
        int n = in.readInt();
        if (n < 0) throw new IOException("corrupt compact graph: negative vertex count");
        if (n > MAX_COUNT) throw new IOException("corrupt compact graph: vertex count " + n);
        // Allocate in growing steps: a corrupt count then hits EOF on the short stream instead of
        // allocating gigabytes up front (gzip only verifies its CRC at the very end).
        int cap = Math.min(n, INITIAL_CAPACITY);
        String[] fqns = new String[cap];
        byte[] kinds = new byte[cap];
        boolean[] owned = new boolean[cap];
        String[] fieldType = new String[cap];
        String[] accessMod = new String[cap];
        List<Map<String, Integer>> idByKind = List.of(new HashMap<>(), new HashMap<>(), new HashMap<>());
        Map<String, List<Integer>> byOwner = new LinkedHashMap<>();
        Map<String, String> pool = new HashMap<>(); // dedups fqn-less strings (types, modifiers, owners)
        for (int i = 0; i < n; i++) {
            if (i == fqns.length) {
                int grown = (int) Math.min(n, (long) fqns.length * 2);
                fqns = Arrays.copyOf(fqns, grown);
                kinds = Arrays.copyOf(kinds, grown);
                owned = Arrays.copyOf(owned, grown);
                fieldType = Arrays.copyOf(fieldType, grown);
                accessMod = Arrays.copyOf(accessMod, grown);
            }
            int kind = in.readByte();
            if (kind < 0 || kind >= CompactGraphWriter.KINDS.length) {
                throw new IOException("corrupt compact graph: bad vertex kind " + kind);
            }
            kinds[i] = (byte) kind;
            fqns[i] = readString(in);
            String classPrefix = readString(in);
            String ownerFqn = readString(in);
            String ft = readString(in);
            String am = readString(in);
            if (fqns[i] == null) throw new IOException("corrupt compact graph: vertex without fqn");
            owned[i] = classPrefix != null && !classPrefix.isEmpty();
            idByKind.get(kind).put(fqns[i], i);
            if (kind == CODE_FIELD) {
                fieldType[i] = pool(pool, ft);
                accessMod[i] = pool(pool, am);
                if (ownerFqn != null) byOwner.computeIfAbsent(pool(pool, ownerFqn), k -> new ArrayList<>()).add(i);
            }
        }
        Map<String, int[]> fieldsByOwner = new HashMap<>();
        byOwner.forEach((k, v) -> fieldsByOwner.put(k, v.stream().mapToInt(Integer::intValue).toArray()));

        int types = CallGraph.EDGE_TYPES.size();
        int[][][] offsets = new int[types][2][];
        int[][][] targets = new int[types][2][];
        long[] edgeCount = new long[types];
        for (int t = 0; t < types; t++) {
            int m = in.readInt();
            if (m < 0) throw new IOException("corrupt compact graph: negative edge count");
            edgeCount[t] = m;
            if (m == 0) continue;
            if (m > MAX_COUNT) throw new IOException("corrupt compact graph: edge count " + m);
            int[] from = readIds(in, m, n), to = readIds(in, m, n);
            csr(from, to, n, offsets[t][0] = new int[n + 1], targets[t][0] = new int[m], false);
            csr(to, from, n, offsets[t][1] = new int[n + 1], targets[t][1] = new int[m], true);
        }
        return new CompactGraph(fqns, kinds, owned, idByKind, fieldsByOwner, fieldType, accessMod,
                offsets, targets, edgeCount);
    }

    /**
     * Counting sort of (key, value) pairs by key into CSR form. Forward lists keep the file order
     * (ArcadeDB's out-edge order); reverse lists are filled from the end of the file order backwards, i.e. grouped by
     * target with the highest from-id first (an approximation of ArcadeDB's newest-first in-edge
     * list, exact only when vertex order equals edge-insertion order).
     */
    private static void csr(int[] key, int[] value, int n, int[] off, int[] out, boolean newestFirst) {
        for (int k : key) off[k + 1]++;
        for (int i = 0; i < n; i++) off[i + 1] += off[i];
        int[] pos = Arrays.copyOf(off, n);
        if (newestFirst) {
            for (int i = key.length - 1; i >= 0; i--) out[pos[key[i]]++] = value[i];
        } else {
            for (int i = 0; i < key.length; i++) out[pos[key[i]]++] = value[i];
        }
    }

    /** Reads {@code m} vertex indexes, growing the array as data arrives (see read()). */
    private static int[] readIds(DataInputStream in, int m, int n) throws IOException {
        int[] a = new int[Math.min(m, INITIAL_CAPACITY)];
        for (int i = 0; i < m; i++) {
            if (i == a.length) a = Arrays.copyOf(a, (int) Math.min(m, (long) a.length * 2));
            a[i] = checkId(in.readInt(), n);
        }
        return a;
    }

    private static int checkId(int id, int n) throws IOException {
        if (id < 0 || id >= n) throw new IOException("corrupt compact graph: vertex index " + id);
        return id;
    }

    private static String pool(Map<String, String> pool, String s) {
        if (s == null) return null;
        String old = pool.putIfAbsent(s, s);
        return old != null ? old : s;
    }

    private static String readString(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len == -1) return null;
        if (len < 0 || len > MAX_STRING_BYTES) throw new IOException("corrupt compact graph: string length " + len);
        byte[] b = new byte[len];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    // ---- traversal helpers ------------------------------------------------------------------

    private int id(int kind, String fqn) {
        if (fqn == null) return -1;
        Integer i = idByKind.get(kind).get(fqn);
        return i == null ? -1 : i;
    }

    /** Neighbours of {@code v} over the given edge types, as the fqn set CallGraph builds. */
    private Set<String> neighbors(int kind, String fqn, boolean in, int... edgeTypes) {
        int v = id(kind, fqn);
        if (v < 0) return Set.of();
        Set<String> result = new LinkedHashSet<>();
        // ArcadeDB keeps one newest-first edge list per vertex, so with several edge types the
        // later-inserted type (implements_iface, after inherits) comes first
        for (int i = edgeTypes.length - 1; i >= 0; i--) forEachNeighbor(v, edgeTypes[i], in, w -> result.add(fqns[w]));
        return result;
    }

    private void forEachNeighbor(int v, int edgeType, boolean in, java.util.function.IntConsumer visitor) {
        int[] off = offsets[edgeType][in ? 1 : 0];
        if (off == null) return;
        int[] tgt = targets[edgeType][in ? 1 : 0];
        for (int i = off[v]; i < off[v + 1]; i++) visitor.accept(tgt[i]);
    }

    // ---- GraphView --------------------------------------------------------------------------

    @Override public Set<String> callers(String fqn) { return neighbors(METHOD, fqn, true, CALLS); }
    @Override public Set<String> callees(String fqn) { return neighbors(METHOD, fqn, false, CALLS); }

    @Override
    public Set<String> calledTargets(Set<String> candidateFqns) {
        if (candidateFqns.isEmpty()) return Set.of();
        int[] off = offsets[CALLS][1];
        if (off == null) return Set.of();
        Set<String> found = new LinkedHashSet<>();
        for (String fqn : candidateFqns) {
            int v = id(METHOD, fqn);
            if (v >= 0 && off[v + 1] > off[v]) found.add(fqn);
        }
        return found;
    }

    /** BFS over calls edges exactly like CallGraph: {@code from == to} gives {@code [from]}. */
    @Override
    public List<String> shortestPath(String fromFqn, String toFqn) {
        int from = id(METHOD, fromFqn), to = id(METHOD, toFqn);
        if (from < 0 || to < 0) return List.of();
        Map<Integer, Integer> prev = new HashMap<>();
        Deque<Integer> queue = new ArrayDeque<>();
        prev.put(from, -1);
        queue.add(from);
        outer:
        while (!queue.isEmpty()) {
            int current = queue.poll();
            if (current == to) break;
            int[] off = offsets[CALLS][0];
            if (off == null) break;
            int[] tgt = targets[CALLS][0];
            for (int i = off[current]; i < off[current + 1]; i++) {
                int callee = tgt[i];
                if (!prev.containsKey(callee)) {
                    prev.put(callee, current);
                    if (callee == to) break outer;
                    queue.add(callee);
                }
            }
        }
        if (!prev.containsKey(to)) return List.of();
        List<String> path = new ArrayList<>();
        for (int cur = to; cur != -1; cur = prev.get(cur)) path.add(0, fqns[cur]);
        return path;
    }

    @Override
    public Stream<String> allFqns() {
        return IntStream.range(0, fqns.length)
                .filter(i -> kinds[i] == METHOD && owned[i])
                .mapToObj(i -> fqns[i]);
    }

    @Override public long methodCount() { return methodCount; }
    @Override public long callCount() { return edgeCount[CALLS]; }

    @Override public Set<String> directSubclasses(String classFqn) {
        return neighbors(CODE_TYPE, classFqn, true, INHERITS, IMPLEMENTS);
    }
    @Override public Set<String> directSuperTypes(String classFqn) {
        return neighbors(CODE_TYPE, classFqn, false, INHERITS, IMPLEMENTS);
    }

    /** BFS over inherits + implements_iface in-edges; excludes the start, discovery order. */
    @Override
    public List<String> allSubclasses(String classFqn) {
        Set<String> visited = new LinkedHashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        queue.add(classFqn);
        visited.add(classFqn);
        while (!queue.isEmpty()) {
            String current = queue.poll();
            int v = id(CODE_TYPE, current);
            if (v < 0) continue;
            for (int t : new int[]{IMPLEMENTS, INHERITS}) {
                forEachNeighbor(v, t, true, w -> { if (visited.add(fqns[w])) queue.add(fqns[w]); });
            }
        }
        visited.remove(classFqn);
        return new ArrayList<>(visited);
    }

    @Override public Set<String> fieldReaders(String fieldFqn) { return neighbors(CODE_FIELD, fieldFqn, true, READS); }
    @Override public Set<String> fieldWriters(String fieldFqn) { return neighbors(CODE_FIELD, fieldFqn, true, WRITES); }
    @Override public Set<String> declaredFields(String classFqn) { return neighbors(CODE_TYPE, classFqn, false, DECLARES); }

    @Override
    public List<CallGraph.FieldInfo> getFieldsOf(String classFqn) {
        int[] ids = classFqn == null ? null : fieldsByOwner.get(classFqn);
        if (ids == null) return List.of();
        List<CallGraph.FieldInfo> result = new ArrayList<>(ids.length);
        for (int i : ids) result.add(new CallGraph.FieldInfo(fqns[i], fieldType[i], accessMod[i])); // vertex order; CallGraph's row order is index-incidental
        return result;
    }

    @Override public Set<String> annotatedWith(String annotationFqn) { return neighbors(CODE_TYPE, annotationFqn, true, ANNOTATED); }
    @Override public Set<String> methodsReturning(String typeFqn) { return neighbors(CODE_TYPE, typeFqn, true, RETURNS); }
    @Override public Set<String> methodsTaking(String typeFqn) { return neighbors(CODE_TYPE, typeFqn, true, TAKES); }

    @Override public void close() {}
}
