package com.pharos.graph;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * Exports a {@link CallGraph} to the immutable compact graph file read by {@link CompactGraph}.
 *
 * <p>Layout (inside one gzip stream, big-endian): magic {@code "PHGR"}, {@code int} version,
 * {@code int} vertex count, then per vertex a kind byte (0 Method, 1 CodeType, 2 CodeField) and the
 * strings fqn, classPrefix, ownerFqn, fieldType, accessMod ({@code int} byte length + UTF-8, or
 * {@code -1} for null); then, for each of {@link CallGraph#EDGE_TYPES} in order, an {@code int}
 * edge count followed by {@code int[] from} and {@code int[] to} (vertex indexes), sorted by
 * {@code from}; edges with equal {@code from} keep the order they were exported in.
 */
public final class CompactGraphWriter {
    static final String MAGIC = "PHGR";
    static final int VERSION = 1;
    static final String[] KINDS = {"Method", "CodeType", "CodeField"};

    private CompactGraphWriter() {}

    /**
     * Writes {@code source} to {@code target} (via a temp file, so readers never see a partial file).
     *
     * @throws IOException also when {@code source} has a damaged property dictionary
     *         ({@link CallGraph#dictionaryDamage()}): its records decode wrongly, and an export would
     *         spread that to every reader and to a writer that restores it
     */
    public static void export(CallGraph source, Path target) throws IOException {
        String damage = source.dictionaryDamage();
        if (damage != null) {
            throw new IOException("refusing to export a call graph with a " + damage + "; "
                    + CallGraph.repairAdvice(source.dbDir()));
        }
        List<CallGraph.VertexRecord> vertices = new ArrayList<>();
        List<Map<String, Integer>> idByKind = List.of(new HashMap<>(), new HashMap<>(), new HashMap<>());
        source.forEachVertex(v -> {
            int kind = kindOf(v.kind());
            if (idByKind.get(kind).putIfAbsent(v.fqn(), vertices.size()) == null) vertices.add(v);
        });

        int[][] from = new int[CallGraph.EDGE_TYPES.size()][];
        int[][] to = new int[from.length][];
        for (int t = 0; t < from.length; t++) {
            IntList f = new IntList(), tt = new IntList();
            source.forEachEdgeRecord(CallGraph.EDGE_TYPES.get(t), e -> {
                Integer a = idByKind.get(kindOf(e.fromKind())).get(e.fromFqn());
                Integer b = idByKind.get(kindOf(e.toKind())).get(e.toFqn());
                if (a != null && b != null) { f.add(a); tt.add(b); }
            });
            sortByFrom(f.toArray(), tt.toArray(), from, to, t);
        }

        Path parent = target.toAbsolutePath().getParent();
        if (parent != null) Files.createDirectories(parent);
        Path tmp = Files.createTempFile(parent, target.getFileName().toString(), ".tmp");
        try {
            try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                    new GZIPOutputStream(Files.newOutputStream(tmp), 1 << 16), 1 << 16))) {
                out.write(MAGIC.getBytes(StandardCharsets.US_ASCII));
                out.writeInt(VERSION);
                out.writeInt(vertices.size());
                for (CallGraph.VertexRecord v : vertices) {
                    out.writeByte(kindOf(v.kind()));
                    writeString(out, v.fqn());
                    writeString(out, v.classPrefix());
                    writeString(out, v.ownerFqn());
                    writeString(out, v.fieldType());
                    writeString(out, v.accessMod());
                }
                for (int t = 0; t < from.length; t++) {
                    out.writeInt(from[t].length);
                    for (int x : from[t]) out.writeInt(x);
                    for (int x : to[t]) out.writeInt(x);
                }
            }
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private static int kindOf(String kind) {
        for (int i = 0; i < KINDS.length; i++) if (KINDS[i].equals(kind)) return i;
        throw new IllegalArgumentException("unknown vertex kind: " + kind);
    }

    /** Stable sort of the parallel arrays by {@code from}. */
    private static void sortByFrom(int[] f, int[] t, int[][] fromOut, int[][] toOut, int slot) {
        long[] keys = new long[f.length];
        for (int i = 0; i < f.length; i++) keys[i] = ((long) f[i] << 32) | i;
        Arrays.sort(keys);
        int[] sf = new int[f.length], st = new int[f.length];
        for (int i = 0; i < keys.length; i++) {
            int idx = (int) keys[i];
            sf[i] = f[idx];
            st[i] = t[idx];
        }
        fromOut[slot] = sf;
        toOut[slot] = st;
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        if (s == null) { out.writeInt(-1); return; }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static final class IntList {
        private int[] a = new int[16];
        private int n;
        void add(int x) { if (n == a.length) a = Arrays.copyOf(a, n * 2); a[n++] = x; }
        int[] toArray() { return Arrays.copyOf(a, n); }
    }
}
