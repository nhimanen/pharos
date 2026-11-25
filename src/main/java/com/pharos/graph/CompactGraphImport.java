package com.pharos.graph;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/**
 * Loads a compact graph file ({@link CompactGraphWriter}) back into a fresh ArcadeDB
 * {@link CallGraph}: the inverse of {@link CompactGraphWriter#export}. The compact file carries
 * every vertex with all its properties and every edge of every {@link CallGraph#EDGE_TYPES} type,
 * so a writer that takes over a shard rebuilds its patchable graph from the published generation
 * instead of re-parsing the project.
 */
public final class CompactGraphImport {

    private CompactGraphImport() {}

    /**
     * Creates the ArcadeDB graph at {@code dbDir} (which must not exist yet) from {@code compactFile}.
     *
     * @throws IOException when the file is not a readable compact graph, or {@code dbDir} exists
     */
    public static void importInto(Path compactFile, Path dbDir) throws IOException {
        if (Files.exists(dbDir)) throw new IOException("graph dir " + dbDir + " already exists");
        List<CallGraph.VertexRecord> vertices = new ArrayList<>();
        int types = CallGraph.EDGE_TYPES.size();
        int[][] from = new int[types][];
        int[][] to = new int[types][];
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(
                new GZIPInputStream(Files.newInputStream(compactFile), 1 << 16), 1 << 16))) {
            byte[] magic = new byte[4];
            in.readFully(magic);
            if (!new String(magic, StandardCharsets.US_ASCII).equals(CompactGraphWriter.MAGIC)) {
                throw new IOException("not a pharos compact graph: bad magic");
            }
            int version = in.readInt();
            if (version != CompactGraphWriter.VERSION) throw new IOException("unsupported compact graph version " + version);
            int n = in.readInt();
            if (n < 0 || n > CompactGraph.MAX_COUNT) throw new IOException("corrupt compact graph: vertex count " + n);
            for (int i = 0; i < n; i++) {
                int kind = in.readByte();
                if (kind < 0 || kind >= CompactGraphWriter.KINDS.length) {
                    throw new IOException("corrupt compact graph: bad vertex kind " + kind);
                }
                String fqn = CompactGraph.readString(in);
                if (fqn == null) throw new IOException("corrupt compact graph: vertex without fqn");
                vertices.add(new CallGraph.VertexRecord(CompactGraphWriter.KINDS[kind], fqn,
                        CompactGraph.readString(in), CompactGraph.readString(in),
                        CompactGraph.readString(in), CompactGraph.readString(in)));
            }
            for (int t = 0; t < types; t++) {
                int m = in.readInt();
                if (m < 0 || m > CompactGraph.MAX_COUNT) throw new IOException("corrupt compact graph: edge count " + m);
                from[t] = readIds(in, m, n);
                to[t] = readIds(in, m, n);
            }
        } catch (ZipException | EOFException e) {
            throw new IOException("not a readable pharos compact graph: " + compactFile + ": " + e.getMessage(), e);
        }
        try (CallGraph graph = CallGraph.open(dbDir)) {
            graph.load(vertices, from, to);
        }
    }

    private static int[] readIds(DataInputStream in, int m, int n) throws IOException {
        int[] a = new int[Math.min(m, 1 << 16)];
        for (int i = 0; i < m; i++) {
            if (i == a.length) a = java.util.Arrays.copyOf(a, (int) Math.min(m, (long) a.length * 2));
            int id = in.readInt();
            if (id < 0 || id >= n) throw new IOException("corrupt compact graph: vertex index " + id);
            a[i] = id;
        }
        return java.util.Arrays.copyOf(a, m);
    }
}
