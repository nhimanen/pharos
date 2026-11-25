package com.pharos.indexer;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Detects documents carrying the misnamed per-model fields {@code vec.legacy} / {@code chunkVec.legacy}
 * that pharos 1.4.0-1.4.1 wrote for the {@code legacy} model while search reads {@code vectorEmbedding} /
 * {@code chunkVectors}. Only <em>live</em> documents count: a deleted doc keeps its field in the
 * segment's FieldInfos until a merge, so FieldInfos alone would re-flag a migrated index.
 */
public final class LegacyVectorFields {
    private LegacyVectorFields() {}

    public static final String MISNAMED_VECTOR = DocumentMapper.vectorFieldName(com.pharos.config.IndexConfig.LEGACY_MODEL_ID);
    public static final String MISNAMED_CHUNKS = DocumentMapper.chunkVectorFieldName(com.pharos.config.IndexConfig.LEGACY_MODEL_ID);

    public static boolean hasLiveMisnamedVectors(IndexReader reader) throws IOException {
        if (hasLiveVectors(reader, MISNAMED_VECTOR)) return true;
        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leaf = ctx.reader();
            Bits live = leaf.getLiveDocs();
            if (leaf.getFieldInfos().fieldInfo(MISNAMED_CHUNKS) != null) {
                BinaryDocValues b = leaf.getBinaryDocValues(MISNAMED_CHUNKS);
                if (b != null) {
                    for (int d = b.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = b.nextDoc()) {
                        if (live == null || live.get(d)) return true;
                    }
                }
            }
        }
        return false;
    }

    /** True when at least one live document carries a KNN vector in {@code field}. */
    public static boolean hasLiveVectors(IndexReader reader, String field) throws IOException {
        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leaf = ctx.reader();
            if (leaf.getFieldInfos().fieldInfo(field) == null) continue;
            FloatVectorValues v = leaf.getFloatVectorValues(field);
            if (v == null) continue;
            Bits live = leaf.getLiveDocs();
            KnnVectorValues.DocIndexIterator it = v.iterator();
            for (int d = it.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = it.nextDoc()) {
                if (live == null || live.get(d)) return true;
            }
        }
        return false;
    }

    /**
     * True when {@code luceneDir} belongs to a lazy reader generation ({@code cluster.reader.mode lazy}):
     * only its eager Lucene files are local and the rest is read from the bucket through the served
     * reader, so a local open would fail (or, at best, report a misleading "no vectors"). The
     * Path-based probes below skip such dirs and answer false; callers that print an answer should
     * check this first and report "unknown on a lazy reader".
     */
    public static boolean isLazyGeneration(Path luceneDir) {
        return luceneDir != null && com.pharos.cluster.warm.LazyGenerations.isLazy(luceneDir.getParent());
    }

    /**
     * Read-only: does the project's Lucene dir hold live vectors in {@code field}? False when there
     * is no index or it cannot be read. This, not the registry's {@code embeddedModels}, is the
     * evidence that a project can answer vector search for a model.
     */
    public static boolean hasLiveVectors(Path luceneDir, String field) {
        if (luceneDir == null || !Files.isDirectory(luceneDir) || isLazyGeneration(luceneDir)) return false;
        try (NIOFSDirectory dir = new NIOFSDirectory(luceneDir);
             DirectoryReader r = DirectoryReader.indexExists(dir) ? DirectoryReader.open(dir) : null) {
            return r != null && hasLiveVectors(r, field);
        } catch (IOException | RuntimeException e) {
            org.slf4j.LoggerFactory.getLogger(LegacyVectorFields.class)
                    .warn("Could not check {} for vectors: {}", luceneDir, e.toString());
            return false;
        }
    }

    /** Read-only check of a project's Lucene dir; false when there is no index. */
    public static boolean hasLiveMisnamedVectors(Path luceneDir) {
        // A lazy generation is a reader's: readers never re-embed, so false is the right answer there.
        if (luceneDir == null || !Files.isDirectory(luceneDir) || isLazyGeneration(luceneDir)) return false;
        try (NIOFSDirectory dir = new NIOFSDirectory(luceneDir);
             DirectoryReader r = DirectoryReader.indexExists(dir) ? DirectoryReader.open(dir) : null) {
            return r != null && hasLiveMisnamedVectors(r);
        } catch (IOException | RuntimeException e) {
            org.slf4j.LoggerFactory.getLogger(LegacyVectorFields.class)
                    .warn("Could not check {} for legacy-named vectors: {}", luceneDir, e.toString());
            return false;
        }
    }
}
