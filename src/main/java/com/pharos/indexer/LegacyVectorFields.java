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
        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leaf = ctx.reader();
            Bits live = leaf.getLiveDocs();
            if (leaf.getFieldInfos().fieldInfo(MISNAMED_VECTOR) != null) {
                FloatVectorValues v = leaf.getFloatVectorValues(MISNAMED_VECTOR);
                if (v != null) {
                    KnnVectorValues.DocIndexIterator it = v.iterator();
                    for (int d = it.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = it.nextDoc()) {
                        if (live == null || live.get(d)) return true;
                    }
                }
            }
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

    /** Read-only check of a project's Lucene dir; false when there is no index. */
    public static boolean hasLiveMisnamedVectors(Path luceneDir) {
        if (luceneDir == null || !Files.isDirectory(luceneDir)) return false;
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
