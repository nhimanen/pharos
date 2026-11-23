package com.pharos.cli;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.IndexFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.NIOFSDirectory;

import java.nio.file.Files;
import java.nio.file.Path;

/** A tiny real project index (stamped, with a Lucene dir) carrying the given vector fields, or none. */
final class VectorIndexFixture {
    private VectorIndexFixture() {}

    /** Registers {@code name} with root {@code base/src-name} and index {@code base/indexes/name}. */
    static ProjectMeta register(ProjectRegistry registry, Path base, String name, String... vectorFields)
            throws Exception {
        Path root = Files.createDirectories(base.resolve("src-" + name));
        Path idx = Files.createDirectories(base.resolve("indexes").resolve(name));
        IndexFormat.write(idx);
        try (NIOFSDirectory dir = new NIOFSDirectory(Files.createDirectories(idx.resolve("lucene")));
             IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            Document d = new Document();
            d.add(new StringField("id", name + ":X", Field.Store.YES));
            for (String f : vectorFields) {
                d.add(new KnnFloatVectorField(f, new float[]{1, 0, 0, 0}, VectorSimilarityFunction.COSINE));
            }
            w.addDocument(d);
            w.commit();
        }
        ProjectMeta m = new ProjectMeta(name, root.toString(), idx.toString());
        registry.register(m);
        return m;
    }
}
