package com.pharos.indexer;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Opens the Lucene {@link Directory} of a project's {@code lucene/} dir. {@link LuceneIndexer} opens
 * every directory through one factory, so where the bytes live (local files, or a reader node's
 * remote generation read through a block cache) is decided here and nowhere in indexing or search.
 */
@FunctionalInterface
public interface LuceneDirectoryFactory {

    /**
     * Lucene's platform default ({@code MMapDirectory} on 64-bit JVMs): index files are read through
     * the OS page cache instead of being copied into per-input heap buffers.
     */
    LuceneDirectoryFactory DEFAULT = FSDirectory::open;

    Directory open(Path luceneDir) throws IOException;

    /**
     * True when {@code luceneDir} holds a commit. Called before every reader lookup, so a factory
     * whose directories are costly to build should answer it without opening one.
     */
    default boolean indexExists(Path luceneDir) throws IOException {
        try (Directory dir = open(luceneDir)) {
            return org.apache.lucene.index.DirectoryReader.indexExists(dir);
        }
    }
}
