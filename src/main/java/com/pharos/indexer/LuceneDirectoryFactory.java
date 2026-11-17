package com.pharos.indexer;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;

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
     * System property that swaps the default for {@link NIOFSDirectory} when set to {@code niofs}.
     * Diagnostics only: the Phase 2 gate compares a lazy reader bitwise against a full copy read
     * through positional reads, since vector scores on mmap inputs can differ in the last ulp.
     */
    String DIRECTORY_PROPERTY = "pharos.lucene.directory";

    /**
     * Lucene's platform default ({@code MMapDirectory} on 64-bit JVMs): index files are read through
     * the OS page cache instead of being copied into per-input heap buffers. With
     * {@code -Dpharos.lucene.directory=niofs}, a {@link NIOFSDirectory} instead.
     */
    LuceneDirectoryFactory DEFAULT = dir -> "niofs".equalsIgnoreCase(System.getProperty(DIRECTORY_PROPERTY))
            ? new NIOFSDirectory(dir) : FSDirectory.open(dir);

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
