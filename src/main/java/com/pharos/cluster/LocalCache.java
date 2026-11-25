package com.pharos.cluster;

import java.nio.file.Path;
import java.util.Optional;

/** Decides where the shard directory (lucene/ and graph) a node serves lives on local disk. */
public interface LocalCache {
    Path shardDir(String project);

    /**
     * Ruling P6-R19: the directory of generation {@code generation} of {@code project} when this node still
     * answers it although it activated another one (a reader's retained previous generation); empty
     * otherwise. The active generation is {@link #shardDir}.
     */
    default Optional<Path> retainedDir(String project, long generation) {
        return Optional.empty();
    }
}
