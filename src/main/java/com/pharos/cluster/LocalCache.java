package com.pharos.cluster;

import java.nio.file.Path;

/** Decides where the shard directory (lucene/ and graph) a node serves lives on local disk. */
public interface LocalCache {
    Path shardDir(String project);
}
