package com.pharos.cluster;

import com.pharos.config.IndexConfig;
import java.nio.file.Path;

/** Single-node cache: shards live directly under the configured index directory. */
public class LocalFsCache implements LocalCache {
    private final IndexConfig config;

    public LocalFsCache(IndexConfig config) {
        this.config = config;
    }

    @Override
    public Path shardDir(String project) {
        return config.getIndexDir().resolve(project);
    }
}
