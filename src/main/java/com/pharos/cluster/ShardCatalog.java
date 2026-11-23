package com.pharos.cluster;

import java.io.IOException;
import java.util.List;

/** The cluster's list of published shards. */
public interface ShardCatalog {
    List<ShardId> shards() throws IOException;
}
