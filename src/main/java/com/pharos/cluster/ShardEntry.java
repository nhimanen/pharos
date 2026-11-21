package com.pharos.cluster;

import java.util.Optional;

/** A catalog entry: the shard and, when registered with one, where writers get its source. */
public record ShardEntry(ShardId id, Optional<ShardSource> source) {
    public ShardEntry {
        source = source == null ? Optional.empty() : source;
    }
}
