package com.pharos.cluster.warm;

import com.pharos.cluster.ShardId;

/** Test access to package-private parts of the lazy reader. */
public final class WarmTestAccess {
    private WarmTestAccess() {}

    public static void objectMissing(LazyGenerations lazy, String shard, long gen, String key) {
        lazy.objectMissing(new ShardId(shard), gen, key);
    }
}
