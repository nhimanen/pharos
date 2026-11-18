package com.pharos.cluster;

/** Test access to package-private runtime parts from other test packages. */
public final class ClusterTestAccess {
    private ClusterTestAccess() {}

    /**
     * Marks {@code shard}'s served generation as garbage-collected on the bucket, exactly as a lazy
     * reader's block read that found an object missing does (the shard becomes degraded).
     */
    public static void markServedGenerationMissing(ClusterRuntime runtime, String shard) {
        long gen = runtime.served().get(new ShardId(shard)).value();
        com.pharos.cluster.warm.WarmTestAccess.objectMissing(runtime.lazyGenerations(), shard, gen,
                "test/" + shard + "/gen-" + gen + "/gone");
    }
}
