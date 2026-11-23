package com.pharos.cluster.warm;

/**
 * Snapshot of {@link BlockCache} counters.
 *
 * @param hits           acquires served by a cached block
 * @param misses         acquires that had to wait for a fetch (including waiters sharing one fetch)
 * @param bytesFetched   bytes fetched from the object store
 * @param evictions      blocks evicted to stay under the cap
 * @param bytesOnDisk    bytes of cached block files (leased blocks included)
 * @param leasesOpen     leases not yet closed
 * @param writeFailures  fetched blocks that could not be written to the cache (served uncached)
 * @param thrashWarnings rate-limited WARNs logged because the working set exceeds the cap
 */
public record WarmStats(long hits, long misses, long bytesFetched, long evictions, long bytesOnDisk,
                        long leasesOpen, long writeFailures, long thrashWarnings) {

    /** Hits over all acquires; 1.0 before the first acquire. */
    public double hitRate() {
        long total = hits + misses;
        return total == 0 ? 1.0 : (double) hits / total;
    }
}
