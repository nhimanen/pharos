package com.example.cache;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * In-memory key-value cache with TTL-based expiry and a get-or-compute fallback.
 */
public class CacheStore {

    private final Map<String, Entry> store = new HashMap<>();

    /**
     * Retrieves a cached value by key.
     * Returns null if the key is absent or the TTL has elapsed.
     *
     * @param key the cache key
     * @return the cached value, or null on miss
     */
    public Object getValue(String key) {
        Entry e = store.get(key);
        if (e == null || e.isExpired()) return null;
        return e.value;
    }

    /**
     * Stores a value under the given key with an optional time-to-live.
     * Overwrites any existing entry for that key.
     *
     * @param key   the cache key
     * @param value the value to cache
     * @param ttl   retention duration; null means the entry never expires
     */
    public void setValue(String key, Object value, Duration ttl) {
        store.put(key, new Entry(value, ttl == null ? null : Instant.now().plus(ttl)));
    }

    /**
     * Removes all entries whose keys start with the given prefix.
     * Useful for invalidating a logical group of related cached values.
     *
     * @param prefix the key prefix to match for eviction
     */
    public void evictByPrefix(String prefix) {
        store.keySet().removeIf(k -> k.startsWith(prefix));
    }

    /**
     * Removes all entries from the cache regardless of their TTL.
     * Flushes the entire cache back to an empty state.
     */
    public void clearAll() {
        store.clear();
    }

    /**
     * Returns the cached value for the key, computing and storing it if absent or expired.
     * This is the standard cache-aside (lazy-loading) pattern: the loader is called exactly
     * once on a cache miss and the result is stored for subsequent calls.
     *
     * @param key    the cache key
     * @param loader supplier invoked on cache miss to compute the value
     * @return the cached or freshly computed value
     */
    public Object getOrCompute(String key, Supplier<Object> loader) {
        Object cached = getValue(key);
        if (cached != null) return cached;
        Object computed = loader.get();
        setValue(key, computed, Duration.ofMinutes(10));
        return computed;
    }

    // ── stubs ─────────────────────────────────────────────────────────────────

    private record Entry(Object value, Instant expiresAt) {
        boolean isExpired() {
            return expiresAt != null && Instant.now().isAfter(expiresAt);
        }
    }
}
