package com.pharos.cluster.objectstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.PublishException;
import com.pharos.cluster.ShardCatalog;
import com.pharos.cluster.ShardEntry;
import com.pharos.cluster.ShardSource;
import com.pharos.cluster.ShardId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The cluster's shard list, {@code catalog/shards.json}: {@code {"shards":[{"id":"...","source":{...}}]}}, source optional.
 * Updated by compare-and-set; unknown fields are preserved.
 */
public final class ObjectStoreCatalog implements ShardCatalog {

    private static final Logger log = LoggerFactory.getLogger(ObjectStoreCatalog.class);

    static final int MAX_ATTEMPTS = 6;
    /** First back-off between CAS attempts; doubles per attempt (at most ~1.5 s in total). */
    static final long BASE_BACKOFF_MS = 50;
    private static final ObjectMapper JSON = new ObjectMapper();

    /** Sleeps between CAS attempts; replaced in tests. */
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final ObjectStore store;
    private final ObjectStoreLayout layout;
    private final Sleeper sleeper;

    public ObjectStoreCatalog(ObjectStore store, ObjectStoreLayout layout) {
        this(store, layout, Thread::sleep);
    }

    ObjectStoreCatalog(ObjectStore store, ObjectStoreLayout layout, Sleeper sleeper) {
        this.store = store;
        this.layout = layout;
        this.sleeper = sleeper;
    }

    /** The listed shards; an entry whose id is not a valid shard id is skipped with a warning. */
    @Override
    public List<ShardId> shards() throws IOException {
        Optional<StoredObject> current = store.get(layout.catalogKey());
        if (current.isEmpty()) return List.of();
        List<ShardId> out = new ArrayList<>();
        for (JsonNode s : shardsArray(parse(current.get().data()))) {
            String id = s.path("id").asText();
            try {
                out.add(new ShardId(id));
            } catch (IllegalArgumentException e) {
                log.warn("Catalog {} lists an invalid shard id '{}'; skipping it", layout.catalogKey(), id);
            }
        }
        return List.copyOf(out);
    }

    /**
     * Empty only when {@code catalog/shards.json} does not exist; a catalog that exists and lists no
     * shard is authoritative (its last project was removed), so readers stop serving everything.
     */
    @Override
    public Optional<List<ShardId>> authoritativeShards() throws IOException {
        if (store.get(layout.catalogKey()).isEmpty()) return Optional.empty();
        return Optional.of(shards());
    }

    /** The listed shards with their sources; the same entries, in the same order, as {@link #shards()}. */
    public List<ShardEntry> entries() throws IOException {
        Optional<StoredObject> current = store.get(layout.catalogKey());
        if (current.isEmpty()) return List.of();
        List<ShardEntry> out = new ArrayList<>();
        for (JsonNode s : shardsArray(parse(current.get().data()))) {
            String id = s.path("id").asText();
            try {
                out.add(new ShardEntry(new ShardId(id), ShardSource.fromJson(s.get("source"))));
            } catch (IllegalArgumentException e) {
                log.warn("Catalog {} lists an invalid shard id '{}'; skipping it", layout.catalogKey(), id);
            }
        }
        return List.copyOf(out);
    }

    /** Adds {@code shard} if absent, keeping any {@code source} it already has. */
    public void register(ShardId shard) throws IOException {
        register(shard, Optional.empty());
    }

    /**
     * Adds {@code shard} if absent and, when {@code source} is present, sets or replaces its source;
     * an empty {@code source} leaves an existing one alone (an older writer that does not know sources
     * cannot erase it). Nothing is written when nothing changes. CAS read-modify-write, up to {@value #MAX_ATTEMPTS} attempts
     * with jittered exponential back-off between them. Idempotent.
     */
    public void register(ShardId shard, Optional<ShardSource> source) throws IOException {
        String key = layout.catalogKey();
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            if (attempt > 1) backoff(attempt);
            Optional<StoredObject> current = store.get(key);
            ObjectNode root = current.isPresent() ? parse(current.get().data()) : JSON.createObjectNode();
            ArrayNode shards = shardsArray(root);
            ObjectNode entry = null;
            for (JsonNode s : shards) {
                if (shard.value().equals(s.path("id").asText()) && s instanceof ObjectNode o) { entry = o; break; }
            }
            if (entry == null) {
                entry = shards.addObject();
                entry.put("id", shard.value());
            } else if (source.isEmpty() || ShardSource.fromJson(entry.get("source")).equals(source)) {
                return;
            }
            if (source.isPresent()) source.get().writeTo(entry.putObject("source"));
            byte[] data = JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(root);
            Optional<String> etag = current.isPresent()
                    ? store.putIfMatch(key, data, current.get().etag())
                    : store.putIfAbsent(key, data);
            if (etag.isPresent()) return;
        }
        throw new PublishConflictException("catalog " + key + " kept changing; could not register shard "
                + shard + " after " + MAX_ATTEMPTS + " attempts");
    }

    /**
     * Removes {@code shard}'s entry (with its source), so work-queue writers release it and readers stop
     * serving it. Nothing else is touched: the shard's generations and writer state stay in the store,
     * and registering it again resumes from them. CAS read-modify-write like {@link #register}. Idempotent.
     *
     * @return true when an entry was removed, false when the catalog did not list the shard
     */
    public boolean unregister(ShardId shard) throws IOException {
        String key = layout.catalogKey();
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            if (attempt > 1) backoff(attempt);
            Optional<StoredObject> current = store.get(key);
            if (current.isEmpty()) return false;
            ObjectNode root = parse(current.get().data());
            ArrayNode shards = shardsArray(root);
            boolean found = false;
            for (int i = shards.size() - 1; i >= 0; i--) {
                if (shard.value().equals(shards.get(i).path("id").asText())) {
                    shards.remove(i);
                    found = true;
                }
            }
            if (!found) return false;
            byte[] data = JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(root);
            if (store.putIfMatch(key, data, current.get().etag()).isPresent()) return true;
        }
        throw new PublishConflictException("catalog " + key + " kept changing; could not unregister shard "
                + shard + " after " + MAX_ATTEMPTS + " attempts");
    }

    /** Sleeps a random time in [d/2, d] with d = base * 2^(attempt-2). */
    private void backoff(int attempt) throws IOException {
        long d = BASE_BACKOFF_MS << (attempt - 2);
        long ms = d / 2 + ThreadLocalRandom.current().nextLong(d / 2 + 1);
        try {
            sleeper.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while updating catalog " + layout.catalogKey());
        }
    }

    private ObjectNode parse(byte[] data) throws PublishException {
        try {
            JsonNode node = JSON.readTree(data);
            if (node instanceof ObjectNode obj) return obj;
        } catch (IOException e) {
            throw new PublishException("unreadable catalog " + layout.catalogKey(), e);
        }
        throw new PublishException("catalog " + layout.catalogKey() + " is not a JSON object");
    }

    private static ArrayNode shardsArray(ObjectNode root) {
        JsonNode shards = root.get("shards");
        if (shards instanceof ArrayNode arr) return arr;
        return root.putArray("shards");
    }
}
