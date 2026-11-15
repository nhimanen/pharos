package com.pharos.cluster.objectstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.PublishException;
import com.pharos.cluster.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The cluster's shard list, {@code catalog/shards.json}: {@code {"shards":[{"id":"..."}]}}.
 * Updated by compare-and-set; unknown fields are preserved.
 */
public final class ObjectStoreCatalog {

    static final int MAX_ATTEMPTS = 5;
    private static final ObjectMapper JSON = new ObjectMapper();

    private final ObjectStore store;
    private final ObjectStoreLayout layout;

    public ObjectStoreCatalog(ObjectStore store, ObjectStoreLayout layout) {
        this.store = store;
        this.layout = layout;
    }

    public List<ShardId> shards() throws IOException {
        Optional<StoredObject> current = store.get(layout.catalogKey());
        if (current.isEmpty()) return List.of();
        List<ShardId> out = new ArrayList<>();
        for (JsonNode s : shardsArray(parse(current.get().data()))) {
            out.add(new ShardId(s.path("id").asText()));
        }
        return List.copyOf(out);
    }

    /** Adds {@code shard} if absent (CAS read-modify-write, up to {@value #MAX_ATTEMPTS} attempts). */
    public void register(ShardId shard) throws IOException {
        String key = layout.catalogKey();
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            Optional<StoredObject> current = store.get(key);
            ObjectNode root = current.isPresent() ? parse(current.get().data()) : JSON.createObjectNode();
            ArrayNode shards = shardsArray(root);
            for (JsonNode s : shards) {
                if (shard.value().equals(s.path("id").asText())) return;
            }
            shards.addObject().put("id", shard.value());
            byte[] data = JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(root);
            Optional<String> etag = current.isPresent()
                    ? store.putIfMatch(key, data, current.get().etag())
                    : store.putIfAbsent(key, data);
            if (etag.isPresent()) return;
        }
        throw new PublishConflictException("catalog " + key + " kept changing; could not register shard "
                + shard + " after " + MAX_ATTEMPTS + " attempts");
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
