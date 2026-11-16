package com.pharos.cluster.objectstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.Generation;
import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardId;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads published generations from an object store. Everything read back is validated before it
 * is returned: {@code LATEST} must name {@code gen-N.json} with {@code N} equal to its
 * {@code generation}, and manifests must pass {@link ManifestValidation#check}.
 */
public final class ObjectStoreReplicaSource implements ReplicaSource {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern MANIFEST_NAME = Pattern.compile("gen-(\\d+)\\.json");

    private final ObjectStore store;
    private final ObjectStoreLayout layout;

    public ObjectStoreReplicaSource(ObjectStore store, ObjectStoreLayout layout) {
        this.store = Objects.requireNonNull(store, "store");
        this.layout = Objects.requireNonNull(layout, "layout");
    }

    @Override
    public Optional<Generation> latest(ShardId shard) throws IOException {
        String key = layout.latestKey(shard);
        Optional<StoredObject> obj = store.get(key);
        if (obj.isEmpty()) return Optional.empty();
        JsonNode node;
        try {
            node = JSON.readTree(obj.get().data());
        } catch (IOException e) {
            throw new IOException("unreadable " + key + ": " + e.getMessage(), e);
        }
        JsonNode g = node == null ? null : node.get("generation");
        JsonNode m = node == null ? null : node.get("manifest");
        if (g == null || !g.isIntegralNumber() || !g.canConvertToLong() || g.asLong() <= 0) {
            throw new IOException(key + " has no valid generation");
        }
        if (m == null || !m.isTextual()) throw new IOException(key + " has no manifest name");
        Matcher mm = MANIFEST_NAME.matcher(m.asText());
        if (!mm.matches()) throw new IOException(key + " names an invalid manifest '" + m.asText() + "'");
        long named;
        try {
            named = Long.parseLong(mm.group(1));
        } catch (NumberFormatException e) {
            throw new IOException(key + " names an invalid manifest '" + m.asText() + "'");
        }
        if (named != g.asLong()) {
            throw new IOException(key + " names manifest " + m.asText() + " for generation " + g.asLong());
        }
        return Optional.of(new Generation(g.asLong()));
    }

    @Override
    public ShardManifest manifest(ShardId shard, Generation gen) throws IOException {
        String key = layout.manifestKey(shard, gen);
        StoredObject obj = store.get(key).orElseThrow(() -> new IOException("missing manifest " + key));
        ShardManifest manifest;
        try {
            manifest = ShardManifest.read(obj.data());
        } catch (IOException e) {
            throw new IOException("unparseable manifest " + key + ": " + e.getMessage(), e);
        }
        ManifestValidation.check(shard, gen, manifest, layout, key);
        return manifest;
    }

    @Override
    public void download(ManifestFile file, Path target) throws IOException {
        store.download(file.key(), target);
    }

    @Override
    public Optional<byte[]> synonyms() throws IOException {
        return store.get(layout.synonymsKey()).map(StoredObject::data);
    }
}
