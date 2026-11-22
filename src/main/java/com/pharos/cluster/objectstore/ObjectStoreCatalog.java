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
import com.pharos.cluster.sharding.ProjectShards;
import com.pharos.cluster.sharding.ShardSpec;
import com.pharos.cluster.sharding.VersionedSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The cluster's shard list, {@code catalog/shards.json}: {@code {"shards":[{"id":"...","source":{...}}]}}, source optional,
 * plus the spec versions and the project ↔ shards mapping ({@code specs}, {@code activeVersion}, {@code projects}; see
 * {@link ProjectShards}). Updated by compare-and-set; unknown fields are preserved.
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

    /** The listed entries and the project mapping, read from one catalog version. */
    public record View(List<ShardEntry> entries, ProjectShards projects) {}

    /** {@link #entries()} and {@link #projectShards()} from a single read, so they always agree. */
    public View view() throws IOException {
        Optional<StoredObject> current = store.get(layout.catalogKey());
        if (current.isEmpty()) return new View(List.of(), ProjectShards.identity(List.of()));
        ObjectNode root = parse(current.get().data());
        List<ShardEntry> out = new ArrayList<>();
        for (JsonNode s : shardsArray(root)) {
            String id = s.path("id").asText();
            try {
                out.add(new ShardEntry(new ShardId(id), ShardSource.fromJson(s.get("source"))));
            } catch (IllegalArgumentException e) {
                log.warn("Catalog {} lists an invalid shard id '{}'; skipping it", layout.catalogKey(), id);
            }
        }
        return new View(List.copyOf(out), ProjectShards.fromCatalog(root, validIds(root), this::warn));
    }

    /**
     * The project ↔ shards mapping and spec versions; a catalog without {@code projects} gives the
     * {@code [project]} identity of its shards.
     */
    @Override
    public ProjectShards projectShards() throws IOException {
        Optional<StoredObject> current = store.get(layout.catalogKey());
        if (current.isEmpty()) return ProjectShards.identity(List.of());
        ObjectNode root = parse(current.get().data());
        return ProjectShards.fromCatalog(root, validIds(root), this::warn);
    }

    /**
     * Records a new spec version, for {@code project} or (empty) as a cluster default, and returns its
     * number: one above the highest so far. When the newest version of the same scope already is
     * {@code spec}, returns that version and writes nothing (a retried call does not add another).
     * Existing versions are never changed.
     */
    public int addSpec(Optional<String> project, ShardSpec spec) throws IOException {
        if (project.isPresent() && !ShardId.isProjectName(project.get())) {
            throw new IllegalArgumentException("'" + project.get() + "' is not a valid cluster project name");
        }
        int[] version = new int[1];
        update("add spec " + spec + project.map(p -> " for project " + p).orElse(""), root -> {
            ArrayNode specs = array(root, "specs");
            int max = 0;
            JsonNode newest = null;
            for (JsonNode s : specs) {
                int v = s.path("version").asInt(0);
                max = Math.max(max, v);
                String p = s.hasNonNull("project") ? s.get("project").asText() : null;
                if (java.util.Objects.equals(p, project.orElse(null))
                        && (newest == null || v > newest.path("version").asInt(0))) newest = s;
            }
            if (newest != null && spec.toString().equals(canonical(newest.path("spec").asText(null)))) {
                version[0] = newest.path("version").asInt();
                return false;
            }
            version[0] = max + 1;
            ObjectNode entry = specs.addObject();
            entry.put("version", version[0]);
            project.ifPresent(p -> entry.put("project", p));
            entry.put("spec", spec.toString());
            return true;
        });
        return version[0];
    }

    /** Makes cluster-default spec {@code version} the active default ({@code activeVersion}). */
    public void setActiveVersion(int version) throws IOException {
        update("set activeVersion " + version, root -> {
            VersionedSpec v = specVersion(root, version);
            if (v.project().isPresent()) {
                throw new IllegalArgumentException("spec version " + version + " belongs to project '"
                        + v.project().get() + "', not the cluster default");
            }
            if (root.path("activeVersion").asInt(-1) == version) return false;
            root.put("activeVersion", version);
            return true;
        });
    }

    /**
     * Sets {@code project}'s entry ({@code projects[]}) to spec {@code specVersion} with {@code shards},
     * replacing any previous entry in the same CAS (a reshard flip), and adds every shard to the
     * {@code shards} list if absent. Checked by {@link ProjectShards#checkProject}.
     */
    public void putProject(String project, int specVersion, List<ShardId> shards) throws IOException {
        update("put project " + project, root -> {
            VersionedSpec v = specVersion(root, specVersion);
            ProjectShards.checkProject(project, v, shards);
            ArrayNode projects = array(root, "projects");
            ObjectNode entry = null;
            for (JsonNode p : projects) {
                if (project.equals(p.path("name").asText(null)) && p instanceof ObjectNode o) { entry = o; break; }
            }
            ArrayNode ids = JSON.createArrayNode();
            shards.forEach(id -> ids.add(id.value()));
            if (entry != null && entry.path("spec").asInt(-1) == specVersion && ids.equals(entry.get("shards"))
                    && listsAll(root, shards)) {
                return false;
            }
            if (entry == null) entry = projects.addObject();
            entry.put("name", project);
            entry.put("spec", specVersion);
            entry.set("shards", ids);
            ArrayNode list = shardsArray(root);
            for (ShardId id : shards) {
                if (!listsAll(root, List.of(id))) list.addObject().put("id", id.value());
            }
            return true;
        });
    }

    private static boolean listsAll(ObjectNode root, List<ShardId> ids) {
        java.util.Set<String> listed = new java.util.HashSet<>();
        for (JsonNode s : shardsArray(root)) listed.add(s.path("id").asText());
        return ids.stream().allMatch(id -> listed.contains(id.value()));
    }

    private VersionedSpec specVersion(ObjectNode root, int version) {
        for (VersionedSpec v : ProjectShards.specsOf(root, this::warn)) {
            if (v.version() == version) return v;
        }
        throw new IllegalArgumentException("catalog " + layout.catalogKey() + " has no spec version " + version);
    }

    private static String canonical(String spec) {
        try {
            return ShardSpec.parse(spec).toString();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private List<ShardId> validIds(ObjectNode root) {
        List<ShardId> out = new ArrayList<>();
        for (JsonNode s : shardsArray(root)) {
            String id = s.path("id").asText();
            if (ShardId.isValid(id)) out.add(new ShardId(id));
        }
        return out;
    }

    private void warn(String message) {
        log.warn("Catalog {}: {}", layout.catalogKey(), message);
    }

    /** A catalog change: returns false when nothing needs writing; may throw IllegalArgumentException. */
    @FunctionalInterface
    private interface Mutation {
        boolean apply(ObjectNode root) throws IOException;
    }

    /** CAS read-modify-write of the catalog, as {@link #register(ShardId, Optional)}. */
    private void update(String what, Mutation mutation) throws IOException {
        String key = layout.catalogKey();
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            if (attempt > 1) backoff(attempt);
            Optional<StoredObject> current = store.get(key);
            ObjectNode root = current.isPresent() ? parse(current.get().data()) : JSON.createObjectNode();
            if (!mutation.apply(root)) return;
            byte[] data = JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(root);
            Optional<String> etag = current.isPresent()
                    ? store.putIfMatch(key, data, current.get().etag())
                    : store.putIfAbsent(key, data);
            if (etag.isPresent()) return;
        }
        throw new PublishConflictException("catalog " + key + " kept changing; could not " + what
                + " after " + MAX_ATTEMPTS + " attempts");
    }

    private static ArrayNode array(ObjectNode root, String field) {
        JsonNode node = root.get(field);
        if (node instanceof ArrayNode arr) return arr;
        return root.putArray(field);
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

    /** Sleeps a random time in [d/2, d] with d = base * 2^(attempt-2). */
    private void backoff(int attempt) throws IOException {
        long d = BASE_BACKOFF_MS << (attempt - 2);
        long ms = d / 2 + ThreadLocalRandom.current().nextLong(d / 2 + 1);
        try {
            sleeper.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while registering in catalog " + layout.catalogKey());
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
        return array(root, "shards");
    }
}
