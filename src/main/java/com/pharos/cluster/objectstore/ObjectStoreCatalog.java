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

    // ------------------------------------------------------------------------- resharding (Task 4)

    /**
     * Requests a reshard of {@code project} to {@code spec} ({@code pharos reshard}): appends a new spec
     * version (always: a reshard is a new version even when the text equals an older one, for example to
     * add the shard of a new module under the same spec) and records {@code projects[].reshard}. A project
     * on the implicit {@code [project]} gets its entry. Idempotent: an open request for the same spec
     * returns its version and writes nothing.
     *
     * @throws IllegalStateException when another reshard of the project is open (requested or staged and
     *         not cancelled) for another spec: one reshard of a project at a time
     */
    public int requestReshard(String project, ShardSpec spec, java.time.Instant now) throws IOException {
        if (!ShardId.isProjectName(project)) {
            throw new IllegalArgumentException("'" + project + "' is not a valid cluster project name");
        }
        int[] version = new int[1];
        update("request a reshard of project " + project, root -> {
            ObjectNode entry = projectEntry(root, project, true);
            String target = spec.toString();
            JsonNode rq = entry.get("reshard");
            if (rq != null && rq.isObject()) {
                VersionedSpec open = specVersion(root, rq.path("spec").asInt(-1));
                if (open.spec().toString().equals(target)) {
                    version[0] = open.version();
                    return false;
                }
                throw new IllegalStateException("a reshard of project " + project + " to " + open.spec()
                        + " (spec version " + open.version() + ") is in progress; cancel it first (pharos reshard "
                        + project + " --cancel)");
            }
            JsonNode st = entry.get("staged");
            if (st != null && st.isObject() && !st.hasNonNull("abandonedAt")) {
                throw new IllegalStateException("project " + project + " has a staged spec version "
                        + st.path("spec").asInt() + " that was not cancelled; cancel it first (pharos reshard "
                        + project + " --cancel)");
            }
            ArrayNode specs = array(root, "specs");
            int max = 0;
            for (JsonNode s : specs) max = Math.max(max, s.path("version").asInt(0));
            version[0] = max + 1;
            ObjectNode v = specs.addObject();
            v.put("version", version[0]);
            v.put("project", project);
            v.put("spec", target);
            ObjectNode req = entry.putObject("reshard");
            req.put("spec", version[0]);
            req.put("requestedAt", now.toString());
            return true;
        });
        return version[0];
    }

    /**
     * Cancels the open reshard of {@code project}: drops the request and marks a staged version that was
     * not flipped {@code abandonedAt} (its shards are GC'd after the retention window). Returns false when
     * there was nothing to cancel.
     */
    public boolean cancelReshard(String project, java.time.Instant now) throws IOException {
        boolean[] cancelled = {false};
        update("cancel the reshard of project " + project, root -> {
            ObjectNode entry = projectEntry(root, project, false);
            if (entry == null) return false;
            boolean changed = entry.remove("reshard") != null;
            JsonNode st = entry.get("staged");
            if (st instanceof ObjectNode o && !o.hasNonNull("abandonedAt")) {
                o.put("abandonedAt", now.toString());
                changed = true;
            }
            cancelled[0] = changed;
            return changed;
        });
        return cancelled[0];
    }

    /**
     * Writes {@code project}'s staged version: spec {@code version} with {@code shards}, staged from the
     * active spec version {@code from} (empty: the implicit {@code [project]}). The request must still
     * name {@code version}. Idempotent for the same version and shards; a staged version of another spec
     * (cancelled) moves to {@code abandoned}.
     *
     * @throws PublishConflictException when the request changed or the active version is no longer
     *         {@code from}
     */
    public void stageReshard(String project, int version, java.util.OptionalInt from, List<ShardId> shards,
                             java.time.Instant now) throws IOException {
        update("stage spec version " + version + " of project " + project, root -> {
            ObjectNode entry = requireRequest(root, project, version);
            checkFrom(entry, project, from);
            ProjectShards.checkProject(project, specVersion(root, version), shards);
            ArrayNode ids = idArray(shards);
            JsonNode st = entry.get("staged");
            if (st instanceof ObjectNode o && o.path("spec").asInt(-1) == version && ids.equals(o.get("shards"))
                    && !o.hasNonNull("abandonedAt") && listsAll(root, shards)) {
                return false;
            }
            if (st instanceof ObjectNode o) abandon(entry, o.path("spec"), o.path("shards"), now);
            ObjectNode staged = entry.putObject("staged");
            staged.put("spec", version);
            staged.set("shards", ids);
            if (from.isPresent()) staged.put("from", from.getAsInt());
            list(root, shards);
            return true;
        });
    }

    /**
     * Records the generation of every staged shard ({@code staged.generations}): the version is complete
     * and readers may sync it. In the same CAS, pins the active version at {@code activeGenerations} (what
     * it serves now): from here on a query reads the active version at exactly those generations, so a
     * coordinator still routing it after the flip (an older catalog read) can never read a shard the flip
     * rewrote at its new generation. Idempotent.
     */
    public void pinStaged(String project, int version, java.util.Map<ShardId, Long> generations,
                          java.util.Map<ShardId, Long> activeGenerations) throws IOException {
        update("pin staged spec version " + version + " of project " + project, root -> {
            ObjectNode entry = requireRequest(root, project, version);
            ObjectNode st = staged(entry, project, version);
            ObjectNode gens = generationsNode(generations);
            List<ShardId> shards = new ArrayList<>();
            for (JsonNode s : st.path("shards")) shards.add(new ShardId(s.asText()));
            if (!generations.keySet().equals(new java.util.HashSet<>(shards))) {
                throw new IllegalArgumentException("generations " + generations.keySet() + " are not the staged shards "
                        + shards);
            }
            List<ShardId> active = new ArrayList<>();
            for (JsonNode s : entry.path("shards")) active.add(new ShardId(s.asText()));
            if (!activeGenerations.keySet().equals(new java.util.HashSet<>(active))) {
                throw new IllegalArgumentException("generations " + activeGenerations.keySet()
                        + " are not the active shards " + active);
            }
            ObjectNode activeGens = generationsNode(activeGenerations);
            if (sameGenerations(gens, st.get("generations")) && sameGenerations(activeGens, entry.get("generations"))) {
                return false;
            }
            st.set("generations", gens);
            entry.set("generations", activeGens);
            return true;
        });
    }

    /**
     * Withdraws the generations of {@code project}'s staged version {@code version} (it was built from an
     * active version that has changed since): readers stop preparing it, and the job builds it again.
     */
    public void unpinStaged(String project, int version) throws IOException {
        update("unpin staged spec version " + version + " of project " + project, root -> {
            ObjectNode entry = projectEntry(root, project, false);
            JsonNode st = entry == null ? null : entry.get("staged");
            if (!(st instanceof ObjectNode o) || o.path("spec").asInt(-1) != version || !o.has("generations")) {
                return false;
            }
            o.remove("generations");
            return true;
        });
    }

    /**
     * The reshard flip, one CAS: {@code project}'s entry becomes its staged version (spec, shards, and its
     * generations as the active pins); the version it replaces becomes {@code previous} with
     * {@code previousGenerations} and {@code retiredAt = now} (an older {@code previous} moves to
     * {@code abandoned}); the staged entry and the request are removed.
     *
     * @throws PublishConflictException when the staged version is not {@code version}, is not pinned, or
     *         the active version changed since it was staged
     */
    public void flipReshard(String project, int version, java.util.Map<ShardId, Long> previousGenerations,
                            java.time.Instant now) throws IOException {
        update("flip project " + project + " to spec version " + version, root -> {
            ObjectNode entry = projectEntry(root, project, false);
            if (entry == null) throw new PublishConflictException("project " + project + " has no catalog entry");
            if (entry.path("spec").asInt(-1) == version && !entry.has("staged")) return false; // flipped already
            ObjectNode st = staged(entry, project, version);
            if (!st.hasNonNull("generations")) {
                throw new PublishConflictException("staged spec version " + version + " of project " + project
                        + " is not complete (no generations)");
            }
            java.util.OptionalInt from = st.hasNonNull("from") ? java.util.OptionalInt.of(st.get("from").asInt())
                    : java.util.OptionalInt.empty();
            checkFrom(entry, project, from);
            // the active version must still be exactly what the staged one was built from (pinStaged pinned
            // it): an index run that published meanwhile dropped or moved the pins, and the staged version
            // would serve its moved files twice
            if (!sameGenerations(generationsNode(previousGenerations), entry.get("generations"))) {
                throw new PublishConflictException("project " + project + " changed since spec version " + version
                        + " was staged (its active generations are " + entry.get("generations") + ", the staged version"
                        + " was built from " + generationsNode(previousGenerations) + "); it is built again");
            }
            ObjectNode prev = JSON.createObjectNode();
            if (entry.hasNonNull("spec")) prev.put("spec", entry.get("spec").asInt());
            prev.set("shards", entry.path("shards").deepCopy());
            prev.set("generations", generationsNode(previousGenerations));
            prev.put("retiredAt", now.toString());
            JsonNode older = entry.get("previous");
            if (older != null && older.isObject()) abandon(entry, older.path("spec"), older.path("shards"), now);
            entry.put("spec", version);
            entry.set("shards", st.get("shards").deepCopy());
            entry.set("generations", st.get("generations").deepCopy());
            entry.set("previous", prev);
            entry.remove("staged");
            entry.remove("reshard");
            return true;
        });
    }

    /**
     * Drops the active pins of {@code project} once an index run published all of {@code shards} (its
     * active shards): readers then follow {@code LATEST} again. No-op when there are none, or when the
     * project's shards are no longer {@code shards}.
     */
    public void clearPins(String project, List<ShardId> shards) throws IOException {
        update("clear the pins of project " + project, root -> {
            ObjectNode entry = projectEntry(root, project, false);
            if (entry == null || !entry.has("generations") || !idArray(shards).equals(entry.get("shards"))) return false;
            entry.remove("generations");
            return true;
        });
    }

    /**
     * After the retention window: drops {@code project}'s previous version (retired at {@code retiredAt})
     * and unlists its shards that no version names any more. Returns the shards unlisted.
     */
    public List<ShardId> dropPrevious(String project, java.time.Instant retiredAt) throws IOException {
        List<ShardId> gone = new ArrayList<>();
        update("drop the previous version of project " + project, root -> {
            gone.clear();
            ObjectNode entry = projectEntry(root, project, false);
            JsonNode prev = entry == null ? null : entry.get("previous");
            if (prev == null || !prev.isObject() || !retiredAt.toString().equals(prev.path("retiredAt").asText())) {
                return false;
            }
            entry.remove("previous");
            gone.addAll(unlistUnreferenced(root, prev.path("shards")));
            return true;
        });
        return List.copyOf(gone);
    }

    /**
     * After the retention window: drops an abandoned version of {@code project} (its {@code since}), and
     * a cancelled staged version ({@code abandonedAt} = {@code since}), unlisting the shards no version
     * names any more. Returns the shards unlisted.
     */
    public List<ShardId> dropAbandoned(String project, java.time.Instant since) throws IOException {
        List<ShardId> gone = new ArrayList<>();
        update("drop an abandoned version of project " + project, root -> {
            gone.clear();
            ObjectNode entry = projectEntry(root, project, false);
            if (entry == null) return false;
            List<JsonNode> dropped = new ArrayList<>();
            JsonNode st = entry.get("staged");
            if (st instanceof ObjectNode o && since.toString().equals(o.path("abandonedAt").asText(null))) {
                dropped.add(o.path("shards"));
                entry.remove("staged");
            }
            JsonNode list = entry.get("abandoned");
            if (list instanceof ArrayNode arr) {
                for (int i = arr.size() - 1; i >= 0; i--) {
                    if (since.toString().equals(arr.get(i).path("since").asText(null))) {
                        dropped.add(arr.get(i).path("shards"));
                        arr.remove(i);
                    }
                }
                if (arr.isEmpty()) entry.remove("abandoned");
            }
            if (dropped.isEmpty()) return false;
            for (JsonNode ids : dropped) gone.addAll(unlistUnreferenced(root, ids));
            return true;
        });
        return List.copyOf(gone);
    }

    /** The entry of {@code project}; with {@code create}, an implicit {@code [project]} project gets one. */
    private static ObjectNode projectEntry(ObjectNode root, String project, boolean create) {
        ArrayNode projects = array(root, "projects");
        for (JsonNode p : projects) {
            if (project.equals(p.path("name").asText(null)) && p instanceof ObjectNode o) return o;
        }
        if (!create) return null;
        ObjectNode entry = projects.addObject();
        entry.put("name", project);
        entry.putArray("shards").add(ShardId.ofProject(project).value());
        list(root, List.of(ShardId.ofProject(project)));
        return entry;
    }

    private ObjectNode requireRequest(ObjectNode root, String project, int version) throws PublishConflictException {
        ObjectNode entry = projectEntry(root, project, false);
        if (entry == null || entry.path("reshard").path("spec").asInt(-1) != version) {
            throw new PublishConflictException("project " + project + " has no open reshard request to spec version "
                    + version + " (cancelled or superseded)");
        }
        return entry;
    }

    private static void checkFrom(ObjectNode entry, String project, java.util.OptionalInt from)
            throws PublishConflictException {
        java.util.OptionalInt active = entry.hasNonNull("spec") ? java.util.OptionalInt.of(entry.get("spec").asInt())
                : java.util.OptionalInt.empty();
        if (!active.equals(from)) {
            throw new PublishConflictException("project " + project + " is at spec version " + active
                    + ", not " + from + ", since its reshard was staged");
        }
    }

    private static ObjectNode staged(ObjectNode entry, String project, int version) throws PublishConflictException {
        JsonNode st = entry.get("staged");
        if (!(st instanceof ObjectNode o) || o.path("spec").asInt(-1) != version || o.hasNonNull("abandonedAt")) {
            throw new PublishConflictException("project " + project + " has no staged spec version " + version);
        }
        return o;
    }

    private static void abandon(ObjectNode entry, JsonNode spec, JsonNode shards, java.time.Instant now) {
        ObjectNode a = array(entry, "abandoned").addObject();
        if (spec != null && spec.canConvertToInt()) a.put("spec", spec.asInt());
        a.set("shards", shards == null ? JSON.createArrayNode() : shards.deepCopy());
        a.put("since", now.toString());
    }

    /** Two {@code {"<shard>": generation}} objects name the same generations (number types aside). */
    private static boolean sameGenerations(JsonNode a, JsonNode b) {
        if (a == null || b == null || !a.isObject() || !b.isObject() || a.size() != b.size()) return false;
        var it = a.fields();
        while (it.hasNext()) {
            var e = it.next();
            JsonNode o = b.get(e.getKey());
            if (o == null || !o.canConvertToLong() || o.asLong() != e.getValue().asLong()) return false;
        }
        return true;
    }

    private static ArrayNode idArray(List<ShardId> shards) {
        ArrayNode ids = JSON.createArrayNode();
        shards.forEach(id -> ids.add(id.value()));
        return ids;
    }

    private static ObjectNode generationsNode(java.util.Map<ShardId, Long> generations) {
        ObjectNode out = JSON.createObjectNode();
        new java.util.TreeMap<>(java.util.Map.copyOf(generations).entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(e -> e.getKey().value(), java.util.Map.Entry::getValue)))
                .forEach(out::put);
        return out;
    }

    private static void list(ObjectNode root, List<ShardId> shards) {
        ArrayNode list = shardsArray(root);
        for (ShardId id : shards) {
            if (!listsAll(root, List.of(id))) list.addObject().put("id", id.value());
        }
    }

    /**
     * Removes from {@code shards[]} those of {@code candidates} that no project entry names any more (its
     * shards, staged, previous or abandoned versions). Returns the removed ids.
     */
    private static List<ShardId> unlistUnreferenced(ObjectNode root, JsonNode candidates) {
        java.util.Set<String> named = new java.util.HashSet<>();
        for (JsonNode p : array(root, "projects")) {
            for (JsonNode s : p.path("shards")) named.add(s.asText());
            for (JsonNode s : p.path("staged").path("shards")) named.add(s.asText());
            for (JsonNode s : p.path("previous").path("shards")) named.add(s.asText());
            for (JsonNode a : p.path("abandoned")) for (JsonNode s : a.path("shards")) named.add(s.asText());
        }
        java.util.Set<String> drop = new java.util.HashSet<>();
        for (JsonNode c : candidates) if (!named.contains(c.asText())) drop.add(c.asText());
        List<ShardId> out = new ArrayList<>();
        ArrayNode list = shardsArray(root);
        for (int i = list.size() - 1; i >= 0; i--) {
            String id = list.get(i).path("id").asText();
            if (drop.contains(id)) {
                list.remove(i);
                if (ShardId.isValid(id)) out.add(new ShardId(id));
            }
        }
        return out;
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
