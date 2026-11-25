package com.pharos.cluster.sharding;

import com.fasterxml.jackson.databind.JsonNode;
import com.pharos.cluster.ShardId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * The project ↔ shards mapping of the catalog: which shards serve a project, and which project a
 * shard belongs to. <b>This is the only place that answers "the project of a shard"</b>; nothing may
 * derive it from the shard id string (a guard test enforces it).
 *
 * <p><b>Catalog model</b> ({@code catalog/shards.json}, all fields optional and preserved by writers
 * that don't know them):
 * <pre>{@code
 * {
 *   "shards":   [{"id": "lucene~ring=s01", "source": {...}}, ...],
 *   "specs":    [{"version": 1, "spec": "[project]"},
 *                {"version": 2, "project": "lucene", "spec": "[project, ring(path, shards=8)]"}],
 *   "activeVersion": 1,
 *   "projects": [{"name": "lucene", "spec": 2, "shards": ["lucene~ring=s01", ...]}]
 * }
 * }</pre>
 * <ul>
 *   <li>{@code specs}: every spec version ever written, numbered across the catalog, never edited
 *       ({@link VersionedSpec});</li>
 *   <li>{@code activeVersion}: the active cluster-default spec version (one without a project); absent
 *       means {@code [project]};</li>
 *   <li>{@code projects}: a project's active spec version and its shards. A project's spec changes
 *       (a reshard flip) by replacing its entry in one CAS.</li>
 *   <li>{@code projects[].staged}: optional {@code {"spec": N, "shards": [...]}}, the shards of a spec
 *       version that is about to become the project's active one (a reshard in progress). Readers sync
 *       them ahead ({@link #syncShards()}); nothing routes to them until the flip. Its shards may
 *       overlap the active ones (a ring change keeps the shards that neither gain nor lose files).
 *       Once every staged shard is published (Task 4) it carries {@code "generations": {"<id>": N}}: the
 *       exact generation of each shard the version is made of ({@link #stagedOf}); {@code abandonedAt}
 *       marks a cancelled one, which nothing syncs any more.</li>
 *   <li>{@code projects[].generations}: after a reshard flip, the generation of each active shard the
 *       flipped version is made of (pins). Readers sync exactly these, and a query reads exactly these,
 *       until the project's next index run publishes all its shards and drops them ({@link #pinsOf}).</li>
 *   <li>{@code projects[].previous}: {@code {"spec": N, "shards": [...], "generations": {...},
 *       "retiredAt": "..."}}, the version a flip retired, kept for the retention window: readers keep
 *       serving its shards, and a query falls back to it while the readers have not activated the new
 *       version yet ({@link #previousOf}). GC then deletes the shards only it had.</li>
 *   <li>{@code projects[].reshard}: {@code {"spec": N, "requestedAt": "..."}}, a requested reshard to spec
 *       version N ({@code pharos reshard}); the writer of the project runs it ({@link #reshardRequestOf}).</li>
 *   <li>{@code projects[].abandoned}: staged or retired versions waiting for GC
 *       ({@code [{"spec": N, "shards": [...], "since": "..."}]}).</li>
 * </ul>
 * <b>Backward compatibility.</b> A shard listed in {@code shards} but under no project entry, whose id
 * is a plain project name, is that project's only shard: the {@code [project]} identity of Phases 1–5.
 * A catalog without {@code projects} is therefore read exactly as before. A keyed id
 * ({@code p~key=value}) under no project entry, or a plain one whose name is now a split project, is an
 * {@link #orphans() orphan}: listed, but no project's shard.
 */
public final class ProjectShards {

    private final Map<String, List<ShardId>> byProject;
    private final Map<ShardId, String> byShard;
    private final Map<String, ShardSpec> specOf;
    private final Map<String, Integer> specVersionOf;
    private final List<VersionedSpec> specs;
    private final OptionalInt activeVersion;
    private final List<ShardId> orphans;
    private final List<ShardId> listed;
    private final Map<String, Staged> staged;
    private final Map<String, Map<ShardId, Long>> pins;
    private final Map<String, Previous> previous;
    private final Map<String, ReshardRequest> requests;
    private final Map<String, List<Abandoned>> abandoned;

    /**
     * The staged spec version of a project (a reshard in progress): its number, its spec and its shards,
     * in catalog order. Synced ahead by readers, never routed to before the flip.
     */
    public record Staged(int specVersion, ShardSpec spec, List<ShardId> shards, Map<ShardId, Long> generations,
                         Optional<java.time.Instant> abandonedAt) {
        public Staged {
            shards = List.copyOf(shards);
            generations = Map.copyOf(generations);
            abandonedAt = abandonedAt == null ? Optional.empty() : abandonedAt;
        }

        public Staged(int specVersion, ShardSpec spec, List<ShardId> shards) {
            this(specVersion, spec, shards, Map.of(), Optional.empty());
        }

        /** True once every shard of the version is published: {@link #generations} names each one's. */
        public boolean pinned() {
            return !generations.isEmpty();
        }

        public boolean abandoned() {
            return abandonedAt.isPresent();
        }
    }

    /**
     * The version a reshard flip retired ({@code projects[].previous}): its spec version (empty for the
     * implicit {@code [project]}), spec, shards, the generation of each, and when it was retired.
     */
    public record Previous(OptionalInt specVersion, ShardSpec spec, List<ShardId> shards,
                           Map<ShardId, Long> generations, java.time.Instant retiredAt) {
        public Previous {
            shards = List.copyOf(shards);
            generations = Map.copyOf(generations);
        }
    }

    /** A requested reshard ({@code projects[].reshard}): the target spec version and when it was asked for. */
    public record ReshardRequest(int specVersion, ShardSpec spec, java.time.Instant requestedAt) {}

    /** A staged or retired version waiting for GC ({@code projects[].abandoned}). */
    public record Abandoned(OptionalInt specVersion, List<ShardId> shards, java.time.Instant since) {
        public Abandoned {
            shards = List.copyOf(shards);
        }
    }

    private ProjectShards(Map<String, List<ShardId>> byProject, Map<String, ShardSpec> specOf,
                          Map<String, Integer> specVersionOf, List<VersionedSpec> specs,
                          OptionalInt activeVersion, List<ShardId> orphans, List<ShardId> listed,
                          Map<String, Staged> staged, Map<String, Map<ShardId, Long>> pins,
                          Map<String, Previous> previous, Map<String, ReshardRequest> requests,
                          Map<String, List<Abandoned>> abandoned) {
        TreeMap<String, List<ShardId>> p = new TreeMap<>();
        Map<ShardId, String> s = new HashMap<>();
        previous.forEach((name, pr) -> {
            for (ShardId id : pr.shards()) s.put(id, name);
        });
        staged.forEach((name, st) -> {
            for (ShardId id : st.shards()) s.put(id, name);
        });
        byProject.forEach((name, shards) -> {
            p.put(name, List.copyOf(shards));
            for (ShardId id : shards) s.put(id, name);
        });
        this.byProject = Collections.unmodifiableMap(p);
        this.byShard = Map.copyOf(s);
        this.listed = List.copyOf(new LinkedHashSet<>(listed));
        this.staged = Map.copyOf(staged);
        this.pins = Map.copyOf(pins);
        this.previous = Map.copyOf(previous);
        this.requests = Map.copyOf(requests);
        this.abandoned = Map.copyOf(abandoned);
        this.specOf = Map.copyOf(specOf);
        this.specVersionOf = Map.copyOf(specVersionOf);
        this.specs = List.copyOf(specs);
        this.activeVersion = activeVersion;
        this.orphans = List.copyOf(orphans);
    }

    /** The {@code [project]} mapping of {@code shards}: each plain id is its own project (peer mode, tests). */
    public static ProjectShards identity(Collection<ShardId> shards) {
        Map<String, List<ShardId>> byProject = new LinkedHashMap<>();
        List<ShardId> orphans = new ArrayList<>();
        for (ShardId id : new LinkedHashSet<>(shards)) {
            if (isPlain(id)) byProject.put(id.value(), List.of(id));
            else orphans.add(id);
        }
        return new ProjectShards(byProject, Map.of(), Map.of(), List.of(), OptionalInt.empty(), orphans,
                List.copyOf(shards), Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }

    /**
     * Reads the mapping from a catalog document ({@link ProjectShards class comment}). Entries that
     * break the model (an unknown spec version, a shard claimed twice, an id that doesn't belong to its
     * project) are skipped and reported to {@code warn}; their shards become orphans.
     *
     * @param shards the valid ids of the catalog's {@code shards} list, in order
     */
    public static ProjectShards fromCatalog(JsonNode root, List<ShardId> shards, Consumer<String> warn) {
        List<VersionedSpec> specs = readSpecs(root.get("specs"), warn);
        Map<Integer, VersionedSpec> byVersion = new HashMap<>();
        for (VersionedSpec v : specs) byVersion.put(v.version(), v);

        OptionalInt active = OptionalInt.empty();
        JsonNode av = root.get("activeVersion");
        if (av != null && !av.isNull()) {
            VersionedSpec v = av.canConvertToInt() ? byVersion.get(av.asInt()) : null;
            if (v == null || v.project().isPresent()) {
                warn.accept("activeVersion " + av + " is not a cluster-default spec version; using [project]");
            } else {
                active = OptionalInt.of(v.version());
            }
        }

        Map<String, List<ShardId>> byProject = new LinkedHashMap<>();
        Map<String, ShardSpec> specOf = new HashMap<>();
        Map<String, Integer> specVersionOf = new HashMap<>();
        Set<ShardId> claimed = new java.util.HashSet<>();
        Map<String, Staged> staged = new HashMap<>();
        Map<String, JsonNode> stagedNodes = new LinkedHashMap<>();
        Map<String, JsonNode> entryNodes = new LinkedHashMap<>();
        JsonNode projects = root.get("projects");
        if (projects != null && projects.isArray()) {
            for (JsonNode p : projects) {
                String name = p.path("name").asText(null);
                try {
                    if (name == null || byProject.containsKey(name)) {
                        throw new IllegalArgumentException(name == null ? "a project entry has no name" : "listed twice");
                    }
                    VersionedSpec spec = null;
                    JsonNode sv = p.get("spec");
                    if (sv != null && !sv.isNull()) {
                        spec = sv.canConvertToInt() ? byVersion.get(sv.asInt()) : null;
                        if (spec == null) throw new IllegalArgumentException("unknown spec version " + sv);
                    }
                    List<ShardId> ids = new ArrayList<>();
                    for (JsonNode s : p.path("shards")) ids.add(new ShardId(s.asText()));
                    checkProject(name, spec, ids);
                    for (ShardId id : ids) {
                        if (claimed.contains(id)) throw new IllegalArgumentException("shard " + id + " is claimed by another project");
                    }
                    claimed.addAll(ids);
                    byProject.put(name, ids);
                    specOf.put(name, spec == null ? ShardSpec.PROJECT : spec.spec());
                    if (spec != null) specVersionOf.put(name, spec.version());
                    JsonNode st = p.get("staged");
                    if (st != null && !st.isNull()) stagedNodes.put(name, st);
                    entryNodes.put(name, p);
                } catch (IllegalArgumentException e) {
                    warn.accept("catalog project entry '" + name + "' skipped: " + e.getMessage());
                }
            }
        }

        // Staged and retired versions, once every active claim is known: their shards may be active
        // shards of the same project (a ring change keeps unaffected shards), never another project's.
        Set<ShardId> stagedClaimed = new java.util.HashSet<>();
        stagedNodes.forEach((name, st) -> {
            try {
                JsonNode sv = st.get("spec");
                VersionedSpec spec = sv != null && sv.canConvertToInt() ? byVersion.get(sv.asInt()) : null;
                if (spec == null) throw new IllegalArgumentException("unknown spec version " + sv);
                List<ShardId> ids = new ArrayList<>();
                for (JsonNode s : st.path("shards")) ids.add(new ShardId(s.asText()));
                checkProject(name, spec, ids);
                checkOwn(name, ids, claimed, byProject, stagedClaimed);
                Map<ShardId, Long> gens = readGenerations(st.get("generations"), ids);
                Optional<java.time.Instant> abandonedAt = instant(st.get("abandonedAt"));
                stagedClaimed.addAll(ids);
                staged.put(name, new Staged(spec.version(), spec.spec(), ids, gens, abandonedAt));
            } catch (IllegalArgumentException e) {
                warn.accept("catalog project entry '" + name + "': staged version skipped: " + e.getMessage());
            }
        });
        Map<String, Map<ShardId, Long>> pins = new HashMap<>();
        Map<String, Previous> previous = new HashMap<>();
        Map<String, ReshardRequest> requests = new HashMap<>();
        Map<String, List<Abandoned>> abandoned = new HashMap<>();
        entryNodes.forEach((name, p) -> {
            try {
                if (p.hasNonNull("generations")) pins.put(name, readGenerations(p.get("generations"), byProject.get(name)));
            } catch (IllegalArgumentException e) {
                warn.accept("catalog project entry '" + name + "': generations ignored: " + e.getMessage());
            }
            JsonNode pr = p.get("previous");
            if (pr != null && !pr.isNull()) {
                try {
                    VersionedSpec spec = specOrDefault(pr.get("spec"), byVersion);
                    List<ShardId> ids = new ArrayList<>();
                    for (JsonNode s : pr.path("shards")) ids.add(new ShardId(s.asText()));
                    checkProject(name, spec, ids);
                    checkOwn(name, ids, claimed, byProject, java.util.Set.of());
                    for (ShardId id : ids) {
                        if (stagedClaimed.contains(id) && !staged.containsKey(name)) {
                            throw new IllegalArgumentException("shard " + id + " is claimed by another project");
                        }
                    }
                    java.time.Instant at = instant(pr.get("retiredAt"))
                            .orElseThrow(() -> new IllegalArgumentException("no retiredAt"));
                    previous.put(name, new Previous(spec == null ? OptionalInt.empty() : OptionalInt.of(spec.version()),
                            spec == null ? ShardSpec.PROJECT : spec.spec(), ids,
                            readGenerations(pr.get("generations"), ids), at));
                    stagedClaimed.addAll(ids);
                } catch (IllegalArgumentException e) {
                    warn.accept("catalog project entry '" + name + "': previous version skipped: " + e.getMessage());
                }
            }
            JsonNode rq = p.get("reshard");
            if (rq != null && !rq.isNull()) {
                JsonNode sv = rq.get("spec");
                VersionedSpec spec = sv != null && sv.canConvertToInt() ? byVersion.get(sv.asInt()) : null;
                if (spec == null) {
                    warn.accept("catalog project entry '" + name + "': reshard request skipped: unknown spec version " + sv);
                } else {
                    requests.put(name, new ReshardRequest(spec.version(), spec.spec(),
                            instant(rq.get("requestedAt")).orElse(java.time.Instant.EPOCH)));
                }
            }
            List<Abandoned> gone = new ArrayList<>();
            for (JsonNode a : p.path("abandoned")) {
                try {
                    List<ShardId> ids = new ArrayList<>();
                    for (JsonNode s : a.path("shards")) ids.add(new ShardId(s.asText()));
                    for (ShardId id : ids) {
                        String owner = null;
                        for (Map.Entry<String, List<ShardId>> e : byProject.entrySet()) {
                            if (e.getValue().contains(id)) owner = e.getKey();
                        }
                        if (owner != null && !owner.equals(name)) {
                            throw new IllegalArgumentException("shard " + id + " is claimed by another project");
                        }
                    }
                    JsonNode sv = a.get("spec");
                    gone.add(new Abandoned(sv != null && sv.canConvertToInt() ? OptionalInt.of(sv.asInt()) : OptionalInt.empty(),
                            ids, instant(a.get("since")).orElse(java.time.Instant.EPOCH)));
                } catch (IllegalArgumentException e) {
                    warn.accept("catalog project entry '" + name + "': abandoned version skipped: " + e.getMessage());
                }
            }
            if (!gone.isEmpty()) abandoned.put(name, gone);
        });
        Set<ShardId> abandonedIds = new java.util.HashSet<>();
        abandoned.values().forEach(l -> l.forEach(a -> abandonedIds.addAll(a.shards())));

        List<ShardId> orphans = new ArrayList<>();
        for (ShardId id : new LinkedHashSet<>(shards)) {
            if (claimed.contains(id) || stagedClaimed.contains(id)) continue;
            if (isPlain(id) && !byProject.containsKey(id.value()) && !abandonedIds.contains(id)) {
                byProject.put(id.value(), List.of(id));
                claimed.add(id);
            } else {
                orphans.add(id);
            }
        }
        return new ProjectShards(byProject, specOf, specVersionOf, specs, active, orphans, shards, staged, pins,
                previous, requests, abandoned);
    }

    /** A staged or retired version's shards: this project's own (active or new), never another's. */
    private static void checkOwn(String name, List<ShardId> ids, Set<ShardId> claimed,
                                 Map<String, List<ShardId>> byProject, Set<ShardId> alsoClaimed) {
        List<ShardId> own = byProject.getOrDefault(name, List.of());
        for (ShardId id : ids) {
            if ((claimed.contains(id) && !own.contains(id)) || alsoClaimed.contains(id)) {
                throw new IllegalArgumentException("shard " + id + " is claimed by another project");
            }
        }
    }

    /** A spec version node, or null (the implicit {@code [project]}) when absent. */
    private static VersionedSpec specOrDefault(JsonNode sv, Map<Integer, VersionedSpec> byVersion) {
        if (sv == null || sv.isNull()) return null;
        VersionedSpec spec = sv.canConvertToInt() ? byVersion.get(sv.asInt()) : null;
        if (spec == null) throw new IllegalArgumentException("unknown spec version " + sv);
        return spec;
    }

    /**
     * A {@code {"<shard id>": generation}} object; empty when absent. When present it must name exactly
     * {@code shards}, each with a generation of at least 1.
     */
    static Map<ShardId, Long> readGenerations(JsonNode node, List<ShardId> shards) {
        if (node == null || node.isNull()) return Map.of();
        if (!node.isObject()) throw new IllegalArgumentException("generations is not an object");
        Map<ShardId, Long> out = new LinkedHashMap<>();
        var it = node.fields();
        while (it.hasNext()) {
            var e = it.next();
            ShardId id = new ShardId(e.getKey());
            if (!e.getValue().canConvertToLong() || e.getValue().asLong() < 1) {
                throw new IllegalArgumentException("generation of " + id + " is not a positive number");
            }
            out.put(id, e.getValue().asLong());
        }
        if (!out.keySet().equals(new java.util.HashSet<>(shards))) {
            throw new IllegalArgumentException("generations name " + out.keySet() + ", the shards are " + shards);
        }
        return out;
    }

    private static Optional<java.time.Instant> instant(JsonNode node) {
        if (node == null || node.isNull() || !node.isTextual()) return Optional.empty();
        try {
            return Optional.of(java.time.Instant.parse(node.asText()));
        } catch (java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException("unreadable time '" + node.asText() + "'");
        }
    }

    /**
     * Checks one project entry: a valid project name, a spec version of this project or a cluster
     * default, and shards that are the project's ids under that spec ({@code [project]}: exactly the
     * project name; otherwise at least one, each {@code <name>~…}, no duplicates).
     *
     * @param spec null for {@code [project]} without a recorded version
     * @throws IllegalArgumentException naming the problem
     */
    public static void checkProject(String name, VersionedSpec spec, List<ShardId> shards) {
        if (!ShardId.isProjectName(name)) {
            throw new IllegalArgumentException("'" + name + "' is not a valid cluster project name");
        }
        if (spec != null && spec.project().isPresent() && !spec.project().get().equals(name)) {
            throw new IllegalArgumentException("spec version " + spec.version() + " belongs to project '"
                    + spec.project().get() + "'");
        }
        if (new LinkedHashSet<>(shards).size() != shards.size()) {
            throw new IllegalArgumentException("a shard is listed twice");
        }
        boolean isDefault = spec == null || spec.spec().isDefault();
        if (isDefault) {
            if (!shards.equals(List.of(ShardId.ofProject(name)))) {
                throw new IllegalArgumentException("spec [project] has exactly one shard, '" + name + "'");
            }
            return;
        }
        if (shards.isEmpty()) throw new IllegalArgumentException("no shards");
        List<ShardSpec.Field> fields = spec.spec().fields();
        Set<String> ringNames = spec.spec().ring().map(r -> Set.copyOf(r.names())).orElse(Set.of());
        for (ShardId id : shards) {
            String[] parts = id.value().split("~", -1);
            boolean ok = parts[0].equals(name) && parts.length == fields.size();
            for (int i = 1; ok && i < parts.length; i++) {
                int eq = parts[i].indexOf('=');
                ShardSpec.Field f = fields.get(i);
                ok = parts[i].substring(0, eq).equals(f.key())
                        && (!(f instanceof ShardSpec.RingField) || ringNames.contains(parts[i].substring(eq + 1)));
            }
            if (!ok) {
                throw new IllegalArgumentException("shard " + id + " is not a shard id of project '" + name
                        + "' under spec " + spec.spec());
            }
        }
    }

    /** The valid spec versions of a catalog document; invalid entries are reported to {@code warn}. */
    public static List<VersionedSpec> specsOf(JsonNode root, Consumer<String> warn) {
        return readSpecs(root.get("specs"), warn);
    }

    private static List<VersionedSpec> readSpecs(JsonNode node, Consumer<String> warn) {
        List<VersionedSpec> out = new ArrayList<>();
        if (node == null || !node.isArray()) return out;
        Set<Integer> seen = new java.util.HashSet<>();
        for (JsonNode s : node) {
            try {
                if (!s.path("version").canConvertToInt()) throw new IllegalArgumentException("no version");
                int version = s.path("version").asInt();
                if (!seen.add(version)) throw new IllegalArgumentException("version " + version + " listed twice");
                String project = s.hasNonNull("project") ? s.get("project").asText() : null;
                if (project != null && !ShardId.isProjectName(project)) {
                    throw new IllegalArgumentException("invalid project '" + project + "'");
                }
                out.add(new VersionedSpec(version, Optional.ofNullable(project), ShardSpec.parse(s.path("spec").asText(null))));
            } catch (IllegalArgumentException e) {
                warn.accept("catalog spec entry " + s + " skipped: " + e.getMessage());
            }
        }
        return out;
    }

    /** A plain project id, the shard of a project under {@code [project]}. */
    private static boolean isPlain(ShardId id) {
        return id.value().indexOf('~') < 0;
    }

    /** The project of {@code shard}, or empty when it belongs to none (unknown, or an orphan). */
    public Optional<String> projectOf(ShardId shard) {
        return Optional.ofNullable(byShard.get(shard));
    }

    /** The shards of {@code project} under its active spec, in catalog order; empty when it is unknown. */
    public List<ShardId> shardsOf(String project) {
        return byProject.getOrDefault(project, List.of());
    }

    /** Every project, sorted. */
    public SortedSet<String> projects() {
        return Collections.unmodifiableSortedSet(new TreeSet<>(byProject.keySet()));
    }

    /** Every shard of every project. */
    public List<ShardId> shards() {
        List<ShardId> out = new ArrayList<>();
        byProject.values().forEach(out::addAll);
        return List.copyOf(out);
    }

    /** The active spec of {@code project}: {@code [project]} unless its entry names another. */
    public ShardSpec specOf(String project) {
        return specOf.getOrDefault(project, ShardSpec.PROJECT);
    }

    /** The spec version {@code project}'s entry names; empty for a project on the implicit {@code [project]}. */
    public OptionalInt specVersionOf(String project) {
        Integer v = specVersionOf.get(project);
        return v == null ? OptionalInt.empty() : OptionalInt.of(v);
    }

    /** Every spec version of the catalog, in catalog order. */
    public List<VersionedSpec> specs() { return specs; }

    /** The active cluster-default spec version; empty means {@code [project]}. */
    public OptionalInt activeVersion() { return activeVersion; }

    /** Listed shards that belong to no project. */
    public List<ShardId> orphans() { return orphans; }

    /** Every valid id of the catalog's {@code shards} list, in catalog order (orphans and staged included). */
    public List<ShardId> listed() { return listed; }

    /** The staged spec version of {@code project} (a reshard in progress), if any. */
    /**
     * Ruling P6-R21: the generation spec version {@code specVersion} of {@code shard}'s project names for it
     * when that is not the project's active version: the previous (retired) version's or a staged version's
     * pinned generation. Empty when {@code specVersion} is the active version, or one this mapping does not know
     * (a newer catalog's), or when the version does not pin the shard.
     */
    public Optional<Long> generationOfVersion(ShardId shard, int specVersion) {
        Optional<String> project = projectOf(shard);
        if (project.isEmpty()) return Optional.empty();
        String p = project.get();
        if (specVersionOf(p).equals(OptionalInt.of(specVersion))) return Optional.empty();
        Optional<Previous> prev = previousOf(p);
        if (prev.isPresent() && prev.get().specVersion().equals(OptionalInt.of(specVersion))) {
            return Optional.ofNullable(prev.get().generations().get(shard));
        }
        Optional<Staged> st = stagedOf(p);
        if (st.isPresent() && st.get().specVersion() == specVersion) {
            return Optional.ofNullable(st.get().generations().get(shard));
        }
        return Optional.empty();
    }

    public Optional<Staged> stagedOf(String project) {
        return Optional.ofNullable(staged.get(project));
    }

    /**
     * True when {@code project}'s active spec is not {@code [project]}: its shards are activated and
     * queried as one set, and its call graph is the project's.
     */
    public boolean isSplit(String project) {
        return !specOf(project).isDefault();
    }

    /**
     * What a reader syncs: every project's active shards, then the staged shards not among them (a
     * reshard in progress, synced ahead of the flip), each once, in project order.
     */
    public List<ShardId> syncShards() {
        LinkedHashSet<ShardId> out = new LinkedHashSet<>();
        byProject.values().forEach(out::addAll);
        for (String p : byProject.keySet()) {
            stagedOf(p).filter(st -> !st.abandoned()).ifPresent(st -> out.addAll(st.shards()));
            previousOf(p).ifPresent(pr -> out.addAll(pr.shards()));
        }
        return List.copyOf(out);
    }

    /**
     * The generation of each active shard of {@code project} its flipped spec version is made of (a
     * reshard's pins, until the project's next index run); empty when the project has none.
     */
    public Map<ShardId, Long> pinsOf(String project) {
        return pins.getOrDefault(project, Map.of());
    }

    /** The version a reshard flip retired, while it is kept (retention); empty otherwise. */
    public Optional<Previous> previousOf(String project) {
        return Optional.ofNullable(previous.get(project));
    }

    /** The reshard requested for {@code project} and not yet flipped or cancelled. */
    public Optional<ReshardRequest> reshardRequestOf(String project) {
        return Optional.ofNullable(requests.get(project));
    }

    /** Staged or retired versions of {@code project} waiting for GC. */
    public List<Abandoned> abandonedOf(String project) {
        return abandoned.getOrDefault(project, List.of());
    }

    /**
     * Every generation of {@code shard} a version of its project names (the active pins, a staged or the
     * previous version's): GC must keep them whatever {@code LATEST} says.
     */
    public java.util.Set<Long> pinnedGenerations(ShardId shard) {
        java.util.Set<Long> out = new java.util.HashSet<>();
        String project = byShard.get(shard);
        if (project == null) return out;
        Long g = pinsOf(project).get(shard);
        if (g != null) out.add(g);
        stagedOf(project).filter(st -> !st.abandoned()).map(st -> st.generations().get(shard)).ifPresent(out::add);
        previousOf(project).map(pr -> pr.generations().get(shard)).ifPresent(out::add);
        return out;
    }
}
