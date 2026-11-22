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
 *       overlap the active ones (a ring change keeps the shards that neither gain nor lose files).</li>
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

    /**
     * The staged spec version of a project (a reshard in progress): its number, its spec and its shards,
     * in catalog order. Synced ahead by readers, never routed to before the flip.
     */
    public record Staged(int specVersion, ShardSpec spec, List<ShardId> shards) {
        public Staged {
            shards = List.copyOf(shards);
        }
    }

    private ProjectShards(Map<String, List<ShardId>> byProject, Map<String, ShardSpec> specOf,
                          Map<String, Integer> specVersionOf, List<VersionedSpec> specs,
                          OptionalInt activeVersion, List<ShardId> orphans, List<ShardId> listed,
                          Map<String, Staged> staged) {
        TreeMap<String, List<ShardId>> p = new TreeMap<>();
        Map<ShardId, String> s = new HashMap<>();
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
                List.copyOf(shards), Map.of());
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
                } catch (IllegalArgumentException e) {
                    warn.accept("catalog project entry '" + name + "' skipped: " + e.getMessage());
                }
            }
        }

        // Staged versions, once every active claim is known: a staged shard may be an active shard of the
        // same project (a ring change keeps unaffected shards), never one of another project.
        Set<ShardId> stagedClaimed = new java.util.HashSet<>();
        stagedNodes.forEach((name, st) -> {
            try {
                JsonNode sv = st.get("spec");
                VersionedSpec spec = sv != null && sv.canConvertToInt() ? byVersion.get(sv.asInt()) : null;
                if (spec == null) throw new IllegalArgumentException("unknown spec version " + sv);
                List<ShardId> ids = new ArrayList<>();
                for (JsonNode s : st.path("shards")) ids.add(new ShardId(s.asText()));
                checkProject(name, spec, ids);
                List<ShardId> own = byProject.getOrDefault(name, List.of());
                for (ShardId id : ids) {
                    if ((claimed.contains(id) && !own.contains(id)) || stagedClaimed.contains(id)) {
                        throw new IllegalArgumentException("shard " + id + " is claimed by another project");
                    }
                }
                stagedClaimed.addAll(ids);
                staged.put(name, new Staged(spec.version(), spec.spec(), ids));
            } catch (IllegalArgumentException e) {
                warn.accept("catalog project entry '" + name + "': staged version skipped: " + e.getMessage());
            }
        });

        List<ShardId> orphans = new ArrayList<>();
        for (ShardId id : new LinkedHashSet<>(shards)) {
            if (claimed.contains(id) || stagedClaimed.contains(id)) continue;
            if (isPlain(id) && !byProject.containsKey(id.value())) {
                byProject.put(id.value(), List.of(id));
                claimed.add(id);
            } else {
                orphans.add(id);
            }
        }
        return new ProjectShards(byProject, specOf, specVersionOf, specs, active, orphans, shards, staged);
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
        for (String p : byProject.keySet()) stagedOf(p).ifPresent(st -> out.addAll(st.shards()));
        return List.copyOf(out);
    }
}
