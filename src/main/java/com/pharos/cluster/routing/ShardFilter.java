package com.pharos.cluster.routing;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.sharding.DocKeys;
import com.pharos.cluster.sharding.ShardSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The key filters of a query beyond its projects, matched against each selected project's spec to
 * prune its shards (Phase 6 pruning): a {@code module}, a {@code pathPrefix} (a directory relative
 * to the project root), a {@code language} (a file extension) and a file {@code kind} ({@code code} or
 * {@code doc}). Null fields filter nothing.
 *
 * <p>A filter prunes only through a spec field keyed on the same thing: {@code module} through
 * {@code module}, {@code language} through {@code language}, {@code kind} through {@code kind}, and a
 * path prefix through {@code prefix(path, depth=D)}. Anything else can't prune: a path filter against
 * a ring (files are placed by the hash of their path), or a filter whose key the spec doesn't have.
 * The project then fans out to all its shards and a routing note says why. Matching is conservative:
 * a shard is dropped only when none of its files can match (key values are compared in their id-safe
 * form, so two raw values sharing one id value keep both). Routing never filters documents itself:
 * documents carry the same values as key fields ({@code DocumentMapper#addKeyFields}) and a query
 * filters them with {@code DocumentMapper#keyFilterQuery}.
 *
 * <p><b>Values</b> (P6-R12), normalised the same way for routing and for documents: {@code module} is
 * the module's directory relative to the project root (the nearest directory holding a build file, not
 * the Maven artifactId), through {@link DocKeys#normalizePath}; {@code pathPrefix} is a directory relative
 * to the project root, matched on whole segments; {@code language} is the file extension, lower case
 * (a leading dot is dropped); {@code kind} is {@code code} or {@code doc}.
 */
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
public record ShardFilter(String module, String pathPrefix, String language, String kind) {

    public static final ShardFilter NONE = new ShardFilter(null, null, null, null);

    public ShardFilter {
        module = blankToNull(module);
        pathPrefix = blankToNull(pathPrefix);
        language = blankToNull(language);
        kind = blankToNull(kind);
    }

    public static ShardFilter module(String module) {
        return new ShardFilter(module, null, null, null);
    }

    public static ShardFilter pathPrefix(String prefix) {
        return new ShardFilter(null, prefix, null, null);
    }

    /**
     * Checks the values can be normalised.
     *
     * @throws IllegalArgumentException naming the bad value (an absolute path, or one with '..')
     */
    public ShardFilter validated() {
        if (module != null) normalizedModule();
        if (pathPrefix != null) normalizedPathPrefix();
        return this;
    }

    /** The module directory, normalised ({@link DocKeys#normalizePath}); {@code "_"} for the project root. */
    public String normalizedModule() {
        if (module == null) return null;
        String m = normalize("module", module);
        return m.isEmpty() ? ShardSpec.NO_VALUE : m;
    }

    /** The path prefix, normalised ({@link DocKeys#normalizePath}); empty for the project root. */
    public String normalizedPathPrefix() {
        return pathPrefix == null ? null : normalize("pathPrefix", pathPrefix);
    }

    /** The file extension, lower case, without a leading dot. */
    public String normalizedLanguage() {
        if (language == null) return null;
        String l = language.startsWith(".") ? language.substring(1) : language;
        return l.toLowerCase(java.util.Locale.ROOT);
    }

    /** The file kind, lower case. */
    public String normalizedKind() {
        return kind == null ? null : kind.toLowerCase(java.util.Locale.ROOT);
    }

    private static String normalize(String what, String path) {
        try {
            return DocKeys.normalizePath(path);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(what + " '" + path + "' must be a directory relative to the project root: "
                    + e.getMessage(), e);
        }
    }

    /** True when it filters nothing. */
    @com.fasterxml.jackson.annotation.JsonIgnore
    public boolean isEmpty() {
        return module == null && pathPrefix == null && language == null && kind == null;
    }

    /** The shards of one project kept by this filter, and why the filter could not prune where it could not. */
    public record Pruned(List<ShardId> kept, List<String> notes) {
        public Pruned {
            kept = List.copyOf(kept);
            notes = List.copyOf(notes);
        }
    }

    /**
     * Prunes {@code shards}, the shards of {@code project} under {@code spec}. A {@code [project]} spec
     * has one shard and nothing to prune: it is kept, without a note.
     */
    public Pruned prune(String project, ShardSpec spec, List<ShardId> shards) {
        if (isEmpty() || spec.isDefault() || shards.isEmpty()) return new Pruned(shards, List.of());
        List<String> notes = new ArrayList<>();
        boolean[] prunes = new boolean[4]; // module, path, language, kind: some spec field compares it
        String prefixWanted = null;
        String[] pathSegs = null;
        if (pathPrefix != null) {
            try {
                String norm = DocKeys.normalizePath(pathPrefix);
                pathSegs = norm.isEmpty() ? new String[0] : norm.split("/");
            } catch (IllegalArgumentException e) {
                notes.add(note(project, spec, shards, "path prefix '" + pathPrefix + "' is not a relative path ("
                        + e.getMessage() + ")"));
            }
        }
        for (ShardSpec.Field f : spec.fields()) {
            switch (f) {
                case ShardSpec.KeyField k when "module".equals(k.key()) -> prunes[0] = module != null;
                case ShardSpec.KeyField k when "language".equals(k.key()) -> prunes[2] = language != null;
                case ShardSpec.KeyField k when "kind".equals(k.key()) -> prunes[3] = kind != null;
                case ShardSpec.PrefixField p -> {
                    if (pathSegs != null) {
                        prefixWanted = String.join("/", java.util.Arrays.asList(pathSegs)
                                .subList(0, Math.min(p.depth(), pathSegs.length)));
                        if (ShardSpec.keyValueIsWhole(prefixWanted) && pathSegs.length > 0) {
                            prunes[1] = true;
                        } else if (pathSegs.length > 0) {
                            notes.add(note(project, spec, shards, "path prefix '" + pathPrefix
                                    + "' is too long to compare with its prefix key"));
                        }
                    }
                }
                default -> { }
            }
        }
        if (module != null && !prunes[0]) notes.add(note(project, spec, shards, "the spec has no module key"));
        if (language != null && !prunes[2]) notes.add(note(project, spec, shards, "the spec has no language key"));
        if (kind != null && !prunes[3]) notes.add(note(project, spec, shards, "the spec has no kind key"));
        if (pathSegs != null && pathSegs.length > 0 && !prunes[1] && notes.stream().noneMatch(n -> n.contains("path prefix"))) {
            String why = spec.ring().isPresent() && spec.fields().stream().noneMatch(x -> x instanceof ShardSpec.PrefixField)
                    ? "path prefix '" + pathPrefix + "' cannot prune a ring (files are placed by the hash of their path)"
                    : "the spec has no prefix key for path prefix '" + pathPrefix + "'";
            notes.add(note(project, spec, shards, why));
        }
        if (!prunes[0] && !prunes[1] && !prunes[2] && !prunes[3]) return new Pruned(shards, notes);

        String moduleValue = prunes[0] ? moduleKeyValue() : null;
        String languageValue = prunes[2] ? ShardSpec.keyValue(normalizedLanguage()) : null;
        String kindValue = prunes[3] ? ShardSpec.keyValue(normalizedKind()) : null;
        String prefixValue = prunes[1] ? ShardSpec.keyValue(prefixWanted) : null;
        int depthWanted = pathSegs == null ? 0 : pathSegs.length;
        List<ShardId> kept = new ArrayList<>();
        for (ShardId id : shards) {
            Map<String, String> kv = id.keyValues();
            boolean ok = (moduleValue == null || moduleValue.equals(kv.get("module")))
                    && (languageValue == null || languageValue.equals(kv.get("language")))
                    && (kindValue == null || kindValue.equals(kv.get("kind")))
                    && (prefixValue == null || prefixMatches(kv.get("prefix"), prefixValue, spec, depthWanted));
            if (ok) kept.add(id);
        }
        List<String> selected = new ArrayList<>();
        if (prunes[0]) selected.add("module '" + module + "'");
        if (prunes[1]) selected.add("path prefix '" + pathPrefix + "'");
        if (prunes[2]) selected.add("language '" + language + "'");
        if (prunes[3]) selected.add("kind '" + kind + "'");
        notes.add("project " + project + ": " + String.join(", ", selected) + " selects " + kept.size() + " of "
                + shards.size() + " shards" + (kept.isEmpty() ? "" : " (" + names(kept) + ")"));
        return new Pruned(kept, notes);
    }

    /** The module as a shard id value; an unnormalisable module keeps every shard (the query refuses it). */
    private String moduleKeyValue() {
        try {
            String m = normalizedModule();
            return ShardSpec.NO_VALUE.equals(m) ? ShardSpec.NO_VALUE : ShardSpec.keyValue(m);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * True when a shard whose prefix key is {@code value} can hold a file under the wanted prefix: equal
     * when the filter has at least {@code depth} directories, else the value continues the wanted
     * directories ({@code '/'} is {@code '-'} in a key value) or is exactly them (files shallower than
     * the depth).
     */
    private static boolean prefixMatches(String value, String wanted, ShardSpec spec, int wantedDepth) {
        if (value == null) return true; // not a shard of this spec's shape: never drop it
        int depth = spec.fields().stream().filter(f -> f instanceof ShardSpec.PrefixField)
                .mapToInt(f -> ((ShardSpec.PrefixField) f).depth()).findFirst().orElse(0);
        if (wantedDepth >= depth) return value.equals(wanted);
        return value.equals(wanted) || value.startsWith(wanted + "-");
    }

    private static String note(String project, ShardSpec spec, List<ShardId> shards, String why) {
        return "project " + project + ": " + why + " under spec " + spec + "; fans out to all " + shards.size()
                + " shards";
    }

    private static String names(List<ShardId> ids) {
        List<String> out = new ArrayList<>(ids.size());
        for (ShardId id : ids) out.add(id.value());
        return String.join(", ", out);
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s.strip();
    }
}
