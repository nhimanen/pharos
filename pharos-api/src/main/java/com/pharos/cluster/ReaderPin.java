package com.pharos.cluster;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * A reader's {@code cluster.reader.shards} (P6-R5): shard ids, and project wildcards
 * ({@code lucene~*}: every shard of project {@code lucene}, whatever its spec). Empty pins nothing:
 * the reader serves every shard. A wildcard matches through the project the catalog maps a shard to,
 * never through the id string, so pinning a project keeps working across a reshard.
 */
public final class ReaderPin {

    private static final ReaderPin NONE = new ReaderPin(Set.of(), Set.of());

    private final Set<ShardId> shards;
    private final Set<String> projects;

    private ReaderPin(Set<ShardId> shards, Set<String> projects) {
        this.shards = Set.copyOf(shards);
        this.projects = Set.copyOf(projects);
    }

    /** Pins nothing. */
    public static ReaderPin none() {
        return NONE;
    }

    /**
     * Parses {@code cluster.reader.shards}.
     *
     * @throws IllegalArgumentException naming an entry that is neither a shard id nor a project wildcard
     */
    public static ReaderPin of(Collection<String> entries) {
        if (entries == null || entries.isEmpty()) return NONE;
        Set<ShardId> shards = new LinkedHashSet<>();
        Set<String> projects = new LinkedHashSet<>();
        for (String e : entries) {
            Optional<String> project = ShardId.projectWildcard(e);
            if (project.isPresent()) {
                projects.add(project.get());
                continue;
            }
            try {
                shards.add(new ShardId(e));
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException(ex.getMessage() + "; or " + ShardId.WILDCARD_HINT, ex);
            }
        }
        return new ReaderPin(shards, projects);
    }

    /** True when it pins nothing (the reader serves every shard). */
    public boolean isEmpty() {
        return shards.isEmpty() && projects.isEmpty();
    }

    /** True when it has a project wildcard (matching then needs the shard's project). */
    public boolean hasWildcards() {
        return !projects.isEmpty();
    }

    /**
     * True when the reader serves {@code shard}: nothing is pinned, the id is pinned, or a wildcard
     * names the project {@code projectOf} maps it to (asked only when a wildcard could match).
     */
    public boolean matches(ShardId shard, Function<ShardId, Optional<String>> projectOf) {
        if (isEmpty() || shards.contains(shard)) return true;
        if (projects.isEmpty()) return false;
        return projectOf.apply(shard).map(projects::contains).orElse(false);
    }

    /** As {@link #matches(ShardId, Function)} for a name that may not be a shard id (never matches then). */
    public boolean matches(String name, Function<ShardId, Optional<String>> projectOf) {
        if (isEmpty()) return true;
        if (name == null || !ShardId.isValid(name)) return false;
        return matches(new ShardId(name), projectOf);
    }

    /** The pinned ids and wildcards, for logs. */
    public List<String> entries() {
        List<String> out = new java.util.ArrayList<>();
        shards.forEach(s -> out.add(s.value()));
        projects.forEach(p -> out.add(ShardId.wildcardOf(p)));
        return out;
    }

    @Override
    public String toString() {
        return entries().toString();
    }
}
