package com.pharos.cluster.sharding;

import java.util.Objects;
import java.util.Optional;

/**
 * One spec version of the catalog: {@code {"version": N, "project": "...", "spec": "[...]"}}. Versions
 * are numbered across the whole catalog and never reused or edited; a version without a project is a
 * cluster default, one with a project applies to that project only.
 */
public record VersionedSpec(int version, Optional<String> project, ShardSpec spec) {
    public VersionedSpec {
        if (version < 1) throw new IllegalArgumentException("spec version must be at least 1, got " + version);
        project = project == null ? Optional.empty() : project;
        Objects.requireNonNull(spec, "spec");
    }
}
