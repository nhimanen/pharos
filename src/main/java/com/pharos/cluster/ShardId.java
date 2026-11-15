package com.pharos.cluster;

import java.util.regex.Pattern;

/** Identifier of an index shard; currently one shard per project. */
public record ShardId(String value) {
    private static final Pattern VALID = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$");

    public ShardId {
        if (value == null || !VALID.matcher(value).matches() || value.contains("..")) {
            throw new IllegalArgumentException("project name '" + value
                    + "' cannot be a shard id: use letters, digits, '.', '_' or '-'");
        }
    }

    public static ShardId ofProject(String project) {
        return new ShardId(project);
    }

    @Override
    public String toString() {
        return value;
    }
}
