package com.pharos.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum NodeRole {
    ALL("all"), WRITER("writer"), READER("reader");

    private final String json;

    NodeRole(String json) { this.json = json; }

    @JsonValue
    public String json() { return json; }

    @JsonCreator
    public static NodeRole fromJson(String s) {
        for (NodeRole r : values()) {
            if (r.json.equalsIgnoreCase(s)) return r;
        }
        throw new IllegalArgumentException("Unknown cluster.role '" + s + "' (expected all, writer or reader)");
    }
}
