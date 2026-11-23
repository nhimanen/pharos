package com.pharos.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum BackendKind {
    LOCAL("local"), OBJECT_STORE("object-store");

    private final String json;

    BackendKind(String json) { this.json = json; }

    @JsonValue
    public String json() { return json; }

    @JsonCreator
    public static BackendKind fromJson(String s) {
        for (BackendKind b : values()) {
            if (b.json.equalsIgnoreCase(s)) return b;
        }
        throw new IllegalArgumentException("Unknown cluster.backend '" + s + "' (expected local or object-store)");
    }
}
