package com.pharos.operator.scale;

import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterSpec.Floor;
import com.pharos.operator.api.PharosClusterSpec.RoleSpec;
import com.pharos.operator.api.PharosClusterSpec.ScheduleEntry;

/**
 * A scaled role and the pharos proposal kinds that move it ({@code Proposals} in pharos: kind
 * {@code add-<role>} / {@code remove-<role>}, target {@code <role>}).
 */
public enum Role {
    READERS("readers", "add-readers", "remove-readers"),
    WRITERS("writers", "add-writers", "remove-writers");

    private final String target;
    private final String addKind;
    private final String removeKind;

    Role(String target, String addKind, String removeKind) {
        this.target = target;
        this.addKind = addKind;
        this.removeKind = removeKind;
    }

    /** The proposal target, also the spec field name. */
    public String target() { return target; }

    public String addKind() { return addKind; }

    public String removeKind() { return removeKind; }

    /** True for this role's add or remove kind. */
    public boolean owns(String kind) {
        return addKind.equals(kind) || removeKind.equals(kind);
    }

    /** The role whose add or remove kind this is, or null (reshard, writer-disk, unknown). */
    public static Role of(String kind) {
        for (Role r : values()) if (r.owns(kind)) return r;
        return null;
    }

    public RoleSpec spec(PharosClusterSpec spec) {
        return this == READERS ? spec.readers() : spec.writers();
    }

    public Floor floor(ScheduleEntry e) {
        return this == READERS ? e.readers() : e.writers();
    }
}
