package com.pharos.operator.scale;

import com.pharos.operator.Defaults;
import com.pharos.operator.api.PharosClusterSpec;

import java.time.Instant;

/**
 * What a role may be scaled to and how fast: the floor {@code min} (the spec's, raised by an active
 * schedule entry), the hard ceiling {@code max}, the step caps and the cooldown. All defaults come
 * from {@link Defaults}.
 */
public record Bounds(int min, int max, int maxStepUp, int maxStepDown, int cooldownSeconds) {

    public Bounds {
        if (min < 0 || max < min) throw new IllegalArgumentException("bounds [" + min + ", " + max + "]");
    }

    /** The role's bounds at {@code now}: the spec's range with the schedule floor applied. */
    public static Bounds of(PharosClusterSpec spec, Role role, Instant now) {
        var a = spec.autoscaling();
        return new Bounds(ScheduleFloors.floor(spec, role, now), Defaults.max(role.spec(spec)),
                Defaults.maxStepUp(a), Defaults.maxStepDown(a), Defaults.cooldownSeconds(a));
    }

    public int clamp(int n) {
        return Math.max(min, Math.min(max, n));
    }
}
