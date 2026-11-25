package com.pharos.operator.scale;

import com.pharos.operator.Defaults;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterSpec.Floor;
import com.pharos.operator.api.PharosClusterSpec.ScheduleEntry;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

/**
 * The {@code min} floor of a role under {@code autoscaling.schedule}. An entry holds from its last
 * firing (its cron, in its {@code timezone}, default UTC) until another entry for the same role fires
 * later; the entry that fired most recently wins (on a tie, the later one in the list). Before any
 * entry has fired, or with {@code autoscaling.enabled: false}, the floor is the spec's {@code min}.
 *
 * <p>The floor never goes below the spec's {@code min} (a hard bound) nor above {@code max}: an
 * entry that "lowers" the floor brings it back down from an earlier entry's raise, at most to
 * {@code min}. Entries the validator would refuse (bad cron or zone) are skipped.
 */
public final class ScheduleFloors {

    private ScheduleFloors() {}

    public static int floor(PharosClusterSpec spec, Role role, Instant now) {
        int min = Defaults.min(role.spec(spec));
        int max = Defaults.max(role.spec(spec));
        var a = spec.autoscaling();
        if (a == null || !Defaults.autoscalingEnabled(a) || a.schedule() == null) return min;
        Integer scheduled = active(a.schedule(), role, now);
        int floor = scheduled == null ? min : Math.max(min, scheduled);
        return Math.min(floor, max);
    }

    /** The {@code min} of the role's entry that fired last at or before {@code now}, or null. */
    static Integer active(List<ScheduleEntry> entries, Role role, Instant now) {
        Instant best = null;
        Integer value = null;
        for (ScheduleEntry e : entries) {
            Floor f = e == null ? null : role.floor(e);
            if (f == null || f.min() == null) continue;
            Instant fired;
            try {
                ZoneId zone = e.timezone() == null ? ZoneOffset.UTC : ZoneId.of(e.timezone());
                fired = CronSchedule.parse(e.cron()).lastFireAtOrBefore(now, zone);
            } catch (IllegalArgumentException | DateTimeException ex) {
                continue;
            }
            if (fired != null && (best == null || !fired.isBefore(best))) {
                best = fired;
                value = f.min();
            }
        }
        return value;
    }
}
