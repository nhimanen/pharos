package com.pharos.operator.scale;

import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterSpec.Autoscaling;
import com.pharos.operator.api.PharosClusterSpec.Floor;
import com.pharos.operator.api.PharosClusterSpec.ScheduleEntry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScheduleFloorsTest {

    static final ZoneId HELSINKI = ZoneId.of("Europe/Helsinki");

    static PharosClusterSpec spec(int min, int max, ScheduleEntry... entries) {
        return AutoscalerTest.cluster(1, 3, min, max, new Autoscaling(true, null, null, null, null, List.of(entries)))
                .getSpec();
    }

    static Instant helsinki(String local) {
        return LocalDateTime.parse(local).atZone(HELSINKI).toInstant();
    }

    @Test
    void scheduleRaisesFloorNeverAboveMax() {
        PharosClusterSpec s = spec(2, 6,
                new ScheduleEntry("0 8 * * 1-5", "Europe/Helsinki", new Floor(4), null),
                new ScheduleEntry("0 18 * * 1-5", "Europe/Helsinki", new Floor(2), null));
        assertThat(ScheduleFloors.floor(s, Role.READERS, helsinki("2026-11-23T07:59"))).isEqualTo(2); // Mon, before 8
        assertThat(ScheduleFloors.floor(s, Role.READERS, helsinki("2026-11-23T08:00"))).isEqualTo(4);
        assertThat(ScheduleFloors.floor(s, Role.READERS, helsinki("2026-11-23T17:59"))).isEqualTo(4);
        assertThat(ScheduleFloors.floor(s, Role.READERS, helsinki("2026-11-23T18:00"))).isEqualTo(2);
        assertThat(ScheduleFloors.floor(s, Role.READERS, helsinki("2026-11-22T12:00"))).isEqualTo(2); // Sunday
        assertThat(ScheduleFloors.floor(s, Role.WRITERS, helsinki("2026-11-23T09:00"))).isEqualTo(1); // readers only

        // never above max (a later spec change to max), never below the spec's min
        PharosClusterSpec high = spec(2, 3, new ScheduleEntry("0 8 * * *", null, new Floor(9), null));
        assertThat(ScheduleFloors.floor(high, Role.READERS, Instant.parse("2026-11-23T09:00:00Z"))).isEqualTo(3);
        PharosClusterSpec low = spec(2, 6, new ScheduleEntry("0 8 * * *", null, new Floor(1), null));
        assertThat(ScheduleFloors.floor(low, Role.READERS, Instant.parse("2026-11-23T09:00:00Z"))).isEqualTo(2);

        // the bounds the autoscaler uses carry it
        assertThat(Bounds.of(s, Role.READERS, helsinki("2026-11-24T10:00"))).isEqualTo(new Bounds(4, 6, 2, 1, 300));
    }

    @Test
    void cronLastFire() {
        CronSchedule every15 = CronSchedule.parse("*/15 * * * *");
        assertThat(every15.lastFireAtOrBefore(Instant.parse("2026-11-23T10:14:59Z"), ZoneOffset.UTC))
                .isEqualTo(Instant.parse("2026-11-23T10:00:00Z"));
        assertThat(every15.lastFireAtOrBefore(Instant.parse("2026-11-23T10:15:00Z"), ZoneOffset.UTC))
                .isEqualTo(Instant.parse("2026-11-23T10:15:00Z"));
        // day of month OR day of week when both are restricted: the 1st, or any Monday
        CronSchedule either = CronSchedule.parse("0 0 1 * MON");
        assertThat(either.lastFireAtOrBefore(Instant.parse("2026-11-27T12:00:00Z"), ZoneOffset.UTC)) // Fri
                .isEqualTo(Instant.parse("2026-11-23T00:00:00Z"));
        assertThat(either.lastFireAtOrBefore(Instant.parse("2026-12-01T12:00:00Z"), ZoneOffset.UTC)) // Tue the 1st
                .isEqualTo(Instant.parse("2026-12-01T00:00:00Z"));
        // weekday 7 is Sunday; ranges with steps; names
        assertThat(CronSchedule.parse("0 8-18/2 * * 7").matches(LocalDateTime.parse("2026-11-22T10:00"))).isTrue();
        assertThat(CronSchedule.parse("0 8-18/2 * * 7").matches(LocalDateTime.parse("2026-11-22T11:00"))).isFalse();
        assertThat(CronSchedule.parse("30 6 * JAN-MAR mon-fri").matches(LocalDateTime.parse("2027-02-01T06:30"))).isTrue();
        // 29 February is found years back
        assertThat(CronSchedule.parse("0 0 29 2 *").lastFireAtOrBefore(Instant.parse("2026-11-23T00:00:00Z"),
                ZoneOffset.UTC)).isEqualTo(Instant.parse("2024-02-29T00:00:00Z"));
        // never fires
        assertThat(CronSchedule.parse("0 0 31 2 *").lastFireAtOrBefore(Instant.parse("2026-11-23T00:00:00Z"),
                ZoneOffset.UTC)).isNull();
        // in the zone: 08:00 Helsinki is 06:00Z in winter
        assertThat(CronSchedule.parse("0 8 * * *").lastFireAtOrBefore(Instant.parse("2026-11-23T07:00:00Z"), HELSINKI))
                .isEqualTo(Instant.parse("2026-11-23T06:00:00Z"));
        // a time skipped by the spring-forward gap (03:30 on 29 March 2026 in Helsinki) fires shifted, once
        assertThat(CronSchedule.parse("30 3 * * *").lastFireAtOrBefore(Instant.parse("2026-03-29T02:00:00Z"), HELSINKI))
                .isEqualTo(Instant.parse("2026-03-29T01:30:00Z"));
        assertThatThrownBy(() -> CronSchedule.parse("0 8 * *")).hasMessageContaining("must have 5 fields");
    }
}
