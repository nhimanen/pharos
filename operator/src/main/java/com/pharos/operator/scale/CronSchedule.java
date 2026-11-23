package com.pharos.operator.scale;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;

/**
 * The standard 5-field cron of {@code autoscaling.schedule}: minute, hour, day of month, month, day
 * of week. Each field is a comma list of {@code *}, {@code N}, {@code N-M}, each optionally
 * {@code /step}; months and weekdays also take {@code JAN}..{@code DEC} and {@code SUN}..{@code SAT},
 * and weekday 7 is Sunday. Macros such as {@code @daily} are not accepted.
 *
 * <p>Days follow classic (Vixie) cron: when both day of month and day of week are restricted (neither
 * starts with {@code *}), a day matches when <em>either</em> does; otherwise both must.
 *
 * <p>{@link #parse} is also the validator's syntax check ({@code validate/Cron}), so what the
 * validator accepts is exactly what the schedule evaluates.
 */
public final class CronSchedule {

    private record Field(String name, int min, int max, List<String> names) {}

    private static final List<String> MONTHS =
            List.of("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC");
    private static final List<String> DAYS = List.of("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT");

    private static final List<Field> FIELDS = List.of(
            new Field("minute", 0, 59, List.of()),
            new Field("hour", 0, 23, List.of()),
            new Field("day of month", 1, 31, List.of()),
            new Field("month", 1, 12, MONTHS),
            new Field("day of week", 0, 7, DAYS));

    /**
     * How far back {@link #lastFireAtOrBefore} looks: a little over 8 years, enough for any valid
     * expression that can fire at all (29 February on a given weekday repeats within 28 years, but
     * 29 February alone within 8).
     */
    static final int LOOKBACK_DAYS = 8 * 366 + 2;

    private final BitSet minutes;
    private final BitSet hours;
    private final BitSet daysOfMonth;
    private final BitSet months;
    private final BitSet daysOfWeek; // 0..6, Sunday = 0
    private final boolean domRestricted;
    private final boolean dowRestricted;

    private CronSchedule(BitSet[] fields, boolean domRestricted, boolean dowRestricted) {
        this.minutes = fields[0];
        this.hours = fields[1];
        this.daysOfMonth = fields[2];
        this.months = fields[3];
        BitSet dow = fields[4];
        if (dow.get(7)) dow.set(0);
        dow.clear(7);
        this.daysOfWeek = dow;
        this.domRestricted = domRestricted;
        this.dowRestricted = dowRestricted;
    }

    /**
     * Parses a 5-field cron expression.
     *
     * @throws IllegalArgumentException with what is wrong with it (the validator's message)
     */
    public static CronSchedule parse(String expr) {
        if (expr == null || expr.isBlank()) throw new IllegalArgumentException("is empty");
        String[] parts = expr.trim().split("\\s+");
        if (parts.length != FIELDS.size()) {
            throw new IllegalArgumentException(
                    "must have 5 fields (minute hour day-of-month month day-of-week), got " + parts.length);
        }
        BitSet[] sets = new BitSet[FIELDS.size()];
        for (int i = 0; i < parts.length; i++) {
            Field f = FIELDS.get(i);
            BitSet set = new BitSet(f.max + 1);
            for (String item : parts[i].split(",", -1)) {
                String p = item(item, f, set);
                if (p != null) throw new IllegalArgumentException(f.name + " field '" + parts[i] + "': " + p);
            }
            sets[i] = set;
        }
        return new CronSchedule(sets, !parts[2].startsWith("*"), !parts[4].startsWith("*"));
    }

    /** True when the expression fires at {@code t}'s minute (in {@code t}'s zone). */
    public boolean matches(LocalDateTime t) {
        return minutes.get(t.getMinute()) && hours.get(t.getHour()) && dayMatches(t.toLocalDate());
    }

    /**
     * The most recent firing at or before {@code now}, evaluated in {@code zone}, or null when it has
     * not fired within {@link #LOOKBACK_DAYS}. A local time skipped by a daylight-saving gap fires at
     * the shifted instant (as {@link LocalDateTime#atZone} resolves it); in an overlap, the earlier one.
     */
    public Instant lastFireAtOrBefore(Instant now, ZoneId zone) {
        LocalDateTime bound = LocalDateTime.ofInstant(now, zone).truncatedTo(ChronoUnit.MINUTES);
        LocalDate day = bound.toLocalDate();
        for (int i = 0; i < LOOKBACK_DAYS; i++, day = day.minusDays(1)) {
            if (!dayMatches(day)) continue;
            LocalTime limit = i == 0 ? bound.toLocalTime() : LocalTime.of(23, 59);
            for (int h = limit.getHour(); h >= 0; h--) {
                if (!hours.get(h)) continue;
                int fromMinute = h == limit.getHour() ? limit.getMinute() : 59;
                for (int m = fromMinute; m >= 0; m--) {
                    if (!minutes.get(m)) continue;
                    ZonedDateTime at = LocalDateTime.of(day, LocalTime.of(h, m)).atZone(zone);
                    Instant fire = at.toInstant();
                    if (!fire.isAfter(now)) return fire;
                }
            }
        }
        return null;
    }

    private boolean dayMatches(LocalDate d) {
        if (!months.get(d.getMonthValue())) return false;
        boolean dom = daysOfMonth.get(d.getDayOfMonth());
        boolean dow = daysOfWeek.get(d.getDayOfWeek() == DayOfWeek.SUNDAY ? 0 : d.getDayOfWeek().getValue());
        if (domRestricted && dowRestricted) return dom || dow;
        return dom && dow;
    }

    private static String item(String item, Field f, BitSet set) {
        if (item.isEmpty()) return "empty list element";
        String range = item;
        int step = 1;
        int slash = item.indexOf('/');
        if (slash >= 0) {
            range = item.substring(0, slash);
            Integer s = number(item.substring(slash + 1));
            if (s == null || s < 1) return "step must be a positive number";
            step = s;
        }
        int from;
        int to;
        if (range.equals("*")) {
            from = f.min;
            to = f.max;
        } else {
            int dash = range.indexOf('-');
            String lo = dash < 0 ? range : range.substring(0, dash);
            Integer a = value(lo, f);
            if (a == null) return "'" + lo + "' is not in " + f.min + ".." + f.max;
            from = a;
            to = a;
            if (dash >= 0) {
                String hi = range.substring(dash + 1);
                Integer b = value(hi, f);
                if (b == null) return "'" + hi + "' is not in " + f.min + ".." + f.max;
                if (b < a) return "range " + range + " is backwards";
                to = b;
            } else if (slash >= 0) {
                to = f.max; // N/step: from N to the end, as cron reads it
            }
        }
        for (int v = from; v <= to; v += step) set.set(v);
        return null;
    }

    private static Integer value(String s, Field f) {
        int named = f.names.indexOf(s.toUpperCase(Locale.ROOT));
        if (named >= 0) return f.names == MONTHS ? named + 1 : named;
        Integer n = number(s);
        return n != null && n >= f.min && n <= f.max ? n : null;
    }

    private static Integer number(String s) {
        if (s.isEmpty() || s.length() > 4 || !s.chars().allMatch(Character::isDigit)) return null;
        return Integer.parseInt(s);
    }
}
