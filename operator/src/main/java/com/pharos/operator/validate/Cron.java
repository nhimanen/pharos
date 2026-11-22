package com.pharos.operator.validate;

import java.util.List;
import java.util.Locale;

/**
 * Syntax check for the standard 5-field cron of {@code autoscaling.schedule}: minute, hour, day of
 * month, month, day of week. Each field is a comma list of {@code *}, {@code N}, {@code N-M}, each
 * optionally {@code /step}; months and weekdays also take {@code JAN}..{@code DEC} and
 * {@code SUN}..{@code SAT}, and weekday 7 is Sunday. Macros such as {@code @daily} are not accepted.
 */
final class Cron {

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

    private Cron() {}

    /** @return null when {@code expr} is a valid 5-field cron, otherwise what is wrong with it */
    static String problem(String expr) {
        if (expr == null || expr.isBlank()) return "is empty";
        String[] parts = expr.trim().split("\\s+");
        if (parts.length != FIELDS.size()) {
            return "must have 5 fields (minute hour day-of-month month day-of-week), got " + parts.length;
        }
        for (int i = 0; i < parts.length; i++) {
            Field f = FIELDS.get(i);
            for (String item : parts[i].split(",", -1)) {
                String p = item(item, f);
                if (p != null) return f.name + " field '" + parts[i] + "': " + p;
            }
        }
        return null;
    }

    private static String item(String item, Field f) {
        if (item.isEmpty()) return "empty list element";
        String range = item;
        int slash = item.indexOf('/');
        if (slash >= 0) {
            range = item.substring(0, slash);
            Integer step = number(item.substring(slash + 1));
            if (step == null || step < 1) return "step must be a positive number";
        }
        if (range.equals("*")) return null;
        int dash = range.indexOf('-');
        String lo = dash < 0 ? range : range.substring(0, dash);
        Integer from = value(lo, f);
        if (from == null) return "'" + lo + "' is not in " + f.min + ".." + f.max;
        if (dash < 0) return null;
        String hi = range.substring(dash + 1);
        Integer to = value(hi, f);
        if (to == null) return "'" + hi + "' is not in " + f.min + ".." + f.max;
        if (to < from) return "range " + range + " is backwards";
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
