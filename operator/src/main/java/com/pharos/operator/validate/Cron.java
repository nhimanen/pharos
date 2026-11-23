package com.pharos.operator.validate;

import com.pharos.operator.scale.CronSchedule;

/**
 * Syntax check for the standard 5-field cron of {@code autoscaling.schedule}. It is
 * {@link CronSchedule#parse}, the parser the schedule floors evaluate with, so the validator accepts
 * exactly what the autoscaler can match.
 */
final class Cron {

    private Cron() {}

    /** @return null when {@code expr} is a valid 5-field cron, otherwise what is wrong with it */
    static String problem(String expr) {
        try {
            CronSchedule.parse(expr);
            return null;
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }
    }
}
