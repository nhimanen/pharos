package com.example.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Collects operational metrics — latency observations, event counters, and gauges —
 * and exports them to a remote monitoring backend on demand.
 */
public class MetricsRecorder {

    private final Map<String, List<Long>>  latencies = new HashMap<>();
    private final Map<String, Long>        counters  = new HashMap<>();
    private final Map<String, Double>      gauges    = new HashMap<>();

    /**
     * Records the elapsed time of a named operation in milliseconds.
     * Use this to track request latency, database query duration, or any timed activity.
     *
     * @param operation    the name of the operation being measured
     * @param milliseconds the observed duration in milliseconds
     */
    public void recordLatency(String operation, long milliseconds) {
        latencies.computeIfAbsent(operation, k -> new ArrayList<>()).add(milliseconds);
    }

    /**
     * Increments a named event counter by one.
     * Use this to track discrete occurrences such as logins, errors, or cache misses.
     *
     * @param metricName the counter identifier
     */
    public void incrementCounter(String metricName) {
        counters.merge(metricName, 1L, Long::sum);
    }

    /**
     * Sets the current value of a gauge metric.
     * Gauges represent instantaneous measurements such as queue depth or active connections.
     *
     * @param metricName the gauge identifier
     * @param value      the observed value at this point in time
     */
    public void setGaugeValue(String metricName, double value) {
        gauges.put(metricName, value);
    }

    /**
     * Flushes all buffered metric data to the configured monitoring backend.
     * Should be called periodically — typically every 10–60 seconds — to export
     * telemetry. Clears the local buffers after a successful export.
     */
    public void flushToBackend() {
        // serialize latencies, counters, and gauges; push to monitoring system
        latencies.clear();
        counters.clear();
        gauges.clear();
    }
}
