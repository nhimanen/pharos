package com.pharos.cluster;

/** Monotonically increasing version of a published shard snapshot. */
public record Generation(long value) implements Comparable<Generation> {
    public static final Generation FIRST = new Generation(1);

    public Generation next() {
        return new Generation(value + 1);
    }

    @Override
    public int compareTo(Generation other) {
        return Long.compare(value, other.value);
    }
}
