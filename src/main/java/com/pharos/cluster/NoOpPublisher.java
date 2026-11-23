package com.pharos.cluster;

import java.util.Optional;

/** Publisher for single-node operation: does nothing. */
public final class NoOpPublisher implements IndexPublisher {
    public static final NoOpPublisher INSTANCE = new NoOpPublisher();

    private NoOpPublisher() {}

    @Override
    public Optional<Generation> publish(LocalSnapshot snapshot) {
        return Optional.empty();
    }
}
