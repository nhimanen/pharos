package com.pharos.cluster;

import java.io.IOException;
import java.util.Optional;

/** Writer-side hook invoked after a local index run commits. */
public interface IndexPublisher {
    /** @return the published generation, or empty when nothing is published (e.g. local backend) */
    Optional<Generation> publish(LocalSnapshot snapshot) throws IOException;

    /**
     * Called before an index run starts, so a project this publisher could never publish fails
     * fast instead of after the whole local index. The default accepts every name.
     *
     * @throws IllegalArgumentException when {@code project} cannot be published
     */
    default void checkPublishable(String project) {}
}
