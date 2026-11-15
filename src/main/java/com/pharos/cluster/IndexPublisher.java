package com.pharos.cluster;

import java.io.IOException;
import java.util.Optional;

/** Writer-side hook invoked after a local index run commits. */
public interface IndexPublisher {
    /** @return the published generation, or empty when nothing is published (e.g. local backend) */
    Optional<Generation> publish(LocalSnapshot snapshot) throws IOException;
}
