package com.pharos.cluster.status;

import java.io.IOException;

/** Reads the {@link StatusSnapshot} a status is built from. */
public interface StatusSource {

    /** @throws IOException when the catalog or the heartbeats can't be listed (the previous status stays) */
    StatusSnapshot read() throws IOException;
}
