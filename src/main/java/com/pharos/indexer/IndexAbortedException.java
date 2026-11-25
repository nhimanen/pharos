package com.pharos.indexer;

import java.io.IOException;

/** An index run stopped at a safe point by {@link IndexAbort}; the project is at its previous commit. */
public final class IndexAbortedException extends IOException {
    public IndexAbortedException(String reason) {
        super("index run aborted at a safe point: " + reason);
    }
}
