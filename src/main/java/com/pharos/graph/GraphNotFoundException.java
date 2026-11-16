package com.pharos.graph;

import java.nio.file.Path;

/** Thrown by {@link CallGraph#openExisting(Path)} when no graph database exists at the path. */
public class GraphNotFoundException extends RuntimeException {

    public GraphNotFoundException(Path dbDir) {
        super("No call graph at " + dbDir);
    }
}
