package com.pharos.graph;

import com.pharos.config.ProjectMeta;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Opens the read-only {@link GraphView} for a project's shard directory. */
public final class GraphViews {
    /** Legacy ArcadeDB dir; the live one is resolved through {@link CallGraph#POINTER_FILE}. */
    public static final String ARCADE_DIR = CallGraph.LEGACY_DIR;
    public static final String COMPACT_FILE = "graph.cgf.gz";

    private GraphViews() {}

    public static GraphView open(Path shardDir) {
        // the writer's live generation (callgraph.current), or the legacy dir without a pointer
        Path arcade = CallGraph.projectGraphDir(shardDir);
        if (Files.isDirectory(arcade)) {
            return CallGraph.openExisting(arcade);
        }
        Path compact = shardDir.resolve(COMPACT_FILE);
        if (Files.exists(compact)) {
            try {
                return CompactGraph.read(compact);
            } catch (IOException e) {
                // surfaced, not swallowed: an unreadable graph must not look like "no graph"
                throw new UncheckedIOException("Cannot read compact graph " + compact, e);
            }
        }
        return EmptyGraphView.INSTANCE;
    }

    public static GraphView open(ProjectMeta meta) {
        return open(Path.of(meta.getIndexPath()));
    }
}
