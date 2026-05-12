package com.pharos.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

/**
 * ArcadeDB-backed module dependency graph.
 *
 * Nodes (vertex type "Module"):
 *   moduleKey    — "groupId:artifactId"  (unique index)
 *   groupId      — Maven groupId
 *   artifactId   — Maven artifactId
 *   version      — display version (mutable)
 *   status       — "INDEXED" | "EXTERNAL"
 *   projectName  — non-null only when status == "INDEXED"
 *
 * Edges (edge type "dependency"):
 *   scope           — Maven scope: compile / test / provided / runtime
 *   declaredVersion — as declared in pom.xml, may be null
 *
 * Stored at {@code ~/.pharos/module-graph.arcadedb/}.
 */
public class ModuleGraph implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ModuleGraph.class);

    private final Database db;

    private ModuleGraph(Database db) {
        this.db = db;
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    public static ModuleGraph open(Path dbDir) {
        DatabaseFactory factory = new DatabaseFactory(dbDir.toString());
        Database db = factory.exists() ? factory.open() : factory.create();
        db.setAsyncFlush(false); // ensure close() blocks until pages are flushed to disk
        initSchema(db);
        return new ModuleGraph(db);
    }

    private static void initSchema(Database db) {
        if (!db.getSchema().existsType("Module")) {
            var moduleType = db.getSchema().createVertexType("Module");
            moduleType.createProperty("moduleKey",   Type.STRING);
            moduleType.createProperty("groupId",     Type.STRING);
            moduleType.createProperty("artifactId",  Type.STRING);
            moduleType.createProperty("version",     Type.STRING);
            moduleType.createProperty("status",      Type.STRING);
            moduleType.createProperty("projectName", Type.STRING);
            var depType = db.getSchema().createEdgeType("dependency");
            depType.createProperty("scope",           Type.STRING);
            depType.createProperty("declaredVersion", Type.STRING);
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Module", "moduleKey");
        }
    }

    // -------------------------------------------------------------------------
    // Mutations
    // -------------------------------------------------------------------------

    /**
     * Upsert a module node by moduleKey.
     * If the node exists and incoming status is "INDEXED" while current is "EXTERNAL",
     * the node is upgraded in place.
     */
    public void addOrUpdate(String moduleKey, String groupId, String artifactId,
                            String version, String status, String projectName) {
        db.begin();
        ResultSet existing = db.query("sql", "SELECT FROM Module WHERE moduleKey = ?", moduleKey);
        if (existing.hasNext()) {
            Result r = existing.next();
            String currentStatus = r.getProperty("status");
            if ("INDEXED".equals(status) && "EXTERNAL".equals(currentStatus)) {
                db.command("sql",
                        "UPDATE Module SET status = ?, version = ?, projectName = ? WHERE moduleKey = ?",
                        status, version != null ? version : "unknown",
                        projectName, moduleKey);
            }
        } else {
            MutableVertex v = db.newVertex("Module");
            v.set("moduleKey",   moduleKey);
            v.set("groupId",     groupId);
            v.set("artifactId",  artifactId);
            v.set("version",     version != null ? version : "unknown");
            v.set("status",      status);
            v.set("projectName", projectName);
            v.save();
        }
        db.commit();
    }

    /**
     * Downgrade a module from INDEXED to EXTERNAL (e.g., when removing a project).
     * Clears projectName. No-op if the module does not exist.
     */
    public void downgradeToExternal(String moduleKey) {
        db.begin();
        db.command("sql",
                "UPDATE Module SET status = 'EXTERNAL', projectName = null WHERE moduleKey = ?",
                moduleKey);
        db.commit();
    }

    /**
     * Upgrade a module from EXTERNAL to INDEXED.
     * No-op if the module does not exist or is already INDEXED.
     */
    public void upgradeToIndexed(String moduleKey, String version, String projectName) {
        db.begin();
        db.command("sql",
                "UPDATE Module SET status = 'INDEXED', version = ?, projectName = ? WHERE moduleKey = ? AND status = 'EXTERNAL'",
                version != null ? version : "unknown", projectName, moduleKey);
        db.commit();
    }

    /**
     * Add a dependency edge from {@code fromKey} to {@code toKey}.
     * Deduplicates by scope: skips if an edge of the same scope already exists between the pair.
     */
    public void addDependency(String fromKey, String toKey, String scope, String declaredVersion) {
        // Dedup check using Java graph traversal (avoids ArcadeDB edge-to-vertex SQL join issues)
        ResultSet fromRs = db.query("sql", "SELECT FROM Module WHERE moduleKey = ?", fromKey);
        if (fromRs.hasNext()) {
            Vertex fromV = fromRs.next().getVertex().orElse(null);
            if (fromV != null) {
                for (Edge edge : fromV.getEdges(Vertex.DIRECTION.OUT, "dependency")) {
                    Vertex inV = edge.getVertex(Vertex.DIRECTION.IN);
                    if (inV != null
                            && toKey.equals(inV.getString("moduleKey"))
                            && scope.equals(edge.getString("scope"))) {
                        return; // duplicate
                    }
                }
            }
        }

        db.begin();
        try {
            db.command("sql",
                    "CREATE EDGE dependency " +
                    "FROM (SELECT FROM Module WHERE moduleKey = ?) " +
                    "TO   (SELECT FROM Module WHERE moduleKey = ?) " +
                    "SET scope = ?, declaredVersion = ?",
                    fromKey, toKey, scope, declaredVersion);
        } catch (Exception e) {
            log.debug("Could not create dependency edge {} → {} ({}): {}",
                    fromKey, toKey, scope, e.getMessage());
        }
        db.commit();
    }

    // -------------------------------------------------------------------------
    // Queries
    // -------------------------------------------------------------------------

    /** Find a module by its moduleKey. O(1) via unique index. */
    public Optional<ModuleNodeData> findByKey(String moduleKey) {
        ResultSet rs = db.query("sql", "SELECT FROM Module WHERE moduleKey = ?", moduleKey);
        if (!rs.hasNext()) return Optional.empty();
        return rs.next().getVertex().map(ModuleGraph::toNodeData);
    }

    /** Find an INDEXED module by its project name (linear scan). */
    public Optional<ModuleNodeData> findByProjectName(String projectName) {
        ResultSet rs = db.query("sql",
                "SELECT FROM Module WHERE projectName = ? AND status = 'INDEXED'", projectName);
        if (!rs.hasNext()) return Optional.empty();
        return rs.next().getVertex().map(ModuleGraph::toNodeData);
    }

    /** All direct dependencies of the module (outgoing edges → targets). */
    public Set<ModuleNodeData> dependencies(String moduleKey) {
        ResultSet rs = db.query("sql",
                "SELECT expand(out('dependency')) FROM Module WHERE moduleKey = ?", moduleKey);
        Set<ModuleNodeData> result = new LinkedHashSet<>();
        while (rs.hasNext()) {
            rs.next().getVertex().map(ModuleGraph::toNodeData).ifPresent(result::add);
        }
        return result;
    }

    /** All modules that depend on the given module (incoming edges → sources). */
    public Set<ModuleNodeData> dependents(String moduleKey) {
        ResultSet rs = db.query("sql",
                "SELECT expand(in('dependency')) FROM Module WHERE moduleKey = ?", moduleKey);
        Set<ModuleNodeData> result = new LinkedHashSet<>();
        while (rs.hasNext()) {
            rs.next().getVertex().map(ModuleGraph::toNodeData).ifPresent(result::add);
        }
        return result;
    }

    /**
     * All dependency edges between two modules (multiple scopes possible).
     * Used to deduplicate edges in {@link #addDependency}.
     */
    public Set<DepEdgeData> edges(String fromKey, String toKey) {
        ResultSet rs = db.query("sql",
                "SELECT FROM dependency WHERE out().moduleKey = ? AND in().moduleKey = ?",
                fromKey, toKey);
        Set<DepEdgeData> result = new LinkedHashSet<>();
        while (rs.hasNext()) {
            Result r = rs.next();
            result.add(new DepEdgeData(
                    r.getProperty("scope"),
                    r.getProperty("declaredVersion")));
        }
        return result;
    }

    /** BFS shortest dependency path between two modules. */
    public List<ModuleNodeData> shortestPath(String fromKey, String toKey) {
        if (findByKey(fromKey).isEmpty() || findByKey(toKey).isEmpty()) return List.of();
        Map<String, String> prev = new LinkedHashMap<>();
        Deque<String> queue = new ArrayDeque<>();
        prev.put(fromKey, null);
        queue.add(fromKey);
        outer:
        while (!queue.isEmpty()) {
            String current = queue.poll();
            if (current.equals(toKey)) break;
            for (ModuleNodeData dep : dependencies(current)) {
                if (!prev.containsKey(dep.moduleKey())) {
                    prev.put(dep.moduleKey(), current);
                    if (dep.moduleKey().equals(toKey)) break outer;
                    queue.add(dep.moduleKey());
                }
            }
        }
        if (!prev.containsKey(toKey)) return List.of();
        List<ModuleNodeData> path = new ArrayList<>();
        for (String cur = toKey; cur != null; cur = prev.get(cur)) {
            findByKey(cur).ifPresent(n -> path.add(0, n));
        }
        return path;
    }

    /** Stream all modules. */
    public Stream<ModuleNodeData> allModules() {
        ResultSet rs = db.query("sql", "SELECT FROM Module");
        List<ModuleNodeData> result = new ArrayList<>();
        while (rs.hasNext()) {
            rs.next().getVertex().map(ModuleGraph::toNodeData).ifPresent(result::add);
        }
        return result.stream();
    }

    public long moduleCount() {
        return db.countType("Module", false);
    }

    public long dependencyCount() {
        return db.countType("dependency", false);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private static ModuleNodeData toNodeData(Vertex v) {
        return new ModuleNodeData(
                v.getString("moduleKey"),
                v.getString("groupId"),
                v.getString("artifactId"),
                v.getString("version"),
                v.getString("status"),
                v.getString("projectName"));
    }

    @Override
    public void close() {
        if (db.isOpen()) {
            db.close();
        }
    }
}
