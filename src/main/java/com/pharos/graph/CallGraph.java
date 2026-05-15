package com.pharos.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

/**
 * ArcadeDB-backed method call graph.
 *
 * Nodes (vertex type "Method"):
 *   fqn          — fully qualified method name, e.g. "com.example.MyClass#myMethod(String,int)"
 *   classPrefix  — qualified class name, e.g. "com.example.MyClass"
 *                  Empty string ("") for external stub vertices (callees from other projects).
 *
 * Edges (edge type "calls"):
 *   Directed: caller → callee.
 *
 * Indexes:
 *   UNIQUE   on Method.fqn         → O(1) lookup by FQN
 *   NON-UNIQUE on Method.classPrefix → O(log N) class eviction
 *
 * Usage:
 *   try (CallGraph g = CallGraph.open(dbDir)) {
 *       g.addMethod(fqn, classPrefix);
 *       g.addCall(callerFqn, calleeFqn);
 *       g.flush();  // commit pending batch
 *   }
 */
public class CallGraph implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CallGraph.class);

    /** Commit vertex/edge batches every N pending operations to bound transaction size. */
    private static final int BATCH_SIZE = 500;

    private final Database db;

    /** Pending method vertices not yet committed: [fqn, classPrefix]. */
    private final List<String[]> methodBatch = new ArrayList<>(BATCH_SIZE);

    /** Pending call edges not yet committed: [callerFqn, calleeFqn]. */
    private final List<String[]> callBatch = new ArrayList<>(BATCH_SIZE);

    private CallGraph(Database db) {
        this.db = db;
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /**
     * Open (or create) an ArcadeDB call graph at the given directory.
     * Schema is auto-created on first open.
     */
    public static CallGraph open(Path dbDir) {
        DatabaseFactory factory = new DatabaseFactory(dbDir.toString());
        Database db = factory.exists() ? factory.open() : factory.create();
        db.setAsyncFlush(false); // ensure close() blocks until pages are flushed to disk
        initSchema(db);
        return new CallGraph(db);
    }

    private static void initSchema(Database db) {
        if (!db.getSchema().existsType("Method")) {
            var methodType = db.getSchema().createVertexType("Method");
            methodType.createProperty("fqn",         Type.STRING);
            methodType.createProperty("classPrefix",  Type.STRING);
            db.getSchema().createEdgeType("calls");
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true,  "Method", "fqn");
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Method", "classPrefix");
        }
    }

    // -------------------------------------------------------------------------
    // Mutations (buffered)
    // -------------------------------------------------------------------------

    /**
     * Add a project-owned method vertex.
     * @param classPrefix the qualified class name; must be non-empty for project-owned methods.
     */
    public void addMethod(String fqn, String classPrefix) {
        methodBatch.add(new String[]{fqn, classPrefix != null ? classPrefix : ""});
        if (methodBatch.size() >= BATCH_SIZE) flushMethods();
    }

    /**
     * Add a directed CALLS edge.  If either endpoint does not exist yet it is added
     * as an external stub (empty classPrefix).
     */
    public void addCall(String callerFqn, String calleeFqn) {
        callBatch.add(new String[]{callerFqn, calleeFqn});
        if (callBatch.size() >= BATCH_SIZE) flushCalls();
    }

    /** Commit all pending method and call batches to the database. */
    public void flush() {
        flushMethods();
        flushCalls();
    }

    /** Delete all vertices and edges (used before a full re-index). */
    public void clear() {
        methodBatch.clear();
        callBatch.clear();
        db.begin();
        db.command("sql", "DELETE VERTEX FROM Method");
        db.commit();
    }

    // -------------------------------------------------------------------------
    // Eviction
    // -------------------------------------------------------------------------

    /**
     * Remove all vertices (and their edges) for the given class prefixes.
     * Uses the classPrefix index for O(log N + M_class) cost — no full scan.
     * ArcadeDB's DELETE VERTEX auto-removes all connected edges.
     */
    public void evictClasses(Collection<String> classPrefixes) {
        if (classPrefixes == null || classPrefixes.isEmpty()) return;
        db.begin();
        for (String cls : classPrefixes) {
            if (cls != null && !cls.isEmpty()) {
                db.command("sql", "DELETE VERTEX FROM Method WHERE classPrefix = ?", cls);
            }
        }
        db.commit();
        log.debug("Evicted {} class(es) from call graph", classPrefixes.size());
    }

    // -------------------------------------------------------------------------
    // Queries
    // -------------------------------------------------------------------------

    /** Returns all method FQNs that directly call {@code fqn} (in-edges). */
    public Set<String> callers(String fqn) {
        Vertex v = lookupVertex(fqn);
        if (v == null) return Set.of();
        Set<String> result = new LinkedHashSet<>();
        for (Vertex caller : v.getVertices(Vertex.DIRECTION.IN, "calls")) {
            String f = caller.getString("fqn");
            if (f != null) result.add(f);
        }
        return result;
    }

    /** Returns all method FQNs called by {@code fqn} (out-edges). */
    public Set<String> callees(String fqn) {
        Vertex v = lookupVertex(fqn);
        if (v == null) return Set.of();
        Set<String> result = new LinkedHashSet<>();
        for (Vertex callee : v.getVertices(Vertex.DIRECTION.OUT, "calls")) {
            String f = callee.getString("fqn");
            if (f != null) result.add(f);
        }
        return result;
    }

    /**
     * Returns the subset of {@code candidateFqns} that appear as callees of any
     * edge in this graph. One O(M+E) graph scan instead of O(N) per-vertex lookups.
     * Used by ModuleBoundaryAnalyzer to find entry points without N×M queries.
     */
    public Set<String> calledTargets(Set<String> candidateFqns) {
        if (candidateFqns.isEmpty()) return Set.of();
        Set<String> found = new LinkedHashSet<>();
        ResultSet rs = db.query("sql", "SELECT FROM Method");
        while (rs.hasNext() && found.size() < candidateFqns.size()) {
            Vertex v = rs.next().getVertex().orElse(null);
            if (v == null) continue;
            for (Vertex callee : v.getVertices(Vertex.DIRECTION.OUT, "calls")) {
                String fqn = callee.getString("fqn");
                if (fqn != null && candidateFqns.contains(fqn)) found.add(fqn);
            }
        }
        return found;
    }

    /**
     * BFS shortest call path from {@code fromFqn} to {@code toFqn}.
     * Returns an empty list if no path exists.
     */
    public List<String> shortestPath(String fromFqn, String toFqn) {
        if (lookupVertex(fromFqn) == null || lookupVertex(toFqn) == null) return List.of();
        Map<String, String> prev = new LinkedHashMap<>(); // fqn → caller fqn (null for start)
        Deque<String> queue = new ArrayDeque<>();
        prev.put(fromFqn, null);
        queue.add(fromFqn);
        outer:
        while (!queue.isEmpty()) {
            String current = queue.poll();
            if (current.equals(toFqn)) break;
            Vertex v = lookupVertex(current);
            if (v == null) continue;
            for (Vertex callee : v.getVertices(Vertex.DIRECTION.OUT, "calls")) {
                String calleeFqn = callee.getString("fqn");
                if (calleeFqn != null && !prev.containsKey(calleeFqn)) {
                    prev.put(calleeFqn, current);
                    if (calleeFqn.equals(toFqn)) break outer;
                    queue.add(calleeFqn);
                }
            }
        }
        if (!prev.containsKey(toFqn)) return List.of();
        List<String> path = new ArrayList<>();
        for (String cur = toFqn; cur != null; cur = prev.get(cur)) {
            path.add(0, cur);
        }
        return path;
    }

    /**
     * Returns all project-owned method FQNs (classPrefix != "").
     * External stubs (added implicitly for unresolved callees) are excluded.
     */
    public Stream<String> allFqns() {
        // Use range predicate (classPrefix > '') rather than not-equal (<> '')
        // so ArcadeDB can satisfy the query via the LSM_TREE index on classPrefix
        // instead of doing a full O(M) type scan.  Stubs have classPrefix='' which
        // sorts below any real package string, so they are correctly excluded.
        ResultSet rs = db.query("sql", "SELECT fqn FROM Method WHERE classPrefix > ''");
        List<String> fqns = new ArrayList<>();
        while (rs.hasNext()) {
            Object v = rs.next().getProperty("fqn");
            if (v != null) fqns.add(v.toString());
        }
        return fqns.stream();
    }

    public long methodCount() {
        return db.countType("Method", false);
    }

    public long callCount() {
        return db.countType("calls", false);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void flushMethods() {
        if (methodBatch.isEmpty()) return;
        db.begin();
        for (String[] m : methodBatch) {
            // Insert only if not already present (idempotent).
            ResultSet rs = db.query("sql", "SELECT @rid FROM Method WHERE fqn = ?", m[0]);
            if (!rs.hasNext()) {
                MutableVertex v = db.newVertex("Method");
                v.set("fqn", m[0]);
                v.set("classPrefix", m[1]);
                v.save();
            }
        }
        db.commit();
        methodBatch.clear();
    }

    private void flushCalls() {
        if (callBatch.isEmpty()) return;

        // Single transaction: look up pre-committed vertices via index (they're visible),
        // create stub vertices directly (no index needed), then wire edges via object refs.
        // This avoids SQL subqueries in CREATE EDGE, which can't see uncommitted vertices.
        db.begin();
        Map<String, MutableVertex> vMap = new HashMap<>();
        for (String[] c : callBatch) {
            for (String fqn : new String[]{c[0], c[1]}) {
                if (!vMap.containsKey(fqn)) {
                    vMap.put(fqn, ensureVertexInTx(fqn));
                }
            }
            MutableVertex from = vMap.get(c[0]);
            MutableVertex to   = vMap.get(c[1]);
            if (from != null && to != null) {
                try {
                    from.newEdge("calls", to, true);
                } catch (Exception e) {
                    log.debug("Could not create call edge {} → {}: {}", c[0], c[1], e.getMessage());
                }
            }
        }
        db.commit();
        callBatch.clear();
    }

    /**
     * Within an open transaction: return a MutableVertex for {@code fqn}.
     * For pre-committed vertices (from flushMethods), the index finds them.
     * For new stubs, creates and saves them in the current transaction.
     */
    private MutableVertex ensureVertexInTx(String fqn) {
        ResultSet rs = db.query("sql", "SELECT FROM Method WHERE fqn = ?", fqn);
        if (rs.hasNext()) {
            Vertex v = rs.next().getVertex().orElse(null);
            if (v != null) return v.modify();
        }
        MutableVertex v = db.newVertex("Method");
        v.set("fqn", fqn);
        v.set("classPrefix", "");
        v.save();
        return v;
    }

    private Vertex lookupVertex(String fqn) {
        ResultSet rs = db.query("sql", "SELECT FROM Method WHERE fqn = ?", fqn);
        if (!rs.hasNext()) return null;
        return rs.next().getVertex().orElse(null);
    }

    @Override
    public void close() {
        if (db.isOpen()) {
            db.close();
        }
    }
}
