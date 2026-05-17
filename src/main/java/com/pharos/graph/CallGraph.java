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

    /**
     * When true the database was just cleared and is guaranteed empty — skip the
     * "SELECT @rid ... WHERE fqn = ?" existence check in flush methods, turning
     * O(N × query_cost) into O(N × HashSet.contains) batch inserts.
     * Cleared and reset by {@link #clear()}.
     */
    private boolean freshStart = false;

    /**
     * FQNs inserted during the current session (populated by flush methods).
     * Replaces the per-entry {@code SELECT @rid} existence check when
     * {@link #freshStart} is true — handles both intra- and inter-batch
     * duplicates without any DB queries.
     * Cleared by {@link #clear()}.
     */
    private final Set<String> insertedMethodFqns   = new LinkedHashSet<>();
    private final Set<String> insertedCodeTypeFqns = new LinkedHashSet<>();
    private final Set<String> insertedCodeFieldFqns = new LinkedHashSet<>();

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
            methodType.createProperty("fqn",        Type.STRING);
            methodType.createProperty("classPrefix", Type.STRING);
            db.getSchema().createEdgeType("calls");
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true,  "Method", "fqn");
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Method", "classPrefix");
        }
        // Knowledge-graph vertex/edge types (additive — safe on existing databases)
        if (!db.getSchema().existsType("CodeType")) {
            var t = db.getSchema().createVertexType("CodeType");
            t.createProperty("fqn",         Type.STRING);
            t.createProperty("classPrefix", Type.STRING);
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "CodeType", "fqn");
        }
        if (!db.getSchema().existsType("CodeField")) {
            var f = db.getSchema().createVertexType("CodeField");
            f.createProperty("fqn",         Type.STRING);
            f.createProperty("ownerFqn",    Type.STRING);
            f.createProperty("fieldType",   Type.STRING);
            f.createProperty("accessMod",   Type.STRING);
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true,  "CodeField", "fqn");
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "CodeField", "ownerFqn");
        }
        for (String edge : new String[]{"inherits", "implements_iface", "declares_field",
                "reads_field", "writes_field", "returns_type", "takes_type", "annotated_by"}) {
            if (!db.getSchema().existsType(edge)) db.getSchema().createEdgeType(edge);
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

    // -------------------------------------------------------------------------
    // Knowledge-graph mutations
    // -------------------------------------------------------------------------

    /** Batch buffers for knowledge-graph vertices and edges. */
    private final List<String[]>  codeTypeBatch  = new ArrayList<>(BATCH_SIZE); // [fqn, classPrefix]
    private final List<String[]>  codeFieldBatch = new ArrayList<>(BATCH_SIZE); // [fqn, ownerFqn, type, access]
    private final List<String[]>  kgEdgeBatch    = new ArrayList<>(BATCH_SIZE); // [edgeType, fromFqn, fromVType, toFqn, toVType]

    public void addCodeType(String fqn, String classPrefix) {
        codeTypeBatch.add(new String[]{fqn, classPrefix != null ? classPrefix : ""});
        if (codeTypeBatch.size() >= BATCH_SIZE) flushCodeTypes();
    }

    public void addCodeField(String fieldFqn, String ownerFqn, String fieldType, String accessMod) {
        codeFieldBatch.add(new String[]{fieldFqn, ownerFqn, fieldType, accessMod});
        if (codeFieldBatch.size() >= BATCH_SIZE) flushCodeFields();
    }

    /** Add a typed knowledge-graph edge. {@code fromType}/{@code toType}: "Method","CodeType","CodeField". */
    public void addKgEdge(String edgeType, String fromFqn, String fromVType, String toFqn, String toVType) {
        kgEdgeBatch.add(new String[]{edgeType, fromFqn, fromVType, toFqn, toVType});
        if (kgEdgeBatch.size() >= BATCH_SIZE) flushKgEdges();
    }

    /** Commit all knowledge-graph pending batches. */
    public void flushKnowledge() {
        flushCodeTypes();
        flushCodeFields();
        flushKgEdges();
    }

    /**
     * Delete all vertices and edges (call graph + knowledge graph).
     * Uses TRUNCATE TYPE (O(1)) rather than DELETE VERTEX (O(M) full scan).
     */
    public void clear() {
        methodBatch.clear();
        callBatch.clear();
        codeTypeBatch.clear();
        codeFieldBatch.clear();
        kgEdgeBatch.clear();
        freshStart = true; // DB is about to be empty — use session sets instead of DB queries
        insertedMethodFqns.clear();
        insertedCodeTypeFqns.clear();
        insertedCodeFieldFqns.clear();
        db.begin();
        for (String edge : new String[]{"calls", "inherits", "implements_iface", "declares_field",
                "reads_field", "writes_field", "returns_type", "takes_type", "annotated_by"}) {
            if (db.getSchema().existsType(edge))
                db.command("sql", "TRUNCATE TYPE " + edge + " UNSAFE");
        }
        db.command("sql", "TRUNCATE TYPE Method UNSAFE");
        if (db.getSchema().existsType("CodeType"))  db.command("sql", "TRUNCATE TYPE CodeType UNSAFE");
        if (db.getSchema().existsType("CodeField")) db.command("sql", "TRUNCATE TYPE CodeField UNSAFE");
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
            if (cls == null || cls.isEmpty()) continue;
            db.command("sql", "DELETE VERTEX FROM Method WHERE classPrefix = ?", cls);
            if (db.getSchema().existsType("CodeType"))
                db.command("sql", "DELETE VERTEX FROM CodeType WHERE classPrefix = ?", cls);
            if (db.getSchema().existsType("CodeField"))
                db.command("sql", "DELETE VERTEX FROM CodeField WHERE ownerFqn = ?", cls);
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
    // Knowledge-graph queries
    // -------------------------------------------------------------------------

    /** Direct subclasses of {@code classFqn} (one hop via inherits or implements_iface, in-edges). */
    public Set<String> directSubclasses(String classFqn) {
        return kgNeighbors(classFqn, "CodeType", Vertex.DIRECTION.IN,
                new String[]{"inherits", "implements_iface"});
    }

    /** Direct superclasses / implemented interfaces of {@code classFqn} (out-edges). */
    public Set<String> directSuperTypes(String classFqn) {
        return kgNeighbors(classFqn, "CodeType", Vertex.DIRECTION.OUT,
                new String[]{"inherits", "implements_iface"});
    }

    /** BFS over inherits + implements_iface to collect all transitive subclasses. */
    public List<String> allSubclasses(String classFqn) {
        return kgBfs(classFqn, "CodeType", Vertex.DIRECTION.IN,
                new String[]{"inherits", "implements_iface"});
    }

    /** All methods that read a field (reads_field in-edges on CodeField vertex). */
    public Set<String> fieldReaders(String fieldFqn) {
        return kgNeighbors(fieldFqn, "CodeField", Vertex.DIRECTION.IN, new String[]{"reads_field"});
    }

    /** All methods that write a field. */
    public Set<String> fieldWriters(String fieldFqn) {
        return kgNeighbors(fieldFqn, "CodeField", Vertex.DIRECTION.IN, new String[]{"writes_field"});
    }

    /** All fields declared by a class (FQNs only). */
    public Set<String> declaredFields(String classFqn) {
        return kgNeighbors(classFqn, "CodeType", Vertex.DIRECTION.OUT, new String[]{"declares_field"});
    }

    /** Rich field info for all fields declared by a class. */
    public List<FieldInfo> getFieldsOf(String classFqn) {
        if (!db.getSchema().existsType("CodeField")) return List.of();
        ResultSet rs = db.query("sql",
                "SELECT fqn, fieldType, accessMod FROM CodeField WHERE ownerFqn = ?", classFqn);
        List<FieldInfo> result = new ArrayList<>();
        while (rs.hasNext()) {
            var rec = rs.next();
            String fqn   = rec.getProperty("fqn");
            String type  = rec.getProperty("fieldType");
            String access = rec.getProperty("accessMod");
            if (fqn != null) result.add(new FieldInfo(fqn, type, access));
        }
        return result;
    }

    /** Field declaration with type and access modifier. */
    public record FieldInfo(String fqn, String fieldType, String accessMod) {
        public String fieldName() {
            int hash = fqn.indexOf('#');
            return hash >= 0 ? fqn.substring(hash + 1) : fqn;
        }
    }

    /** All methods/classes annotated with the given annotation FQN or simple name. */
    public Set<String> annotatedWith(String annotationFqn) {
        return kgNeighbors(annotationFqn, "CodeType", Vertex.DIRECTION.IN, new String[]{"annotated_by"});
    }

    /** All methods that return the given type. */
    public Set<String> methodsReturning(String typeFqn) {
        return kgNeighbors(typeFqn, "CodeType", Vertex.DIRECTION.IN, new String[]{"returns_type"});
    }

    /** All methods that take the given type as a parameter. */
    public Set<String> methodsTaking(String typeFqn) {
        return kgNeighbors(typeFqn, "CodeType", Vertex.DIRECTION.IN, new String[]{"takes_type"});
    }

    private Set<String> kgNeighbors(String fqn, String vType, Vertex.DIRECTION dir, String[] edgeTypes) {
        Vertex v = lookupKgVertex(fqn, vType);
        if (v == null) return Set.of();
        Set<String> result = new LinkedHashSet<>();
        for (Vertex neighbor : v.getVertices(dir, edgeTypes)) {
            String f = neighbor.getString("fqn");
            if (f != null) result.add(f);
        }
        return result;
    }

    private List<String> kgBfs(String startFqn, String vType, Vertex.DIRECTION dir, String[] edgeTypes) {
        Set<String> visited = new LinkedHashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        queue.add(startFqn);
        visited.add(startFqn);
        while (!queue.isEmpty()) {
            String current = queue.poll();
            Vertex v = lookupKgVertex(current, vType);
            if (v == null) continue;
            for (Vertex n : v.getVertices(dir, edgeTypes)) {
                String f = n.getString("fqn");
                if (f != null && visited.add(f)) queue.add(f);
            }
        }
        visited.remove(startFqn);
        return new ArrayList<>(visited);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void flushMethods() {
        if (methodBatch.isEmpty()) return;
        db.begin();
        for (String[] m : methodBatch) {
            if (freshStart) {
                // Fast path: use in-memory set — O(1), handles intra- and inter-batch duplicates.
                if (!insertedMethodFqns.add(m[0])) continue;
            } else {
                if (db.query("sql", "SELECT @rid FROM Method WHERE fqn = ?", m[0]).hasNext()) continue;
            }
            MutableVertex v = db.newVertex("Method");
            v.set("fqn", m[0]);
            v.set("classPrefix", m[1]);
            v.save();
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

    private Vertex lookupKgVertex(String fqn, String vType) {
        ResultSet rs = db.query("sql", "SELECT FROM " + vType + " WHERE fqn = ?", fqn);
        if (!rs.hasNext()) return null;
        return rs.next().getVertex().orElse(null);
    }

    private void flushCodeTypes() {
        if (codeTypeBatch.isEmpty()) return;
        db.begin();
        for (String[] m : codeTypeBatch) {
            if (freshStart) {
                if (!insertedCodeTypeFqns.add(m[0])) continue;
            } else {
                if (db.query("sql", "SELECT @rid FROM CodeType WHERE fqn = ?", m[0]).hasNext()) continue;
            }
            MutableVertex v = db.newVertex("CodeType");
            v.set("fqn", m[0]);
            v.set("classPrefix", m[1]);
            v.save();
        }
        db.commit();
        codeTypeBatch.clear();
    }

    private void flushCodeFields() {
        if (codeFieldBatch.isEmpty()) return;
        db.begin();
        for (String[] f : codeFieldBatch) {
            if (freshStart) {
                if (!insertedCodeFieldFqns.add(f[0])) continue;
            } else {
                if (db.query("sql", "SELECT @rid FROM CodeField WHERE fqn = ?", f[0]).hasNext()) continue;
            }
            MutableVertex v = db.newVertex("CodeField");
            v.set("fqn",       f[0]);
            v.set("ownerFqn",  f[1]);
            v.set("fieldType", f[2]);
            v.set("accessMod", f[3]);
            v.save();
        }
        db.commit();
        codeFieldBatch.clear();
    }

    private void flushKgEdges() {
        if (kgEdgeBatch.isEmpty()) return;
        db.begin();
        for (String[] e : kgEdgeBatch) {
            String edgeType = e[0], fromFqn = e[1], fromVType = e[2], toFqn = e[3], toVType = e[4];
            try {
                MutableVertex from = ensureKgVertex(fromFqn, fromVType);
                MutableVertex to   = ensureKgVertex(toFqn,   toVType);
                if (from != null && to != null) from.newEdge(edgeType, to, true);
            } catch (Exception ex) {
                log.debug("KG edge {} → {} ({}): {}", fromFqn, toFqn, edgeType, ex.getMessage());
            }
        }
        db.commit();
        kgEdgeBatch.clear();
    }

    private MutableVertex ensureKgVertex(String fqn, String vType) {
        String sql = "SELECT FROM " + vType + " WHERE fqn = ?";
        ResultSet rs = db.query("sql", sql, fqn);
        if (rs.hasNext()) {
            Vertex v = rs.next().getVertex().orElse(null);
            if (v != null) return v.modify();
        }
        // Fall back to Method vertex for method FQNs in relational edges
        if ("Method".equals(vType)) {
            rs = db.query("sql", "SELECT FROM Method WHERE fqn = ?", fqn);
            if (rs.hasNext()) {
                Vertex v = rs.next().getVertex().orElse(null);
                if (v != null) return v.modify();
            }
        }
        MutableVertex v = db.newVertex(vType);
        v.set("fqn", fqn);
        v.set("classPrefix", "");
        v.save();
        return v;
    }

    @Override
    public void close() {
        if (db.isOpen()) {
            db.close();
        }
    }
}
