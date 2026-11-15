package com.pharos.graph;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/** A graph with no vertices; returned when a project has no graph on disk. */
public final class EmptyGraphView implements GraphView {
    public static final EmptyGraphView INSTANCE = new EmptyGraphView();

    private EmptyGraphView() {}

    @Override public Set<String> callers(String fqn) { return Set.of(); }
    @Override public Set<String> callees(String fqn) { return Set.of(); }
    @Override public Set<String> calledTargets(Set<String> candidateFqns) { return Set.of(); }
    @Override public List<String> shortestPath(String fromFqn, String toFqn) { return List.of(); }
    @Override public Stream<String> allFqns() { return Stream.empty(); }
    @Override public long methodCount() { return 0; }
    @Override public long callCount() { return 0; }
    @Override public Set<String> directSubclasses(String classFqn) { return Set.of(); }
    @Override public Set<String> directSuperTypes(String classFqn) { return Set.of(); }
    @Override public List<String> allSubclasses(String classFqn) { return List.of(); }
    @Override public Set<String> fieldReaders(String fieldFqn) { return Set.of(); }
    @Override public Set<String> fieldWriters(String fieldFqn) { return Set.of(); }
    @Override public Set<String> declaredFields(String classFqn) { return Set.of(); }
    @Override public List<CallGraph.FieldInfo> getFieldsOf(String classFqn) { return List.of(); }
    @Override public Set<String> annotatedWith(String annotationFqn) { return Set.of(); }
    @Override public Set<String> methodsReturning(String typeFqn) { return Set.of(); }
    @Override public Set<String> methodsTaking(String typeFqn) { return Set.of(); }
    @Override public void close() {}
}
