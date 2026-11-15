package com.pharos.graph;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/** Read-only view of a project's call/knowledge graph. Every graph read path goes through this. */
public interface GraphView extends AutoCloseable {
    Set<String> callers(String fqn);
    Set<String> callees(String fqn);
    Set<String> calledTargets(Set<String> candidateFqns);
    List<String> shortestPath(String fromFqn, String toFqn);
    Stream<String> allFqns();
    long methodCount();
    long callCount();
    Set<String> directSubclasses(String classFqn);
    Set<String> directSuperTypes(String classFqn);
    List<String> allSubclasses(String classFqn);
    Set<String> fieldReaders(String fieldFqn);
    Set<String> fieldWriters(String fieldFqn);
    Set<String> declaredFields(String classFqn);
    List<CallGraph.FieldInfo> getFieldsOf(String classFqn);
    Set<String> annotatedWith(String annotationFqn);
    Set<String> methodsReturning(String typeFqn);
    Set<String> methodsTaking(String typeFqn);

    @Override
    void close();
}
