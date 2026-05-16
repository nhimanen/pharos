package com.pharos.parser.model;

import java.util.List;

/**
 * Structural relationships extracted from a parsed project beyond method call edges.
 * Produced by {@link com.pharos.parser.CodeParser#buildRelationships} and consumed by
 * {@link com.pharos.graph.KnowledgeGraphBuilder} to populate the knowledge graph.
 *
 * <p>All FQNs follow the same format used throughout the codebase:
 * <ul>
 *   <li>Class/interface: {@code com.example.MyClass}</li>
 *   <li>Method:          {@code com.example.MyClass#myMethod(Type1,Type2)}</li>
 *   <li>Field:           {@code com.example.MyClass#fieldName}</li>
 * </ul>
 *
 * <p>Non-Java parsers may leave {@code fields}, {@code reads}, and {@code writes} empty
 * when field-access tracking is unavailable due to dynamic dispatch semantics.
 */
public record ParsedRelationships(
        String projectName,
        List<TypeEdge>  inherits,         // subclass → superclass (extends)
        List<TypeEdge>  implementsEdges,  // class → interface (implements)
        List<FieldDecl> fields,           // field declarations per class
        List<FieldAccess> reads,          // method reads field
        List<FieldAccess> writes,         // method writes field
        List<TypeEdge>  returns,          // method → return-type class
        List<TypeEdge>  takes,            // method → parameter-type class
        List<TypeEdge>  annotatedBy       // method/class/field → annotation class
) {
    /** A directed relationship between two FQNs (class, method, field, or annotation). */
    public record TypeEdge(String from, String to) {}

    /** A field declared inside a class. */
    public record FieldDecl(String fieldFqn, String ownerFqn, String fieldType,
                             String accessModifier) {}

    /** A method reads or writes a field. */
    public record FieldAccess(String methodFqn, String fieldFqn) {}

    public static ParsedRelationships empty(String projectName) {
        return new ParsedRelationships(projectName, List.of(), List.of(), List.of(),
                List.of(), List.of(), List.of(), List.of(), List.of());
    }
}
