package com.pharos.graph;

import com.pharos.parser.model.ParsedRelationships;
import com.pharos.parser.model.ParsedRelationships.FieldAccess;
import com.pharos.parser.model.ParsedRelationships.FieldDecl;
import com.pharos.parser.model.ParsedRelationships.TypeEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Populates the knowledge-graph vertex/edge types in an open {@link CallGraph} from
 * a {@link ParsedRelationships} instance produced by the language parsers.
 *
 * <p>Must be called AFTER {@link CallGraphBuilder} has already flushed Method vertices,
 * because knowledge-graph edges that connect to methods look up those vertices.
 *
 * <p>Build order:
 * <ol>
 *   <li>CodeType vertices (one per class/interface in relationships)</li>
 *   <li>CodeField vertices (one per declared field)</li>
 *   <li>flush vertex batches</li>
 *   <li>All typed edges (inherits, implements_iface, declares_field, reads_field,
 *       writes_field, returns_type, takes_type, annotated_by)</li>
 *   <li>flush edge batch</li>
 * </ol>
 */
public class KnowledgeGraphBuilder {

    private static final Logger log = LoggerFactory.getLogger(KnowledgeGraphBuilder.class);

    public void build(CallGraph graph, ParsedRelationships rel) {
        if (rel == null) return;

        // --- Pass 1: vertices ---

        // CodeType vertices for every class/interface that appears in any edge
        rel.inherits().forEach(e       -> addCodeTypeFromEdge(graph, e));
        rel.implementsEdges().forEach(e -> addCodeTypeFromEdge(graph, e));
        rel.returns().forEach(e        -> { addCodeTypeFromEdge(graph, e); });
        rel.takes().forEach(e          -> { addCodeTypeFromEdge(graph, e); });
        rel.annotatedBy().forEach(e    -> addCodeTypeFromTo(graph, e.to()));

        // CodeField vertices
        for (FieldDecl fd : rel.fields()) {
            addCodeTypeFromClass(graph, fd.ownerFqn());
            graph.addCodeField(fd.fieldFqn(), fd.ownerFqn(), fd.fieldType(), fd.accessModifier());
        }

        graph.flushKnowledge(); // commit vertices before edges

        // --- Pass 2: edges ---

        for (TypeEdge e : rel.inherits()) {
            graph.addKgEdge("inherits", e.from(), "CodeType", e.to(), "CodeType");
        }
        for (TypeEdge e : rel.implementsEdges()) {
            graph.addKgEdge("implements_iface", e.from(), "CodeType", e.to(), "CodeType");
        }
        for (FieldDecl fd : rel.fields()) {
            graph.addKgEdge("declares_field", fd.ownerFqn(), "CodeType", fd.fieldFqn(), "CodeField");
        }
        for (FieldAccess fa : rel.reads()) {
            graph.addKgEdge("reads_field", fa.methodFqn(), "Method", fa.fieldFqn(), "CodeField");
        }
        for (FieldAccess fa : rel.writes()) {
            graph.addKgEdge("writes_field", fa.methodFqn(), "Method", fa.fieldFqn(), "CodeField");
        }
        for (TypeEdge e : rel.returns()) {
            graph.addKgEdge("returns_type", e.from(), "Method", e.to(), "CodeType");
        }
        for (TypeEdge e : rel.takes()) {
            graph.addKgEdge("takes_type", e.from(), "Method", e.to(), "CodeType");
        }
        for (TypeEdge e : rel.annotatedBy()) {
            // from can be a method FQN or a class FQN — pick vertex type accordingly
            String fromVType = e.from().contains("#") ? "Method" : "CodeType";
            graph.addKgEdge("annotated_by", e.from(), fromVType, e.to(), "CodeType");
        }

        graph.flushKnowledge();
        log.debug("Knowledge graph built: {} inherits, {} implements, {} fields, {} reads, {} writes",
                rel.inherits().size(), rel.implementsEdges().size(), rel.fields().size(),
                rel.reads().size(), rel.writes().size());
    }

    private static void addCodeTypeFromEdge(CallGraph graph, TypeEdge e) {
        addCodeTypeFromClass(graph, e.from());
        addCodeTypeFromClass(graph, e.to());
    }

    private static void addCodeTypeFromTo(CallGraph graph, String fqn) {
        addCodeTypeFromClass(graph, fqn);
    }

    private static void addCodeTypeFromClass(CallGraph graph, String fqn) {
        if (fqn == null || fqn.isBlank()) return;
        // Store the class FQN as classPrefix (mirrors Method vertex convention)
        // so evictClasses(classFqn) can delete CodeType vertices with the same query.
        graph.addCodeType(fqn, fqn);
    }
}
