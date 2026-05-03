package com.pharos.graph;

import com.pharos.parser.model.ParsedMethod;
import com.pharos.parser.model.ParsedProject;
import com.pharos.parser.model.CallReference;

/**
 * Builds a CallGraph from a ParsedProject by processing all call references.
 */
public class CallGraphBuilder {

    /**
     * Populates the given graph from the parsed project.
     * All methods are added as nodes; resolved call references become edges.
     */
    public void build(CallGraph graph, ParsedProject project) {
        // Add all methods as nodes first
        for (ParsedMethod method : project.allMethods()) {
            graph.addMethod(method.fqn());
        }

        // Add CALLS edges for all resolved call references
        for (ParsedMethod method : project.allMethods()) {
            for (CallReference call : method.calledMethods()) {
                if (call.resolved()) {
                    graph.addCall(method.fqn(), call.calleeFqn());
                }
                // Unresolved calls are stored in meta.json for cross-project linking
            }
        }
    }
}
