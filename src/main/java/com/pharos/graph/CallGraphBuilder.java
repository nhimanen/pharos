package com.pharos.graph;

import com.pharos.parser.model.ParsedFile;
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
        for (ParsedMethod method : project.allMethods()) {
            graph.addMethod(method.fqn());
        }
        for (ParsedMethod method : project.allMethods()) {
            for (CallReference call : method.calledMethods()) {
                if (call.resolved()) {
                    graph.addCall(method.fqn(), call.calleeFqn());
                }
            }
        }
    }

    /**
     * Adds nodes and edges for a single parsed file into an existing graph.
     * Used during incremental indexing to splice fresh call-graph data without
     * re-parsing the entire project.
     */
    public void buildFile(CallGraph graph, ParsedFile file) {
        for (ParsedMethod method : file.methods()) {
            graph.addMethod(method.fqn());
        }
        for (ParsedMethod method : file.methods()) {
            for (CallReference call : method.calledMethods()) {
                if (call.resolved()) {
                    graph.addCall(method.fqn(), call.calleeFqn());
                }
            }
        }
    }
}
