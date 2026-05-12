package com.pharos.graph;

import com.pharos.parser.model.CallReference;
import com.pharos.parser.model.ParsedFile;
import com.pharos.parser.model.ParsedMethod;
import com.pharos.parser.model.ParsedProject;

/**
 * Builds a CallGraph from parsed sources by processing call references.
 *
 * Pass 1: add all method vertices (with classPrefix).
 * Pass 2: flush vertices, then add all resolved call edges.
 * Final:  flush edges.
 *
 * Two explicit flush() calls ensure method vertices are committed before
 * edge creation attempts to look them up.
 */
public class CallGraphBuilder {

    /**
     * Populates the given graph from the parsed project.
     * All methods are added as nodes; resolved call references become edges.
     */
    public void build(CallGraph graph, ParsedProject project) {
        for (ParsedMethod method : project.allMethods()) {
            graph.addMethod(method.fqn(), method.qualifiedClassName());
        }
        graph.flush(); // commit vertices before adding edges

        for (ParsedMethod method : project.allMethods()) {
            for (CallReference call : method.calledMethods()) {
                if (call.resolved()) {
                    graph.addCall(method.fqn(), call.calleeFqn());
                }
            }
        }
        graph.flush();
    }

    /**
     * Adds nodes and edges for a single parsed file into an existing graph.
     * Used during incremental indexing to splice fresh data without re-parsing the project.
     */
    public void buildFile(CallGraph graph, ParsedFile file) {
        for (ParsedMethod method : file.methods()) {
            graph.addMethod(method.fqn(), method.qualifiedClassName());
        }
        graph.flush(); // commit vertices before adding edges

        for (ParsedMethod method : file.methods()) {
            for (CallReference call : method.calledMethods()) {
                if (call.resolved()) {
                    graph.addCall(method.fqn(), call.calleeFqn());
                }
            }
        }
        graph.flush();
    }
}
