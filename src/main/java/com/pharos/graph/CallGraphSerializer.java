package com.pharos.graph;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.graphml.GraphMLExporter;
import org.jgrapht.nio.graphml.GraphMLImporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Serializes and deserializes CallGraph to/from GraphML format.
 * Stored at ~/.pharos/indexes/<project>/graph.graphml.
 *
 * Note: Node IDs in GraphML must be valid NMTOKEN (no angle brackets, spaces etc).
 * We use URL-encoded FQNs as IDs and store the original FQN as a "fqn" attribute.
 */
public class CallGraphSerializer {

    private static final Logger log = LoggerFactory.getLogger(CallGraphSerializer.class);

    /** Safe encode a FQN for use as a GraphML node ID (NMTOKEN-safe). */
    private static String encodeId(String fqn) {
        return "n_" + URLEncoder.encode(fqn, StandardCharsets.UTF_8)
                .replace("+", "%20").replace("%", "_");
    }

    private static String decodeId(String encodedId) {
        if (encodedId.startsWith("n_")) {
            // Only restore _XX → %XX where XX are hex digits (what encodeId produced).
            // Plain underscores in the original FQN must not be touched.
            String body = encodedId.substring(2)
                    .replaceAll("_([0-9A-Fa-f]{2})", "%$1");
            return URLDecoder.decode(body, StandardCharsets.UTF_8);
        }
        return URLDecoder.decode(encodedId, StandardCharsets.UTF_8);
    }

    /** Save the call graph to a GraphML file. */
    public void save(CallGraph graph, Path outputFile) throws IOException {
        Files.createDirectories(outputFile.getParent());

        GraphMLExporter<String, DefaultEdge> exporter = new GraphMLExporter<>();
        // Use encoded ID as node ID; store original FQN as "fqn" attribute
        exporter.setVertexIdProvider(v -> encodeId(v));
        exporter.setVertexAttributeProvider(v -> {
            Map<String, Attribute> attrs = new LinkedHashMap<>();
            attrs.put("fqn", DefaultAttribute.createAttribute(v));
            return attrs;
        });
        // Register the fqn attribute
        exporter.registerAttribute("fqn", GraphMLExporter.AttributeCategory.NODE,
                org.jgrapht.nio.AttributeType.STRING);

        Path tmp = outputFile.resolveSibling(outputFile.getFileName() + ".tmp");
        try (Writer writer = new OutputStreamWriter(
                Files.newOutputStream(tmp), StandardCharsets.UTF_8)) {
            exporter.exportGraph(graph.getInternalGraph(), writer);
        }
        Files.move(tmp, outputFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        log.debug("Saved call graph to {}", outputFile);
    }

    /** Load a call graph from a GraphML file. */
    public CallGraph load(Path inputFile) throws IOException {
        if (!Files.exists(inputFile)) {
            return new CallGraph();
        }

        CallGraph graph = new CallGraph();

        // Map from encoded ID -> original FQN
        Map<String, String> idToFqn = new HashMap<>();

        GraphMLImporter<String, DefaultEdge> importer = new GraphMLImporter<>();
        // Vertex factory: use encoded ID as placeholder, resolve to FQN via attribute
        importer.setVertexFactory(id -> id);
        importer.addVertexAttributeConsumer((pair, attr) -> {
            if ("fqn".equals(pair.getSecond())) {
                idToFqn.put(pair.getFirst(), attr.getValue());
            }
        });

        // Load into a temporary graph with encoded IDs
        org.jgrapht.graph.DirectedPseudograph<String, DefaultEdge> tmpGraph =
                new org.jgrapht.graph.DirectedPseudograph<>(DefaultEdge.class);
        try (Reader reader = new InputStreamReader(
                Files.newInputStream(inputFile), StandardCharsets.UTF_8)) {
            importer.importGraph(tmpGraph, reader);
        }

        // Rebuild graph with decoded FQN nodes
        for (String encodedId : tmpGraph.vertexSet()) {
            String fqn = idToFqn.getOrDefault(encodedId, decodeId(encodedId));
            graph.addMethod(fqn);
        }
        for (DefaultEdge edge : tmpGraph.edgeSet()) {
            String srcEncoded = tmpGraph.getEdgeSource(edge);
            String tgtEncoded = tmpGraph.getEdgeTarget(edge);
            String src = idToFqn.getOrDefault(srcEncoded, decodeId(srcEncoded));
            String tgt = idToFqn.getOrDefault(tgtEncoded, decodeId(tgtEncoded));
            graph.addCall(src, tgt);
        }

        log.debug("Loaded call graph from {}: {} nodes, {} edges",
                inputFile, graph.nodeCount(), graph.edgeCount());
        return graph;
    }
}
