package com.pharos.graph;

import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.AttributeType;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Serializes and deserializes {@link ModuleGraph} to/from GraphML.
 *
 * Node ID scheme: {@code "m_" + URLEncoded(moduleKey)} — colons in groupId:artifactId
 * are not valid NMTOKEN characters, so we URL-encode them.
 *
 * Node attributes: groupId, artifactId, version, status, projectName
 * Edge attributes: scope, declaredVersion
 *
 * Uses the same two-phase import pattern as {@link CallGraphSerializer}:
 * collect attributes into maps first, then reconstruct typed objects.
 */
public class ModuleGraphSerializer {

    private static final Logger log = LoggerFactory.getLogger(ModuleGraphSerializer.class);

    // ---------------------------------------------------------------------------
    // ID encoding
    // ---------------------------------------------------------------------------

    private static String encodeId(String moduleKey) {
        return "m_" + URLEncoder.encode(moduleKey, StandardCharsets.UTF_8)
                .replace("+", "%20").replace("%", "_");
    }

    private static String decodeId(String encoded) {
        if (encoded.startsWith("m_")) {
            encoded = encoded.substring(2).replace("_", "%");
        }
        return URLDecoder.decode(encoded, StandardCharsets.UTF_8);
    }

    // ---------------------------------------------------------------------------
    // Save
    // ---------------------------------------------------------------------------

    public void save(ModuleGraph moduleGraph, Path outputFile) throws IOException {
        Files.createDirectories(outputFile.getParent());

        GraphMLExporter<ModuleNode, ModuleDep> exporter = new GraphMLExporter<>();

        // Node ID + attributes
        exporter.setVertexIdProvider(n -> encodeId(n.getModuleKey()));
        exporter.setVertexAttributeProvider(n -> {
            Map<String, Attribute> attrs = new LinkedHashMap<>();
            attrs.put("groupId",     DefaultAttribute.createAttribute(n.getGroupId()));
            attrs.put("artifactId",  DefaultAttribute.createAttribute(n.getArtifactId()));
            attrs.put("version",     DefaultAttribute.createAttribute(n.getVersion()));
            attrs.put("status",      DefaultAttribute.createAttribute(n.getStatus().name()));
            attrs.put("projectName", DefaultAttribute.createAttribute(
                    n.getProjectName() != null ? n.getProjectName() : ""));
            return attrs;
        });

        // Edge ID (required for DirectedMultigraph — multiple edges between same pair)
        AtomicLong edgeSeq = new AtomicLong();
        exporter.setEdgeIdProvider(e -> {
            ModuleNode src = moduleGraph.getInternalGraph().getEdgeSource(e);
            ModuleNode tgt = moduleGraph.getInternalGraph().getEdgeTarget(e);
            return encodeId(src.getModuleKey()) + "_to_" + encodeId(tgt.getModuleKey())
                    + "_" + e.getScope() + "_" + edgeSeq.incrementAndGet();
        });
        exporter.setEdgeAttributeProvider(e -> {
            Map<String, Attribute> attrs = new LinkedHashMap<>();
            attrs.put("scope",          DefaultAttribute.createAttribute(e.getScope()));
            attrs.put("declaredVersion", DefaultAttribute.createAttribute(
                    e.getDeclaredVersion() != null ? e.getDeclaredVersion() : ""));
            return attrs;
        });

        // Register schema declarations
        exporter.registerAttribute("groupId",        GraphMLExporter.AttributeCategory.NODE, AttributeType.STRING);
        exporter.registerAttribute("artifactId",     GraphMLExporter.AttributeCategory.NODE, AttributeType.STRING);
        exporter.registerAttribute("version",        GraphMLExporter.AttributeCategory.NODE, AttributeType.STRING);
        exporter.registerAttribute("status",         GraphMLExporter.AttributeCategory.NODE, AttributeType.STRING);
        exporter.registerAttribute("projectName",    GraphMLExporter.AttributeCategory.NODE, AttributeType.STRING);
        exporter.registerAttribute("scope",          GraphMLExporter.AttributeCategory.EDGE, AttributeType.STRING);
        exporter.registerAttribute("declaredVersion", GraphMLExporter.AttributeCategory.EDGE, AttributeType.STRING);

        Path tmp = outputFile.resolveSibling(outputFile.getFileName() + ".tmp");
        try (Writer writer = new OutputStreamWriter(
                Files.newOutputStream(tmp), StandardCharsets.UTF_8)) {
            exporter.exportGraph(moduleGraph.getInternalGraph(), writer);
        }
        Files.move(tmp, outputFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        log.debug("Saved module graph to {}: {} nodes, {} edges",
                outputFile, moduleGraph.nodeCount(), moduleGraph.edgeCount());
    }

    // ---------------------------------------------------------------------------
    // Load
    // ---------------------------------------------------------------------------

    /**
     * Load a module graph from a GraphML file.
     * Returns an empty {@link ModuleGraph} if the file does not exist.
     */
    public ModuleGraph load(Path inputFile) throws IOException {
        if (!Files.exists(inputFile)) {
            return new ModuleGraph();
        }

        // Phase 1: import into a plain String/String graph, capturing all attributes
        Map<String, Map<String, String>> vertexAttrs = new HashMap<>();
        Map<String, Map<String, String>> edgeAttrs   = new HashMap<>();

        AtomicInteger edgeIdGen = new AtomicInteger();
        DirectedMultigraph<String, String> tmpGraph =
                new DirectedMultigraph<>(null, () -> "e" + edgeIdGen.incrementAndGet(), false);

        GraphMLImporter<String, String> importer = new GraphMLImporter<>();
        importer.setVertexFactory(id -> id);
        importer.addVertexAttributeConsumer((pair, attr) ->
                vertexAttrs.computeIfAbsent(pair.getFirst(), k -> new LinkedHashMap<>())
                        .put(pair.getSecond(), attr.getValue()));
        importer.addEdgeAttributeConsumer((pair, attr) ->
                edgeAttrs.computeIfAbsent(pair.getFirst().toString(), k -> new LinkedHashMap<>())
                        .put(pair.getSecond(), attr.getValue()));

        try (Reader reader = new InputStreamReader(
                Files.newInputStream(inputFile), StandardCharsets.UTF_8)) {
            importer.importGraph(tmpGraph, reader);
        }

        // Phase 2: reconstruct ModuleGraph from attributes
        ModuleGraph graph = new ModuleGraph();
        Map<String, ModuleNode> idToNode = new HashMap<>();

        for (String encodedId : tmpGraph.vertexSet()) {
            Map<String, String> attrs = vertexAttrs.getOrDefault(encodedId, Map.of());
            String groupId     = attrs.getOrDefault("groupId",    "unknown");
            String artifactId  = attrs.getOrDefault("artifactId", "unknown");
            String version     = attrs.getOrDefault("version",    "unknown");
            String statusStr   = attrs.getOrDefault("status",     "EXTERNAL");
            String projectName = attrs.getOrDefault("projectName", "");

            ModuleNode.Status status;
            try {
                status = ModuleNode.Status.valueOf(statusStr);
            } catch (IllegalArgumentException e) {
                status = ModuleNode.Status.EXTERNAL;
            }

            ModuleNode node = (status == ModuleNode.Status.INDEXED && !projectName.isEmpty())
                    ? ModuleNode.indexed(groupId, artifactId, version, projectName)
                    : ModuleNode.external(groupId, artifactId, version);

            ModuleNode canonical = graph.addOrUpdate(node);
            idToNode.put(encodedId, canonical);
        }

        for (String edgeId : tmpGraph.edgeSet()) {
            String srcId = tmpGraph.getEdgeSource(edgeId);
            String tgtId = tmpGraph.getEdgeTarget(edgeId);
            Map<String, String> attrs = edgeAttrs.getOrDefault(edgeId, Map.of());

            String scope          = attrs.getOrDefault("scope", "compile");
            String declaredVersion = attrs.get("declaredVersion");
            if ("".equals(declaredVersion)) declaredVersion = null;

            ModuleNode src = idToNode.get(srcId);
            ModuleNode tgt = idToNode.get(tgtId);
            if (src != null && tgt != null) {
                graph.addDependency(src, tgt, new ModuleDep(scope, declaredVersion));
            }
        }

        log.debug("Loaded module graph from {}: {} nodes, {} edges",
                inputFile, graph.nodeCount(), graph.edgeCount());
        return graph;
    }
}
