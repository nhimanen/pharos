package com.pharos.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Reads Node.js/JavaScript project metadata from {@code package.json}.
 * Produces a {@link MavenPomReader.PomInfo} so the project can be registered
 * in the module dependency graph alongside Maven, Gradle, and Python projects.
 */
public class PackageJsonReader {

    private static final Logger log = LoggerFactory.getLogger(PackageJsonReader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String GROUP = "npm";

    public static boolean isNodeProject(Path projectRoot) {
        return Files.exists(projectRoot.resolve("package.json"));
    }

    public Optional<MavenPomReader.PomInfo> read(Path projectRoot) {
        Path pkg = projectRoot.resolve("package.json");
        if (!Files.exists(pkg)) return Optional.empty();
        try {
            JsonNode root = MAPPER.readTree(pkg.toFile());
            String name    = text(root, "name", projectRoot.getFileName().toString());
            String version = text(root, "version", "unknown");
            // npm package names can be scoped: @scope/name → scope:name as group:artifact
            String group, artifact;
            if (name.startsWith("@") && name.contains("/")) {
                group    = name.substring(1, name.indexOf('/'));
                artifact = name.substring(name.indexOf('/') + 1);
            } else {
                group    = GROUP;
                artifact = name;
            }

            List<MavenPomReader.MavenDependency> deps = new ArrayList<>();
            for (String depField : List.of("dependencies", "peerDependencies")) {
                JsonNode depNode = root.path(depField);
                if (depNode.isObject()) {
                    depNode.fields().forEachRemaining(e -> deps.add(
                            new MavenPomReader.MavenDependency(
                                    GROUP, e.getKey().replaceAll("^@[^/]+/", ""),
                                    e.getValue().asText(null), "runtime")));
                }
            }

            log.debug("package.json project detected: {}:{} v{}", group, artifact, version);
            return Optional.of(new MavenPomReader.PomInfo(
                    new MavenPomReader.MavenCoordinates(group, artifact, version),
                    deps, List.of()));
        } catch (Exception e) {
            log.debug("Failed to read package.json at {}: {}", projectRoot, e.getMessage());
            return Optional.empty();
        }
    }

    private static String text(JsonNode node, String field, String fallback) {
        JsonNode n = node.path(field);
        return n.isTextual() ? n.asText() : fallback;
    }
}
