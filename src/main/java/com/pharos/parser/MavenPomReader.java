package com.pharos.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Reads Maven pom.xml files using the JDK DOM parser (no extra dependencies).
 * Extracts coordinates, dependency declarations, and multi-module structure.
 *
 * Limitations (by design — out of scope):
 * - Does NOT resolve ${property} expressions in version strings
 * - Does NOT follow parent POM chains beyond the immediate <parent> block
 * - Does NOT parse <dependencyManagement> as actual dependencies
 */
public class MavenPomReader {

    private static final Logger log = LoggerFactory.getLogger(MavenPomReader.class);

    // ---------------------------------------------------------------------------
    // Public data types
    // ---------------------------------------------------------------------------

    public record MavenCoordinates(String groupId, String artifactId, String version) {
        /** Stable identity key — version is intentionally excluded. */
        public String moduleKey() { return groupId + ":" + artifactId; }
        /** Full GAV string. */
        public String gav()       { return groupId + ":" + artifactId + ":" + version; }
    }

    public record MavenDependency(
            String groupId,
            String artifactId,
            String version,   // may be null (managed/property) or a ${...} expression
            String scope      // null → treat as "compile"
    ) {
        public String moduleKey() { return groupId + ":" + artifactId; }
        public String effectiveScope() { return scope != null ? scope : "compile"; }
    }

    public record PomInfo(
            MavenCoordinates coordinates,
            List<MavenDependency> dependencies,
            List<String> modules    // relative submodule paths from <modules>
    ) {
        public boolean isMultiModule() { return !modules.isEmpty(); }
    }

    // ---------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------

    /**
     * Locates a pom.xml at {@code projectRoot/pom.xml}.
     * Returns empty if no pom.xml exists.
     */
    public static Optional<Path> findPom(Path projectRoot) {
        Path pom = projectRoot.resolve("pom.xml");
        return Files.exists(pom) ? Optional.of(pom) : Optional.empty();
    }

    /**
     * Parses the given pom.xml file.
     * Returns {@code Optional.empty()} on any parse failure (never throws).
     */
    public Optional<PomInfo> read(Path pomFile) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            // Disable external entity resolution for safety
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            Document doc = factory.newDocumentBuilder().parse(pomFile.toFile());
            doc.getDocumentElement().normalize();
            return Optional.of(parse(doc));
        } catch (Exception e) {
            log.warn("Failed to parse pom.xml at {}: {}", pomFile, e.getMessage());
            return Optional.empty();
        }
    }

    // ---------------------------------------------------------------------------
    // Private parsing logic
    // ---------------------------------------------------------------------------

    private PomInfo parse(Document doc) {
        Element root = doc.getDocumentElement();

        // Own coordinates — groupId/version may be absent and inherited from <parent>
        String groupId    = directChild(root, "groupId");
        String artifactId = directChild(root, "artifactId");
        String version    = directChild(root, "version");

        // Inherit from <parent> when missing
        NodeList parentNodes = root.getElementsByTagName("parent");
        if (parentNodes.getLength() > 0) {
            Element parent = (Element) parentNodes.item(0);
            if (groupId == null)  groupId  = directChild(parent, "groupId");
            if (version == null)  version  = directChild(parent, "version");
        }

        MavenCoordinates coords = new MavenCoordinates(
                groupId    != null ? groupId    : "unknown",
                artifactId != null ? artifactId : "unknown",
                version    != null ? version    : "unknown"
        );

        // Dependencies — skip anything inside <dependencyManagement>
        List<MavenDependency> deps = new ArrayList<>();
        NodeList depNodes = root.getElementsByTagName("dependency");
        for (int i = 0; i < depNodes.getLength(); i++) {
            if (!(depNodes.item(i) instanceof Element dep)) continue;
            if (insideManagement(dep)) continue;

            String dg = directChild(dep, "groupId");
            String da = directChild(dep, "artifactId");
            if (dg == null || da == null) continue;

            deps.add(new MavenDependency(
                    dg, da,
                    directChild(dep, "version"),   // null = managed by BOM
                    directChild(dep, "scope")      // null = compile
            ));
        }

        // Multi-module submodule paths
        List<String> submodules = new ArrayList<>();
        NodeList moduleNodes = root.getElementsByTagName("module");
        for (int i = 0; i < moduleNodes.getLength(); i++) {
            String text = moduleNodes.item(i).getTextContent().trim();
            if (!text.isEmpty()) submodules.add(text);
        }

        return new PomInfo(coords, deps, submodules);
    }

    /**
     * Returns the text content of the first *direct* child element with {@code tag},
     * or null if absent or empty.
     */
    private static String directChild(Element parent, String tag) {
        NodeList nodes = parent.getElementsByTagName(tag);
        for (int i = 0; i < nodes.getLength(); i++) {
            org.w3c.dom.Node node = nodes.item(i);
            if (node.getParentNode() == parent) {
                String text = node.getTextContent().trim();
                return text.isEmpty() ? null : text;
            }
        }
        return null;
    }

    /** Returns true if the element is a descendant of a {@code <dependencyManagement>} element. */
    private static boolean insideManagement(Element dep) {
        org.w3c.dom.Node parent = dep.getParentNode();
        while (parent != null) {
            if (parent instanceof Element e &&
                    "dependencyManagement".equals(e.getTagName())) {
                return true;
            }
            parent = parent.getParentNode();
        }
        return false;
    }
}
