package com.pharos.config;

import com.pharos.parser.model.CallReference;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Metadata for an indexed project, stored in the global registry.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProjectMeta {

    private String name;
    private String rootPath;
    private String indexPath;
    private Instant lastIndexed;
    private int methodCount;
    private int classCount;
    private int fileCount;
    private List<String> knownPackages;    // for cross-project resolution heuristics
    private List<String> linkedProjects;   // explicitly linked projects
    private List<UnresolvedRef> unresolvedRefs; // saved for later cross-project linking

    // Maven coordinates — null when no pom.xml was found during indexing
    private String groupId;
    private String artifactId;
    private String mavenVersion;   // named mavenVersion to avoid naming ambiguity

    public ProjectMeta() {
        this.knownPackages = new ArrayList<>();
        this.linkedProjects = new ArrayList<>();
        this.unresolvedRefs = new ArrayList<>();
    }

    public ProjectMeta(String name, String rootPath, String indexPath) {
        this();
        this.name = name;
        this.rootPath = rootPath;
        this.indexPath = indexPath;
    }

    /**
     * Simplified unresolved call ref for JSON serialization.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class UnresolvedRef {
        public String callerFqn;
        public String calleeMethodName;
        public int line;

        public UnresolvedRef() {}
        public UnresolvedRef(String callerFqn, String calleeMethodName, int line) {
            this.callerFqn = callerFqn;
            this.calleeMethodName = calleeMethodName;
            this.line = line;
        }

        public static UnresolvedRef from(CallReference ref) {
            return new UnresolvedRef(ref.callerFqn(), ref.calleeSimpleName(), ref.lineNumber());
        }
    }

    // Getters and setters
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getRootPath() { return rootPath; }
    public void setRootPath(String rootPath) { this.rootPath = rootPath; }

    public String getIndexPath() { return indexPath; }
    public void setIndexPath(String indexPath) { this.indexPath = indexPath; }

    public Instant getLastIndexed() { return lastIndexed; }
    public void setLastIndexed(Instant lastIndexed) { this.lastIndexed = lastIndexed; }

    public int getMethodCount() { return methodCount; }
    public void setMethodCount(int methodCount) { this.methodCount = methodCount; }

    public int getClassCount() { return classCount; }
    public void setClassCount(int classCount) { this.classCount = classCount; }

    public int getFileCount() { return fileCount; }
    public void setFileCount(int fileCount) { this.fileCount = fileCount; }

    public List<String> getKnownPackages() { return knownPackages; }
    public void setKnownPackages(List<String> knownPackages) { this.knownPackages = knownPackages; }

    public List<String> getLinkedProjects() { return linkedProjects; }
    public void setLinkedProjects(List<String> linkedProjects) { this.linkedProjects = linkedProjects; }

    public List<UnresolvedRef> getUnresolvedRefs() { return unresolvedRefs; }
    public void setUnresolvedRefs(List<UnresolvedRef> unresolvedRefs) { this.unresolvedRefs = unresolvedRefs; }

    public String getGroupId()              { return groupId; }
    public void setGroupId(String groupId)  { this.groupId = groupId; }

    public String getArtifactId()              { return artifactId; }
    public void setArtifactId(String aid)      { this.artifactId = aid; }

    public String getMavenVersion()               { return mavenVersion; }
    public void setMavenVersion(String version)   { this.mavenVersion = version; }
}
