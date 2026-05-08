package com.pharos.config;

import com.pharos.parser.model.CallReference;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * Stable hash of {@link #unresolvedRefs} contents — used by the cross-project linker
     * guard to skip expensive graph linking when nothing has changed since the last run.
     * Zero means "not yet computed" (pre-existing index).
     */
    private int unresolvedRefsHash;

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
        /** Simple class name of the receiver (e.g. "IndexWriter"), null if unknown. */
        public String receiverTypeName;
        /** Number of arguments at the call site, 0 if unknown. */
        public int paramCount;
        /**
         * Package inferred from imports: if the file importing this class had
         * {@code import org.apache.lucene.index.IndexWriter}, this is {@code "org.apache.lucene.index"}.
         * Null when no matching import was found.
         */
        public String packageHint;
        public int line;

        public UnresolvedRef() {}

        /** Minimal constructor for tests and legacy callers. */
        public UnresolvedRef(String callerFqn, String calleeMethodName, int line) {
            this(callerFqn, calleeMethodName, null, 0, null, line);
        }

        public UnresolvedRef(String callerFqn, String calleeMethodName,
                             String receiverTypeName, int paramCount, String packageHint, int line) {
            this.callerFqn = callerFqn;
            this.calleeMethodName = calleeMethodName;
            this.receiverTypeName = receiverTypeName;
            this.paramCount = paramCount;
            this.packageHint = packageHint;
            this.line = line;
        }

        /**
         * Build an {@link UnresolvedRef} from a call reference, resolving the package hint
         * from the file's import list.
         *
         * @param ref         the unresolved call reference
         * @param fileImports all import names declared in the file (fully-qualified or wildcard)
         */
        public static UnresolvedRef from(CallReference ref, List<String> fileImports) {
            String packageHint = resolvePackageHint(ref.receiverTypeName(), fileImports);
            return new UnresolvedRef(ref.callerFqn(), ref.calleeSimpleName(),
                    ref.receiverTypeName(), ref.paramCount(), packageHint, ref.lineNumber());
        }

        /** Backward-compatible factory without import context. */
        public static UnresolvedRef from(CallReference ref) {
            return from(ref, List.of());
        }

        private static String resolvePackageHint(String receiverTypeName, List<String> imports) {
            if (receiverTypeName == null || imports.isEmpty()) return null;
            for (String imp : imports) {
                if (imp.endsWith("." + receiverTypeName)) {
                    // Exact import: "org.apache.lucene.index.IndexWriter" → "org.apache.lucene.index"
                    return imp.substring(0, imp.lastIndexOf('.'));
                }
            }
            // No exact match — wildcard imports can't pinpoint the package without resolving
            return null;
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

    public int getUnresolvedRefsHash()                { return unresolvedRefsHash; }
    public void setUnresolvedRefsHash(int hash)       { this.unresolvedRefsHash = hash; }
}
