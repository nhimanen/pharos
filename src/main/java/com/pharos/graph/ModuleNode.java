package com.pharos.graph;

import java.util.Objects;

/**
 * A node in the module-level dependency graph.
 *
 * Identity is defined by {@code moduleKey = "groupId:artifactId"}.
 * Two nodes with the same moduleKey but different versions are the same graph node —
 * this mirrors Maven's version-conflict resolution (nearest-wins).
 * The {@code version} field is display-only and may change when a node is upgraded.
 *
 * Status:
 *   INDEXED  — source code is in our registry; {@code projectName} is non-null
 *   EXTERNAL — only Maven coordinates known; no source indexed
 */
public final class ModuleNode {

    public enum Status { INDEXED, EXTERNAL }

    private final String moduleKey;    // "groupId:artifactId" — identity
    private final String groupId;
    private final String artifactId;
    private String version;            // display-only, mutable on upgrade
    private Status status;
    private String projectName;        // non-null only when status == INDEXED

    public ModuleNode(String groupId, String artifactId, String version,
                      Status status, String projectName) {
        this.groupId     = Objects.requireNonNull(groupId);
        this.artifactId  = Objects.requireNonNull(artifactId);
        this.version     = version != null ? version : "unknown";
        this.moduleKey   = groupId + ":" + artifactId;
        this.status      = Objects.requireNonNull(status);
        this.projectName = projectName;
    }

    /** Factory for an external dependency (no source indexed). */
    public static ModuleNode external(String groupId, String artifactId, String version) {
        return new ModuleNode(groupId, artifactId, version, Status.EXTERNAL, null);
    }

    /** Factory for an indexed module (source is in ProjectRegistry). */
    public static ModuleNode indexed(String groupId, String artifactId, String version,
                                     String projectName) {
        return new ModuleNode(groupId, artifactId, version, Status.INDEXED,
                Objects.requireNonNull(projectName));
    }

    /**
     * Upgrade this node from EXTERNAL to INDEXED when source is subsequently indexed.
     */
    public void upgrade(String newVersion, String projectName) {
        this.version     = newVersion != null ? newVersion : this.version;
        this.status      = Status.INDEXED;
        this.projectName = Objects.requireNonNull(projectName);
    }

    /**
     * Downgrade this node from INDEXED to EXTERNAL when the project index is removed.
     * Preserves module coordinates and edges so other projects' dependency info remains intact.
     */
    public void downgrade() {
        this.status      = Status.EXTERNAL;
        this.projectName = null;
    }

    // --- Identity: moduleKey only ---

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ModuleNode n)) return false;
        return moduleKey.equals(n.moduleKey);
    }

    @Override
    public int hashCode() { return moduleKey.hashCode(); }

    @Override
    public String toString() {
        return moduleKey + " [" + status +
                (projectName != null ? "/" + projectName : "") + "]";
    }

    // Getters
    public String getModuleKey()   { return moduleKey; }
    public String getGroupId()     { return groupId; }
    public String getArtifactId()  { return artifactId; }
    public String getVersion()     { return version; }
    public Status getStatus()      { return status; }
    public String getProjectName() { return projectName; }
    public boolean isIndexed()     { return status == Status.INDEXED; }
}
