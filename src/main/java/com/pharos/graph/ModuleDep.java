package com.pharos.graph;

import org.jgrapht.graph.DefaultEdge;

/**
 * A directed dependency edge in the module graph.
 * Direction: source DEPENDS ON target.
 *
 * Scope follows Maven conventions: compile, test, provided, runtime.
 * A null scope is treated as "compile" by {@link #effectiveScope()}.
 */
public class ModuleDep extends DefaultEdge {

    private final String scope;
    /** The version string as declared in the depending module's pom.xml.
     *  May be null (version managed by parent BOM) or a ${property} expression. */
    private final String declaredVersion;

    public ModuleDep(String scope, String declaredVersion) {
        this.scope          = scope != null ? scope : "compile";
        this.declaredVersion = declaredVersion;
    }

    public static ModuleDep of(String scope, String declaredVersion) {
        return new ModuleDep(scope, declaredVersion);
    }

    public static ModuleDep compile(String declaredVersion) {
        return new ModuleDep("compile", declaredVersion);
    }

    public static ModuleDep test(String declaredVersion) {
        return new ModuleDep("test", declaredVersion);
    }

    public String getScope()           { return scope; }
    public String effectiveScope()     { return scope; }
    public String getDeclaredVersion() { return declaredVersion; }

    @Override
    public String toString() {
        return "(" + getSource() + " -[" + scope + "]-> " + getTarget() + ")";
    }
}
