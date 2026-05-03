package com.pharos.parser.model;

import java.util.List;

/**
 * Represents a parsed Java method or constructor with full context.
 * This is the primary indexable unit in the search engine.
 *
 * Models the same richness as mcp_server_code_extractor's CodeSymbol:
 * - name, kind, location (start/end line)
 * - parent (class context)
 * - parameters (name + type)
 * - return type, javadoc, annotations, access modifier
 * - call references (outgoing calls)
 */
public record ParsedMethod(
        String id,                    // "project:com.example.MyClass#myMethod(String,int)"
        String projectName,
        String packageName,
        String className,             // simple class name
        String qualifiedClassName,    // fully-qualified class name
        String methodName,
        String signature,             // "public String myMethod(String arg, int count)"
        String returnType,            // "String", "void", "List<String>", etc.
        List<String> paramTypes,      // ["String", "int"]
        List<String> paramNames,      // ["arg", "count"]
        String body,                  // full method body text (including braces)
        String javadoc,               // javadoc comment text, null if none
        List<String> annotations,     // ["@Override", "@Deprecated"]
        String accessModifier,        // "public", "private", "protected", "package-private"
        boolean isStatic,
        boolean isConstructor,
        boolean isAbstract,
        boolean isSynchronized,
        List<String> thrownExceptions, // ["IOException", "IllegalArgumentException"]
        List<CallReference> calledMethods, // outgoing call references
        String filePath,              // absolute path to source file
        int startLine,
        int endLine
) {
    /** Builds the canonical ID for a method. */
    public static String buildId(String projectName, String qualifiedClassName, String methodName, List<String> paramTypes) {
        String params = String.join(",", paramTypes);
        return projectName + ":" + qualifiedClassName + "#" + methodName + "(" + params + ")";
    }

    /** Returns the FQN without project prefix, e.g. "com.example.MyClass#myMethod(String,int)" */
    public String fqn() {
        String params = String.join(",", paramTypes);
        return qualifiedClassName + "#" + methodName + "(" + params + ")";
    }
}
