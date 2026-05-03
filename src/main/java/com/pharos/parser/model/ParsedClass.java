package com.pharos.parser.model;

import java.util.List;

/**
 * Represents a parsed Java class, interface, enum, or record.
 * Used as context when extracting methods.
 */
public record ParsedClass(
        String projectName,
        String packageName,
        String className,           // simple name
        String qualifiedClassName,  // fully-qualified name
        String kind,                // "class", "interface", "enum", "record", "annotation"
        String superclass,          // null for Object or unknown
        List<String> interfaces,    // implemented interfaces
        List<String> annotations,
        String accessModifier,      // "public", "protected", "package-private"
        boolean isAbstract,
        boolean isStatic,           // for nested static classes
        String javadoc,             // class-level Javadoc comment, null if absent
        String filePath,
        int startLine,
        int endLine
) {}
