package com.pharos.parser.model;

import java.util.List;

/**
 * Represents a parsed Java source file containing one or more classes/interfaces.
 */
public record ParsedFile(
        String filePath,
        String packageName,
        List<String> imports,
        List<ParsedClass> classes,
        List<ParsedMethod> methods
) {}
