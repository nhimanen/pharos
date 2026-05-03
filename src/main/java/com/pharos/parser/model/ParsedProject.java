package com.pharos.parser.model;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Aggregates all parsed files for a project.
 */
public record ParsedProject(
        String projectName,
        String rootPath,
        List<ParsedFile> files
) {
    public List<ParsedMethod> allMethods() {
        return files.stream()
                .flatMap(f -> f.methods().stream())
                .collect(Collectors.toList());
    }

    public List<ParsedClass> allClasses() {
        return files.stream()
                .flatMap(f -> f.classes().stream())
                .collect(Collectors.toList());
    }

    /** All packages present in this project — used for cross-project linking heuristics. */
    public Set<String> knownPackages() {
        return files.stream()
                .map(ParsedFile::packageName)
                .filter(p -> p != null && !p.isEmpty())
                .collect(Collectors.toSet());
    }

    /** All unresolved call references — saved for later cross-project resolution. */
    public List<CallReference> unresolvedCalls() {
        return allMethods().stream()
                .flatMap(m -> m.calledMethods().stream())
                .filter(c -> !c.resolved())
                .collect(Collectors.toList());
    }
}
