package com.pharos.parser;

import com.pharos.parser.model.ParsedFile;
import com.pharos.parser.model.ParsedProject;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Language-agnostic code parser interface.
 * Currently implemented by JavaCodeParser; extensible to other languages.
 */
public interface CodeParser {

    /**
     * Parse a single source file.
     *
     * @param file        absolute path to the source file
     * @param projectName logical project name (used in IDs)
     * @return parsed file with all classes and methods
     */
    ParsedFile parseFile(Path file, String projectName) throws IOException;

    /**
     * Parse all source files in a project directory tree.
     *
     * @param projectRoot root directory of the project (e.g., src/main/java or the repo root)
     * @param projectName logical project name
     * @return aggregated parsed project
     */
    ParsedProject parseProject(Path projectRoot, String projectName) throws IOException;

    /**
     * Parse a pre-collected list of source files.
     *
     * <p>This overload is called by the shared-manifest path in {@code ProjectIndexManager},
     * which performs a single {@code walkFileTree} across all parsers and dispatches files
     * by extension.  Implementations may reuse internal parser state (e.g. a shared
     * {@code CombinedTypeSolver}) across the batch rather than recreating it per file.
     *
     * <p>The default implementation simply calls {@link #parseFile} for each path.
     */
    default ParsedProject parseFiles(List<Path> files, Path projectRoot,
                                     String projectName) throws IOException {
        List<ParsedFile> results = new java.util.ArrayList<>();
        for (Path file : files) {
            try {
                results.add(parseFile(file, projectName));
            } catch (Exception e) {
                // log via the implementation's own logger; we can't log here without a field
            }
        }
        return new ParsedProject(projectName, projectRoot.toString(), results);
    }

    /**
     * Returns file extensions handled by this parser (e.g., [".java"]).
     */
    List<String> supportedExtensions();
}
