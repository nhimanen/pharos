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
     * Returns file extensions handled by this parser (e.g., [".java"]).
     */
    List<String> supportedExtensions();
}
