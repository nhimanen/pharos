package com.pharos.parser;

import java.nio.file.*;
import java.util.List;

/**
 * Python source code parser — delegates to {@code python-extractor.py} via subprocess.
 *
 * <p>Extends {@link ScriptBasedCodeParser}: all subprocess boilerplate and JSON→model
 * mapping live in the base class. This class provides the Python-specific hooks:
 * "self"/"cls" parameter filtering and the {@code def name(params)} signature format.
 *
 * <p>Call references are all unresolved — Python's dynamic dispatch cannot be
 * statically resolved without a type inference engine.
 */
public class PythonCodeParser extends ScriptBasedCodeParser {

    private static final List<String> EXTENSIONS = List.of(".py");

    @Override
    protected String scriptResourceName() {
        return "python-extractor.py";
    }

    @Override
    protected List<String> runtimeCommand() {
        return List.of("python3");
    }

    @Override
    public List<String> supportedExtensions() {
        return EXTENSIONS;
    }

    /** Remove "self" and "cls" from displayed parameters. */
    @Override
    protected List<String> filterParams(List<String> params) {
        return params.stream()
                .filter(p -> !p.equals("self") && !p.equals("cls"))
                .toList();
    }

    /** Python signature format: {@code @decorator def name(params)}. */
    @Override
    protected String buildSignatureString(String name, List<String> allParams, List<String> decorators) {
        return (decorators.isEmpty() ? "" : "@" + String.join(" @", decorators) + " ")
                + "def " + name + "(" + String.join(", ", allParams) + ")";
    }

    /**
     * Detects a Python project's source root.
     * Checks {@code src/} first (common layout), then falls back to projectRoot.
     */
    public static Path detectSourceRoot(Path projectRoot) {
        Path src = projectRoot.resolve("src");
        if (Files.isDirectory(src)) return src;
        return projectRoot;
    }
}
