package com.pharos.parser;

import java.util.List;

/**
 * JavaScript/TypeScript parser — delegates to {@code js-ts-extractor.js} via Node.js.
 *
 * Handles .js, .ts, .jsx, .tsx, .mjs, .cjs files.
 * Requires Node.js to be available on PATH as {@code node}.
 *
 * The extractor uses no npm packages — only built-in Node.js APIs —
 * so no {@code npm install} step is needed.
 */
public class JsCodeParser extends ScriptBasedCodeParser {

    private static final List<String> EXTENSIONS =
            List.of(".js", ".ts", ".jsx", ".tsx", ".mjs", ".cjs");

    public JsCodeParser() { super(); }

    public JsCodeParser(int parseThreads) { super(parseThreads); }

    @Override
    protected String scriptResourceName() {
        return "js-ts-extractor.js";
    }

    @Override
    protected List<String> runtimeCommand() {
        return List.of("node");
    }

    @Override
    public List<String> supportedExtensions() {
        return EXTENSIONS;
    }

    /** TypeScript/JS return types are unresolvable without a full compiler; use "any". */
    @Override
    protected String defaultReturnType() {
        return "any";
    }

    /**
     * Signature format: {@code functionName(param1, param2)}.
     * TypeScript type annotations are stripped by the extractor before params arrive here.
     */
    @Override
    protected String buildSignatureString(String name, List<String> allParams, List<String> decorators) {
        String prefix = decorators.isEmpty() ? "" : "@" + String.join(" @", decorators) + " ";
        return prefix + name + "(" + String.join(", ", allParams) + ")";
    }
}
