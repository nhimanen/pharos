package com.pharos.parser;

import java.util.List;

/**
 * Terraform (.tf) parser — delegates to {@code terraform-extractor.py} via subprocess.
 *
 * <p>Terraform has no OOP structure, so every HCL block (resource, data, module,
 * variable, output, provider, and each key inside a locals block) is emitted by
 * the extractor as a top-level "function" with {@code class_name=null}, attached
 * to a synthetic per-file module pseudo-class by {@link ScriptBasedCodeParser}.
 *
 * <p>Requires the third-party {@code python-hcl2} PyPI package in addition to
 * {@code python3} — see CLAUDE.md.
 */
public class TerraformCodeParser extends ScriptBasedCodeParser {

    private static final List<String> EXTENSIONS = List.of(".tf");

    public TerraformCodeParser() { super(); }

    public TerraformCodeParser(int parseThreads) { super(parseThreads); }

    @Override
    protected String scriptResourceName() {
        return "terraform-extractor.py";
    }

    @Override
    protected List<String> runtimeCommand() {
        return List.of("python3");
    }

    @Override
    public List<String> supportedExtensions() {
        return EXTENSIONS;
    }

    /** Block kind + name prefix already conveys the type; no return-type concept in HCL. */
    @Override
    protected String defaultReturnType() {
        return "";
    }

    /** Signature format: {@code resource aws_instance.web(ami, instance_type)}. */
    @Override
    protected String buildSignatureString(String name, List<String> allParams, List<String> decorators) {
        String kind = decorators.isEmpty() ? "" : decorators.get(0) + " ";
        return kind + name + "(" + String.join(", ", allParams) + ")";
    }
}
