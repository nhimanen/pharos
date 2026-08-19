package com.pharos.parser;

import com.pharos.parser.model.ParsedMethod;
import com.pharos.parser.model.ParsedProject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class TerraformCodeParserTest {

    private static final Path SAMPLE_APP = sampleAppPath();
    private static final String PROJECT = "sample-terraform-app";

    @BeforeAll
    static void requirePythonHcl2() {
        Assumptions.assumeTrue(isPython3Available(), "python3 not found on PATH — skipping Terraform tests");
        Assumptions.assumeTrue(isHcl2Available(), "python-hcl2 not installed — skipping Terraform tests");
    }

    @Test
    void supportedExtensions_includesTf() {
        assertThat(new TerraformCodeParser().supportedExtensions()).contains(".tf");
    }

    @Test
    void parseProject_findsAllTfFiles() throws Exception {
        ParsedProject project = new TerraformCodeParser().parseProject(SAMPLE_APP, PROJECT);

        // variables.tf, main.tf, modules/vpc/main.tf
        assertThat(project.files()).hasSize(3);
    }

    @Test
    void parseProject_extractsResourceBlock() throws Exception {
        ParsedProject project = new TerraformCodeParser().parseProject(SAMPLE_APP, PROJECT);

        List<String> names = project.allMethods().stream().map(ParsedMethod::methodName).toList();
        assertThat(names).contains("aws_instance.web", "aws_vpc.this");
    }

    @Test
    void parseProject_extractsVariableBlock() throws Exception {
        ParsedProject project = new TerraformCodeParser().parseProject(SAMPLE_APP, PROJECT);

        List<String> names = project.allMethods().stream().map(ParsedMethod::methodName).toList();
        assertThat(names).contains("var.region", "var.instance_type");
    }

    @Test
    void parseProject_extractsModuleAndDataAndOutputBlocks() throws Exception {
        ParsedProject project = new TerraformCodeParser().parseProject(SAMPLE_APP, PROJECT);

        List<String> names = project.allMethods().stream().map(ParsedMethod::methodName).toList();
        assertThat(names).contains("module.vpc", "data.aws_ami.app", "output.instance_id");
    }

    @Test
    void parseProject_extractsVariableDescription() throws Exception {
        ParsedProject project = new TerraformCodeParser().parseProject(SAMPLE_APP, PROJECT);

        ParsedMethod region = project.allMethods().stream()
                .filter(m -> m.methodName().equals("var.region"))
                .findFirst().orElseThrow();
        assertThat(region.javadoc()).isEqualTo("AWS region to deploy into");
    }

    @Test
    void parseProject_resourceHasCallReferenceToVariable() throws Exception {
        ParsedProject project = new TerraformCodeParser().parseProject(SAMPLE_APP, PROJECT);

        ParsedMethod web = project.allMethods().stream()
                .filter(m -> m.methodName().equals("aws_instance.web"))
                .findFirst().orElseThrow();
        List<String> callees = web.calledMethods().stream()
                .map(c -> c.calleeSimpleName())
                .toList();
        assertThat(callees).contains("var.instance_type", "data.aws_ami.app");
    }

    @Test
    void parseProject_callReferencesAreUnresolved() throws Exception {
        ParsedProject project = new TerraformCodeParser().parseProject(SAMPLE_APP, PROJECT);

        assertThat(project.allMethods().stream()
                .flatMap(m -> m.calledMethods().stream())
                .filter(c -> c.resolved())
                .count()).isZero();
    }

    @Test
    void parseProject_decoratorReflectsBlockKind() throws Exception {
        ParsedProject project = new TerraformCodeParser().parseProject(SAMPLE_APP, PROJECT);

        ParsedMethod web = project.allMethods().stream()
                .filter(m -> m.methodName().equals("aws_instance.web"))
                .findFirst().orElseThrow();
        assertThat(web.annotations()).contains("resource");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Path sampleAppPath() {
        try {
            var url = TerraformCodeParserTest.class.getClassLoader()
                    .getResource("test-projects/sample-terraform-app");
            if (url == null) throw new RuntimeException("sample-terraform-app test resource not found");
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isPython3Available() {
        try {
            Process p = new ProcessBuilder("python3", "--version")
                    .redirectErrorStream(true).start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isHcl2Available() {
        try {
            Process p = new ProcessBuilder("python3", "-c", "import hcl2")
                    .redirectErrorStream(true).start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
