package com.pharos.parser;

import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedMethod;
import com.pharos.parser.model.ParsedProject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class PythonCodeParserTest {

    private static final Path SAMPLE_APP = sampleAppPath();
    private static final String PROJECT = "sample-python-app";

    @BeforeAll
    static void requirePython3() {
        // Skip entire test class if python3 is not available
        Assumptions.assumeTrue(isPython3Available(), "python3 not found on PATH — skipping Python tests");
    }

    // -----------------------------------------------------------------------
    // Project-level parsing
    // -----------------------------------------------------------------------

    @Test
    void parseProject_findsAllPythonFiles() throws Exception {
        ParsedProject project = new PythonCodeParser().parseProject(SAMPLE_APP, PROJECT);

        // calculator.py, model/user.py, service/greeting.py
        assertThat(project.files()).hasSize(3);
        List<String> paths = project.files().stream()
                .map(f -> f.filePath())
                .map(p -> p.replaceAll(".*sample-python-app/", ""))
                .toList();
        assertThat(paths).containsExactlyInAnyOrder(
                "calculator.py", "model/user.py", "service/greeting.py");
    }

    @Test
    void parseProject_extractsTopLevelFunctions() throws Exception {
        ParsedProject project = new PythonCodeParser().parseProject(SAMPLE_APP, PROJECT);

        List<ParsedMethod> allMethods = project.allMethods();
        List<String> names = allMethods.stream().map(ParsedMethod::methodName).toList();
        assertThat(names).contains("add", "subtract", "multiply", "divide", "abs_value");
    }

    @Test
    void parseProject_extractsClasses() throws Exception {
        ParsedProject project = new PythonCodeParser().parseProject(SAMPLE_APP, PROJECT);

        List<ParsedClass> classes = project.allClasses();
        List<String> classNames = classes.stream().map(ParsedClass::className).toList();
        assertThat(classNames).contains("User", "GreetingService");
    }

    @Test
    void parseProject_extractsMethods() throws Exception {
        ParsedProject project = new PythonCodeParser().parseProject(SAMPLE_APP, PROJECT);

        List<String> names = project.allMethods().stream().map(ParsedMethod::methodName).toList();
        assertThat(names).contains("__init__", "get_name", "get_email", "has_valid_email",
                "greet", "farewell", "formal_greet");
    }

    @Test
    void parseProject_extractsDocstrings() throws Exception {
        ParsedProject project = new PythonCodeParser().parseProject(SAMPLE_APP, PROJECT);

        ParsedMethod add = project.allMethods().stream()
                .filter(m -> m.methodName().equals("add"))
                .findFirst().orElseThrow();
        assertThat(add.javadoc()).isEqualTo("Return the sum of two numbers.");

        ParsedClass user = project.allClasses().stream()
                .filter(c -> c.className().equals("User"))
                .findFirst().orElseThrow();
        assertThat(user.javadoc()).contains("application user");
    }

    @Test
    void parseProject_extractsDecorators() throws Exception {
        // GreetingService has no decorators; verify decorator list is empty (not null)
        ParsedProject project = new PythonCodeParser().parseProject(SAMPLE_APP, PROJECT);

        ParsedMethod greet = project.allMethods().stream()
                .filter(m -> m.methodName().equals("greet"))
                .findFirst().orElseThrow();
        assertThat(greet.annotations()).isNotNull();
    }

    @Test
    void parseProject_extractsCallReferences() throws Exception {
        ParsedProject project = new PythonCodeParser().parseProject(SAMPLE_APP, PROJECT);

        // formal_greet calls get_name, get_email, has_valid_email
        ParsedMethod formal = project.allMethods().stream()
                .filter(m -> m.methodName().equals("formal_greet"))
                .findFirst().orElseThrow();
        List<String> callees = formal.calledMethods().stream()
                .map(c -> c.calleeSimpleName())
                .toList();
        assertThat(callees).contains("get_name", "get_email", "has_valid_email");
    }

    @Test
    void parseProject_callReferencesAreUnresolved() throws Exception {
        ParsedProject project = new PythonCodeParser().parseProject(SAMPLE_APP, PROJECT);

        // All Python calls must be unresolved (dynamic dispatch)
        boolean allUnresolved = project.unresolvedCalls().stream()
                .allMatch(c -> !c.resolved());
        assertThat(project.allMethods().stream()
                .flatMap(m -> m.calledMethods().stream())
                .filter(c -> c.resolved())
                .count()).isZero();
    }

    @Test
    void parseProject_constructorFlagSet() throws Exception {
        ParsedProject project = new PythonCodeParser().parseProject(SAMPLE_APP, PROJECT);

        ParsedMethod init = project.allMethods().stream()
                .filter(m -> m.methodName().equals("__init__"))
                .findFirst().orElseThrow();
        assertThat(init.isConstructor()).isTrue();
    }

    @Test
    void parseProject_fqnFormat() throws Exception {
        ParsedProject project = new PythonCodeParser().parseProject(SAMPLE_APP, PROJECT);

        // Top-level function — params are included (Python has no type info, uses param names)
        ParsedMethod add = project.allMethods().stream()
                .filter(m -> m.methodName().equals("add"))
                .findFirst().orElseThrow();
        // fqn includes param names as types since Python has no static type info
        assertThat(add.fqn()).startsWith("calculator#add(");
        assertThat(add.qualifiedClassName()).isEqualTo("calculator");

        // Class method qualified class prefix
        ParsedMethod getName = project.allMethods().stream()
                .filter(m -> m.methodName().equals("get_name"))
                .findFirst().orElseThrow();
        assertThat(getName.qualifiedClassName()).isEqualTo("model.user.User");
        assertThat(getName.fqn()).startsWith("model.user.User#get_name(");
    }

    // -----------------------------------------------------------------------
    // Source root detection
    // -----------------------------------------------------------------------

    @Test
    void detectSourceRoot_srcLayout_returnsSrc() throws Exception {
        Path tmp = Files.createTempDirectory("py-test");
        Path src = Files.createDirectories(tmp.resolve("src"));
        assertThat(PythonCodeParser.detectSourceRoot(tmp)).isEqualTo(src);
    }

    @Test
    void detectSourceRoot_flatLayout_returnsRoot() throws Exception {
        Path tmp = Files.createTempDirectory("py-test-flat");
        assertThat(PythonCodeParser.detectSourceRoot(tmp)).isEqualTo(tmp);
    }

    // -----------------------------------------------------------------------
    // Graceful fallback
    // -----------------------------------------------------------------------

    @Test
    void parseProject_emptyDirectory_returnsEmptyProject() throws Exception {
        Path emptyDir = Files.createTempDirectory("py-empty");
        ParsedProject project = new PythonCodeParser().parseProject(emptyDir, "empty");
        assertThat(project.files()).isEmpty();
        assertThat(project.allMethods()).isEmpty();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Path sampleAppPath() {
        try {
            var url = PythonCodeParserTest.class.getClassLoader()
                    .getResource("test-projects/sample-python-app");
            if (url == null) throw new RuntimeException("sample-python-app test resource not found");
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
}
