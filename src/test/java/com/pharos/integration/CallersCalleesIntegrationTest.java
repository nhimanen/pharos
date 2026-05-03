package com.pharos.integration;

import com.pharos.graph.CallGraph;
import com.pharos.graph.CallGraphBuilder;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.model.ParsedMethod;
import com.pharos.parser.model.ParsedProject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for "where is this method used?" (callers) and
 * "what does this method call?" (callees) queries.
 *
 * Parses the sample-app test project, builds a real CallGraph, and
 * verifies that the graph reflects the actual inter-method calls in
 * GreetingService → User.
 */
class CallersCalleesIntegrationTest {

    private static CallGraph graph;

    // Resolved FQNs — discovered from the parsed project rather than hard-coded
    private static String getName;
    private static String getEmail;
    private static String greet;
    private static String farewell;
    private static String formalGreet;

    @BeforeAll
    static void buildGraph() throws Exception {
        URL resource = CallersCalleesIntegrationTest.class
                .getClassLoader().getResource("test-projects/sample-app");
        assertThat(resource).as("sample-app test project not found").isNotNull();
        Path projectRoot = Paths.get(resource.toURI());

        JavaCodeParser parser = new JavaCodeParser();
        ParsedProject project = parser.parseProject(projectRoot, "sample-app");

        graph = new CallGraph();
        new CallGraphBuilder().build(graph, project);

        // Resolve FQNs from parsed methods so we don't hard-code parameter formats
        getName    = fqn(project, "User",           "getName");
        getEmail   = fqn(project, "User",           "getEmail");
        greet      = fqn(project, "GreetingService","greet");
        farewell   = fqn(project, "GreetingService","farewell");
        formalGreet= fqn(project, "GreetingService","formalGreet");
    }

    // --- all methods appear as nodes ---

    @Test
    void graph_containsAllParsedMethods() {
        assertThat(graph.contains(getName)).isTrue();
        assertThat(graph.contains(getEmail)).isTrue();
        assertThat(graph.contains(greet)).isTrue();
        assertThat(graph.contains(farewell)).isTrue();
        assertThat(graph.contains(formalGreet)).isTrue();
    }

    // --- callees: "what does this method call?" ---

    @Test
    void callees_ofGreet_includesGetName() {
        Set<String> callees = graph.getCallees(greet);
        assertThat(callees).contains(getName);
    }

    @Test
    void callees_ofFarewell_includesGetName() {
        Set<String> callees = graph.getCallees(farewell);
        assertThat(callees).contains(getName);
    }

    @Test
    void callees_ofFormalGreet_includesGetNameAndGetEmail() {
        Set<String> callees = graph.getCallees(formalGreet);
        assertThat(callees)
                .contains(getName)
                .contains(getEmail);
    }

    @Test
    void callees_ofGetName_isEmpty() {
        // getName() is a simple field accessor — it calls nothing
        assertThat(graph.getCallees(getName)).isEmpty();
    }

    // --- callers: "where is this method used?" ---

    @Test
    void callers_ofGetName_includesAllThreeGreetingMethods() {
        Set<String> callers = graph.getCallers(getName);
        assertThat(callers)
                .contains(greet)
                .contains(farewell)
                .contains(formalGreet);
    }

    @Test
    void callers_ofGetEmail_includesFormalGreet() {
        Set<String> callers = graph.getCallers(getEmail);
        assertThat(callers).contains(formalGreet);
    }

    @Test
    void callers_ofGetEmail_doesNotIncludeGreetOrFarewell() {
        Set<String> callers = graph.getCallers(getEmail);
        assertThat(callers)
                .doesNotContain(greet)
                .doesNotContain(farewell);
    }

    @Test
    void callers_ofUnknownFqn_returnsEmpty() {
        assertThat(graph.getCallers("com.example.DoesNotExist#phantom()")).isEmpty();
    }

    // --- helper ---

    private static String fqn(ParsedProject project, String className, String methodName) {
        return project.allMethods().stream()
                .filter(m -> m.className().equals(className) && m.methodName().equals(methodName))
                .map(ParsedMethod::fqn)
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "Could not find method " + className + "#" + methodName + " in parsed project"));
    }
}
