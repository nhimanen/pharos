package com.pharos.graph;

import com.pharos.parser.model.ParsedRelationships;
import com.pharos.parser.model.ParsedRelationships.FieldAccess;
import com.pharos.parser.model.ParsedRelationships.FieldDecl;
import com.pharos.parser.model.ParsedRelationships.TypeEdge;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link KnowledgeGraphBuilder}.
 *
 * Each test opens an isolated ArcadeDB instance, pre-populates Method vertices
 * (required before KG edges that touch methods), then calls
 * {@link KnowledgeGraphBuilder#build(CallGraph, ParsedRelationships)} and asserts
 * the expected vertices/edges appear via the CallGraph query APIs.
 */
class KnowledgeGraphBuilderTest {

    @TempDir
    Path tempDir;

    private final KnowledgeGraphBuilder builder = new KnowledgeGraphBuilder();

    // -------------------------------------------------------------------------
    // null / empty guard
    // -------------------------------------------------------------------------

    @Test
    void build_nullRelationships_isNoOp() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("null-rel.arcadedb"))) {
            assertThatCode(() -> builder.build(g, null)).doesNotThrowAnyException();
        }
    }

    @Test
    void build_emptyRelationships_producesNoVerticesOrEdges() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("empty-rel.arcadedb"))) {
            builder.build(g, ParsedRelationships.empty("test-project"));

            // No vertices created → all queries return empty
            assertThat(g.directSubclasses("com.example.Any")).isEmpty();
            assertThat(g.declaredFields("com.example.Any")).isEmpty();
        }
    }

    // -------------------------------------------------------------------------
    // inherits edges
    // -------------------------------------------------------------------------

    @Test
    void build_inheritsEdge_populatesDirectSubclasses() {
        var rel = new ParsedRelationships(
                "proj",
                List.of(new TypeEdge("com.example.Dog", "com.example.Animal")),
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of());

        try (CallGraph g = CallGraph.open(tempDir.resolve("inherits.arcadedb"))) {
            builder.build(g, rel);

            assertThat(g.directSubclasses("com.example.Animal"))
                    .containsExactly("com.example.Dog");
        }
    }

    @Test
    void build_inheritsEdge_populatesDirectSuperTypes() {
        var rel = new ParsedRelationships(
                "proj",
                List.of(new TypeEdge("com.example.Cat", "com.example.Animal")),
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of());

        try (CallGraph g = CallGraph.open(tempDir.resolve("supertype.arcadedb"))) {
            builder.build(g, rel);

            assertThat(g.directSuperTypes("com.example.Cat"))
                    .containsExactly("com.example.Animal");
        }
    }

    @Test
    void build_multipleInheritsEdges_allPresent() {
        var rel = new ParsedRelationships(
                "proj",
                List.of(
                        new TypeEdge("com.example.Dog", "com.example.Animal"),
                        new TypeEdge("com.example.Cat", "com.example.Animal")
                ),
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of());

        try (CallGraph g = CallGraph.open(tempDir.resolve("multi-inherits.arcadedb"))) {
            builder.build(g, rel);

            assertThat(g.directSubclasses("com.example.Animal"))
                    .containsExactlyInAnyOrder("com.example.Dog", "com.example.Cat");
        }
    }

    // -------------------------------------------------------------------------
    // implements_iface edges
    // -------------------------------------------------------------------------

    @Test
    void build_implementsEdge_populatesDirectSubclasses() {
        var rel = new ParsedRelationships(
                "proj",
                List.of(),
                List.of(new TypeEdge("com.example.MyList", "java.util.List")),
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of());

        try (CallGraph g = CallGraph.open(tempDir.resolve("implements.arcadedb"))) {
            builder.build(g, rel);

            assertThat(g.directSubclasses("java.util.List"))
                    .containsExactly("com.example.MyList");
        }
    }

    @Test
    void build_implementsEdge_appearsInDirectSuperTypes() {
        var rel = new ParsedRelationships(
                "proj",
                List.of(),
                List.of(new TypeEdge("com.example.Handler", "com.example.Runnable")),
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of());

        try (CallGraph g = CallGraph.open(tempDir.resolve("iface-super.arcadedb"))) {
            builder.build(g, rel);

            assertThat(g.directSuperTypes("com.example.Handler"))
                    .contains("com.example.Runnable");
        }
    }

    // -------------------------------------------------------------------------
    // declares_field + CodeField vertices
    // -------------------------------------------------------------------------

    @Test
    void build_fieldDecl_populatesDeclaredFields() {
        var rel = new ParsedRelationships(
                "proj",
                List.of(), List.of(),
                List.of(
                        new FieldDecl("com.example.Person#name", "com.example.Person",
                                "String", "private"),
                        new FieldDecl("com.example.Person#age",  "com.example.Person",
                                "int",    "private")
                ),
                List.of(), List.of(), List.of(), List.of(), List.of());

        try (CallGraph g = CallGraph.open(tempDir.resolve("field-decl.arcadedb"))) {
            builder.build(g, rel);

            assertThat(g.declaredFields("com.example.Person"))
                    .containsExactlyInAnyOrder(
                            "com.example.Person#name",
                            "com.example.Person#age");
        }
    }

    // -------------------------------------------------------------------------
    // reads_field / writes_field
    // -------------------------------------------------------------------------

    @Test
    void build_readsEdge_populatesFieldReaders() {
        // Must have the method pre-populated so KG edge can find it
        var rel = new ParsedRelationships(
                "proj",
                List.of(), List.of(),
                List.of(new FieldDecl("com.example.Foo#bar", "com.example.Foo", "String", "private")),
                List.of(new FieldAccess("com.example.Foo#getBar()", "com.example.Foo#bar")),
                List.of(), List.of(), List.of(), List.of());

        try (CallGraph g = CallGraph.open(tempDir.resolve("reads.arcadedb"))) {
            g.addMethod("com.example.Foo#getBar()", "com.example.Foo");
            g.flush();

            builder.build(g, rel);

            assertThat(g.fieldReaders("com.example.Foo#bar"))
                    .containsExactly("com.example.Foo#getBar()");
        }
    }

    @Test
    void build_writesEdge_populatesFieldWriters() {
        var rel = new ParsedRelationships(
                "proj",
                List.of(), List.of(),
                List.of(new FieldDecl("com.example.Foo#bar", "com.example.Foo", "String", "private")),
                List.of(),
                List.of(new FieldAccess("com.example.Foo#setBar(String)", "com.example.Foo#bar")),
                List.of(), List.of(), List.of());

        try (CallGraph g = CallGraph.open(tempDir.resolve("writes.arcadedb"))) {
            g.addMethod("com.example.Foo#setBar(String)", "com.example.Foo");
            g.flush();

            builder.build(g, rel);

            assertThat(g.fieldWriters("com.example.Foo#bar"))
                    .containsExactly("com.example.Foo#setBar(String)");
        }
    }

    // -------------------------------------------------------------------------
    // returns_type / takes_type
    // -------------------------------------------------------------------------

    @Test
    void build_returnsEdge_populatesMethodsReturning() {
        var rel = new ParsedRelationships(
                "proj",
                List.of(), List.of(), List.of(), List.of(), List.of(),
                List.of(new TypeEdge("com.example.Repo#find(long)", "com.example.User")),
                List.of(), List.of());

        try (CallGraph g = CallGraph.open(tempDir.resolve("returns.arcadedb"))) {
            g.addMethod("com.example.Repo#find(long)", "com.example.Repo");
            g.flush();

            builder.build(g, rel);

            assertThat(g.methodsReturning("com.example.User"))
                    .containsExactly("com.example.Repo#find(long)");
        }
    }

    @Test
    void build_takesEdge_populatesMethodsTaking() {
        var rel = new ParsedRelationships(
                "proj",
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of(),
                List.of(new TypeEdge("com.example.Svc#process(Order)", "com.example.Order")),
                List.of());

        try (CallGraph g = CallGraph.open(tempDir.resolve("takes.arcadedb"))) {
            g.addMethod("com.example.Svc#process(Order)", "com.example.Svc");
            g.flush();

            builder.build(g, rel);

            assertThat(g.methodsTaking("com.example.Order"))
                    .containsExactly("com.example.Svc#process(Order)");
        }
    }

    // -------------------------------------------------------------------------
    // annotated_by edges
    // -------------------------------------------------------------------------

    @Test
    void build_annotatedByEdge_onMethod_populatesAnnotatedWith() {
        var rel = new ParsedRelationships(
                "proj",
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(),
                List.of(new TypeEdge("com.example.Ctrl#get()", "org.springframework.GetMapping")));

        try (CallGraph g = CallGraph.open(tempDir.resolve("anno-method.arcadedb"))) {
            g.addMethod("com.example.Ctrl#get()", "com.example.Ctrl");
            g.flush();

            builder.build(g, rel);

            assertThat(g.annotatedWith("org.springframework.GetMapping"))
                    .containsExactly("com.example.Ctrl#get()");
        }
    }

    @Test
    void build_annotatedByEdge_onClass_populatesAnnotatedWith() {
        var rel = new ParsedRelationships(
                "proj",
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(),
                List.of(new TypeEdge("com.example.MyService", "org.springframework.Service")));

        try (CallGraph g = CallGraph.open(tempDir.resolve("anno-class.arcadedb"))) {
            builder.build(g, rel);

            assertThat(g.annotatedWith("org.springframework.Service"))
                    .containsExactly("com.example.MyService");
        }
    }

    // -------------------------------------------------------------------------
    // allSubclasses (transitive BFS) via builder
    // -------------------------------------------------------------------------

    @Test
    void build_transitiveInheritance_allSubclassesReturnsFullHierarchy() {
        var rel = new ParsedRelationships(
                "proj",
                List.of(
                        new TypeEdge("com.example.B", "com.example.A"),
                        new TypeEdge("com.example.C", "com.example.B")
                ),
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of());

        try (CallGraph g = CallGraph.open(tempDir.resolve("transitive.arcadedb"))) {
            builder.build(g, rel);

            assertThat(g.allSubclasses("com.example.A"))
                    .containsExactlyInAnyOrder("com.example.B", "com.example.C");
        }
    }

    // -------------------------------------------------------------------------
    // Combined scenario — all edge types in a single build call
    // -------------------------------------------------------------------------

    @Test
    void build_combinedRelationships_allEdgesPopulated() {
        var rel = new ParsedRelationships(
                "proj",
                /* inherits */         List.of(new TypeEdge("com.example.Dog", "com.example.Animal")),
                /* implementsEdges */  List.of(new TypeEdge("com.example.Dog", "com.example.Domestic")),
                /* fields */           List.of(new FieldDecl("com.example.Dog#name",
                                                             "com.example.Dog", "String", "private")),
                /* reads */            List.of(new FieldAccess("com.example.Dog#getName()",
                                                               "com.example.Dog#name")),
                /* writes */           List.of(new FieldAccess("com.example.Dog#setName(String)",
                                                               "com.example.Dog#name")),
                /* returns */          List.of(new TypeEdge("com.example.Factory#create()",
                                                            "com.example.Dog")),
                /* takes */            List.of(new TypeEdge("com.example.Shelter#accept(Dog)",
                                                            "com.example.Dog")),
                /* annotatedBy */      List.of(new TypeEdge("com.example.Dog#getName()",
                                                            "com.example.Override")));

        try (CallGraph g = CallGraph.open(tempDir.resolve("combined.arcadedb"))) {
            g.addMethod("com.example.Dog#getName()",        "com.example.Dog");
            g.addMethod("com.example.Dog#setName(String)",  "com.example.Dog");
            g.addMethod("com.example.Factory#create()",     "com.example.Factory");
            g.addMethod("com.example.Shelter#accept(Dog)",  "com.example.Shelter");
            g.flush();

            builder.build(g, rel);

            assertThat(g.directSubclasses("com.example.Animal"))
                    .contains("com.example.Dog");
            assertThat(g.directSubclasses("com.example.Domestic"))
                    .contains("com.example.Dog");
            assertThat(g.declaredFields("com.example.Dog"))
                    .containsExactly("com.example.Dog#name");
            assertThat(g.fieldReaders("com.example.Dog#name"))
                    .containsExactly("com.example.Dog#getName()");
            assertThat(g.fieldWriters("com.example.Dog#name"))
                    .containsExactly("com.example.Dog#setName(String)");
            assertThat(g.methodsReturning("com.example.Dog"))
                    .containsExactly("com.example.Factory#create()");
            assertThat(g.methodsTaking("com.example.Dog"))
                    .containsExactly("com.example.Shelter#accept(Dog)");
            assertThat(g.annotatedWith("com.example.Override"))
                    .containsExactly("com.example.Dog#getName()");
        }
    }
}
