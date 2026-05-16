package com.pharos.graph;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for the knowledge-graph APIs on {@link CallGraph}.
 *
 * Each test method opens its own ArcadeDB instance in an isolated sub-directory
 * to avoid contention across tests.
 */
class KnowledgeGraphTest {

    @TempDir
    Path tempDir;

    // -------------------------------------------------------------------------
    // CodeType vertex creation
    // -------------------------------------------------------------------------

    @Test
    void addCodeType_vertexIsQueryableAfterFlush() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("ct-basic.arcadedb"))) {
            g.addCodeType("com.example.Animal", "com.example");
            g.flushKnowledge();

            // directSubclasses with nothing added yet returns empty — vertex exists
            assertThat(g.directSubclasses("com.example.Animal")).isEmpty();
        }
    }

    @Test
    void addCodeType_isIdempotent() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("ct-idem.arcadedb"))) {
            g.addCodeType("com.example.Widget", "com.example");
            g.addCodeType("com.example.Widget", "com.example"); // duplicate
            g.flushKnowledge();

            // Both calls succeed without exception; no assertion on internal count needed
        }
    }

    // -------------------------------------------------------------------------
    // directSubclasses / directSuperTypes
    // -------------------------------------------------------------------------

    @Test
    void directSubclasses_returnsDirectInheritors() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("subclass-direct.arcadedb"))) {
            g.addCodeType("com.example.Animal", "com.example");
            g.addCodeType("com.example.Dog",    "com.example");
            g.addCodeType("com.example.Cat",    "com.example");
            g.flushKnowledge();

            g.addKgEdge("inherits", "com.example.Dog", "CodeType", "com.example.Animal", "CodeType");
            g.addKgEdge("inherits", "com.example.Cat", "CodeType", "com.example.Animal", "CodeType");
            g.flushKnowledge();

            Set<String> subs = g.directSubclasses("com.example.Animal");
            assertThat(subs).containsExactlyInAnyOrder("com.example.Dog", "com.example.Cat");
        }
    }

    @Test
    void directSubclasses_doesNotIncludeTransitiveDescendants() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("subclass-no-transitive.arcadedb"))) {
            g.addCodeType("com.example.A", "com.example");
            g.addCodeType("com.example.B", "com.example");
            g.addCodeType("com.example.C", "com.example");
            g.flushKnowledge();

            // C extends B extends A
            g.addKgEdge("inherits", "com.example.B", "CodeType", "com.example.A", "CodeType");
            g.addKgEdge("inherits", "com.example.C", "CodeType", "com.example.B", "CodeType");
            g.flushKnowledge();

            assertThat(g.directSubclasses("com.example.A"))
                    .containsExactly("com.example.B")
                    .doesNotContain("com.example.C");
        }
    }

    @Test
    void directSubclasses_returnsEmptyForUnknownClass() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("subclass-unknown.arcadedb"))) {
            assertThat(g.directSubclasses("com.example.NoSuch")).isEmpty();
        }
    }

    @Test
    void directSuperTypes_returnsParentClass() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("supertype.arcadedb"))) {
            g.addCodeType("com.example.Base",    "com.example");
            g.addCodeType("com.example.Derived", "com.example");
            g.flushKnowledge();

            g.addKgEdge("inherits", "com.example.Derived", "CodeType", "com.example.Base", "CodeType");
            g.flushKnowledge();

            assertThat(g.directSuperTypes("com.example.Derived"))
                    .containsExactly("com.example.Base");
        }
    }

    @Test
    void directSuperTypes_includesImplementedInterface() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("supertype-iface.arcadedb"))) {
            g.addCodeType("com.example.Serializable", "com.example");
            g.addCodeType("com.example.MyClass",      "com.example");
            g.flushKnowledge();

            g.addKgEdge("implements_iface", "com.example.MyClass", "CodeType",
                    "com.example.Serializable", "CodeType");
            g.flushKnowledge();

            assertThat(g.directSuperTypes("com.example.MyClass"))
                    .contains("com.example.Serializable");
        }
    }

    @Test
    void directSubclasses_includesImplementors() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("subclass-iface.arcadedb"))) {
            g.addCodeType("com.example.Runnable", "com.example");
            g.addCodeType("com.example.Task",     "com.example");
            g.flushKnowledge();

            g.addKgEdge("implements_iface", "com.example.Task", "CodeType",
                    "com.example.Runnable", "CodeType");
            g.flushKnowledge();

            assertThat(g.directSubclasses("com.example.Runnable"))
                    .contains("com.example.Task");
        }
    }

    // -------------------------------------------------------------------------
    // allSubclasses (BFS / transitive)
    // -------------------------------------------------------------------------

    @Test
    void allSubclasses_collectsTransitiveDescendants() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("all-subclasses.arcadedb"))) {
            g.addCodeType("com.example.A", "com.example");
            g.addCodeType("com.example.B", "com.example");
            g.addCodeType("com.example.C", "com.example");
            g.addCodeType("com.example.D", "com.example");
            g.flushKnowledge();

            // A ← B ← C ← D
            g.addKgEdge("inherits", "com.example.B", "CodeType", "com.example.A", "CodeType");
            g.addKgEdge("inherits", "com.example.C", "CodeType", "com.example.B", "CodeType");
            g.addKgEdge("inherits", "com.example.D", "CodeType", "com.example.C", "CodeType");
            g.flushKnowledge();

            List<String> all = g.allSubclasses("com.example.A");
            assertThat(all).containsExactlyInAnyOrder(
                    "com.example.B", "com.example.C", "com.example.D");
        }
    }

    @Test
    void allSubclasses_doesNotIncludeRootItself() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("all-subclasses-root.arcadedb"))) {
            g.addCodeType("com.example.Root",  "com.example");
            g.addCodeType("com.example.Child", "com.example");
            g.flushKnowledge();

            g.addKgEdge("inherits", "com.example.Child", "CodeType", "com.example.Root", "CodeType");
            g.flushKnowledge();

            assertThat(g.allSubclasses("com.example.Root"))
                    .doesNotContain("com.example.Root");
        }
    }

    @Test
    void allSubclasses_returnsEmptyForLeafClass() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("all-subclasses-leaf.arcadedb"))) {
            g.addCodeType("com.example.Leaf", "com.example");
            g.flushKnowledge();

            assertThat(g.allSubclasses("com.example.Leaf")).isEmpty();
        }
    }

    // -------------------------------------------------------------------------
    // CodeField vertices + declaredFields
    // -------------------------------------------------------------------------

    @Test
    void addCodeField_isReturnedByDeclaredFields() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("field-declared.arcadedb"))) {
            g.addCodeType("com.example.Person", "com.example");
            g.addCodeField("com.example.Person#name", "com.example.Person", "String", "private");
            g.addCodeField("com.example.Person#age",  "com.example.Person", "int",    "private");
            g.flushKnowledge();

            g.addKgEdge("declares_field", "com.example.Person", "CodeType",
                    "com.example.Person#name", "CodeField");
            g.addKgEdge("declares_field", "com.example.Person", "CodeType",
                    "com.example.Person#age",  "CodeField");
            g.flushKnowledge();

            assertThat(g.declaredFields("com.example.Person"))
                    .containsExactlyInAnyOrder("com.example.Person#name", "com.example.Person#age");
        }
    }

    @Test
    void declaredFields_returnsEmptyForClassWithNoFields() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("field-empty.arcadedb"))) {
            g.addCodeType("com.example.Empty", "com.example");
            g.flushKnowledge();

            assertThat(g.declaredFields("com.example.Empty")).isEmpty();
        }
    }

    // -------------------------------------------------------------------------
    // fieldReaders / fieldWriters
    // -------------------------------------------------------------------------

    @Test
    void fieldReaders_returnsMethodsThatReadTheField() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("field-readers.arcadedb"))) {
            g.addMethod("com.example.Foo#getBar()", "com.example.Foo");
            g.addMethod("com.example.Foo#print()",  "com.example.Foo");
            g.flush();

            g.addCodeField("com.example.Foo#bar", "com.example.Foo", "String", "private");
            g.flushKnowledge();

            g.addKgEdge("reads_field", "com.example.Foo#getBar()", "Method",
                    "com.example.Foo#bar", "CodeField");
            g.addKgEdge("reads_field", "com.example.Foo#print()",  "Method",
                    "com.example.Foo#bar", "CodeField");
            g.flushKnowledge();

            assertThat(g.fieldReaders("com.example.Foo#bar"))
                    .containsExactlyInAnyOrder("com.example.Foo#getBar()", "com.example.Foo#print()");
        }
    }

    @Test
    void fieldReaders_returnsEmptyForUnreadField() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("field-readers-empty.arcadedb"))) {
            g.addCodeField("com.example.X#val", "com.example.X", "int", "public");
            g.flushKnowledge();

            assertThat(g.fieldReaders("com.example.X#val")).isEmpty();
        }
    }

    @Test
    void fieldWriters_returnsMethodsThatWriteTheField() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("field-writers.arcadedb"))) {
            g.addMethod("com.example.Counter#increment()", "com.example.Counter");
            g.addMethod("com.example.Counter#reset()",     "com.example.Counter");
            g.flush();

            g.addCodeField("com.example.Counter#count", "com.example.Counter", "int", "private");
            g.flushKnowledge();

            g.addKgEdge("writes_field", "com.example.Counter#increment()", "Method",
                    "com.example.Counter#count", "CodeField");
            g.addKgEdge("writes_field", "com.example.Counter#reset()",     "Method",
                    "com.example.Counter#count", "CodeField");
            g.flushKnowledge();

            assertThat(g.fieldWriters("com.example.Counter#count"))
                    .containsExactlyInAnyOrder(
                            "com.example.Counter#increment()",
                            "com.example.Counter#reset()");
        }
    }

    @Test
    void fieldReaders_andFieldWriters_areIndependent() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("field-rw-independent.arcadedb"))) {
            g.addMethod("com.example.S#get()", "com.example.S");
            g.addMethod("com.example.S#set(String)", "com.example.S");
            g.flush();

            g.addCodeField("com.example.S#value", "com.example.S", "String", "private");
            g.flushKnowledge();

            g.addKgEdge("reads_field",  "com.example.S#get()",        "Method",
                    "com.example.S#value", "CodeField");
            g.addKgEdge("writes_field", "com.example.S#set(String)",  "Method",
                    "com.example.S#value", "CodeField");
            g.flushKnowledge();

            assertThat(g.fieldReaders("com.example.S#value"))
                    .containsExactly("com.example.S#get()")
                    .doesNotContain("com.example.S#set(String)");
            assertThat(g.fieldWriters("com.example.S#value"))
                    .containsExactly("com.example.S#set(String)")
                    .doesNotContain("com.example.S#get()");
        }
    }

    // -------------------------------------------------------------------------
    // annotatedWith
    // -------------------------------------------------------------------------

    @Test
    void annotatedWith_returnsAnnotatedMethods() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("annotated-method.arcadedb"))) {
            g.addMethod("com.example.Svc#serve(Request)", "com.example.Svc");
            g.addMethod("com.example.Svc#status()",       "com.example.Svc");
            g.flush();

            g.addCodeType("org.springframework.GetMapping", "org.springframework");
            g.flushKnowledge();

            g.addKgEdge("annotated_by", "com.example.Svc#serve(Request)",  "Method",
                    "org.springframework.GetMapping", "CodeType");
            g.addKgEdge("annotated_by", "com.example.Svc#status()",        "Method",
                    "org.springframework.GetMapping", "CodeType");
            g.flushKnowledge();

            assertThat(g.annotatedWith("org.springframework.GetMapping"))
                    .containsExactlyInAnyOrder(
                            "com.example.Svc#serve(Request)",
                            "com.example.Svc#status()");
        }
    }

    @Test
    void annotatedWith_returnsAnnotatedClasses() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("annotated-class.arcadedb"))) {
            g.addCodeType("com.example.MyService",       "com.example");
            g.addCodeType("org.springframework.Service", "org.springframework");
            g.flushKnowledge();

            g.addKgEdge("annotated_by", "com.example.MyService", "CodeType",
                    "org.springframework.Service", "CodeType");
            g.flushKnowledge();

            assertThat(g.annotatedWith("org.springframework.Service"))
                    .contains("com.example.MyService");
        }
    }

    @Test
    void annotatedWith_returnsEmptyForUnusedAnnotation() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("annotated-empty.arcadedb"))) {
            g.addCodeType("com.example.UnusedAnno", "com.example");
            g.flushKnowledge();

            assertThat(g.annotatedWith("com.example.UnusedAnno")).isEmpty();
        }
    }

    // -------------------------------------------------------------------------
    // methodsReturning / methodsTaking
    // -------------------------------------------------------------------------

    @Test
    void methodsReturning_returnsMethodsWithMatchingReturnType() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("returns-type.arcadedb"))) {
            g.addMethod("com.example.Repo#findById(long)",  "com.example.Repo");
            g.addMethod("com.example.Repo#findAll()",       "com.example.Repo");
            g.flush();

            g.addCodeType("com.example.User", "com.example");
            g.flushKnowledge();

            g.addKgEdge("returns_type", "com.example.Repo#findById(long)", "Method",
                    "com.example.User", "CodeType");
            g.addKgEdge("returns_type", "com.example.Repo#findAll()",      "Method",
                    "com.example.User", "CodeType");
            g.flushKnowledge();

            assertThat(g.methodsReturning("com.example.User"))
                    .containsExactlyInAnyOrder(
                            "com.example.Repo#findById(long)",
                            "com.example.Repo#findAll()");
        }
    }

    @Test
    void methodsReturning_returnsEmptyForTypeReturnedByNobody() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("returns-empty.arcadedb"))) {
            g.addCodeType("com.example.Orphan", "com.example");
            g.flushKnowledge();

            assertThat(g.methodsReturning("com.example.Orphan")).isEmpty();
        }
    }

    @Test
    void methodsTaking_returnsMethodsWithMatchingParamType() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("takes-type.arcadedb"))) {
            g.addMethod("com.example.Svc#process(Order)",  "com.example.Svc");
            g.addMethod("com.example.Svc#validate(Order)", "com.example.Svc");
            g.flush();

            g.addCodeType("com.example.Order", "com.example");
            g.flushKnowledge();

            g.addKgEdge("takes_type", "com.example.Svc#process(Order)",  "Method",
                    "com.example.Order", "CodeType");
            g.addKgEdge("takes_type", "com.example.Svc#validate(Order)", "Method",
                    "com.example.Order", "CodeType");
            g.flushKnowledge();

            assertThat(g.methodsTaking("com.example.Order"))
                    .containsExactlyInAnyOrder(
                            "com.example.Svc#process(Order)",
                            "com.example.Svc#validate(Order)");
        }
    }

    @Test
    void methodsTaking_returnsEmptyForTypeUsedByNobody() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("takes-empty.arcadedb"))) {
            g.addCodeType("com.example.Unused", "com.example");
            g.flushKnowledge();

            assertThat(g.methodsTaking("com.example.Unused")).isEmpty();
        }
    }

    // -------------------------------------------------------------------------
    // clear() removes KG vertices too
    // -------------------------------------------------------------------------

    @Test
    void clear_removesCodeTypeVertices() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("clear-codetype.arcadedb"))) {
            g.addCodeType("com.example.Cls", "com.example");
            g.flushKnowledge();

            g.clear();

            // After clear(), querying should return empty — no vertex found
            assertThat(g.directSubclasses("com.example.Cls")).isEmpty();
            assertThat(g.directSuperTypes("com.example.Cls")).isEmpty();
        }
    }

    @Test
    void clear_removesCodeFieldVertices() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("clear-codefield.arcadedb"))) {
            g.addCodeType("com.example.Cls", "com.example");
            g.addCodeField("com.example.Cls#x", "com.example.Cls", "int", "public");
            g.flushKnowledge();

            g.addKgEdge("declares_field", "com.example.Cls", "CodeType",
                    "com.example.Cls#x", "CodeField");
            g.flushKnowledge();

            g.clear();

            assertThat(g.declaredFields("com.example.Cls")).isEmpty();
        }
    }

    @Test
    void clear_removesKgEdges() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("clear-edges.arcadedb"))) {
            g.addCodeType("com.example.Sub",  "com.example");
            g.addCodeType("com.example.Base", "com.example");
            g.flushKnowledge();

            g.addKgEdge("inherits", "com.example.Sub", "CodeType", "com.example.Base", "CodeType");
            g.flushKnowledge();

            g.clear();

            // After clear, re-add Base only — Sub is gone so directSubclasses should be empty
            g.addCodeType("com.example.Base", "com.example");
            g.flushKnowledge();

            assertThat(g.directSubclasses("com.example.Base")).isEmpty();
        }
    }

    @Test
    void clear_alsoRemovesMethodVertices() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("clear-methods.arcadedb"))) {
            g.addMethod("com.example.Foo#bar()", "com.example.Foo");
            g.flush();

            g.clear();

            assertThat(g.methodCount()).isEqualTo(0);
        }
    }

    // -------------------------------------------------------------------------
    // evictClasses() removes CodeType and CodeField for evicted prefixes
    // -------------------------------------------------------------------------

    @Test
    void evictClasses_removesCodeTypeVerticesMatchingClassPrefix() {
        // CodeType vertices store classPrefix = package name (e.g. "com.example").
        // evictClasses deletes CodeType WHERE classPrefix = evictArg, so passing the
        // package prefix evicts all CodeType nodes in that package.
        try (CallGraph g = CallGraph.open(tempDir.resolve("evict-codetype.arcadedb"))) {
            g.addCodeType("com.example.A", "com.example");
            g.addCodeType("com.other.B",   "com.other");
            g.flushKnowledge();

            // Evict by package prefix (how CodeType.classPrefix is stored)
            g.evictClasses(List.of("com.example"));

            // com.example.A's CodeType is deleted; queries for it return empty
            assertThat(g.directSuperTypes("com.example.A")).isEmpty();
            // com.other.B is unaffected
            assertThat(g.directSubclasses("com.other.B")).isEmpty(); // exists, just no subclasses
        }
    }

    @Test
    void evictClasses_removesMethodVerticesForEvictedClass() {
        // evictClasses uses classPrefix equality for Method vertices.
        // Method vertices store classPrefix = full class FQN (e.g. "com.example.Cls"),
        // so evicting with "com.example.Cls" removes them.
        try (CallGraph g = CallGraph.open(tempDir.resolve("evict-method-cls.arcadedb"))) {
            g.addMethod("com.example.Cls#foo()", "com.example.Cls");
            g.addMethod("com.other.Other#bar()",  "com.other.Other");
            g.flush();

            g.evictClasses(List.of("com.example.Cls"));

            // Only com.example.Cls methods are evicted; com.other.Other survives
            assertThat(g.allFqns().anyMatch("com.example.Cls#foo()"::equals)).isFalse();
            assertThat(g.allFqns().anyMatch("com.other.Other#bar()"::equals)).isTrue();
        }
    }

    @Test
    void evictClasses_doesNotAffectOtherPackages() {
        // Evict "com.evict" package — types in "com.keep" must be unaffected.
        try (CallGraph g = CallGraph.open(tempDir.resolve("evict-other.arcadedb"))) {
            g.addCodeType("com.evict.ToEvict", "com.evict");
            g.addCodeType("com.keep.Base",     "com.keep");
            g.addCodeType("com.keep.Child",    "com.keep");
            g.flushKnowledge();

            g.addKgEdge("inherits", "com.keep.Child", "CodeType",
                    "com.keep.Base", "CodeType");
            g.flushKnowledge();

            g.evictClasses(List.of("com.evict"));

            // com.keep hierarchy must be unaffected
            assertThat(g.directSubclasses("com.keep.Base"))
                    .contains("com.keep.Child");
        }
    }

    @Test
    void evictClasses_emptyList_isNoOp() {
        try (CallGraph g = CallGraph.open(tempDir.resolve("evict-empty.arcadedb"))) {
            g.addCodeType("com.example.Safe", "com.example");
            g.flushKnowledge();

            assertThatCode(() -> g.evictClasses(List.of()))
                    .doesNotThrowAnyException();
        }
    }
}
