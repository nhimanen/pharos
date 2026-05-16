package com.pharos.indexer;

import com.pharos.parser.model.CallReference;
import com.pharos.parser.model.ParsedMethod;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link DocumentMapper#synthesizeDescription} and
 * {@link DocumentMapper#formatReturnType} — the code-to-NL embedding synthesis.
 */
class SynthesizedDescriptionTest {

    // -------------------------------------------------------------------------
    // formatReturnType
    // -------------------------------------------------------------------------

    @Nested
    class FormatReturnType {

        @Test void void_returnsNull()            { assertNull(DocumentMapper.formatReturnType("void")); }
        @Test void null_returnsNull()             { assertNull(DocumentMapper.formatReturnType(null)); }
        @Test void blank_returnsNull()            { assertNull(DocumentMapper.formatReturnType("  ")); }

        @Test void simpleType_returnedAsIs()      { assertEquals("User",    DocumentMapper.formatReturnType("User")); }
        @Test void primitiveInt_returnedAsIs()    { assertEquals("int",     DocumentMapper.formatReturnType("int")); }
        @Test void primitiveBoolean_returnedAsIs(){ assertEquals("boolean", DocumentMapper.formatReturnType("boolean")); }
        @Test void string_returnedAsIs()          { assertEquals("String",  DocumentMapper.formatReturnType("String")); }

        @Test
        void list_formattedAsListOf() {
            assertEquals("list of User", DocumentMapper.formatReturnType("List<User>"));
        }

        @Test
        void arrayList_formattedAsListOf() {
            assertEquals("list of Order", DocumentMapper.formatReturnType("ArrayList<Order>"));
        }

        @Test
        void set_formattedAsSetOf() {
            assertEquals("set of String", DocumentMapper.formatReturnType("Set<String>"));
        }

        @Test
        void collection_formattedAsCollectionOf() {
            assertEquals("collection of Item", DocumentMapper.formatReturnType("Collection<Item>"));
        }

        @Test
        void stream_formattedAsStreamOf() {
            assertEquals("stream of Result", DocumentMapper.formatReturnType("Stream<Result>"));
        }

        @Test
        void mapSimple_formattedAsMapOf() {
            assertEquals("map of String to User", DocumentMapper.formatReturnType("Map<String,User>"));
        }

        @Test
        void mapWithSpaces_formattedAsMapOf() {
            assertEquals("map of String to Long", DocumentMapper.formatReturnType("Map<String, Long>"));
        }

        @Test
        void hashMap_formattedAsMapOf() {
            assertEquals("map of String to Integer", DocumentMapper.formatReturnType("HashMap<String, Integer>"));
        }

        @Test
        void optional_formattedAsOptional() {
            assertEquals("optional User", DocumentMapper.formatReturnType("Optional<User>"));
        }

        @Test
        void completableFuture_formattedAsFuture() {
            String result = DocumentMapper.formatReturnType("CompletableFuture<Result>");
            assertTrue(result.contains("Result"), "Should mention Result: " + result);
        }

        @Test
        void array_formattedAsArrayOf() {
            assertEquals("array of String", DocumentMapper.formatReturnType("String[]"));
        }

        @Test
        void intArray_formattedAsArrayOf() {
            assertEquals("array of int", DocumentMapper.formatReturnType("int[]"));
        }

        @Test
        void nestedGeneric_outerTypeReturned() {
            // List<Map<String,User>> — outer "list of" is preserved, inner is stripped
            String result = DocumentMapper.formatReturnType("List<Map<String,User>>");
            assertEquals("list of Map", result);
        }

        @Test
        void unknownGeneric_outerNameReturned() {
            assertEquals("ResponseEntity", DocumentMapper.formatReturnType("ResponseEntity<User>"));
        }
    }

    // -------------------------------------------------------------------------
    // synthesizeDescription
    // -------------------------------------------------------------------------

    @Nested
    class SynthesizeDescription {

        @Test
        void noArgs_noReturn_justVerbPhrase() {
            ParsedMethod m = method("doSomething", "void", List.of(), List.of(), List.of());
            String desc = DocumentMapper.synthesizeDescription(m);
            assertTrue(desc.contains("do something"), "Should contain split method name: " + desc);
            assertFalse(desc.contains("returns"), "void should not say returns: " + desc);
        }

        @Test
        void withParams_givenClausePresent() {
            ParsedMethod m = method("findByTenant", "List<User>",
                    List.of("tenantId"), List.of("String"), List.of());
            String desc = DocumentMapper.synthesizeDescription(m);
            assertTrue(desc.contains("tenant id"), "Should contain split param name: " + desc);
            assertTrue(desc.contains("given"), "Should have given clause: " + desc);
        }

        @Test
        void listReturnType_naturalLanguageFormat() {
            ParsedMethod m = method("findActive", "List<User>",
                    List.of("tenantId"), List.of("String"), List.of());
            String desc = DocumentMapper.synthesizeDescription(m);
            assertTrue(desc.contains("list of User"), "Should say 'list of User': " + desc);
        }

        @Test
        void mapReturnType_naturalLanguageFormat() {
            ParsedMethod m = method("groupByCategory", "Map<String,List<Item>>",
                    List.of(), List.of(), List.of());
            String desc = DocumentMapper.synthesizeDescription(m);
            // Map<String,List<Item>> → outer is Map → "map of String to List"
            assertTrue(desc.contains("map of"), "Should contain 'map of': " + desc);
        }

        @Test
        void withResolvedCallees_callsClausePresent() {
            CallReference callee = CallReference.resolved(
                    "com.example.Repo#findActive(String)",
                    "com.example.Repo#findByStatus(String)",
                    1, 10);
            ParsedMethod m = method("findActive", "List<User>",
                    List.of("tenantId"), List.of("String"), List.of(callee));
            String desc = DocumentMapper.synthesizeDescription(m);
            assertTrue(desc.contains("calls"), "Should have calls clause: " + desc);
            assertTrue(desc.contains("find by status"), "Should contain split callee name: " + desc);
        }

        @Test
        void unresolvedCalleesIgnored() {
            CallReference unresolved = CallReference.unresolved(
                    "com.example.Repo#findActive(String)",
                    "doSomething", null, 0, 5);
            ParsedMethod m = method("findActive", "User",
                    List.of(), List.of(), List.of(unresolved));
            String desc = DocumentMapper.synthesizeDescription(m);
            assertFalse(desc.contains("calls"), "Unresolved callees should not appear: " + desc);
        }

        @Test
        void constructor_createsPhraseUsed() {
            ParsedMethod m = new ParsedMethod(
                    "proj:com.example.UserService#<init>(UserRepo)",
                    "proj", "com.example", "UserService", "com.example.UserService",
                    "<init>", "UserService(UserRepo repo)", "UserService",
                    List.of("UserRepo"), List.of("repo"),
                    "{ this.repo = repo; }", null, List.of(), "public",
                    false, true, false, false,
                    List.of(), List.of(), "/src/UserService.java", 5, 8
            );
            String desc = DocumentMapper.synthesizeDescription(m);
            assertTrue(desc.startsWith("creates UserService"), "Constructor should start with 'creates': " + desc);
        }

        @Test
        void multipleParams_allListed() {
            ParsedMethod m = method("processPayment", "boolean",
                    List.of("order", "amount", "currency"),
                    List.of("Order", "BigDecimal", "String"),
                    List.of());
            String desc = DocumentMapper.synthesizeDescription(m);
            assertTrue(desc.contains("order"), "Should contain 'order': " + desc);
            assertTrue(desc.contains("amount"), "Should contain 'amount': " + desc);
            assertTrue(desc.contains("currency"), "Should contain 'currency': " + desc);
        }
    }

    // -------------------------------------------------------------------------
    // buildEmbeddingText integration — synthesized desc used when no javadoc
    // -------------------------------------------------------------------------

    @Nested
    class BuildEmbeddingText {

        @Test
        void withJavadoc_javadocUsedNotSynthesis() {
            ParsedMethod m = method("findUser", "User", List.of("id"), List.of("Long"), List.of());
            ParsedMethod withDoc = withJavadoc(m, "Finds a user by their unique identifier.");
            String text = DocumentMapper.buildEmbeddingText(withDoc);
            assertTrue(text.contains("Finds a user"), "Javadoc should be in text: " + text);
        }

        @Test
        void withoutJavadoc_synthesisApplied() {
            ParsedMethod m = method("findActive", "List<User>",
                    List.of("tenantId"), List.of("String"), List.of());
            String text = DocumentMapper.buildEmbeddingText(m);
            assertTrue(text.contains("find active"), "Split name should appear: " + text);
            assertTrue(text.contains("list of User"), "Collection return should appear: " + text);
        }

        @Test
        void withoutJavadoc_signatureStillIncluded() {
            ParsedMethod m = method("findActive", "List<User>",
                    List.of("tenantId"), List.of("String"), List.of());
            String text = DocumentMapper.buildEmbeddingText(m);
            assertTrue(text.contains(m.signature()), "Signature should still be in text: " + text);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ParsedMethod method(String name, String returnType,
                                        List<String> paramNames, List<String> paramTypes,
                                        List<CallReference> calls) {
        String fqn = "proj:com.example.MyService#" + name + "()";
        String sig = returnType + " " + name + "(" +
                String.join(", ", paramNames) + ")";
        return new ParsedMethod(
                fqn, "proj", "com.example", "MyService", "com.example.MyService",
                name, sig, returnType,
                paramTypes, paramNames,
                "{ return null; }", null, List.of(), "public",
                false, false, false, false,
                List.of(), calls, "/src/MyService.java", 10, 15
        );
    }

    private static ParsedMethod withJavadoc(ParsedMethod m, String javadoc) {
        return new ParsedMethod(
                m.id(), m.projectName(), m.packageName(), m.className(),
                m.qualifiedClassName(), m.methodName(), m.signature(), m.returnType(),
                m.paramTypes(), m.paramNames(), m.body(), javadoc, m.annotations(),
                m.accessModifier(), m.isStatic(), m.isConstructor(), m.isAbstract(),
                m.isSynchronized(), m.thrownExceptions(), m.calledMethods(),
                m.filePath(), m.startLine(), m.endLine()
        );
    }
}
