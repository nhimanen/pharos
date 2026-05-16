package com.pharos.parser;

import com.pharos.parser.model.ParsedRelationships;
import com.pharos.parser.model.ParsedRelationships.FieldAccess;
import com.pharos.parser.model.ParsedRelationships.FieldDecl;
import com.pharos.parser.model.ParsedRelationships.TypeEdge;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link ParsedRelationships} and its inner record types.
 *
 * These tests are intentionally simple: they verify the factory method,
 * record accessor semantics, and inner record constructors.
 */
class ParsedRelationshipsTest {

    // -------------------------------------------------------------------------
    // ParsedRelationships.empty()
    // -------------------------------------------------------------------------

    @Test
    void empty_returnsInstanceWithGivenProjectName() {
        ParsedRelationships rel = ParsedRelationships.empty("my-project");

        assertThat(rel.projectName()).isEqualTo("my-project");
    }

    @Test
    void empty_inheritsIsEmpty() {
        assertThat(ParsedRelationships.empty("p").inherits()).isEmpty();
    }

    @Test
    void empty_implementsEdgesIsEmpty() {
        assertThat(ParsedRelationships.empty("p").implementsEdges()).isEmpty();
    }

    @Test
    void empty_fieldsIsEmpty() {
        assertThat(ParsedRelationships.empty("p").fields()).isEmpty();
    }

    @Test
    void empty_readsIsEmpty() {
        assertThat(ParsedRelationships.empty("p").reads()).isEmpty();
    }

    @Test
    void empty_writesIsEmpty() {
        assertThat(ParsedRelationships.empty("p").writes()).isEmpty();
    }

    @Test
    void empty_returnsIsEmpty() {
        assertThat(ParsedRelationships.empty("p").returns()).isEmpty();
    }

    @Test
    void empty_takesIsEmpty() {
        assertThat(ParsedRelationships.empty("p").takes()).isEmpty();
    }

    @Test
    void empty_annotatedByIsEmpty() {
        assertThat(ParsedRelationships.empty("p").annotatedBy()).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Record constructor — all fields round-trip correctly
    // -------------------------------------------------------------------------

    @Test
    void constructor_allFieldsAccessibleViaAccessors() {
        var inherits    = List.of(new TypeEdge("com.example.Dog", "com.example.Animal"));
        var implEdges   = List.of(new TypeEdge("com.example.Dog", "com.example.Domestic"));
        var fields      = List.of(new FieldDecl("com.example.Dog#name", "com.example.Dog",
                                                "String", "private"));
        var reads       = List.of(new FieldAccess("com.example.Dog#getName()",
                                                   "com.example.Dog#name"));
        var writes      = List.of(new FieldAccess("com.example.Dog#setName(String)",
                                                   "com.example.Dog#name"));
        var returns     = List.of(new TypeEdge("com.example.Factory#create()", "com.example.Dog"));
        var takes       = List.of(new TypeEdge("com.example.Shelter#accept(Dog)", "com.example.Dog"));
        var annotatedBy = List.of(new TypeEdge("com.example.Dog#getName()", "java.lang.Override"));

        var rel = new ParsedRelationships("proj", inherits, implEdges, fields,
                                          reads, writes, returns, takes, annotatedBy);

        assertThat(rel.projectName()).isEqualTo("proj");
        assertThat(rel.inherits()).isSameAs(inherits);
        assertThat(rel.implementsEdges()).isSameAs(implEdges);
        assertThat(rel.fields()).isSameAs(fields);
        assertThat(rel.reads()).isSameAs(reads);
        assertThat(rel.writes()).isSameAs(writes);
        assertThat(rel.returns()).isSameAs(returns);
        assertThat(rel.takes()).isSameAs(takes);
        assertThat(rel.annotatedBy()).isSameAs(annotatedBy);
    }

    // -------------------------------------------------------------------------
    // TypeEdge inner record
    // -------------------------------------------------------------------------

    @Test
    void typeEdge_fromAndTo_roundTrip() {
        var edge = new TypeEdge("com.example.Sub", "com.example.Super");

        assertThat(edge.from()).isEqualTo("com.example.Sub");
        assertThat(edge.to()).isEqualTo("com.example.Super");
    }

    @Test
    void typeEdge_equalityBasedOnValues() {
        var e1 = new TypeEdge("A", "B");
        var e2 = new TypeEdge("A", "B");
        var e3 = new TypeEdge("A", "C");

        assertThat(e1).isEqualTo(e2);
        assertThat(e1).isNotEqualTo(e3);
    }

    @Test
    void typeEdge_hashCodeConsistentWithEquals() {
        var e1 = new TypeEdge("X", "Y");
        var e2 = new TypeEdge("X", "Y");

        assertThat(e1.hashCode()).isEqualTo(e2.hashCode());
    }

    // -------------------------------------------------------------------------
    // FieldDecl inner record
    // -------------------------------------------------------------------------

    @Test
    void fieldDecl_allAccessorsReturnCorrectValues() {
        var fd = new FieldDecl("com.example.Cls#counter", "com.example.Cls", "int", "protected");

        assertThat(fd.fieldFqn()).isEqualTo("com.example.Cls#counter");
        assertThat(fd.ownerFqn()).isEqualTo("com.example.Cls");
        assertThat(fd.fieldType()).isEqualTo("int");
        assertThat(fd.accessModifier()).isEqualTo("protected");
    }

    @Test
    void fieldDecl_equalityBasedOnValues() {
        var fd1 = new FieldDecl("pkg.A#x", "pkg.A", "String", "public");
        var fd2 = new FieldDecl("pkg.A#x", "pkg.A", "String", "public");
        var fd3 = new FieldDecl("pkg.A#y", "pkg.A", "String", "public");

        assertThat(fd1).isEqualTo(fd2);
        assertThat(fd1).isNotEqualTo(fd3);
    }

    // -------------------------------------------------------------------------
    // FieldAccess inner record
    // -------------------------------------------------------------------------

    @Test
    void fieldAccess_allAccessorsReturnCorrectValues() {
        var fa = new FieldAccess("com.example.Cls#doSomething()", "com.example.Cls#value");

        assertThat(fa.methodFqn()).isEqualTo("com.example.Cls#doSomething()");
        assertThat(fa.fieldFqn()).isEqualTo("com.example.Cls#value");
    }

    @Test
    void fieldAccess_equalityBasedOnValues() {
        var fa1 = new FieldAccess("pkg.Cls#m()", "pkg.Cls#f");
        var fa2 = new FieldAccess("pkg.Cls#m()", "pkg.Cls#f");
        var fa3 = new FieldAccess("pkg.Cls#n()", "pkg.Cls#f");

        assertThat(fa1).isEqualTo(fa2);
        assertThat(fa1).isNotEqualTo(fa3);
    }

    // -------------------------------------------------------------------------
    // Immutability — lists returned by empty() are unmodifiable
    // -------------------------------------------------------------------------

    @Test
    void empty_inherits_isUnmodifiable() {
        var rel = ParsedRelationships.empty("p");

        assertThatException()
                .isThrownBy(() -> rel.inherits().add(new TypeEdge("A", "B")));
    }

    @Test
    void empty_fields_isUnmodifiable() {
        var rel = ParsedRelationships.empty("p");

        assertThatException()
                .isThrownBy(() -> rel.fields().add(
                        new FieldDecl("pkg.A#x", "pkg.A", "int", "public")));
    }
}
