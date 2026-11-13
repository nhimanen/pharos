package com.pharos.parser;

import com.github.javaparser.ast.body.*;
import com.github.javaparser.ast.comments.JavadocComment;
import com.github.javaparser.ast.expr.AnnotationExpr;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import com.pharos.parser.model.ParsedClass;

import java.util.ArrayList;
import java.util.List;

/**
 * Visits Java class, interface, enum, and record declarations to extract ParsedClass metadata.
 * The extracted class context is passed to MethodVisitor for method extraction.
 */
public class ClassVisitor extends VoidVisitorAdapter<List<ParsedClass>> {

    private final String projectName;
    private final String packageName;
    private final String filePath;

    public ClassVisitor(String projectName, String packageName, String filePath) {
        this.projectName = projectName;
        this.packageName = packageName;
        this.filePath = filePath;
    }

    @Override
    public void visit(ClassOrInterfaceDeclaration n, List<ParsedClass> collector) {
        collector.add(buildClass(n, n.isInterface() ? "interface" : "class"));
        super.visit(n, collector);
    }

    @Override
    public void visit(EnumDeclaration n, List<ParsedClass> collector) {
        String qualifiedName = qualifiedName(packageName, n);
        List<String> ifaces = n.getImplementedTypes().stream()
                .map(t -> t.asString())
                .toList();
        List<String> annotations = n.getAnnotations().stream()
                .map(AnnotationExpr::getNameAsString)
                .toList();

        collector.add(new ParsedClass(
                projectName, packageName, n.getNameAsString(), qualifiedName,
                "enum", null, ifaces, annotations,
                extractAccess(n.getAccessSpecifier().asString()),
                false, false,
                extractJavadoc(n),
                filePath,
                n.getBegin().map(p -> p.line).orElse(0),
                n.getEnd().map(p -> p.line).orElse(0)
        ));
        super.visit(n, collector);
    }

    @Override
    public void visit(RecordDeclaration n, List<ParsedClass> collector) {
        String qualifiedName = qualifiedName(packageName, n);
        List<String> ifaces = n.getImplementedTypes().stream()
                .map(t -> t.asString())
                .toList();
        List<String> annotations = n.getAnnotations().stream()
                .map(AnnotationExpr::getNameAsString)
                .toList();

        collector.add(new ParsedClass(
                projectName, packageName, n.getNameAsString(), qualifiedName,
                "record", null, ifaces, annotations,
                "public", false, false,
                extractJavadoc(n),
                filePath,
                n.getBegin().map(p -> p.line).orElse(0),
                n.getEnd().map(p -> p.line).orElse(0)
        ));
        super.visit(n, collector);
    }

    @Override
    public void visit(AnnotationDeclaration n, List<ParsedClass> collector) {
        String qualifiedName = qualifiedName(packageName, n);
        List<String> annotations = n.getAnnotations().stream()
                .map(AnnotationExpr::getNameAsString)
                .toList();

        collector.add(new ParsedClass(
                projectName, packageName, n.getNameAsString(), qualifiedName,
                "annotation", null, List.of(), annotations,
                extractAccess(n.getAccessSpecifier().asString()),
                false, false,
                extractJavadoc(n),
                filePath,
                n.getBegin().map(p -> p.line).orElse(0),
                n.getEnd().map(p -> p.line).orElse(0)
        ));
        super.visit(n, collector);
    }

    private ParsedClass buildClass(ClassOrInterfaceDeclaration n, String kind) {
        String simpleName = n.getNameAsString();
        // Build qualified name including enclosing classes for nested types
        String qualifiedName = qualifiedName(packageName, n);

        String superclass = n.getExtendedTypes().isEmpty() ? null
                : n.getExtendedTypes().get(0).asString();
        List<String> ifaces = n.getImplementedTypes().stream()
                .map(t -> t.asString())
                .toList();
        List<String> annotations = n.getAnnotations().stream()
                .map(AnnotationExpr::getNameAsString)
                .toList();

        return new ParsedClass(
                projectName, packageName, simpleName, qualifiedName, kind,
                superclass, ifaces, annotations,
                extractAccess(n.getAccessSpecifier().asString()),
                n.isAbstract(), n.isStatic(),
                extractJavadoc(n),
                filePath,
                n.getBegin().map(p -> p.line).orElse(0),
                n.getEnd().map(p -> p.line).orElse(0)
        );
    }

    private static String extractJavadoc(com.github.javaparser.ast.Node n) {
        return n.getComment()
                .filter(c -> c instanceof JavadocComment)
                .map(c -> c.getContent().trim())
                .orElse(null);
    }

    /**
     * Binary-style qualified name of a type, including every enclosing type
     * (class, interface, enum, record or annotation): {@code pkg.Outer.Inner}.
     * Matches what JavaParser's symbol solver reports for nested types, so method
     * ids line up with resolved call targets.
     */
    public static String qualifiedName(String packageName, TypeDeclaration<?> n) {
        List<String> parts = new ArrayList<>();
        parts.add(n.getNameAsString());
        com.github.javaparser.ast.Node parent = n.getParentNode().orElse(null);
        while (parent instanceof TypeDeclaration<?> enclosing) {
            parts.add(0, enclosing.getNameAsString());
            parent = enclosing.getParentNode().orElse(null);
        }
        String classPath = String.join(".", parts);
        return packageName == null || packageName.isEmpty() ? classPath : packageName + "." + classPath;
    }

    private String extractAccess(String accessSpec) {
        return switch (accessSpec.toLowerCase()) {
            case "public" -> "public";
            case "protected" -> "protected";
            case "private" -> "private";
            default -> "package-private";
        };
    }
}
