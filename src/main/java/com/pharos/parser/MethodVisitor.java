package com.pharos.parser;

import com.github.javaparser.ast.body.*;
import com.github.javaparser.ast.comments.JavadocComment;
import com.github.javaparser.ast.expr.*;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import com.github.javaparser.resolution.declarations.ResolvedMethodDeclaration;
import com.pharos.parser.model.CallReference;
import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Visits method and constructor declarations inside a Java class.
 * Extracts full context: signature, body, javadoc, annotations, and outgoing call references.
 *
 * Mirrors mcp_server_code_extractor's approach: structured AST extraction with
 * fallback for unresolved symbols (external library calls).
 */
public class MethodVisitor extends VoidVisitorAdapter<List<ParsedMethod>> {

    private static final Logger log = LoggerFactory.getLogger(MethodVisitor.class);

    private final String projectName;
    private final String filePath;
    private final ParsedClass containingClass;

    public MethodVisitor(String projectName, String filePath, ParsedClass containingClass) {
        this.projectName = projectName;
        this.filePath = filePath;
        this.containingClass = containingClass;
    }

    @Override
    public void visit(MethodDeclaration n, List<ParsedMethod> collector) {
        try {
            collector.add(buildMethod(n));
        } catch (Exception e) {
            log.debug("Skipping method {} in {}: {}", n.getNameAsString(), filePath, e.getMessage());
        }
        // Do NOT call super.visit() — we handle nested classes at the file level
    }

    @Override
    public void visit(ConstructorDeclaration n, List<ParsedMethod> collector) {
        try {
            collector.add(buildConstructor(n));
        } catch (Exception e) {
            log.debug("Skipping constructor {} in {}: {}", n.getNameAsString(), filePath, e.getMessage());
        }
    }

    private ParsedMethod buildMethod(MethodDeclaration n) {
        List<String> paramTypes = n.getParameters().stream()
                .map(p -> p.getType().asString())
                .toList();
        List<String> paramNames = n.getParameters().stream()
                .map(p -> p.getNameAsString())
                .toList();

        String id = ParsedMethod.buildId(projectName, containingClass.qualifiedClassName(),
                n.getNameAsString(), paramTypes);
        String callerFqn = containingClass.qualifiedClassName() + "#"
                + n.getNameAsString() + "(" + String.join(",", paramTypes) + ")";

        List<CallReference> calls = extractCalls(n.getBody().orElse(null), callerFqn);

        String access = extractAccess(n.getAccessSpecifier().asString());
        String signature = buildSignature(n, access, paramTypes, paramNames);
        String body = n.toString();
        String javadoc = extractJavadoc(n);

        List<String> annotations = n.getAnnotations().stream()
                .map(AnnotationExpr::getNameAsString)
                .toList();
        List<String> thrown = n.getThrownExceptions().stream()
                .map(t -> t.asString())
                .toList();

        return new ParsedMethod(
                id, projectName,
                containingClass.packageName(),
                containingClass.className(),
                containingClass.qualifiedClassName(),
                n.getNameAsString(),
                signature,
                n.getType().asString(),
                paramTypes, paramNames,
                body, javadoc, annotations, access,
                n.isStatic(), false, n.isAbstract(), n.isSynchronized(),
                thrown, calls,
                filePath,
                n.getBegin().map(p -> p.line).orElse(0),
                n.getEnd().map(p -> p.line).orElse(0)
        );
    }

    private ParsedMethod buildConstructor(ConstructorDeclaration n) {
        List<String> paramTypes = n.getParameters().stream()
                .map(p -> p.getType().asString())
                .toList();
        List<String> paramNames = n.getParameters().stream()
                .map(p -> p.getNameAsString())
                .toList();

        String id = ParsedMethod.buildId(projectName, containingClass.qualifiedClassName(),
                "<init>", paramTypes);
        String callerFqn = containingClass.qualifiedClassName() + "#<init>("
                + String.join(",", paramTypes) + ")";

        List<CallReference> calls = extractCalls(n.getBody(), callerFqn);

        String access = extractAccess(n.getAccessSpecifier().asString());
        String annotations_str = n.getAnnotations().stream()
                .map(AnnotationExpr::getNameAsString)
                .collect(Collectors.joining(" "));
        String signature = (annotations_str.isEmpty() ? "" : "@" + annotations_str + " ")
                + access + " " + n.getNameAsString()
                + "(" + buildParamList(paramTypes, paramNames) + ")";

        List<String> annotations = n.getAnnotations().stream()
                .map(AnnotationExpr::getNameAsString)
                .toList();
        List<String> thrown = n.getThrownExceptions().stream()
                .map(t -> t.asString())
                .toList();

        return new ParsedMethod(
                id, projectName,
                containingClass.packageName(),
                containingClass.className(),
                containingClass.qualifiedClassName(),
                "<init>",
                signature,
                containingClass.className(), // return type for constructors = class name
                paramTypes, paramNames,
                n.toString(), extractJavadoc(n), annotations, access,
                false, true, false, false,
                thrown, calls,
                filePath,
                n.getBegin().map(p -> p.line).orElse(0),
                n.getEnd().map(p -> p.line).orElse(0)
        );
    }

    /**
     * Extracts all method call expressions from a method body.
     * Uses JavaParser's symbol resolver when possible; falls back to unresolved references.
     */
    private List<CallReference> extractCalls(BlockStmt body, String callerFqn) {
        if (body == null) return List.of();

        List<CallReference> calls = new ArrayList<>();
        body.findAll(MethodCallExpr.class).forEach(call -> {
            int line = call.getBegin().map(p -> p.line).orElse(0);
            try {
                ResolvedMethodDeclaration resolved = call.resolve();
                StringBuilder params = new StringBuilder();
                int numParams = resolved.getNumberOfParams();
                for (int i = 0; i < numParams; i++) {
                    if (i > 0) params.append(",");
                    try {
                        // Use simple type name to match how declarations store their paramTypes
                        String fullType = resolved.getParam(i).getType().describe();
                        params.append(simplifyType(fullType));
                    } catch (Exception ex) {
                        params.append("?");
                    }
                }
                String pkg = resolved.getPackageName();
                String className = resolved.getClassName();
                // Build qualified class name (handle nested classes separated by ".")
                String qualifiedClass = pkg.isEmpty() ? className : pkg + "." + className;
                String calleeFqn = qualifiedClass + "#"
                        + resolved.getName() + "(" + params + ")";
                calls.add(CallReference.resolved(callerFqn, calleeFqn, numParams, line));
            } catch (Throwable e) {
                // Expected for external library calls, unresolved types, or recursive var
                // type inference (StackOverflowError from JavaParser's symbol solver)
                String receiverTypeName = call.getScope()
                        .map(scope -> extractReceiverType(body, scope))
                        .orElse(null);
                int paramCount = call.getArguments().size();
                calls.add(CallReference.unresolved(callerFqn, call.getNameAsString(),
                        receiverTypeName, paramCount, line));
            }
        });
        return calls;
    }

    /**
     * Attempts to determine the simple type name of a call receiver using AST-only analysis
     * (no symbol resolution). Looks up variable/parameter declarations in the enclosing scope.
     *
     * Examples: {@code writer.addDocument(d)} → scope is NameExpr "writer" → looks for
     * {@code IndexWriter writer} in body/params → returns "IndexWriter".
     */
    private String extractReceiverType(BlockStmt body, Expression scope) {
        if (!(scope instanceof NameExpr nameExpr)) return null;
        String varName = nameExpr.getNameAsString();

        // Check local variable declarations in method body
        for (VariableDeclarator vd : body.findAll(VariableDeclarator.class)) {
            if (vd.getNameAsString().equals(varName)) {
                return stripGenerics(vd.getType().asString());
            }
        }

        // Check method/constructor parameters
        var enclosingMethod = body.findAncestor(com.github.javaparser.ast.body.MethodDeclaration.class);
        if (enclosingMethod.isPresent()) {
            for (var param : enclosingMethod.get().getParameters()) {
                if (param.getNameAsString().equals(varName)) {
                    return stripGenerics(param.getType().asString());
                }
            }
        }
        var enclosingCtor = body.findAncestor(com.github.javaparser.ast.body.ConstructorDeclaration.class);
        if (enclosingCtor.isPresent()) {
            for (var param : enclosingCtor.get().getParameters()) {
                if (param.getNameAsString().equals(varName)) {
                    return stripGenerics(param.getType().asString());
                }
            }
        }

        return null;
    }

    /** Strips generic type arguments: {@code "List<String>"} → {@code "List"}. */
    private String stripGenerics(String typeName) {
        int lt = typeName.indexOf('<');
        return lt > 0 ? typeName.substring(0, lt).trim() : typeName.trim();
    }

    private String buildSignature(MethodDeclaration n, String access,
                                   List<String> paramTypes, List<String> paramNames) {
        StringBuilder sb = new StringBuilder();
        if (!n.getAnnotations().isEmpty()) {
            n.getAnnotations().forEach(a -> sb.append("@").append(a.getNameAsString()).append(" "));
        }
        sb.append(access).append(" ");
        if (n.isStatic()) sb.append("static ");
        if (n.isAbstract()) sb.append("abstract ");
        if (n.isSynchronized()) sb.append("synchronized ");
        if (!n.getTypeParameters().isEmpty()) {
            sb.append("<").append(n.getTypeParameters().stream()
                    .map(tp -> tp.asString()).collect(Collectors.joining(", ")))
                    .append("> ");
        }
        sb.append(n.getType().asString()).append(" ").append(n.getNameAsString());
        sb.append("(").append(buildParamList(paramTypes, paramNames)).append(")");
        if (!n.getThrownExceptions().isEmpty()) {
            sb.append(" throws ").append(n.getThrownExceptions().stream()
                    .map(t -> t.asString()).collect(Collectors.joining(", ")));
        }
        return sb.toString();
    }

    private String buildParamList(List<String> types, List<String> names) {
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            parts.add(types.get(i) + " " + (i < names.size() ? names.get(i) : "arg" + i));
        }
        return String.join(", ", parts);
    }

    private String extractJavadoc(BodyDeclaration<?> n) {
        return n.getComment()
                .filter(c -> c instanceof JavadocComment)
                .map(c -> c.getContent().trim())
                .orElse(null);
    }

    /**
     * Simplifies a fully-qualified type name to match how JavaParser stores paramTypes
     * in method declarations (which use the name as written in source, usually simple names).
     * e.g. "java.nio.file.Path" → "Path", "java.util.List<java.nio.file.Path>" → "List<Path>"
     */
    private String simplifyType(String fullType) {
        if (fullType == null) return "?";
        // Handle array types
        StringBuilder suffix = new StringBuilder();
        while (fullType.endsWith("[]")) {
            suffix.append("[]");
            fullType = fullType.substring(0, fullType.length() - 2);
        }
        // Varargs
        if (fullType.endsWith("...")) {
            suffix.append("...");
            fullType = fullType.substring(0, fullType.length() - 3);
        }
        // Handle generics: simplify each type argument
        int lt = fullType.indexOf('<');
        if (lt > 0) {
            String base = simplifyType(fullType.substring(0, lt));
            String inner = fullType.substring(lt + 1, fullType.length() - 1);
            // Simplify inner types (comma-separated, but may have nested generics)
            String simplifiedInner = simplifyTypeArgs(inner);
            return base + "<" + simplifiedInner + ">" + suffix;
        }
        // Strip package prefix
        int dot = fullType.lastIndexOf('.');
        String simple = dot >= 0 ? fullType.substring(dot + 1) : fullType;
        return simple + suffix;
    }

    private String simplifyTypeArgs(String args) {
        // Split on top-level commas (not inside angle brackets)
        List<String> parts = new ArrayList<>();
        int depth = 0, start = 0;
        for (int i = 0; i < args.length(); i++) {
            char c = args.charAt(i);
            if (c == '<') depth++;
            else if (c == '>') depth--;
            else if (c == ',' && depth == 0) {
                parts.add(args.substring(start, i).trim());
                start = i + 1;
            }
        }
        parts.add(args.substring(start).trim());
        return parts.stream().map(this::simplifyType).collect(Collectors.joining(","));
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
