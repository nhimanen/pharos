package com.pharos.indexer;

import com.pharos.parser.model.CallReference;
import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedMethod;
import org.apache.lucene.document.*;
import org.apache.lucene.index.VectorSimilarityFunction;

import java.util.List;

/**
 * Maps a ParsedMethod to a Lucene Document and defines all field names.
 *
 * Field strategy:
 * - TextField (tokenized): body, javadoc, signature, methodName, className — for full-text search
 * - StringField (not tokenized): id, project, package, filePath, accessModifier — for exact match/filter
 * - IntPoint + StoredField: startLine, endLine — for range queries and retrieval
 * - KnnFloatVectorField: vectorEmbedding — for semantic KNN search (only when embeddings available)
 */
public class DocumentMapper {

    // Field names
    public static final String F_ID                = "id";
    public static final String F_PROJECT           = "project";
    public static final String F_PACKAGE           = "package";
    public static final String F_CLASS_NAME        = "className";
    public static final String F_QUALIFIED_CLASS   = "qualifiedClassName";
    public static final String F_METHOD_NAME       = "methodName";
    public static final String F_SIGNATURE         = "signature";
    public static final String F_RETURN_TYPE       = "returnType";
    public static final String F_PARAM_TYPES       = "paramTypes";
    public static final String F_BODY              = "body";
    public static final String F_JAVADOC           = "javadoc";
    public static final String F_ANNOTATIONS       = "annotations";
    public static final String F_ACCESS            = "accessModifier";
    public static final String F_IS_STATIC         = "isStatic";
    public static final String F_IS_CONSTRUCTOR    = "isConstructor";
    public static final String F_CALLED_METHODS    = "calledMethods";
    public static final String F_CALLER_METHODS    = "callerMethods";
    public static final String F_FILE_PATH         = "filePath";
    public static final String F_START_LINE        = "startLine";
    public static final String F_END_LINE          = "endLine";
    public static final String F_VECTOR            = "vectorEmbedding";

    /**
     * Discriminates method documents from class documents in the same index.
     * Values: "method" | "class"
     */
    public static final String F_DOC_TYPE          = "docType";

    /**
     * Source scope of the document.
     * Values: "prod" (production Java), "test" (test/benchmark Java), "docs" (non-Java files)
     */
    public static final String F_SCOPE             = "scope";

    // Graph-derived fields (populated during two-pass indexing)
    /** Number of methods that call this method — used for graph-boosted ranking. */
    public static final String F_IN_DEGREE         = "inDegree";
    /**
     * Space-separated simple names of methods that call this method.
     * Indexed as TextField so semantic search can find "methods in the X call chain".
     * e.g. "index indexFull indexIncremental" for a heavily-used internal method.
     */
    public static final String F_CALLER_CONTEXT    = "callerContext";

    private DocumentMapper() {}

    /**
     * Classifies a file path into one of three scopes:
     *   "docs" — non-Java files (markdown, config, shell scripts)
     *   "test" — Java test or benchmark sources
     *   "prod" — everything else (production Java)
     */
    static String computeScope(String filePath, boolean isChunk) {
        if (isChunk) return "docs";
        if (filePath == null) return "prod";
        String p = filePath.replace('\\', '/');
        if (p.endsWith(".md") || p.endsWith(".txt") || p.endsWith(".yml")
                || p.endsWith(".yaml") || p.endsWith(".sh") || p.endsWith(".xml")) return "docs";
        if (p.contains("/src/test/") || p.contains("/test/")
                || p.contains("/benchmark/") || p.contains("/jmh/")) return "test";
        return "prod";
    }

    /**
     * Converts a ParsedMethod to a Lucene Document.
     *
     * @param method      the parsed method
     * @param embedding   optional vector embedding (null = skip KnnFloatVectorField)
     * @param inDegree    number of methods that call this one (from call graph, 0 if unknown)
     * @param callerNames simple method names of callers (for callerContext text field)
     */
    public static Document toDocument(ParsedMethod method, float[] embedding,
                                       int inDegree, List<String> callerNames) {
        Document doc = new Document();

        // --- Document type discriminator ---
        // Chunks (generic file sections) are identified by the __chunk__ annotation
        boolean isChunk = method.annotations() != null && method.annotations().contains("__chunk__");
        doc.add(new StringField(F_DOC_TYPE, isChunk ? "chunk" : "method", Field.Store.YES));
        doc.add(new StringField(F_SCOPE, computeScope(method.filePath(), isChunk), Field.Store.YES));

        // --- Exact match fields (StringField: stored, not tokenized) ---
        doc.add(new StringField(F_ID, method.id(), Field.Store.YES));
        doc.add(new StringField(F_PROJECT, method.projectName(), Field.Store.YES));
        doc.add(new StringField(F_PACKAGE, nvl(method.packageName()), Field.Store.YES));
        doc.add(new StringField(F_RETURN_TYPE, nvl(method.returnType()), Field.Store.YES));
        doc.add(new StringField(F_ACCESS, nvl(method.accessModifier()), Field.Store.YES));
        doc.add(new StringField(F_FILE_PATH, method.filePath(), Field.Store.YES));
        doc.add(new StringField(F_IS_STATIC, String.valueOf(method.isStatic()), Field.Store.YES));
        doc.add(new StringField(F_IS_CONSTRUCTOR, String.valueOf(method.isConstructor()), Field.Store.YES));

        // --- Full-text fields (TextField: stored, tokenized) ---
        doc.add(new TextField(F_CLASS_NAME, nvl(method.className()), Field.Store.YES));
        doc.add(new TextField(F_QUALIFIED_CLASS, nvl(method.qualifiedClassName()), Field.Store.YES));
        doc.add(new TextField(F_METHOD_NAME, nvl(method.methodName()), Field.Store.YES));
        // Split camelCase/underscore method name for natural-language matching
        // e.g. "getUserById" → "get user by id"
        String splitName = splitIdentifier(method.methodName());
        if (!splitName.equals(method.methodName())) {
            doc.add(new TextField(F_METHOD_NAME, splitName, Field.Store.NO));
            // Also index the signature with the split name substituted in
            String sig = nvl(method.signature());
            String splitSig = sig.startsWith(nvl(method.methodName()))
                    ? splitName + sig.substring(method.methodName().length())
                    : splitName + " " + sig;
            doc.add(new TextField(F_SIGNATURE, splitSig, Field.Store.NO));
        }
        doc.add(new TextField(F_SIGNATURE, nvl(method.signature()), Field.Store.YES));
        doc.add(new TextField(F_BODY, nvl(method.body()), Field.Store.YES));
        if (method.javadoc() != null && !method.javadoc().isBlank()) {
            doc.add(new TextField(F_JAVADOC, method.javadoc(), Field.Store.YES));
        }

        // --- Multi-value: param types (StringField, repeated) ---
        for (String paramType : method.paramTypes()) {
            doc.add(new StringField(F_PARAM_TYPES, paramType, Field.Store.YES));
        }

        // --- Annotations (stored as comma-separated TextField for search) ---
        if (!method.annotations().isEmpty()) {
            doc.add(new TextField(F_ANNOTATIONS,
                    String.join(" ", method.annotations()), Field.Store.YES));
        }

        // --- Call references (multi-value StringField, not tokenized — exact lookup) ---
        for (CallReference call : method.calledMethods()) {
            if (call.resolved()) {
                doc.add(new StringField(F_CALLED_METHODS, call.calleeFqn(), Field.Store.YES));
            }
        }

        // --- Line numbers (IntPoint for range queries + StoredField for retrieval) ---
        doc.add(new IntPoint(F_START_LINE, method.startLine()));
        doc.add(new StoredField(F_START_LINE, method.startLine()));
        doc.add(new IntPoint(F_END_LINE, method.endLine()));
        doc.add(new StoredField(F_END_LINE, method.endLine()));

        // --- Graph-derived fields ---
        // NumericDocValuesField: used for score boosting at query time (not stored as text)
        doc.add(new NumericDocValuesField(F_IN_DEGREE, inDegree));
        // Also store for retrieval in stats/display
        doc.add(new StoredField(F_IN_DEGREE, inDegree));
        // Caller names as searchable text (e.g. "index indexFull buildAndSaveGraph")
        // Lucene terms are capped at 32766 bytes — truncate to avoid IndexWriter errors
        // on highly-connected methods (e.g. IndexWriter with hundreds of callers).
        if (callerNames != null && !callerNames.isEmpty()) {
            String callerContext = String.join(" ", callerNames);
            if (callerContext.length() > 32_000) {
                callerContext = callerContext.substring(0, 32_000);
                // trim to last complete word
                int lastSpace = callerContext.lastIndexOf(' ');
                if (lastSpace > 0) callerContext = callerContext.substring(0, lastSpace);
            }
            doc.add(new TextField(F_CALLER_CONTEXT, callerContext, Field.Store.YES));
        }

        // --- Vector embedding (only when available) ---
        if (embedding != null && embedding.length > 0) {
            doc.add(new KnnFloatVectorField(F_VECTOR, embedding, VectorSimilarityFunction.COSINE));
        }

        return doc;
    }

    /** Convenience overload for callers that don't have graph data yet (incremental updates). */
    public static Document toDocument(ParsedMethod method, float[] embedding) {
        return toDocument(method, embedding, 0, List.of());
    }

    /** Build the embedding input text for a method — concatenates javadoc + signature + body (truncated). */
    public static String buildEmbeddingText(ParsedMethod method) {
        // Document chunks use a different format: breadcrumb context + raw content
        if (method.annotations() != null && method.annotations().contains("__chunk__")) {
            return buildChunkEmbeddingText(method);
        }
        StringBuilder sb = new StringBuilder();
        if (method.javadoc() != null && !method.javadoc().isBlank()) {
            sb.append("/** ").append(method.javadoc().trim()).append(" */\n");
        }
        // Include split identifier so the embedding captures natural-language semantics
        // e.g. "getUserById" → "get user by id" alongside the original camelCase signature
        String splitName = splitIdentifier(method.methodName());
        if (!splitName.equals(method.methodName())) {
            sb.append("// ").append(splitName).append('\n');
        }
        sb.append(method.signature()).append(" {\n");
        String body = method.body();
        // Truncate body to stay within model context (jina-v2-base-code: 8192 tokens ≈ 32 000 chars;
        // 8 000 chars leaves room for javadoc + signature overhead)
        if (body != null && body.length() > 8_000) {
            body = body.substring(0, 8_000) + "...";
        }
        if (body != null) sb.append(body);
        sb.append("\n}");
        return sb.toString();
    }

    /**
     * Embedding text for a document chunk: heading breadcrumb + content.
     * Provides contextual grounding so semantically similar sections across
     * different files can be found by vector search.
     */
    private static String buildChunkEmbeddingText(ParsedMethod chunk) {
        StringBuilder sb = new StringBuilder();
        // File-level description as outer context
        if (chunk.javadoc() != null && !chunk.javadoc().isBlank()) {
            sb.append("Document: ").append(chunk.javadoc().trim()).append("\n");
        }
        // Heading breadcrumb (stored in signature field)
        if (chunk.signature() != null && !chunk.signature().isBlank()) {
            sb.append("Section: ").append(chunk.signature().trim()).append("\n\n");
        }
        // Raw content, truncated
        String body = chunk.body();
        if (body != null && body.length() > 4_000) {
            body = body.substring(0, 4_000) + "...";
        }
        if (body != null) sb.append(body);
        return sb.toString();
    }

    private static String nvl(String s) {
        return s != null ? s : "";
    }

    /**
     * Splits a camelCase or underscore-separated identifier into lowercase words.
     * Examples:
     *   "getUserById"      → "get user by id"
     *   "MAX_RETRY_COUNT"  → "max retry count"
     *   "parseXMLDocument" → "parse x m l document"  (single-char runs kept as-is by BM25)
     */
    static String splitIdentifier(String name) {
        if (name == null || name.isBlank()) return nvl(name);
        // Split on underscores first, then on camelCase transitions
        String[] parts = name.split("_");
        StringBuilder result = new StringBuilder();
        for (String part : parts) {
            if (!part.isEmpty()) {
                // Insert space before each uppercase letter that follows a lowercase letter
                // or before an uppercase letter followed by a lowercase (e.g. "XMLParser" → "XML Parser")
                String split = part
                        .replaceAll("([a-z])([A-Z])", "$1 $2")
                        .replaceAll("([A-Z]+)([A-Z][a-z])", "$1 $2");
                if (!result.isEmpty()) result.append(' ');
                result.append(split.toLowerCase());
            }
        }
        return result.toString();
    }

    /**
     * Adds caller method references to an existing document.
     * Called after the full project is indexed when reverse lookup is built.
     */
    public static void addCallerField(Document doc, String callerFqn) {
        doc.add(new StringField(F_CALLER_METHODS, callerFqn, Field.Store.YES));
    }

    /**
     * Converts a ParsedClass to a Lucene Document.
     *
     * @param cls             the parsed class
     * @param synthesizedBody concatenation of all method signatures + javadocs in the class
     * @param embedding       optional vector embedding (null = skip KnnFloatVectorField)
     */
    public static Document toClassDocument(ParsedClass cls, String synthesizedBody, float[] embedding) {
        Document doc = new Document();

        // --- Document type discriminator ---
        doc.add(new StringField(F_DOC_TYPE, "class", Field.Store.YES));
        doc.add(new StringField(F_SCOPE, computeScope(nvl(cls.filePath()), false), Field.Store.YES));

        // --- ID: "project:qualifiedClassName" ---
        String id = cls.projectName() + ":" + cls.qualifiedClassName();
        doc.add(new StringField(F_ID, id, Field.Store.YES));

        // --- Exact match fields ---
        doc.add(new StringField(F_PROJECT, cls.projectName(), Field.Store.YES));
        doc.add(new StringField(F_PACKAGE, nvl(cls.packageName()), Field.Store.YES));
        doc.add(new StringField(F_ACCESS, nvl(cls.accessModifier()), Field.Store.YES));
        doc.add(new StringField(F_FILE_PATH, nvl(cls.filePath()), Field.Store.YES));
        doc.add(new StringField(F_IS_STATIC, String.valueOf(cls.isStatic()), Field.Store.YES));
        // Store kind ("class", "interface", "enum", "record", "annotation") and superclass for display
        doc.add(new StringField("kind", nvl(cls.kind()), Field.Store.YES));
        doc.add(new StringField("superclass", nvl(cls.superclass()), Field.Store.YES));

        // --- Full-text fields ---
        doc.add(new TextField(F_CLASS_NAME, nvl(cls.className()), Field.Store.YES));
        doc.add(new TextField(F_QUALIFIED_CLASS, nvl(cls.qualifiedClassName()), Field.Store.YES));
        // Use className as methodName field too so field boosts still apply on the name
        doc.add(new TextField(F_METHOD_NAME, nvl(cls.className()), Field.Store.YES));
        // Split camelCase/underscore class name for natural-language matching
        String splitClassName = splitIdentifier(cls.className());
        if (!splitClassName.equals(cls.className())) {
            doc.add(new TextField(F_CLASS_NAME, splitClassName, Field.Store.NO));
            doc.add(new TextField(F_METHOD_NAME, splitClassName, Field.Store.NO));
        }
        if (cls.javadoc() != null && !cls.javadoc().isBlank()) {
            doc.add(new TextField(F_JAVADOC, cls.javadoc(), Field.Store.YES));
        }
        if (!cls.interfaces().isEmpty()) {
            doc.add(new TextField("interfaces", String.join(" ", cls.interfaces()), Field.Store.YES));
        }
        if (!cls.annotations().isEmpty()) {
            doc.add(new TextField(F_ANNOTATIONS, String.join(" ", cls.annotations()), Field.Store.YES));
        }
        // Synthesized body: all method signatures + javadocs concatenated — gives BM25 the vocabulary
        doc.add(new TextField(F_BODY, nvl(synthesizedBody), Field.Store.YES));

        // --- Line numbers ---
        doc.add(new IntPoint(F_START_LINE, cls.startLine()));
        doc.add(new StoredField(F_START_LINE, cls.startLine()));
        doc.add(new IntPoint(F_END_LINE, cls.endLine()));
        doc.add(new StoredField(F_END_LINE, cls.endLine()));

        // --- In-degree placeholder (classes don't have call-graph in-degrees; stored as 0) ---
        doc.add(new NumericDocValuesField(F_IN_DEGREE, 0));
        doc.add(new StoredField(F_IN_DEGREE, 0));

        // --- Vector embedding ---
        if (embedding != null && embedding.length > 0) {
            doc.add(new KnnFloatVectorField(F_VECTOR, embedding, VectorSimilarityFunction.COSINE));
        }

        return doc;
    }

    /** Build embedding input text for a class — javadoc + synthesized body (truncated). */
    public static String buildClassEmbeddingText(ParsedClass cls, String synthesizedBody) {
        StringBuilder sb = new StringBuilder();
        if (cls.javadoc() != null && !cls.javadoc().isBlank()) {
            sb.append("/** ").append(cls.javadoc().trim()).append(" */\n");
        }
        // Include split class name for natural-language embedding
        // e.g. "UserSessionManager" → "user session manager"
        String splitName = splitIdentifier(cls.className());
        if (!splitName.equals(cls.className())) {
            sb.append("// ").append(splitName).append('\n');
        }
        sb.append(cls.kind()).append(" ").append(cls.qualifiedClassName()).append(" {\n");
        String body = synthesizedBody;
        // Class synthesized body can be larger — cap at 16 000 chars
        if (body != null && body.length() > 16_000) {
            body = body.substring(0, 16_000) + "...";
        }
        if (body != null) sb.append(body);
        sb.append("\n}");
        return sb.toString();
    }
}
