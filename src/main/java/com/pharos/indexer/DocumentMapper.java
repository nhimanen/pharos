package com.pharos.indexer;

import com.pharos.parser.model.CallReference;
import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedMethod;
import org.apache.lucene.document.*;
import org.apache.lucene.index.VectorSimilarityFunction;

import java.util.Arrays;
import java.util.Map;

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
    /**
     * Legacy single-vector Lucene field, written by pharos versions before
     * multi-provider embeddings landed. New documents use per-model field names
     * derived from {@link #vectorFieldName(String)} instead. Search-side readers
     * fall back to this field when the queried project's {@code embeddedModels}
     * list is empty (i.e. it was indexed by an older pharos).
     *
     * <p>Value is intentionally still {@code "vectorEmbedding"} so existing
     * Lucene segments can be read without migration.
     *
     * @deprecated use {@link #vectorFieldName(String)} for new code; this constant
     *             remains only for read-side backward compatibility.
     */
    @Deprecated
    public static final String F_VECTOR_LEGACY     = "vectorEmbedding";
    /**
     * Legacy chunk-vectors field (LateInteractionField), same backward-compat
     * story as {@link #F_VECTOR_LEGACY}.
     *
     * @deprecated use {@link #chunkVectorFieldName(String)} for new code.
     */
    @Deprecated
    public static final String F_CHUNK_VECTORS_LEGACY = "chunkVectors";
    /**
     * Multi-vector field for late-interaction rescoring.
     * Stores one embedding vector per logical chunk of the document (via
     * {@link LateInteractionField}) alongside the single representative vector
     * (per-model KNN field) used for HNSW approximate retrieval.
     */
    public static final String F_CHUNK_VECTORS     = F_CHUNK_VECTORS_LEGACY;
    /**
     * Compact binary stored field: {@code [N:int32][start0:int32][end0:int32]...[startN-1][endN-1]}.
     * Parallel to {@link #F_CHUNK_VECTORS} — entry i gives the source line range of chunk i.
     * Used by {@link com.pharos.search.SnippetResolver} to map the best-scoring chunk index
     * back to a file line range without re-reading source files.
     */
    public static final String F_CHUNK_LINE_RANGES = "chunkLineRanges";

    /**
     * Discriminates method documents from class documents in the same index.
     * Values: "method" | "class"
     */
    public static final String F_DOC_TYPE          = "docType";
    /**
     * Class-type discriminator for class documents — not stored for method/chunk documents.
     * Values: {@code "interface"}, {@code "abstract"}, {@code "enum"}, {@code "record"},
     * {@code "annotation"}, {@code "class"} (concrete).
     * Used by {@link com.pharos.search.KeywordSearchStrategy#classTypeBonus} to boost
     * abstract/interface results when the query intent is {@code INTERFACE}, and to
     * boost concrete classes when intent is {@code IMPLEMENTATION}.
     */
    public static final String F_CLASS_TYPE        = "classType";

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
        if (p.endsWith(".md") || p.endsWith(".txt") || p.endsWith(".yml") || p.endsWith(".adoc")
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
        // Body may be null (lazy loading) — read from source file when not in memory.
        // preloadedLines is non-null when the caller has already read the file for this batch.
        String methodBody = method.body() != null ? method.body()
                : readBodyFromFile(method.filePath(), method.startLine(), method.endLine(), null);
        doc.add(new TextField(F_BODY, methodBody, Field.Store.YES));
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
            doc.add(new KnnFloatVectorField(F_VECTOR_LEGACY, embedding, VectorSimilarityFunction.COSINE));
        }

        return doc;
    }

    /**
     * Multi-vector overload: writes one {@link LateInteractionField} (all chunk embeddings)
     * and one {@link KnnFloatVectorField} (mean of chunk embeddings as the HNSW representative).
     *
     * @param chunkEmbeddings one float[] per logical chunk; must be non-null and non-empty
     */
    public static Document toDocumentMultiVec(ParsedMethod method, float[][] chunkEmbeddings,
                                               int inDegree, List<String> callerNames) {
        return toDocumentMultiVec(method, chunkEmbeddings, null, inDegree, callerNames);
    }

    /**
     * Multi-vector overload with explicit chunk line ranges.
     *
     * @param chunkLineRanges parallel to {@code chunkEmbeddings}: {@code int[i] = {startLine, endLine}}
     *                        for chunk i.  May be null — ranges are omitted from the document.
     */
    public static Document toDocumentMultiVec(ParsedMethod method, float[][] chunkEmbeddings,
                                               int[][] chunkLineRanges,
                                               int inDegree, List<String> callerNames) {
        return toDocumentMultiVec(method, chunkEmbeddings, chunkLineRanges, inDegree, callerNames, null);
    }

    /** Overload with preloaded source lines for efficient lazy body reading. */
    public static Document toDocumentMultiVec(ParsedMethod method, float[][] chunkEmbeddings,
                                               int[][] chunkLineRanges,
                                               int inDegree, List<String> callerNames,
                                               java.util.List<String> preloadedLines) {
        float[] representative = meanPool(chunkEmbeddings);
        Document doc = toDocument(method, representative, inDegree, callerNames, preloadedLines);
        doc.add(new LateInteractionField(F_CHUNK_VECTORS, chunkEmbeddings));
        if (chunkLineRanges != null && chunkLineRanges.length > 0) {
            doc.add(new StoredField(F_CHUNK_LINE_RANGES, encodeLineRanges(chunkLineRanges)));
        }
        return doc;
    }

    /**
     * Overload that accepts preloaded source lines so lazy body reads (body == null)
     * share a single in-memory list rather than re-reading the file per method.
     */
    public static Document toDocument(ParsedMethod method, float[] embedding,
                                       int inDegree, List<String> callerNames,
                                       java.util.List<String> preloadedLines) {
        if (method.body() != null || preloadedLines == null || preloadedLines.isEmpty()) {
            return toDocument(method, embedding, inDegree, callerNames);
        }
        // Create a body-filled copy so the normal toDocument path finds a non-null body.
        String body = readBodyFromFile(method.filePath(), method.startLine(),
                method.endLine(), preloadedLines);
        ParsedMethod withBody = new com.pharos.parser.model.ParsedMethod(
                method.id(), method.projectName(), method.packageName(), method.className(),
                method.qualifiedClassName(), method.methodName(), method.signature(),
                method.returnType(), method.paramTypes(), method.paramNames(),
                body, method.javadoc(), method.annotations(), method.accessModifier(),
                method.isStatic(), method.isConstructor(), method.isAbstract(),
                method.isSynchronized(), method.thrownExceptions(), method.calledMethods(),
                method.filePath(), method.startLine(), method.endLine());
        return toDocument(withBody, embedding, inDegree, callerNames);
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
            // Cap javadoc — generated/copied javadoc can be huge.
            String jd = method.javadoc().trim();
            if (jd.length() > 4_000) jd = jd.substring(0, 4_000) + "...";
            sb.append("/** ").append(jd).append(" */\n");
        } else {
            // No javadoc — synthesize a natural-language description from the parsed structure
            // so the embedding captures semantics even for undocumented methods.
            String synthesized = synthesizeDescription(method);
            if (!synthesized.isBlank()) {
                sb.append("// ").append(synthesized).append('\n');
            }
        }
        // Include split identifier so the embedding captures natural-language semantics
        // e.g. "getUserById" → "get user by id" alongside the original camelCase signature
        String splitName = splitIdentifier(method.methodName());
        if (!splitName.equals(method.methodName())) {
            sb.append("// ").append(splitName).append('\n');
        }
        sb.append(method.signature()).append(" {\n");
        // Body may be null (lazy loading) — read from source file when not in memory
        String body = method.body() != null ? method.body()
                : readBodyFromFile(method.filePath(), method.startLine(), method.endLine());
        // Truncate body to stay within model context (jina-v2-base-code: 8192 tokens ≈ 32 000 chars;
        // 8 000 chars leaves room for javadoc + signature overhead)
        if (body != null && body.length() > 8_000) {
            body = body.substring(0, 8_000) + "...";
        }
        if (body != null) sb.append(body);
        sb.append("\n}");
        String result = sb.toString();
        if (result.length() > 16_000) {
            result = result.substring(0, 16_000) + "\n// ... (truncated)\n}";
        }
        return result;
    }

    /**
     * Synthesizes a compact natural-language description from a method's parsed structure
     * when no javadoc is present.
     *
     * <p>Format: <code>{verb phrase}: given {params} — returns {type} — calls {callees}</code>
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code findActive(String tenantId)} → {@code "find active: given tenant id — returns list of User — calls findByTenantAndStatus"}</li>
     *   <li>{@code processPayment(Order order, BigDecimal amount)} → {@code "process payment: given order, amount — returns boolean"}</li>
     *   <li>{@code UserService(UserRepo repo)} → {@code "creates UserService: given repo"}</li>
     * </ul>
     */
    public static String synthesizeDescription(ParsedMethod method) {
        StringBuilder sb = new StringBuilder();

        // Verb phrase from split method name
        if (method.isConstructor()) {
            sb.append("creates ").append(method.className());
        } else {
            String verbPhrase = splitIdentifier(method.methodName());
            if (!verbPhrase.isBlank()) sb.append(verbPhrase);
        }

        // Parameters
        List<String> pNames = method.paramNames();
        List<String> pTypes = method.paramTypes();
        if (!pNames.isEmpty()) {
            List<String> paramParts = new java.util.ArrayList<>();
            for (int i = 0; i < pNames.size(); i++) {
                String pName = pNames.get(i);
                if ("self".equals(pName) || "this".equals(pName)) continue;
                String label = splitIdentifier(pName);
                if (!label.isBlank()) paramParts.add(label);
            }
            if (!paramParts.isEmpty()) {
                sb.append(": given ").append(String.join(", ", paramParts));
            }
        }

        // Return type (skip for constructors and void)
        if (!method.isConstructor()) {
            String formatted = formatReturnType(method.returnType());
            if (formatted != null) {
                sb.append(" — returns ").append(formatted);
            }
        }

        // Top-5 called method names (resolved only)
        if (method.calledMethods() != null) {
            String callees = method.calledMethods().stream()
                    .filter(CallReference::resolved)
                    .map(c -> {
                        String fqn = c.calleeFqn();
                        int hash = fqn.indexOf('#');
                        int paren = fqn.indexOf('(');
                        String name = hash >= 0
                                ? fqn.substring(hash + 1, paren > hash ? paren : fqn.length())
                                : c.calleeSimpleName();
                        return splitIdentifier(name);
                    })
                    .filter(s -> !s.isBlank())
                    .distinct()
                    .limit(5)
                    .collect(java.util.stream.Collectors.joining(", "));
            if (!callees.isBlank()) {
                sb.append(" — calls ").append(callees);
            }
        }

        return sb.toString();
    }

    /**
     * Formats a Java return type into natural language, with collection-awareness.
     *
     * <ul>
     *   <li>{@code List<User>}       → {@code "list of User"}</li>
     *   <li>{@code Set<Order>}        → {@code "set of Order"}</li>
     *   <li>{@code Map<String,User>}  → {@code "map of String to User"}</li>
     *   <li>{@code Optional<User>}    → {@code "optional User"}</li>
     *   <li>{@code User[]}            → {@code "array of User"}</li>
     *   <li>{@code User}              → {@code "User"}</li>
     *   <li>{@code void}              → {@code null} (omitted from description)</li>
     * </ul>
     */
    public static String formatReturnType(String returnType) {
        if (returnType == null || returnType.isBlank()) return null;
        String rt = returnType.trim();
        if ("void".equals(rt)) return null;

        // Array: Type[]
        if (rt.endsWith("[]")) {
            String element = rt.substring(0, rt.length() - 2).trim();
            return "array of " + stripGenericParams(element);
        }

        int lt = rt.indexOf('<');
        if (lt > 0) {
            String outer = rt.substring(0, lt).trim();
            String inner = rt.substring(lt + 1, rt.lastIndexOf('>') > lt ? rt.lastIndexOf('>') : rt.length()).trim();
            String outerLower = outer.toLowerCase();

            // Map variants
            if (MAP_TYPES.contains(outerLower)) {
                int comma = inner.indexOf(',');
                if (comma > 0) {
                    String key   = stripGenericParams(inner.substring(0, comma).trim());
                    String value = stripGenericParams(inner.substring(comma + 1).trim());
                    return "map of " + key + " to " + value;
                }
                return "map";
            }

            // Optional / Future / Mono / Flux (single-element wrapper)
            if (WRAPPER_TYPES.contains(outerLower)) {
                return outerLower.replace("completablefuture", "future").replace("listenablefuture", "future")
                        + " " + stripGenericParams(inner);
            }

            // Collection types
            String collectionWord = COLLECTION_WORDS.get(outerLower);
            if (collectionWord != null) {
                return collectionWord + " of " + stripGenericParams(inner);
            }

            // Unknown generic — just use outer name
            return outer;
        }

        // Primitive or simple class name
        return rt;
    }

    private static String stripGenericParams(String type) {
        if (type == null) return "";
        int lt = type.indexOf('<');
        String base = lt >= 0 ? type.substring(0, lt).trim() : type.trim();
        return base.replace("[]", "").trim();
    }

    private static final java.util.Set<String> MAP_TYPES = java.util.Set.of(
            "map", "hashmap", "linkedhashmap", "treemap", "concurrentmap",
            "concurrenthashmap", "enummap", "sortedmap", "navigablemap");

    private static final java.util.Set<String> WRAPPER_TYPES = java.util.Set.of(
            "optional", "completablefuture", "listenablefuture", "future",
            "mono", "flux", "observable", "single", "maybe");

    private static final java.util.Map<String, String> COLLECTION_WORDS;
    static {
        java.util.Map<String, String> m = new java.util.LinkedHashMap<>();
        for (String t : new String[]{"list","arraylist","linkedlist","copyonwritearraylist","vector"})
            m.put(t, "list");
        for (String t : new String[]{"set","hashset","linkedhashset","treeset","enumset","sortedset","navigableset"})
            m.put(t, "set");
        for (String t : new String[]{"collection","iterable","queue","deque","arraydeque","linkedblockingqueue"})
            m.put(t, "collection");
        m.put("stream", "stream");
        m.put("page",   "page");
        m.put("slice",  "slice");
        COLLECTION_WORDS = java.util.Collections.unmodifiableMap(m);
    }

    /**
     * Embedding text for a document chunk: heading breadcrumb + content.
     * Provides contextual grounding so semantically similar sections across
     * different files can be found by vector search.
     */
    private static String buildChunkEmbeddingText(ParsedMethod chunk) {
        StringBuilder sb = new StringBuilder();
        // File-level description as outer context. Cap at 4k chars to keep
        // breadcrumb a breadcrumb; the chunk's own body carries the real
        // content. Documents with massive preambles (whole-file READMEs
        // pinned in javadoc) were the source of 1.3 MB inputs.
        if (chunk.javadoc() != null && !chunk.javadoc().isBlank()) {
            String preamble = chunk.javadoc().trim();
            if (preamble.length() > 4_000) preamble = preamble.substring(0, 4_000) + "...";
            sb.append("Document: ").append(preamble).append("\n");
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
     * Derives the {@link #F_CLASS_TYPE} value from a parsed class.
     * Merges {@code kind} and {@code isAbstract} into a single searchable token:
     * {@code interface}, {@code abstract}, {@code enum}, {@code record},
     * {@code annotation}, or {@code class} (concrete).
     */
    private static String classTypeOf(com.pharos.parser.model.ParsedClass cls) {
        if (cls.kind() == null) return "class";
        return switch (cls.kind()) {
            case "interface"   -> "interface";
            case "enum"        -> "enum";
            case "record"      -> "record";
            case "annotation"  -> "annotation";
            default            -> cls.isAbstract() ? "abstract" : "class";
        };
    }

    /**
     * Splits a camelCase or underscore-separated identifier into lowercase words.
     * Examples:
     *   "getUserById"      → "get user by id"
     *   "MAX_RETRY_COUNT"  → "max retry count"
     *   "parseXMLDocument" → "parse x m l document"  (single-char runs kept as-is by BM25)
     */
    public static String splitIdentifier(String name) {
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
        String docType = "document".equals(cls.kind()) ? "doc" : "class";
        doc.add(new StringField(F_DOC_TYPE, docType, Field.Store.YES));
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
        // classType: merges kind + isAbstract into one field for boosting/filtering
        doc.add(new StringField(F_CLASS_TYPE, classTypeOf(cls), Field.Store.YES));

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
            doc.add(new KnnFloatVectorField(F_VECTOR_LEGACY, embedding, VectorSimilarityFunction.COSINE));
        }

        return doc;
    }

    /**
     * Multi-vector overload for classes: mean-pools chunk embeddings as representative
     * vector and stores all chunks in a {@link LateInteractionField}.
     */
    public static Document toClassDocumentMultiVec(ParsedClass cls, String synthesizedBody,
                                                    float[][] chunkEmbeddings) {
        return toClassDocumentMultiVec(cls, synthesizedBody, chunkEmbeddings, null);
    }

    /** Multi-vector overload for classes with explicit chunk line ranges. */
    public static Document toClassDocumentMultiVec(ParsedClass cls, String synthesizedBody,
                                                    float[][] chunkEmbeddings,
                                                    int[][] chunkLineRanges) {
        float[] representative = meanPool(chunkEmbeddings);
        Document doc = toClassDocument(cls, synthesizedBody, representative);
        doc.add(new LateInteractionField(F_CHUNK_VECTORS, chunkEmbeddings));
        if (chunkLineRanges != null && chunkLineRanges.length > 0) {
            doc.add(new StoredField(F_CHUNK_LINE_RANGES, encodeLineRanges(chunkLineRanges)));
        }
        return doc;
    }

    // ── Multi-model vector field naming ──────────────────────────────────────

    /**
     * Maps an embedding {@code modelId} to its Lucene KNN field name.
     * The mapping is deterministic and stable: lowercase, replace anything
     * outside {@code [a-z0-9._-]} with {@code _}, collapse runs of {@code _},
     * cap at 96 chars. Same {@code modelId} always yields the same field name.
     *
     * <p>Examples:
     * <pre>
     *   "jina-code-v2"                      → "vec.jina-code-v2"
     *   "Qwen/Qwen3-Embedding-4B@1024"      → "vec.qwen_qwen3-embedding-4b_1024"
     * </pre>
     */
    public static String vectorFieldName(String modelId) {
        return "vec." + sanitizeModelId(modelId);
    }

    /** Per-model chunk-vectors (LateInteractionField) name; same sanitization. */
    public static String chunkVectorFieldName(String modelId) {
        return "chunkVec." + sanitizeModelId(modelId);
    }

    /**
     * Resolves the Lucene KNN field to read for a given model id, handling the
     * legacy-alias fallback. Pre-upgrade indexes carry the old {@code vectorEmbedding}
     * field with no model identity; we treat them as belonging to the synthetic
     * {@link com.pharos.config.IndexConfig#LEGACY_MODEL_ID} so search code can route
     * a single legacy-config query to either the old field name or a new per-model
     * field uniformly.
     */
    public static String vectorFieldFor(String modelId) {
        if (com.pharos.config.IndexConfig.LEGACY_MODEL_ID.equals(modelId)) return F_VECTOR_LEGACY;
        return vectorFieldName(modelId);
    }

    /** Counterpart of {@link #vectorFieldFor(String)} for the chunk-vectors field. */
    public static String chunkVectorFieldFor(String modelId) {
        if (com.pharos.config.IndexConfig.LEGACY_MODEL_ID.equals(modelId)) return F_CHUNK_VECTORS_LEGACY;
        return chunkVectorFieldName(modelId);
    }

    private static String sanitizeModelId(String modelId) {
        if (modelId == null || modelId.isBlank()) {
            throw new IllegalArgumentException("modelId must be non-blank for field naming");
        }
        String s = modelId.toLowerCase(java.util.Locale.ROOT)
                .replaceAll("[^a-z0-9._-]", "_")
                .replaceAll("_+", "_");
        if (s.length() > 96) s = s.substring(0, 96);
        return s;
    }

    // ── Multi-model document overloads ───────────────────────────────────────
    // Each new overload delegates to the corresponding legacy overload with a
    // null embedding (which suppresses the legacy F_VECTOR_LEGACY field), then
    // attaches one KnnFloatVectorField per (modelId, vector) entry under the
    // per-model field name from vectorFieldName(modelId). Same pattern for
    // chunk-vector LateInteractionFields. Chunk line ranges remain a single
    // model-independent field — the chunk decomposition is shared.

    /**
     * Multi-model variant of {@link #toDocument(ParsedMethod, float[], int, List, List)}.
     * Writes one {@link KnnFloatVectorField} per non-null vector in
     * {@code vectorsByModel}, keyed by per-model field names. Pass an empty map
     * to skip vector storage entirely (keyword-only indexing).
     */
    public static Document toDocument(ParsedMethod method,
                                       Map<String, float[]> vectorsByModel,
                                       int inDegree, List<String> callerNames,
                                       List<String> preloadedLines) {
        Document doc = toDocument(method, (float[]) null, inDegree, callerNames, preloadedLines);
        attachVectorFields(doc, vectorsByModel);
        return doc;
    }

    /** Multi-model variant of {@link #toClassDocument(ParsedClass, String, float[])}. */
    public static Document toClassDocument(ParsedClass cls, String synthesizedBody,
                                            Map<String, float[]> vectorsByModel) {
        Document doc = toClassDocument(cls, synthesizedBody, (float[]) null);
        attachVectorFields(doc, vectorsByModel);
        return doc;
    }

    /**
     * Multi-model variant of {@link #toDocumentMultiVec(ParsedMethod, float[][], int[][], int, List, List)}.
     * For each model in {@code chunkVectorsByModel}, mean-pools the chunk vectors as the
     * KNN representative and attaches the full chunk array as the per-model
     * {@link LateInteractionField}. Chunk line ranges are model-independent.
     */
    public static Document toDocumentMultiVec(ParsedMethod method,
                                               Map<String, float[][]> chunkVectorsByModel,
                                               int[][] chunkLineRanges,
                                               int inDegree, List<String> callerNames,
                                               List<String> preloadedLines) {
        Map<String, float[]> reps = meanPoolPerModel(chunkVectorsByModel);
        Document doc = toDocument(method, reps, inDegree, callerNames, preloadedLines);
        attachChunkVectorFields(doc, chunkVectorsByModel);
        if (chunkLineRanges != null && chunkLineRanges.length > 0) {
            doc.add(new StoredField(F_CHUNK_LINE_RANGES, encodeLineRanges(chunkLineRanges)));
        }
        return doc;
    }

    /** Multi-model variant of {@link #toClassDocumentMultiVec(ParsedClass, String, float[][], int[][])}. */
    public static Document toClassDocumentMultiVec(ParsedClass cls, String synthesizedBody,
                                                    Map<String, float[][]> chunkVectorsByModel,
                                                    int[][] chunkLineRanges) {
        Map<String, float[]> reps = meanPoolPerModel(chunkVectorsByModel);
        Document doc = toClassDocument(cls, synthesizedBody, reps);
        attachChunkVectorFields(doc, chunkVectorsByModel);
        if (chunkLineRanges != null && chunkLineRanges.length > 0) {
            doc.add(new StoredField(F_CHUNK_LINE_RANGES, encodeLineRanges(chunkLineRanges)));
        }
        return doc;
    }

    private static void attachVectorFields(Document doc, Map<String, float[]> vectorsByModel) {
        if (vectorsByModel == null) return;
        for (Map.Entry<String, float[]> e : vectorsByModel.entrySet()) {
            float[] v = e.getValue();
            if (v == null || v.length == 0) continue;
            doc.add(new KnnFloatVectorField(
                    vectorFieldName(e.getKey()), v, VectorSimilarityFunction.COSINE));
        }
    }

    private static void attachChunkVectorFields(Document doc,
                                                 Map<String, float[][]> chunkVectorsByModel) {
        if (chunkVectorsByModel == null) return;
        for (Map.Entry<String, float[][]> e : chunkVectorsByModel.entrySet()) {
            float[][] cv = e.getValue();
            if (cv == null || cv.length == 0) continue;
            doc.add(new LateInteractionField(chunkVectorFieldName(e.getKey()), cv));
        }
    }

    private static Map<String, float[]> meanPoolPerModel(Map<String, float[][]> chunkVectorsByModel) {
        if (chunkVectorsByModel == null || chunkVectorsByModel.isEmpty()) return Map.of();
        Map<String, float[]> out = new java.util.LinkedHashMap<>(chunkVectorsByModel.size());
        for (Map.Entry<String, float[][]> e : chunkVectorsByModel.entrySet()) {
            float[][] cv = e.getValue();
            if (cv == null || cv.length == 0) continue;
            out.put(e.getKey(), meanPool(cv));
        }
        return out;
    }

    /**
     * Encodes chunk line ranges as a compact byte array.
     * Format: {@code [N:int32][start0:int32][end0:int32]...[startN-1:int32][endN-1:int32]}.
     */
    public static byte[] encodeLineRanges(int[][] ranges) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(4 + ranges.length * 8);
        buf.putInt(ranges.length);
        for (int[] r : ranges) { buf.putInt(r[0]); buf.putInt(r[1]); }
        return buf.array();
    }

    /** Decodes a byte array produced by {@link #encodeLineRanges}. */
    public static int[][] decodeLineRanges(byte[] encoded) {
        if (encoded == null || encoded.length < 4) return new int[0][];
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(encoded);
        int n = buf.getInt();
        int[][] ranges = new int[n][2];
        for (int i = 0; i < n; i++) { ranges[i][0] = buf.getInt(); ranges[i][1] = buf.getInt(); }
        return ranges;
    }

    /**
     * Reads a method or class body directly from the source file.
     * Called when {@link com.pharos.parser.model.ParsedMethod#body()} returns null
     * (lazy-loading strategy — bodies are not stored in ParsedMethod to reduce heap).
     *
     * <p>Performance: callers in the indexing pipeline preload the file's line list once
     * and pass it here to avoid repeated {@link java.nio.file.Files#readAllLines} calls
     * for every method in the same file.
     *
     * @param lines  preloaded source lines for the file, or null to read on demand
     */
    public static String readBodyFromFile(String filePath, int startLine, int endLine,
                                           java.util.List<String> lines) {
        if (startLine < 1 || endLine < startLine) return "";
        java.util.List<String> src = lines;
        if (src == null) {
            try {
                src = java.nio.file.Files.readAllLines(java.nio.file.Path.of(filePath));
            } catch (java.io.IOException e) {
                return "";
            }
        }
        int from = Math.min(startLine - 1, src.size());
        int to   = Math.min(endLine, src.size());
        return from < to ? String.join("\n", src.subList(from, to)) : "";
    }

    /** Convenience overload that reads from disk without a preloaded line list. */
    public static String readBodyFromFile(String filePath, int startLine, int endLine) {
        return readBodyFromFile(filePath, startLine, endLine, null);
    }

    /** Mean-pools an array of chunk embeddings into one representative vector. */
    public static float[] meanPool(float[][] chunks) {
        if (chunks == null || chunks.length == 0) return null;
        if (chunks.length == 1) return chunks[0];
        int dims = chunks[0].length;
        float[] mean = new float[dims];
        for (float[] v : chunks) {
            for (int d = 0; d < dims; d++) mean[d] += v[d];
        }
        for (int d = 0; d < dims; d++) mean[d] /= chunks.length;
        return mean;
    }

    /** Build embedding input text for a class — javadoc + synthesized body (truncated). */
    public static String buildClassEmbeddingText(ParsedClass cls, String synthesizedBody) {
        StringBuilder sb = new StringBuilder();
        if (cls.javadoc() != null && !cls.javadoc().isBlank()) {
            // Cap javadoc independently — document-kind classes can carry huge
            // preambles (entire markdown headers, license blocks, etc.).
            String jd = cls.javadoc().trim();
            if (jd.length() > 4_000) jd = jd.substring(0, 4_000) + "...";
            sb.append("/** ").append(jd).append(" */\n");
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
        // Final hard cap. Catches any combination of huge javadoc + body + name
        // that could still exceed what an embedding model can consume.
        String result = sb.toString();
        if (result.length() > 20_000) {
            result = result.substring(0, 20_000) + "\n// ... (truncated)\n}";
        }
        return result;
    }
}
