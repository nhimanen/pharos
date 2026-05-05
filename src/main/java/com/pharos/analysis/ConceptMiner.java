package com.pharos.analysis;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Standalone concept-mining tool for building a synonym dictionary from
 * class-level javadocs in an indexed project.
 *
 * <h3>Mining strategy (option 3)</h3>
 * <ol>
 *   <li>Read all {@code docType=class} documents from the Lucene index
 *       (no re-parsing of source files needed — javadoc is stored at index time).
 *   <li>Extract {@code {@link ...}} and {@code @see} cross-references — these are
 *       ground-truth concept-to-class links written by the authors themselves.
 *   <li>Compute TF-IDF over the javadoc corpus to identify discriminative terms:
 *       words that are characteristic of a class but not common across all classes.
 *   <li>Output two tables for human review:
 *       <ul>
 *         <li>Per-class: top discriminative terms + outbound @link/@see targets
 *         <li>Inverted: term → classes (ready for use as synonym rules)
 *       </ul>
 * </ol>
 *
 * <p><b>Usage:</b>
 * <pre>{@code
 *   mvn exec:java -Dexec.mainClass=com.pharos.analysis.ConceptMiner \
 *                 -Dexec.args="lucene"
 * }</pre>
 *
 * <p>The output is designed for human review before integrating synonym rules
 * into a Lucene {@code SynonymGraphFilter} or query-expansion pipeline.
 */
public class ConceptMiner {

    // ── Regex patterns for @link / @see extraction ───────────────────────────

    /** {@link ClassName}, {@link pkg.ClassName#method}, {@link ClassName#method()} */
    private static final Pattern INLINE_LINK =
            Pattern.compile("\\{@link\\s+([\\w.#()]+)\\}");

    /** @see ClassName or @see pkg.ClassName */
    private static final Pattern SEE_TAG =
            Pattern.compile("@see\\s+([\\w.#()]+)");

    // ── English + Java stopwords ──────────────────────────────────────────────

    private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
        // English function words
        "a","an","the","and","or","but","if","in","on","at","to","for","of","with",
        "by","from","as","is","was","are","were","be","been","being","have","has",
        "had","do","does","did","will","would","could","should","may","might","shall",
        "that","this","these","those","it","its","we","our","you","your","they","their",
        "he","she","him","her","not","no","nor","so","yet","both","either","neither",
        "each","few","more","most","other","some","such","than","then","when","where",
        "which","who","whom","how","what","all","any","can","there","into",
        "about","also","just","only","very","over","under","after","before","between",
        "during","through","without","within","against","along","following","across",
        "per","i","e","g","etc","eg","ie",
        // Javadoc boilerplate
        "returns","return","param","throws","see","since","deprecated","note","use",
        "used","using","uses","given","new","creates","created",
        "provides","provided","represents","represented","contains","contained",
        "implements","implemented","extends","extended","wraps","wrapped","allows",
        "allowed","calls","called","method","methods","class","classes",
        "interface","interfaces","object","objects","instance","instances","type","types",
        "value","values","result","results","list","array","map","set","null","true",
        "false","int","long","string","boolean","void","byte","char","float","double",
        "public","private","protected","static","final","abstract","default","super",
        "exception","error","runtime",
        // Common filler in technical docs
        "example","following","follows","above","below","current","currently",
        "based","case","cases","same","different","specific","particular",
        "simple","basic","general","common","similar","equivalent",
        "whether","non","sub","pre","post","multi","single","multiple"
    ));

    // ── Entry point ───────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        String projectName = args.length > 0 ? args[0] : "lucene";
        int topTermsPerClass   = args.length > 1 ? Integer.parseInt(args[1]) : 12;
        int minDocFreq         = 2;   // term must appear in ≥2 class docs (not a typo)
        int maxDocFreqFraction = 30;  // term must appear in ≤30% of classes (not noise)
        double tfidfThreshold  = 0.05; // minimum TF-IDF score to include a term

        System.out.printf("ConceptMiner — project: %s | top terms/class: %d%n%n",
                projectName, topTermsPerClass);

        IndexConfig config     = IndexConfig.load();
        ProjectRegistry registry = new ProjectRegistry(config);
        LuceneIndexer indexer  = new LuceneIndexer(config);

        registry.find(projectName).orElseThrow(() ->
                new IllegalArgumentException("Project not indexed: " + projectName));

        IndexReader reader = indexer.openMultiReader(List.of(projectName));
        try {
            new ConceptMiner(topTermsPerClass, minDocFreq, maxDocFreqFraction, tfidfThreshold)
                    .mine(reader);
        } finally {
            indexer.close();
        }
    }

    // ── Instance ──────────────────────────────────────────────────────────────

    private final int    topTermsPerClass;
    private final int    minDocFreq;
    private final int    maxDocFreqFraction;
    private final double tfidfThreshold;

    public ConceptMiner(int topTermsPerClass, int minDocFreq,
                        int maxDocFreqFraction, double tfidfThreshold) {
        this.topTermsPerClass   = topTermsPerClass;
        this.minDocFreq         = minDocFreq;
        this.maxDocFreqFraction = maxDocFreqFraction;
        this.tfidfThreshold     = tfidfThreshold;
    }

    // ── Mining pipeline ───────────────────────────────────────────────────────

    public void mine(IndexReader reader) throws IOException {
        // 1. Load all class documents
        List<ClassDoc> docs = loadClassDocs(reader);
        System.out.printf("Loaded %d class documents%n%n", docs.size());

        // 2. Build document-frequency table: term → number of class docs containing it
        Map<String, Integer> docFreq = buildDocFreq(docs);
        int totalDocs = docs.size();
        int maxDf = Math.max(1, totalDocs * maxDocFreqFraction / 100);

        // 3. Compute TF-IDF and extract top terms per class
        for (ClassDoc doc : docs) {
            doc.topTerms = topTermsByTfIdf(doc, docFreq, totalDocs, maxDf);
        }

        // 4. Build inverted index: term → classes
        Map<String, List<String>> inverted = buildInverted(docs, docFreq, totalDocs, maxDf);

        // 5. Print output
        printConceptMap(docs);
        printInvertedIndex(inverted, docFreq, totalDocs);
        printCrossReferences(docs);
    }

    // ── Document loading ──────────────────────────────────────────────────────

    private List<ClassDoc> loadClassDocs(IndexReader reader) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        // Search for all class-type documents
        TopDocs hits = searcher.search(
                new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, "class")),
                Integer.MAX_VALUE);
        StoredFields storedFields = searcher.storedFields();

        List<ClassDoc> docs = new ArrayList<>(hits.scoreDocs.length);
        for (ScoreDoc sd : hits.scoreDocs) {
            Document luceneDoc = storedFields.document(sd.doc);
            String className  = luceneDoc.get(DocumentMapper.F_CLASS_NAME);
            String qualName   = luceneDoc.get(DocumentMapper.F_QUALIFIED_CLASS);
            String javadoc    = nvl(luceneDoc.get(DocumentMapper.F_JAVADOC));
            String body       = nvl(luceneDoc.get(DocumentMapper.F_BODY));

            // Combine javadoc + synthesized body as text source; javadoc is primary
            String text = javadoc.isBlank() ? body : javadoc + "\n" + body;
            if (className == null || className.isBlank()) continue;

            docs.add(new ClassDoc(className, qualName, javadoc, text,
                    extractLinks(javadoc)));
        }
        // Sort by class name for stable output
        docs.sort(Comparator.comparing(d -> d.className));
        return docs;
    }

    // ── TF-IDF ───────────────────────────────────────────────────────────────

    private Map<String, Integer> buildDocFreq(List<ClassDoc> docs) {
        Map<String, Integer> df = new HashMap<>();
        for (ClassDoc doc : docs) {
            for (String term : uniqueTerms(doc.text)) {
                df.merge(term, 1, Integer::sum);
            }
        }
        return df;
    }

    private List<String> topTermsByTfIdf(ClassDoc doc, Map<String, Integer> docFreq,
                                          int totalDocs, int maxDf) {
        Map<String, Integer> tf = termFreq(doc.text);
        int docLen = tf.values().stream().mapToInt(i -> i).sum();
        if (docLen == 0) return List.of();

        return tf.entrySet().stream()
                .filter(e -> {
                    int df = docFreq.getOrDefault(e.getKey(), 0);
                    return df >= minDocFreq && df <= maxDf;
                })
                .map(e -> {
                    double tfNorm = (double) e.getValue() / docLen;
                    double idf    = Math.log((double) totalDocs / docFreq.get(e.getKey()));
                    return Map.entry(e.getKey(), tfNorm * idf);
                })
                .filter(e -> e.getValue() >= tfidfThreshold)
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(topTermsPerClass)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    private Map<String, List<String>> buildInverted(List<ClassDoc> docs,
                                                     Map<String, Integer> docFreq,
                                                     int totalDocs, int maxDf) {
        Map<String, List<String>> inv = new TreeMap<>();
        for (ClassDoc doc : docs) {
            for (String term : doc.topTerms) {
                int df = docFreq.getOrDefault(term, 0);
                if (df >= minDocFreq && df <= maxDf) {
                    inv.computeIfAbsent(term, k -> new ArrayList<>()).add(doc.className);
                }
            }
        }
        return inv;
    }

    // ── Term extraction helpers ───────────────────────────────────────────────

    private Map<String, Integer> termFreq(String text) {
        Map<String, Integer> freq = new HashMap<>();
        for (String raw : text.split("[^a-zA-Z0-9]+")) {
            String term = normalize(raw);
            if (term != null) freq.merge(term, 1, Integer::sum);
        }
        return freq;
    }

    private Set<String> uniqueTerms(String text) {
        Set<String> terms = new HashSet<>();
        for (String raw : text.split("[^a-zA-Z0-9]+")) {
            String term = normalize(raw);
            if (term != null) terms.add(term);
        }
        return terms;
    }

    /**
     * Normalizes a raw token: lowercase, strip leading/trailing digits,
     * reject stopwords, short tokens, and pure-numeric tokens.
     */
    private String normalize(String raw) {
        if (raw.length() < 3) return null;
        String lower = raw.toLowerCase();
        // Strip leading/trailing digits
        lower = lower.replaceAll("^\\d+|\\d+$", "");
        if (lower.length() < 3) return null;
        if (STOPWORDS.contains(lower)) return null;
        // Skip purely numeric or version-like tokens
        if (lower.matches("\\d+(\\.\\d+)*")) return null;
        return lower;
    }

    // ── @link / @see extraction ───────────────────────────────────────────────

    private List<String> extractLinks(String javadoc) {
        List<String> links = new ArrayList<>();
        Matcher m1 = INLINE_LINK.matcher(javadoc);
        while (m1.find()) links.add(simpleClassName(m1.group(1)));
        Matcher m2 = SEE_TAG.matcher(javadoc);
        while (m2.find()) links.add(simpleClassName(m2.group(1)));
        return links.stream()
                .filter(s -> !s.isBlank() && Character.isUpperCase(s.charAt(0)))
                .distinct()
                .collect(Collectors.toList());
    }

    /** Extracts simple class name from a potentially qualified/method reference. */
    private String simpleClassName(String ref) {
        // Strip method part: "ClassName#method()" → "ClassName"
        int hash = ref.indexOf('#');
        if (hash >= 0) ref = ref.substring(0, hash);
        // Strip package: "org.apache.lucene.index.IndexWriter" → "IndexWriter"
        int dot = ref.lastIndexOf('.');
        if (dot >= 0) ref = ref.substring(dot + 1);
        // Strip generic params
        int lt = ref.indexOf('<');
        if (lt >= 0) ref = ref.substring(0, lt);
        return ref.trim();
    }

    // ── Output ────────────────────────────────────────────────────────────────

    private void printConceptMap(List<ClassDoc> docs) {
        System.out.println(bar());
        System.out.println("  CONCEPT MAP — discriminative terms per class (TF-IDF)");
        System.out.println(bar());
        System.out.printf("  %-40s  %s%n", "Class", "Top terms");
        System.out.println("  " + "─".repeat(100));

        for (ClassDoc doc : docs) {
            if (doc.topTerms.isEmpty() && doc.links.isEmpty()) continue;
            System.out.printf("  %-40s  %s%n",
                    truncate(doc.className, 38),
                    String.join(", ", doc.topTerms));
            if (!doc.links.isEmpty()) {
                System.out.printf("  %-40s  @link: %s%n", "",
                        String.join(", ", doc.links.stream().limit(6).toList()));
            }
        }
        System.out.println(bar());
        System.out.println();
    }

    private void printInvertedIndex(Map<String, List<String>> inverted,
                                     Map<String, Integer> docFreq, int totalDocs) {
        System.out.println(bar());
        System.out.println("  SYNONYM CANDIDATES — concept term → classes");
        System.out.printf("  (terms appearing in 2–%d%% of classes; sorted by specificity)%n",
                maxDocFreqFraction);
        System.out.println(bar());
        System.out.printf("  %-28s  %5s  %s%n", "Term", "df", "Classes");
        System.out.println("  " + "─".repeat(100));

        // Sort by doc-frequency ascending (most specific / discriminative first)
        inverted.entrySet().stream()
                .sorted(Comparator.comparingInt(e ->
                        docFreq.getOrDefault(e.getKey(), 0)))
                .forEach(e -> {
                    int df = docFreq.getOrDefault(e.getKey(), 0);
                    String classes = e.getValue().stream()
                            .sorted()
                            .collect(Collectors.joining(", "));
                    System.out.printf("  %-28s  %5d  %s%n",
                            e.getKey(), df, truncate(classes, 80));
                });

        System.out.println(bar());
        System.out.println();
    }

    private void printCrossReferences(List<ClassDoc> docs) {
        // Only print classes that have @link/@see references
        List<ClassDoc> withLinks = docs.stream()
                .filter(d -> !d.links.isEmpty())
                .toList();

        System.out.println(bar());
        System.out.printf("  @LINK / @SEE CROSS-REFERENCES  (%d classes with explicit refs)%n",
                withLinks.size());
        System.out.println(bar());
        System.out.printf("  %-40s  %s%n", "Class", "References");
        System.out.println("  " + "─".repeat(100));

        for (ClassDoc doc : withLinks) {
            System.out.printf("  %-40s  %s%n",
                    truncate(doc.className, 38),
                    String.join(", ", doc.links));
        }
        System.out.println(bar());
    }

    // ── Internal data model ───────────────────────────────────────────────────

    private static class ClassDoc {
        final String       className;
        final String       qualName;
        final String       javadoc;
        final String       text;    // javadoc + synthesized body
        final List<String> links;   // @link / @see targets
        List<String>       topTerms = List.of();

        ClassDoc(String className, String qualName, String javadoc,
                 String text, List<String> links) {
            this.className = className;
            this.qualName  = qualName;
            this.javadoc   = javadoc;
            this.text      = text;
            this.links     = links;
        }
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private static String nvl(String s) { return s != null ? s : ""; }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }

    private static String bar() { return "━".repeat(72); }

    // ── Auto-expansion: append new synonym rules to synonyms.txt ─────────────

    /**
     * Mines class-level TF-IDF concepts from {@code reader} and appends any
     * <em>new</em> synonym rules to {@code synonymFile}.
     *
     * <p>A rule is considered "new" if none of its left-hand terms already appear
     * on the left-hand side of an existing rule in the file.  This prevents
     * duplicate growth across incremental index runs.
     *
     * <p>Rules are written in directed format:
     * <pre>  discriminativeterm => classname</pre>
     * so that typing the mined concept term at query time expands to the class name.
     *
     * <p>Called automatically by {@link com.pharos.indexer.ProjectIndexManager}
     * after each successful full or incremental index run.
     *
     * @param reader      open Lucene reader for the indexed project
     * @param synonymFile path to {@code ~/.pharos/synonyms.txt}
     * @param projectName project name used in the comment header
     * @return number of new rules appended
     */
    public int appendNewSynonyms(IndexReader reader, Path synonymFile,
                                  String projectName) throws IOException {
        // Load existing left-hand terms to avoid duplicates
        Set<String> existingLhs = loadExistingLhs(synonymFile);

        List<ClassDoc> docs = loadClassDocs(reader);
        Map<String, Integer> docFreq = buildDocFreq(docs);
        int totalDocs = docs.size();
        int maxDf = Math.max(1, totalDocs * maxDocFreqFraction / 100);

        for (ClassDoc doc : docs) {
            doc.topTerms = topTermsByTfIdf(doc, docFreq, totalDocs, maxDf);
        }

        // Build new rules: discriminative term => className (lowercased)
        // Only include terms with df in [minDocFreq, maxDf] that aren't already covered
        List<String> newRules = new ArrayList<>();
        String date = java.time.LocalDate.now().toString();

        for (ClassDoc doc : docs) {
            String classTarget = doc.className.toLowerCase();
            for (String term : doc.topTerms) {
                if (existingLhs.contains(term)) continue;
                // Only emit terms that look like meaningful concepts (≥5 chars, not pure classname)
                if (term.length() < 5) continue;
                if (term.equals(classTarget)) continue;
                newRules.add(String.format("%-32s => %s  # auto:%s:%s",
                        term, classTarget, projectName, date));
                existingLhs.add(term); // prevent duplicates within this batch
            }
        }

        if (newRules.isEmpty()) return 0;

        // Append to file
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%n# ── Auto-mined from '%s' on %s (%d rules) ─%n",
                projectName, date, newRules.size()));
        for (String rule : newRules) {
            sb.append(rule).append('\n');
        }

        Files.writeString(synonymFile, sb.toString(),
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND);

        return newRules.size();
    }

    /** Reads existing synonyms.txt and collects all left-hand terms (before {@code =>}). */
    private static Set<String> loadExistingLhs(Path synonymFile) throws IOException {
        Set<String> lhs = new HashSet<>();
        if (!Files.exists(synonymFile)) return lhs;
        for (String line : Files.readAllLines(synonymFile)) {
            String stripped = line.strip();
            if (stripped.isEmpty() || stripped.startsWith("#")) continue;
            String left = stripped.contains("=>")
                    ? stripped.split("=>")[0] : stripped;
            for (String part : left.split(",")) {
                String t = part.strip().toLowerCase();
                if (!t.isEmpty()) lhs.add(t);
            }
        }
        return lhs;
    }
}
