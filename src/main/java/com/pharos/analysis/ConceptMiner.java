package com.pharos.analysis;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
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

    private static final Pattern HYPHEN_COMPOUND =
            Pattern.compile("([a-zA-Z0-9]{1,})-([a-zA-Z0-9]{1,})");

    // ── Acronym / parenthetical expansion patterns ────────────────────────────
    // "finite state machine (FST)" → bigrams/trigrams of the expansion → FST
    private static final Pattern PAT_EXPANSION_ABBREV =
            Pattern.compile("([a-zA-Z][\\w\\s-]{3,35})\\s*\\(([A-Z]{2,10})\\)");
    // "WAND (Weak AND)" → bigrams of expansion → WANDScorer
    private static final Pattern PAT_ABBREV_EXPANSION =
            Pattern.compile("\\b([A-Z]{2,10})\\s+\\(([a-zA-Z][\\w\\s-]{3,35})\\)");

    // ── Entry point ───────────────────────────────────────────────────────────

    static void main(String[] args) throws Exception {
        String projectName = args.length > 0 ? args[0] : "lucene";

        System.out.printf("ConceptMiner — project: %s%n%n", projectName);

        IndexConfig config       = IndexConfig.load();
        ProjectRegistry registry = new ProjectRegistry(config);
        LuceneIndexer indexer    = new LuceneIndexer(config);

        registry.find(projectName).orElseThrow(() ->
                new IllegalArgumentException("Project not indexed: " + projectName));

        IndexReader reader = indexer.openMultiReader(List.of(projectName));
        try {
            new ConceptMiner().mine(reader);
        } finally {
            indexer.close();
        }
    }

    public ConceptMiner() {}

    // ── Diagnostic pipeline ───────────────────────────────────────────────────

    /**
     * Runs the combined mining strategy and prints a human-readable report to
     * stdout for inspection.  Intended for CLI use; not called during normal indexing.
     */
    public void mine(IndexReader reader) throws IOException {
        List<ClassDoc> docs = loadClassDocs(reader);
        System.out.printf("Loaded %d class documents%n%n", docs.size());

        Map<String, Set<String>> rules = mineAll(reader);

        // Invert rules for the per-class concept map view
        Map<String, List<String>> byClass = new TreeMap<>();
        for (Map.Entry<String, Set<String>> e : rules.entrySet())
            for (String cls : e.getValue())
                byClass.computeIfAbsent(cls, k -> new ArrayList<>()).add(e.getKey());

        printConceptMap(byClass);
        printSynonymCandidates(rules);
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
            String kind       = nvl(luceneDoc.get("kind"));
            String className  = luceneDoc.get(DocumentMapper.F_CLASS_NAME);
            String qualName   = luceneDoc.get(DocumentMapper.F_QUALIFIED_CLASS);
            String javadoc    = nvl(luceneDoc.get(DocumentMapper.F_JAVADOC));
            String body       = nvl(luceneDoc.get(DocumentMapper.F_BODY));

            // Combine javadoc + synthesized body as text source; javadoc is primary
            String text = javadoc.isBlank() ? body : javadoc + "\n" + body;
            if (className == null || className.isBlank()) continue;
            if ("document".equals(kind)) continue; // skip GenericFileParser docs (non-code files)

            docs.add(new ClassDoc(className, qualName, javadoc, text,
                    extractLinks(javadoc)));
        }
        // Sort by class name for stable output
        docs.sort(Comparator.comparing(d -> d.className));
        return docs;
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

    private void printConceptMap(Map<String, List<String>> byClass) {
        System.out.println(bar());
        System.out.println("  CONCEPT MAP — mined terms per class (combined strategy)");
        System.out.println(bar());
        System.out.printf("  %-40s  %s%n", "Class", "Mined terms");
        System.out.println("  " + "─".repeat(100));

        byClass.forEach((cls, terms) -> {
            if (terms.isEmpty()) return;
            List<String> sorted = terms.stream().sorted().toList();
            System.out.printf("  %-40s  %s%n",
                    truncate(cls, 38),
                    truncate(String.join(", ", sorted), 80));
        });

        System.out.println(bar());
        System.out.println();
    }

    private void printSynonymCandidates(Map<String, Set<String>> rules) {
        System.out.println(bar());
        System.out.println("  SYNONYM CANDIDATES — term → classes");
        System.out.println(bar());
        System.out.printf("  %-32s  %s%n", "Term", "Classes");
        System.out.println("  " + "─".repeat(100));

        new TreeMap<>(rules).forEach((term, classes) -> {
            String cls = classes.stream().sorted().collect(Collectors.joining(", "));
            System.out.printf("  %-32s  %s%n", term, truncate(cls, 80));
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
        final String       text;  // javadoc + synthesized body
        final List<String> links; // @link / @see targets

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

    // ── Combined mining ───────────────────────────────────────────────────────

    /**
     * Mines synonym candidates from two sources that proved effective on a
     * vocabulary-gap golden set. Other sources (method names, package paths,
     * @link cross-references, PPMI single tokens) only find terms already present
     * in the index — BM25 handles those without synonyms.
     *
     * <ol>
     *   <li><b>Class name decomposition</b> — camelCase tokens, adjacent bigrams,
     *       and adjacent trigrams (e.g. {@code skiplist} from
     *       {@code MultiLevelSkipListReader}, {@code weakand} from
     *       {@code WANDScorer}).
     *   <li><b>Javadoc prose bigrams (top-P% PPMI per class)</b> — adjacent word
     *       pairs where at least one token is a non-stopword of ≥ 3 chars, plus
     *       hyphen-merged compounds ({@code "KD-tree"} → {@code kdtree}).
     *       Scored by PPMI; the top {@value #DEFAULT_BIGRAM_PCT}% per class are
     *       kept.  Percentile-based so the threshold adapts to any corpus size.
     * </ol>
     */
    // Top-P% of positive-PPMI bigrams kept per class. Percentile is corpus-agnostic.
    private static final double DEFAULT_BIGRAM_PCT = 10.0;

    public Map<String, Set<String>> mineAll(IndexReader reader) throws IOException {
        return mineAll(reader, DEFAULT_BIGRAM_PCT);
    }

    /** Variant that exposes the bigram percentile for tuning. */
    public Map<String, Set<String>> mineAll(IndexReader reader, double bigramTopPct)
            throws IOException {
        List<ClassDoc> docs = loadClassDocs(reader);
        Map<String, Set<String>> result = new HashMap<>();

        // ── Source 1: class name tokens + bigrams + trigrams ──────────────────
        for (ClassDoc d : docs) {
            String[] toks = splitIdentifier(d.className).split("\\s+");
            for (String t : toks)
                if (normalize(t) != null) addRule(result, t, d.className);
            for (int i = 0; i < toks.length - 1; i++) {
                String bi = toks[i] + toks[i + 1];
                if (bi.length() >= 4) addRule(result, bi, d.className);
            }
            for (int i = 0; i < toks.length - 2; i++) {
                String tri = toks[i] + toks[i + 1] + toks[i + 2];
                if (tri.length() >= 6) addRule(result, tri, d.className);
            }
        }

        // ── Source 3: parenthetical acronym expansions ────────────────────────
        // Handles two patterns found in technical javadocs:
        //   "finite state machine (FST)" → trigram "finitestatemachine" → FST
        //   "WAND (Weak AND)"            → bigram  "weakand"            → WANDScorer
        for (ClassDoc d : docs) {
            String sent = firstSentenceOf(d.javadoc);
            if (sent.isBlank()) continue;
            Matcher m1 = PAT_EXPANSION_ABBREV.matcher(sent);
            while (m1.find()) emitExpansionNgrams(m1.group(1), d.className, result);
            Matcher m2 = PAT_ABBREV_EXPANSION.matcher(sent);
            while (m2.find()) emitExpansionNgrams(m2.group(2), d.className, result);
        }

        // ── Source 2: javadoc prose bigrams (top-P% PPMI per class) ──────────
        // Percentile mode: rank bigrams by PPMI within each class, keep top-P%.
        // This is corpus-agnostic — a class with 20 bigrams keeps 2 at P=10%,
        // a class with 200 keeps 20; the absolute PPMI scale doesn't matter.
        Map<String, Integer> bigramDf = new HashMap<>();
        for (ClassDoc d : docs)
            for (String b : new HashSet<>(hybridBigrams(d.text)))
                bigramDf.merge(b, 1, Integer::sum);
        int totalDocs = docs.size();

        for (ClassDoc d : docs) {
            List<String> bigrams = hybridBigrams(d.text);
            Map<String, Integer> tf = new HashMap<>();
            for (String b : bigrams) tf.merge(b, 1, Integer::sum);
            int docLen = bigrams.size();
            if (docLen == 0) continue;

            List<Map.Entry<String, Double>> scored = new ArrayList<>();
            for (Map.Entry<String, Integer> e : tf.entrySet()) {
                int df = bigramDf.getOrDefault(e.getKey(), 1);
                double ppmi = Math.log(((double) e.getValue() / docLen)
                                       / ((double) df / totalDocs));
                if (ppmi > 0) scored.add(Map.entry(e.getKey(), ppmi));
            }
            if (scored.isEmpty()) continue;
            scored.sort(Map.Entry.<String, Double>comparingByValue().reversed());
            int keep = Math.max(1, (int) Math.ceil(scored.size() * bigramTopPct / 100.0));
            scored.subList(0, Math.min(keep, scored.size()))
                  .forEach(e -> addRule(result, e.getKey(), d.className));
        }

        // Drop terms too long to be useful queries or starting with a digit
        result.entrySet().removeIf(e -> {
            String t = e.getKey();
            return t.length() > 20 || Character.isDigit(t.charAt(0));
        });

        return result;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void addRule(Map<String, Set<String>> map, String term, String className) {
        map.computeIfAbsent(term, k -> new HashSet<>()).add(className);
    }

    /** Splits phrase on spaces/hyphens and emits adjacent bigrams and trigrams. */
    private static void emitExpansionNgrams(String phrase, String className,
                                             Map<String, Set<String>> result) {
        String[] words = phrase.trim().toLowerCase().split("[\\s-]+");
        for (int i = 0; i < words.length - 1; i++) {
            if (words[i].length() < 2 || words[i + 1].length() < 2) continue;
            String bi = words[i] + words[i + 1];
            if (bi.length() >= 4) addRule(result, bi, className);
        }
        for (int i = 0; i < words.length - 2; i++) {
            if (words[i].length() < 2) continue;
            String tri = words[i] + words[i + 1] + words[i + 2];
            if (tri.length() >= 6) addRule(result, tri, className);
        }
    }

    /** Splits a camelCase or underscore-separated identifier into lowercase words. */
    private static String splitIdentifier(String name) {
        if (name == null || name.isBlank()) return nvl(name);
        String[] parts = name.split("_");
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            if (part.isEmpty()) continue;
            String split = part.replaceAll("([a-z])([A-Z])", "$1 $2")
                               .replaceAll("([A-Z]+)([A-Z][a-z])", "$1 $2");
            if (!sb.isEmpty()) sb.append(' ');
            sb.append(split.toLowerCase());
        }
        return sb.toString();
    }

    /** Returns the first sentence of a javadoc comment (up to 200 chars if no period found). */
    private static String firstSentenceOf(String javadoc) {
        if (javadoc == null || javadoc.isBlank()) return "";
        String clean = javadoc.replaceAll("(?m)^\\s*\\*\\s?", " ").trim();
        for (int i = 0; i < clean.length(); i++) {
            char c = clean.charAt(i);
            if ((c == '.' || c == '!' || c == '?') &&
                    (i + 1 >= clean.length() || Character.isWhitespace(clean.charAt(i + 1)))) {
                return clean.substring(0, i);
            }
        }
        return clean.substring(0, Math.min(clean.length(), 200));
    }

    /**
     * Hybrid bigrams: hyphen-merged compounds + adjacent pairs where at least
     * one token is a non-stopword of >= 3 chars.
     */
    private static List<String> hybridBigrams(String text) {
        List<String> bigrams = new ArrayList<>();
        Matcher hm = HYPHEN_COMPOUND.matcher(text);
        while (hm.find()) {
            String merged = (hm.group(1) + hm.group(2)).toLowerCase();
            if (merged.length() >= 4) bigrams.add(merged);
        }
        String[] tokens = text.toLowerCase().split("[^a-zA-Z0-9]+");
        for (int i = 0; i < tokens.length - 1; i++) {
            String t1 = tokens[i], t2 = tokens[i + 1];
            boolean t1ok = t1.length() >= 3 && !STOPWORDS.contains(t1);
            boolean t2ok = t2.length() >= 3 && !STOPWORDS.contains(t2);
            if ((t1ok || t2ok) && t1.length() >= 2 && t2.length() >= 2) {
                String bi = t1 + t2;
                if (bi.length() >= 4) bigrams.add(bi);
            }
        }
        return bigrams;
    }

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
        Set<String> existingLhs = loadExistingLhs(synonymFile);

        // Mine using all seven sources with default configuration
        Map<String, Set<String>> allRules = mineAll(reader);

        List<String> newRules = new ArrayList<>();
        String date = java.time.LocalDate.now().toString();
        // Track within-batch LHS to prevent duplicate terms in the same run
        Set<String> emitted = new HashSet<>(existingLhs);

        // Iterate in stable order for deterministic output
        for (Map.Entry<String, Set<String>> e : new TreeMap<>(allRules).entrySet()) {
            String term = e.getKey();
            if (term.length() < 4)      continue; // skip very short tokens
            if (emitted.contains(term)) continue; // already covered

            for (String cls : new TreeSet<>(e.getValue())) {
                String classTarget = cls.toLowerCase();
                if (term.equals(classTarget)) continue; // skip identity rules
                newRules.add(String.format("%-32s => %s  # auto:%s:%s",
                        term, classTarget, projectName, date));
            }
            emitted.add(term);
        }

        if (newRules.isEmpty()) return 0;

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%n# ── Auto-mined from '%s' on %s (%d rules) ─%n",
                projectName, date, newRules.size()));
        for (String rule : newRules) sb.append(rule).append('\n');

        Files.writeString(synonymFile, sb.toString(),
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND);

        return newRules.size();
    }

    /**
     * Removes all synonym rules auto-mined from {@code projectName} from {@code synonymFile}.
     * Strips both the section-header comment and every rule line tagged {@code # auto:<projectName>:}.
     *
     * @return number of lines removed
     */
    public static int removeProjectSynonyms(Path synonymFile, String projectName) throws IOException {
        if (!Files.exists(synonymFile)) return 0;
        List<String> lines = Files.readAllLines(synonymFile);
        String headerMarker  = "# ── Auto-mined from '" + projectName + "'";
        String ruleMarker    = "# auto:" + projectName + ":";
        List<String> kept = new ArrayList<>();
        int removed = 0;
        for (String line : lines) {
            if (line.contains(headerMarker) || line.contains(ruleMarker)) {
                removed++;
            } else {
                kept.add(line);
            }
        }
        if (removed == 0) return 0;
        // Collapse runs of blank lines left by removed sections (max one blank between blocks)
        List<String> compacted = new ArrayList<>();
        boolean lastBlank = false;
        for (String line : kept) {
            boolean isBlank = line.isBlank();
            if (isBlank && lastBlank) continue;
            compacted.add(line);
            lastBlank = isBlank;
        }
        Files.writeString(synonymFile, String.join(System.lineSeparator(), compacted));
        return removed;
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
