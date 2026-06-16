package com.pharos.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.ConcatenateGraphFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * FST-backed phrase tagger.
 *
 * <p>Compiles a phrase dictionary (loaded from a classpath CSV) into a Lucene
 * {@link FST} at construction time, then tags text by
 * walking FST arcs over the analyzed token stream.
 *
 * <p>CSV format: {@code phrase,intent,doctype} where {@code doctype} may be empty.
 * Lines starting with {@code //} are treated as comments.
 *
 * <p>Pipeline:
 * <ol>
 *   <li>Parse CSV rows, analyze each phrase into an FST key.</li>
 *   <li>Compile sorted (key → intent\tdocType) pairs into an FST via {@link FSTCompiler}.</li>
 *   <li>Tag text with forward-maximum-match arc walking.</li>
 * </ol>
 */
class IntentTagger {

    private static final Logger log = LoggerFactory.getLogger(IntentTagger.class);

    /** Separator byte between tokens in FST keys — matches ConcatenateGraphFilter.SEP_LABEL. */
    private static final int SEP = ConcatenateGraphFilter.SEP_LABEL;

    private final FST<BytesRef> fst;
    private final Analyzer analyzer;

    private IntentTagger(FST<BytesRef> fst, Analyzer analyzer) {
        this.fst = fst;
        this.analyzer = analyzer;
    }

    // ── Factory ───────────────────────────────────────────────────────────────

    /**
     * Load a code-search intent dictionary from the classpath and compile it into an FST.
     *
     * @param csvResource classpath path, e.g. {@code "/intent-code-search.csv"}
     */
    static IntentTagger fromCsv(String csvResource) throws IOException {
        Analyzer analyzer = buildAnalyzer();
        TreeMap<BytesRef, String> sorted = new TreeMap<>();
        int loaded = 0;

        try (InputStream is = IntentTagger.class.getResourceAsStream(csvResource)) {
            if (is == null) throw new IOException("Intent CSV not found on classpath: " + csvResource);
            BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String line;
            boolean header = true;
            while ((line = br.readLine()) != null) {
                line = line.strip();
                if (line.isBlank() || line.startsWith("//")) continue;
                if (header) { header = false; continue; }  // skip CSV header row
                String[] cols = line.split(",", -1);
                if (cols.length < 2 || cols[0].isBlank()) continue;
                String phrase = cols[0].strip();
                String intent = cols[1].strip();
                String docType = cols.length > 2 ? cols[2].strip() : "";

                BytesRef key = fstKey(phrase, analyzer);
                if (key == null || key.length == 0) continue;

                if (sorted.putIfAbsent(key, intent + "\t" + docType) == null) {
                    loaded++;
                }
            }
        }
        log.debug("loaded {} phrases from {}", loaded, csvResource);
        return compileFst(sorted, analyzer);
    }

    private static IntentTagger compileFst(TreeMap<BytesRef, String> sorted, Analyzer analyzer)
            throws IOException {
        ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
        FSTCompiler<BytesRef> compiler =
                new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs).build();
        IntsRefBuilder scratch = new IntsRefBuilder();

        for (var entry : sorted.entrySet()) {
            BytesRef key = entry.getKey();
            BytesRef value = new BytesRef(entry.getValue().getBytes(StandardCharsets.UTF_8));
            scratch.clear();
            for (int i = 0; i < key.length; i++) scratch.append(key.bytes[key.offset + i] & 0xFF);
            compiler.add(scratch.get(), value);
        }

        FST.FSTMetadata<BytesRef> meta = compiler.compile();
        FST<BytesRef> fst = meta == null ? null : FST.fromFSTReader(meta, compiler.getFSTReader());
        if (fst == null) {
            log.warn("FST compiled to null — empty dictionary?");
            return new IntentTagger(null, analyzer);
        }
        log.debug("FST compiled ({} bytes RAM)", fst.ramBytesUsed());
        return new IntentTagger(fst, analyzer);
    }

    // ── Tagging ───────────────────────────────────────────────────────────────

    /**
     * Returns the first (longest-match) intent found in {@code text} as {@code [intent, docType]},
     * or {@code null} if no phrase matches. docType may be null.
     */
    String[] tag(String text) throws IOException {
        if (fst == null || text == null || text.isBlank()) return null;

        List<String> tokens = new ArrayList<>();

        try (TokenStream stream = analyzer.tokenStream("phrase", new StringReader(text))) {
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                tokens.add(term.toString());
            }
            stream.end();
        }

        List<byte[]> tokenBytes = new ArrayList<>();
        for (String t : tokens) tokenBytes.add(t.getBytes(StandardCharsets.UTF_8));

        FST.Arc<BytesRef> arc = new FST.Arc<>();
        FST.BytesReader fstReader = fst.getBytesReader();

        for (int i = 0; i < tokens.size(); i++) {
            fst.getFirstArc(arc);
            BytesRef accumulated = fst.outputs.getNoOutput();
            int matchEndJ = -1;
            BytesRef matchOut = null;

            outer:
            for (int j = i; j < tokens.size(); j++) {
                if (j > i) {
                    if (fst.findTargetArc(SEP, arc, arc, fstReader) == null) break;
                    accumulated = fst.outputs.add(accumulated, arc.output());
                }
                for (byte b : tokenBytes.get(j)) {
                    if (fst.findTargetArc(b & 0xFF, arc, arc, fstReader) == null) break outer;
                    accumulated = fst.outputs.add(accumulated, arc.output());
                }
                if (arc.isFinal()) {
                    matchEndJ = j;
                    matchOut = fst.outputs.add(accumulated, arc.nextFinalOutput());
                }
            }

            if (matchEndJ >= 0) {
                String combined = new String(
                        matchOut.bytes, matchOut.offset, matchOut.length, StandardCharsets.UTF_8);
                String[] fields = combined.split("\t", 2);
                String intent = fields[0];
                String docType = fields.length > 1 && !fields[1].isBlank() ? fields[1] : null;
                return new String[]{ intent, docType };
            }
        }
        return null;
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private static BytesRef fstKey(String phrase, Analyzer analyzer) throws IOException {
        TokenStream base = analyzer.tokenStream("phrase", new StringReader(phrase));
        try (ConcatenateGraphFilter concat = new ConcatenateGraphFilter(base)) {
            CharTermAttribute charAttr = concat.addAttribute(CharTermAttribute.class);
            concat.reset();
            BytesRef result = null;
            if (concat.incrementToken()) {
                result = new BytesRef(charAttr.toString().getBytes(StandardCharsets.UTF_8));
            }
            concat.end();
            return result;
        }
    }

    private static Analyzer buildAnalyzer() {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String field) {
                Tokenizer t = new StandardTokenizer();
                TokenStream s = new LowerCaseFilter(t);
                s = new ASCIIFoldingFilter(s);
                return new TokenStreamComponents(t, s);
            }
        };
    }
}
