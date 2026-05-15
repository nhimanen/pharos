package com.pharos.analysis;

import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads {@code synonyms.txt} from the Pharos config directory and builds a
 * Lucene {@link SynonymMap} for query-time synonym expansion.
 *
 * <h3>File format</h3>
 * One rule per line, blank lines and {@code #} comments ignored.
 * Two rule types:
 * <pre>
 *   # Directed (one-way): left-hand phrases expand to right-hand terms at query time
 *   near real time => nrt, directoryreader
 *   jump ahead postings => multilevelskiplistreader
 *
 *   # Undirected (equivalent, comma-separated): any token matches all others
 *   bm25, okapi bm25
 * </pre>
 *
 * <p>All tokens are lowercased before insertion.  Multi-word phrases on the
 * left-hand side are collapsed to a single space-separated string, which
 * {@link SynonymMap.Builder} treats as a multi-word synonym input.
 *
 * <p>The built map is applied <b>at query time only</b> (in
 * {@link com.pharos.indexer.LuceneIndexer#buildQueryAnalyzer}), so no
 * re-indexing is required when the synonym file is updated.
 */
public class SynonymProvider {

    private static final Logger log = LoggerFactory.getLogger(SynonymProvider.class);

    private final Path synonymFile;

    public SynonymProvider(Path synonymFile) {
        this.synonymFile = synonymFile;
    }

    /**
     * Returns {@code true} if the synonym file exists and is non-empty.
     */
    public boolean isAvailable() {
        try {
            return Files.exists(synonymFile) && Files.size(synonymFile) > 0;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Parses the synonym file and builds a {@link SynonymMap}.
     *
     * @param dedup if true, deduplicates synonym pairs (prevents index corruption
     *              if the same rule is written twice by the auto-expander)
     */
    public SynonymMap load(boolean dedup) throws IOException, ParseException {
        SynonymMap.Builder builder = new SynonymMap.Builder(dedup);
        int ruleCount = 0;

        for (String raw : Files.readAllLines(synonymFile)) {
            String line = raw.strip();
            if (line.isEmpty() || line.startsWith("#")) continue;
            // Strip inline comments (e.g. "term => class  # auto:project:date")
            int commentIdx = line.indexOf('#');
            if (commentIdx >= 0) line = line.substring(0, commentIdx).strip();
            if (line.isEmpty()) continue;

            if (line.contains("=>")) {
                // Directed: "phrase one, phrase two => target1, target2"
                String[] parts = line.split("=>", 2);
                List<String> inputs  = splitPhrases(parts[0]);
                List<String> outputs = splitPhrases(parts[1]);
                if (inputs.isEmpty() || outputs.isEmpty()) continue;

                for (String input : inputs) {
                    for (String output : outputs) {
                        builder.add(toCharsRef(input), toCharsRef(output), true);
                        ruleCount++;
                    }
                }
            } else if (line.contains(",")) {
                // Undirected: "term a, term b, term c"
                List<String> terms = splitPhrases(line);
                for (int i = 0; i < terms.size(); i++) {
                    for (int j = 0; j < terms.size(); j++) {
                        if (i != j) {
                            builder.add(toCharsRef(terms.get(i)), toCharsRef(terms.get(j)), true);
                            ruleCount++;
                        }
                    }
                }
            }
        }

        log.info("Loaded {} synonym rules from {}", ruleCount, synonymFile);
        return builder.build();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static List<String> splitPhrases(String s) {
        List<String> result = new ArrayList<>();
        for (String part : s.split(",")) {
            String p = part.strip().toLowerCase();
            if (!p.isEmpty()) result.add(p);
        }
        return result;
    }

    /**
     * Converts a (possibly multi-word) phrase to a {@link CharsRef} suitable
     * for {@link SynonymMap.Builder#add}.
     * Multi-word phrases are joined with the SynonymMap word separator (0x0).
     */
    private static CharsRef toCharsRef(String phrase) {
        String[] words = phrase.split("\\s+");
        return SynonymMap.Builder.join(words, new CharsRefBuilder());
    }
}
