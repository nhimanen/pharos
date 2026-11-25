package com.pharos.search.distributed;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.search.FstQueryClassifier;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import com.pharos.search.VectorSearchStrategy;
import com.pharos.search.distributed.ParityCorpus.HashEmbeddings;
import com.pharos.search.pipeline.KeywordRetrievalStage;
import com.pharos.search.pipeline.NoOpCrossEncoder;
import com.pharos.search.pipeline.RetrievalStage;
import com.pharos.search.pipeline.UnifiedRetrievalStage;
import com.pharos.search.pipeline.VectorRetrievalStage;
import com.pharos.indexer.LuceneIndexer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.util.Constants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import com.sun.management.HotSpotDiagnosticMXBean;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static com.pharos.search.distributed.ParityCorpus.FIXTURE;
import static com.pharos.search.distributed.ParityCorpus.JAVA;
import static com.pharos.search.distributed.ParityCorpus.PHAROS;
import static com.pharos.search.distributed.ParityCorpus.PY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The pipeline split (raw candidates + {@link CandidateShaper}) must not move a single local result:
 * ids, exact order and float score bits are compared against outputs recorded by the pre-split code
 * (8629976), for the eval set's pharos queries (over a frozen pharos source snapshot) and the
 * sample-app queries, on every pipeline type. See {@link ParityCorpus} for the corpus and how the
 * expected outputs are recorded.
 *
 * <p>Hermetic: the engine reads {@code synonyms.txt} and {@code pipelines.yaml} from a private empty
 * directory, never from the shared {@code pharos.home} other tests write to.
 *
 * <p>Float arithmetic: the expected vector scores were computed with fused multiply-add. Lucene
 * chooses fused or unfused FMA per CPU ({@code Constants.HAS_FAST_SCALAR_FMA}: unfused on AMD without
 * AVX-512), which moves cosine scores by an ulp, so surefire pins {@code -Dlucene.useScalarFMA=true}
 * (see the pom). The 16-dim test vectors stay on Lucene's scalar loop at 256- and 512-bit vector
 * widths; narrower hardware takes a vectorized loop and cannot reproduce the bits.
 *
 * <p>Regenerate (only when ranking is meant to change):
 * {@code mvn test -Dtest=PipelineSplitParityTest -Dpharos.regenerateParity=true}.
 */
class PipelineSplitParityTest {

    private static Path tmp;
    private static Path configBase;
    private static LuceneIndexer lucene;
    private static SearchEngine engine;

    @BeforeAll
    static void openFrozenIndexes() throws Exception {
        tmp = Files.createTempDirectory("pipeline-split-parity");
        configBase = Files.createDirectories(tmp.resolve("config")); // no synonyms, built-in pipelines
        if (Boolean.getBoolean("pharos.regenerateParity")) ParityCorpus.buildIndexZip(tmp.resolve("build"));

        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("run/indexes"));
        lucene = new LuceneIndexer(config);
        ProjectRegistry registry = ParityCorpus.openFrozen(tmp.resolve("run"), lucene);
        engine = new SearchEngine(lucene, new HashEmbeddings(), registry, new NoOpCrossEncoder(),
                new FstQueryClassifier(), configBase);
    }

    @AfterAll
    static void cleanup() throws IOException {
        if (lucene != null) lucene.close();
        if (tmp != null) {
            try (Stream<Path> s = Files.walk(tmp)) {
                s.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    @Test
    @DisabledIfSystemProperty(named = "pharos.regenerateParity", matches = "true")
    void localResultsBitwiseIdentical() throws Exception {
        if (!Files.exists(FIXTURE)) fail("parity fixture missing — record it with -Dpharos.regenerateParity=true");
        assumeRecordedArithmetic();
        String actual = ParityCorpus.snapshot(engine);
        String expected = Files.readString(FIXTURE);
        if (!actual.equals(expected)) {
            Path out = Path.of("target/pipeline-split-parity.actual.json");
            Files.writeString(out, actual);
            fail("local results moved — diff " + FIXTURE + " " + out);
        }
        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Skips where the recorded float bits cannot be reproduced (no FMA, vectors narrower than 256 bits,
     * or a C1-only VM, where Lucene never fuses); fails when only the configuration is missing (Lucene chose unfused FMA on a CPU that has it).
     */
    static void assumeRecordedArithmetic() {
        HotSpotDiagnosticMXBean hotspot = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
        boolean fma = Boolean.parseBoolean(hotspot.getVMOption("UseFMA").getValue());
        int maxVectorBytes = Integer.parseInt(hotspot.getVMOption("MaxVectorSize").getValue());
        assumeTrue(fma && maxVectorBytes >= 32 && !Constants.IS_CLIENT_VM,
                "parity fixture needs FMA, >= 256-bit vectors and C2 (UseFMA=" + fma + ", MaxVectorSize="
                        + maxVectorBytes + ", clientVM=" + Constants.IS_CLIENT_VM + ")");
        if (!Constants.HAS_FAST_SCALAR_FMA) {
            fail("Lucene uses unfused multiply-add on this CPU, so vector scores differ from the fixture by an ulp:"
                    + " run with -Dlucene.useScalarFMA=true (the surefire argLine sets it)");
        }
    }

    /** The stage-level raw API a shard serves, shaped by the coordinator, equals the stage's own results. */
    @Test
    void rawRetrievalPlusShapingEqualsStageRetrieval() throws Exception {
        HashEmbeddings embedder = new HashEmbeddings();
        KeywordSearchStrategy kw = new KeywordSearchStrategy(configBase.resolve("synonyms.txt"), new SearcherFactory());
        KeywordRetrievalStage keywordStage = new KeywordRetrievalStage(kw);
        VectorRetrievalStage vectorStage = new VectorRetrievalStage(new VectorSearchStrategy(embedder));
        UnifiedRetrievalStage unifiedStage = new UnifiedRetrievalStage(kw, embedder);
        FstQueryClassifier router = new FstQueryClassifier();
        IndexReader reader = lucene.openMultiReader(List.of(JAVA, PY, PHAROS));

        for (String q : List.of("BordaMerger", "interface for embedding generation",
                "find code that skips re-indexing files that have not changed", "greeting service")) {
            SearchRequest req = new SearchRequest(q, SearchRequest.SearchType.UNIFIED, null, null, 10,
                    "text", null, null, 0).withClassification(router.classify(q));
            SearchTrace trace = new SearchTrace();

            assertSame(keywordStage.retrieve(reader, req, trace), CandidateShaper.shape(
                    keywordStage.retrieveRaw(new IndexSearcher(reader), req, trace),
                    KeywordSearchStrategy.TYPE, KeywordSearchStrategy.shapingIntent(req)), q);
            assertSame(vectorStage.retrieve(reader, req, trace), CandidateShaper.shape(
                    vectorStage.retrieveRaw(new IndexSearcher(reader), req, trace),
                    VectorSearchStrategy.TYPE, null), q);
            UnifiedRetrievalStage.RawRetrieval shard =
                    unifiedStage.retrieveRaw(new IndexSearcher(reader), req, embedder.embed(q), null);
            assertSame(unifiedStage.retrieve(reader, req, trace),
                    CandidateShaper.shape(shard.candidates(), UnifiedRetrievalStage.TYPE, shard.shapingIntent()), q);
        }

        RetrievalStage plain = (r, q, t) -> List.of();
        assertThatThrownBy(() -> plain.retrieveRaw(new IndexSearcher(reader), null, null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** Raw retrieval sets the similarity it scores with: a searcher's earlier similarity must not leak in. */
    @Test
    void rawRetrievalIgnoresTheSearchersPriorSimilarity() throws Exception {
        HashEmbeddings embedder = new HashEmbeddings();
        KeywordSearchStrategy kw = new KeywordSearchStrategy(configBase.resolve("synonyms.txt"), new SearcherFactory());
        UnifiedRetrievalStage unified = new UnifiedRetrievalStage(kw, embedder);
        IndexReader reader = lucene.openMultiReader(List.of(JAVA, PY, PHAROS));

        for (String q : List.of("BordaMerger", "configure parse thread count")) {
            SearchRequest req = new SearchRequest(q, SearchRequest.SearchType.UNIFIED, null, null, 10,
                    "text", null, null, 0);
            IndexSearcher used = new IndexSearcher(reader);
            used.setSimilarity(new ClassicSimilarity());

            assertThat(scores(kw.retrieveRaw(used, req))).as("keyword " + q).isNotEmpty()
                    .isEqualTo(scores(kw.retrieveRaw(new IndexSearcher(reader), req)));
            // BM25-only (no query vector): the raw scores are the recall pass's BM25 scores themselves
            used.setSimilarity(new ClassicSimilarity());
            assertThat(scores(unified.retrieveRaw(used, req, null, null).candidates()))
                    .as("unified bm25 " + q).isNotEmpty()
                    .isEqualTo(scores(unified.retrieveRaw(new IndexSearcher(reader), req, null, null).candidates()));
            used.setSimilarity(new ClassicSimilarity());
            assertThat(scores(unified.retrieveRaw(used, req, embedder.embed(q), null).candidates()))
                    .as("unified " + q).isNotEmpty()
                    .isEqualTo(scores(unified.retrieveRaw(new IndexSearcher(reader), req, embedder.embed(q), null)
                            .candidates()));
        }
    }

    private static List<String> scores(List<RawCandidate> raw) {
        return raw.stream().map(c -> c.result().id() + " " + Float.floatToRawIntBits(c.rawScore())).toList();
    }

    private static void assertSame(List<SearchResult> expected, List<SearchResult> actual, String q) {
        assertThat(actual).as(q).isNotEmpty();
        assertThat(actual.stream().map(r -> r.id() + " " + Float.floatToRawIntBits(r.score()) + " " + r.searchType())
                .toList()).as(q)
                .isEqualTo(expected.stream()
                        .map(r -> r.id() + " " + Float.floatToRawIntBits(r.score()) + " " + r.searchType()).toList());
    }

    @Test
    @EnabledIfSystemProperty(named = "pharos.regenerateParity", matches = "true")
    void regenerate() throws Exception {
        Files.createDirectories(FIXTURE.getParent());
        Files.writeString(FIXTURE, ParityCorpus.snapshot(engine));
    }
}
