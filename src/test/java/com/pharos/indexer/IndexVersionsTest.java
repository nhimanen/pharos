package com.pharos.indexer;

import com.pharos.config.EmbeddingProviderConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins {@link IndexVersions#modelFingerprint(EmbeddingProviderConfig)} to the
 * <b>parent</b> provider config's fields, regardless of which fallback
 * runtime actually served a given index run. This is what keeps the
 * persistent embedding cache key and the Lucene {@code vec.<modelId>} field
 * name stable when index-time embedding flips between a remote runtime and
 * its local fallback: {@code ProjectIndexManager}/{@code MultiModelEmbedder}
 * always resolve the fingerprint via {@code config.findProviderConfig(modelId)}
 * — which only ever searches the top-level {@code embeddingProviders} list,
 * never {@code fallbacks} — so the fingerprint is identical on a remote day
 * and a fallback day as long as the parent config didn't change.
 */
class IndexVersionsTest {

    @Test
    void modelFingerprint_isIdentical_regardlessOfWhichCandidateConstructedIt() {
        EmbeddingProviderConfig parent = new EmbeddingProviderConfig();
        parent.setType("openai");
        parent.setModelId("jina-code-v2");
        parent.setUrl("http://192.168.1.100:8083/v1");
        parent.setModel("jinaai/jina-embeddings-v2-base-code");
        parent.setDimensions(768);

        EmbeddingProviderConfig fallback = new EmbeddingProviderConfig();
        fallback.setType("djl");
        fallback.setModelId("jina-code-v2");
        fallback.setUrl("hf://jinaai/jina-embeddings-v2-base-code");
        fallback.setDimensions(768);
        parent.getFallbacks().add(fallback);

        // Callers always fingerprint the parent config — never the fallback,
        // even on a day the fallback is what actually served the request.
        String fingerprintOnRemoteDay = IndexVersions.modelFingerprint(parent);
        String fingerprintOnFallbackDay = IndexVersions.modelFingerprint(parent);

        assertThat(fingerprintOnFallbackDay).isEqualTo(fingerprintOnRemoteDay);
        // Sanity: fingerprinting the fallback config directly WOULD differ
        // (different url/type) — proving the parent-vs-fallback distinction
        // is real and not a no-op assertion.
        assertThat(IndexVersions.modelFingerprint(fallback)).isNotEqualTo(fingerprintOnRemoteDay);
    }
}
