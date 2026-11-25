package com.pharos.indexer;

import com.pharos.config.EmbeddingProviderConfig;
import com.pharos.config.Pooling;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Default-config fingerprints must never change (that would invalidate every cache). */
class FingerprintCompatTest {

    private static EmbeddingProviderConfig jina() {
        EmbeddingProviderConfig c = new EmbeddingProviderConfig();
        c.setType("djl");
        c.setModelId("jina-code-v2");
        c.setUrl("hf://jinaai/jina-embeddings-v2-base-code");
        c.setDimensions(768);
        return c;
    }

    @Test
    void defaultConfigFingerprintUnchanged() {
        // Pinned literal: modelId:dims:url:model — as written by pharos before pooling/prefixes.
        assertThat(IndexVersions.modelFingerprint(jina()))
                .isEqualTo("jina-code-v2:768:hf://jinaai/jina-embeddings-v2-base-code:");
    }

    @Test
    void poolingAndEachPrefixChangeTheFingerprintDifferently() {
        String base = IndexVersions.modelFingerprint(jina());

        EmbeddingProviderConfig cls = jina();
        cls.setPooling(Pooling.CLS);
        EmbeddingProviderConfig qp = jina();
        qp.setQueryPrefix("query: ");
        EmbeddingProviderConfig dp = jina();
        dp.setDocumentPrefix("query: ");   // same text on the other side must differ from qp

        String fCls = IndexVersions.modelFingerprint(cls);
        String fQp = IndexVersions.modelFingerprint(qp);
        String fDp = IndexVersions.modelFingerprint(dp);

        assertThat(fCls).startsWith(base).isNotEqualTo(base);
        assertThat(fQp).startsWith(base).isNotEqualTo(base);
        assertThat(fDp).startsWith(base).isNotEqualTo(base);
        assertThat(java.util.Set.of(fCls, fQp, fDp)).hasSize(3);
    }

    @Test
    void explicitMeanAndEmptyPrefixesKeepTheLegacyFingerprint() {
        EmbeddingProviderConfig c = jina();
        c.setPooling(Pooling.MEAN);
        c.setQueryPrefix("");
        c.setDocumentPrefix(null);
        assertThat(IndexVersions.modelFingerprint(c)).isEqualTo(IndexVersions.modelFingerprint(jina()));
    }
}
