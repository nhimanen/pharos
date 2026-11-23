package com.pharos.embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.translate.TranslatorContext;
import com.pharos.config.Pooling;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** NDArray-level pooling tests on synthetic hidden states (no model download). */
class PoolingTranslatorTest {

    private NDManager manager;

    @BeforeEach
    void setUp() { manager = NDManager.newBaseManager(); }

    @AfterEach
    void tearDown() { manager.close(); }

    /** [2, 4, 3]: row 0 has 4 real tokens, row 1 has 2 real tokens + 2 padding. */
    private NDArray hidden() {
        float[] h = {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                2, 4, 6, 4, 8, 10, 100, 100, 100, 100, 100, 100
        };
        return manager.create(h, new ai.djl.ndarray.types.Shape(2, 4, 3));
    }

    private NDArray mask() {
        return manager.create(new long[][]{{1, 1, 1, 1}, {1, 1, 0, 0}});
    }

    private static void assertRows(float[][] actual, float[][] expected) {
        assertThat(actual).hasDimensions(expected.length, expected[0].length);
        for (int i = 0; i < expected.length; i++) {
            assertThat(actual[i]).containsExactly(expected[i]);
        }
    }

    @Test
    void meanPoolingMatchesToday() {
        float[][] out = EmbeddingPooling.pool(hidden(), mask(), Pooling.MEAN);
        assertRows(out, new float[][]{{5.5f, 6.5f, 7.5f}, {3f, 6f, 8f}});
    }

    @Test
    void clsPoolingTakesFirstToken() {
        float[][] out = EmbeddingPooling.pool(hidden(), mask(), Pooling.CLS);
        assertRows(out, new float[][]{{1, 2, 3}, {2, 4, 6}});
    }

    @Test
    void twoDimensionalOutputWithClsFailsClearly() {
        NDArray pooled = manager.create(new float[][]{{1, 2, 3}, {4, 5, 6}});
        assertThatThrownBy(() -> EmbeddingPooling.pool(pooled, mask(), Pooling.CLS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pooling")
                .hasMessageContaining("mean");
    }

    @Test
    void translatorsWithTwoDimensionalOutputAndCls_failClearly() {
        NDArray pooled = manager.create(new float[][]{{1, 2, 3}, {4, 5, 6}});
        NDList out = new NDList(pooled);
        assertThatThrownBy(() -> new BatchSentenceTransformerTranslator(null, 8, Pooling.CLS)
                .processOutput(ctx(mask(), 2), out))
                .hasMessageContaining("pooling").hasMessageContaining("mean");
        NDArray m1 = manager.create(new long[][]{{1, 1}});
        NDList out1 = new NDList(manager.create(new float[][]{{1, 2, 3}}));
        assertThatThrownBy(() -> new SentenceTransformerTranslator(null, 8, Pooling.CLS)
                .processOutput(ctx(m1), out1))
                .hasMessageContaining("pooling").hasMessageContaining("mean");
    }

    @Test
    void batchAndSingleAgree() throws Exception {
        for (Pooling p : Pooling.values()) {
            float[][] batch = EmbeddingPooling.pool(hidden(), mask(), p);

            // Single translator, row 1 only (no padding: seq_len 2).
            NDArray h1 = hidden().get("1:2, :2, :");
            NDArray m1 = manager.create(new long[][]{{1, 1}});
            SentenceTransformerTranslator single = new SentenceTransformerTranslator(null, 8, p);
            float[] one = single.processOutput(ctx(m1), new NDList(h1));
            assertThat(one).containsExactly(batch[1]);

            BatchSentenceTransformerTranslator bt = new BatchSentenceTransformerTranslator(null, 8, p);
            float[][] viaTranslator = bt.processOutput(ctx(mask(), 2), new NDList(hidden()));
            assertRows(viaTranslator, batch);
        }
    }

    private TranslatorContext ctx(NDArray mask) {
        return ctx(mask, -1);
    }

    private TranslatorContext ctx(NDArray mask, int batchSize) {
        Map<String, Object> attach = new HashMap<>();
        attach.put("attentionMask", mask);
        attach.put("batchAttentionMask", mask);
        attach.put("batchSize", batchSize);
        return (TranslatorContext) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{TranslatorContext.class},
                (proxy, m, args) -> switch (m.getName()) {
                    case "getAttachment" -> attach.get((String) args[0]);
                    case "getNDManager" -> manager;
                    default -> throw new UnsupportedOperationException(m.getName());
                });
    }
}
