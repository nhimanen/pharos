package com.pharos.embedding;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.translate.Batchifier;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;
import ai.djl.util.PairList;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Batch translator for cross-encoder ONNX models.
 *
 * <p>Encodes all (query, document) pairs in a single native call via
 * {@code batchEncode(PairList)}, pads to the longest sequence in the batch,
 * and runs a single ONNX forward pass producing scores for all N pairs at once.
 *
 * <p>Input:  {@code List<String[]>} where each element is {@code {query, document}}.
 * <p>Output: {@code float[N]} relevance scores in [0, 1].
 */
class CrossEncoderBatchTranslator implements Translator<List<String[]>, float[]> {

    private final Path    tokenizerJson;
    private final int     maxLength;
    private final boolean includeTokenTypeIds;
    private HuggingFaceTokenizer tokenizer;

    CrossEncoderBatchTranslator(Path tokenizerJson, int maxLength, boolean includeTokenTypeIds) {
        this.tokenizerJson       = tokenizerJson;
        this.maxLength           = maxLength;
        this.includeTokenTypeIds = includeTokenTypeIds;
    }

    @Override
    public void prepare(TranslatorContext ctx) throws IOException {
        tokenizer = HuggingFaceTokenizer.newInstance(
                tokenizerJson,
                Map.of(
                        "maxLength",      String.valueOf(maxLength),
                        "modelMaxLength", String.valueOf(maxLength),
                        "padding",        "true",    // pad to longest in batch
                        "truncation",     "true"
                ));
    }

    @Override
    public NDList processInput(TranslatorContext ctx, List<String[]> pairs) {
        int n = pairs.size();
        PairList<String, String> pairList = new PairList<>(n);
        for (String[] pair : pairs) {
            pairList.add(pair[0], pair[1]);
        }

        Encoding[] encodings = tokenizer.batchEncode(pairList);
        int seqLen = encodings[0].getIds().length; // all same length after padding

        long[] allInputIds = new long[n * seqLen];
        long[] allAttnMask = new long[n * seqLen];
        long[] allTypeIds  = new long[n * seqLen];

        for (int i = 0; i < n; i++) {
            long[] ids   = encodings[i].getIds();
            long[] mask  = encodings[i].getAttentionMask();
            long[] types = encodings[i].getTypeIds();
            System.arraycopy(ids,   0, allInputIds, i * seqLen, ids.length);
            System.arraycopy(mask,  0, allAttnMask, i * seqLen, mask.length);
            System.arraycopy(types, 0, allTypeIds,  i * seqLen, types.length);
        }

        NDManager mgr = ctx.getNDManager();
        NDArray inputIds = mgr.create(allInputIds).reshape(n, seqLen);
        NDArray attnMask = mgr.create(allAttnMask).reshape(n, seqLen);

        if (includeTokenTypeIds) {
            NDArray tokenTypes = mgr.create(allTypeIds).reshape(n, seqLen);
            return new NDList(inputIds, attnMask, tokenTypes);
        }
        return new NDList(inputIds, attnMask);
    }

    /** Extracts per-row scores from logits of shape {@code [N, num_labels]}. */
    @Override
    public float[] processOutput(TranslatorContext ctx, NDList list) {
        NDArray logits = list.get(0); // [N, num_labels]
        long[] shape = logits.getShape().getShape();
        int n         = (int) shape[0];
        int numLabels = (int) shape[1];
        float[] flat  = logits.toFloatArray(); // length = N * numLabels

        float[] scores = new float[n];
        for (int i = 0; i < n; i++) {
            if (numLabels == 1) {
                scores[i] = CrossEncoderTranslator.sigmoid(flat[i]);
            } else {
                float expSum = 0f;
                for (int j = 0; j < numLabels; j++) {
                    expSum += (float) Math.exp(flat[i * numLabels + j]);
                }
                scores[i] = (float) Math.exp(flat[i * numLabels + numLabels - 1]) / expSum;
            }
        }
        return scores;
    }

    @Override
    public Batchifier getBatchifier() {
        return null; // we handle batching ourselves
    }
}
