package com.pharos.embedding;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.translate.Batchifier;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

/**
 * Translator for cross-encoder ONNX models (e.g. ms-marco-MiniLM-L-6-v2).
 *
 * <p>A cross-encoder takes a (query, document) pair as a single concatenated
 * sequence: {@code [CLS] query [SEP] document [SEP]}. The model produces a
 * single logit per pair; applying sigmoid normalises it to [0, 1].
 *
 * <h3>Input tensors</h3>
 * <ul>
 *   <li>{@code input_ids}      — token ids for the concatenated pair
 *   <li>{@code attention_mask} — 1 for real tokens, 0 for padding
 *   <li>{@code token_type_ids} — 0 for query tokens, 1 for document tokens
 * </ul>
 *
 * <h3>Output tensor</h3>
 * {@code logits} — shape {@code [1, 1]}; higher = more relevant.
 * {@link #sigmoid} normalises to [0, 1].
 */
public class CrossEncoderTranslator implements Translator<String[], Float> {

    private final Path    tokenizerJson;
    private final int     maxLength;
    private final boolean includeTokenTypeIds;
    private HuggingFaceTokenizer tokenizer;

    /**
     * @param tokenizerJson      local path to tokenizer.json
     * @param maxLength          maximum sequence length (query + document combined)
     * @param includeTokenTypeIds true for BERT-based models (ms-marco, BGE);
     *                            false for RoBERTa-based models (stsb-roberta)
     */
    public CrossEncoderTranslator(Path tokenizerJson, int maxLength,
                                   boolean includeTokenTypeIds) {
        this.tokenizerJson      = tokenizerJson;
        this.maxLength          = maxLength;
        this.includeTokenTypeIds = includeTokenTypeIds;
    }

    /** Defaults to including token_type_ids (BERT-style). */
    public CrossEncoderTranslator(Path tokenizerJson, int maxLength) {
        this(tokenizerJson, maxLength, true);
    }

    @Override
    public void prepare(TranslatorContext ctx) throws IOException {
        tokenizer = HuggingFaceTokenizer.newInstance(
                tokenizerJson,
                Map.of(
                        "maxLength",      String.valueOf(maxLength),
                        "modelMaxLength", String.valueOf(maxLength),
                        "padding",        "false",
                        "truncation",     "true"
                ));
    }

    /** {@code pair[0]} = query, {@code pair[1]} = document. */
    @Override
    public NDList processInput(TranslatorContext ctx, String[] pair) {
        Encoding enc = tokenizer.encode(pair[0], pair[1]);
        NDManager mgr = ctx.getNDManager();

        NDArray inputIds = mgr.create(enc.getIds())           .reshape(1, -1);
        NDArray attnMask = mgr.create(enc.getAttentionMask()) .reshape(1, -1);

        if (includeTokenTypeIds) {
            NDArray tokenTypes = mgr.create(enc.getTypeIds()).reshape(1, -1);
            return new NDList(inputIds, attnMask, tokenTypes);
        }
        return new NDList(inputIds, attnMask);
    }

    /** Extracts the relevance score from the logits tensor. */
    @Override
    public Float processOutput(TranslatorContext ctx, NDList list) {
        NDArray logits = list.get(0); // shape: [1, num_labels]
        float[] values = logits.squeeze().toFloatArray();
        // num_labels=1: single logit → sigmoid for probability
        // num_labels=2: [neg_logit, pos_logit] → softmax, take positive class
        if (values.length == 1) {
            return sigmoid(values[0]);
        } else {
            // softmax over [neg, pos], return positive class probability
            float expPos = (float) Math.exp(values[values.length - 1]);
            float expSum = 0;
            for (float v : values) expSum += (float) Math.exp(v);
            return expPos / expSum;
        }
    }

    @Override
    public Batchifier getBatchifier() { return null; }

    static float sigmoid(float x) {
        return (float) (1.0 / (1.0 + Math.exp(-x)));
    }
}
