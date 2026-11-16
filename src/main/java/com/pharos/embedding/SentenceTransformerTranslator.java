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
 * Translator for any sentence-transformer style ONNX model loaded from HuggingFace Hub.
 *
 * Pipeline:
 *   1. Tokenize input with HuggingFaceTokenizer loaded from a local tokenizer.json (truncates at maxLength tokens)
 *   2. Run ONNX model with input_ids, attention_mask, token_type_ids
 *   3. Pool last_hidden_state (masked mean, or the CLS token when configured) → float[] embedding
 *
 * The attention mask is stashed on the TranslatorContext between processInput and processOutput.
 */
public class SentenceTransformerTranslator implements Translator<String, float[]> {

    private static final String MASK_KEY = "attentionMask";

    private final Path tokenizerJson;
    private final int maxLength;
    private final com.pharos.config.Pooling pooling;
    private HuggingFaceTokenizer tokenizer;

    /**
     * @param tokenizerJson local path to the tokenizer.json file
     * @param maxLength     max token length passed to the tokenizer (e.g. 8192)
     */
    public SentenceTransformerTranslator(Path tokenizerJson, int maxLength) {
        this(tokenizerJson, maxLength, com.pharos.config.Pooling.MEAN);
    }

    public SentenceTransformerTranslator(Path tokenizerJson, int maxLength, com.pharos.config.Pooling pooling) {
        this.tokenizerJson = tokenizerJson;
        this.maxLength = maxLength;
        this.pooling = pooling == null ? com.pharos.config.Pooling.MEAN : pooling;
    }

    @Override
    public void prepare(TranslatorContext ctx) throws IOException {
        // padding=false: each input is tokenized to its actual length, not padded to maxLength.
        // With 8k context windows, padding=true forces every input — even tiny methods — to allocate
        // [1, 8192, hidden] tensors (~25 MB each). Disabling padding shrinks tensors to actual token count.
        tokenizer = HuggingFaceTokenizer.newInstance(
                tokenizerJson,
                Map.of(
                        "maxLength",      String.valueOf(maxLength),
                        "modelMaxLength", String.valueOf(maxLength),
                        "padding",        "false",
                        "truncation",     "true"
                ));
    }

    @Override
    public NDList processInput(TranslatorContext ctx, String input) {
        Encoding encoding = tokenizer.encode(input);
        NDManager manager = ctx.getNDManager();

        NDArray inputIds      = manager.create(encoding.getIds()).reshape(1, -1);
        NDArray attentionMask = manager.create(encoding.getAttentionMask()).reshape(1, -1);

        // Stash mask for mean pooling in processOutput
        ctx.setAttachment(MASK_KEY, attentionMask);

        // Jina v2 ONNX model takes [input_ids, attention_mask] only (no token_type_ids)
        return new NDList(inputIds, attentionMask);
    }

    @Override
    public float[] processOutput(TranslatorContext ctx, NDList list) {
        // last_hidden_state: [1, seq_len, hidden_size]
        NDArray hidden = list.get(0);
        NDArray mask = (NDArray) ctx.getAttachment(MASK_KEY);

        // Pooling intermediates are owned by ctx.getNDManager() and released when the context closes.
        // toFloatArray() copies to Java primitives, so the result is safe after the context closes.
        return EmbeddingPooling.pool(hidden, mask, pooling)[0];
    }

    @Override
    public Batchifier getBatchifier() {
        return null; // we handle batching manually (single input at a time)
    }
}
