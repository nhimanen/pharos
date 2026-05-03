package com.pharos.embedding;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
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
 *   3. Mean-pool last_hidden_state over non-padding tokens → float[] embedding
 *
 * The attention mask is stashed on the TranslatorContext between processInput and processOutput.
 */
public class SentenceTransformerTranslator implements Translator<String, float[]> {

    private static final String MASK_KEY = "attentionMask";

    private final Path tokenizerJson;
    private final int maxLength;
    private HuggingFaceTokenizer tokenizer;

    /**
     * @param tokenizerJson local path to the tokenizer.json file
     * @param maxLength     max token length passed to the tokenizer (e.g. 8192)
     */
    public SentenceTransformerTranslator(Path tokenizerJson, int maxLength) {
        this.tokenizerJson = tokenizerJson;
        this.maxLength = maxLength;
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

        // All intermediate tensors (maskExpanded, products) are created inside a child NDManager
        // and released immediately when the try block exits, rather than waiting for GC pressure
        // to trigger deallocation of the parent TranslatorContext manager.
        // All intermediate tensors are created inside a child NDManager and released immediately
        // when the try block exits, rather than waiting for GC to trigger deallocation.
        try (NDManager child = ctx.getNDManager().newSubManager()) {
            NDArray maskFloat = mask.toType(DataType.FLOAT32, false);
            maskFloat.attach(child);
            NDArray maskExpanded = maskFloat
                    .reshape(1, mask.getShape().get(1), 1)
                    .broadcast(hidden.getShape());
            maskExpanded.attach(child);

            NDArray product = hidden.mul(maskExpanded);
            product.attach(child);
            NDArray sumEmbeddings = product.sum(new int[]{1});
            sumEmbeddings.attach(child);
            NDArray sumMask = maskExpanded.sum(new int[]{1}).clip(1e-9f, Float.MAX_VALUE);
            sumMask.attach(child);

            NDArray mean = sumEmbeddings.div(sumMask);
            mean.attach(child);

            // toFloatArray() copies to a Java primitive array — safe to return after child closes
            return mean.squeeze(0).toFloatArray();
        }
    }

    @Override
    public Batchifier getBatchifier() {
        return null; // we handle batching manually (single input at a time)
    }
}
