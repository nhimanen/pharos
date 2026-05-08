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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Batched sentence-transformer translator: accepts a {@code List<String>} and returns
 * a {@code float[][]} (one embedding per input).
 *
 * <p>Unlike {@link SentenceTransformerTranslator} which processes one string at a time,
 * this translator pads all sequences in the batch to the longest sequence in the batch
 * and runs the ONNX model in a single forward pass.  This amortises JNI and
 * ONNX Runtime session overhead across the entire batch.
 *
 * <p>Used via a dedicated {@code Predictor<List<String>, float[][]>} — the caller
 * invokes {@code predictor.predict(texts)} (not {@code batchPredict}), so no
 * {@link Batchifier} is needed.
 */
public class BatchSentenceTransformerTranslator implements Translator<List<String>, float[][]> {

    private static final String MASK_KEY = "batchAttentionMask";
    private static final String SIZE_KEY = "batchSize";

    private final Path tokenizerJson;
    private final int maxLength;
    private HuggingFaceTokenizer tokenizer;

    /**
     * @param tokenizerJson local path to the tokenizer.json file
     * @param maxLength     per-sequence token limit (same as the single-item translator)
     */
    public BatchSentenceTransformerTranslator(Path tokenizerJson, int maxLength) {
        this.tokenizerJson = tokenizerJson;
        this.maxLength = maxLength;
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

    @Override
    public NDList processInput(TranslatorContext ctx, List<String> inputs) {
        int batchSize = inputs.size();

        // Tokenize each input without padding — we'll pad manually to batch-max length
        Encoding[] encodings = inputs.stream()
                .map(text -> tokenizer.encode(text != null ? text : ""))
                .toArray(Encoding[]::new);

        // Find the longest sequence in this batch (capped at maxLength by the tokenizer)
        int maxLen = Arrays.stream(encodings)
                .mapToInt(enc -> enc.getIds().length)
                .max()
                .orElse(1);

        // Build 2-D arrays — zero-pad shorter sequences (pad token = 0, mask = 0)
        long[][] inputIdsBatch      = new long[batchSize][maxLen];
        long[][] attentionMaskBatch = new long[batchSize][maxLen];
        for (int i = 0; i < batchSize; i++) {
            long[] ids  = encodings[i].getIds();
            long[] mask = encodings[i].getAttentionMask();
            System.arraycopy(ids,  0, inputIdsBatch[i],      0, ids.length);
            System.arraycopy(mask, 0, attentionMaskBatch[i], 0, mask.length);
        }

        NDManager manager = ctx.getNDManager();
        NDArray inputIds      = manager.create(inputIdsBatch);      // [batch, seq]
        NDArray attentionMask = manager.create(attentionMaskBatch); // [batch, seq]

        ctx.setAttachment(MASK_KEY, attentionMask);
        ctx.setAttachment(SIZE_KEY, batchSize);

        return new NDList(inputIds, attentionMask);
    }

    @Override
    public float[][] processOutput(TranslatorContext ctx, NDList list) {
        // last_hidden_state: [batch, seq_len, hidden_size]
        NDArray hidden = list.get(0);
        NDArray mask   = (NDArray) ctx.getAttachment(MASK_KEY); // [batch, seq_len]
        int batchSize  = (int) ctx.getAttachment(SIZE_KEY);

        // Mean-pool over the sequence dimension (dim=1) using the attention mask
        NDArray maskFloat    = mask.toType(DataType.FLOAT32, false);                   // [batch, seq]
        NDArray maskExpanded = maskFloat
                .reshape(batchSize, mask.getShape().get(1), 1)
                .broadcast(hidden.getShape());                                          // [batch, seq, hidden]
        NDArray sumEmbeddings = hidden.mul(maskExpanded).sum(new int[]{1});             // [batch, hidden]
        NDArray sumMask       = maskExpanded.sum(new int[]{1}).clip(1e-9f, Float.MAX_VALUE); // [batch, hidden]
        NDArray pooled        = sumEmbeddings.div(sumMask);                             // [batch, hidden]

        float[][] results = new float[batchSize][];
        for (int i = 0; i < batchSize; i++) {
            results[i] = pooled.get(i).toFloatArray();
        }
        return results;
    }

    @Override
    public Batchifier getBatchifier() {
        // We handle batching ourselves inside processInput/processOutput
        return null;
    }
}
