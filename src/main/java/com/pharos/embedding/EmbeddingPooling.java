package com.pharos.embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.types.DataType;
import com.pharos.config.Pooling;

/**
 * Reduces a model's token hidden states to one vector per input; shared by the single and
 * batch translators so both modes stay identical. No normalization happens here (or anywhere
 * in the translators) for either mode.
 */
final class EmbeddingPooling {

    private EmbeddingPooling() {}

    /**
     * @param hidden first model output, {@code [batch, seq, hidden]}
     * @param mask   attention mask, {@code [batch, seq]}
     * @return {@code [batch][hidden]}
     * @throws IllegalStateException for CLS pooling when the output is not 3-D (model already pooled)
     */
    static float[][] pool(NDArray hidden, NDArray mask, Pooling pooling) {
        int batch = (int) hidden.getShape().get(0);
        NDArray pooled;
        if (pooling == Pooling.CLS) {
            if (hidden.getShape().dimension() != 3) {
                throw new IllegalStateException(
                        "pooling 'cls' needs a 3-D [batch, seq, hidden] model output but got shape "
                        + hidden.getShape() + "; the model already pools its output, so set pooling to 'mean'");
            }
            pooled = hidden.get(":, 0, :");
        } else {
            // Unchanged historic behaviour. A 2-D (already pooled) output is not special-cased:
            // the mask broadcast below fails for it, exactly as before this change.
            NDArray maskFloat = mask.toType(DataType.FLOAT32, false);
            NDArray maskExpanded = maskFloat
                    .reshape(batch, mask.getShape().get(1), 1)
                    .broadcast(hidden.getShape());
            NDArray sum = hidden.mul(maskExpanded).sum(new int[]{1});
            NDArray count = maskExpanded.sum(new int[]{1}).clip(1e-9f, Float.MAX_VALUE);
            pooled = sum.div(count);
        }
        float[][] out = new float[batch][];
        for (int i = 0; i < batch; i++) out[i] = pooled.get(i).toFloatArray();
        return out;
    }
}
