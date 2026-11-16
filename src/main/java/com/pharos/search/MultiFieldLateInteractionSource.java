package com.pharos.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.LateInteractionFloatValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Late-interaction source over several candidate chunk-vector fields (the legacy model's
 * {@code chunkVectors} and {@code chunkVec.legacy}). Each document carries at most one, so per
 * document the first field that has a value wins; documents with none keep the rescorer's fallback.
 */
public final class MultiFieldLateInteractionSource extends LateInteractionFloatValuesSource {
    private final List<String> fields;
    private final float[][] query;
    private final VectorSimilarityFunction sim;

    private MultiFieldLateInteractionSource(List<String> fields, float[][] query, VectorSimilarityFunction sim) {
        super(fields.get(0), query, sim);
        this.fields = fields;
        this.query = query;
        this.sim = sim;
    }

    /** Rescorer over {@code fields} that keeps the first-pass score for docs with no chunk vectors. */
    public static LateInteractionFloatValuesSource create(List<String> fields, float[][] q, VectorSimilarityFunction sim) {
        return new MultiFieldLateInteractionSource(fields, q, sim);
    }

    public static org.apache.lucene.search.LateInteractionRescorer fallbackRescorer(
            List<String> fields, float[][] query, VectorSimilarityFunction sim) {
        return new org.apache.lucene.search.LateInteractionRescorer(
                new MultiFieldLateInteractionSource(fields, query, sim)) {
            @Override
            protected float combine(float firstPassScore, boolean hasValue, double value) {
                return hasValue ? (float) value : firstPassScore;
            }
        };
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        List<DoubleValues> parts = new ArrayList<>(fields.size());
        for (String f : fields) {
            if (ctx.reader().getFieldInfos().fieldInfo(f) == null) continue;
            DoubleValues v = new LateInteractionFloatValuesSource(f, query, sim).getValues(ctx, scores);
            if (v != null) parts.add(v);
        }
        if (parts.size() <= 1) {
            return parts.isEmpty() ? super.getValues(ctx, scores) : parts.get(0);
        }
        return new DoubleValues() {
            private DoubleValues hit;
            @Override public double doubleValue() throws IOException { return hit.doubleValue(); }
            @Override public boolean advanceExact(int doc) throws IOException {
                for (DoubleValues v : parts) if (v.advanceExact(doc)) { hit = v; return true; }
                return false;
            }
        };
    }

    @Override public boolean equals(Object o) {
        return o instanceof MultiFieldLateInteractionSource m && m.fields.equals(fields)
                && java.util.Arrays.deepEquals(m.query, query) && m.sim == sim;
    }
    @Override public int hashCode() { return java.util.Objects.hash(fields, java.util.Arrays.deepHashCode(query), sim); }
}
