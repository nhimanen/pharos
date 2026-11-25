package com.pharos.cluster.objectstore;

import com.pharos.cluster.Generation;
import com.pharos.cluster.ShardId;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ObjectStoreLayoutTest {

    private final ObjectStoreLayout layout = new ObjectStoreLayout("idx");
    private static final Generation GEN = new Generation(7);

    /** A project id gives the Phase 1–5 keys byte for byte, so existing buckets keep working. */
    @Test
    void projectIdsKeepTheirKeys() {
        ShardId p = ShardId.ofProject("my-proj.v2");
        assertThat(layout.shardPrefix(p)).isEqualTo("idx/shards/my-proj.v2/");
        assertThat(layout.latestKey(p)).isEqualTo("idx/shards/my-proj.v2/LATEST");
        assertThat(layout.leaseKey(p)).isEqualTo("idx/shards/my-proj.v2/lease");
        assertThat(layout.manifestKey(p, GEN)).isEqualTo("idx/shards/my-proj.v2/manifests/gen-7.json");
        assertThat(layout.luceneKey(p, "_0.cfs", "abc")).isEqualTo("idx/shards/my-proj.v2/lucene/_0.cfs@abc");
        assertThat(layout.graphKey(p, GEN, "c1")).isEqualTo("idx/shards/my-proj.v2/graph/gen-7@c1.cgf.gz");
        assertThat(layout.refsKey(p, GEN, "c2")).isEqualTo("idx/shards/my-proj.v2/refs/gen-7@c2.json.gz");
        assertThat(layout.writerStatePrefix(p)).isEqualTo("idx/shards/my-proj.v2/writer-state/");
        assertThat(layout.writerStateGenPrefix(p, GEN)).isEqualTo("idx/shards/my-proj.v2/writer-state/gen-7/");
        assertThat(layout.writerStateIndexKey(p, GEN)).isEqualTo("idx/shards/my-proj.v2/writer-state/gen-7/index.json");
        assertThat(layout.writerStateBlobKey(p, "ff")).isEqualTo("idx/shards/my-proj.v2/writer-state/blobs/ff");
        assertThat(new ObjectStoreLayout("").latestKey(p)).isEqualTo("shards/my-proj.v2/LATEST");
    }

    /** Ruling P6-R3: every key builder that takes a shard embeds it URL-safe encoded. */
    @Test
    void everyShardKeyBuilderEncodesTheId() throws Exception {
        ShardId keyed = new ShardId("lucene~module=core~ring=s03");
        String encodedPrefix = "idx/shards/lucene~module%3Dcore~ring%3Ds03/";
        List<String> builders = new ArrayList<>();
        for (Method m : ObjectStoreLayout.class.getDeclaredMethods()) {
            if (!Modifier.isPublic(m.getModifiers()) || Modifier.isStatic(m.getModifiers())) continue;
            Class<?>[] types = m.getParameterTypes();
            if (types.length == 0 || types[0] != ShardId.class) continue;
            Object[] args = new Object[types.length];
            args[0] = keyed;
            for (int i = 1; i < types.length; i++) {
                if (types[i] == Generation.class) args[i] = GEN;
                else if (types[i] == String.class) args[i] = "x" + i;
                else throw new AssertionError("unexpected parameter type " + types[i] + " of " + m);
            }
            String key = (String) m.invoke(layout, args);
            builders.add(m.getName());
            assertThat(key).as(m.getName()).startsWith(encodedPrefix).doesNotContain("=");
            assertThat(key.substring("idx/shards/".length()).split("/")[0]).as(m.getName())
                    .isEqualTo(keyed.pathSegment());
            assertThat(ShardId.fromPathSegment(key.split("/")[2])).as(m.getName()).contains(keyed);
        }
        assertThat(builders).contains("shardPrefix", "latestKey", "leaseKey", "manifestKey", "luceneKey", "graphKey",
                "refsKey", "writerStatePrefix", "writerStateGenPrefix", "writerStateIndexKey", "writerStateBlobKey");
    }

    /** Two distinct ids never share a prefix: one is never a key-prefix of the other's keys. */
    @Test
    void distinctIdsHaveDisjointPrefixes() {
        String a = layout.shardPrefix(ShardId.ofProject("lucene"));
        String b = layout.shardPrefix(new ShardId("lucene~ring=s01"));
        String c = layout.shardPrefix(new ShardId("lucene~ring=s010"));
        assertThat(b).doesNotStartWith(a);
        assertThat(c).doesNotStartWith(b);
    }
}
