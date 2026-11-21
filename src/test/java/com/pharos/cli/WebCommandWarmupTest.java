package com.pharos.cli;

import com.pharos.cluster.Backends;
import com.pharos.cluster.ClusterFixture;
import com.pharos.cluster.ClusterRuntime;
import com.pharos.cluster.NodeRole;
import com.pharos.cluster.ReplicationBackend;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.TestBackends;
import com.pharos.config.IndexConfig;
import com.pharos.indexer.LuceneIndexer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code pharos web} warms the search models on every role except {@code writer}, which serves no search. */
class WebCommandWarmupTest {

    @TempDir Path tmp;

    @Test
    void writerSkipsTheSearchModelWarmUp() throws Exception {
        for (NodeRole role : NodeRole.values()) {
            IndexConfig config = ClusterFixture.config(tmp.resolve(role.json()), role, "b");
            ReplicationBackend backend = role == NodeRole.ALL ? Backends.create(config)
                    : TestBackends.objectStore(config, new InMemoryObjectStore());
            LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
            ClusterRuntime rt = new ClusterRuntime(config, backend, ClusterRuntime.registryFor(config), lucene);
            try {
                assertThat(new WebCommand(null, null, rt).isWriter()).as(role.json()).isEqualTo(role == NodeRole.WRITER);
            } finally {
                rt.close();
                lucene.close();
            }
        }
        assertThat(new WebCommand(null, null, null).isWriter()).as("no cluster runtime: a single node").isFalse();
    }
}
