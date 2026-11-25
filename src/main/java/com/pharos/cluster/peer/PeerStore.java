package com.pharos.cluster.peer;

import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ObjectStoreReplicaSource;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A peer-mode writer's published state on local disk ({@code cluster.peers.storeDir}): the
 * object-store layout with no prefix ({@code catalog/shards.json}, {@code catalog/synonyms.txt},
 * {@code shards/<shard>/LATEST}, {@code manifests/gen-N.json} and the content-named
 * {@code lucene/<file>@<crc>}, {@code graph/…}, {@code refs/…}), so manifest keys are the same names
 * as in a bucket and the Phase 2 block cache keys stay valid.
 *
 * @param signal signalled after each publish in this process (the {@code latest} long-poll)
 */
public record PeerStore(FsObjectStore store, ObjectStoreLayout layout, ObjectStoreCatalog catalog,
                        ObjectStoreReplicaSource source, GenerationSignal signal) {

    public static PeerStore open(Path dir) throws IOException {
        FsObjectStore store = new FsObjectStore(dir);
        ObjectStoreLayout layout = new ObjectStoreLayout("");
        return new PeerStore(store, layout, new ObjectStoreCatalog(store, layout),
                new ObjectStoreReplicaSource(store, layout), new GenerationSignal());
    }
}
