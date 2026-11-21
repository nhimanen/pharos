package com.pharos.cluster.status;

import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.StoredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/** Where the shared {@link StatusState} lives, with compare-and-set writes. */
public interface StatusStateStore {

    /** A state and the version it was read at (null: absent). */
    record Versioned(StatusState state, String etag) {}

    /** The current state; {@link StatusState#EMPTY} with a null etag when there is none. */
    Versioned read() throws IOException;

    /** Writes {@code next} if the state is still at {@code etag} (null: still absent). False when it changed. */
    boolean write(StatusState next, String etag) throws IOException;

    /** The bucket's {@code catalog/status/state.json}, by conditional puts. */
    static StatusStateStore bucket(ObjectStore store, ObjectStoreLayout layout) {
        return new Bucket(store, layout.statusStateKey());
    }

    /** A single node's state: in memory, never written anywhere. */
    static StatusStateStore memory() {
        return new Memory();
    }

    final class Bucket implements StatusStateStore {
        private static final Logger log = LoggerFactory.getLogger(Bucket.class);
        private final ObjectStore store;
        private final String key;

        Bucket(ObjectStore store, String key) {
            this.store = store;
            this.key = key;
        }

        @Override
        public Versioned read() throws IOException {
            Optional<StoredObject> o = store.get(key);
            if (o.isEmpty()) return new Versioned(StatusState.EMPTY, null);
            try {
                return new Versioned(StatusState.read(o.get().data()), o.get().etag());
            } catch (IOException e) { // unreadable: start over, replacing it by CAS on its etag
                log.warn("Ignoring the unreadable status state {}: {}", key, e.toString());
                return new Versioned(StatusState.EMPTY, o.get().etag());
            }
        }

        @Override
        public boolean write(StatusState next, String etag) throws IOException {
            byte[] data = next.toJson();
            return (etag == null ? store.putIfAbsent(key, data) : store.putIfMatch(key, data, etag)).isPresent();
        }
    }

    final class Memory implements StatusStateStore {
        private StatusState state = StatusState.EMPTY;
        private long version;

        @Override
        public synchronized Versioned read() {
            return new Versioned(state, version == 0 ? null : Long.toString(version));
        }

        @Override
        public synchronized boolean write(StatusState next, String etag) {
            String now = version == 0 ? null : Long.toString(version);
            if (!java.util.Objects.equals(now, etag)) return false;
            state = next;
            version++;
            return true;
        }
    }
}
