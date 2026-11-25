package com.pharos.cluster.objectstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryObjectStoreTest extends ObjectStoreContract {
    private final InMemoryObjectStore store = new InMemoryObjectStore();

    @Override
    ObjectStore store() { return store; }

    @Test
    void defaultStatFiltersTheListingForTheExactKey() throws Throwable {
        // A store that only implements the abstract methods gets stat() from the interface default.
        ObjectStore viaDefault = (ObjectStore) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ObjectStore.class}, (proxy, m, args) -> m.isDefault()
                        ? java.lang.reflect.InvocationHandler.invokeDefault(proxy, m, args)
                        : m.invoke(store, args));
        store.put("d/key", new byte[3]);
        store.put("d/key2", new byte[7]);
        assertThat(viaDefault.stat("d/key")).contains(store.stat("d/key").orElseThrow());
        assertThat(viaDefault.stat("d/key").orElseThrow().size()).isEqualTo(3);
        assertThat(viaDefault.stat("d/ke")).isEmpty();
    }

    @Test
    void failedRangeLeavesNoTarget(@TempDir Path dir) throws IOException {
        store.put("range/fail", new byte[1000]);
        store.failMidBodyOn("range/fail");
        Path t = dir.resolve("fail.bin");
        assertThatThrownBy(() -> store.downloadRange("range/fail", 0, 500, t)).isInstanceOf(IOException.class);
        assertThat(t).doesNotExist();
        try (var files = Files.list(dir)) {
            assertThat(files).isEmpty(); // temp file cleaned up too
        }
    }
}
