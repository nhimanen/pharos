package com.pharos.cluster.objectstore;

class InMemoryObjectStoreTest extends ObjectStoreContract {
    private final InMemoryObjectStore store = new InMemoryObjectStore();

    @Override
    ObjectStore store() { return store; }
}
