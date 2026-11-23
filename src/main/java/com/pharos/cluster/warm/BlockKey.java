package com.pharos.cluster.warm;

import java.util.Objects;

/**
 * Identity of one fixed-size block of an object: block {@code index} covers bytes
 * {@code [index × blockSize, min((index + 1) × blockSize, objectSize))}. Object keys are
 * content-named, so the same block is shared by every generation that references the object.
 */
public record BlockKey(String objectKey, long index) {
    public BlockKey {
        Objects.requireNonNull(objectKey, "objectKey");
        if (index < 0) throw new IllegalArgumentException("negative block index: " + index);
    }
}
