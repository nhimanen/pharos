package com.pharos.cluster.sharding;

import java.nio.charset.StandardCharsets;

/**
 * MurmurHash3 x64 128-bit (Austin Appleby's public-domain reference, seed 0), reduced to its first
 * 64 bits ({@code h1}). This equals Guava's {@code Hashing.murmur3_128().hashBytes(b).asLong()},
 * which the golden-value test pins, so ring positions are identical on every platform and JVM.
 * Strings are hashed as their UTF-8 bytes.
 */
public final class Murmur3 {

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    private Murmur3() {}

    /** {@code murmur3_64(s)}: the first 64 bits of MurmurHash3_x64_128 of the UTF-8 bytes of {@code s}. */
    public static long hash64(String s) {
        return hash64(s.getBytes(StandardCharsets.UTF_8));
    }

    public static long hash64(byte[] data) {
        int len = data.length;
        int nblocks = len / 16;
        long h1 = 0;
        long h2 = 0;
        for (int i = 0; i < nblocks; i++) {
            long k1 = getLongLE(data, i * 16);
            long k2 = getLongLE(data, i * 16 + 8);
            h1 ^= mixK1(k1);
            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;
            h2 ^= mixK2(k2);
            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }
        int tail = nblocks * 16;
        long k1 = 0;
        long k2 = 0;
        switch (len & 15) {
            case 15: k2 ^= (long) (data[tail + 14] & 0xff) << 48;
            case 14: k2 ^= (long) (data[tail + 13] & 0xff) << 40;
            case 13: k2 ^= (long) (data[tail + 12] & 0xff) << 32;
            case 12: k2 ^= (long) (data[tail + 11] & 0xff) << 24;
            case 11: k2 ^= (long) (data[tail + 10] & 0xff) << 16;
            case 10: k2 ^= (long) (data[tail + 9] & 0xff) << 8;
            case 9:  k2 ^= (long) (data[tail + 8] & 0xff);
                     h2 ^= mixK2(k2);
            case 8:  k1 ^= (long) (data[tail + 7] & 0xff) << 56;
            case 7:  k1 ^= (long) (data[tail + 6] & 0xff) << 48;
            case 6:  k1 ^= (long) (data[tail + 5] & 0xff) << 40;
            case 5:  k1 ^= (long) (data[tail + 4] & 0xff) << 32;
            case 4:  k1 ^= (long) (data[tail + 3] & 0xff) << 24;
            case 3:  k1 ^= (long) (data[tail + 2] & 0xff) << 16;
            case 2:  k1 ^= (long) (data[tail + 1] & 0xff) << 8;
            case 1:  k1 ^= (long) (data[tail] & 0xff);
                     h1 ^= mixK1(k1);
            default: break;
        }
        h1 ^= len;
        h2 ^= len;
        h1 += h2;
        h2 += h1;
        h1 = fmix64(h1);
        h2 = fmix64(h2);
        h1 += h2;
        return h1;
    }

    private static long mixK1(long k1) {
        k1 *= C1;
        k1 = Long.rotateLeft(k1, 31);
        return k1 * C2;
    }

    private static long mixK2(long k2) {
        k2 *= C2;
        k2 = Long.rotateLeft(k2, 33);
        return k2 * C1;
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long getLongLE(byte[] b, int off) {
        return (b[off] & 0xffL)
                | (b[off + 1] & 0xffL) << 8
                | (b[off + 2] & 0xffL) << 16
                | (b[off + 3] & 0xffL) << 24
                | (b[off + 4] & 0xffL) << 32
                | (b[off + 5] & 0xffL) << 40
                | (b[off + 6] & 0xffL) << 48
                | (b[off + 7] & 0xffL) << 56;
    }
}
