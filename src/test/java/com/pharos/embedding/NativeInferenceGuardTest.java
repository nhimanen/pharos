package com.pharos.embedding;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeInferenceGuardTest {

    @AfterEach
    void reset() {
        NativeInferenceGuard.resetForTests();
    }

    @Test
    void ticketsCountInFlightCalls() {
        int before = NativeInferenceGuard.inFlight();
        try (var a = NativeInferenceGuard.enter(); var b = NativeInferenceGuard.enter()) {
            assertThat(NativeInferenceGuard.inFlight()).isEqualTo(before + 2);
        }
        assertThat(NativeInferenceGuard.inFlight()).isEqualTo(before);
    }

    @Test
    void shutdownWaitsForTheRunningCall_thenRefusesNewOnes() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        AtomicLong finishedAt = new AtomicLong();
        Thread worker = new Thread(() -> {
            try (var t = NativeInferenceGuard.enter()) {
                started.countDown();
                Thread.sleep(300);                       // a model call in progress
                finishedAt.set(System.nanoTime());
            } catch (InterruptedException ignored) {
            }
        });
        worker.start();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(NativeInferenceGuard.beginShutdown(5_000)).isTrue();
        long shutdownReturnedAt = System.nanoTime();

        assertThat(finishedAt.get()).as("call finished before shutdown proceeded").isPositive()
                .isLessThanOrEqualTo(shutdownReturnedAt);
        assertThatThrownBy(NativeInferenceGuard::enter).isInstanceOf(IllegalStateException.class);
        worker.join();
    }

    @Test
    void shutdownGivesUpAfterTheTimeout() throws Exception {
        CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
        Thread stuck = new Thread(() -> {
            try (var t = NativeInferenceGuard.enter()) {
                started.countDown();
                release.await();
            } catch (InterruptedException ignored) {
            }
        });
        stuck.start();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(NativeInferenceGuard.beginShutdown(100)).isFalse();
        release.countDown();
        stuck.join();
    }
}
