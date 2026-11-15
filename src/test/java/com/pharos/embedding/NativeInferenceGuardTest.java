package com.pharos.embedding;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.nio.file.Path;

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
    void closingATicketTwiceDoesNotUnderCount() {
        int before = NativeInferenceGuard.inFlight();
        var t = NativeInferenceGuard.enter();
        t.close();
        t.close();
        assertThat(NativeInferenceGuard.inFlight()).isEqualTo(before);
    }

    /** Child JVM: holds a ticket for {@code args[0]} ms (a model call in progress), then exits 0. */
    public static final class HoldTicket {
        public static void main(String[] args) throws Exception {
            try (var t = NativeInferenceGuard.enter()) {
                System.out.println("READY");
                System.out.flush();
                Thread.sleep(Long.parseLong(args[0]));
            }
            // as Main does: drain, then exit with the signal's code if one started the shutdown
            NativeInferenceGuard.beginShutdown();
            System.exit(NativeInferenceGuard.exitCode(0));
        }
    }

    private static Process startChild(long holdMs) throws Exception {
        Process p = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                HoldTicket.class.getName(), Long.toString(holdMs))
                .redirectErrorStream(true).start();
        var in = new java.io.BufferedReader(new java.io.InputStreamReader(p.getInputStream()));
        String line;
        while ((line = in.readLine()) != null && !line.contains("READY")) { /* JVM startup noise */ }
        return p;
    }

    private static void signal(Process p, String sig) throws Exception {
        new ProcessBuilder("kill", "-" + sig, Long.toString(p.pid())).start().waitFor();
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void sigterm_waitsForTheRunningCall_thenExitsWith143() throws Exception {
        Process p = startChild(1_500);
        long t0 = System.nanoTime();
        signal(p, "TERM");
        assertThat(p.waitFor(20, TimeUnit.SECONDS)).isTrue();
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertThat(p.exitValue()).isEqualTo(143);
        assertThat(ms).as("exit waited for the in-flight call").isGreaterThanOrEqualTo(1_000);
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void secondSignal_exitsImmediately() throws Exception {
        Process p = startChild(30_000);
        signal(p, "TERM");
        Thread.sleep(300);
        long t0 = System.nanoTime();
        signal(p, "TERM");
        assertThat(p.waitFor(10, TimeUnit.SECONDS)).isTrue();
        assertThat(p.exitValue()).isEqualTo(143);
        assertThat((System.nanoTime() - t0) / 1_000_000).isLessThan(5_000);
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
