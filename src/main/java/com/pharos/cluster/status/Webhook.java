package com.pharos.cluster.status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Delivers status events to {@code cluster.status.webhookUrl} as JSON POSTs, one event per request,
 * in order, on its own daemon thread: {@link #send} only enqueues and never blocks status building.
 * A delivery that fails (an I/O error or a non-2xx answer) is retried with exponential back-off
 * ({@code firstBackoff}, doubling) up to {@code attempts} tries in all, then given up with a WARN;
 * the next event is tried afresh. When the queue is full the event is dropped with a WARN. Logs name
 * only the webhook's host: its URL may carry a secret.
 */
public final class Webhook implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Webhook.class);

    public static final int DEFAULT_ATTEMPTS = 4;
    public static final Duration DEFAULT_FIRST_BACKOFF = Duration.ofSeconds(1);
    static final int QUEUE = 256;
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    /** Sleeps between attempts; replaced in tests. */
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final URI uri;
    private final String host;
    private final int attempts;
    private final Duration firstBackoff;
    private final Sleeper sleeper;
    private final HttpClient http;
    private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(QUEUE);
    private final Thread worker;
    private volatile boolean closed;
    private volatile long delivered;
    private volatile long givenUp;

    public Webhook(String url) {
        this(url, DEFAULT_ATTEMPTS, DEFAULT_FIRST_BACKOFF, Thread::sleep);
    }

    Webhook(String url, int attempts, Duration firstBackoff, Sleeper sleeper) {
        this.uri = URI.create(Objects.requireNonNull(url, "url").trim());
        this.host = uri.getHost();
        this.attempts = Math.max(1, attempts);
        this.firstBackoff = firstBackoff;
        this.sleeper = sleeper;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        this.worker = Thread.ofPlatform().daemon().name("pharos-status-webhook").start(this::run);
    }

    /** Enqueues {@code events} for delivery; never blocks. */
    public void send(List<StatusEvent> events) {
        for (StatusEvent e : events) {
            if (closed) return;
            if (!queue.offer(e.toJson())) {
                log.warn("Status webhook ({}): queue full, dropping a {} event", host, e.type());
            }
        }
    }

    /** Events delivered (a 2xx answer). */
    public long delivered() { return delivered; }

    /** Events given up after every attempt failed. */
    public long givenUp() { return givenUp; }

    private void run() {
        while (!closed) {
            String body;
            try {
                body = queue.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                return;
            }
            if (body == null) continue;
            try {
                deliver(body);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void deliver(String body) throws InterruptedException {
        long backoff = firstBackoff.toMillis();
        String last = null;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            if (attempt > 1) {
                sleeper.sleep(backoff);
                backoff = Math.min(backoff * 2, 60_000);
            }
            if (closed) return;
            try {
                HttpResponse<Void> r = http.send(HttpRequest.newBuilder(uri).timeout(REQUEST_TIMEOUT)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(),
                        HttpResponse.BodyHandlers.discarding());
                if (r.statusCode() / 100 == 2) {
                    delivered++;
                    return;
                }
                last = "HTTP " + r.statusCode();
            } catch (java.io.IOException | RuntimeException e) {
                last = e.getClass().getSimpleName();
            }
        }
        givenUp++;
        log.warn("Status webhook ({}): giving up on an event after {} attempts (last: {})", host, attempts, last);
    }

    @Override
    public void close() {
        closed = true;
        worker.interrupt();
        http.close();
    }
}
