package com.pharos.cluster.status;

import io.javalin.http.sse.SseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

/**
 * The {@code /cluster/events} server-sent event streams of this node, plus the optional webhook.
 * Each event goes out as SSE {@code event: <type>} with the event's JSON as {@code data}. Events
 * carry proposals and scale summaries only, never node addresses, so every client gets the same
 * stream with or without the cluster token.
 *
 * <p>Delivery never blocks the caller. {@link #broadcast} only offers each event to every client's
 * own bounded queue ({@value #QUEUE} items); a sender thread per client (virtual) does the socket
 * writes. A client whose queue overflows (it doesn't read) or whose write fails is dropped and its
 * stream closed. At most {@value #MAX_CLIENTS} clients per node: {@link #tryReserve} refuses more
 * (the route answers 503).
 *
 * <p>A comment every {@code keepAlive} keeps idle streams (and the router's idle timeout) alive. Its
 * timer runs only while a client is connected, so a node nobody watches runs no thread for it.
 */
public final class StatusEvents implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StatusEvents.class);

    public static final int MAX_CLIENTS = 64;
    static final int QUEUE = 32;

    /** One SSE stream as the hub writes it; {@link #of(SseClient)} adapts Javalin's client. */
    public interface Sink {
        void event(String type, String data);

        void comment(String text);

        void onClose(Runnable r);

        void close();
    }

    /** A Javalin SSE client as a sink (kept open after the handler returns). */
    public static Sink of(SseClient c) {
        c.keepAlive();
        return new Sink() {
            @Override public void event(String type, String data) { c.sendEvent(type, data); }

            @Override public void comment(String text) { c.sendComment(text); }

            @Override public void onClose(Runnable r) { c.onClose(r); }

            @Override public void close() { c.close(); }
        };
    }

    private record Item(String type, String data) {}

    private final class Stream {
        final Sink sink;
        final BlockingQueue<Item> queue = new ArrayBlockingQueue<>(QUEUE);
        volatile Thread sender;

        Stream(Sink sink) { this.sink = sink; }

        void run() {
            try {
                while (streams.contains(this)) {
                    Item it = queue.take();
                    if (it.type() == null) sink.comment(it.data());
                    else sink.event(it.type(), it.data());
                }
            } catch (InterruptedException e) {
                // dropped
            } catch (RuntimeException e) {
                drop(this, "write failed: " + e);
            }
        }
    }

    private final Set<Stream> streams = ConcurrentHashMap.newKeySet();
    private final AtomicInteger slots = new AtomicInteger();
    private final Duration keepAlive;
    private final Webhook webhook;
    private final Object timerMu = new Object();
    private ScheduledExecutorService timer; // guarded by timerMu; runs only while a client is connected
    private volatile IntConsumer clientsChanged = n -> { };
    private volatile boolean closed;

    /** @param webhook null without {@code cluster.status.webhookUrl} */
    public StatusEvents(Duration keepAlive, Webhook webhook) {
        this.keepAlive = keepAlive;
        this.webhook = webhook;
    }

    public Duration keepAlive() {
        return keepAlive;
    }

    /** Called with the new client count whenever a client joins or leaves. */
    public void onClientsChanged(IntConsumer listener) {
        this.clientsChanged = listener == null ? n -> { } : listener;
    }

    /** Takes one of the {@value #MAX_CLIENTS} client slots; false when all are taken (or closed). */
    public boolean tryReserve() {
        while (true) {
            int n = slots.get();
            if (closed || n >= MAX_CLIENTS) return false;
            if (slots.compareAndSet(n, n + 1)) return true;
        }
    }

    /** Gives back a slot reserved for a client that never got added. */
    public void release() {
        slots.updateAndGet(n -> Math.max(0, n - 1));
    }

    /** Adds a client on a slot taken by {@link #tryReserve}: kept until it disconnects, is dropped, or {@link #close}. */
    public void add(Sink sink) {
        if (closed) {
            release();
            sink.close();
            return;
        }
        Stream s = new Stream(sink);
        streams.add(s);
        sink.onClose(() -> drop(s, null));
        s.queue.offer(new Item(null, "pharos cluster events"));
        s.sender = Thread.ofVirtual().name("pharos-status-events-sender").start(s::run);
        startTimer();
        clientsChanged.accept(streams.size());
    }

    public int clientCount() {
        return streams.size();
    }

    /** Offers {@code events} to every client's queue; never blocks. */
    public void broadcast(List<StatusEvent> events) {
        for (StatusEvent e : events) {
            Item it = new Item(e.type(), e.toJson());
            for (Stream s : streams) offer(s, it);
        }
    }

    private void offer(Stream s, Item it) {
        if (!s.queue.offer(it)) drop(s, "it does not read (queue of " + QUEUE + " full)");
    }

    /** Sends {@code events} to the webhook, if one is configured (enqueue only). */
    public void notifyWebhook(List<StatusEvent> events) {
        if (webhook != null && !events.isEmpty()) webhook.send(events);
    }

    private void ping() {
        Item it = new Item(null, "keep-alive");
        for (Stream s : streams) offer(s, it);
    }

    private void drop(Stream s, String why) {
        if (!streams.remove(s)) return;
        release();
        if (why != null) log.info("Dropping a /cluster/events client: {}", why);
        Thread t = s.sender;
        if (t != null && t != Thread.currentThread()) t.interrupt();
        try {
            s.sink.close();
        } catch (RuntimeException ignored) {
            // already gone
        }
        stopTimerIfIdle();
        clientsChanged.accept(streams.size());
    }

    private void startTimer() {
        synchronized (timerMu) {
            if (timer != null || closed || streams.isEmpty()) return;
            timer = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pharos-status-events-keepalive");
                t.setDaemon(true);
                return t;
            });
            long every = Math.max(1, keepAlive.toMillis());
            timer.scheduleWithFixedDelay(this::ping, every, every, TimeUnit.MILLISECONDS);
        }
    }

    private void stopTimerIfIdle() {
        synchronized (timerMu) {
            if (timer == null || !streams.isEmpty()) return;
            timer.shutdownNow();
            timer = null;
        }
    }

    /** True while the keep-alive timer runs (a client is connected). */
    public boolean keepAliveRunning() {
        synchronized (timerMu) {
            return timer != null;
        }
    }

    @Override
    public void close() {
        closed = true;
        for (Stream s : List.copyOf(streams)) drop(s, null);
        synchronized (timerMu) {
            if (timer != null) timer.shutdownNow();
            timer = null;
        }
        if (webhook != null) webhook.close();
    }
}
