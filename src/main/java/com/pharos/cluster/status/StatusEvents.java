package com.pharos.cluster.status;

import io.javalin.http.sse.SseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The {@code /cluster/events} server-sent event streams of this node, plus the optional webhook.
 * Each event goes out as SSE {@code event: <type>} with the event's JSON as {@code data}; a comment
 * line every {@code keepAlive} keeps idle streams (and the router's idle timeout) alive. A client
 * whose write fails is dropped. Broadcasting never blocks on the webhook (it only enqueues).
 */
public final class StatusEvents implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StatusEvents.class);

    private final Set<SseClient> clients = ConcurrentHashMap.newKeySet();
    private final Duration keepAlive;
    private final Webhook webhook;
    private final ScheduledExecutorService timer;

    /** @param webhook null without {@code cluster.status.webhookUrl} */
    public StatusEvents(Duration keepAlive, Webhook webhook) {
        this.keepAlive = keepAlive;
        this.webhook = webhook;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pharos-status-events-keepalive");
            t.setDaemon(true);
            return t;
        });
        long every = Math.max(1, keepAlive.toMillis());
        timer.scheduleWithFixedDelay(this::ping, every, every, TimeUnit.MILLISECONDS);
    }

    public Duration keepAlive() {
        return keepAlive;
    }

    /** Registers a new {@code /cluster/events} client: kept open until it disconnects or {@link #close}. */
    public void add(SseClient client) {
        client.keepAlive();
        client.onClose(() -> clients.remove(client));
        clients.add(client);
        try {
            client.sendComment("pharos cluster events");
        } catch (RuntimeException e) {
            clients.remove(client);
        }
    }

    public int clientCount() {
        return clients.size();
    }

    /** Sends {@code events} to every SSE client. */
    public void broadcast(List<StatusEvent> events) {
        if (events.isEmpty()) return;
        for (StatusEvent e : events) {
            String json = e.toJson();
            for (SseClient c : clients) {
                try {
                    if (c.terminated()) {
                        clients.remove(c);
                        continue;
                    }
                    synchronized (c) { // one frame at a time per stream (the keep-alive writes too)
                        c.sendEvent(e.type(), json);
                    }
                } catch (RuntimeException ex) {
                    clients.remove(c);
                    log.debug("Dropping a /cluster/events client: {}", ex.toString());
                }
            }
        }
    }

    /** Sends {@code events} to the webhook, if one is configured (enqueue only). */
    public void notifyWebhook(List<StatusEvent> events) {
        if (webhook != null && !events.isEmpty()) webhook.send(events);
    }

    private void ping() {
        for (SseClient c : clients) {
            try {
                if (c.terminated()) {
                    clients.remove(c);
                    continue;
                }
                synchronized (c) {
                    c.sendComment("keep-alive");
                }
            } catch (RuntimeException e) {
                clients.remove(c);
            }
        }
    }

    @Override
    public void close() {
        timer.shutdownNow();
        for (SseClient c : clients) {
            try {
                c.close();
            } catch (RuntimeException ignored) {
                // already gone
            }
        }
        clients.clear();
        if (webhook != null) webhook.close();
    }
}
