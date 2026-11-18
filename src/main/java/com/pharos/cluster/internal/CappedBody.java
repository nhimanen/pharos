package com.pharos.cluster.internal;

import java.io.ByteArrayOutputStream;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * Collects a response body up to {@code cap} bytes and then stops reading (the caller rejects a
 * body that reached the cap), so a runaway peer cannot fill the heap. Used with
 * {@code HttpClient.sendAsync} and a bounded wait, so the whole exchange, body included, has a
 * deadline (the JDK's request timeout ends once the headers arrive).
 */
public final class CappedBody implements HttpResponse.BodySubscriber<byte[]> {
    private final int cap;
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final CompletableFuture<byte[]> result = new CompletableFuture<>();
    private Flow.Subscription subscription;

    public CappedBody(int cap) { this.cap = cap; }

    @Override public CompletionStage<byte[]> getBody() { return result; }

    @Override public void onSubscribe(Flow.Subscription s) {
        subscription = s;
        if (cap <= 0) { // nothing wanted (e.g. a declared length already over the limit): don't read
            s.cancel();
            result.complete(new byte[0]);
            return;
        }
        s.request(Long.MAX_VALUE);
    }

    @Override public void onNext(List<ByteBuffer> items) {
        if (result.isDone()) return;
        for (ByteBuffer b : items) {
            int n = Math.min(b.remaining(), cap - out.size());
            byte[] chunk = new byte[n];
            b.get(chunk);
            out.write(chunk, 0, n);
            if (out.size() >= cap) {
                subscription.cancel();
                result.complete(out.toByteArray());
                return;
            }
        }
    }

    @Override public void onError(Throwable t) { result.completeExceptionally(t); }

    @Override public void onComplete() { result.complete(out.toByteArray()); }
}
