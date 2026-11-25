package com.pharos.cluster.membership;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Task 5: the warm p95 window of {@link LoadCounter} and its heartbeat field. */
class LatencyWindowTest {

    private static final long MS = 1_000_000L;

    @Test
    void p95OverTheLastMinuteOnly() {
        AtomicLong now = new AtomicLong(1_000L * 1_000_000_000L);
        LatencyWindow w = new LatencyWindow(now::get);
        assertThat(w.p95Millis()).as("no requests").isNull();
        for (int i = 0; i < 95; i++) w.record(8 * MS);
        for (int i = 0; i < 5; i++) w.record(900 * MS);
        assertThat(w.p95Millis()).as("95th of 100 falls in the 7-10 ms bucket").isEqualTo(10L);
        w.record(900 * MS);
        assertThat(w.p95Millis()).isEqualTo(1000L);

        now.addAndGet(30L * 1_000_000_000L);
        for (int i = 0; i < 1000; i++) w.record(2 * MS);
        assertThat(w.p95Millis()).as("both batches in the window").isEqualTo(10L);
        now.addAndGet(31L * 1_000_000_000L); // the first batch left the window, the second did not
        assertThat(w.p95Millis()).isEqualTo(2L);
        now.addAndGet(60L * 1_000_000_000L);
        assertThat(w.p95Millis()).as("everything is older than a minute").isNull();
        w.record(70_000 * MS);
        assertThat(w.p95Millis()).as("overflow bucket reports the last bound").isEqualTo(60_000L);
    }

    @Test
    void loadCounterRecordsLatencyAndTheHeartbeatCarriesIt() throws Exception {
        AtomicLong now = new AtomicLong(5_000_000_000L);
        LoadCounter load = new LoadCounter(new LatencyWindow(now::get));
        load.begin();
        load.end(120 * MS);
        load.begin();
        load.end(); // no latency (not a timed request)
        assertThat(load.warmP95Millis()).isEqualTo(150L);
        assertThat(load.inFlight()).isZero();

        ObjectMapper m = new ObjectMapper();
        ReaderHeartbeat hb = new ReaderHeartbeat("r", "http://r", List.of(), false, null, 0, 0, 1L, null, null, 150L, 4L);
        ReaderHeartbeat back = m.readValue(m.writeValueAsBytes(hb), ReaderHeartbeat.class);
        assertThat(back.warmP95Millis()).isEqualTo(150L);
        assertThat(back.catalogAgeSeconds()).isEqualTo(4L);
        String old = "{\"node\":\"r\",\"advertiseUrl\":\"http://r\",\"shards\":[],\"draining\":false,\"inFlight\":0,"
                + "\"qps\":1.0,\"startedAt\":1}";
        ReaderHeartbeat fromOld = m.readValue(old, ReaderHeartbeat.class);
        assertThat(fromOld.warmP95Millis()).as("an older reader's heartbeat").isNull();
        assertThat(fromOld.catalogAgeSeconds()).isNull();
        assertThat(m.writeValueAsString(new ReaderHeartbeat("r", "http://r", List.of(), false, null, 0, 0, 1L)))
                .as("null fields are omitted").doesNotContain("warmP95Millis").doesNotContain("catalogAgeSeconds");
    }
}
