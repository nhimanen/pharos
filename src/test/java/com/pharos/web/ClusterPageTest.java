package com.pharos.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.status.ClusterPageFixtures;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The Cluster page (web/cluster.js, drawn from {@code /cluster/status}). There is no JS test runner:
 * this test serves the page and its assets, checks the golden status fixture against the one list of
 * status fields cluster.js reads ({@code STATUS_FIELDS}), and, where {@code node} is on PATH, runs
 * cluster.js's pure functions on the fixtures and checks what they render.
 */
class ClusterPageTest {

    private static final Path CLUSTER_JS = Path.of("src/main/resources/web/cluster.js");
    private static final Path GOLDEN = Path.of("src/test/resources/cluster/status-golden.json");
    private static final ObjectMapper JSON = new ObjectMapper();

    // ── served ───────────────────────────────────────────────

    @Test
    void servesThePageAndItsAssets() throws Exception {
        WebServer server = new WebServer(null, null, null, null, null);
        io.javalin.Javalin app = server.start(0);
        try {
            HttpClient http = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + app.port();
            var index = get(http, base + "/");
            assertThat(index.statusCode()).isEqualTo(200);
            String html = index.body();
            assertThat(html).contains("id=\"btn-cluster\" class=\"btn-view\"", "id=\"cluster-view\"",
                    "id=\"cluster-content\"", "id=\"cluster-live\"");
            assertThat(html.indexOf("src=\"/cluster.js\"")).as("cluster.js loads (deferred) before app.js")
                    .isPositive().isLessThan(html.indexOf("src=\"/app.js\""));

            var js = get(http, base + "/cluster.js");
            assertThat(js.statusCode()).isEqualTo(200);
            assertThat(js.headers().firstValue("Content-Type").orElse("")).contains("javascript");
            assertThat(js.body()).contains("const STATUS_FIELDS = [", "PharosCluster");
            var appJs = get(http, base + "/app.js");
            assertThat(appJs.statusCode()).isEqualTo(200);
            assertThat(appJs.body()).contains("PharosCluster.toView(", "PharosCluster.renderPage(",
                    "new EventSource('/cluster/events')", "fetch('/cluster/status'", "CLUSTER_POLL_MS = 10000");
            var css = get(http, base + "/style.css");
            assertThat(css.statusCode()).isEqualTo(200);
            assertThat(css.body()).contains("#cluster-view", ".cl-st-lease-expired");

            // a node without a status service: the page shows a notice for the 404
            assertThat(get(http, base + "/cluster/status").statusCode()).isEqualTo(404);
        } finally {
            app.stop();
        }
    }

    // ── the field list ───────────────────────────────────────

    /**
     * Every field cluster.js reads is in the golden fixture's shape, and has a value in it or in the
     * resharding fixtures (the golden cluster has no reshard: its {@code reshard} and {@code staged} are
     * null and its {@code specs} empty).
     */
    @Test
    void theGoldenFixtureHasEveryFieldClusterJsReads() throws Exception {
        List<String> fields = statusFields();
        assertThat(fields).hasSizeGreaterThan(40).doesNotHaveDuplicates()
                .noneMatch(f -> f.contains("advertiseUrl")); // the page gets the public view
        JsonNode golden = JSON.readTree(GOLDEN.toFile());
        JsonNode building = JSON.readTree(ClusterPageFixtures.json(ClusterPageFixtures.resharding(false)));
        JsonNode pinned = JSON.readTree(ClusterPageFixtures.json(ClusterPageFixtures.resharding(true)));
        for (String f : fields) {
            List<JsonNode> values = new ArrayList<>(present(golden, f));
            values.addAll(present(building, f));
            values.addAll(present(pinned, f));
            assertThat(values).as(f + " is non-null somewhere in the fixtures").anyMatch(n -> !n.isNull());
        }
    }

    @Test
    void clusterJsReadsNoStatusFieldMissingFromTheList() throws Exception {
        Set<String> listed = new TreeSet<>();
        for (String f : statusFields()) for (String seg : f.split("\\.")) listed.add(seg.replace("[]", ""));
        Set<String> statusKeys = new TreeSet<>();
        collectKeys(JSON.readTree(GOLDEN.toFile()), statusKeys);

        String src = Files.readString(CLUSTER_JS);
        src = src.replaceAll("(?s)const STATUS_FIELDS = \\[.*?\\];", "")
                .replaceAll("(?m)^\\s*//.*$", "")
                .replaceAll("(?s)/\\*.*?\\*/", "");
        Set<String> unlisted = new TreeSet<>();
        Matcher m = Pattern.compile("\\.([A-Za-z_$][\\w$]*)").matcher(src);
        while (m.find()) {
            String name = m.group(1);
            if (statusKeys.contains(name) && !listed.contains(name)) unlisted.add(name);
        }
        assertThat(unlisted).as("status fields cluster.js reads: add them to STATUS_FIELDS").isEmpty();
    }

    // ── rendered (node) ──────────────────────────────────────

    @Test
    void rendersTheGoldenFixture(@TempDir Path tmp) throws Exception {
        JsonNode out = render(tmp, Files.readString(GOLDEN));
        JsonNode view = out.path("view");

        // backlog: pending shards oldest first, source against published revision, the wait
        JsonNode pending = view.path("backlog").path("pending");
        assertThat(names(pending, "shard")).containsExactly("alpha", "beta");
        assertThat(pending.get(0).path("waiting").asLong()).isEqualTo(493);
        assertThat(pending.get(0).path("sourceRevision").asText()).isEqualTo("9a0b7c3");
        assertThat(pending.get(0).path("publishedRevision").asText()).isEqualTo("4f1c2e9");
        assertThat(pending.get(1).path("sourceRevision").isNull()).isTrue();
        // a writer's job, its phase and progress
        JsonNode job = view.path("backlog").path("jobs").get(0);
        assertThat(job.path("node").asText()).isEqualTo("w1");
        assertThat(job.path("job").path("phase").asText()).isEqualTo("embed");
        assertThat(job.path("job").path("progress").asDouble()).isEqualTo(0.42);
        assertThat(view.path("backlog").path("jobs").get(1).path("job").isNull()).isTrue();

        String page = out.path("html").asText();
        Document doc = xml(page);
        Element backlog = byId(doc, "cl-backlog");
        String backlogText = backlog.getTextContent();
        assertThat(backlogText.indexOf("alpha")).isLessThan(backlogText.indexOf("beta"));
        assertThat(backlogText).contains("8 m 13 s", "7 m 0 s", "embed", "42%");
        assertThat(page).contains("class=\"cl-progress-fill\" style=\"width:42%\"");

        // diagram: owners from the lease, the dead lease holder drawn dashed, pending marked on chips
        assertThat(boxes(view, "writers")).as("a ghost writer sorts in by node id")
                .containsExactly("w1:alpha", "w2:gamma", "w3:beta");
        assertThat(view.path("diagram").path("writers").get(2).path("live").asBoolean()).isFalse();
        assertThat(boxes(view, "readers")).containsExactly("r1:alpha,beta,gamma", "r2:alpha,gamma");
        Element svg = (Element) doc.getElementsByTagName("svg").item(0);
        assertThat(count(svg, "circle", "cl-pending")).as("alpha and beta, on every box showing them")
                .isEqualTo(/* w1, w3, bucket×2, r1×2, r2×1 */ 7);
        assertThat(count(svg, "g", "cl-st-lease-expired")).isEqualTo(3); // w3, bucket, r1
        assertThat(count(svg, "g", "cl-degraded")).isEqualTo(1);          // gamma on r2
        assertThat(count(svg, "g", "cl-low")).isEqualTo(1);               // w2

        // nodes, sharding, scale, health
        assertThat(names(view.path("nodes"), "node")).containsExactly("w1", "w2", "r1", "r2");
        assertThat(byId(doc, "cl-nodes").getTextContent()).contains("812 MiB", "1.0 GiB", "97.3%", "72.4",
                "640 ms", "412 GiB", "low");
        assertThat(byId(doc, "cl-sharding").getTextContent()).contains("project=alpha", "lease expired",
                "Spec version1");
        Element scale = byId(doc, "cl-scale");
        assertThat(scale.getTextContent()).contains("add-readers", "QPS per reader and warm p95 above target",
                "Start 1 reader", "qpsPerReader", "69.25", "reshard", "68%", "writer-disk", "3.0 GiB");
        assertThat(byId(doc, "cl-health").getTextContent()).contains("lease-expired", "writer w2 low on disk");

        assertLegible(out.path("layoutWide"), 1280);
        assertLegible(out.path("layoutNarrow"), 420);
        xml(out.path("htmlNarrow").asText());
    }

    @Test
    void rendersASingleNodeAsOneNodeHoldingEveryShard(@TempDir Path tmp) throws Exception {
        JsonNode out = render(tmp, ClusterPageFixtures.json(ClusterPageFixtures.singleNode()));
        JsonNode view = out.path("view");
        assertThat(view.path("single").asBoolean()).isTrue();
        JsonNode node = view.path("diagram").path("node");
        assertThat(names(node.path("names"), null)).containsExactly("laptop");
        assertThat(names(node.path("chips"), "shard")).containsExactly("docs", "pharos", "sample-app", "scratch");
        assertThat(view.path("diagram").path("writers")).isEmpty();
        assertThat(view.path("diagram").path("readers")).isEmpty();
        assertThat(view.path("nodes")).hasSize(1);
        assertThat(view.path("nodes").get(0).path("role").asText()).isEqualTo("writer + reader");
        assertThat(names(view.path("nodes").get(0).path("shards"), null)).hasSize(4);
        assertThat(out.path("layoutWide").path("boxes")).hasSize(1);
        assertThat(out.path("layoutWide").path("links")).isEmpty();
        xml(out.path("html").asText());
        xml(out.path("htmlNarrow").asText());
        assertLegible(out.path("layoutNarrow"), 420);
    }

    @Test
    void aLargeClusterKeepsALegibleDiagram(@TempDir Path tmp) throws Exception {
        JsonNode out = render(tmp, ClusterPageFixtures.json(ClusterPageFixtures.large()));
        JsonNode view = out.path("view");
        assertThat(view.path("diagram").path("writers")).as("9 live writers and w06 without a heartbeat").hasSize(10);
        List<String> writerIds = new ArrayList<>();
        view.path("diagram").path("writers").forEach(w -> writerIds.add(w.path("names").get(0).asText()));
        assertThat(writerIds).as("the ghost writer sorts in by node id")
                .containsExactly("w01", "w02", "w03", "w04", "w05", "w06", "w07", "w08", "w09", "w10");
        assertThat(view.path("diagram").path("writers").get(5).path("live").asBoolean()).isFalse();
        assertThat(view.path("diagram").path("readers")).hasSize(10);
        assertLegible(out.path("layoutWide"), 1280);
        assertLegible(out.path("layoutNarrow"), 420);
        xml(out.path("html").asText());
        xml(out.path("htmlNarrow").asText());
        // past MAX_BOXES, boxes with the same shards merge (r01..r04 serve all 8 shards, ready)
        JsonNode grouped = out.path("grouped");
        assertThat(grouped.size()).isLessThan(14);
        assertThat(names(grouped.get(0).path("names"), null)).hasSizeGreaterThan(1);
    }

    /** A reshard in progress: staged shards marked, a table per spec version, progress in shards and files. */
    @Test
    void rendersAStagedVersion(@TempDir Path tmp) throws Exception {
        JsonNode out = render(tmp, ClusterPageFixtures.json(ClusterPageFixtures.resharding()));
        JsonNode view = out.path("view");
        JsonNode r = view.path("reshards").get(0);
        assertThat(view.path("reshards")).hasSize(1);
        assertThat(r.path("project").asText()).isEqualTo("lucene");
        assertThat(r.path("phase").asText()).isEqualTo("building");
        assertThat(r.path("shardsDone").asInt()).isEqualTo(2);
        assertThat(r.path("shardsTotal").asInt()).isEqualTo(3);
        assertThat(r.path("filesDone").asInt()).isEqualTo(10);
        assertThat(r.path("filesTotal").asInt()).isEqualTo(40);
        assertThat(r.path("holder").asText()).isEqualTo("w1");

        String page = out.path("html").asText();
        Document doc = xml(page);
        // diagram: s03 (only in the staged version) is marked staged on the bucket and on w1, the writer
        // building it (the reshard lease holder); the reader serves the active version's s01 and s02,
        // unmarked while nothing of them is rewritten yet
        Element svg = (Element) doc.getElementsByTagName("svg").item(0);
        assertThat(count(svg, "g", "cl-staged")).as("s03 on the bucket and on w1").isEqualTo(2);
        assertThat(count(svg, "g", "cl-st-staged")).isEqualTo(2);
        assertThat(boxes(view, "writers")).containsExactly("w1:docs,lucene~ring=s01,lucene~ring=s02,lucene~ring=s03");
        assertThat(boxes(view, "readers")).containsExactly("r1:docs,lucene~ring=s01,lucene~ring=s02");
        assertThat(byId(doc, "cl-layout").getTextContent()).contains("staged (reshard, not served yet)");

        String reshards = byId(doc, "cl-reshards").getTextContent();
        assertThat(reshards).contains("lucene", "building", "v1 → v2", "[project, ring(path, shards=3, vnodes=512)]",
                "2 / 3", "10 / 40", "w1");
        assertThat(page).contains("aria-valuenow=\"67\"", "aria-valuenow=\"25\"");

        // sharding: spec versions, the project, and one shard table per version
        String sharding = byId(doc, "cl-sharding").getTextContent();
        assertThat(sharding).contains("Spec versions", "Spec version 1 · active", "Spec version 2 · staged",
                "project=lucene, ring=s03", "not served yet").doesNotContain("rewritten in staged version");
        assertThat(sharding.indexOf("Spec version 1 · active")).isLessThan(sharding.indexOf("Spec version 2 · staged"));
        JsonNode versions = view.path("sharding").path("versions");
        assertThat(versions.get(0).path("state").asText()).isEqualTo("active");
        assertThat(versions.get(1).path("state").asText()).isEqualTo("staged");
        JsonNode lucene = view.path("sharding").path("projects").get(1);
        assertThat(lucene.path("project").asText()).isEqualTo("lucene");
        assertThat(lucene.path("staged").path("state").asText()).isEqualTo("building");
        assertThat(names(view.path("sharding").path("projects"), "project")).containsExactly("docs", "lucene");

        assertLegible(out.path("layoutWide"), 1280);
        assertLegible(out.path("layoutNarrow"), 420);
        xml(out.path("htmlNarrow").asText());

        // once pinned, a shard both versions share but the staged one rewrote is marked too, on every box;
        // s02, kept as it is, is not
        JsonNode pinnedOut = render(tmp, ClusterPageFixtures.json(ClusterPageFixtures.resharding(true)));
        Document pinned = xml(pinnedOut.path("html").asText());
        Element pinnedSvg = (Element) pinned.getElementsByTagName("svg").item(0);
        assertThat(count(pinnedSvg, "g", "cl-staged")).as("s01 on bucket, w1, r1; s03 on bucket, w1").isEqualTo(5);
        assertThat(byId(pinned, "cl-sharding").getTextContent()).contains("rewritten in staged version", "4 → 5");
        assertThat(byId(pinned, "cl-reshards").getTextContent()).contains("pinned", "3 / 3", "40 / 40",
                "reader r1 (lucene~ring=s03)");
        // the version an earlier flip retired: its shard faded on the bucket, in no writer's box, its own table
        assertThat(count(pinnedSvg, "g", "cl-retired")).isEqualTo(1);
        assertThat(names(pinnedOut.path("view").path("diagram").path("writers"), null)).hasSize(1);
        assertThat(byId(pinned, "cl-sharding").getTextContent()).contains("Spec version 3 · previous",
                "[project, ring(path, shards=1, vnodes=512)]");
        assertThat(byId(pinned, "cl-reshards").getTextContent()).contains("web", "flipped", "v3 → v4");
    }

    /**
     * Status strings are data: a hostile heartbeat (any string as node id, state, shard, phase…)
     * must not break out of an attribute or inject markup. Classes come only from allow-lists.
     */
    @Test
    void hostileStatusStringsRenderAsText(@TempDir Path tmp) throws Exception {
        String a = "\"><img src=x onerror=alert(1)>";
        String b = "x\" onmouseover=\"y";
        var st = (com.fasterxml.jackson.databind.node.ObjectNode) JSON.readTree(GOLDEN.toFile());
        var layout = (com.fasterxml.jackson.databind.node.ObjectNode) st.get("layout");
        int i = 0;
        for (JsonNode n : layout.get("shards")) {
            var o = (com.fasterxml.jackson.databind.node.ObjectNode) n;
            String evil = (i++ % 2 == 0 ? a : b) + i;
            o.put("shard", evil);
            o.put("state", i % 2 == 0 ? a : b);
            o.put("writer", b);
            ((com.fasterxml.jackson.databind.node.ObjectNode) o.get("keys")).put(a, b);
            ((com.fasterxml.jackson.databind.node.ObjectNode) o.get("lease")).put("writer", a + i);
            ((com.fasterxml.jackson.databind.node.ObjectNode) o.get("backlog")).put("sourceRevision", a)
                    .put("publishedRevision", b);
            o.putArray("readers").add(a).add(b);
        }
        i = 0;
        for (JsonNode n : layout.get("writers")) {
            var o = (com.fasterxml.jackson.databind.node.ObjectNode) n;
            o.put("node", (i++ % 2 == 0 ? a : b) + "w" + i);
            o.putArray("leases").add(a);
            if (o.get("indexing").isObject()) {
                ((com.fasterxml.jackson.databind.node.ObjectNode) o.get("indexing")).put("phase", a).put("shard", b)
                        .put("progress", b);
            }
        }
        i = 0;
        for (JsonNode n : layout.get("readers")) {
            var o = (com.fasterxml.jackson.databind.node.ObjectNode) n;
            o.put("node", (i++ % 2 == 0 ? a : b) + "r" + i);
            o.put("state", i % 2 == 0 ? a : b);
            o.putArray("shards").add(a + "1").add(b + "2").add(a);
            o.putArray("degradedShards").add(a + "1").add(b);
        }
        for (JsonNode n : st.get("health")) {
            ((com.fasterxml.jackson.databind.node.ObjectNode) n).put("kind", a).put("target", b).put("message", a + b);
        }
        for (JsonNode n : st.get("proposals")) {
            var o = (com.fasterxml.jackson.databind.node.ObjectNode) n;
            o.put("kind", b).put("target", a).put("reason", a).put("action", b).put("since", a).put("id", b);
            ((com.fasterxml.jackson.databind.node.ObjectNode) o.get("metrics")).put(a, b).put(b, 1.5);
        }
        st.put("asOf", a);
        st.put("mode", b);

        JsonNode out = render(tmp, JSON.writeValueAsString(st));
        for (String key : List.of("html", "htmlNarrow")) {
            String page = out.path(key).asText();
            assertThat(page).as(key).doesNotContain("<img", "onerror=\"", "onmouseover=\"", "x\" ");
            Document doc = xml(page);
            NodeList all = doc.getElementsByTagName("*");
            for (int k = 0; k < all.getLength(); k++) {
                Element e = (Element) all.item(k);
                assertThat(e.getTagName()).isNotEqualToIgnoringCase("img");
                var attrs = e.getAttributes();
                for (int j = 0; j < attrs.getLength(); j++) {
                    String name = attrs.item(j).getNodeName();
                    assertThat(name).as("event handler attribute on <%s>", e.getTagName()).doesNotStartWith("on");
                    if (name.equals("class")) {
                        assertThat(attrs.item(j).getNodeValue()).as("class tokens come from allow-lists")
                                .matches("[a-z0-9 -]*");
                    }
                }
            }
            // the strings are still shown, as text
            assertThat(doc.getDocumentElement().getTextContent()).contains("<img src=x onerror=alert(1)>");
        }
    }

    // ── helpers ──────────────────────────────────────────────

    private static HttpResponse<String> get(HttpClient http, String url) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).build(), HttpResponse.BodyHandlers.ofString());
    }

    static List<String> statusFields() throws Exception {
        Matcher block = Pattern.compile("(?s)const STATUS_FIELDS = \\[(.*?)\\];").matcher(Files.readString(CLUSTER_JS));
        assertThat(block.find()).as("cluster.js declares const STATUS_FIELDS = [...];").isTrue();
        List<String> out = new ArrayList<>();
        Matcher s = Pattern.compile("'([^']+)'").matcher(block.group(1));
        while (s.find()) out.add(s.group(1));
        return out;
    }

    /**
     * {@code a.b[].c}: every object on the path has the key (it may be null; lists may be empty); returns
     * the field's values.
     */
    private static List<JsonNode> present(JsonNode root, String path) {
        List<JsonNode> cur = List.of(root);
        for (String seg : path.split("\\.")) {
            boolean list = seg.endsWith("[]");
            String key = list ? seg.substring(0, seg.length() - 2) : seg;
            List<JsonNode> next = new ArrayList<>();
            for (JsonNode n : cur) {
                if (n.isNull()) continue;
                assertThat(n.isObject()).as(path + ": object expected before " + key).isTrue();
                assertThat(n.has(key)).as(path + ": golden fixture lacks " + key).isTrue();
                JsonNode v = n.get(key);
                if (list) {
                    assertThat(v.isArray()).as(path + ": " + key + " is a list").isTrue();
                    v.forEach(next::add);
                } else {
                    next.add(v);
                }
            }
            cur = next;
        }
        return cur;
    }

    /** Every object key in the status, except the open maps the page renders whole (metrics, keys). */
    private static void collectKeys(JsonNode n, Set<String> out) {
        if (n.isArray()) n.forEach(c -> collectKeys(c, out));
        if (!n.isObject()) return;
        for (Iterator<String> it = n.fieldNames(); it.hasNext(); ) {
            String k = it.next();
            out.add(k);
            if (!k.equals("metrics") && !k.equals("keys")) collectKeys(n.get(k), out);
        }
    }

    private static boolean nodeAvailable() {
        try {
            Process p = new ProcessBuilder("node", "--version").redirectErrorStream(true).start();
            return p.waitFor(20, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static final String RENDER_SCRIPT = """
            const C = require(process.argv[2]);
            const st = JSON.parse(require('fs').readFileSync(process.argv[3], 'utf8'));
            const v = C.toView(st);
            const all = v.diagram.writers.concat(v.diagram.readers);
            process.stdout.write(JSON.stringify({
              view: v,
              html: C.renderPage(v, 1254),
              htmlNarrow: C.renderPage(v, 394),
              layoutWide: C.diagramLayout(v, 1280),
              layoutNarrow: C.diagramLayout(v, 420),
              grouped: C.groupBoxes(v.diagram.readers.concat(v.diagram.readers.map(b =>
                Object.assign({}, b, { names: [b.names[0] + 'x'] }))), 12),
            }));
            """;

    /** Runs cluster.js on a status in node; skipped where node is not on PATH. */
    private static JsonNode render(Path tmp, String statusJson) throws Exception {
        assumeTrue(nodeAvailable(), "node not on PATH: cluster.js rendering not checked");
        Path script = tmp.resolve("render.js");
        Path status = tmp.resolve("status.json");
        Files.writeString(script, RENDER_SCRIPT);
        Files.writeString(status, statusJson);
        Process p = new ProcessBuilder("node", script.toString(), CLUSTER_JS.toAbsolutePath().toString(),
                status.toString()).redirectError(tmp.resolve("stderr.txt").toFile()).start();
        byte[] stdout = p.getInputStream().readAllBytes();
        assertThat(p.waitFor(60, TimeUnit.SECONDS)).isTrue();
        String err = Files.readString(tmp.resolve("stderr.txt"));
        assertThat(p.exitValue()).as("node failed: " + err).isZero();
        assertThat(err).as("no script errors or warnings").isEmpty();
        return JSON.readTree(stdout);
    }

    /** The page as XML: every tag closed and every attribute quoted, or this throws. */
    private static Document xml(String html) throws Exception {
        var f = javax.xml.parsers.DocumentBuilderFactory.newInstance();
        f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        return f.newDocumentBuilder().parse(new ByteArrayInputStream(
                ("<div>" + html + "</div>").getBytes(StandardCharsets.UTF_8)));
    }

    private static Element byId(Document doc, String id) {
        NodeList all = doc.getElementsByTagName("*");
        for (int i = 0; i < all.getLength(); i++) {
            Element e = (Element) all.item(i);
            if (id.equals(e.getAttribute("id"))) return e;
        }
        throw new AssertionError("no element #" + id);
    }

    private static int count(Element root, String tag, String cls) {
        NodeList l = root.getElementsByTagName(tag);
        int n = 0;
        for (int i = 0; i < l.getLength(); i++) {
            String c = ((Element) l.item(i)).getAttribute("class");
            if ((" " + c + " ").contains(" " + cls + " ")) n++;
        }
        return n;
    }

    private static List<String> names(JsonNode list, String field) {
        List<String> out = new ArrayList<>();
        list.forEach(n -> out.add(field == null ? n.asText() : n.path(field).asText()));
        return out;
    }

    private static List<String> boxes(JsonNode view, String column) {
        List<String> out = new ArrayList<>();
        for (JsonNode b : view.path("diagram").path(column)) {
            Set<String> chips = new LinkedHashSet<>(names(b.path("chips"), "shard"));
            out.add(b.path("names").get(0).asText() + ":" + String.join(",", chips));
        }
        return out;
    }

    /** Within the width, no two boxes overlap, and every chip sits inside its box. */
    private static void assertLegible(JsonNode layout, int width) {
        assertThat(layout.path("width").asInt()).isLessThanOrEqualTo(width);
        List<JsonNode> boxes = new ArrayList<>();
        layout.path("boxes").forEach(boxes::add);
        for (JsonNode b : boxes) {
            double x = b.path("x").asDouble(), y = b.path("y").asDouble();
            double w = b.path("w").asDouble(), h = b.path("h").asDouble();
            assertThat(x).isGreaterThanOrEqualTo(0);
            assertThat(x + w).as("box inside the diagram").isLessThanOrEqualTo(width);
            assertThat(y + h).isLessThanOrEqualTo(layout.path("height").asDouble());
            assertThat(w).as("box wide enough to read").isGreaterThanOrEqualTo(150);
            for (JsonNode c : b.path("flow").path("pos")) {
                assertThat(c.path("x").asDouble() + c.path("w").asDouble()).isLessThanOrEqualTo(w - 16 + 0.001);
            }
        }
        for (int i = 0; i < boxes.size(); i++) {
            for (int j = i + 1; j < boxes.size(); j++) {
                JsonNode a = boxes.get(i), b = boxes.get(j);
                boolean apart = a.path("x").asDouble() + a.path("w").asDouble() <= b.path("x").asDouble()
                        || b.path("x").asDouble() + b.path("w").asDouble() <= a.path("x").asDouble()
                        || a.path("y").asDouble() + a.path("h").asDouble() <= b.path("y").asDouble()
                        || b.path("y").asDouble() + b.path("h").asDouble() <= a.path("y").asDouble();
                assertThat(apart).as("boxes %d and %d overlap", i, j).isTrue();
            }
        }
    }
}
