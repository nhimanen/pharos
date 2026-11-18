#!/usr/bin/env python3
"""Phase 3 gate driver (standard library only). Driven by scripts/cluster-gate-phase3.sh, which has
already started MinIO on 127.0.0.1:9000, published lucene (--no-embed) and pharos (embedded), and
built the single-node reference home ($GATE/reference) from a copy of the writer's local indexes.

Starts and stops every JVM itself, by PID (Popen), one at a time, checking MemAvailable:

  reference   the single-node reference (role all, :7305) answers the gate queries (see specs()):
              the 99 pharos + lucene benchmark queries, keyword with NO project filter, hybrid,
              hybrid-reranked and vector scoped to pharos (limit 10). Saved to $GATE/ref-results.json; stopped.
  ranking     R1 {pharos} :7301, R2 {lucene} :7302, R3 (all) :7303 (lazy readers). The same queries
              on R1, R2 and R3. Keyword unscoped on R1 (fan-out: lucene is on R2/R3); the vector
              family on R2 (fan-out: pharos is on R1/R3); everything on R3 (holds all: local path).
                keyword          top-10 identical (ids and order) on >= 98% of queries
                hybrid, hybrid-reranked   mean Kendall tau >= 0.95 and top-1 identical on >= 95%
                                 (scoped to pharos and coordinated by R2 {lucene}: the single node
                                 refuses unscoped vector queries, lucene having no vectors)
                                 (tau over the ids both top-10s share, as DistributedSearchTest)
                vector           reported under the Phase 2 ulp rule (4 ulp / 1e-6 relative,
                                 id swaps only within near-ties)
                R3 vs reference  identical (bitwise: ids, order, score bits, every field)
              The readers stay up for the elasticity phase.
  elasticity  router :7080 in front. ~QPS requests/s for LOAD seconds (open loop, a thread pool),
              cycling the 99 queries: half unscoped keyword, a quarter keyword scoped to the query's
              own project, a quarter hybrid scoped to pharos. At JOIN_AT s R4 (all, :7304) starts; at DRAIN_AT s R2 gets
              SIGTERM (R3 still covers lucene). Pass: 0 failed requests (non-200, error, timeout) and 0
              responses whose warnings (body or X-Pharos-Warnings) mention "unavailable".
              Latency p50/p95 before (< JOIN_AT), during (JOIN_AT .. R2 exit), after (R2 exit .. end).

--self-test checks the statistics on canned inputs; --dry-run prints the plan.
"""
import argparse
import concurrent.futures
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.dont_write_bytecode = True
import cluster_gate  # noqa: E402  (Phase 1: load_queries)
import cluster_gate_phase2 as p2  # noqa: E402  (Phase 2: statistics, comparisons, Reader)

LIMIT = 10
PIPELINES = ("keyword", "hybrid", "hybrid-reranked", "vector")
KEYWORD_IDENTICAL_MIN = 0.98
TAU_MIN = 0.95
TOP1_MIN = 0.95
PORTS = {"reference": 7305, "r1": 7301, "r2": 7302, "r3": 7303, "r4": 7304, "router": 7080}
SHARDS = {"r1": ["pharos"], "r2": ["lucene"], "r3": [], "r4": []}

# --------------------------------------------------------------------------- statistics


def kendall_tau(x, y):
    """Kendall tau over the ids both lists share (DistributedSearchTest.kendallTau): 1.0 under 2 shared."""
    common = [i for i in x if i in y]
    if len(common) < 2:
        return 1.0
    px = {v: k for k, v in enumerate(x)}
    py = {v: k for k, v in enumerate(y)}
    conc = disc = 0
    for i in range(len(common)):
        for j in range(i + 1, len(common)):
            dx = px[common[i]] - px[common[j]]
            dy = py[common[i]] - py[common[j]]
            if dx * dy > 0:
                conc += 1
            elif dx * dy < 0:
                disc += 1
    return (conc - disc) / (conc + disc)


def kendall_tau_absent(x, y, depth=LIMIT):
    """Stricter companion (reported only): tau-b over the union, an id missing from a list ranked
    just past its end (tied), so membership differences count too."""
    union = list(dict.fromkeys(list(x) + list(y)))
    if len(union) < 2:
        return 1.0
    rx = {v: (x.index(v) if v in x else depth) for v in union}
    ry = {v: (y.index(v) if v in y else depth) for v in union}
    conc = disc = tx = ty = 0
    for i in range(len(union)):
        for j in range(i + 1, len(union)):
            a, b = union[i], union[j]
            dx, dy = rx[a] - rx[b], ry[a] - ry[b]
            if dx == 0 and dy == 0:
                continue
            if dx == 0:
                tx += 1
            elif dy == 0:
                ty += 1
            elif dx * dy > 0:
                conc += 1
            else:
                disc += 1
    denom = ((conc + disc + tx) * (conc + disc + ty)) ** 0.5
    return 1.0 if denom == 0 else (conc - disc) / denom


def ids(results):
    return [r.get("id") for r in results]


def compare_rank(ref, got, pipeline):
    """Per-pipeline comparison of two result maps {query: results}. Returns a stats dict."""
    st = {"pipeline": pipeline, "queries": 0, "top10_identical": 0, "top1_identical": 0,
          "score_bitwise_identical": 0, "taus": [], "taus_absent": [], "overlap": [], "diffs": [],
          "empty_ref": 0, "empty_got": 0}
    if pipeline == "vector":
        st.update({"ulp_identical": 0, "ulp_tolerant": 0, "ulp_failures": [], "max_ulps": 0, "swaps": 0})
    for q, a in ref.items():
        b = got[q]
        ia, ib = ids(a), ids(b)
        st["queries"] += 1
        st["empty_ref"] += not a
        st["empty_got"] += not b
        if ia == ib:
            st["top10_identical"] += 1
        else:
            st["diffs"].append({"q": q, "ref": ia, "got": ib})
        if ia[:1] == ib[:1]:
            st["top1_identical"] += 1
        if p2.compare_strict(a, b) is None:
            st["score_bitwise_identical"] += 1
        st["taus"].append(kendall_tau(ia, ib))
        st["taus_absent"].append(kendall_tau_absent(ia, ib))
        st["overlap"].append(len(set(ia) & set(ib)) / max(1, len(set(ia) | set(ib))))
        if pipeline == "vector":
            if p2.compare_strict(a, b) is None:
                st["ulp_identical"] += 1
            else:
                why, swaps, mu = p2.compare_tolerant(a, b)
                st["max_ulps"] = max(st["max_ulps"], mu)
                if why is None:
                    st["ulp_tolerant"] += 1
                    st["swaps"] += swaps
                else:
                    st["ulp_failures"].append(f"{q}: {why}")
    n = max(1, st["queries"])
    st["top10_identical_pct"] = 100.0 * st["top10_identical"] / n
    st["top1_identical_pct"] = 100.0 * st["top1_identical"] / n
    st["mean_tau"] = sum(st["taus"]) / n
    st["min_tau"] = min(st["taus"]) if st["taus"] else None
    st["mean_tau_absent"] = sum(st["taus_absent"]) / n
    st["mean_overlap"] = sum(st["overlap"]) / n
    return st


def verdict(st):
    """(pass, rule text) for a pipeline's comparison against the reference."""
    pl = st["pipeline"]
    if pl == "keyword":
        ok = st["top10_identical_pct"] >= 100 * KEYWORD_IDENTICAL_MIN
        return ok, f"top-10 identical {st['top10_identical_pct']:.1f}% (>= {100 * KEYWORD_IDENTICAL_MIN:.0f}%)"
    if pl in ("hybrid", "hybrid-reranked"):
        ok = st["mean_tau"] >= TAU_MIN and st["top1_identical_pct"] >= 100 * TOP1_MIN
        return ok, (f"mean tau {st['mean_tau']:.4f} (>= {TAU_MIN}), top-1 identical {st['top1_identical_pct']:.1f}%"
                    f" (>= {100 * TOP1_MIN:.0f}%)")
    ok = not st["ulp_failures"]
    return ok, (f"ulp rule: identical {st['ulp_identical']}, within tolerance {st['ulp_tolerant']}"
                f" (max {st['max_ulps']} ulp, {st['swaps']} near-tie swap groups), failures {len(st['ulp_failures'])}")


def windows(samples, join_at, gone_at, end):
    """Latency summaries per window. samples: (t_start, latency_s, ok)."""
    def summ(lo, hi):
        lat = [d for t, d, _ in samples if lo <= t < hi]
        if not lat:
            return {"n": 0}
        s = p2.summary(lat)
        return {"n": s["n"], "p50_ms": 1000 * s["p50"], "p95_ms": 1000 * s["p95"], "max_ms": 1000 * s["max"],
                "range_s": [round(lo, 1), round(min(hi, end), 1)]}
    out = {"before": summ(0, join_at), "during": summ(join_at, gone_at), "after": summ(gone_at, float("inf"))}
    for k, (lo, hi) in {"min0-60": (0, 60), "min60-120": (60, 120), "min120-180": (120, float("inf"))}.items():
        out[k] = summ(lo, hi)
    return out


def has_unavailable(warnings):
    return [w for w in warnings if "unavailable" in w.lower()]


# --------------------------------------------------------------------------- HTTP


def fetch(base, q, pipeline, project=None, limit=LIMIT, timeout=120):
    """(seconds, status, results, warnings). Never raises for HTTP/connection errors (status -1)."""
    params = {"q": q, "pipeline": pipeline, "limit": limit}
    if project:
        params["project"] = project
    url = f"{base}/api/search?" + urllib.parse.urlencode(params)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            body, status, hdr = r.read(), r.status, r.headers.get("X-Pharos-Warnings")
    except urllib.error.HTTPError as e:
        body, status, hdr = e.read(), e.code, e.headers.get("X-Pharos-Warnings")
    except Exception as e:  # connection refused, reset, timeout
        return time.perf_counter() - t0, -1, [], [f"client error: {e!r}"]
    dt = time.perf_counter() - t0
    warnings = []
    if hdr:
        try:
            warnings += list(json.loads(hdr))
        except ValueError:
            warnings.append(hdr)
    results = []
    try:
        data = json.loads(body)
    except ValueError:
        data = {"error": body[:200].decode(errors="replace")}
    if isinstance(data, list):
        results = data
    elif isinstance(data, dict):
        results = data.get("results", [])
        warnings += [w for w in data.get("warnings", []) or [] if w not in warnings]
        if "results" not in data and status == 200:
            status = -2
    return dt, status, results, warnings


def get_json(url, timeout=10):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


# --------------------------------------------------------------------------- nodes


class Node(p2.Reader):
    """A reader (role reader, pinned shards), the reference (role all, no cluster) or the router."""

    def __init__(self, name, kind="reader", shards=None, fresh=False):
        super().__init__(name, PORTS[name], fresh=fresh)
        self.kind = kind
        self.shards = shards or []
        if kind == "reference":
            self.home = Path(os.environ["GATE"]) / "reference"
            self.ph = self.home / ".pharos"
            self.cache_dir = self.ph / "cluster-cache"

    def write_config(self):
        self.ph.mkdir(parents=True, exist_ok=True)
        link = self.home / ".djl.ai"
        if not link.is_symlink():
            link.symlink_to(os.environ["GATE_MODELS"])
        cfg = {
            "embeddingProviders": [{"type": "djl", "modelId": os.environ["GATE_MODEL_ID"],
                                    "url": os.environ["GATE_MODEL_URL"], "dimensions": 768, "maxTokens": 512}],
            "searchEmbeddingModel": os.environ["GATE_MODEL_ID"], "crossEncoderEnabled": True,
        }
        if self.kind != "reference":
            cfg["cluster"] = {
                "role": "reader", "backend": "object-store", "nodeId": "gate-" + self.name,
                "pollIntervalSeconds": 10, "cacheDir": str(self.cache_dir),
                "advertiseUrl": f"http://127.0.0.1:{self.port}", "token": os.environ["GATE_TOKEN"],
                "reader": {"mode": "lazy", "shards": self.shards},
                "objectStore": {
                    "endpoint": "http://127.0.0.1:9000", "region": "us-east-1", "bucket": "pharos-gate",
                    "prefix": "pharos", "pathStyle": True,
                    "accessKeyEnv": "PHAROS_GATE_ACCESS_KEY", "secretKeyEnv": "PHAROS_GATE_SECRET_KEY"},
            }
        (self.ph / "config.json").write_text(json.dumps(cfg, indent=2))

    def command(self):
        heap = os.environ.get("GATE_REF_HEAP", "2g") if self.kind == "reference" else os.environ.get(
            "GATE_WEB_HEAP", "1g")
        cmd = super().command()
        cmd[1] = "-Xmx" + heap
        if self.kind == "router":
            cmd = cmd[:cmd.index("-jar") + 2] + ["router", "--port", str(self.port)]
        return cmd

    def wait_up(self, timeout=600):
        """Seconds from JVM start until ready: /health 200 with ready (reader: 'serving N shard(s)'
        logged with the expected N); the router: it lists `want` healthy readers (checked elsewhere)."""
        end = time.perf_counter() + timeout
        want = {"r1": 1, "r2": 1}.get(self.name, 2)
        pat = re.compile(r"serving (\d+) shard\(s\)")
        while time.perf_counter() < end:
            if self.proc.poll() is not None:
                raise RuntimeError(f"{self.name} exited {self.proc.returncode}; see {self.log}")
            try:
                if self.kind == "router":  # up once it answers at all (503 until a reader is healthy)
                    try:
                        p2.health(self.base)
                    except urllib.error.HTTPError:
                        pass
                    return time.perf_counter() - self.t_start
                else:
                    h = p2.health(self.base)
                    if h.get("ready") or h.get("status") == "ok":
                        if self.kind == "reader":
                            m = pat.search(self.log.read_text(errors="replace"))
                            if not m:
                                raise RuntimeError("ready without the serving line")
                            if int(m.group(1)) != want:
                                raise RuntimeError(f"{self.name} serves {m.group(1)} shard(s), not {want}")
                        return time.perf_counter() - self.t_start
            except (OSError, ValueError):
                pass
            time.sleep(0.2)
        raise RuntimeError(f"{self.name} not up after {timeout}s; see {self.log}")

    def rss_mib(self):
        try:
            for ln in open(f"/proc/{self.proc.pid}/status"):
                if ln.startswith("VmRSS"):
                    return int(ln.split()[1]) // 1024
        except OSError:
            pass
        return None


def start(node, min_mem_gib=1.5):
    m = p2.mem_available_gib()
    print(f"-- starting {node.name} :{node.port} (MemAvailable {m:.1f} GiB)")
    if m < min_mem_gib:
        raise RuntimeError(f"MemAvailable {m:.1f} GiB < {min_mem_gib} GiB: not starting {node.name}")
    node.start()
    t = node.wait_up()
    print(f"   {node.name} up in {t:.1f}s, pid {node.proc.pid}, RSS {node.rss_mib()} MiB;"
          f" MemAvailable {p2.mem_available_gib():.1f} GiB")
    return t


def wait_cluster(base, nodes, timeout=120):
    """Until `base`'s /cluster/readers lists every node in `nodes` as ready."""
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            data = get_json(base + "/cluster/readers")
            rs = data.get("readers", data) if isinstance(data, dict) else data
            states = {r.get("node"): r.get("state") for r in rs}
            last = states
            if all(states.get("gate-" + n) == "ready" for n in nodes):
                return states
        except (OSError, ValueError):
            pass
        time.sleep(1)
    raise RuntimeError(f"{base} never listed {nodes} ready (last {last})")


def gate_queries():
    base = cluster_gate.load_queries()
    return base, list(dict.fromkeys(q for q, _ in base))


def specs():
    """(pipeline, scope, query) of the ranking comparison. keyword: all 99 with no project filter.
    hybrid, hybrid-reranked, vector: all 99 scoped to pharos, because a single node REFUSES an
    unscoped vector-bearing query when one project (lucene, --no-embed) has no vectors of the search
    model (HTTP 500 VectorModelUnavailableException, run 1); gated on the 45 pharos-origin queries."""
    base, qs = gate_queries()
    out = [("keyword", None, q) for q in qs]
    out += [(pl, "pharos", q) for pl in ("hybrid", "hybrid-reranked", "vector") for q in qs]
    return out


def key(pl, scope, q):
    return f"{pl}|{scope or '*'}|{q}"


def run_queries(base_url, sp):
    """{key: results}, warnings {text: count}, failures."""
    res, warns, fails = {}, {}, []
    for pl, scope, q in sp:
        _, status, results, w = fetch(base_url, q, pl, scope)
        k = key(pl, scope, q)
        if status != 200:
            fails.append(f"{k}: status {status} {w}")
        res[k] = results
        for x in w:
            warns[x[:160]] = warns.get(x[:160], 0) + 1
    return res, warns, fails


def by_pipeline(res, pl, subset=None):
    """{query key: results} of one pipeline (optionally only the queries in `subset`)."""
    return {k: v for k, v in res.items() if k.split("|", 1)[0] == pl
            and (subset is None or k.split("|", 2)[2] in subset)}


def save(name, data):
    p = Path(os.environ["GATE"]) / f"results-{name}.json"
    p.write_text(json.dumps(data, indent=2))
    print(f"   results -> {p}")


# --------------------------------------------------------------------------- phases


def phase_reference():
    sp = specs()
    ref = Node("reference", kind="reference")
    start(ref, min_mem_gib=2.5)
    try:
        run_queries(ref.base, sp)  # warm-up pass (models, caches)
        res, warns, fails = run_queries(ref.base, sp)
    finally:
        ref.stop()
    (Path(os.environ["GATE"]) / "ref-results.json").write_text(json.dumps(res))
    print(f"REFERENCE {len(sp)} (pipeline, scope, query) answers; failures {len(fails)}; warnings {warns}")
    for f in fails[:10]:
        print("   " + f)
    save("reference", {"answers": len(sp), "warnings": warns, "failures": fails,
                       "non_empty": {pl: sum(1 for v in by_pipeline(res, pl).values() if v) for pl in PIPELINES}})
    return not fails


NODES = {}


def report_cmp(label, ref, res, pl, gated_subset, rule_kind):
    """Compares one pipeline; rule_kind 'brief' (the Phase 3 criteria) or 'bitwise' (local path)."""
    st = compare_rank(by_pipeline(ref, pl, gated_subset), by_pipeline(res, pl, gated_subset), pl)
    if rule_kind == "brief":
        good, rule = verdict(st)
    else:
        good = st["score_bitwise_identical"] == st["queries"]
        rule = f"bitwise identical {st['score_bitwise_identical']}/{st['queries']}"
    print(f"   {label:34s} {pl:16s} n={st['queries']:3d} {rule}; ids identical {st['top10_identical']},"
          f" score-bitwise {st['score_bitwise_identical']}, top-1 {st['top1_identical_pct']:.1f}%,"
          f" tau mean {st['mean_tau']:.4f} min {st['min_tau']:.3f} (tau-b w/ absent {st['mean_tau_absent']:.4f}),"
          f" overlap {st['mean_overlap']:.3f}, empty ref/got {st['empty_ref']}/{st['empty_got']}"
          f" -> {'PASS' if good else 'FAIL'}")
    for d in st["diffs"][:4]:
        print(f"      diff {d['q']!r}: ref {d['ref'][:3]} got {d['got'][:3]}")
    if pl == "vector" and st["ulp_failures"]:
        for f in st["ulp_failures"][:4]:
            print("      ulp: " + f)
    st["pass"], st["rule"] = good, rule
    st.pop("taus_absent")
    return good, st


def phase_ranking():
    base, qs = gate_queries()
    pharos_q = {q for q, p in base if p == "pharos"}
    sp = specs()
    ref = json.loads((Path(os.environ["GATE"]) / "ref-results.json").read_text())
    for name in ("r1", "r2", "r3"):
        NODES[name] = Node(name, shards=SHARDS[name], fresh=True)
        start(NODES[name])
    wait_cluster(NODES["r1"].base, ["r1", "r2", "r3"])
    out = {"answers": len(sp), "pharos_queries": len(pharos_q), "nodes": {}, "checks": []}
    ok = True
    got = {}
    for name in ("r1", "r2", "r3"):
        run_queries(NODES[name].base, sp)  # warm-up pass
        res, warns, fails = run_queries(NODES[name].base, sp)
        got[name] = res
        (Path(os.environ["GATE"]) / f"{name}-results.json").write_text(json.dumps(res))
        out["nodes"][name] = {"warnings": warns, "failures": fails}
        print(f"RANKING {name}: failures {len(fails)}; warnings {warns}")
        for f in fails[:10]:
            print("   " + f)
        ok = ok and not fails
    # (label, node, pipeline, subset, rule, gated)
    checks = [("R1 {pharos} unscoped (fan-out)", "r1", "keyword", None, "brief", True)]
    for pl in ("hybrid", "hybrid-reranked", "vector"):
        checks.append(("R2 {lucene} -> pharos (fan-out)", "r2", pl, pharos_q, "brief", True))
    for pl in ("hybrid", "hybrid-reranked", "vector"):
        checks.append(("R2 -> pharos, all 99 texts", "r2", pl, None, "brief", False))
        checks.append(("R1 pharos-scoped (local)", "r1", pl, None, "bitwise", False))
    checks.append(("R2 {lucene} unscoped keyword (fan-out)", "r2", "keyword", None, "brief", False))
    for pl in PIPELINES:
        checks.append(("R3 (all) local", "r3", pl, None, "bitwise", True))
    for label, node, pl, subset, rule, gated in checks:
        good, st = report_cmp(label + ("" if gated else " [info]"), ref, got[node], pl, subset, rule)
        st.update({"label": label, "node": node, "gated": gated})
        out["checks"].append(st)
        if gated:
            ok = ok and good
    # Unscoped vector-bearing queries on R1: the single node refuses them; recorded, not compared.
    unscoped = {}
    for pl in ("hybrid", "vector"):
        n_ok = n_hits = 0
        warns = {}
        for q in qs:
            _, status, results, w = fetch(NODES["r1"].base, q, pl)
            n_ok += status == 200
            n_hits += bool(results)
            for x in w:
                warns[x[:160]] = warns.get(x[:160], 0) + 1
        unscoped[pl] = {"ok": n_ok, "non_empty": n_hits, "warnings": warns}
        print(f"   R1 unscoped {pl} [not comparable: the single node answers 500]: {n_ok}/{len(qs)} answered 200,"
              f" {n_hits} non-empty; warnings {warns}")
    out["r1_unscoped_vector_family"] = unscoped
    out["rss_mib"] = {n: NODES[n].rss_mib() for n in NODES}
    print(f"   RSS MiB {out['rss_mib']}; MemAvailable {p2.mem_available_gib():.1f} GiB")
    out["pass"] = ok
    save("ranking", out)
    return ok


def phase_elasticity(load_s, qps, join_at, drain_at):
    base_q, _ = gate_queries()
    for name in ("r1", "r2", "r3"):  # elasticity alone (re-run) starts them here
        if name not in NODES or NODES[name].proc is None:
            NODES[name] = Node(name, shards=SHARDS[name], fresh=False)
            start(NODES[name])
    wait_cluster(NODES["r1"].base, ["r1", "r2", "r3"])
    router = Node("router", kind="router", fresh=True)
    NODES["router"] = router
    start(router, min_mem_gib=1.0)
    end = time.time() + 30
    while time.time() < end:
        try:
            if p2.health(router.base).get("status") in ("ok", "degraded"):
                break
        except (OSError, ValueError):
            pass
        time.sleep(0.5)
    time.sleep(2.5)  # one router poll after all three are healthy
    r4 = Node("r4", shards=SHARDS["r4"], fresh=True)
    NODES["r4"] = r4

    # schedule: request i at t = i / qps
    n = int(load_s * qps)
    plan = []
    for i in range(n):
        q, proj = base_q[i % len(base_q)]
        # half unscoped keyword; a quarter keyword scoped to the query's own project; a quarter
        # hybrid scoped to pharos (unscoped hybrid is refused on a node holding both, as single node)
        kind = i % 4
        if kind in (0, 2):
            plan.append((i / qps, q, None, "keyword"))
        elif kind == 1:
            plan.append((i / qps, q, proj, "keyword"))
        else:
            plan.append((i / qps, q, "pharos", "hybrid"))
    samples, events = [], {}
    lock = threading.Lock()
    t0 = time.perf_counter()

    def one(item):
        at, q, proj, pl = item
        delay = at - (time.perf_counter() - t0)
        if delay > 0:
            time.sleep(delay)
        ts = time.perf_counter() - t0
        dt, status, results, warns = fetch(router.base, q, pl, proj, timeout=60)
        rec = {"t": ts, "lat": dt, "status": status, "q": q, "project": proj, "pipeline": pl,
               "hits": len(results), "warnings": warns, "unavailable": has_unavailable(warns)}
        with lock:
            samples.append(rec)

    def timeline():
        time.sleep(max(0, join_at - (time.perf_counter() - t0)))
        events["r4_start"] = time.perf_counter() - t0
        print(f"   t={events['r4_start']:.1f}s start R4 (MemAvailable {p2.mem_available_gib():.1f} GiB)")
        r4.start()
        try:
            r4.wait_up()
            events["r4_ready"] = time.perf_counter() - t0
            print(f"   t={events['r4_ready']:.1f}s R4 ready (pid {r4.proc.pid}, RSS {r4.rss_mib()} MiB)")
        except RuntimeError as e:
            events["r4_error"] = str(e)
            print(f"   R4 failed: {e}")
        time.sleep(max(0, drain_at - (time.perf_counter() - t0)))
        r2 = NODES["r2"]
        events["r2_sigterm"] = time.perf_counter() - t0
        print(f"   t={events['r2_sigterm']:.1f}s SIGTERM R2 pid {r2.proc.pid}")
        r2.proc.terminate()  # by PID
        rc = r2.proc.wait(120)
        events["r2_exit"] = time.perf_counter() - t0
        events["r2_exit_code"] = rc
        print(f"   t={events['r2_exit']:.1f}s R2 exited {rc}")

    tl = threading.Thread(target=timeline, daemon=True)
    print(f"-- load: {n} requests at {qps}/s for {load_s}s through the router; MemAvailable"
          f" {p2.mem_available_gib():.1f} GiB")
    tl.start()
    with concurrent.futures.ThreadPoolExecutor(max_workers=48) as ex:
        list(ex.map(one, plan))
    tl.join(150)
    elapsed = time.perf_counter() - t0
    samples.sort(key=lambda r: r["t"])
    failed = [r for r in samples if r["status"] != 200]
    unavailable = [r for r in samples if r["unavailable"]]
    other_warn = {}
    for r in samples:
        for w in r["warnings"]:
            if w not in r["unavailable"]:
                other_warn[w[:160]] = other_warn.get(w[:160], 0) + 1
    gone = events.get("r2_exit", drain_at)
    win = windows([(r["t"], r["lat"], r["status"] == 200) for r in samples], join_at, gone, elapsed)
    achieved = len(samples) / max(1e-9, samples[-1]["t"]) if samples else 0
    try:
        healthy_after = p2.health(router.base)
    except (OSError, ValueError) as e:
        healthy_after = str(e)
    router_log = router.log.read_text(errors="replace")
    out = {"requests": len(samples), "planned": n, "qps_target": qps, "qps_achieved": achieved,
           "elapsed_s": elapsed, "events": events, "failed": len(failed), "unavailable": len(unavailable),
           "failed_samples": failed[:20], "unavailable_samples": unavailable[:20], "other_warnings": other_warn,
           "windows": win, "router_healthy_lines": re.findall(r"Router: healthy readers .*", router_log),
           "router_unreachable_lines": len(re.findall(r"is unreachable", router_log)),
           "by_status": {}, "rss_mib": {k: v.rss_mib() for k, v in NODES.items() if v.proc}}
    for r in samples:
        out["by_status"][str(r["status"])] = out["by_status"].get(str(r["status"]), 0) + 1
    ok = not failed and not unavailable and len(samples) == n
    out["pass"] = ok
    (Path(os.environ["GATE"]) / "elasticity-samples.json").write_text(json.dumps(samples))
    print(f"ELASTICITY {len(samples)}/{n} requests in {elapsed:.1f}s ({achieved:.1f}/s); failed {len(failed)};"
          f" 'unavailable' warnings {len(unavailable)}; status {out['by_status']} -> {'PASS' if ok else 'FAIL'}")
    for k in ("before", "during", "after", "min0-60", "min60-120", "min120-180"):
        w = win[k]
        if w.get("n"):
            print(f"   {k:10s} {w['range_s']} n={w['n']} p50 {w['p50_ms']:.1f} ms p95 {w['p95_ms']:.1f} ms"
                  f" max {w['max_ms']:.1f} ms")
    print(f"   events {events}; other warnings {other_warn}")
    for line in out["router_healthy_lines"]:
        print("   router: " + line)
    for r in (failed + unavailable)[:10]:
        print(f"   bad: t={r['t']:.1f} {r['status']} {r['pipeline']} {r['project']} {r['q']!r} {r['warnings']}")
    save("elasticity", out)
    return ok


def stop_all():
    for name, node in list(NODES.items()):
        node.stop()


# --------------------------------------------------------------------------- self-test


def self_test():
    assert kendall_tau(list("abcde"), list("abcde")) == 1.0
    assert kendall_tau(list("abcde"), list("edcba")) == -1.0
    assert abs(kendall_tau(list("abcd"), list("abdc")) - (5 - 1) / 6) < 1e-12
    assert kendall_tau(list("abcz"), list("abcy")) == 1.0          # membership ignored (shared only)
    assert kendall_tau(list("ab"), list("xy")) == 1.0               # < 2 shared
    assert kendall_tau_absent(list("abc"), list("abc")) == 1.0
    assert kendall_tau_absent(list("abcz"), list("abcy")) < 1.0     # membership counts here
    assert kendall_tau_absent(list("ab"), list("ba")) < 0
    r = lambda i, s=1.0: {"id": i, "score": s}  # noqa: E731
    ref = {"q1": [r("a", 3), r("b", 2), r("c", 1)], "q2": [r("x", 2), r("y", 1)]}
    got = {"q1": [r("a", 3), r("c", 2), r("b", 1)], "q2": [r("x", 2), r("y", 1)]}
    st = compare_rank(ref, got, "hybrid")
    assert st["top10_identical"] == 1 and st["top1_identical"] == 2 and st["score_bitwise_identical"] == 1
    assert abs(st["mean_tau"] - ((1 / 3) + 1) / 2) < 1e-12, st["mean_tau"]
    ok, _ = verdict(st)
    assert not ok  # tau 0.667
    kw = compare_rank({f"q{i}": [r("a")] for i in range(100)},
                      {f"q{i}": [r("a")] if i >= 2 else [r("b")] for i in range(100)}, "keyword")
    assert kw["top10_identical_pct"] == 98.0 and verdict(kw)[0]
    kw = compare_rank({f"q{i}": [r("a")] for i in range(100)},
                      {f"q{i}": [r("a")] if i >= 3 else [r("b")] for i in range(100)}, "keyword")
    assert not verdict(kw)[0]
    import struct
    up = struct.unpack("<f", struct.pack("<i", p2.f32_bits(2.0) + 1))[0]
    vec = compare_rank({"q": [r("a", 3.0), r("b", 2.0)]}, {"q": [r("a", 3.0), r("b", up)]}, "vector")
    assert vec["ulp_tolerant"] == 1 and verdict(vec)[0]
    vec = compare_rank({"q": [r("a", 3.0), r("b", 2.0)]}, {"q": [r("b", 3.0), r("a", 2.0)]}, "vector")
    assert not verdict(vec)[0]
    s = [(t, 0.010 if t < 60 else 0.030 if t < 125 else 0.020, True) for t in range(180)]
    w = windows(s, 60, 125, 180)
    assert w["before"]["n"] == 60 and abs(w["before"]["p95_ms"] - 10) < 1e-9
    assert w["during"]["n"] == 65 and abs(w["during"]["p95_ms"] - 30) < 1e-9
    assert w["after"]["n"] == 55 and abs(w["after"]["p95_ms"] - 20) < 1e-9
    assert has_unavailable(["shard lucene unavailable: x; results exclude it", "other"]) == [
        "shard lucene unavailable: x; results exclude it"]
    assert has_unavailable(["vector search unavailable"]) and not has_unavailable(["shard x has no vectors"])
    print("self-test OK")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--phases", default="reference,ranking,elasticity")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        self_test()
        return 0
    phases = [p.strip() for p in args.phases.split(",") if p.strip()]
    unknown = set(phases) - {"reference", "ranking", "elasticity"}
    if unknown:
        ap.error(f"unknown phases {sorted(unknown)}")
    load_s = int(os.environ.get("GATE_LOAD_SECONDS", "180"))
    qps = float(os.environ.get("GATE_QPS", "20"))
    join_at = float(os.environ.get("GATE_JOIN_AT", "60"))
    drain_at = float(os.environ.get("GATE_DRAIN_AT", "120"))
    base, qs = gate_queries()
    if args.dry_run:
        print(f"driver (dry run): phases {phases}; {len(base)} benchmark queries ({len(qs)} distinct) x"
              f" -> {len(specs())} ranking answers per node; elasticity {qps}/s for {load_s}s, R4 at"
              f" {join_at}s, SIGTERM R2 at {drain_at}s")
        return 0
    if len(base) != 99:
        print(f"WARNING: {len(base)} pharos+lucene queries, expected 99")
    results = {}
    try:
        for p in phases:
            print(f"== phase {p} (MemAvailable {p2.mem_available_gib():.1f} GiB)")
            if p == "reference":
                results[p] = phase_reference()
            elif p == "ranking":
                results[p] = phase_ranking()
            else:
                results[p] = phase_elasticity(load_s, qps, join_at, drain_at)
    finally:
        stop_all()
    verdict_all = all(results.values())
    print("GATE " + ("PASSED" if verdict_all else "FAILED") + ": "
          + ", ".join(f"{p} {'pass' if ok else 'FAIL'}" for p, ok in results.items()))
    return 0 if verdict_all else 1


if __name__ == "__main__":
    sys.exit(main())
