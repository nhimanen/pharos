#!/usr/bin/env python3
"""Phase 2 gate driver (standard library only). Driven by scripts/cluster-gate-phase2.sh, which has
already started MinIO on 127.0.0.1:9000 and published lucene (--no-embed) and pharos (embedded).

This script starts the reader JVMs itself, one configuration at a time, and stops them by PID:

  cold  N trials. Each trial deletes the reader's home (block cache, generation dirs, registry),
        starts a lazy reader, waits until it is ready (/health 200, which a reader answers only once
        its first sync is served AND its search models have loaded; the "serving 2 shard(s)" line
        is logged at that moment), then times the first answered query: one lucene BM25 query (the
        trial's own, from 20 distinct lucene queries of eval/benchmark.py), then one vector query on
        pharos. Pass: lucene first-query p95 < 3 s (vector reported only). Start->ready is reported.
  warm  A lazy reader (warm cache) and a full-mode reader (MMap, production) run side by side; the
        99 pharos+lucene benchmark queries (keyword) run 3 times per side, interleaved query by query. Pass: lazy
        p95 <= 1.10 x full p95. Then top-10 checks, keyword on all 99 plus vector and hybrid on the
        pharos queries: against the production full copy (default MMapDirectory) every result (ids,
        order, score bits, stored fields) is identical; against the same copy re-opened with
        NIOFSDirectory (-Dpharos.lucene.directory=niofs), non-vector results are identical and
        vector/hybrid scores agree within 4 ulp (float32) or 1e-6 relative, with id swaps only among
        such near-ties (Lucene scores vectors on mmap and heap inputs in different evaluation orders).
  cap   cacheMaxBytes = 50% of the lucene shard's Lucene bytes, rounded down to a block multiple
        (the block size is halved from the default until the cap holds >= 4 blocks, as config
        validation requires); all queries run twice while /health bytesOnDisk is sampled every
        second. Pass: max sample <= cacheMaxBytes + maxConcurrentFetches x blockSizeBytes, and
        top-10 identical to full mode under the warm rules. The hit rate is recorded.

Results go to $GATE/results-<phase>.json and stdout. --self-test checks the statistics and the
comparison rules against canned inputs; --dry-run prints the plan.
"""
import argparse
import json
import math
import os
import re
import shutil
import struct
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
import cluster_gate  # noqa: E402  (Phase 1 helpers: load_queries)

COLD_P95_LIMIT_S = 3.0
WARM_RATIO_LIMIT = 1.10
ULP_TOL = 4
REL_TOL = 1e-6
DEFAULT_BLOCK = 8 << 20
DEFAULT_FETCHES = 16
LIMIT = 10

# --------------------------------------------------------------------------- statistics


def percentile(values, p):
    """Nearest-rank percentile (p in (0, 100]): the smallest value with at least p% at or below it."""
    if not values:
        raise ValueError("percentile of no values")
    s = sorted(values)
    k = max(1, math.ceil(p / 100.0 * len(s)))
    return s[k - 1]


def summary(values):
    return {"n": len(values), "p50": percentile(values, 50), "p95": percentile(values, 95),
            "max": max(values), "mean": sum(values) / len(values)}


def f32_bits(x):
    return struct.unpack("<i", struct.pack("<f", x))[0]


def ulps(a, b):
    """Distance in float32 ulps (scores are Java floats; the JSON repr round-trips to them)."""
    ia, ib = f32_bits(a), f32_bits(b)
    if ia < 0:
        ia = -2**31 - ia  # map negative floats onto a monotonic integer line
    if ib < 0:
        ib = -2**31 - ib
    return abs(ia - ib)


def near(a, b):
    if a == b:
        return True
    return ulps(a, b) <= ULP_TOL or abs(a - b) <= REL_TOL * max(abs(a), abs(b))


def cache_cap(lucene_bytes, block=DEFAULT_BLOCK, min_block=64 << 10):
    """(cacheMaxBytes, blockSizeBytes): 50% of the shard rounded down to a block multiple, with the
    block halved until the cap holds at least 4 blocks."""
    while block >= min_block:
        cap = (lucene_bytes // 2) // block * block
        if cap >= 4 * block:
            return cap, block
        block //= 2
    raise ValueError(f"lucene shard of {lucene_bytes} bytes is too small for a 4-block cap")


# --------------------------------------------------------------------------- comparisons


def strip_score(r):
    return {k: v for k, v in r.items() if k != "score"}


def compare_strict(a, b):
    """Bitwise: same ids, order, scores (float32 bits) and every other field. Returns None or a reason."""
    if len(a) != len(b):
        return f"length {len(a)} vs {len(b)}"
    for i, (x, y) in enumerate(zip(a, b)):
        if x.get("id") != y.get("id"):
            return f"rank {i + 1}: id {x.get('id')} vs {y.get('id')}"
        if f32_bits(x.get("score", 0.0)) != f32_bits(y.get("score", 0.0)):
            return f"rank {i + 1} {x.get('id')}: score {x.get('score')!r} vs {y.get('score')!r}"
        if x != y:
            diff = sorted(k for k in set(x) | set(y) if x.get(k) != y.get(k))
            return f"rank {i + 1} {x.get('id')}: fields differ {diff}"
    return None


def compare_tolerant(a, b, limit=LIMIT):
    """Float-tolerance rule (NIOFS copy). Returns (reason or None, swaps, max_ulps): scores near() rank by rank; ids may differ
    only within groups of consecutive near-tied scores (the group touching the last rank of a full
    page may also exchange ids with ranks past the page); shared ids carry identical other fields."""
    if len(a) != len(b):
        return f"length {len(a)} vs {len(b)}", 0, 0
    max_u = 0
    for i, (x, y) in enumerate(zip(a, b)):
        sx, sy = x.get("score", 0.0), y.get("score", 0.0)
        max_u = max(max_u, ulps(sx, sy))
        if not near(sx, sy):
            return f"rank {i + 1}: score {sx!r} vs {sy!r} ({ulps(sx, sy)} ulp)", 0, max_u
    swaps = 0
    i, n = 0, len(a)
    while i < n:
        j = i
        while j + 1 < n and near(b[j]["score"], b[j + 1]["score"]):
            j += 1
        ga = [r.get("id") for r in a[i:j + 1]]
        gb = [r.get("id") for r in b[i:j + 1]]
        if ga != gb:
            boundary = j == n - 1 and n == limit
            if sorted(ga) != sorted(gb) and not boundary:
                return f"ranks {i + 1}-{j + 1}: ids {ga} vs {gb} outside a near-tie group", swaps, max_u
            swaps += 1
        i = j + 1
    by_id = {r.get("id"): r for r in b}
    for r in a:
        o = by_id.get(r.get("id"))
        if o is not None and strip_score(r) != strip_score(o):
            diff = sorted(k for k in set(r) | set(o) if k != "score" and r.get(k) != o.get(k))
            return f"{r.get('id')}: fields differ {diff}", swaps, max_u
    return None, swaps, max_u


# --------------------------------------------------------------------------- queries


def queries():
    """(query, project, pipeline) for the gate: keyword on the 99 pharos+lucene benchmark queries,
    plus vector and hybrid on the pharos ones (lucene is indexed without embeddings)."""
    base = cluster_gate.load_queries()
    out = [(q, p, "keyword") for q, p in base]
    out += [(q, p, pl) for pl in ("vector", "hybrid") for q, p in base if p == "pharos"]
    return base, out


def search(base, q, project, pipeline, timeout=120):
    url = f"{base}/api/search?" + urllib.parse.urlencode(
        {"q": q, "pipeline": pipeline, "limit": LIMIT, "project": project})
    t0 = time.perf_counter()
    with urllib.request.urlopen(url, timeout=timeout) as r:
        body = r.read()
    dt = time.perf_counter() - t0
    data = json.loads(body)
    if isinstance(data, dict):
        if "results" not in data:
            raise RuntimeError(f"unexpected answer for {q!r}: {body[:300]!r}")
        data = data["results"]
    return dt, data


def health(base):
    with urllib.request.urlopen(base + "/health", timeout=10) as r:
        return json.loads(r.read())


# --------------------------------------------------------------------------- readers


class Reader:
    """One reader web JVM with its own user.home / pharos.home under $GATE/readers/<name>."""

    def __init__(self, name, port, mode="lazy", reader_cfg=None, niofs=False, fresh=False):
        self.name, self.port, self.mode = name, port, mode
        self.base = f"http://127.0.0.1:{port}"
        self.home = Path(os.environ["GATE"]) / "readers" / name
        self.ph = self.home / ".pharos"
        self.cache_dir = self.ph / "cluster-cache"
        self.niofs = niofs
        self.reader_cfg = dict(reader_cfg or {})
        self.proc = None
        self.log = None
        if fresh and self.home.exists():
            shutil.rmtree(self.home)

    def write_config(self):
        self.ph.mkdir(parents=True, exist_ok=True)
        link = self.home / ".djl.ai"
        if not link.is_symlink():
            link.symlink_to(os.environ["GATE_MODELS"])
        rc = {"mode": self.mode}
        rc.update(self.reader_cfg)
        cfg = {
            "embeddingProviders": [{"type": "djl", "modelId": os.environ["GATE_MODEL_ID"],
                                    "url": os.environ["GATE_MODEL_URL"], "dimensions": 768, "maxTokens": 512}],
            "searchEmbeddingModel": os.environ["GATE_MODEL_ID"], "crossEncoderEnabled": False,
            "cluster": {
                "role": "reader", "backend": "object-store", "nodeId": "gate-" + self.name,
                "pollIntervalSeconds": 10, "cacheDir": str(self.cache_dir), "reader": rc,
                "objectStore": {
                    "endpoint": "http://127.0.0.1:9000", "region": "us-east-1", "bucket": "pharos-gate",
                    "prefix": "pharos", "pathStyle": True,
                    "accessKeyEnv": "PHAROS_GATE_ACCESS_KEY", "secretKeyEnv": "PHAROS_GATE_SECRET_KEY"},
            },
        }
        (self.ph / "config.json").write_text(json.dumps(cfg, indent=2))

    def command(self):
        cmd = ["java", "-Xmx" + os.environ.get("GATE_WEB_HEAP", "1500m"), "--enable-native-access=ALL-UNNAMED",
               "--add-opens", "java.base/java.nio.channels.spi=ALL-UNNAMED", "--add-modules", "jdk.incubator.vector",
               "-Dpolyglot.engine.WarnInterpreterOnly=false",
               "-Dlogback.configurationFile=" + os.environ["GATE_LOGBACK"],
               "-Duser.home=" + str(self.home), "-Dpharos.home=" + str(self.ph)]
        if self.niofs:
            cmd.append("-Dpharos.lucene.directory=niofs")
        return cmd + ["-jar", os.environ["GATE_JAR"], "web", "--port", str(self.port), "--no-open"]

    def start(self, tag=""):
        self.write_config()
        self.log = Path(os.environ["GATE"]) / f"reader-{self.name}{tag}.log"
        lf = open(self.log, "w")
        self.t_start = time.perf_counter()
        self.proc = subprocess.Popen(self.command(), stdout=lf, stderr=subprocess.STDOUT)
        lf.close()
        return self

    def wait_serving(self, timeout=600):
        """Seconds from JVM start to the 'serving N shard(s)' line and to /health ready."""
        end = time.perf_counter() + timeout
        t_serving = None
        pat = re.compile(r"serving (\d+) shard\(s\)")
        while time.perf_counter() < end:
            if self.proc.poll() is not None:
                raise RuntimeError(f"reader {self.name} exited {self.proc.returncode}; see {self.log}")
            if t_serving is None:
                m = pat.search(self.log.read_text(errors="replace"))
                if m:
                    t_serving = time.perf_counter() - self.t_start
                    if m.group(1) != "2":
                        raise RuntimeError(f"reader {self.name} serves {m.group(1)} shard(s), not 2; see {self.log}")
            if t_serving is not None:
                try:
                    if health(self.base).get("ready"):
                        return t_serving, time.perf_counter() - self.t_start
                except (OSError, ValueError):
                    pass
            time.sleep(0.05)
        raise RuntimeError(f"reader {self.name} not serving after {timeout}s; see {self.log}")

    def models_ready_ms(self):
        m = re.search(r"Search models ready \((\d+) ms\)", self.log.read_text(errors="replace"))
        return int(m.group(1)) if m else None

    def wait_models(self, timeout=300):
        """Seconds from JVM start until the background model warm-up logged 'Search models ready'."""
        end = time.perf_counter() + timeout
        while time.perf_counter() < end:
            if self.models_ready_ms() is not None:
                return time.perf_counter() - self.t_start
            if self.proc.poll() is not None:
                raise RuntimeError(f"reader {self.name} exited; see {self.log}")
            time.sleep(0.05)
        raise RuntimeError(f"reader {self.name}: no 'Search models ready' line after {timeout}s; see {self.log}")

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()  # by PID (Popen), never by name
            try:
                self.proc.wait(30)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None

    def lucene_bytes(self):
        ms = sorted(self.cache_dir.glob("lucene/gen-*/manifest.json"))
        if not ms:
            raise RuntimeError(f"no lucene manifest under {self.cache_dir}")
        m = json.loads(ms[-1].read_text())
        return sum(f["length"] for f in m["files"] if f["role"] == "lucene")


def mem_available_gib():
    for ln in open("/proc/meminfo"):
        if ln.startswith("MemAvailable"):
            return int(ln.split()[1]) / 1048576
    return float("nan")


def save(phase, data):
    p = Path(os.environ["GATE"]) / f"results-{phase}.json"
    p.write_text(json.dumps(data, indent=2))
    print(f"   results -> {p}")


# --------------------------------------------------------------------------- phases


COLD_VARIANTS = {
    # name: (reader mode, wait for the search models before the first query, gated)
    "cold": ("lazy", False, True),          # the gate: first answered query after ready
    "cold-full": ("full", False, False),    # control: full-mode reader (local files)
    # diagnostic from before readiness waited for the models: ready, then also wait for the
    # "Search models ready" line (now logged before ready, so this equals "cold")
    "cold-models": ("lazy", True, False),
}


def phase_cold(trials, variant="cold"):
    mode, wait_models, gated = COLD_VARIANTS[variant]
    base, _ = queries()
    lucene_q = [q for q, p in base if p == "lucene"]
    pharos_q = [q for q, p in base if p == "pharos"]
    lucene_q = list(dict.fromkeys(lucene_q))[:20]
    if len(lucene_q) < 20:
        raise RuntimeError("need 20 distinct lucene queries")
    rows = []
    for t in range(trials):
        r = Reader(variant, 7301, mode=mode, fresh=True)  # empties block cache + generation dirs + registry
        print(f"-- {variant} trial {t + 1}/{trials}: MemAvailable {mem_available_gib():.1f} GiB")
        r.start(f"-{t + 1:02d}")
        try:
            t_serving, t_ready = r.wait_serving()
            t_models = r.wait_models() if wait_models else None
            before = (health(r.base).get("readerCache") or {}).get("bytesFetched")
            lq = lucene_q[t % 20]
            dl, res_l = search(r.base, lq, "lucene", "keyword")
            mid = (health(r.base).get("readerCache") or {}).get("bytesFetched")
            vq = pharos_q[t % len(pharos_q)]
            dv, res_v = search(r.base, vq, "pharos", "vector")
            rc = health(r.base).get("readerCache") or {}
            models_ms = r.models_ready_ms()
        finally:
            r.stop()
        row = {"trial": t + 1, "serving_s": round(t_serving, 3), "ready_s": round(t_ready, 3),
               "models_wait_s": None if t_models is None else round(t_models, 3), "models_ready_ms": models_ms,
               "lucene_query": lq, "lucene_s": round(dl, 4), "lucene_hits": len(res_l),
               "vector_query": vq, "vector_s": round(dv, 4), "vector_hits": len(res_v),
               "fetched_before": before, "fetched_after_lucene": mid,
               "bytesOnDisk": rc.get("bytesOnDisk"), "misses": rc.get("misses"),
               "bytesFetched": rc.get("bytesFetched")}
        rows.append(row)
        print(f"   serving {t_serving:.2f}s ready {t_ready:.2f}s models {models_ms} ms | lucene {lq!r} {dl:.3f}s"
              f" ({len(res_l)} hits, fetched {before}->{mid}) | vector {vq!r} {dv:.3f}s ({len(res_v)} hits)"
              f" | fetched {rc.get('bytesFetched')}")
    lucene = summary([r["lucene_s"] for r in rows])
    vector = summary([r["vector_s"] for r in rows])
    serving = summary([r["serving_s"] for r in rows])
    ready = summary([r["ready_s"] for r in rows])
    ok = lucene["p95"] < COLD_P95_LIMIT_S and all(r["lucene_hits"] > 0 for r in rows)
    vec_ok = all(r["vector_hits"] > 0 for r in rows)
    out = {"variant": variant, "mode": mode, "wait_models": wait_models, "gated": gated, "trials": rows,
           "lucene": lucene, "vector": vector, "serving": serving, "ready": ready, "pass": ok,
           "vector_non_empty": vec_ok}
    tag = variant.upper()
    print(f"{tag} lucene first query p95 {lucene['p95']:.3f}s (p50 {lucene['p50']:.3f}, max {lucene['max']:.3f})"
          f" limit < {COLD_P95_LIMIT_S}s -> {'PASS' if ok else 'FAIL'}{'' if gated else ' (not gated)'}")
    print(f"{tag} vector first query p95 {vector['p95']:.3f}s (p50 {vector['p50']:.3f}; not gated)"
          f"{'' if vec_ok else ' -- some vector answers EMPTY'}")
    print(f"{tag} start->serving p50 {serving['p50']:.2f}s p95 {serving['p95']:.2f}s;"
          f" start->ready p50 {ready['p50']:.2f}s p95 {ready['p95']:.2f}s")
    save(variant, out)
    return (ok and vec_ok) if gated else True


def run_all(reader, qs):
    """Every query once: (timings by pipeline, results by (q, project, pipeline))."""
    times, res = {}, {}
    for q, p, pl in qs:
        dt, r = search(reader.base, q, p, pl)
        times.setdefault(pl, []).append(dt)
        res[f"{pl}|{p}|{q}"] = r
    return times, res


def check(a_res, b_res, rule):
    """Compares two result maps; rule 'strict' everywhere, or 'tolerant' (strict on keyword)."""
    st = {"compared": 0, "identical": 0, "tolerant": 0, "swaps": 0, "max_ulps": 0, "failures": []}
    for k, a in a_res.items():
        b = b_res[k]
        st["compared"] += 1
        pl = k.split("|", 1)[0]
        if rule == "strict" or pl == "keyword":
            why = compare_strict(a, b)
            if why is None:
                st["identical"] += 1
            else:
                st["failures"].append(f"{k}: {why}")
        else:
            if compare_strict(a, b) is None:
                st["identical"] += 1
                continue
            why, swaps, mu = compare_tolerant(a, b)
            st["max_ulps"] = max(st["max_ulps"], mu)
            if why is None:
                st["tolerant"] += 1
                st["swaps"] += swaps
            else:
                st["failures"].append(f"{k}: {why}")
    return st


def report_check(label, st):
    ok = not st["failures"]
    print(f"{label}: compared {st['compared']}, identical {st['identical']}, within tolerance {st['tolerant']}"
          f" (near-tie swap groups {st['swaps']}, max {st['max_ulps']} ulp), failures {len(st['failures'])}"
          f" -> {'PASS' if ok else 'FAIL'}")
    for f in st["failures"][:20]:
        print("   " + f)
    return ok


def phase_warm():
    base, qs = queries()
    if len(base) != 99:
        print(f"WARNING: {len(base)} pharos+lucene queries, expected 99")
    gate = Path(os.environ["GATE"])
    lazy = Reader("lazy", 7301, fresh=True)
    full = Reader("full", 7302, mode="full", fresh=True)
    print(f"-- warm: MemAvailable {mem_available_gib():.1f} GiB; starting lazy :7301 and full :7302")
    lazy.start()
    full.start()
    out = {}
    try:
        out["lazy_start"] = lazy.wait_serving()
        out["full_start"] = full.wait_serving()
        print(f"   lazy serving/ready {out['lazy_start']}, full {out['full_start']}")
        run_all(lazy, qs)  # warm the block cache (and both JVMs)
        run_all(full, qs)
        out["lazy_after_warmup"] = health(lazy.base).get("readerCache")
        # 3 runs per side, interleaved query by query (the side that goes first alternates), so
        # load from other processes on a shared host lands on both sides alike.
        timed = {"lazy": {}, "full": {}}
        sides = {"lazy": lazy, "full": full}
        for run in range(3):
            res = {"lazy": {}, "full": {}}
            for i, (q, p, pl) in enumerate(qs):
                order = ("lazy", "full") if (run + i) % 2 == 0 else ("full", "lazy")
                for side in order:
                    dt, r = search(sides[side].base, q, p, pl)
                    timed[side].setdefault(pl, []).append(dt)
                    res[side][f"{pl}|{p}|{q}"] = r
            if run == 2:
                lazy_res, full_res = res["lazy"], res["full"]
                (gate / "ref-full-mmap.json").write_text(json.dumps(full_res))
                (gate / "ref-lazy.json").write_text(json.dumps(lazy_res))
        out["lazy_cache"] = health(lazy.base).get("readerCache")
        out["timings"] = {s: {pl: summary(v) for pl, v in d.items()} for s, d in timed.items()}
        lk, fk = out["timings"]["lazy"]["keyword"], out["timings"]["full"]["keyword"]
        ratio = lk["p95"] / fk["p95"]
        out["ratio_keyword_p95"] = ratio
        warm_ok = ratio <= WARM_RATIO_LIMIT
        print(f"WARM keyword p95 lazy {lk['p95'] * 1000:.1f} ms vs full {fk['p95'] * 1000:.1f} ms"
              f" (n={lk['n']} each; p50 {lk['p50'] * 1000:.1f} vs {fk['p50'] * 1000:.1f}) ratio {ratio:.3f}"
              f" limit <= {WARM_RATIO_LIMIT} -> {'PASS' if warm_ok else 'FAIL'}")
        for pl in ("vector", "hybrid"):
            lv, fv = out["timings"]["lazy"][pl], out["timings"]["full"][pl]
            print(f"   {pl} p95 lazy {lv['p95'] * 1000:.1f} ms vs full {fv['p95'] * 1000:.1f} ms (not gated)")
        mm = check(lazy_res, full_res, "strict")
        strict_ok = report_check("TOP10 lazy vs full MMap (bitwise, stored fields)", mm)
        out["strict"] = mm

        full.stop()
        niofs = Reader("full", 7302, mode="full", niofs=True)  # same home: the same full copy
        print("-- re-opening the same full copy with NIOFSDirectory")
        niofs.start("-niofs")
        try:
            out["niofs_start"] = niofs.wait_serving()
            _, niofs_res = run_all(niofs, qs)
        finally:
            niofs.stop()
        (gate / "ref-full-niofs.json").write_text(json.dumps(niofs_res))
        _, lazy_res2 = run_all(lazy, qs)
        stv = check(lazy_res2, niofs_res, "tolerant")
        niofs_ok = report_check("TOP10 lazy vs full NIOFS (keyword identical; vector/hybrid tolerant)", stv)
        out["niofs"] = stv
        non_empty = sum(1 for v in niofs_res.values() if v)
        out["non_empty"] = non_empty
        print(f"   non-empty answers {non_empty}/{len(niofs_res)}")
        vac_ok = non_empty >= 0.9 * len(niofs_res)
        if not vac_ok:
            print("FAIL: fewer than 90% non-empty answers (vacuous comparison)")
    finally:
        lazy.stop()
        full.stop()
    out["pass"] = {"warm_p95": warm_ok, "strict_mmap": strict_ok, "tolerant_niofs": niofs_ok,
                   "non_vacuous": vac_ok}
    save("warm", out)
    return all(out["pass"].values())


def phase_cap():
    gate = Path(os.environ["GATE"])
    _, qs = queries()
    refs = {}
    for name in ("niofs", "mmap"):
        p = gate / f"ref-full-{name}.json"
        if not p.is_file():
            raise RuntimeError(f"{p} missing: run the warm phase first")
        refs[name] = json.loads(p.read_text())
    lb = Reader("lazy", 7301).lucene_bytes()
    cap, block = cache_cap(lb)
    fetches = DEFAULT_FETCHES
    bound = cap + fetches * block
    print(f"-- cap: lucene shard Lucene bytes {lb}; cacheMaxBytes {cap} ({cap / 2**20:.1f} MiB),"
          f" blockSizeBytes {block} ({block >> 10} KiB), maxConcurrentFetches {fetches}; bound {bound}")
    r = Reader("cap", 7301, reader_cfg={"cacheMaxBytes": cap, "blockSizeBytes": block,
                                        "maxConcurrentFetches": fetches}, fresh=True)
    samples, stop = [], threading.Event()

    def sampler():
        while not stop.is_set():
            try:
                rc = health(r.base).get("readerCache") or {}
                if "bytesOnDisk" in rc:
                    samples.append((time.time(), rc["bytesOnDisk"]))
            except (OSError, ValueError):
                pass
            stop.wait(1.0)

    print(f"   MemAvailable {mem_available_gib():.1f} GiB")
    r.start()
    th = threading.Thread(target=sampler, daemon=True)
    out = {"lucene_bytes": lb, "cacheMaxBytes": cap, "blockSizeBytes": block, "maxConcurrentFetches": fetches,
           "bound": bound}
    try:
        th.start()
        out["start"] = r.wait_serving()
        _, res1 = run_all(r, qs)
        _, res2 = run_all(r, qs)
        time.sleep(1.5)
        out["readerCache"] = health(r.base).get("readerCache")
    finally:
        stop.set()
        th.join(5)
        r.stop()
    mx = max(s for _, s in samples) if samples else None
    out["samples"] = len(samples)
    out["max_bytesOnDisk"] = mx
    cap_ok = mx is not None and mx <= bound and len(samples) >= 5
    rc = out["readerCache"] or {}
    print(f"CAP max bytesOnDisk {mx} over {len(samples)} samples; cap {cap} + {fetches} x {block} = {bound}"
          f" -> {'PASS' if cap_ok else 'FAIL'}; hit rate {rc.get('hitRate')}, evictions {rc.get('evictions')},"
          f" misses {rc.get('misses')}, bytesFetched {rc.get('bytesFetched')}")
    if mx is not None and mx > cap:
        print(f"   note: max sample exceeds cacheMaxBytes by {mx - cap} bytes (in-flight allowance {fetches * block})")
    ok_s = report_check("CAP TOP10 vs full MMap (bitwise), pass 1", check(res1, refs["mmap"], "strict"))
    st2 = check(res2, refs["mmap"], "strict")
    ok_s2 = report_check("CAP TOP10 vs full MMap (bitwise), pass 2", st2)
    stn = check(res2, refs["niofs"], "tolerant")
    ok_n = report_check("CAP TOP10 vs full NIOFS (tolerant), pass 2", stn)
    out["strict_mmap_pass2"], out["tolerant_niofs_pass2"] = st2, stn
    out["pass"] = {"cap": cap_ok, "strict_mmap": ok_s and ok_s2, "tolerant_niofs": ok_n}
    save("cap", out)
    return all(out["pass"].values())


# --------------------------------------------------------------------------- self-test


def self_test():
    assert percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], 95) == 19
    assert percentile([5.0], 95) == 5.0
    assert percentile(list(range(1, 101)), 95) == 95
    assert percentile([3, 1, 2], 50) == 2
    assert summary([1.0, 3.0])["mean"] == 2.0
    assert ulps(1.0, 1.0) == 0
    one_up = struct.unpack("<f", struct.pack("<i", f32_bits(1.0) + 1))[0]
    four_up = struct.unpack("<f", struct.pack("<i", f32_bits(1.0) + 4))[0]
    nine_up = struct.unpack("<f", struct.pack("<i", f32_bits(1.0) + 9))[0]  # 1.07e-6 relative
    assert ulps(1.0, one_up) == 1 and near(1.0, four_up) and not near(1.0, nine_up)
    assert near(1000.0, 1000.0005) and not near(1000.0, 1000.01)
    assert cache_cap(55_601_636) == (24 << 20, 4 << 20), cache_cap(55_601_636)
    assert cache_cap(200 << 20) == (96 << 20, 8 << 20)
    assert cache_cap(2 * (32 << 20)) == (32 << 20, 8 << 20)

    def r(i, s, **kw):
        d = {"id": i, "score": s, "label": i}
        d.update(kw)
        return d
    a = [r("a", 3.0), r("b", 2.0), r("c", 1.0)]
    assert compare_strict(a, [dict(x) for x in a]) is None
    two_up = struct.unpack("<f", struct.pack("<i", f32_bits(2.0) + 1))[0]
    assert "score" in compare_strict(a, [r("a", 3.0), r("b", two_up), r("c", 1.0)])
    assert "fields" in compare_strict(a, [r("a", 3.0), r("b", 2.0, label="x"), r("c", 1.0)])
    # tolerant: 1-ulp score drift ok; swap inside a near-tie ok; swap across distinct scores not
    t = struct.unpack("<f", struct.pack("<i", f32_bits(2.0) + 1))[0]
    assert compare_tolerant(a, [r("a", 3.0), r("b", t), r("c", 1.0)])[0] is None
    tie = [r("a", 3.0), r("b", 2.0), r("c", t)]
    swapped = [r("a", 3.0), r("c", t), r("b", 2.0)]
    why, swaps, _ = compare_tolerant(tie, swapped)
    assert why is None and swaps == 1, why
    assert compare_tolerant(a, [r("b", 3.0), r("a", 2.0), r("c", 1.0)])[0] is not None
    assert compare_tolerant(a, [r("a", 3.0), r("b", 2.5), r("c", 1.0)])[0] is not None
    # a full page whose last near-tie group exchanges an id with rank 11 is a boundary swap
    page = [r(f"x{i}", 10.0 - i) for i in range(9)] + [r("p", 0.5)]
    other = [r(f"x{i}", 10.0 - i) for i in range(9)] + [r("q", 0.5)]
    assert compare_tolerant(page, other)[0] is None
    assert compare_tolerant(page[:9] + [r("p", 0.5)], page[:8] + [r("z", 2.0), r("p", 0.5)])[0] is not None
    print("self-test OK")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--phases", default="cold,warm,cap")
    ap.add_argument("--trials", type=int, default=20)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        self_test()
        return 0
    phases = [p.strip() for p in args.phases.split(",") if p.strip()]
    unknown = set(phases) - {"warm", "cap"} - set(COLD_VARIANTS)
    if unknown:
        ap.error(f"unknown phases {sorted(unknown)}")
    if args.dry_run:
        base, qs = queries()
        print(f"driver (dry run): phases {phases}; {args.trials} cold trials; {len(base)} benchmark queries"
              f" -> {len(qs)} gate queries (keyword {len(base)}, vector+hybrid on pharos)")
        return 0
    results = {}
    for p in phases:
        print(f"== phase {p} (MemAvailable {mem_available_gib():.1f} GiB)")
        if p in COLD_VARIANTS:
            results[p] = phase_cold(args.trials, p)
        else:
            results[p] = {"warm": phase_warm, "cap": phase_cap}[p]()
    verdict = all(results.values())
    def label(p, ok):
        if p in COLD_VARIANTS and not COLD_VARIANTS[p][2]:
            return f"{p} n/a (diagnostic)"
        return f"{p} {'pass' if ok else 'FAIL'}"
    print("GATE " + ("PASSED" if verdict else "FAILED") + ": " + ", ".join(label(p, ok) for p, ok in results.items()))
    return 0 if verdict else 1


if __name__ == "__main__":
    sys.exit(main())
