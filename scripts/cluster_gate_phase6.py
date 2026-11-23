#!/usr/bin/env python3
"""Phase 6 gate driver (standard library only). Driven by scripts/cluster-gate-phase6.sh, which has
started MinIO on 127.0.0.1:$GATE_S3_PORT, built the bare repos $GATE/repos/{lucene,pharos}.git and had a
bootstrap writer publish both projects as generation 1 under spec [project] (lucene "embed": false).

Every JVM is started and stopped here, by PID (Popen). Work-queue writers W1 W2, readers R1-R3 (all
shards, lazy, jina for the hybrid and vector queries, no cross-encoder) and the router run throughout,
with ~10 QPS of mixed queries through the router (unscoped keyword, keyword scoped to the query's
project, unscoped hybrid, pharos keyword with a module filter, lucene keyword with a path prefix).
Background monitors record the catalog (catalog/shards.json), both reshard job records and every
shard's LATEST every 0.5 s.

  setup      start the nodes; wait until both writers hold a project and every project is published by
             a work-queue writer with nothing pending.
  before     ranking reference: the 99 pharos + lucene benchmark query texts, unscoped, pipelines
             keyword, hybrid, vector (limit 10) on R1, before any reshard.
  reshard    `pharos reshard lucene --spec "[project, ring(path, shards=4)]"`, then shards=5, then
             `pharos reshard pharos --spec "[project, module]"`, each under load, each waited to its flip.
             Pass: the 4 -> 5 step parses <= 25% of lucene's indexed files and every shard it keeps has its
             generation unchanged; 0 embedding provider calls (cache misses) during the pharos reshard.
  after      the same ranking on R1 after the three reshards. Pass: the Phase 3 rules (keyword top-10
             identical >= 98%; hybrid mean tau >= 0.95 and top-1 >= 95%); vector by ruling P6-R17 (recall:
             per-query top-10 overlap >= 90%, mean >= 98%, shared documents' raw (pre-graph-boost) scores
             bitwise identical, ruling P6-R20: the trace's rawScores); keyword pages that are identical may
             differ in score by at most 1e-4 relative (P6-R20).
  pruning    a module-filtered pharos query touches only that module's shard (trace: routing note and the
             fan-out shard count); a path prefix on ring-sharded lucene fans out to all its shards (traced).
  interrupt  `pharos reshard lucene --spec "[project, ring(path, shards=6)]"`; the writer running it is
             killed (-9, by PID) mid-build. Pass: lucene keeps answering at its version N while the job
             is open, and the job resumes on the other writer and flips.
  check      0 failed queries over the whole run; docCount per version; the marker check.

--dry-run prints the plan.
"""
import argparse
import concurrent.futures
import json
import os
import re
import signal
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
import cluster_gate  # noqa: E402  (load_queries)
import cluster_gate_phase3 as p3  # noqa: E402  (compare_rank, verdict, fetch)
import cluster_gate_phase4 as p4  # noqa: E402  (S3)

HB = 5
POLL = 10
TTL = 30
MIN_HOLD = 120
QPS = 10
PORTS = {"router": 7690, "r1": 7601, "r2": 7602, "r3": 7603, "w1": 7611, "w2": 7612}
READERS = ["r1", "r2", "r3"]
WRITERS = ["w1", "w2"]
PIPELINES = ("keyword", "hybrid", "vector")
PHAROS_MODULE = "src/test/resources/test-projects/sample-app"
LUCENE_PREFIX = "lucene/core"
SPEC_RING4 = "[project, ring(path, shards=4)]"
SPEC_RING5 = "[project, ring(path, shards=5)]"
SPEC_RING6 = "[project, ring(path, shards=6)]"
SPEC_MODULE = "[project, module]"
MOVE_MAX = 0.25
VECTOR_MIN_OVERLAP = 0.90   # ruling P6-R17
VECTOR_MEAN_OVERLAP = 0.98
KEYWORD_MAX_REL_DRIFT = 1e-4  # ruling P6-R20: residual score drift accepted on identical keyword pages
T0 = time.time()
RESULTS = {}


def now():
    return time.time()


def log(msg):
    print(f"[{now() - T0:7.1f}s] {msg}", flush=True)


def gate():
    return Path(os.environ["GATE"])


def save():
    (gate() / "results-phase6.json").write_text(json.dumps(RESULTS, indent=2, default=str))


def mem_gib():
    for ln in open("/proc/meminfo"):
        if ln.startswith("MemAvailable"):
            return int(ln.split()[1]) / 1048576
    return float("nan")


def s3_cfg():
    return {"endpoint": f"http://127.0.0.1:{os.environ['GATE_S3_PORT']}", "region": "us-east-1",
            "bucket": "pharos-gate", "prefix": "pharos", "pathStyle": True,
            "accessKeyEnv": "PHAROS_GATE_ACCESS_KEY", "secretKeyEnv": "PHAROS_GATE_SECRET_KEY"}


def provider():
    return [{"type": "djl", "modelId": os.environ["GATE_MODEL_ID"], "url": os.environ["GATE_MODEL_URL"],
             "dimensions": 768, "maxTokens": 512}]


# --------------------------------------------------------------------------- nodes


class Node:
    def __init__(self, name, kind):
        self.name, self.kind, self.port = name, kind, PORTS.get(name)
        self.node_id = "gate-" + name
        self.base = f"http://127.0.0.1:{self.port}"
        self.home = gate() / "nodes" / name
        self.ph = self.home / ".pharos"
        self.proc = self.log = None
        self.starts = 0

    def config(self):
        cluster = {"backend": "object-store", "nodeId": self.node_id, "cacheDir": str(self.ph / "cluster-cache"),
                   "heartbeatSeconds": HB, "pollIntervalSeconds": 10, "token": os.environ["GATE_TOKEN"],
                   "retentionGenerations": 1000, "retentionMinutes": 100000, "objectStore": s3_cfg()}
        cfg = {"crossEncoderEnabled": False, "embeddingProviders": provider(),
               "searchEmbeddingModel": os.environ["GATE_MODEL_ID"], "cluster": cluster}
        if self.kind == "writer":
            cluster["role"] = "writer"
            cluster["writer"] = {"workQueue": True, "workDir": str(self.home / "work"), "pollSeconds": POLL,
                                 "leaseTtlSeconds": TTL, "minHoldSeconds": MIN_HOLD,
                                 "disk": {"minFreeBytes": 8 << 30, "minFreePercent": 0}}
        elif self.kind == "cli":
            cluster["role"] = "writer"
            cluster["writer"] = {"workQueue": False}
        else:
            cluster["role"] = "reader"
            cluster["advertiseUrl"] = self.base
            cluster["reader"] = {"mode": "lazy"}
        return cfg

    def jvm(self, heap):
        return ["java", "-Xmx" + heap, "-XX:+UseG1GC", "-XX:G1PeriodicGCInterval=15000",
                "--enable-native-access=ALL-UNNAMED", "--add-opens", "java.base/java.nio.channels.spi=ALL-UNNAMED",
                "--add-modules", "jdk.incubator.vector", "-Dpolyglot.engine.WarnInterpreterOnly=false",
                "-Dlogback.configurationFile=" + os.environ["GATE_LOGBACK"],
                "-Duser.home=" + str(self.home), "-Dpharos.home=" + str(self.ph), "-jar", os.environ["GATE_JAR"]]

    def prepare(self):
        self.ph.mkdir(parents=True, exist_ok=True)
        link = self.home / ".djl.ai"
        if not link.is_symlink():
            link.symlink_to(os.environ["GATE_MODELS"])
        (self.ph / "config.json").write_text(json.dumps(self.config(), indent=2))

    def start(self):
        self.prepare()
        heap = os.environ["GATE_WRITER_HEAP"] if self.kind == "writer" else os.environ["GATE_WEB_HEAP"]
        cmd = self.jvm(heap) + (["router", "--port", str(self.port)] if self.kind == "router"
                                else ["web", "--port", str(self.port), "--no-open"])
        self.starts += 1
        self.log = gate() / "logs" / f"{self.name}-{self.starts}.log"
        with open(self.log, "w") as lf:
            self.proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT)
        with open(gate() / "pids.txt", "a") as f:
            f.write(f"{self.proc.pid} {self.name}\n")
        log(f"started {self.name} ({self.kind}) pid {self.proc.pid} :{self.port}; MemAvailable {mem_gib():.1f} GiB")
        return self

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def wait_health(self, timeout=600):
        end = now() + timeout
        while now() < end:
            if self.proc.poll() is not None:
                raise RuntimeError(f"{self.name} exited {self.proc.returncode}; see {self.log}")
            try:
                with urllib.request.urlopen(self.base + "/health", timeout=5) as r:
                    if r.status == 200:
                        return
            except urllib.error.HTTPError:
                if self.kind == "router":
                    return
            except OSError:
                pass
            time.sleep(0.5)
        raise RuntimeError(f"{self.name} not healthy after {timeout}s")

    def stop(self):
        if not self.alive():
            return
        self.proc.terminate()  # SIGTERM, by PID
        try:
            self.proc.wait(120)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(30)
        log(f"stopped {self.name} pid {self.proc.pid}: exit {self.proc.returncode}")

    def kill9(self):
        self.proc.kill()  # SIGKILL, by PID
        self.proc.wait(30)
        log(f"KILLED -9 {self.name} pid {self.proc.pid}")

    def log_offset(self):
        return self.log.stat().st_size if self.log and self.log.exists() else 0

    def log_since(self, off):
        with open(self.log, "rb") as f:
            f.seek(off)
            return f.read().decode(errors="replace")

    def rss_mib(self):
        try:
            for ln in open(f"/proc/{self.proc.pid}/status"):
                if ln.startswith("VmRSS"):
                    return int(ln.split()[1]) // 1024
        except (OSError, AttributeError):
            pass
        return None


NODES = {}


def stop_all():
    for n in list(NODES.values()):
        try:
            n.stop()
        except Exception as e:
            log(f"stopping {n.name} failed: {e!r}")


def cli(*args, timeout=300):
    """`pharos <args>` from the CLI home (bucket config only); (exit code, output)."""
    n = NODES.setdefault("cli", Node("cli", "cli"))
    n.prepare()
    r = subprocess.run(n.jvm("512m") + list(args), capture_output=True, text=True, timeout=timeout)
    return r.returncode, (r.stdout + r.stderr).strip()


# --------------------------------------------------------------------------- bucket and monitors


class Bucket:
    def __init__(self):
        self.s3 = p4.S3(endpoint=f"127.0.0.1:{os.environ['GATE_S3_PORT']}")

    def catalog(self):
        return self.s3.get_json("catalog/shards.json") or {}

    def project(self, name, cat=None):
        """The project's catalog entry; under the default [project] spec there is none (the catalog lists
        only the shard), which reads as spec version None with the one shard named after the project."""
        cat = cat if cat is not None else self.catalog()
        for p in cat.get("projects", []) or []:
            if p.get("name") == name:
                return p
        if any(s.get("id") == name for s in cat.get("shards", [])):
            return {"name": name, "spec": None, "shards": [name], "implicit": True}
        return None

    def job(self, project):
        try:
            return self.s3.get_json(f"projects/{project}/reshard/job.json")
        except (OSError, ValueError):
            return None

    def latest(self):
        """{shard dir name (encoded): generation} for every shard with a LATEST."""
        out = {}
        for k, _, _ in self.s3.list("shards/"):
            if k.endswith("/LATEST"):
                b = self.s3.get_json(k)
                out[urllib.parse.unquote(k[len("shards/"):-len("/LATEST")])] = b.get("generation") if b else None
        return out


def active_spec(p):
    """(spec version, shard ids) of a catalog project entry."""
    shards = [s if isinstance(s, str) else s.get("id") for s in p.get("shards", [])]
    return p.get("spec"), shards


class Monitors:
    def __init__(self):
        self.b = Bucket()
        self.stop_ev = threading.Event()
        self.lock = threading.Lock()
        self.samples = []      # (t, kind, project, status, latency, nresults, warnings)
        self.load_on = threading.Event()
        self.catalog_changes = 0
        self.jobs = {}
        base, _ = p3.gate_queries()
        self.queries = base

    def start(self):
        for fn in (self._catalog_loop, self._load_loop):
            threading.Thread(target=fn, daemon=True, name=fn.__name__).start()

    def close(self):
        self.stop_ev.set()
        self.load_on.clear()

    def _catalog_loop(self):
        last, lastjob = None, {}
        with open(gate() / "catalog-timeline.jsonl", "w") as out:
            while not self.stop_ev.is_set():
                t = now()
                try:
                    cat = self.b.catalog()
                    s = json.dumps(cat, sort_keys=True)
                    if s != last:
                        out.write(json.dumps({"t": round(t - T0, 2), "kind": "catalog", "catalog": cat}) + "\n")
                        last = s
                        self.catalog_changes += 1
                    for pr in ("lucene", "pharos"):
                        j = self.b.job(pr)
                        js = json.dumps(j, sort_keys=True)
                        if js != lastjob.get(pr):
                            out.write(json.dumps({"t": round(t - T0, 2), "kind": "job", "project": pr, "job": j}) + "\n")
                            lastjob[pr] = js
                            with self.lock:
                                self.jobs[pr] = j
                    out.flush()
                except Exception as e:
                    out.write(json.dumps({"t": round(t - T0, 2), "kind": "error", "error": repr(e)[:200]}) + "\n")
                self.stop_ev.wait(max(0.0, 0.5 - (now() - t)))

    def _load_loop(self):
        router = f"http://127.0.0.1:{PORTS['router']}"
        kinds = ["keyword", "keyword-scoped", "hybrid", "keyword-module", "keyword-path"]
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=32)
        i = 0
        with open(gate() / "load.jsonl", "w") as out:
            def one(t, kind, q, proj):
                params = {"q": q, "pipeline": "hybrid" if kind == "hybrid" else "keyword", "limit": 10}
                project = None
                if kind == "keyword-scoped":
                    project = proj
                elif kind == "keyword-module":
                    project = "pharos"
                    params["module"] = PHAROS_MODULE
                elif kind == "keyword-path":
                    project = "lucene"
                    params["pathPrefix"] = LUCENE_PREFIX
                if project:
                    params["project"] = project
                t1 = time.perf_counter()
                status, n, warns = -1, 0, []
                try:
                    with urllib.request.urlopen(f"{router}/api/search?" + urllib.parse.urlencode(params),
                                                timeout=30) as r:
                        body, status = r.read(), r.status
                        hdr = r.headers.get("X-Pharos-Warnings")
                    data = json.loads(body)
                    res = data if isinstance(data, list) else data.get("results")
                    if res is None:
                        status = -2
                    else:
                        n = len(res)
                    if isinstance(data, dict):
                        warns += data.get("warnings", []) or []
                    if hdr:
                        try:
                            warns += [w for w in json.loads(hdr) if w not in warns]
                        except ValueError:
                            warns.append(hdr)
                except urllib.error.HTTPError as e:
                    status, warns = e.code, [e.read()[:200].decode(errors="replace")]
                except Exception as e:
                    status, warns = -1, [repr(e)[:200]]
                rec = (round(t - T0, 2), kind, project, status, round(time.perf_counter() - t1, 4), n, warns)
                with self.lock:
                    self.samples.append(rec)
                    out.write(json.dumps(rec) + "\n")

            nxt = now()
            while not self.stop_ev.is_set():
                if not self.load_on.is_set():
                    self.load_on.wait(1)
                    nxt = now()
                    continue
                q, proj = self.queries[i % len(self.queries)]
                kind = kinds[i % len(kinds)]
                i += 1
                pool.submit(one, now(), kind, q, proj)
                nxt += 1.0 / QPS
                time.sleep(max(0.0, nxt - now()))
        pool.shutdown(wait=True)

    def window(self, t0, t1=None):
        with self.lock:
            return [s for s in self.samples if s[0] >= t0 - T0 and (t1 is None or s[0] < t1 - T0)]


def failed(s):
    """A failed query: non-200, a client error, or a warning that a shard or reader was unavailable."""
    _, _, _, status, _, _, warns = s
    return status != 200 or any("unavailable" in w.lower() or "no live reader" in w.lower() for w in warns)


def wait_until(pred, timeout, what, every=1.0):
    end = now() + timeout
    while now() < end:
        v = pred()
        if v:
            return v
        time.sleep(every)
    raise RuntimeError(f"timeout ({timeout}s) waiting for {what}")


# --------------------------------------------------------------------------- phases


MON = None


def phase_setup():
    global MON
    for r in READERS:
        NODES[r] = Node(r, "reader").start()
    for w in WRITERS:
        NODES[w] = Node(w, "writer").start()
    for n in READERS + WRITERS:
        NODES[n].wait_health()
    NODES["router"] = Node("router", "router").start()
    NODES["router"].wait_health()
    MON = Monitors()
    MON.start()
    b = MON.b
    t0 = now()

    def settled():
        cat = b.catalog()
        hbs = {}
        for k, _, _ in b.s3.list("catalog/writers/"):
            if k.endswith(".json"):
                hbs[k.rsplit("/", 1)[1][:-5]] = b.s3.get_json(k)
        held = {}
        for w, hb in hbs.items():
            for s in hb.get("leases") or []:
                held[s] = w
        pending = [(w, s) for w, hb in hbs.items() for s, bl in (hb.get("backlog") or {}).items()
                   if (bl or {}).get("pending")]
        busy = [w for w, hb in hbs.items() if hb.get("indexing")]
        lat = b.latest()
        # each project's holder reports its source revision published (the bootstrap's gen 1 may carry
        # no sourceRevision, in which case the holder republishes it first)
        current = all(((hbs.get(held.get(p)) or {}).get("backlog") or {}).get(p, {}).get("publishedRevision")
                      not in (None, "") for p in ("lucene", "pharos") if held.get(p))
        ok = (current and not pending and not busy
              and len(set(held.values())) == 2 and set(held) >= {"lucene", "pharos"})
        return {"latest": lat, "held": held, "catalog": cat} if ok else None

    st = wait_until(settled, 3600, "both projects republished by the work-queue writers, one per writer", 5)
    log(f"setup settled after {now() - t0:.0f}s: LATEST {st['latest']}, held {st['held']}")
    # readers serving the writers' generations
    time.sleep(30)
    MON.load_on.set()
    time.sleep(20)
    RESULTS["setup"] = {"settle_s": round(now() - t0), "latest": st["latest"], "held": st["held"],
                        "rss_mib": {n: NODES[n].rss_mib() for n in NODES if n != "cli"}}
    save()
    return True


def fetch_traced(base, q, pipeline, timeout=120):
    """p3.fetch with ?trace=true: each result gains "raw", its raw (pre-graph-boost) score per stage type
    (ruling P6-R20, searchMeta.rawScores)."""
    params = {"q": q, "pipeline": pipeline, "limit": p3.LIMIT, "trace": "true"}
    url = f"{base}/api/search?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            body, status = r.read(), r.status
    except urllib.error.HTTPError as e:
        body, status = e.read(), e.code
    except Exception as e:  # connection refused, reset, timeout
        return -1, [], [f"client error: {e!r}"]
    try:
        data = json.loads(body)
    except ValueError:
        return (status if status != 200 else -2), [], [body[:200].decode(errors="replace")]
    if isinstance(data, list):
        return status, data, []
    results = data.get("results", [])
    meta = data.get("searchMeta") or {}
    LAST_META[q, pipeline] = {"routing": meta.get("routing", []), "statistics": meta.get("statistics")}
    raw = meta.get("rawScores") or {}
    for res in results:
        res["raw"] = {t: m[res.get("id")] for t, m in raw.items() if res.get("id") in m}
    if "results" not in data and status == 200:
        status = -2
    return status, results, list(data.get("warnings") or [])


LAST_META = {}  # (query, pipeline) -> {"routing", "statistics"} of the last traced answer (P6-R22 diagnostics)


def run_queries_traced(base, sp):
    """p3.run_queries over fetch_traced: {key: results}, warnings {text: count}, failures."""
    res, warns, fails, per = {}, {}, [], {}
    for pl, scope, q in sp:
        status, results, w = fetch_traced(base, q, pl)
        k = p3.key(pl, scope, q)
        if status != 200:
            fails.append(f"{k}: status {status} {w}")
        res[k] = results
        per[k] = sorted(w)
        for x in w:
            warns[x[:160]] = warns.get(x[:160], 0) + 1
    p3.LAST_WARNINGS[base] = per
    return res, warns, fails


def ranking_pass(label):
    _, qs = p3.gate_queries()
    sp = [(pl, None, q) for pl in PIPELINES for q in qs]
    base = NODES["r1"].base
    run_queries_traced(base, sp)  # warm-up
    res, warns, fails = run_queries_traced(base, sp)
    (gate() / f"ranking-{label}-meta.json").write_text(json.dumps(
        {p3.key(pl, scope, q): LAST_META.get((q, pl)) for pl, scope, q in sp}))
    (gate() / f"ranking-{label}.json").write_text(json.dumps(res))
    (gate() / f"ranking-{label}-warnings.json").write_text(json.dumps(p3.LAST_WARNINGS[base]))
    log(f"ranking {label}: {len(sp)} answers, failures {len(fails)}, warnings {warns}")
    return res, fails


def phase_before():
    _, fails = ranking_pass("before")
    RESULTS["before"] = {"failures": fails}
    save()
    return not fails


def holder_of(project):
    j = MON.b.job(project) or {}
    return j.get("writer")


def reshard(project, spec, label, kill_mid=False):
    b = MON.b
    cat0 = b.catalog()
    v0, shards0 = active_spec(b.project(project, cat0))
    lat0 = b.latest()
    offs = {w: NODES[w].log_offset() for w in WRITERS}
    t_req = now()
    rc, out = cli("reshard", project, "--spec", spec)
    log(f"reshard {label}: exit {rc}: {out}")
    if rc != 0:
        raise RuntimeError(f"pharos reshard {project} failed: {out}")
    m = re.search(r"spec version (\d+)", out)
    target = int(m.group(1))
    rec = {"label": label, "spec": spec, "from_version": v0, "from_shards": shards0, "to_version": target,
           "t_request": round(t_req - T0, 1), "latest_before": lat0}
    killed = None
    if kill_mid:
        def mid():
            j = b.job(project) or {}
            if j.get("spec") != target:
                return None
            ft, fd = j.get("filesTotal"), j.get("filesDone")
            if ft and fd is not None and fd * 2 >= ft and j.get("phase") not in ("built", "pinned", "flipped"):
                return j
            if j.get("affected"):
                return j
            return None
        j = wait_until(mid, 1800, f"{project} reshard mid-build", 0.2)
        victim = {"gate-" + w: w for w in WRITERS}[j["writer"]]
        cat_k = b.catalog()
        NODES[victim].kill9()
        killed = {"t": round(now() - T0, 1), "writer": victim, "job": j,
                  "active_version_at_kill": b.project(project, cat_k).get("spec")}
        rec["killed"] = killed
        log(f"killed {victim} mid-reshard: job phase {j.get('phase')}, files {j.get('filesDone')}/{j.get('filesTotal')},"
            f" affected {j.get('affected')}")
        # version N keeps serving while the job is open
        served = []

        def probe_until_flip():
            p = b.project(project)
            v, _ = active_spec(p)
            dt, status, results, warns = p3.fetch(NODES["r2"].base, "IndexWriter", "keyword", project, timeout=30)
            served.append((round(now() - T0, 1), v, status, len(results), warns))
            return v == target and not p.get("staged")
        wait_until(probe_until_flip, 3600, f"{project} flip to {target} after the kill", 2)
        n_ok = sum(1 for _, v, s, n, w in served if v == v0 and s == 200 and n > 0)
        n_old = sum(1 for _, v, *_ in served if v == v0)
        rec["served_while_open"] = {"probes_at_version_N": n_old, "ok_with_results": n_ok,
                                    "failed": [x for x in served if x[1] == v0 and (x[2] != 200 or x[3] == 0)][:5]}
    else:
        def flipped():
            p = b.project(project)
            v, _ = active_spec(p)
            return v == target and not p.get("staged")
        wait_until(flipped, 3600, f"{project} flip to spec version {target}", 1)
    t_flip = now()
    rec["t_flip"] = round(t_flip - T0, 1)
    rec["duration_s"] = round(t_flip - t_req, 1)
    time.sleep(5)
    job = b.job(project)
    rec["job"] = job
    rec["latest_after"] = b.latest()
    cat1 = b.catalog()
    rec["catalog_entry_after"] = b.project(project, cat1)
    rec["to_shards"] = active_spec(rec["catalog_entry_after"])[1]
    logs = {w: NODES[w].log_since(offs[w]) if NODES[w].log else "" for w in WRITERS}
    moves = []
    embeds = []
    for w, text in logs.items():
        for ln in text.splitlines():
            mm = re.search(r"Reshard of '" + project + r"' from .*?: (\d+) of (\d+) files move; shards written \[(.*?)\],"
                           r" kept \[(.*?)\], dropped \[(.*?)\]", ln)
            if mm:
                moves.append({"writer": w, "moved": int(mm.group(1)), "indexed": int(mm.group(2)),
                              "written": mm.group(3), "kept": mm.group(4), "dropped": mm.group(5)})
            me = re.search(r"Embedding cache\[(.*?)\]: (\d+) hit\(s\), (\d+) miss\(es\) for '(.*?)'", ln)
            if me:
                embeds.append({"writer": w, "model": me.group(1), "hits": int(me.group(2)),
                               "misses": int(me.group(3)), "project": me.group(4)})
        rec.setdefault("log_lines", {})[w] = [ln for ln in text.splitlines()
                                              if "Reshard" in ln or "Embedding cache" in ln or "FULL" in ln
                                              or "restored" in ln or "Published" in ln][:80]
    rec["moves"] = moves
    rec["embeds"] = embeds
    log(f"reshard {label}: flipped after {rec['duration_s']}s; moves {moves}; embeds {embeds}")
    return rec


def phase_reshard():
    out = {}
    out["lucene4"] = reshard("lucene", SPEC_RING4, "lucene [project] -> ring 4")
    save_part("reshard", out)
    out["lucene5"] = r5 = reshard("lucene", SPEC_RING5, "lucene ring 4 -> 5")
    save_part("reshard", out)
    out["pharos"] = rp = reshard("pharos", SPEC_MODULE, "pharos [project] -> [project, module]")
    save_part("reshard", out)
    ok = True
    # 4 -> 5: parsed files <= 25 % of the indexed files; kept shards' generations unchanged
    mv = r5["moves"][-1] if r5["moves"] else None
    job = r5["job"] or {}
    parsed = job.get("parsedFiles", job.get("parsed"))
    if mv:
        frac = mv["moved"] / mv["indexed"]
        kept = job.get("unaffected") or []
        unchanged = {s: (r5["latest_before"].get(s), r5["latest_after"].get(s)) for s in kept}
        move_ok = frac <= MOVE_MAX and (parsed is None or parsed <= mv["moved"])
        kept_ok = all(a == b for a, b in unchanged.values())
        out["criterion_4to5"] = {"moved": mv["moved"], "indexed": mv["indexed"], "fraction": round(frac, 4),
                                 "parsed": parsed, "kept": kept, "kept_generations": unchanged,
                                 "rewritten": job.get("affected"), "pass_moved": move_ok, "pass_kept": kept_ok}
        ok = ok and move_ok and kept_ok
    else:
        out["criterion_4to5"] = {"error": "no move line in the writers' logs"}
        ok = False
    pe = [e for e in rp["embeds"] if e["project"] == "pharos"]
    misses = sum(e["misses"] for e in pe)
    hits = sum(e["hits"] for e in pe)
    emb_ok = misses == 0 and hits > 0
    out["criterion_pharos_embed"] = {"embed_lines": pe, "provider_calls": misses, "cache_hits": hits,
                                     "pass": emb_ok}
    ok = ok and emb_ok
    out["pass"] = ok
    save_part("reshard", out)
    log(f"RESHARD 4->5 {out['criterion_4to5']}; pharos embed {out['criterion_pharos_embed']}")
    return ok


def save_part(name, data):
    RESULTS[name] = data
    save()


def vector_recall_verdict(ref, got, st):
    """Ruling P6-R17: after a resplit the shards' HNSW graphs differ, so vector parity is judged by recall:
    per-query top-10 overlap >= VECTOR_MIN_OVERLAP, mean overlap >= VECTOR_MEAN_OVERLAP, and the documents both
    lists share have bitwise-identical raw (pre-graph-boost) scores (ruling P6-R20: the page-normalised graph
    boost moves the final scores whenever recall brings in a document of another in-degree). Overlap =
    |shared| / max(|ref|, |got|)."""
    overlaps, worst, score_diffs = [], [], []
    missing_raw = 0
    for q, a in ref.items():
        b = got.get(q, [])
        def raw(r):
            v = (r.get("raw") or {}).get("vector")
            return p2bits(v if v is not None else r.get("score"))
        missing_raw += sum(1 for r in a + b if (r.get("raw") or {}).get("vector") is None)
        sa = {r.get("id"): raw(r) for r in a}
        sb = {r.get("id"): raw(r) for r in b}
        shared = set(sa) & set(sb)
        ov = len(shared) / max(1, len(sa), len(sb))
        overlaps.append(ov)
        if ov < VECTOR_MIN_OVERLAP:
            worst.append((q, round(ov, 2)))
        for i in shared:
            if sa[i] != sb[i]:
                score_diffs.append((q, i, sa[i], sb[i]))
    mean = sum(overlaps) / max(1, len(overlaps))
    ok = not worst and mean >= VECTOR_MEAN_OVERLAP and not score_diffs
    st.update({"recall_mean_overlap": mean, "recall_below_min": worst, "recall_shared_score_diffs": score_diffs[:20],
               "results_without_raw_score": missing_raw})
    return ok, (f"P6-R17 recall: mean overlap {mean:.4f} (>= {VECTOR_MEAN_OVERLAP}), queries below {VECTOR_MIN_OVERLAP}:"
                f" {len(worst)}, shared docs with other raw-score bits: {len(score_diffs)}"
                f" (results without a raw score: {missing_raw})")


def keyword_drift(ref, got, st):
    """Ruling P6-R20: on keyword pages with identical ids, the largest relative score difference."""
    worst, over = 0.0, []
    for q, a in ref.items():
        b = got.get(q, [])
        if [r.get("id") for r in a] != [r.get("id") for r in b]:
            continue
        for x, y in zip(a, b):
            sx, sy = float(x.get("score") or 0), float(y.get("score") or 0)
            rel = abs(sx - sy) / max(abs(sx), abs(sy), 1e-30)
            worst = max(worst, rel)
            if rel > KEYWORD_MAX_REL_DRIFT:
                over.append((q, x.get("id"), sx, sy))
    st.update({"keyword_max_rel_drift": worst, "keyword_drift_over_bar": over[:20]})
    return not over, f"identical pages: max relative score drift {worst:.2e} (<= {KEYWORD_MAX_REL_DRIFT:.0e})"


def p2bits(score):
    import struct
    return None if score is None else struct.unpack(">I", struct.pack(">f", float(score)))[0]


def compare(ref_label, got_label, key):
    ref = json.loads((gate() / f"ranking-{ref_label}.json").read_text())
    got = json.loads((gate() / f"ranking-{got_label}.json").read_text())
    ok, checks = True, []
    for pl in PIPELINES:
        st = p3.compare_rank(p3.by_pipeline(ref, pl), p3.by_pipeline(got, pl), pl)
        good, rule = p3.verdict(st)
        if pl == "vector":
            good, rule = vector_recall_verdict(p3.by_pipeline(ref, pl), p3.by_pipeline(got, pl), st)
        if pl == "keyword":
            drift_ok, drift_rule = keyword_drift(p3.by_pipeline(ref, pl), p3.by_pipeline(got, pl), st)
            good, rule = good and drift_ok, rule + "; " + drift_rule
        log(f"   {key} {pl:8s} n={st['queries']} {rule}; ids identical {st['top10_identical']}, score-bitwise"
            f" {st['score_bitwise_identical']}, top-1 {st['top1_identical_pct']:.1f}%, tau {st['mean_tau']:.4f}"
            f" -> {'PASS' if good else 'FAIL'}")
        for d in st["diffs"][:5]:
            log(f"      diff {d['q']!r}: ref {d['ref'][:4]} got {d['got'][:4]}")
        st.pop("taus_absent", None)
        st.pop("taus", None)
        st["pass"], st["rule"] = good, rule
        checks.append(st)
        ok = ok and good
    return ok, checks


def phase_after():
    _, fails = ranking_pass("after")
    ok, checks = compare("before", "after", "after-vs-before")
    RESULTS["after"] = {"failures": fails, "checks": checks, "pass": ok and not fails}
    save()
    return ok and not fails


def traced(params):
    q = dict(params, trace="true", limit=10)
    with urllib.request.urlopen(f"http://127.0.0.1:{PORTS['router']}/api/search?" + urllib.parse.urlencode(q),
                                timeout=60) as r:
        data = json.loads(r.read())
    meta = data.get("searchMeta", {})
    stages = [s["name"] for s in meta.get("stages", [])]
    fan = [int(m.group(1)) for s in stages for m in [re.search(r"fan-out \((\d+) shards\)", s)] if m]
    return {"params": params, "routing": meta.get("routing", []), "stages": stages, "fanout_shards": fan,
            "results": [(x.get("id"), x.get("filePath")) for x in data.get("results", [])],
            "warnings": data.get("warnings")}


def phase_pruning():
    b = MON.b
    ph = b.project("pharos")
    lu = b.project("lucene")
    _, ph_shards = active_spec(ph)
    _, lu_shards = active_spec(lu)
    mod = traced({"q": "Calculator", "pipeline": "keyword", "project": "pharos", "module": PHAROS_MODULE})
    mod_h = traced({"q": "greeting service", "pipeline": "hybrid", "project": "pharos", "module": PHAROS_MODULE})
    path = traced({"q": "IndexWriter", "pipeline": "keyword", "project": "lucene", "pathPrefix": LUCENE_PREFIX})
    n = len(ph_shards)
    mod_ok = (any(f"search: 1 of {n}" in r for r in mod["routing"]) and mod["fanout_shards"] == [1]
              and mod["results"] and all(PHAROS_MODULE in (fp or "") for _, fp in mod["results"]))
    path_ok = (path["fanout_shards"] == [len(lu_shards)] and any("path prefix" in r for r in path["routing"])
               and path["results"])
    RESULTS["pruning"] = {"pharos_shards": ph_shards, "lucene_shards": lu_shards, "module_keyword": mod,
                          "module_hybrid_info": mod_h, "path_keyword": path, "pass_module": bool(mod_ok),
                          "pass_path": bool(path_ok)}
    save()
    log(f"PRUNING module: routing {mod['routing']} fan-out {mod['fanout_shards']} -> {mod_ok}; path: routing"
        f" {path['routing']} fan-out {path['fanout_shards']} of {len(lu_shards)} -> {path_ok}")
    return bool(mod_ok and path_ok)


def phase_interrupt():
    rec = reshard("lucene", SPEC_RING6, "lucene ring 5 -> 6, writer killed mid-build", kill_mid=True)
    job = rec["job"] or {}
    killed = rec["killed"]
    other = "gate-" + [w for w in WRITERS if w != killed["writer"]][0]
    resumed = job.get("writer") == other and job.get("spec") == rec["to_version"]
    sw = rec["served_while_open"]
    n_ok = sw["probes_at_version_N"] > 0 and sw["ok_with_results"] == sw["probes_at_version_N"]
    win = [s for s in MON.window(T0 + killed["t"], T0 + rec["t_flip"]) if s[2] == "lucene" or s[2] is None]
    rec["load_during_takeover"] = {"queries": len(win), "failed": sum(1 for s in win if failed(s))}
    rec["pass_version_N_served"] = bool(n_ok and killed["active_version_at_kill"] == rec["from_version"]
                                        and rec["load_during_takeover"]["failed"] == 0)
    rec["pass_resumed_and_flipped"] = bool(resumed)
    RESULTS["interrupt"] = rec
    save()
    log(f"INTERRUPT served-N {rec['pass_version_N_served']} ({sw}), resumed on {job.get('writer')} -> "
        f"{rec['pass_resumed_and_flipped']}")
    # the final ranking after ring 6 (info)
    ranking_pass("final")
    ok, checks = compare("before", "final", "final-vs-before [info]")
    RESULTS["final_ranking_info"] = {"checks": checks, "pass": ok}
    save()
    return rec["pass_version_N_served"] and rec["pass_resumed_and_flipped"]


def phase_check():
    MON.load_on.clear()
    time.sleep(5)
    with MON.lock:
        s = list(MON.samples)
    fails = [x for x in s if failed(x)]
    by_kind = {}
    for x in s:
        k = by_kind.setdefault(x[1], {"n": 0, "failed": 0, "lat": []})
        k["n"] += 1
        k["failed"] += failed(x)
        k["lat"].append(x[4])
    for k in by_kind.values():
        lat = sorted(k.pop("lat"))
        k["p50_ms"] = round(1000 * lat[len(lat) // 2])
        k["p95_ms"] = round(1000 * lat[min(len(lat) - 1, int(0.95 * len(lat)))])
    warn_counts = {}
    for x in s:
        for w in x[6]:
            warn_counts[w[:140]] = warn_counts.get(w[:140], 0) + 1
    cat = MON.b.catalog()
    home = Path.home()
    newer = subprocess.run(["find", str(home / ".pharos"), str(home / ".djl.ai"), "-newer", str(gate() / "marker")],
                           capture_output=True, text=True)
    RESULTS["check"] = {"queries": len(s), "failed": len(fails), "failed_samples": fails[:20], "by_kind": by_kind,
                        "warnings": warn_counts, "catalog_projects": cat.get("projects"),
                        "marker_newer": newer.stdout.strip(), "marker_err": newer.stderr.strip()[:200],
                        "rss_mib": {n: NODES[n].rss_mib() for n in NODES if n != "cli"}}
    save()
    log(f"CHECK queries {len(s)}, failed {len(fails)}; by kind {by_kind}; marker newer: {newer.stdout.strip()!r}")
    return not fails


PHASES = {"setup": phase_setup, "before": phase_before, "reshard": phase_reshard, "after": phase_after,
          "pruning": phase_pruning, "interrupt": phase_interrupt, "check": phase_check}


def _stop_on_signal(signum, frame):
    # a background job of a non-interactive shell starts with SIGINT ignored: install both handlers, so a
    # stop by PID (SIGTERM or SIGINT) still runs main's cleanup, which stops every JVM by its PID
    raise KeyboardInterrupt(f"signal {signum}")


def main():
    signal.signal(signal.SIGTERM, _stop_on_signal)
    signal.signal(signal.SIGINT, _stop_on_signal)
    ap = argparse.ArgumentParser()
    ap.add_argument("--phases", default=",".join(PHASES))
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    phases = [p for p in a.phases.split(",") if p]
    if a.dry_run:
        print(__doc__)
        print("phases:", phases)
        return 0
    (gate() / "logs").mkdir(exist_ok=True)
    verdicts = {}
    try:
        for p in phases:
            log(f"== phase {p}")
            try:
                verdicts[p] = PHASES[p]()
            except Exception as e:
                log(f"phase {p} ERROR: {e!r}")
                verdicts[p] = False
                RESULTS.setdefault("errors", {})[p] = repr(e)
                save()
                if p in ("setup", "reshard"):
                    break
            log(f"== phase {p}: {'PASS' if verdicts[p] else 'FAIL'}")
    finally:
        if MON:
            MON.close()
        stop_all()
        RESULTS["verdicts"] = verdicts
        save()
    log(f"verdicts {verdicts}")
    return 0 if verdicts and all(verdicts.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
