#!/usr/bin/env python3
"""Phase 5 gate driver (standard library only): the Phase 1-3 gates with NO bucket, plus writer down
and writer load. Driven by scripts/cluster-gate-phase5.sh, which has written the writer configs
(cluster.backend peer-pull), indexed lucene (--no-embed), pharos and mypal (jina) on W1 and the alpha
fixture on W2, and built the single-node reference home. There is no MinIO and no object store.

Every reader finds writers ONLY through cluster.peers.discovery `seeds:http://127.0.0.1:7300`; W1's
peer list names :7300 and :7310, so W2 is learned by gossip. Coordinators and `pharos router` find
readers only through peer membership (heartbeats POSTed to the writers). Every JVM is a Popen child,
started and stopped by its PID; each has its own user.home and pharos.home under $GATE.

  p1         Phase 1 re-run. W1 (`pharos web`, :7300), the reference (role all, no models, :7305) and a
             lazy reader (:7301). cluster_gate.compare: the 99 pharos+lucene keyword queries, callers,
             usages and impact, reader vs single node, 0 mismatches. cluster_gate.freshness: a probe
             method appended to pharos, re-indexed incrementally by a separate `pharos index` JVM on
             W1's home (every peer-mode publish comes from such a process), visible on the reader
             within 10 s + download + 5 s. Then the second writer: W2 (:7310, project alpha) starts
             mid-run under 10 QPS of keyword load on the reader; pass: the reader serves alpha within
             2 refreshes (2 x 10 s) of W2 answering, and 0 failed queries.
  p2cold, p2warm, p2cap   cluster_gate_phase2's cold (20 trials, lucene first query p95 < 3 s), warm
             (GATE_WARM_RUNS runs, default 10: median per-run keyword p95 ratio lazy/full <= 1.10 and
             CI upper <= 1.15; top-10 bitwise vs MMap; NIOFS tolerant) and cap phases, unchanged, on
             peer-pull readers pinned to lucene and pharos.
  reference  cluster_gate_phase3's reference (copy of W1's indexes as they are now, role all).
  ranking    cluster_gate_phase3's ranking criteria. R1 {pharos} and R2 {lucene, mypal} run together
             (each coordinates over the other), then R3 (all) alone (memory: never three jina readers).
  router     Phase 3 elasticity with peer membership: router :7080 over R1 and R2, 20 QPS for 180 s;
             R4 {lucene, mypal} joins at 60 s, R2 gets SIGTERM at 120 s. Pass: 0 failed, 0 "unavailable".
  down       Writer down. Readers D1 (full) and D2 (lazy) serve every shard behind the router; ~10 QPS
             of keyword through the router; W1 gets SIGKILL at 10 s; the load runs 2 min after that
             (pass: 0 failed); at 50 s D2 is restarted (SIGTERM, same home) and must serve its on-disk
             generations (ready, every warmed query answered as before); W1 restarts, readers resume
             long-polling (W1's /health peers.transfers.longPolls > 0), then a new pharos generation
             published by a separate index JVM is visible on D1 and D2 within 5 s.
  load       Writer load. A single cold full-mode reader (3 trials, heap GATE_LOAD_HEAP, default 384m,
             no models) sets the single-reader cold time; then 6 cold readers start at once. Pass: W1's
             transfers in flight never exceed maxTransfersPerWriter (sampled every 50 ms, and its own
             peakTransfersInFlight), and every reader is serving within 3 x the single-reader time.
             Bytes from peers and from the writer are reported.
  check      Marker check (nothing under ~/.pharos or ~/.djl.ai newer than $GATE/marker), every gate
             process stopped, every gate port free.

Results: $GATE/results-<phase>.json; memory samples: $GATE/memory.log. --dry-run prints the plan;
--self-test checks the helpers.
"""
import argparse
import concurrent.futures
import json
import os
import re
import shutil
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
import cluster_gate as p1  # noqa: E402
import cluster_gate_phase2 as p2  # noqa: E402
import cluster_gate_phase3 as p3  # noqa: E402

PHASES = ["p1", "p2cold", "p2warm", "p2cap", "reference", "ranking", "router", "down", "load", "check"]
REFRESH_S = int(os.environ.get("GATE_REFRESH_SECONDS", "10"))
W1_URL = "http://127.0.0.1:7300"
W2_URL = "http://127.0.0.1:7310"
GATE_PORTS = [7080, 7300, 7301, 7302, 7303, 7304, 7305, 7310, 7321, 7322, 7323, 7324, 7325, 7326, 7327]
LOAD_READERS = 6
DOWN_BAR_S = 5.0
JOIN_REFRESHES = 2
ALL = []  # every Popen-backed node this driver started (for stop_all and the check phase)


def gate():
    return Path(os.environ["GATE"])


def now():
    return time.perf_counter()


def get_json(url, timeout=10, token=False):
    h = {"X-Pharos-Cluster-Token": os.environ["GATE_TOKEN"]} if token else {}
    return p3.get_json(url, timeout=timeout, headers=h)


def health(base, timeout=5):
    """/health JSON whatever the status code (a warming reader answers 503 with a body)."""
    try:
        with urllib.request.urlopen(base + "/health", timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read())
        except ValueError:
            return {"status": f"http {e.code}"}


def save(name, data):
    p = gate() / f"results-{name}.json"
    p.write_text(json.dumps(data, indent=2, default=str))
    print(f"   results -> {p}")


# --------------------------------------------------------------------------- memory monitor


def child_rss():
    """{pid: (RSS MiB, command tail)} of this driver's child processes (every gate JVM is one)."""
    me, out = os.getpid(), {}
    for d in Path("/proc").iterdir():
        if not d.name.isdigit():
            continue
        try:
            stat = (d / "stat").read_text()
            ppid = int(stat[stat.rfind(")") + 2:].split()[1])
            if ppid != me:
                continue
            rss = 0
            for ln in (d / "status").read_text().splitlines():
                if ln.startswith("VmRSS"):
                    rss = int(ln.split()[1]) // 1024
            cmd = (d / "cmdline").read_bytes().split(b"\0")
            home = next((c.decode()[len("-Duser.home="):] for c in cmd if c.startswith(b"-Duser.home=")), "?")
            out[int(d.name)] = (rss, home.rsplit("/", 1)[-1])
        except (OSError, ValueError, IndexError):
            continue
    return out


class MemoryMonitor:
    """MemAvailable and the gate JVMs' RSS every 5 s to $GATE/memory.log; keeps the minimum."""

    def __init__(self):
        self.min_avail = float("inf")
        self.max_rss = 0
        self.stop_ev = threading.Event()
        self.th = threading.Thread(target=self.run, daemon=True)

    def run(self):
        with open(gate() / "memory.log", "a") as f:
            while not self.stop_ev.is_set():
                avail = p2.mem_available_gib()
                kids = child_rss()
                rss = {f"{name}:{pid}": v for pid, (v, name) in kids.items()}
                tot = sum(rss.values())
                self.min_avail = min(self.min_avail, avail)
                self.max_rss = max(self.max_rss, tot)
                f.write(json.dumps({"t": time.strftime("%H:%M:%S"), "memAvailableGiB": round(avail, 2),
                                    "gateRssMiB": tot, "rss": rss}) + "\n")
                f.flush()
                self.stop_ev.wait(5)


MEM = None


# --------------------------------------------------------------------------- nodes


def jvm(heap, home):
    return ["java", "-Xmx" + heap, "--enable-native-access=ALL-UNNAMED",
            "--add-opens", "java.base/java.nio.channels.spi=ALL-UNNAMED", "--add-modules", "jdk.incubator.vector",
            "-Dpolyglot.engine.WarnInterpreterOnly=false", "-Dlogback.configurationFile=" + os.environ["GATE_LOGBACK"],
            "-Duser.home=" + str(home), "-Dpharos.home=" + str(home / ".pharos")]


class Proc:
    name = "?"
    proc = None
    log = None

    def popen(self, cmd, tag):
        self.log = gate() / f"{self.name}{tag}.log"
        lf = open(self.log, "w")
        self.t_start = now()
        self.proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT)
        lf.close()
        if self not in ALL:
            ALL.append(self)
        return self

    def rss_mib(self):
        try:
            for ln in open(f"/proc/{self.proc.pid}/status"):
                if ln.startswith("VmRSS"):
                    return int(ln.split()[1]) // 1024
        except (OSError, AttributeError):
            pass
        return None

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def stop(self, timeout=60):
        if self.alive():
            self.proc.terminate()  # by PID (Popen), never by name
            try:
                self.proc.wait(timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        rc = self.proc.returncode if self.proc else None
        self.proc = None
        return rc

    def kill9(self):
        os.kill(self.proc.pid, signal.SIGKILL)  # by PID
        rc = self.proc.wait(30)
        self.proc = None
        return rc


class Writer(Proc):
    """`pharos web` with W1's or W2's config (written by the shell script: role writer, peer-pull)."""

    def __init__(self, name, port):
        self.name, self.port = name, port
        self.base = f"http://127.0.0.1:{port}"
        self.home = gate() / "nodes" / name

    def start(self, tag=""):
        heap = os.environ.get("GATE_WRITER_HEAP", "512m")
        return self.popen(jvm(heap, self.home) + ["-jar", os.environ["GATE_JAR"], "web", "--port", str(self.port),
                                                 "--no-open"], f"-web{tag}")

    def wait_up(self, timeout=120):
        """Seconds until /replication/catalog answers (the writer serves its shards)."""
        end = now() + timeout
        while now() < end:
            if not self.alive():
                raise RuntimeError(f"{self.name} exited; see {self.log}")
            try:
                cat = get_json(self.base + "/replication/catalog", timeout=2, token=True)
                if cat.get("shards"):
                    return now() - self.t_start, cat
            except (OSError, ValueError):
                pass
            time.sleep(0.1)
        raise RuntimeError(f"{self.name} not up after {timeout}s; see {self.log}")

    def transfers(self):
        return ((health(self.base, timeout=2).get("peers") or {}).get("transfers")) or {}


class Peer(p2.Reader, Proc):
    """A peer-pull reader (p2.peer_cluster), the router (`pharos router` on the same config), or the
    single-node reference (role all, no cluster). Models off unless asked (keyword-only phases)."""

    def __init__(self, name, port, mode="lazy", shards=None, models=False, cross=False, kind="reader",
                 reader_cfg=None, fresh=False, heap=None, home=None):
        p2.Reader.__init__(self, name, port, mode=mode, reader_cfg=reader_cfg, fresh=False)
        self.kind, self.models, self.cross, self.heap = kind, models, cross, heap
        self.shards = list(shards or [])
        if home is not None:
            self.home = home
            self.ph = home / ".pharos"
            self.cache_dir = self.ph / "cluster-cache"
        if fresh and self.home.exists():
            shutil.rmtree(self.home)

    def write_config(self):
        self.ph.mkdir(parents=True, exist_ok=True)
        link = self.home / ".djl.ai"
        if not link.is_symlink():
            link.symlink_to(os.environ["GATE_MODELS"])
        cfg = {"crossEncoderEnabled": self.cross}
        if self.models:
            cfg["embeddingProviders"] = [{"type": "djl", "modelId": os.environ["GATE_MODEL_ID"],
                                          "url": os.environ["GATE_MODEL_URL"], "dimensions": 768, "maxTokens": 512}]
            cfg["searchEmbeddingModel"] = os.environ["GATE_MODEL_ID"]
        if self.kind != "reference":
            rc = {"mode": self.mode, "shards": self.shards}
            rc.update(self.reader_cfg)
            cfg["cluster"] = p2.peer_cluster(self.name, self.port, self.cache_dir, rc)
        (self.ph / "config.json").write_text(json.dumps(cfg, indent=2))

    def start(self, tag=""):
        self.write_config()
        heap = self.heap or os.environ.get("GATE_WEB_HEAP", "640m")
        cmd = jvm(heap, self.home) + ["-jar", os.environ["GATE_JAR"]]
        cmd += ["router", "--port", str(self.port)] if self.kind == "router" else \
            ["web", "--port", str(self.port), "--no-open"]
        return self.popen(cmd, tag)

    def wait_ready(self, want=None, timeout=600):
        """Seconds from JVM start until /health ready (reader: and 'serving N shard(s)' with N == want)."""
        end = now() + timeout
        pat = re.compile(r"serving (\d+) shard\(s\)")
        while now() < end:
            if not self.alive():
                raise RuntimeError(f"{self.name} exited {self.proc and self.proc.returncode}; see {self.log}")
            try:
                h = health(self.base, timeout=2)
                if self.kind == "router":
                    if h.get("status") in ("ok", "degraded"):
                        return now() - self.t_start
                elif h.get("ready") or (self.kind == "reference" and h.get("status") == "ok"):
                    if self.kind == "reader" and want is not None:
                        m = pat.findall(self.log.read_text(errors="replace"))
                        if not m or int(m[-1]) != want:
                            raise RuntimeError(f"{self.name} ready, serving {m[-1] if m else '?'} shard(s), not {want}")
                    return now() - self.t_start
            except (OSError, ValueError):
                pass
            time.sleep(0.05)
        raise RuntimeError(f"{self.name} not ready after {timeout}s; see {self.log}")

    def stop(self, timeout=60):
        return Proc.stop(self, timeout)

    def peers(self):
        return health(self.base).get("peers") or {}


def start_all(nodes, want=None, min_mem_gib=1.0):
    for n in nodes:
        m = p2.mem_available_gib()
        if m < min_mem_gib:
            raise RuntimeError(f"MemAvailable {m:.1f} GiB < {min_mem_gib}: not starting {n.name}")
        n.start()
    out = {}
    for n in nodes:
        out[n.name] = round(n.wait_ready(want=(want or {}).get(n.name)), 2)
        print(f"   {n.name} :{n.port} ready after {out[n.name]} s (pid {n.proc.pid}, RSS {n.rss_mib()} MiB)")
    return out


def stop_all():
    for n in reversed(ALL):
        try:
            n.stop(timeout=90)
        except Exception as e:  # keep stopping the others
            print(f"   stopping {n.name}: {e}")


W1 = None
W1_SHARDS = []  # the shards W1's catalog lists (a reader serving everything serves this many)


def ensure_w1():
    global W1, W1_SHARDS
    if W1 is None:
        W1 = Writer("w1", 7300)
    if not W1.alive():
        W1.start()
        t, cat = W1.wait_up()
        W1_SHARDS = sorted(s["shard"] for s in cat["shards"])
        print(f"-- W1 `pharos web` :7300 up in {t:.1f}s (pid {W1.proc.pid}); catalog "
              f"{[(s['shard'], s['latest']) for s in cat['shards']]}")
    return W1


# --------------------------------------------------------------------------- load


def load_plan(qps, seconds, mix="keyword"):
    """(at, query, project, pipeline): the 99 pharos+lucene queries cycled; half unscoped, half scoped
    to the query's own project (keyword); mix 'phase3' is the Phase 3 elasticity mix (with hybrid)."""
    base = p1.load_queries()
    plan = []
    for i in range(int(qps * seconds)):
        q, proj = base[i % len(base)]
        k = i % 4
        if mix == "phase3":
            plan.append((i / qps, q, [None, proj, None, "pharos"][k], ["keyword", "keyword", "hybrid", "hybrid"][k]))
        else:
            plan.append((i / qps, q, None if k % 2 == 0 else proj, "keyword"))
    return plan


def run_load(target, plan, workers=32, timeout=60):
    """Runs the plan open-loop against target (a base URL or a callable giving one). Samples sorted by t."""
    samples, lock = [], threading.Lock()
    t0 = now()

    def one(item):
        at, q, proj, pl = item
        d = at - (now() - t0)
        if d > 0:
            time.sleep(d)
        ts = now() - t0
        base = target() if callable(target) else target
        dt, status, results, warns = p3.fetch(base, q, pl, proj, timeout=timeout)
        with lock:
            samples.append({"t": ts, "lat": dt, "status": status, "q": q, "project": proj, "pipeline": pl,
                            "hits": len(results), "warnings": warns, "unavailable": p3.has_unavailable(warns)})

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(one, plan))
    samples.sort(key=lambda r: r["t"])
    return samples, t0


def summarise_load(samples, planned):
    failed = [r for r in samples if r["status"] != 200]
    unav = [r for r in samples if r["unavailable"]]
    by = {}
    for r in samples:
        by[str(r["status"])] = by.get(str(r["status"]), 0) + 1
    lat = p2.summary([r["lat"] for r in samples]) if samples else {}
    warn = {}
    for r in samples:
        for w in r["warnings"]:
            warn[w[:160]] = warn.get(w[:160], 0) + 1
    return {"planned": planned, "requests": len(samples), "failed": len(failed), "unavailable": len(unav),
            "by_status": by, "warnings": warn, "p50_ms": 1000 * lat.get("p50", 0), "p95_ms": 1000 * lat.get("p95", 0),
            "max_ms": 1000 * lat.get("max", 0), "failed_samples": failed[:20], "unavailable_samples": unav[:10]}


# --------------------------------------------------------------------------- p1: Phase 1 + second writer


def append_probe(src_root, marker):
    target = next(Path(src_root).rglob("BordaMerger.java"), None) or next(Path(src_root).rglob("*.java"))
    text = target.read_text()
    idx = text.rstrip().rfind("}")
    target.write_text(text[:idx] + f"\n    public static int {marker}() {{ return 42; }}\n" + text[idx:])
    return target


def reindex_cmd(heap):
    home = gate() / "nodes" / "w1"
    return jvm(heap, home) + ["-jar", os.environ["GATE_JAR"], "index", str(Path(os.environ["GATE_SRC"]) / "pharos"),
                              "--project", "pharos", "--no-progress"]


def phase_p1():
    w1 = ensure_w1()
    out = {"w1_shards": W1_SHARDS}
    rebuild_reference()  # W1's indexes as they are now: the single node of the comparison
    ref = Peer("reference", 7305, kind="reference", home=gate() / "reference", heap=os.environ.get("GATE_REF_HEAP", "2g"))
    r = Peer("p1", 7301, mode="lazy", fresh=True)  # every shard: lucene, pharos, mypal (W2 not up yet)
    print(f"-- p1: reference :7305 (role all, no models) and reader p1 :7301 (lazy, seeds:{W1_URL})")
    out["ready_s"] = start_all([ref, r], want={"p1": len(W1_SHARDS)})
    args = argparse.Namespace(writer=ref.base, reader=r.base)
    out["compare"] = p1.compare(args)
    ref.stop()
    # Freshness: a separate `pharos index` JVM on W1's home publishes pharos generation N+1.
    os.environ["GATE_REINDEX_CMD"] = " ".join(reindex_cmd(os.environ.get("GATE_INDEX_HEAP", "3g")))
    os.environ["GATE_REINDEX_LOG"] = str(gate() / "p1-freshness-reindex.log")
    os.environ["GATE_PHAROS_SRC"] = str(Path(os.environ["GATE_SRC"]) / "pharos")
    os.environ["GATE_READER_LOG"] = str(r.log)
    os.environ["GATE_POLL_INTERVAL"] = str(REFRESH_S)
    print(f"-- p1 freshness (bound {REFRESH_S} s refresh + download + 5 s); MemAvailable {p2.mem_available_gib():.1f} GiB")
    out["freshness"] = p1.freshness(args)

    # Second writer mid-run, under keyword load on the reader.
    w2 = Writer("w2", 7310)
    plan = load_plan(10, 90)
    res = {}

    def loader():
        res["samples"], res["t0"] = run_load(r.base, plan)

    th = threading.Thread(target=loader, daemon=True)
    t_load = now()
    th.start()
    time.sleep(15)
    print(f"-- p1 second writer: start W2 :7310 (alpha) at t={now() - t_load:.1f}s of the load")
    w2.start()
    up_s, cat = w2.wait_up()
    t_up = now()
    print(f"   W2 answers /replication/catalog after {up_s:.1f}s: {cat['shards']}")
    t_seen = t_alpha = None
    probes = []
    while now() - t_up < 120 and t_alpha is None:
        if t_seen is None:
            ws = r.peers().get("writers") or []
            if any(w.get("node") == "gate-w2" and w.get("live") for w in ws):
                t_seen = now() - t_up
        dt, status, results, warns = p3.fetch(r.base, "Inventory add", "keyword", "alpha", timeout=10)
        probes.append({"t": round(now() - t_up, 2), "status": status, "hits": len(results)})
        if status == 200 and any((x.get("id") or "").startswith("alpha:") for x in results):
            t_alpha = now() - t_up
        else:
            time.sleep(0.25)
    th.join(200)
    samples = res.get("samples", [])
    ld = summarise_load(samples, len(plan))
    bar = JOIN_REFRESHES * REFRESH_S
    join_ok = t_alpha is not None and t_alpha <= bar and ld["failed"] == 0 and ld["requests"] == len(plan)
    out["second_writer"] = {"w2_up_s": up_s, "seen_live_after_s": t_seen, "alpha_served_after_s": t_alpha,
                            "bar_s": bar, "probes": probes[-5:], "load": ld, "pass": join_ok,
                            "reader_writers": r.peers().get("writers")}
    print(f"P1 SECOND WRITER: reader lists W2 live after {t_seen} s, serves alpha after "
          f"{t_alpha if t_alpha is None else round(t_alpha, 2)} s (bar {bar} s = {JOIN_REFRESHES} refreshes);"
          f" load {ld['requests']}/{len(plan)}, failed {ld['failed']}, unavailable {ld['unavailable']},"
          f" p95 {ld['p95_ms']:.0f} ms -> {'PASS' if join_ok else 'FAIL'}")
    w2.stop()
    r.stop()
    out["pass"] = {"compare": out["compare"], "freshness": out["freshness"], "second_writer": join_ok}
    save("p1", out)
    return all(out["pass"].values())


# --------------------------------------------------------------------------- p2: Phase 2 phases


def phase_p2(which):
    ensure_w1()
    os.environ["GATE_READER_SHARDS"] = "lucene,pharos"  # Phase 2 is about lucene and pharos (2 shards)
    os.environ.setdefault("GATE_WARM_RUNS", "10")
    try:
        if which == "p2cold":
            return p2.phase_cold(int(os.environ.get("GATE_COLD_TRIALS", "20")), "cold")
        if which == "p2warm":
            return p2.phase_warm()
        return p2.phase_cap()
    finally:
        os.environ.pop("GATE_READER_SHARDS", None)


# --------------------------------------------------------------------------- Phase 3: reference, ranking, router


def rebuild_reference():
    """The reference is a copy of W1's indexes as they are now (p1's freshness published a new
    pharos generation): exactly what readers serve. W1 never writes its index while serving."""
    src, dst = gate() / "nodes" / "w1" / ".pharos", gate() / "reference" / ".pharos"
    if dst.parent.exists():
        shutil.rmtree(dst.parent)
    skip = {"peer-store", "cluster-cache", "logs", "config.json"}
    shutil.copytree(src, dst, ignore=lambda d, names: [n for n in names if Path(d) == src and
                                                       (n in skip or n.endswith((".pid", ".lock")))])
    reg = dst / "registry.json"
    reg.write_text(reg.read_text().replace(str(src) + "/", str(dst) + "/"))


def phase_reference():
    ensure_w1()
    rebuild_reference()
    return p3.phase_reference()


def phase_ranking():
    """cluster_gate_phase3.phase_ranking's criteria; R1+R2 together, then R3 alone (memory)."""
    ensure_w1()
    sp = p3.specs()
    g = gate()
    ref = json.loads((g / "ref-results.json").read_text())
    ref_warn = json.loads((g / "ref-warnings.json").read_text())
    out = {"answers": len(sp), "projects": p3.PROJECTS, "shards": p3.SHARDS, "nodes": {}, "checks": [], "rss_mib": {}}
    got, ok = {}, True

    def measure(names):
        nonlocal ok
        for name in names:
            res, warns, fails = p3.run_queries(p3.NODES[name].base, sp)  # warm-up pass
            res, warns, fails = p3.run_queries(p3.NODES[name].base, sp)
            got[name] = res
            (g / f"{name}-results.json").write_text(json.dumps(res))
            per = p3.LAST_WARNINGS[p3.NODES[name].base]
            same = sum(1 for k in ref_warn if per.get(k) == ref_warn[k])
            out["nodes"][name] = {"warnings": warns, "failures": fails, "warnings_identical": same}
            print(f"RANKING {name}: failures {len(fails)}; warnings {warns}; per-answer warnings identical"
                  f" {same}/{len(ref_warn)}")
            for f in fails[:10]:
                print("   " + f)
            ok = ok and not fails and same == len(ref_warn)
        for name in names:
            out["rss_mib"][name] = p3.NODES[name].rss_mib()

    for name in ("r1", "r2"):
        p3.NODES[name] = p3.Node(name, shards=p3.SHARDS[name], fresh=True)
        p3.start(p3.NODES[name])
    states = p3.wait_cluster(p3.NODES["r1"].base, ["r1", "r2"])
    out["membership_r1"] = states
    measure(["r1", "r2"])
    for name in ("r1", "r2"):
        p3.NODES[name].stop()
    p3.NODES["r3"] = p3.Node("r3", shards=p3.SHARDS["r3"], fresh=True)
    p3.start(p3.NODES["r3"])
    measure(["r3"])
    p3.NODES["r3"].stop()
    checks = [("R1 {pharos} unscoped (fan-out)", "r1", pl, "brief", True) for pl in p3.PIPELINES]
    checks += [("R2 {%s} unscoped (fan-out)" % ", ".join(p3.SHARDS["r2"]), "r2", pl, "brief", False)
               for pl in p3.PIPELINES]
    checks += [("R3 (all) local", "r3", pl, "bitwise", True) for pl in p3.PIPELINES]
    for label, node, pl, rule, gated in checks:
        good, st = p3.report_cmp(label + ("" if gated else " [info]"), ref, got[node], pl, None, rule)
        st.update({"label": label, "node": node, "gated": gated})
        out["checks"].append(st)
        if gated:
            ok = ok and good
    out["pass"] = ok
    save("ranking", out)
    return ok


def phase_router():
    """Phase 3 elasticity through `pharos router` over peer membership: R1 {pharos}, R2 {lucene, mypal};
    R4 {lucene, mypal} joins, R2 drains (R4 covers its shards). Same load and pass rule as Phase 3."""
    ensure_w1()
    load_s = float(os.environ.get("GATE_LOAD_SECONDS", "180"))
    qps = float(os.environ.get("GATE_QPS", "20"))
    join_at = float(os.environ.get("GATE_JOIN_AT", "60"))
    drain_at = float(os.environ.get("GATE_DRAIN_AT", "120"))
    p3.SHARDS["r4"] = list(p3.SHARDS["r2"])
    # The Phase 3 mix is keyword and hybrid: no reranking, so no cross-encoder in memory (6 GB budget).
    os.environ["GATE_CROSS_ENCODER"] = "0"
    for name in ("r1", "r2"):
        if name not in p3.NODES or p3.NODES[name].proc is None:
            p3.NODES[name] = p3.Node(name, shards=p3.SHARDS[name], fresh=False)
            p3.start(p3.NODES[name])
    p3.wait_cluster(p3.NODES["r1"].base, ["r1", "r2"])
    router = p3.Node("router", kind="router", fresh=True)
    p3.NODES["router"] = router
    p3.start(router, min_mem_gib=0.8)
    end = now() + 60
    while now() < end:
        try:
            if p2.health(router.base).get("status") in ("ok", "degraded"):
                break
        except (OSError, ValueError):
            pass
        time.sleep(0.5)
    time.sleep(2.5)
    r4 = p3.Node("r4", shards=p3.SHARDS["r4"], fresh=True)
    p3.NODES["r4"] = r4
    plan = load_plan(qps, load_s, mix="phase3")
    events = {}
    t_ref = {}

    def timeline():
        t0 = t_ref["t0"]
        time.sleep(max(0, join_at - (now() - t0)))
        events["r4_start"] = now() - t0
        print(f"   t={events['r4_start']:.1f}s start R4 {p3.SHARDS['r4']} (MemAvailable {p2.mem_available_gib():.1f} GiB)")
        r4.start()
        try:
            r4.wait_up()
            events["r4_ready"] = now() - t0
            print(f"   t={events['r4_ready']:.1f}s R4 ready (pid {r4.proc.pid}, RSS {r4.rss_mib()} MiB)")
        except RuntimeError as e:
            events["r4_error"] = str(e)
            print(f"   R4 failed: {e}")
        time.sleep(max(0, drain_at - (now() - t0)))
        r2 = p3.NODES["r2"]
        events["r2_sigterm"] = now() - t0
        print(f"   t={events['r2_sigterm']:.1f}s SIGTERM R2 pid {r2.proc.pid}")
        r2.proc.terminate()  # by PID
        events["r2_exit_code"] = r2.proc.wait(120)
        events["r2_exit"] = now() - t0
        print(f"   t={events['r2_exit']:.1f}s R2 exited {events['r2_exit_code']}")

    print(f"-- router load: {len(plan)} requests at {qps}/s for {load_s:.0f}s (Phase 3 mix); MemAvailable"
          f" {p2.mem_available_gib():.1f} GiB")
    res = {}
    t_ref["t0"] = now()
    tl = threading.Thread(target=timeline, daemon=True)
    tl.start()
    res["samples"], _ = run_load(router.base, plan, workers=48)
    tl.join(150)
    samples = res["samples"]
    ld = summarise_load(samples, len(plan))
    gone = events.get("r2_exit", drain_at)
    win = p3.windows([(r["t"], r["lat"], r["status"] == 200) for r in samples], join_at, gone, load_s)
    router_log = router.log.read_text(errors="replace")
    ok = ld["failed"] == 0 and ld["unavailable"] == 0 and ld["requests"] == len(plan) and "r4_error" not in events
    out = {"load": ld, "events": events, "windows": win, "pass": ok,
           "router_healthy_lines": re.findall(r"Router: healthy readers .*", router_log)[-10:],
           "rss_mib": {k: v.rss_mib() for k, v in p3.NODES.items() if v.proc}}
    (gate() / "router-samples.json").write_text(json.dumps(samples))
    print(f"ROUTER {ld['requests']}/{len(plan)}; failed {ld['failed']}; unavailable {ld['unavailable']};"
          f" status {ld['by_status']} -> {'PASS' if ok else 'FAIL'}")
    for k in ("before", "during", "after"):
        w = win[k]
        if w.get("n"):
            print(f"   {k:7s} {w['range_s']} n={w['n']} p50 {w['p50_ms']:.1f} ms p95 {w['p95_ms']:.1f} ms max {w['max_ms']:.1f} ms")
    print(f"   events {events}; warnings {ld['warnings']}")
    for line in out["router_healthy_lines"]:
        print("   router: " + line)
    for r in ld["failed_samples"][:10]:
        print(f"   bad: t={r['t']:.1f} {r['status']} {r['pipeline']} {r['project']} {r['q']!r} {r['warnings']}")
    save("router", out)
    os.environ.pop("GATE_CROSS_ENCODER", None)
    for n in ("router", "r1", "r4", "r2"):
        if n in p3.NODES:
            p3.NODES[n].stop()
    return ok


# --------------------------------------------------------------------------- down: writer down


def warmed_answers(base, qs):
    out = {}
    for q, proj in qs:
        for scope in (None, proj):
            _, status, results, _ = p3.fetch(base, q, "keyword", scope)
            out[f"{scope or '*'}|{q}"] = (status, [x.get("id") for x in results])
    return out


def wait_visible(base, marker, deadline_s):
    t0 = now()
    while now() - t0 < deadline_s:
        _, status, results, _ = p3.fetch(base, marker, "keyword", "pharos", timeout=5)
        if status == 200 and any(marker in (x.get("id") or "") for x in results):
            return now() - t0
        time.sleep(0.1)
    return None


def phase_down():
    w1 = ensure_w1()
    qs = p1.load_queries()
    d1 = Peer("d1", 7321, mode="full", fresh=True)
    d2 = Peer("d2", 7322, mode="lazy", fresh=True)
    router = Peer("router-down", 7080, kind="router", fresh=True)
    print("-- down: D1 (full) :7321, D2 (lazy) :7322, every shard; router :7080")
    out = {"ready_s": start_all([d1, d2], want={"d1": len(W1_SHARDS), "d2": len(W1_SHARDS)})}
    router.start()
    router.wait_ready()
    p3.wait_cluster(d1.base, ["d1", "d2"])
    before = {n.name: warmed_answers(n.base, qs) for n in (d1, d2)}  # also warms D2's block cache
    run_load(router.base, load_plan(10, 10))  # the router has seen both readers serve
    kill_at, restart_at, after_kill = 10.0, 50.0, 120.0
    plan = load_plan(10, kill_at + after_kill + 2)
    events, res = {}, {}

    def timeline():
        t0 = res["t0"]
        time.sleep(max(0, kill_at - (now() - t0)))
        events["w1_pid"] = w1.proc.pid
        events["w1_kill"] = now() - t0
        events["w1_rc"] = w1.kill9()
        print(f"   t={events['w1_kill']:.1f}s SIGKILL W1 (pid {events['w1_pid']}, rc {events['w1_rc']})")
        time.sleep(max(0, restart_at - (now() - t0)))
        events["d2_sigterm"] = now() - t0
        events["d2_rc"] = d2.stop(timeout=90)
        events["d2_exit"] = now() - t0
        print(f"   t={events['d2_exit']:.1f}s D2 exited {events['d2_rc']} (SIGTERM); restarting it on its home")
        d2.start("-restart")
        try:
            # want=None: with no writer the syncer's own served map stays empty ("serving 0 shard(s)" is
            # logged) while the engine serves the generations activated from disk; judged by the answers
            events["d2_ready_s"] = d2.wait_ready(want=None, timeout=300)
            m = re.findall(r"serving (\d+) shard\(s\)", d2.log.read_text(errors="replace"))
            events["d2_serving_line"] = int(m[-1]) if m else None
            m = re.findall(r"Activated registered generations of \[([^]]*)\]", d2.log.read_text(errors="replace"))
            events["d2_activated_from_disk"] = m[-1] if m else None
            events["d2_ready"] = now() - t0
            print(f"   t={events['d2_ready']:.1f}s D2 ready again after {events['d2_ready_s']:.1f}s, writer still down")
        except RuntimeError as e:
            events["d2_error"] = str(e)
            print(f"   D2 restart failed: {e}")

    res["t0"] = now()
    tl = threading.Thread(target=timeline, daemon=True)
    tl.start()
    samples, _ = run_load(router.base, plan)
    tl.join(400)
    ld = summarise_load(samples, len(plan))
    post_kill = [s for s in samples if s["t"] >= events.get("w1_kill", kill_at)]
    span = (post_kill[-1]["t"] - post_kill[0]["t"]) if post_kill else 0
    load_ok = ld["failed"] == 0 and ld["requests"] == len(plan) and span >= after_kill - 1
    # D2 restarted while the writer is down: it serves its on-disk generations, answers as before.
    d2_after = warmed_answers(d2.base, qs) if "d2_error" not in events else {}
    same = sum(1 for k, v in before["d2"].items() if d2_after.get(k) == v)
    d2_bad = [k for k, v in before["d2"].items() if d2_after.get(k) != v][:10]
    d2_ok = "d2_error" not in events and same == len(before["d2"]) and all(v[0] == 200 for v in d2_after.values())
    d2_peers = d2.peers()
    d1_after = warmed_answers(d1.base, qs)
    d1_same = sum(1 for k, v in before["d1"].items() if d1_after.get(k) == v)
    print(f"DOWN LOAD {ld['requests']}/{len(plan)} via the router, {len(post_kill)} after the kill over {span:.1f}s;"
          f" failed {ld['failed']}; status {ld['by_status']}; p95 {ld['p95_ms']:.0f} ms -> {'PASS' if load_ok else 'FAIL'}")
    print(f"DOWN RESTART D2 (writer down): ready {events.get('d2_ready_s')}; warmed answers identical "
          f"{same}/{len(before['d2'])} -> {'PASS' if d2_ok else 'FAIL'}; D1 identical {d1_same}/{len(before['d1'])}")
    for k in d2_bad:
        print(f"   differs {k!r}: before {before['d2'][k]} after {d2_after.get(k)}")

    # The writer comes back: readers resume long-polling; a new publish is visible within 5 s.
    w1.start("-back")
    up_s, _ = w1.wait_up()
    t_up = now()
    lp = None
    while now() - t_up < 120:
        tr = w1.transfers()
        if tr.get("longPolls", 0) >= 2:  # at least one long-poll from each reader
            lp = now() - t_up
            break
        time.sleep(0.2)
    print(f"   W1 back up after {up_s:.1f}s; long-polls held: {w1.transfers().get('longPolls')} after"
          f" {lp if lp is None else round(lp, 1)} s")
    marker = "gateDownProbe" + str(int(time.time()))
    append_probe(Path(os.environ["GATE_SRC"]) / "pharos", marker)
    cmd = reindex_cmd(os.environ.get("GATE_REINDEX_HEAP", "2g"))
    with open(gate() / "down-reindex.log", "w") as lf:
        rc = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT).returncode
    t_pub = now()
    pub = re.findall(r"Published shard pharos generation (\d+)", (gate() / "down-reindex.log").read_text(errors="replace"))
    vis = {}
    if rc == 0:
        lock = threading.Lock()

        def watch(n):
            v = wait_visible(n.base, marker, 60)
            with lock:
                vis[n.name] = None if v is None else round(v, 3)

        ths = [threading.Thread(target=watch, args=(n,)) for n in (d1, d2)]
        for t in ths:
            t.start()
        for t in ths:
            t.join(90)
    # visibility measured from the publish returning (each watcher starts right after it)
    back_ok = rc == 0 and lp is not None and all(v is not None and v <= DOWN_BAR_S for v in vis.values()) and len(vis) == 2
    print(f"DOWN WRITER BACK: re-index rc {rc}, published pharos generation {pub[-1] if pub else '?'};"
          f" visible after {vis} s (bar {DOWN_BAR_S} s) -> {'PASS' if back_ok else 'FAIL'}")
    out.update({"events": events, "load": ld, "post_kill_requests": len(post_kill), "post_kill_span_s": span,
                "d2_identical": same, "d2_compared": len(before["d2"]), "d2_differs": d2_bad,
                "d2_peers_after_restart": d2_peers, "d1_identical": d1_same,
                "writer_back": {"up_s": up_s, "longpolls_after_s": lp, "reindex_rc": rc, "generation": pub[-1] if pub else None,
                                "visible_s": vis, "bar_s": DOWN_BAR_S, "transfers": w1.transfers()},
                "router_log_tail": router.log.read_text(errors="replace")[-3000:]})
    out["pass"] = {"queries_while_down": load_ok, "restart_serves_on_disk": d2_ok, "writer_back_5s": back_ok}
    (gate() / "down-samples.json").write_text(json.dumps(samples))
    save("down", out)
    for n in (router, d1, d2):
        n.stop()
    return all(out["pass"].values())


# --------------------------------------------------------------------------- load: writer load


def phase_load():
    w1 = ensure_w1()
    heap = os.environ.get("GATE_LOAD_HEAP", "384m")
    mode = os.environ.get("GATE_LOAD_MODE", "full")
    trials = int(os.environ.get("GATE_LOAD_SINGLE_TRIALS", "3"))
    cap = int(w1.transfers().get("maxTransfers") or 0)
    out = {"mode": mode, "heap": heap, "maxTransfersPerWriter": cap, "single": []}
    print(f"-- load: {mode}-mode readers, heap {heap}, no models; W1 maxTransfersPerWriter {cap}")
    for t in range(trials):
        c = Peer("c0", 7327, mode=mode, fresh=True, heap=heap)
        c.start(f"-{t + 1}")
        try:
            s = c.wait_ready(want=len(W1_SHARDS))
            pf = c.peers().get("peerFetch")
        finally:
            c.stop()
        out["single"].append({"ready_s": round(s, 2), "peerFetch": pf})
        print(f"   single cold reader trial {t + 1}: serving after {s:.2f}s; {pf}")
    single = p2.median([x["ready_s"] for x in out["single"]])
    bound = 3 * single
    before = w1.transfers()
    readers = [Peer(f"c{i + 1}", 7321 + i, mode=mode, fresh=True, heap=heap) for i in range(LOAD_READERS)]
    samples, stop_ev = [], threading.Event()

    def sampler():
        while not stop_ev.is_set():
            try:
                tr = w1.transfers()
                samples.append((round(now() - t0, 3), tr.get("transfersInFlight"), tr.get("peakTransfersInFlight"),
                                tr.get("transfersRefused")))
            except (OSError, ValueError):
                pass
            stop_ev.wait(0.05)

    t0 = now()
    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    print(f"   starting {LOAD_READERS} cold readers at once (MemAvailable {p2.mem_available_gib():.1f} GiB)")
    for r in readers:
        r.start()
    ready = {}
    errors = {}
    lock = threading.Lock()

    def wait(r):
        try:
            v = r.wait_ready(want=len(W1_SHARDS), timeout=600)
        except RuntimeError as e:
            with lock:
                errors[r.name] = str(e)
            return
        with lock:
            ready[r.name] = round(v, 2)

    ws = [threading.Thread(target=wait, args=(r,)) for r in readers]
    for w in ws:
        w.start()
    for w in ws:
        w.join(700)
    time.sleep(1.0)
    stop_ev.set()
    th.join(5)
    after = w1.transfers()
    fetch = {r.name: r.peers().get("peerFetch") for r in readers if r.alive()}
    rss = {r.name: r.rss_mib() for r in readers}
    for r in readers:
        r.stop()
    max_seen = max((s[1] or 0) for s in samples) if samples else None
    peak = after.get("peakTransfersInFlight")
    from_peers = sum((f or {}).get("bytesFromPeers", 0) for f in fetch.values())
    from_writer = sum((f or {}).get("bytesFromWriter", 0) for f in fetch.values())
    files_peers = sum((f or {}).get("filesFromPeers", 0) for f in fetch.values())
    files_writer = sum((f or {}).get("fromWriter", 0) for f in fetch.values())
    served_peers = sum((f or {}).get("servedToPeers", 0) for f in fetch.values())
    w_bytes = (after.get("bytesServed") or 0) - (before.get("bytesServed") or 0)
    cap_ok = max_seen is not None and max_seen <= cap and peak is not None and peak <= cap
    time_ok = not errors and len(ready) == LOAD_READERS and max(ready.values()) <= bound
    out.update({"single_median_s": single, "bound_s": bound, "ready_s": ready, "errors": errors,
                "max_in_flight_sampled": max_seen, "samples": len(samples), "writer_before": before, "writer_after": after,
                "writer_bytes_served": w_bytes, "bytes_from_peers": from_peers, "bytes_from_writer": from_writer,
                "files_from_peers": files_peers, "files_from_writer": files_writer, "served_to_peers": served_peers,
                "peer_fetch": fetch, "rss_mib": rss,
                "pass": {"writer_cap": cap_ok, "serving_within_3x": time_ok}})
    (gate() / "load-transfer-samples.json").write_text(json.dumps(samples))
    print(f"LOAD writer transfers in flight: max sampled {max_seen} over {len(samples)} samples, peak"
          f" {peak}, refused (503) {(after.get('transfersRefused') or 0) - (before.get('transfersRefused') or 0)};"
          f" cap {cap} -> {'PASS' if cap_ok else 'FAIL'}")
    print(f"LOAD ready {ready} (max {max(ready.values()) if ready else None} s) vs 3 x single {single:.2f} s ="
          f" {bound:.2f} s -> {'PASS' if time_ok else 'FAIL'} {errors or ''}")
    print(f"LOAD bytes: from peers {from_peers} ({files_peers} files), from the writer {from_writer}"
          f" ({files_writer} transfers); writer served {w_bytes} bytes; readers served {served_peers} transfers to peers")
    save("load", out)
    return cap_ok and time_ok


# --------------------------------------------------------------------------- check


def phase_check():
    stop_all()
    marker = gate() / "marker"
    home = Path.home()
    newer = subprocess.run(["find", str(home / ".pharos"), str(home / ".djl.ai"), "-newer", str(marker)],
                           capture_output=True, text=True).stdout.split()
    alive = [n.name for n in ALL if n.alive()]
    busy = []
    for port in GATE_PORTS:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1):
                busy.append(port)
        except urllib.error.HTTPError:
            busy.append(port)
        except OSError:
            pass
    ok = not alive and not busy
    out = {"newer_than_marker": newer, "alive": alive, "ports_in_use": busy, "pass": ok,
           "min_mem_available_gib": MEM.min_avail if MEM else None, "max_gate_rss_mib": MEM.max_rss if MEM else None}
    print(f"CHECK files newer than the marker under ~/.pharos and ~/.djl.ai: {newer or 'none'}; gate processes alive:"
          f" {alive or 'none'}; gate ports in use: {busy or 'none'} -> {'PASS' if ok else 'FAIL'}"
          f" (judge any listed file by hand: the user's own daemons write ~/.pharos/logs)")
    save("check", out)
    return ok


# --------------------------------------------------------------------------- main


def self_test():
    os.environ.setdefault("GATE_TOKEN", "t")
    plan = load_plan(10, 2)
    assert len(plan) == 20 and plan[1][0] == 0.1 and plan[0][2] is None and plan[1][2] is not None
    assert all(p[3] == "keyword" for p in plan)
    p3plan = load_plan(20, 1, mix="phase3")
    assert [p[3] for p in p3plan[:4]] == ["keyword", "keyword", "hybrid", "hybrid"] and p3plan[3][2] == "pharos"
    s = summarise_load([{"t": 0, "lat": 0.1, "status": 200, "warnings": [], "unavailable": []},
                        {"t": 1, "lat": 0.2, "status": 503, "warnings": ["x"], "unavailable": []}], 2)
    assert s["failed"] == 1 and s["by_status"] == {"200": 1, "503": 1} and s["warnings"] == {"x": 1}
    cfg = p2.peer_cluster("n", 7301, Path("/tmp/c"), {"mode": "full"})
    assert cfg["backend"] == "peer-pull" and cfg["peers"]["discovery"].startswith("seeds:") and "objectStore" not in cfg
    print("self-test OK")


def main():
    global MEM
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--phases", default=",".join(PHASES))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        self_test()
        return 0
    phases = [p.strip() for p in args.phases.split(",") if p.strip()]
    unknown = set(phases) - set(PHASES)
    if unknown:
        ap.error(f"unknown phases {sorted(unknown)}")
    if args.dry_run:
        print(f"driver (dry run): phases {phases}; {len(p1.load_queries())} pharos+lucene queries;"
              f" refresh {REFRESH_S} s; {LOAD_READERS} cold readers in the load phase")
        return 0
    if not p2.peer_mode():
        print("cluster_gate_phase5: GATE_BACKEND must be peer-pull")
        return 2
    MEM = MemoryMonitor()
    MEM.th.start()
    results = {}
    try:
        for p in phases:
            print(f"== phase {p} (MemAvailable {p2.mem_available_gib():.1f} GiB) at {time.strftime('%H:%M:%S')}")
            try:
                if p == "p1":
                    results[p] = phase_p1()
                elif p.startswith("p2"):
                    results[p] = phase_p2(p)
                elif p == "check":
                    results[p] = phase_check()
                else:
                    results[p] = globals()["phase_" + p]()
            except Exception as e:  # record and go on: one phase's failure must not hide the others
                import traceback
                traceback.print_exc()
                print(f"PHASE {p} ERROR: {e!r}")
                results[p] = False
                for n in list(ALL):
                    if n is not W1:
                        n.stop()
                for n in list(p3.NODES.values()):
                    n.stop()
    finally:
        stop_all()
        for n in list(p3.NODES.values()):
            n.stop()
        MEM.stop_ev.set()
    print(f"memory: min MemAvailable {MEM.min_avail:.2f} GiB; max gate RSS {MEM.max_rss} MiB")
    verdict = all(results.values())
    print("GATE " + ("PASSED" if verdict else "FAILED") + ": "
          + ", ".join(f"{p} {'pass' if ok else 'FAIL'}" for p, ok in results.items()))
    return 0 if verdict else 1


if __name__ == "__main__":
    sys.exit(main())
