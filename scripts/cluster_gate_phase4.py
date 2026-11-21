#!/usr/bin/env python3
"""Phase 4 gate driver (standard library only). Driven by scripts/cluster-gate-phase4.sh, which has
already started MinIO on 127.0.0.1:9000, built the bare repos $GATE/repos/{lucene,pharos,alpha,beta}.git
with edit clones in $GATE/edit/<shard>, and had a bootstrap writer (workQueue false) publish each shard
as generation 1 with its git source recorded in the catalog (lucene, alpha, beta with "embed": false).

Every JVM is started and stopped here, by PID (Popen). Readers R1, R2 (all shards, lazy, no embedding
model: the load is keyword) and the router run throughout, with ~10 QPS of keyword load through the
router. Work-queue writers w1..w6 and wlow come and go. Background monitors poll /cluster/status through
the router (0.5 s), the bucket (writer heartbeats, LATEST, leases; 0.5 s), listen to /cluster/events
(SSE) and run a webhook sink on 127.0.0.1:7390.

  failover   w1 alone takes all four shards, then w2 and w3 start. K1: w1 is killed (-9) while its
             heartbeat shows lucene in the commit phase. K2: the pharos holder is killed while it
             embeds a 40-file pharos change. Pass (each): LATEST and its manifest valid (every file
             present, sizes equal, parent chain), another writer holds the lease within TTL + one poll,
             it restores and publishes; embedding misses on the resume <= changed files.
  scale      from one writer to four (one per shard) and back to one, a source change every 30 s
             (round robin alpha, pharos, beta, lucene). Pass: every change published exactly once
             (manifest sourceRevision), consecutive generations (parentGeneration), 0 failed queries;
             each writer-count change visible on /cluster/status within one heartbeat of the bucket
             change; each change pending on /cluster/status within one writer poll of its push, and
             out of the backlog within one heartbeat of its publish.
  proposals  load raised to 30 QPS (target 8 per reader, window 60 s). Pass: an add-readers proposal
             on /cluster/status, on /cluster/events and delivered to the webhook sink.
  disk       ruling R5: a writer (wlow) whose writer.disk.minFreeBytes sits just below the measured free
             space. Pass: its lucene run is refused or aborted before free space crosses the threshold,
             lucene's last generation stays served, another writer takes the lease and publishes,
             /cluster/status shows writer-low-disk once a filler file pushes free space below the
             threshold, and wlow recovers once the filler is deleted.
  check      the manifest chain of every shard, published-once over the whole run, the marker check.

--dry-run prints the plan; --self-test checks the bookkeeping on canned inputs.
"""
import argparse
import concurrent.futures
import datetime
import hashlib
import hmac
import http.server
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
import xml.etree.ElementTree as ET
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.dont_write_bytecode = True
import cluster_gate  # noqa: E402  (load_queries)

HB = 5            # cluster.heartbeatSeconds
POLL = 10         # cluster.writer.pollSeconds
TTL = 30          # cluster.writer.leaseTtlSeconds
MIN_HOLD = 120    # cluster.writer.minHoldSeconds
CHANGE_EVERY = 30
BASE_QPS = 10
HIGH_QPS = 30
QPS_TARGET = 8
ADD_READERS_WINDOW = 60
MIN_FREE_FLOOR = 8 << 30   # the gate never lets / go below this
SHARDS = ["alpha", "pharos", "beta", "lucene"]
PORTS = {"router": 7080, "r1": 7401, "r2": 7402, "w1": 7411, "w2": 7412, "w3": 7413, "w4": 7414, "w5": 7415,
         "w6": 7416, "wlow": 7417}
WEBHOOK_PORT = 7390
GIB = 1 << 30
MIB = 1 << 20

T0 = time.time()


def now():
    return time.time()


def rel(t):
    return None if t is None else round(t - T0, 1)


def log(msg):
    print(f"[{now() - T0:7.1f}s] {msg}", flush=True)


def gate_dir():
    return Path(os.environ["GATE"])


def mem_available_gib():
    for ln in open("/proc/meminfo"):
        if ln.startswith("MemAvailable"):
            return int(ln.split()[1]) / 1048576
    return float("nan")


def free_bytes(path=None):
    st = os.statvfs(str(path or gate_dir()))
    return st.f_bavail * st.f_frsize


def pct(values, p):
    """Nearest-rank percentile."""
    if not values:
        return None
    v = sorted(values)
    k = max(0, min(len(v) - 1, int(-(-p * len(v) // 100)) - 1))
    return v[k]


# --------------------------------------------------------------------------- S3 (SigV4, path style)


class S3:
    def __init__(self, endpoint="127.0.0.1:9000", bucket="pharos-gate", prefix="pharos/"):
        self.host, self.bucket, self.prefix = endpoint, bucket, prefix
        self.ak = os.environ["PHAROS_GATE_ACCESS_KEY"]
        self.sk = os.environ["PHAROS_GATE_SECRET_KEY"]

    @staticmethod
    def _h(key, msg):
        return hmac.new(key, msg.encode(), hashlib.sha256).digest()

    def _request(self, method, key="", query=None, timeout=10):
        t = datetime.datetime.now(datetime.timezone.utc)
        amz, day = t.strftime("%Y%m%dT%H%M%SZ"), t.strftime("%Y%m%d")
        uri = "/" + self.bucket + ("/" + urllib.parse.quote(key, safe="/-_.~") if key else "")
        q = "&".join(f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(v, safe='-_.~')}"
                     for k, v in sorted((query or {}).items()))
        ph = hashlib.sha256(b"").hexdigest()
        headers = f"host:{self.host}\nx-amz-content-sha256:{ph}\nx-amz-date:{amz}\n"
        signed = "host;x-amz-content-sha256;x-amz-date"
        creq = "\n".join([method, uri, q, headers, signed, ph])
        scope = f"{day}/us-east-1/s3/aws4_request"
        sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(creq.encode()).hexdigest()])
        k = self._h(("AWS4" + self.sk).encode(), day)
        for part in ("us-east-1", "s3", "aws4_request"):
            k = self._h(k, part)
        sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
        req = urllib.request.Request(f"http://{self.host}{uri}" + ("?" + q if q else ""), method=method, headers={
            "x-amz-date": amz, "x-amz-content-sha256": ph,
            "Authorization": f"AWS4-HMAC-SHA256 Credential={self.ak}/{scope}, SignedHeaders={signed}, Signature={sig}"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.read(), dict(r.headers)
        except urllib.error.HTTPError as e:
            return e.code, e.read(), dict(e.headers)

    def get(self, key):
        """bytes or None (404). `key` is relative to the prefix."""
        st, body, _ = self._request("GET", self.prefix + key)
        if st == 404:
            return None
        if st != 200:
            raise OSError(f"S3 GET {key}: HTTP {st}")
        return body

    def get_json(self, key):
        b = self.get(key)
        return None if b is None else json.loads(b)

    def head(self, full_key):
        """Content length of a key given in full (with the prefix), None when absent."""
        st, _, h = self._request("HEAD", full_key)
        if st == 404:
            return None
        if st != 200:
            raise OSError(f"S3 HEAD {full_key}: HTTP {st}")
        return int(h.get("Content-Length", h.get("content-length", -1)))

    def list(self, sub):
        """[(key relative to the prefix, size, lastModified)]."""
        out, token = [], None
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        while True:
            q = {"list-type": "2", "prefix": self.prefix + sub}
            if token:
                q["continuation-token"] = token
            st, body, _ = self._request("GET", "", q)
            if st != 200:
                raise OSError(f"S3 LIST {sub}: HTTP {st}")
            root = ET.fromstring(body)
            for c in root.findall(ns + "Contents"):
                out.append((c.find(ns + "Key").text[len(self.prefix):], int(c.find(ns + "Size").text),
                            c.find(ns + "LastModified").text))
            if root.findtext(ns + "IsTruncated") == "true":
                token = root.findtext(ns + "NextContinuationToken")
            else:
                return out


# --------------------------------------------------------------------------- nodes


def object_store_cfg():
    return {"endpoint": "http://127.0.0.1:9000", "region": "us-east-1", "bucket": "pharos-gate", "prefix": "pharos",
            "pathStyle": True, "accessKeyEnv": "PHAROS_GATE_ACCESS_KEY", "secretKeyEnv": "PHAROS_GATE_SECRET_KEY"}


def status_cfg():
    return {"targets": {"warmP95Millis": 10000, "qpsPerReader": QPS_TARGET},
            "windows": {"addReadersSeconds": ADD_READERS_WINDOW, "clearGraceSeconds": 30},
            "eventsKeepAliveSeconds": 10, "webhookUrl": f"http://127.0.0.1:{WEBHOOK_PORT}/hook"}


class Node:
    """A reader, the router or a work-queue writer: its own user.home / pharos.home under $GATE/nodes."""

    def __init__(self, name, kind, disk=None):
        self.name, self.kind, self.port = name, kind, PORTS[name]
        self.node_id = "gate-" + name
        self.base = f"http://127.0.0.1:{self.port}"
        self.home = gate_dir() / "nodes" / name
        self.ph = self.home / ".pharos"
        self.disk = disk
        self.proc = None
        self.log = None
        self.starts = 0
        self.t_start = self.t_exit = None
        self.exit_code = None

    def config(self):
        cluster = {"backend": "object-store", "nodeId": self.node_id, "cacheDir": str(self.ph / "cluster-cache"),
                   "heartbeatSeconds": HB, "pollIntervalSeconds": 10, "token": os.environ["GATE_TOKEN"],
                   "retentionGenerations": 1000, "retentionMinutes": 100000, "status": status_cfg(),
                   "objectStore": object_store_cfg()}
        cfg = {"crossEncoderEnabled": False, "cluster": cluster}
        if self.kind == "writer":
            cluster["role"] = "writer"
            disk = {"minFreeBytes": MIN_FREE_FLOOR, "minFreePercent": 0, "checkSeconds": 5, "hysteresisBytes": 512 * MIB}
            disk.update(self.disk or {})
            cluster["writer"] = {"workQueue": True, "workDir": str(self.home / "work"), "pollSeconds": POLL,
                                 "leaseTtlSeconds": TTL, "minHoldSeconds": MIN_HOLD, "disk": disk}
            cfg["embeddingProviders"] = [{"type": "djl", "modelId": os.environ["GATE_MODEL_ID"],
                                          "url": os.environ["GATE_MODEL_URL"], "dimensions": 768, "maxTokens": 512}]
            # a writer serves no search: no search model, so the web start does not warm (load) jina
            cfg["searchEmbeddingModel"] = "writer-serves-no-search"
        else:
            cluster["role"] = "reader"
            cluster["advertiseUrl"] = self.base
            cluster["reader"] = {"mode": "lazy"}
        return cfg

    def command(self):
        heap = os.environ.get("GATE_WRITER_HEAP", "2g") if self.kind == "writer" else os.environ.get("GATE_WEB_HEAP",
                                                                                                    "640m")
        cmd = ["java", "-Xmx" + heap, "-XX:+UseG1GC", "-XX:G1PeriodicGCInterval=15000",
               "--enable-native-access=ALL-UNNAMED", "--add-opens", "java.base/java.nio.channels.spi=ALL-UNNAMED",
               "--add-modules", "jdk.incubator.vector", "-Dpolyglot.engine.WarnInterpreterOnly=false",
               "-Dlogback.configurationFile=" + os.environ["GATE_LOGBACK"],
               "-Duser.home=" + str(self.home), "-Dpharos.home=" + str(self.ph), "-jar", os.environ["GATE_JAR"]]
        if self.kind == "router":
            return cmd + ["router", "--port", str(self.port)]
        return cmd + ["web", "--port", str(self.port), "--no-open"]

    def start(self):
        if mem_available_gib() < 1.0:
            log(f"WARNING: MemAvailable {mem_available_gib():.1f} GiB before starting {self.name}")
        self.ph.mkdir(parents=True, exist_ok=True)
        link = self.home / ".djl.ai"
        if not link.is_symlink():
            link.symlink_to(os.environ["GATE_MODELS"])
        (self.ph / "config.json").write_text(json.dumps(self.config(), indent=2))
        self.starts += 1
        (gate_dir() / "logs").mkdir(exist_ok=True)
        self.log = gate_dir() / "logs" / f"{self.name}-{self.starts}.log"
        lf = open(self.log, "w")
        self.t_start = now()
        self.t_exit = self.exit_code = None
        self.proc = subprocess.Popen(self.command(), stdout=lf, stderr=subprocess.STDOUT)
        lf.close()
        log(f"started {self.name} ({self.kind}) pid {self.proc.pid} :{self.port}; MemAvailable"
            f" {mem_available_gib():.1f} GiB")
        return self

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def wait_health(self, timeout=300):
        end = now() + timeout
        while now() < end:
            if self.proc.poll() is not None:
                raise RuntimeError(f"{self.name} exited {self.proc.returncode}; see {self.log}")
            try:
                with urllib.request.urlopen(self.base + "/health", timeout=5) as r:
                    if r.status == 200:
                        return now() - self.t_start
            except OSError:
                pass
            time.sleep(0.3)
        raise RuntimeError(f"{self.name} not healthy after {timeout}s; see {self.log}")

    def stop(self, timeout=120):
        """SIGTERM by PID, wait for the exit; returns (t_signal, t_exit, code)."""
        if not self.alive():
            return None
        t_sig = now()
        self.proc.terminate()
        try:
            self.exit_code = self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            log(f"{self.name} did not exit within {timeout}s of SIGTERM; SIGKILL")
            self.proc.kill()
            self.exit_code = self.proc.wait(30)
        self.t_exit = now()
        log(f"stopped {self.name} pid {self.proc.pid}: exit {self.exit_code} {self.t_exit - t_sig:.1f}s after SIGTERM")
        return t_sig, self.t_exit, self.exit_code

    def kill9(self):
        t = now()
        self.proc.kill()  # SIGKILL, by PID
        self.exit_code = self.proc.wait(30)
        self.t_exit = now()
        log(f"KILLED -9 {self.name} pid {self.proc.pid}")
        return t

    def log_offset(self):
        return self.log.stat().st_size if self.log and self.log.exists() else 0

    def log_since(self, offset):
        with open(self.log, "rb") as f:
            f.seek(offset)
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


def node(name, kind=None, disk=None):
    if name not in NODES:
        NODES[name] = Node(name, kind or ("writer" if name.startswith("w") else "reader"), disk=disk)
    return NODES[name]


def stop_all():
    for n in list(NODES.values()):
        if n.alive():
            try:
                n.stop(90)
            except Exception as e:  # keep stopping the others
                log(f"stopping {n.name} failed: {e!r}")


# --------------------------------------------------------------------------- HTTP helpers


def get_json(url, timeout=10, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def search(base, q, project=None, timeout=30):
    params = {"q": q, "pipeline": "keyword", "limit": 10}
    if project:
        params["project"] = project
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(f"{base}/api/search?" + urllib.parse.urlencode(params), timeout=timeout) as r:
            body, status = r.read(), r.status
    except urllib.error.HTTPError as e:
        body, status = e.read(), e.code
    except Exception as e:
        return time.perf_counter() - t0, -1, 0, repr(e)[:200]
    dt = time.perf_counter() - t0
    try:
        data = json.loads(body)
        n = len(data if isinstance(data, list) else data.get("results", []))
        if status == 200 and isinstance(data, dict) and "results" not in data:
            status = -2
    except ValueError:
        n, status = 0, (-3 if status == 200 else status)
    return dt, status, n, None if status == 200 else body[:200].decode(errors="replace")


# --------------------------------------------------------------------------- monitors


class Monitors:
    """Background threads: status poller, bucket poller, SSE listener, webhook sink, load, changes."""

    def __init__(self):
        self.s3 = S3()
        self.stop_ev = threading.Event()
        self.lock = threading.Lock()
        self.status = []          # compact status records
        self.status_full = {}     # writer count -> [(t, full status json)] at change points
        self.last_status = None
        self.status_errors = 0
        self.bucket = []          # (t, kind, key, value) changes
        self.hb = {}              # node -> (t, heartbeat json)
        self.latest = {}          # shard -> gen
        self.leases = {}          # shard -> (writer, released)
        self.writer_keys = set()
        self.sse = []             # (t, event, data)
        self.hooks = []           # (t, body)
        self.samples = []         # load samples: (t, lat, status, project, label, err)
        self.qps = BASE_QPS
        self.label = "setup"
        self.load_on = threading.Event()
        self.changes = []         # {t, shard, sha, n, kind, files, phase}
        self.changes_on = threading.Event()
        self.revs = {s: [git(s, "rev-parse", "HEAD")] for s in SHARDS}   # ordered commits per shard
        self.git_lock = threading.Lock()
        self.min_free = None
        self.hook_server = None

    def start(self):
        self.hook_server = http.server.ThreadingHTTPServer(("127.0.0.1", WEBHOOK_PORT), self._hook_handler())
        threading.Thread(target=self.hook_server.serve_forever, daemon=True, name="webhook").start()
        for fn in (self._status_loop, self._bucket_loop, self._sse_loop, self._load_loop, self._change_loop,
                   self._free_loop, self._mem_loop):
            threading.Thread(target=fn, daemon=True, name=fn.__name__).start()

    def close(self):
        self.stop_ev.set()
        self.load_on.clear()
        self.changes_on.clear()
        if self.hook_server:
            self.hook_server.shutdown()

    def _hook_handler(self):
        mon = self

        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                n = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(n)
                with mon.lock:
                    try:
                        mon.hooks.append((now(), json.loads(body)))
                    except ValueError:
                        mon.hooks.append((now(), {"raw": body[:500].decode(errors="replace")}))
                self.send_response(204)
                self.end_headers()

            def log_message(self, *a):
                pass
        return H

    def _status_loop(self):
        url = f"http://127.0.0.1:{PORTS['router']}/cluster/status"
        prev_writers = None
        with open(gate_dir() / "status-timeline.jsonl", "w") as out:
            while not self.stop_ev.is_set():
                t = now()
                try:
                    st = get_json(url, timeout=5)
                    rec = compact_status(t, st)
                    with self.lock:
                        self.status.append(rec)
                        self.last_status = st
                        if rec["nwriters"] != prev_writers:
                            self.status_full.setdefault(str(rec["nwriters"]), []).append((t, st))
                            prev_writers = rec["nwriters"]
                    out.write(json.dumps(rec) + "\n")
                except Exception:
                    self.status_errors += 1
                self.stop_ev.wait(max(0.0, 0.5 - (now() - t)))

    def _bucket_loop(self):
        while not self.stop_ev.is_set():
            t = now()
            try:
                keys = {k.rsplit("/", 1)[1][:-5] for k, _, _ in self.s3.list("catalog/writers/") if k.endswith(".json")}
                for k in sorted(keys - self.writer_keys):
                    self.bucket.append((t, "writer+", k, None))
                for k in sorted(self.writer_keys - keys):
                    self.bucket.append((t, "writer-", k, None))
                self.writer_keys = keys
                for k in keys:
                    try:
                        hb = self.s3.get_json(f"catalog/writers/{k}.json")
                    except (OSError, ValueError):
                        hb = None
                    if hb is not None:
                        self.hb[k] = (t, hb)
                for s in SHARDS:
                    lt = self.s3.get_json(f"shards/{s}/LATEST")
                    g = lt.get("generation") if lt else None
                    if g != self.latest.get(s):
                        self.bucket.append((t, "latest", s, g))
                        self.latest[s] = g
                    le = self.s3.get_json(f"shards/{s}/lease")
                    v = (le.get("writer"), bool(le.get("released"))) if le else None
                    if v != self.leases.get(s):
                        self.bucket.append((t, "lease", s, v))
                        self.leases[s] = v
            except Exception as e:
                self.bucket.append((t, "error", "", repr(e)[:200]))
            self.stop_ev.wait(max(0.0, 0.5 - (now() - t)))

    def _sse_loop(self):
        url = f"http://127.0.0.1:{PORTS['router']}/cluster/events"
        while not self.stop_ev.is_set():
            try:
                req = urllib.request.Request(url, headers={"Accept": "text/event-stream"})
                with urllib.request.urlopen(req, timeout=60) as r:
                    with self.lock:
                        self.sse.append((now(), "_open", None))
                    ev, data = None, []
                    for raw in r:
                        if self.stop_ev.is_set():
                            return
                        line = raw.decode(errors="replace").rstrip("\r\n")
                        if line.startswith("event:"):
                            ev = line[6:].strip()
                        elif line.startswith("data:"):
                            data.append(line[5:].strip())
                        elif line == "":
                            if ev or data:
                                try:
                                    d = json.loads("\n".join(data)) if data else None
                                except ValueError:
                                    d = "\n".join(data)
                                with self.lock:
                                    self.sse.append((now(), ev, d))
                            ev, data = None, []
            except Exception as e:
                with self.lock:
                    self.sse.append((now(), "_error", repr(e)[:200]))
                self.stop_ev.wait(2)

    def _load_loop(self):
        base_q = cluster_gate.load_queries()
        router = f"http://127.0.0.1:{PORTS['router']}"
        ex = concurrent.futures.ThreadPoolExecutor(max_workers=48)
        i = 0
        next_t = time.perf_counter()

        def one(q, proj, label):
            ts = now()
            dt, st, n, err = search(router, q, proj)
            with self.lock:
                self.samples.append((ts, dt, st, proj, label, err))

        while not self.stop_ev.is_set():
            if not self.load_on.is_set():
                self.load_on.wait(0.5)
                next_t = time.perf_counter()
                continue
            q, proj = base_q[i % len(base_q)]
            k = i % 4  # unscoped, scoped to the query's own project, unscoped, scoped to a fixture
            scope = None if k in (0, 2) else (proj if k == 1 else ["alpha", "beta"][(i // 4) % 2])
            if k == 3:
                q = "scale count add"
            ex.submit(one, q, scope, self.label)
            i += 1
            next_t += 1.0 / max(0.1, self.qps)
            d = next_t - time.perf_counter()
            if d > 0:
                time.sleep(d)
            elif d < -5:
                next_t = time.perf_counter()
        ex.shutdown(wait=True)

    def _change_loop(self):
        n = 0
        while not self.stop_ev.is_set():
            if not self.changes_on.wait(1):
                continue
            shard = SHARDS[n % len(SHARDS)]
            try:
                c = self.push_change(shard, n)
                log(f"change #{n} {shard} {c['sha'][:10]}")
            except Exception as e:
                log(f"change #{n} {shard} FAILED: {e!r}")
            n += 1
            self.stop_ev.wait(CHANGE_EVERY)

    def push_change(self, shard, n, kind="append", files=None):
        with self.git_lock:
            edit = gate_dir() / "edit" / shard
            touched = []
            if kind == "append":
                fs = change_files(shard)
                f = fs[n % len(fs)]
                with open(edit / f, "a") as fh:
                    fh.write(f"// gate change {n}\n")
                touched = [f]
            else:  # a comment in the first method body of each of `files`
                for f in files:
                    if insert_method_comment(edit / f, n):
                        touched.append(f)
            git(shard, "commit", "-q", "-am", f"gate change {n} ({kind}, {len(touched)} files)")
            sha = git(shard, "rev-parse", "HEAD")
            git(shard, "push", "-q", "origin", "main")
            t = now()
            c = {"t": t, "shard": shard, "sha": sha, "n": n, "kind": kind, "files": touched, "phase": self.label}
            with self.lock:
                self.revs[shard].append(sha)
                self.changes.append(c)
            return c

    def _free_loop(self):
        while not self.stop_ev.is_set():
            f = free_bytes()
            self.min_free = f if self.min_free is None else min(self.min_free, f)
            if f < MIN_FREE_FLOOR:
                log(f"free space {f / GIB:.2f} GiB below {MIN_FREE_FLOOR / GIB:.0f} GiB: removing the filler")
                remove_filler()
            self.stop_ev.wait(0.2)

    def _mem_loop(self):
        with open(gate_dir() / "memory.log", "w") as out:
            while not self.stop_ev.is_set():
                rss = {n.name: n.rss_mib() for n in list(NODES.values()) if n.alive()}
                out.write(json.dumps({"t": rel(now()), "memAvailableGiB": round(mem_available_gib(), 2),
                                      "freeGiB": round(free_bytes() / GIB, 2), "rssMiB": rss}) + "\n")
                out.flush()
                self.stop_ev.wait(15)

    def status_since(self, t):
        with self.lock:
            return [r for r in self.status if r["t"] >= t]

    def wait_status(self, pred, timeout, what):
        end = now() + timeout
        while now() < end:
            with self.lock:
                r = self.status[-1] if self.status else None
            if r is not None and pred(r):
                return r
            time.sleep(0.25)
        raise RuntimeError(f"timeout ({timeout}s) waiting for {what}")


def compact_status(t, st):
    lay = st.get("layout", {})
    rec = {"t": t, "asOf": st.get("asOf"), "summary": lay.get("summary"), "shards": {}, "writers": {},
           "health": [(h.get("kind"), h.get("target")) for h in st.get("health", [])],
           "proposals": [(p.get("kind"), p.get("target"), p.get("id")) for p in st.get("proposals", [])],
           "backlogSummary": st.get("backlogSummary")}
    for sh in lay.get("shards", []):
        lease = sh.get("lease") or {}
        rec["shards"][sh.get("shard")] = {"gen": sh.get("generation"), "writer": sh.get("writer"),
                                          "lease": lease.get("writer"), "expired": lease.get("expired"),
                                          "released": lease.get("released"), "state": sh.get("state"),
                                          "backlog": sh.get("backlog")}
    for w in lay.get("writers", []):
        rec["writers"][w.get("node")] = {"leases": w.get("leases"), "indexing": w.get("indexing"),
                                         "stopping": w.get("stopping"), "disk": w.get("disk")}
    rec["nwriters"] = len(rec["writers"])
    return rec


# --------------------------------------------------------------------------- git and changes


def git(shard, *args):
    edit = gate_dir() / "edit" / shard
    out = subprocess.run(["git", "-c", "user.name=pharos-gate", "-c", "user.email=gate@localhost", "-C", str(edit),
                          *args], check=True, capture_output=True, text=True)
    return out.stdout.strip()


_FILES = {}


def change_files(shard):
    """The files the round-robin changes append a comment to (fixed, sorted)."""
    if shard not in _FILES:
        edit = gate_dir() / "edit" / shard
        if shard == "lucene":
            fs = sorted(edit.glob("lucene/core/src/java/org/apache/lucene/util/*.java"))[:40]
        elif shard == "pharos":
            fs = sorted(edit.glob("src/main/java/**/*.java"))[:40]
        else:
            fs = sorted(edit.glob("src/main/java/**/*.java"))
        _FILES[shard] = [str(p.relative_to(edit)) for p in fs]
        if not _FILES[shard]:
            raise RuntimeError(f"no change files for {shard}")
    return _FILES[shard]


def k2_files():
    edit = gate_dir() / "edit" / "pharos"
    fs = sorted(edit.glob("src/main/java/**/*.java"))
    return [str(p.relative_to(edit)) for p in fs[-40:]]


METHOD_RE = re.compile(r"^\s+(public|protected|private|static|final|synchronized|default|<|[A-Z][\w<>,\[\] ?]*\s)[^;=]*\)"
                       r"\s*(throws [\w.,\s]+)?\{\s*$")
NOT_METHOD = re.compile(r"\b(class|interface|enum|record|if|for|while|switch|try|catch|else|do|new)\b")


def insert_method_comment(path, n):
    """Inserts a comment as the first line of the first method body; True when the file changed."""
    lines = path.read_text().splitlines(keepends=True)
    for i, ln in enumerate(lines):
        if METHOD_RE.match(ln) and not NOT_METHOD.search(ln.split("(")[0]):
            indent = re.match(r"^\s*", ln).group(0)
            lines.insert(i + 1, f"{indent}    // gate k2 edit {n}\n")
            path.write_text("".join(lines))
            return True
    return False


FILLER = None


def remove_filler():
    global FILLER
    if FILLER is not None and FILLER.exists():
        FILLER.unlink()
        log(f"removed filler {FILLER}")


# --------------------------------------------------------------------------- bucket checks


def manifest(s3, shard, gen):
    return s3.get_json(f"shards/{shard}/manifests/gen-{gen}.json")


def check_generation(s3, shard):
    """LATEST and its manifest: generation matches, every file present with the manifest's length,
    parent = generation - 1. Returns (ok, detail)."""
    lt = s3.get_json(f"shards/{shard}/LATEST")
    if not lt:
        return False, "no LATEST"
    g = lt.get("generation")
    if lt.get("manifest") != f"gen-{g}.json":
        return False, f"LATEST names {lt.get('manifest')} for generation {g}"
    m = manifest(s3, shard, g)
    if m is None:
        return False, f"manifest gen-{g} missing"
    if m.get("generation") != g:
        return False, f"manifest generation {m.get('generation')} != {g}"
    if g > 1 and m.get("parentGeneration") != g - 1:
        return False, f"gen {g} parent {m.get('parentGeneration')}"
    bad = []
    for f in m.get("files", []):
        key = f.get("key")
        size = s3.head(key)
        if size is None:
            size = s3.head(s3.prefix + key)
        if size != f.get("length"):
            bad.append(f"{f.get('name')}: {size} != {f.get('length')}")
    if bad:
        return False, f"gen {g}: " + "; ".join(bad[:5])
    return True, f"gen {g}, {len(m.get('files', []))} files present with their lengths, writer {m.get('writerNode')}"


def chain(s3, shard):
    """Every manifest of the shard: [(gen, parent, writerNode, sourceRevision, createdAt)] by gen."""
    out = []
    for k, _, _ in s3.list(f"shards/{shard}/manifests/"):
        if not re.search(r"gen-(\d+)\.json$", k):
            continue
        m = s3.get_json(k)
        pm = m.get("projectMeta") or {}
        out.append((m.get("generation"), m.get("parentGeneration"), m.get("writerNode"), pm.get("sourceRevision"),
                    m.get("createdAt")))
    return sorted(out, key=lambda r: r[0])


def chain_verdict(rows, latest_gen):
    """(ok, problems): generations 1..LATEST all present once, parent = gen - 1, none above LATEST."""
    problems = []
    gens = [r[0] for r in rows]
    if gens != list(range(1, latest_gen + 1)):
        missing = sorted(set(range(1, latest_gen + 1)) - set(gens))
        extra = sorted(g for g in gens if g > latest_gen)
        dup = sorted({g for g in gens if gens.count(g) > 1})
        problems.append(f"missing {missing} extra {extra} dup {dup}")
    for g, parent, *_ in rows:
        if g > 1 and parent != g - 1:
            problems.append(f"gen {g} parent {parent}")
        if g == 1 and parent not in (None, 0):
            problems.append(f"gen 1 parent {parent}")
    return not problems, problems


def published_once(rows, revs, since_idx=1):
    """Each change (commit index >= since_idx in revs) is published: covered by a generation whose
    revision is it or a later commit; no two generations publish the same revision."""
    idx = {sha: i for i, sha in enumerate(revs)}
    by_rev = {}
    for g, _, w, rev, _ in rows:
        if rev is not None:
            by_rev.setdefault(rev, []).append(g)
    dup = {r[:10]: gs for r, gs in by_rev.items() if len(gs) > 1}
    unknown = sorted(g for g, _, _, rev, _ in rows if rev is not None and rev not in idx)
    published_idx = sorted(idx[rev] for rev in by_rev if rev in idx)
    top = max(published_idx) if published_idx else -1
    lost = [i for i in range(since_idx, len(revs)) if i > top]
    coalesced = [i for i in range(since_idx, len(revs)) if i <= top and i not in published_idx]
    return {"changes": max(0, len(revs) - since_idx), "duplicates": dup, "unknown_revisions": unknown,
            "lost": lost, "coalesced": coalesced, "published_revisions": len(by_rev),
            "unrevisioned_generations": sorted(g for g, _, _, rev, _ in rows if rev is None)}


# --------------------------------------------------------------------------- phases


RESULTS = {}
MON = None


def save(name, data):
    RESULTS[name] = data
    p = gate_dir() / f"results-{name}.json"
    p.write_text(json.dumps(data, indent=2, default=str))
    log(f"results -> {p}")


def writers_alive():
    return [n for n in NODES.values() if n.kind == "writer" and n.alive()]


def hb_of(node_id):
    v = MON.hb.get(node_id)
    return None if v is None else v[1]


def lease_holder(shard):
    v = MON.leases.get(shard)
    return None if v is None or v[1] else v[0]


def wait_until(pred, timeout, what, step=0.25):
    end = now() + timeout
    while now() < end:
        v = pred()
        if v:
            return v
        time.sleep(step)
    raise RuntimeError(f"timeout ({timeout}s) waiting for {what}")


def shard_revision(shard):
    g = MON.latest.get(shard)
    if g is None:
        return None
    return ((manifest(MON.s3, shard, g) or {}).get("projectMeta") or {}).get("sourceRevision")


def all_published():
    """Every shard's LATEST manifest carries the edit clone's HEAD revision, and no writer is indexing."""
    for s in SHARDS:
        if shard_revision(s) != MON.revs[s][-1]:
            return False
    for n in writers_alive():
        hb = hb_of(n.node_id)
        if hb and hb.get("indexing"):
            return False
    return True


def miss_lines(text, shard):
    pat = re.compile(r"Embedding cache\[([^\]]+)\]: (\d+) hit\(s\), (\d+) miss\(es\) for '" + re.escape(shard) + "'")
    return [(m.group(1), int(m.group(2)), int(m.group(3))) for m in pat.finditer(text)]


def takeover(victim, shard, t_kill, changed_files, others):
    """After killing `victim`: lease holder change, the next publish, LATEST validity, provider calls."""
    offsets = {o.name: o.log_offset() for o in others}
    gen_before = MON.latest.get(shard)
    ok_before, detail_before = check_generation(MON.s3, shard)
    holder = wait_until(lambda: lease_holder(shard) not in (None, victim.node_id) and lease_holder(shard),
                        TTL + POLL + 120, f"another writer holding {shard}")
    t_lease = now()
    for t, kind, key, val in list(MON.bucket):  # the exact time from the bucket record of the lease change
        if kind == "lease" and key == shard and t >= t_kill and val and val[0] == holder and not val[1]:
            t_lease = t
            break
    log(f"{shard}: lease taken by {holder} {t_lease - t_kill:.1f}s after the kill")
    wait_until(lambda: (MON.latest.get(shard) or 0) > (gen_before or 0), 900, f"{shard} published by the new owner",
               step=1)
    t_pub = next((t for t, kind, key, val in list(MON.bucket) if kind == "latest" and key == shard and t >= t_kill
                  and val and val > (gen_before or 0)), now())
    gen_after = MON.latest[shard]
    m = manifest(MON.s3, shard, gen_after)
    ok_after, detail_after = check_generation(MON.s3, shard)
    owner = next((o for o in others if o.node_id == holder), None)
    text = owner.log_since(offsets[owner.name]) if owner else ""
    misses = miss_lines(text, shard)
    lines = [ln for ln in text.splitlines() if shard in ln and re.search(
        r"[Rr]estor|FULL index|incremental index|Published shard|backup", ln)]
    provider_calls = sum(x[2] for x in misses)
    within = (t_lease - t_kill) <= TTL + POLL
    res = {"shard": shard, "victim": victim.node_id, "t_kill": rel(t_kill), "gen_at_kill": gen_before,
           "latest_valid_at_kill": [ok_before, detail_before], "new_holder": holder,
           "lease_after_s": round(t_lease - t_kill, 1), "lease_bar_s": TTL + POLL, "lease_within_bar": within,
           "published_gen": gen_after, "publish_after_s": round(t_pub - t_kill, 1),
           "publisher": m.get("writerNode"), "latest_valid_after": [ok_after, detail_after],
           "embedding_cache_lines": misses, "provider_calls": provider_calls, "changed_files": changed_files,
           "provider_calls_within_bar": provider_calls <= changed_files, "owner_log_lines": lines[:12]}
    res["pass"] = bool(ok_before and ok_after and within and res["provider_calls_within_bar"]
                       and m.get("writerNode") == holder)
    log("TAKEOVER " + json.dumps({k: res[k] for k in ("shard", "lease_after_s", "publish_after_s", "provider_calls",
                                                       "changed_files", "pass")}))
    return res


def phase_failover():
    out = {}
    w1 = node("w1").start()
    w1.wait_health()
    wait_until(lambda: len((hb_of(w1.node_id) or {}).get("leases") or []) == 4, 180, "w1 holding all four shards")
    log("w1 holds all four shards")
    for n in ("w2", "w3"):
        node(n).start()
    for n in ("w2", "w3"):
        node(n).wait_health()
    seen = []

    def lucene_commit():
        hb = hb_of(w1.node_id) or {}
        ix = hb.get("indexing") or {}
        key = (ix.get("shard"), ix.get("phase"))
        if not seen or tuple(seen[-1][1:]) != key:
            seen.append((rel(now()), *key))
        if (MON.latest.get("lucene") or 0) > 1 and (manifest(MON.s3, "lucene", MON.latest["lucene"]) or {}).get(
                "writerNode") == w1.node_id:
            raise RuntimeError("w1 published lucene before its commit phase was seen")
        return ix if ix.get("shard") == "lucene" and ix.get("phase") == "commit" else None
    ix = wait_until(lucene_commit, 1200, "w1 indexing lucene in the commit phase", step=0.2)
    log(f"w1 indexing {ix}; killing it")
    held = (hb_of(w1.node_id) or {}).get("leases", [])
    t_k1 = w1.kill9()
    out["k1_phases_seen"] = seen
    out["k1_indexing_at_kill"] = ix
    out["k1"] = takeover(w1, "lucene", t_k1, 0, [node("w2"), node("w3")])
    out["k1"]["victim_leases"] = held
    wait_until(all_published, 1500, "all shards published after K1", step=2)
    out["k1"]["all_current_after_s"] = round(now() - t_k1, 1)
    out["valid_after_k1"] = {s: check_generation(MON.s3, s) for s in SHARDS}
    # K2: the pharos holder while it embeds a 40-file change. First a small change, so the holder has a
    # local index (a takeover owner that had nothing to publish has none) and the K2 run is incremental.
    w = MON.push_change("pharos", 8999)
    wait_until(lambda: shard_revision("pharos") == w["sha"], 900, "the pharos warm-up change published", step=1)
    wait_until(all_published, 600, "all shards current before K2", step=2)
    holder_id = lease_holder("pharos")
    victim = next(n for n in writers_alive() if n.node_id == holder_id)
    other = [n for n in writers_alive() if n is not victim]
    c = MON.push_change("pharos", 9000, kind="method", files=k2_files())
    log(f"K2: pushed a pharos change of {len(c['files'])} files ({c['sha'][:10]}); holder {victim.name}")
    seen2 = []

    def pharos_embed():
        hb = hb_of(victim.node_id) or {}
        ix = hb.get("indexing") or {}
        key = (ix.get("shard"), ix.get("phase"))
        if not seen2 or tuple(seen2[-1][1:]) != key:
            seen2.append((rel(now()), *key))
        if shard_revision("pharos") == c["sha"]:
            raise RuntimeError("the pharos change was published before its embed phase was seen")
        return ix if ix.get("shard") == "pharos" and ix.get("phase") == "embed" else None
    ix2 = wait_until(pharos_embed, 600, f"{victim.name} embedding pharos", step=0.2)
    log(f"{victim.name} indexing {ix2}; killing it")
    t_k2 = victim.kill9()
    out["k2_change"] = {"sha": c["sha"], "files": len(c["files"])}
    out["k2_phases_seen"] = seen2
    out["k2_indexing_at_kill"] = ix2
    out["k2"] = takeover(victim, "pharos", t_k2, len(c["files"]), other)
    out["k2"]["published_revision_is_change"] = shard_revision("pharos") == c["sha"]
    wait_until(all_published, 1500, "all shards published after K2", step=2)
    out["k2"]["all_current_after_s"] = round(now() - t_k2, 1)
    out["pass"] = out["k1"]["pass"] and out["k2"]["pass"]
    save("failover", out)
    return out["pass"]


def writer_count_events(t_from, t_to):
    """For each bucket writer+/writer- event in [t_from, t_to]: when /cluster/status first showed it."""
    evs = []
    for t, kind, key, _ in list(MON.bucket):
        if kind not in ("writer+", "writer-") or not (t_from <= t <= t_to):
            continue
        want_present = kind == "writer+"
        seen = None
        for r in MON.status_since(t - 1):
            if (key in r["writers"]) == want_present:
                seen = r["t"]
                break
        evs.append({"node": key, "change": kind, "bucket_t": rel(t), "status_t": rel(seen),
                    "lag_s": None if seen is None else round(max(0.0, seen - t), 2)})
    return evs


def change_latencies(changes):
    """Per change: appeared in the backlog (pending) after the push, and out of it after its publish."""
    out = []
    mcache = {}
    for c in changes:
        s, revs = c["shard"], MON.revs[c["shard"]]
        ci = revs.index(c["sha"])
        idx = {sha: i for i, sha in enumerate(revs)}
        t_pend = t_out = None
        how = None
        for r in MON.status_since(c["t"]):
            b = (r["shards"].get(s) or {}).get("backlog") or {}
            src_i = idx.get(b.get("sourceRevision"), -1)
            pub_i = idx.get(b.get("publishedRevision"), -1)
            if t_pend is None and b.get("pending") and src_i >= ci:
                t_pend, how = r["t"], "pending"
            if t_pend is None and pub_i >= ci:
                t_pend, how = r["t"], "published-before-seen-pending"
            if pub_i >= ci:
                t_out = r["t"]
                break
        t_pub = None
        for t, kind, key, val in list(MON.bucket):
            if kind == "latest" and key == s and t >= c["t"] - 1 and val:
                if (s, val) not in mcache:
                    mcache[(s, val)] = ((manifest(MON.s3, s, val) or {}).get("projectMeta") or {}).get("sourceRevision")
                if idx.get(mcache[(s, val)], -1) >= ci:
                    t_pub = t
                    break
        out.append({"n": c["n"], "shard": s, "sha": c["sha"][:10], "t_push": rel(c["t"]),
                    "pending_after_s": None if t_pend is None else round(t_pend - c["t"], 1), "how": how,
                    "published_after_s": None if t_pub is None else round(t_pub - c["t"], 1),
                    "cleared_after_publish_s": None if (t_pub is None or t_out is None)
                    else round(t_out - t_pub, 1)})
    return out


def phase_scale():
    out = {"events": []}
    live = writers_alive()
    while len(live) > 1:  # one writer to start from
        live[-1].stop()
        live = writers_alive()
    if not live:
        node("w1").start().wait_health()
        live = writers_alive()
    base = live[0]
    wait_until(lambda: len((hb_of(base.node_id) or {}).get("leases") or []) == 4, 300, f"{base.name} holding 4")
    wait_until(all_published, 900, "all shards current before scaling", step=2)
    out["start_writer"] = base.node_id
    MON.label = "scale"
    t_scale = now()
    MON.changes_on.set()
    plan = [("start", "w4", 240), ("start", "w5", 240), ("start", "w6", 300),
            ("stop", "w6", 180), ("stop", "w5", 180), ("stop", "w4", 240)]
    for what, name, dwell in plan:
        n = node(name)
        if what == "start":
            n.start()
            n.wait_health()
        else:
            n.stop()
        out["events"].append({"t": rel(now()), "action": what, "writer": n.node_id,
                              "writers_alive": [w.node_id for w in writers_alive()]})
        time.sleep(dwell)
        held = {w.node_id: (hb_of(w.node_id) or {}).get("leases") for w in writers_alive()}
        out["events"][-1]["leases_after_dwell"] = held
        log(f"after {what} {name}: leases {held}")
    MON.changes_on.clear()
    time.sleep(2)
    t_changes_end = now()
    wait_until(all_published, 900, "every change published after scaling", step=2)
    time.sleep(2 * HB)
    t_end = now()
    scale_changes = [c for c in MON.changes if t_scale <= c["t"] <= t_changes_end]
    out["changes"] = len(scale_changes)
    lat = change_latencies(scale_changes)
    out["change_latencies"] = lat
    pend = [x["pending_after_s"] for x in lat if x["pending_after_s"] is not None]
    clr = [x["cleared_after_publish_s"] for x in lat if x["cleared_after_publish_s"] is not None]
    out["pending_max_s"], out["pending_p50_s"], out["pending_p95_s"] = (max(pend) if pend else None), pct(pend, 50), \
        pct(pend, 95)
    out["pending_over_poll"] = [x for x in lat if x["pending_after_s"] is None or x["pending_after_s"] > POLL]
    out["cleared_max_s"], out["cleared_p50_s"] = (max(clr) if clr else None), pct(clr, 50)
    out["cleared_over_hb"] = [x for x in lat if x["cleared_after_publish_s"] is None
                              or x["cleared_after_publish_s"] > HB]
    out["writer_count_events"] = writer_count_events(t_scale - 5, t_end)
    lags = [e["lag_s"] for e in out["writer_count_events"] if e["lag_s"] is not None]
    out["writer_count_lag_max_s"] = max(lags) if lags else None
    out["writer_count_unseen"] = [e for e in out["writer_count_events"] if e["lag_s"] is None]
    samples = [s for s in MON.samples if t_scale <= s[0] <= t_end]
    out["queries"] = len(samples)
    out["failed_queries"] = [s for s in samples if s[2] != 200][:20]
    out["failed_count"] = sum(1 for s in samples if s[2] != 200)
    lats = [s[1] for s in samples if s[2] == 200]
    out["latency_ms"] = {"p50": round(1000 * (pct(lats, 50) or 0), 1), "p95": round(1000 * (pct(lats, 95) or 0), 1),
                         "max": round(1000 * max(lats or [0]), 1)}
    out["t_scale"], out["t_end"] = rel(t_scale), rel(t_end)
    out["pass_writer_count"] = bool(out["writer_count_events"]) and not out["writer_count_unseen"] \
        and (out["writer_count_lag_max_s"] or 0) <= HB
    out["pass_backlog_pending"] = not out["pending_over_poll"]
    out["pass_backlog_clear"] = not out["cleared_over_hb"]
    out["pass_queries"] = out["failed_count"] == 0
    save("scale", out)
    log(f"SCALE: changes {out['changes']}, failed queries {out['failed_count']}/{out['queries']}, writer-count lag max"
        f" {out['writer_count_lag_max_s']} s, pending max {out['pending_max_s']} s ({len(out['pending_over_poll'])}"
        f" over {POLL} s), cleared max {out['cleared_max_s']} s ({len(out['cleared_over_hb'])} over {HB} s)")
    return out["pass_writer_count"] and out["pass_backlog_pending"] and out["pass_backlog_clear"] and out["pass_queries"]


def phase_proposals():
    out = {}
    t0 = now()
    MON.label = "proposals"
    MON.qps = HIGH_QPS
    log(f"load raised to {HIGH_QPS} QPS")
    try:
        r = MON.wait_status(lambda r: any(p[0] == "add-readers" for p in r["proposals"]), 360,
                            "an add-readers proposal on /cluster/status")
        pid = next(p[2] for p in r["proposals"] if p[0] == "add-readers")
        out["status_after_s"] = round(r["t"] - t0, 1)
        with MON.lock:
            out["proposal"] = next((p for p in (MON.last_status or {}).get("proposals", [])
                                    if p.get("kind") == "add-readers"), None)

        def sse_hit():
            with MON.lock:
                return next(((t, e, d) for t, e, d in MON.sse if t >= t0 and isinstance(d, dict)
                             and d.get("type") == "appeared" and (d.get("proposal") or {}).get("kind") == "add-readers"),
                            None)

        def hook_hits():
            with MON.lock:
                return [(t, b) for t, b in MON.hooks if t >= t0 and isinstance(b, dict)
                        and (b.get("proposal") or {}).get("kind") == "add-readers"]
        ev = wait_until(sse_hit, 60, "the add-readers event on /cluster/events")
        hk = wait_until(hook_hits, 60, "the add-readers webhook delivery")
        out["sse"] = {"after_s": round(ev[0] - t0, 1), "event": ev[1], "data": ev[2]}
        out["webhook"] = {"deliveries": len(hk), "first_after_s": round(hk[0][0] - t0, 1), "body": hk[0][1]}
        out["same_id"] = (ev[2].get("proposal") or {}).get("id") == pid == (hk[0][1].get("proposal") or {}).get("id")
        out["pass"] = True
    except RuntimeError as e:
        out["error"] = str(e)
        out["pass"] = False
    MON.qps = BASE_QPS
    MON.label = "after-proposals"
    log(f"load back to {BASE_QPS} QPS; waiting for the proposal to clear (info)")
    try:
        MON.wait_status(lambda r: not any(p[0] == "add-readers" for p in r["proposals"]), 300, "proposal cleared")
        out["cleared_after_s"] = round(now() - t0, 1)
    except RuntimeError as e:
        out["cleared"] = str(e)
    with MON.lock:
        out["sse_events"] = [(rel(t), e, d) for t, e, d in MON.sse if t >= t0 and e != "_open"][:40]
        out["webhook_all"] = [(rel(t), b) for t, b in MON.hooks if t >= t0][:20]
    samples = [s for s in MON.samples if s[0] >= t0]
    out["queries"] = len(samples)
    out["failed_count"] = sum(1 for s in samples if s[2] != 200)
    save("proposals", out)
    return out["pass"]


def lucene_estimate():
    """The work loop's estimate for lucene on a writer without a local index (WorkLoop.estimateWithoutLocalIndex
    and DiskGuard.EstimatedRun.peakBytes): full run 2 L + G + S + C, plus extra L + G + C, plus 64 MiB."""
    m = manifest(MON.s3, "lucene", MON.latest["lucene"])
    L = sum(f["length"] for f in m["files"] if f["role"] == "lucene")
    G = sum(f["length"] for f in m["files"] if f["role"] != "lucene")
    S = 0
    for root, dirs, files in os.walk(gate_dir() / "edit" / "lucene"):
        dirs[:] = [d for d in dirs if d != ".git"]
        for f in files:
            try:
                S += os.lstat(os.path.join(root, f)).st_size
            except OSError:
                pass
    C = L
    return {"lucene": L, "graph": G, "source": S, "peak": 2 * L + G + S + C + (L + G + C) + 64 * MIB}


def phase_disk():
    global FILLER
    out = {}
    MON.label = "disk"
    live = writers_alive()
    while len(live) > 1:
        live[-1].stop()
        live = writers_alive()
    other = live[0] if live else node("w1").start()
    other.wait_health()
    wait_until(all_published, 900, "all shards current before the disk step", step=2)
    est = lucene_estimate()
    # headroom: the checkouts it makes (about the lucene source) fit, the lucene run does not
    margin = est["source"] + est["peak"] // 2 + 64 * MIB
    free0 = free_bytes()
    threshold = free0 - margin
    if threshold < MIN_FREE_FLOOR:
        raise RuntimeError(f"not enough disk for the disk step: threshold {threshold / GIB:.1f} GiB")
    out.update({"estimate": est, "margin": margin, "free_at_start": free0, "threshold": threshold})
    log(f"disk: free {free0 / GIB:.3f} GiB, lucene estimate {est['peak'] / MIB:.0f} MiB, threshold"
        f" {threshold / GIB:.3f} GiB (margin {margin / MIB:.0f} MiB)")
    wlow = node("wlow", disk={"minFreeBytes": threshold, "minFreePercent": 0, "checkSeconds": 2,
                              "hysteresisBytes": 128 * MIB})
    wlow.start()
    wlow.wait_health()
    off = wlow.log_offset()
    other.stop()  # wlow takes every shard
    wait_until(lambda: len((hb_of(wlow.node_id) or {}).get("leases") or []) == 4, 180, "wlow holding all four")
    time.sleep(3 * POLL)  # its first polls prepare (clone) every shard it took
    MON.min_free = None
    gen0 = MON.latest["lucene"]
    c = MON.push_change("lucene", 9100)
    log(f"disk: pushed lucene change {c['sha'][:10]}; wlow must refuse or abort its run")
    wait_until(lambda: lease_holder("lucene") != wlow.node_id, 300, "wlow releasing lucene")
    t_rel = now()
    min_free = MON.min_free
    text = wlow.log_since(off)
    out["wlow_log"] = [ln for ln in text.splitlines()
                       if re.search(r"fit|low on disk|abort|[Rr]efus|releas|[Ee]stimat", ln)][:15]
    out["refused"] = any("fit" in ln for ln in out["wlow_log"])
    out["aborted"] = any("aborted at a safe point" in ln for ln in out["wlow_log"])
    out["min_free_during_attempt"] = min_free
    out["crossed"] = min_free is not None and min_free < threshold
    out["release_after_push_s"] = round(t_rel - c["t"], 1)
    out["latest_unchanged_at_release"] = MON.latest["lucene"] == gen0
    log(f"disk: lucene released by wlow {out['release_after_push_s']}s after the push (refused {out['refused']},"
        f" aborted {out['aborted']}); min free {min_free / GIB:.3f} GiB vs threshold {threshold / GIB:.3f}")
    other.start()
    other.wait_health()
    wait_until(lambda: lease_holder("lucene") == other.node_id, TTL + POLL + 120, f"{other.name} taking lucene")
    out["taken_by"] = other.node_id
    out["taken_after_release_s"] = round(now() - t_rel, 1)
    wait_until(lambda: shard_revision("lucene") == c["sha"], 900, "the lucene change published", step=1)
    out["published_by"] = manifest(MON.s3, "lucene", MON.latest["lucene"]).get("writerNode")
    out["published_after_push_s"] = round(now() - c["t"], 1)
    out["gen_before"], out["gen_after"] = gen0, MON.latest["lucene"]
    served = [s for s in MON.samples if c["t"] <= s[0] <= now()]
    out["queries_during"] = len(served)
    out["failed_during"] = sum(1 for s in served if s[2] != 200)
    out["lucene_queries_during"] = sum(1 for s in served if s[3] == "lucene" and s[2] == 200)
    # low: a filler pushes free space below the threshold
    fill = free_bytes() - threshold + 256 * MIB
    if free_bytes() - fill < MIN_FREE_FLOOR:
        raise RuntimeError("the filler would cross the gate's own free-space floor")
    FILLER = gate_dir() / "filler.bin"
    with open(FILLER, "wb") as f:
        os.posix_fallocate(f.fileno(), 0, fill)
    t_fill = now()
    out["filler_bytes"] = fill
    log(f"disk: filler {fill / MIB:.0f} MiB written; free {free_bytes() / GIB:.3f} GiB")
    r = MON.wait_status(lambda r: ("writer-low-disk", wlow.node_id) in [tuple(h) for h in r["health"]], 60,
                        "the writer-low-disk entry")
    out["low_disk_entry_after_s"] = round(r["t"] - t_fill, 1)
    out["low_disk_proposal"] = [p for p in r["proposals"] if p[0] == "writer-disk"]
    out["health_at_low"] = r["health"]
    held = (hb_of(wlow.node_id) or {}).get("leases") or []
    target = next((s for s in ("alpha", "beta", "pharos") if s in held), None)
    if target:  # a change of a shard wlow holds is handed over while it is low
        c2 = MON.push_change(target, 9101)
        wait_until(lambda: shard_revision(target) == c2["sha"], 600, f"{target} change published while wlow is low",
                   step=1)
        out["low_handover"] = {"shard": target, "published_by": manifest(MON.s3, target, MON.latest[target]).get(
            "writerNode"), "after_s": round(now() - c2["t"], 1)}
    remove_filler()
    t_free = now()
    r = MON.wait_status(lambda r: ("writer-low-disk", wlow.node_id) not in [tuple(h) for h in r["health"]]
                        and ((r["writers"].get(wlow.node_id) or {}).get("disk") or {}).get("low") is False, 120,
                        "wlow recovered")
    out["recovered_after_s"] = round(r["t"] - t_free, 1)
    held = (hb_of(wlow.node_id) or {}).get("leases") or []
    out["wlow_leases_after_recovery"] = held
    if held:  # recovered: it publishes a change of a shard it holds
        s3 = held[0]
        c3 = MON.push_change(s3, 9102)
        wait_until(lambda: shard_revision(s3) == c3["sha"], 600, f"{s3} change published after recovery", step=1)
        out["after_recovery"] = {"shard": s3, "published_by": manifest(MON.s3, s3, MON.latest[s3]).get("writerNode"),
                                 "after_s": round(now() - c3["t"], 1)}
    out["pass"] = bool((out["refused"] or out["aborted"]) and not out["crossed"] and out["latest_unchanged_at_release"]
                       and out["published_by"] == other.node_id and out["failed_during"] == 0
                       and (not held or (out.get("after_recovery") or {}).get("published_by") == wlow.node_id))
    save("disk", out)
    return out["pass"]


def phase_check():
    out = {"shards": {}}
    ok = True
    for s in SHARDS:
        rows = chain(MON.s3, s)
        latest = MON.s3.get_json(f"shards/{s}/LATEST")["generation"]
        cv, problems = chain_verdict(rows, latest)
        po = published_once(rows, MON.revs[s])
        valid = check_generation(MON.s3, s)
        out["shards"][s] = {"latest": latest, "generations": len(rows), "chain_ok": cv, "chain_problems": problems,
                            "published_once": po, "latest_valid": valid,
                            "rows": [(g, p, w, (r or "")[:10], c) for g, p, w, r, c in rows]}
        good = cv and not po["duplicates"] and not po["lost"] and not po["unknown_revisions"] and valid[0]
        ok = ok and good
        log(f"CHECK {s}: LATEST {latest}, {len(rows)} manifests, chain {'ok' if cv else problems},"
            f" duplicates {po['duplicates']}, lost {po['lost']}, coalesced {len(po['coalesced'])}")
    with MON.lock:
        samples = list(MON.samples)
    out["queries_total"] = len(samples)
    out["failed_total"] = sum(1 for s in samples if s[2] != 200)
    out["failed_by_phase"] = {}
    for s in samples:
        if s[2] != 200:
            out["failed_by_phase"][s[4]] = out["failed_by_phase"].get(s[4], 0) + 1
    out["failed_samples"] = [s for s in samples if s[2] != 200][:20]
    out["status_poll_errors"] = MON.status_errors
    out["pass"] = ok and out["failed_total"] == 0
    save("check", out)
    return out["pass"]


def marker_check():
    m = (gate_dir() / "marker").stat().st_mtime
    newer = []
    for base in (Path.home() / ".pharos", Path.home() / ".djl.ai"):
        for root, dirs, files in os.walk(base):
            for f in files:
                p = os.path.join(root, f)
                try:
                    if os.lstat(p).st_mtime > m:
                        newer.append(p)
                except OSError:
                    pass
    return newer


# --------------------------------------------------------------------------- self-test


def self_test():
    rows = [(1, None, "b", None, ""), (2, 1, "w1", "a", ""), (3, 2, "w2", "c", ""), (4, 3, "w2", "d", "")]
    assert chain_verdict(rows, 4) == (True, [])
    assert not chain_verdict(rows[:2] + rows[3:], 4)[0]
    assert not chain_verdict(rows + [(5, 3, "x", "e", "")], 4)[0]
    po = published_once(rows, ["seed", "a", "b", "c", "d"])
    assert po["lost"] == [] and po["coalesced"] == [2] and not po["duplicates"], po
    po = published_once(rows + [(5, 4, "w", "d", "")], ["seed", "a", "b", "c", "d", "e"])
    assert po["duplicates"] and po["lost"] == [5], po
    assert pct([1, 2, 3, 4], 50) == 2 and pct([5], 95) == 5 and pct(list(range(1, 101)), 95) == 95
    assert METHOD_RE.match("    public int add(int n) {\n") and not NOT_METHOD.search("    public int add")
    assert not METHOD_RE.match("public class Foo {\n")
    assert METHOD_RE.match("    private static List<String> names(Path p) throws IOException {\n")
    assert NOT_METHOD.search("    } else if (x) {".split("(")[0])
    st = {"layout": {"summary": {"shards": 1, "writers": 1, "readers": 2},
                     "shards": [{"shard": "a", "generation": 3, "lease": {"writer": "w"}, "backlog": {"pending": True}}],
                     "writers": [{"node": "w", "leases": ["a"], "indexing": None}]},
          "health": [{"kind": "writer-low-disk", "target": "w"}], "proposals": [{"kind": "add-readers", "id": "x"}]}
    r = compact_status(1.0, st)
    assert r["nwriters"] == 1 and r["shards"]["a"]["lease"] == "w" and ("writer-low-disk", "w") in r["health"]
    print("self-test OK")


# --------------------------------------------------------------------------- main


def main():
    global MON
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--phases", default="failover,scale,proposals,disk,check")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        self_test()
        return 0
    phases = [p.strip() for p in args.phases.split(",") if p.strip() and p.strip() != "none"]
    unknown = set(phases) - {"failover", "scale", "proposals", "disk", "check"}
    if unknown:
        ap.error(f"unknown phases {sorted(unknown)}")
    if args.dry_run:
        print(f"driver (dry run): phases {phases}; heartbeat {HB}s, writer poll {POLL}s, lease TTL {TTL}s, min hold"
              f" {MIN_HOLD}s; load {BASE_QPS} QPS keyword (proposals {HIGH_QPS}); a change every {CHANGE_EVERY}s")
        return 0
    if not phases:
        print("driver: no phases")
        return 0
    results = {}
    MON = Monitors()
    try:
        for name, kind in (("r1", "reader"), ("r2", "reader"), ("router", "router")):
            node(name, kind).start()
        for name in ("r1", "r2", "router"):
            NODES[name].wait_health()
        MON.start()
        time.sleep(3)
        MON.load_on.set()
        log(f"readers and router up; load {BASE_QPS} QPS")
        for p in phases:
            log(f"== phase {p} (MemAvailable {mem_available_gib():.1f} GiB, free {free_bytes() / GIB:.1f} GiB)")
            fn = {"failover": phase_failover, "scale": phase_scale, "proposals": phase_proposals, "disk": phase_disk,
                  "check": phase_check}[p]
            try:
                results[p] = fn()
            except Exception as e:
                import traceback
                traceback.print_exc()
                log(f"phase {p} FAILED with {e!r}")
                results[p] = False
                save(p + "-error", {"error": repr(e)})
    finally:
        if MON is not None:
            MON.load_on.clear()
        time.sleep(1)
        remove_filler()
        stop_all()
        if MON is not None:
            MON.close()
            with MON.lock:
                (gate_dir() / "bucket-events.json").write_text(json.dumps(
                    [(rel(t), k, key, v) for t, k, key, v in MON.bucket]))
                (gate_dir() / "sse-events.json").write_text(json.dumps([(rel(t), e, d) for t, e, d in MON.sse]))
                (gate_dir() / "webhook.json").write_text(json.dumps([(rel(t), b) for t, b in MON.hooks]))
                (gate_dir() / "load-samples.json").write_text(json.dumps(
                    [(rel(t), round(d, 4), st, p, lb, e) for t, d, st, p, lb, e in MON.samples]))
                (gate_dir() / "changes.json").write_text(json.dumps([dict(c, t=rel(c["t"])) for c in MON.changes]))
                (gate_dir() / "status-at-writer-changes.json").write_text(json.dumps(
                    {k: [(rel(t), s) for t, s in v[:8]] for k, v in MON.status_full.items()}))
        newer = marker_check()
        save("safety", {"newer_than_marker": newer[:50], "count": len(newer),
                        "min_free_gib": None if MON is None or MON.min_free is None else round(MON.min_free / GIB, 2)})
    verdict = all(results.values())
    print("GATE " + ("PASSED" if verdict else "FAILED") + ": "
          + ", ".join(f"{p} {'pass' if ok else 'FAIL'}" for p, ok in results.items()))
    return 0 if verdict else 1


if __name__ == "__main__":
    sys.exit(main())
