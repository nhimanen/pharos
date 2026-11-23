#!/usr/bin/env python3
"""End-to-end gate of the pharos Kubernetes operator on minikube (driver; see scripts/operator-e2e.sh).

Runs against the cluster kubectl points at, in namespace E2E_NS (default "pharos"), where the CRD, the
RBAC and the operator are already installed and the images loaded (operator-e2e.sh does that). Every
pod it creates uses the pharos image; nothing outside the cluster is indexed, read or written, apart
from the output directory (E2E_OUT).

Phases (E2E_PHASES, comma-separated, default all, in this order):
  fixture     git server pod with two tiny generated repos (alpha: Java, beta: Python), served over
              dumb HTTP from inside the cluster; helper pods (mc for the bucket, an unlabelled probe)
  cluster     creates the PharosCluster "e2e": MinIO, alpha --no-embed, beta with embeddings on,
              2 writers, 1..3 readers, short cooldown and trigger windows
  ready       waits for phase Ready with every project published
  infra       DNS of the headless Services, NetworkPolicy (the operator's status reads are admitted,
              an unlabelled pod is not), StatefulSet SSA fields (last-scale annotation, revisions)
  reference   single-node pharos pod indexing the same commits; top-10 of every query must match
  load        query load until add-readers is applied (and re-applied after the cooldown), with an
              operator restart in between; 0 failed queries
  scaledown   load removed (a trickle stays): readers step back down to min; 0 failed queries
  takeover    kubectl delete --force on a writer holding a lease, a commit to its shard: another
              live holder publishes the next generation; 0 failed queries
  writerdown  spec.writers 2 -> 1 while both shards have a new commit: one writer at a time, the
              removed one drains and its leases move; 0 failed queries
  generations every shard's manifests form one parentGeneration chain up to LATEST (no lost or
              duplicate generation)
  preflight   spec.version changes with a tampered bucket: format 999 -> PreflightRefused (Job failed
              by its podFailurePolicy), an unreadable manifest -> PreflightFailed; no workload changes
  upgrade     the bucket restored, a version change rolls readers, writers, router in that order
  reshard     `pharos reshard alpha --spec "[project, ring(path, shards=2)]"` in a writer pod: the
              writer builds and flips it; /cluster/status lists alpha as one published project on two
              ring shards with the same docCount, the operator's ProjectsPublished stays True throughout
              and its status carries that docCount, and every query's top-10 matches a fresh
              single-node reference indexed at the current commits
  secrets     no token or MinIO key in any non-Secret object (-o yaml), Event, or the operator's logs
  reapply     kubectl delete of the PharosCluster keeps the MinIO PVC and credentials Secret (no owner
              reference); the same cluster applied again adopts them and answers every query as before
  teardown    deletes the PharosCluster and the e2e pods; owned objects are garbage-collected; then
              the documented full teardown: the cluster's PVCs and Secrets by label

With E2E_EMBEDDINGS=<model> (operator-e2e.sh --with-embeddings) the vector run replaces the list:
fixture,cluster,ready,reference,embedcache,generations,secrets,teardown on 1 writer and 1 reader that
download the model on first start; beta is embedded, and the reference also compares vector and hybrid
top-10. embedcache: the writer's work volume is emptied and its pod deleted; after a commit the next run
must be incremental (no FULL run: file state and embed cache restored from the bucket), and vector and
hybrid search must find the new file.

Prints one line per check and ends with "E2E RESULT: PASS (n checks)" or "E2E RESULT: FAIL ...".
Exit code 0 only on PASS.
"""
import base64
import json
import os
import random
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

NS = os.environ.get("E2E_NS", "pharos")
PROFILE = os.environ.get("E2E_PROFILE", "pharos-e2e")
# every kubectl call names the e2e context; main() refuses to start without the run's own kubeconfig
KUBECTL = ["kubectl", "--context", PROFILE]
NAME = "e2e"
IMAGE_REPO = os.environ.get("E2E_PHAROS_IMAGE", "pharos")
VERSION = os.environ.get("E2E_VERSION", "1.4.1-e2e")
# the same image under other tags: the pre-flight and upgrade scenarios (operator-e2e.sh tags them)
V_REFUSED, V_FAILED, V_UPGRADE = (os.environ.get("E2E_VERSION_REFUSED", "1.4.2-e2e"),
                                  os.environ.get("E2E_VERSION_FAILED", "1.4.3-e2e"),
                                  os.environ.get("E2E_VERSION_UPGRADE", "1.4.4-e2e"))
MC_IMAGE = os.environ.get("E2E_MC_IMAGE", "docker.io/pgsty/mc:RELEASE.2026-11-12T00-00-00Z")
OUT = Path(os.environ.get("E2E_OUT", "e2e-out"))
PHASES_ALL = ["fixture", "cluster", "ready", "infra", "reference", "load", "scaledown", "takeover",
              "writerdown", "generations", "preflight", "upgrade", "reshard", "secrets", "reapply", "teardown"]

# E2E_EMBEDDINGS=<model> (operator-e2e.sh --with-embeddings): the vector run. One writer and one reader
# that download the model on first start; beta is embedded, alpha stays --no-embed.
EMBEDDINGS = os.environ.get("E2E_EMBEDDINGS") or None
PHASES_EMBEDDINGS = ["fixture", "cluster", "ready", "reference", "embedcache", "generations", "secrets",
                     "teardown"]

COOLDOWN = 60
MAX_STEP_UP = 1
MAX_STEP_DOWN = 1
READERS_MIN, READERS_MAX = 1, 3
WRITERS_MIN, WRITERS_MAX = 2, 2
QPS_PER_READER = 4
LOAD_QPS = 14          # > 3 readers x 4: add-readers stays in force up to max
TRICKLE_QPS = 0.5      # < 20 % of 4 per reader: remove-readers can rise, queries keep running

GIT_URL = f"http://e2e-git.{NS}.svc:8000"
# mc alias "pharos", the operator's bucket "pharos", pharos's key prefix "pharos"
BUCKET_ROOT = "pharos/pharos/pharos"
PROJECTS = [("alpha", False), ("beta", True)]  # (name, embed)

QUERIES = [
    ("calculate tax for a region", "alpha"), ("invoice total with tax", "alpha"),
    ("refund a payment", "alpha"), ("find customer by email", "alpha"), ("monthly report csv line", "alpha"),
    ("charge card token", "alpha"), ("send payment reminder", "alpha"), ("vat rate", "alpha"),
    ("tokenize text into words", "beta"), ("stem word suffix", "beta"), ("inverted index search", "beta"),
    ("bm25 score", "beta"), ("inverse document frequency", "beta"), ("normalize whitespace", "beta"),
    ("add document to index", "beta"), ("stop words", "beta"),
]

# --------------------------------------------------------------------------------------------- fixture

ALPHA = {
    "pom.xml": """<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>e2e</groupId><artifactId>alpha</artifactId><version>1.0</version>
</project>
""",
    "src/main/java/e2e/billing/Customer.java": """package e2e.billing;

/** A customer with an email address and a billing region. */
public record Customer(String id, String name, String email, String region) {}
""",
    "src/main/java/e2e/billing/LineItem.java": """package e2e.billing;

import java.math.BigDecimal;

/** One line of an invoice: a description, a quantity and a unit price. */
public record LineItem(String description, int quantity, BigDecimal unitPrice) {

    /** The line's amount before tax. */
    public BigDecimal amount() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}
""",
    "src/main/java/e2e/billing/Invoice.java": """package e2e.billing;

import java.math.BigDecimal;
import java.util.List;

/** An invoice for a customer. */
public record Invoice(String number, Customer customer, List<LineItem> lines, boolean paid) {

    /** Sum of the line amounts, before tax. */
    public BigDecimal subtotal() {
        BigDecimal sum = BigDecimal.ZERO;
        for (LineItem line : lines) sum = sum.add(line.amount());
        return sum;
    }
}
""",
    "src/main/java/e2e/billing/TaxCalculator.java": """package e2e.billing;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** Calculates value added tax by region. */
public class TaxCalculator {

    /** The VAT rate of a region, as a fraction. */
    public BigDecimal vatRate(String region) {
        return switch (region) {
            case "FI" -> new BigDecimal("0.255");
            case "SE" -> new BigDecimal("0.25");
            default -> new BigDecimal("0.20");
        };
    }

    /** Calculate the tax of an amount for a region, rounded to cents. */
    public BigDecimal calculateTax(BigDecimal amount, String region) {
        return amount.multiply(vatRate(region)).setScale(2, RoundingMode.HALF_UP);
    }
}
""",
    "src/main/java/e2e/billing/InvoiceService.java": """package e2e.billing;

import java.math.BigDecimal;
import java.util.List;

/** Creates invoices and computes their totals. */
public class InvoiceService {

    private final TaxCalculator taxes = new TaxCalculator();
    private final PaymentGateway payments = new PaymentGateway();

    /** Create an invoice for a customer from line items. */
    public Invoice createInvoice(Customer customer, List<LineItem> lines) {
        return new Invoice("INV-" + System.nanoTime(), customer, List.copyOf(lines), false);
    }

    /** The invoice total with tax: subtotal plus the customer's regional tax. */
    public BigDecimal totalWithTax(Invoice invoice) {
        BigDecimal subtotal = invoice.subtotal();
        return subtotal.add(taxes.calculateTax(subtotal, invoice.customer().region()));
    }

    /** Send a payment reminder for an unpaid invoice. */
    public String sendReminder(Invoice invoice) {
        if (invoice.paid()) return "";
        return "Reminder to " + invoice.customer().email() + ": please pay " + totalWithTax(invoice);
    }

    /** Charge the invoice total to a card. */
    public String pay(Invoice invoice, String cardToken) {
        return payments.charge(invoice, cardToken, totalWithTax(invoice));
    }
}
""",
    "src/main/java/e2e/billing/PaymentGateway.java": """package e2e.billing;

import java.math.BigDecimal;

/** Talks to the card payment provider. */
public class PaymentGateway {

    /** Charge an amount to a card token; returns the payment id. */
    public String charge(Invoice invoice, String cardToken, BigDecimal amount) {
        if (cardToken == null || cardToken.isBlank()) throw new IllegalArgumentException("no card token");
        return "PAY-" + invoice.number() + "-" + amount;
    }

    /** Refund part or all of a payment. */
    public String refund(String paymentId, BigDecimal amount) {
        if (amount.signum() <= 0) throw new IllegalArgumentException("refund amount must be positive");
        return "REFUND-" + paymentId + "-" + amount;
    }
}
""",
    "src/main/java/e2e/billing/CustomerRepository.java": """package e2e.billing;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Stores customers and finds them by email. */
public class CustomerRepository {

    private final Map<String, Customer> byEmail = new HashMap<>();

    /** Save or replace a customer. */
    public void save(Customer customer) {
        byEmail.put(customer.email().toLowerCase(), customer);
    }

    /** Find a customer by email address, ignoring case. */
    public Optional<Customer> findByEmail(String email) {
        return Optional.ofNullable(byEmail.get(email.toLowerCase()));
    }
}
""",
    "src/main/java/e2e/billing/ReportFormatter.java": """package e2e.billing;

import java.util.List;

/** Formats invoice reports. */
public class ReportFormatter {

    private final InvoiceService invoices = new InvoiceService();

    /** One CSV line of an invoice: number, customer email, total with tax. */
    public String csvLine(Invoice invoice) {
        return invoice.number() + "," + invoice.customer().email() + "," + invoices.totalWithTax(invoice);
    }

    /** The monthly report: a CSV header and one line per invoice. */
    public String formatMonthlyReport(List<Invoice> monthInvoices) {
        StringBuilder sb = new StringBuilder("number,email,total\\n");
        for (Invoice invoice : monthInvoices) sb.append(csvLine(invoice)).append('\\n');
        return sb.toString();
    }
}
""",
}

BETA = {
    "README.md": "# textkit\n\nA tiny text search toolkit (pharos e2e fixture).\n",
    "textkit/__init__.py": '"""textkit: tokenize, stem, index and score text."""\n',
    "textkit/tokenizer.py": '''"""Tokenizer."""
import re

STOP_WORDS = {"a", "an", "the", "and", "or", "of", "to", "in"}


def normalize_whitespace(text):
    """Collapse runs of whitespace into single spaces."""
    return re.sub(r"\\s+", " ", text).strip()


def tokenize(text):
    """Tokenize text into lower-case words, dropping stop words."""
    words = re.findall(r"[a-z0-9]+", normalize_whitespace(text).lower())
    return [w for w in words if w not in STOP_WORDS]
''',
    "textkit/stemmer.py": '''"""A very small suffix stemmer."""

SUFFIXES = ("ingly", "edly", "ing", "ed", "es", "s", "ly")


def strip_suffix(word, suffixes=SUFFIXES):
    """Strip the first matching suffix from a word."""
    for suffix in suffixes:
        if word.endswith(suffix) and len(word) - len(suffix) >= 3:
            return word[: -len(suffix)]
    return word


def stem(word):
    """Stem a word by stripping a common suffix."""
    return strip_suffix(word.lower())
''',
    "textkit/scoring.py": '''"""Ranking functions."""
import math


def idf(df, n):
    """Inverse document frequency of a term found in df of n documents."""
    return math.log(1 + (n - df + 0.5) / (df + 0.5))


def bm25(tf, df, n, doc_len, avg_len, k1=1.2, b=0.75):
    """BM25 score of one term in one document."""
    norm = tf * (k1 + 1) / (tf + k1 * (1 - b + b * doc_len / avg_len))
    return idf(df, n) * norm
''',
    "textkit/index.py": '''"""An inverted index."""
from collections import defaultdict

from .scoring import bm25
from .stemmer import stem
from .tokenizer import tokenize


class InvertedIndex:
    """Maps stemmed terms to the documents that contain them."""

    def __init__(self):
        self.postings = defaultdict(dict)
        self.lengths = {}

    def add_document(self, doc_id, text):
        """Add a document to the index."""
        terms = [stem(t) for t in tokenize(text)]
        self.lengths[doc_id] = len(terms)
        for term in terms:
            self.postings[term][doc_id] = self.postings[term].get(doc_id, 0) + 1

    def search(self, query, limit=10):
        """Search the index and return the best documents by BM25."""
        n = len(self.lengths) or 1
        avg = sum(self.lengths.values()) / n
        scores = defaultdict(float)
        for term in (stem(t) for t in tokenize(query)):
            docs = self.postings.get(term, {})
            for doc_id, tf in docs.items():
                scores[doc_id] += bm25(tf, len(docs), n, self.lengths[doc_id], avg)
        return sorted(scores.items(), key=lambda kv: -kv[1])[:limit]
''',
}

MAKE_REPOS = r"""set -eu
export HOME=/tmp
for repo in alpha beta; do
  w=/srv/work/$repo
  mkdir -p "$w"
  for f in /fixture/files/${repo}__*; do
    rel=$(basename "$f" | sed "s/^${repo}__//; s/__/\//g")
    mkdir -p "$w/$(dirname "$rel")"
    cp "$f" "$w/$rel"
  done
  git -C "$w" init -q -b main
  git -C "$w" add -A
  git -C "$w" -c user.name=e2e -c user.email=e2e@example.invalid commit -q -m "fixture $repo"
  git clone -q --bare "$w" "/srv/git/$repo.git"
  git -C "/srv/git/$repo.git" update-server-info
  git -C "$w" remote add origin "/srv/git/$repo.git"
done
echo "fixture repos ready"
exec python3 -m http.server 8000 --directory /srv/git
"""

# usage: commit.sh <repo> <path in repo>  (the file's content on stdin)
COMMIT = r"""set -eu
export HOME=/tmp
w=/srv/work/$1
mkdir -p "$w/$(dirname "$2")"
cat > "$w/$2"
git -C "$w" add -A
git -C "$w" -c user.name=e2e -c user.email=e2e@example.invalid commit -q -m "e2e change $2"
git -C "$w" push -q origin main
git -C "/srv/git/$1.git" update-server-info
git -C "$w" rev-parse HEAD
"""

# ---------------------------------------------------------------------------------------- reporting

CHECKS = []   # (name, ok, detail)
T0 = time.time()


def log(msg):
    print(f"[{time.time() - T0:7.1f}s] {msg}", flush=True)


def check(name, ok, detail=""):
    CHECKS.append((name, bool(ok), detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""), flush=True)
    return bool(ok)


class Abort(Exception):
    pass


# ------------------------------------------------------------------------------------------ kubectl

def kubectl(*args, stdin=None, check_rc=True, timeout=180):
    p = subprocess.run([*KUBECTL, "-n", NS, *args], input=stdin, capture_output=True, text=True, timeout=timeout)
    if check_rc and p.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args[:3])}... failed: {p.stderr.strip()[:400]}")
    return p.stdout


def kjson(*args, check_rc=True):
    out = kubectl(*args, "-o", "json", check_rc=check_rc)
    return json.loads(out) if out.strip() else None


def apply(obj):
    kubectl("apply", "-f", "-", stdin=json.dumps(obj))


def get_or_none(kind, name):
    p = subprocess.run([*KUBECTL, "-n", NS, "get", kind, name, "-o", "json"], capture_output=True, text=True)
    return json.loads(p.stdout) if p.returncode == 0 else None


def wait_for(what, pred, timeout, interval=5):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = pred()
        except Exception as e:  # transient API errors count as "not yet"
            last = None
            err = str(e)[:200]
        else:
            err = None
        if last:
            return last
        time.sleep(interval)
    raise Abort(f"timed out after {timeout}s waiting for {what}" + (f" (last error: {err})" if err else ""))


def cluster():
    return kjson("get", "pharoscluster", NAME)


def condition(c, ctype):
    for x in (c.get("status") or {}).get("conditions") or []:
        if x.get("type") == ctype:
            return x
    return {}


def sts(name):
    return get_or_none("statefulset", f"{NAME}-{name}")


def replicas(name):
    s = sts(name)
    return None if s is None else s["spec"].get("replicas")


def pod_exec(pod, script, stdin=None, container=None, timeout=120):
    args = ["exec", "-i", pod] + (["-c", container] if container else []) + ["--", "sh", "-c", script]
    return kubectl(*args, stdin=stdin, timeout=timeout)


def cluster_status():
    """/cluster/status with the token view, read inside a reader pod (the token never leaves it)."""
    script = ("python3 - <<'EOF'\nimport os, urllib.request\n"
              "r = urllib.request.Request('http://127.0.0.1:7070/cluster/status', headers={"
              "'X-Pharos-Cluster-Token': os.environ['PHAROS_TOKEN']})\n"
              "print(urllib.request.urlopen(r, timeout=10).read().decode())\nEOF")
    for pod in ready_pods("reader") + ready_pods("writer"):
        try:
            return json.loads(pod_exec(pod, script, container="pharos", timeout=30))
        except Exception:
            continue
    raise RuntimeError("no pharos pod answered /cluster/status")


def ready_pods(component):
    pods = kjson("get", "pods", "-l", f"pharos.dev/cluster={NAME},app.kubernetes.io/component={component}")
    out = []
    for p in pods["items"]:
        if p["metadata"].get("deletionTimestamp"):
            continue
        conds = {c["type"]: c["status"] for c in (p.get("status") or {}).get("conditions") or []}
        if conds.get("Ready") == "True":
            out.append(p["metadata"]["name"])
    return sorted(out)


def minikube_ip():
    return os.environ.get("E2E_NODE_IP") or subprocess.run(
        ["minikube", "-p", PROFILE, "ip"],
        capture_output=True, text=True, check=True).stdout.strip()


def node_port(service):
    svc = kjson("get", "service", service)
    return svc["spec"]["ports"][0]["nodePort"]


# ------------------------------------------------------------------------------------------- search

def search(base, query, project, timeout=10, pipeline="keyword"):
    """Top-10 ids of a search; raises on any failure (HTTP, timeout, shape, warnings)."""
    url = f"{base}/api/search?" + urllib.parse.urlencode(
        {"q": query, "pipeline": pipeline, "limit": 10, "project": project})
    with urllib.request.urlopen(url, timeout=timeout) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status}")
        data = json.loads(r.read())
    warnings = []
    if isinstance(data, dict):
        warnings = data.get("warnings") or []
        data = data.get("results")
    if not isinstance(data, list):
        raise RuntimeError("unexpected response shape")
    if warnings:
        raise RuntimeError(f"warnings: {warnings}")
    return [x.get("id") for x in data]


class Load:
    """Paced query load; every query must answer with a non-empty top-10 and no warnings."""

    def __init__(self, base, qps, label):
        self.base, self.qps, self.label = base, qps, label
        self.ok = 0
        self.failed = 0
        self.errors = []
        self.stop_flag = threading.Event()
        self.lock = threading.Lock()
        self.threads = []

    def set_qps(self, qps):
        self.qps = qps

    def _run(self, idx, n):
        rnd = random.Random(idx)
        nxt = time.time()
        while not self.stop_flag.is_set():
            interval = n / max(self.qps, 0.01)
            nxt += interval
            q, project = rnd.choice(QUERIES)
            try:
                ids = search(self.base, q, project)
                if not ids:
                    raise RuntimeError("empty result")
                with self.lock:
                    self.ok += 1
            except Exception as e:
                with self.lock:
                    self.failed += 1
                    if len(self.errors) < 20:
                        self.errors.append(f"{time.strftime('%H:%M:%S')} {project} {q!r}: {str(e)[:160]}")
            delay = nxt - time.time()
            if delay > 0:
                self.stop_flag.wait(delay)
            else:
                nxt = time.time()

    def start(self, threads=6):
        for i in range(threads):
            t = threading.Thread(target=self._run, args=(i, threads), daemon=True)
            t.start()
            self.threads.append(t)
        return self

    def stop(self):
        self.stop_flag.set()
        for t in self.threads:
            t.join(timeout=15)

    def snapshot(self):
        with self.lock:
            return self.ok, self.failed, list(self.errors)


class Timeline:
    """Every 2 s: readers/writers replicas and their last-scale annotations."""

    def __init__(self):
        self.rows = []
        self.stop_flag = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self.stop_flag.is_set():
            try:
                items = kjson("get", "statefulsets", "-l", f"pharos.dev/cluster={NAME}")["items"]
                row = {"t": time.time()}
                for s in items:
                    comp = s["metadata"]["labels"].get("app.kubernetes.io/component")
                    if comp in ("reader", "writer"):
                        row[comp] = s["spec"].get("replicas")
                        row[comp + "LastScale"] = (s["metadata"].get("annotations") or {}).get("pharos.dev/last-scale")
                self.rows.append(row)
            except Exception:
                pass
            self.stop_flag.wait(2)

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.stop_flag.set()
        self.thread.join(timeout=10)

    def changes(self, comp, since=0.0):
        # wait for a row taken after this call, so a change just observed elsewhere is in the rows
        now = time.time()
        deadline = now + 15
        while time.time() < deadline and not (self.rows and self.rows[-1]["t"] > now):
            time.sleep(0.5)
        out = []
        prev = None
        for r in self.rows:
            if comp not in r:
                continue
            # rows before `since` only seed the starting value: a change applied within one poll of
            # `since` still shows as one
            if r["t"] >= since and prev is not None and r[comp] != prev:
                out.append((r["t"], prev, r[comp]))
            prev = r[comp]
        return out


# --------------------------------------------------------------------------------------- the state

class State:
    def __init__(self):
        self.base = None          # the router's URL (NodePort)
        self.ref_base = None
        self.timeline = None
        self.secrets = {}         # name -> value, in memory only
        self.loads = []           # every Load of phases load..writerdown
        self.commits = {}         # repo -> number of e2e commits
        self.expected = {}        # (query, project) -> top-10 ids from the reference


ST = State()


def router_base():
    if ST.base is None:
        ST.base = f"http://{minikube_ip()}:{node_port(NAME + '-router')}"
    return ST.base


def commit(repo, label):
    ST.commits[repo] = ST.commits.get(repo, 0) + 1
    n = ST.commits[repo]
    if repo == "alpha":
        path = f"src/main/java/e2e/billing/Change{n}.java"
        body = (f"package e2e.billing;\n\n/** e2e change {n} ({label}): zebra quokka marker {n}. */\n"
                f"public class Change{n} {{\n    /** Marker method {n}. */\n"
                f"    public int quokkaMarker{n}() {{ return {n}; }}\n}}\n")
    else:
        path = f"textkit/change_{n}.py"
        body = f'"""e2e change {n} ({label}): zebra quokka marker {n}."""\n\n\ndef quokka_marker_{n}():\n    return {n}\n'
    rev = pod_exec("e2e-git", f"sh /fixture/scripts/commit.sh {repo} {path}", stdin=body).strip().splitlines()[-1]
    log(f"committed {repo} {path} -> {rev[:12]}")
    return rev


def shard_generation(status, project):
    for s in (status.get("layout") or {}).get("shards") or []:
        if s.get("shard") == project or (s.get("keys") or {}).get("project") == project:
            return s
    return None


def wait_published(project, min_generation, timeout=420):
    def ok():
        s = shard_generation(cluster_status(), project)
        g = s and s.get("generation")
        return g if g is not None and g >= min_generation else None
    return wait_for(f"{project} generation >= {min_generation}", ok, timeout, interval=5)


def wait_marker_searchable(project, n, timeout=420):
    term = f"quokkaMarker{n}" if project == "alpha" else f"quokka_marker_{n}"
    def ok():
        ids = search(router_base(), term, project)
        want = f"Change{n}" if project == "alpha" else f"change_{n}"
        return any(want in (i or "") for i in ids)
    return wait_for(f"{project} change {n} searchable through the router", ok, timeout, interval=5)


# ------------------------------------------------------------------------------------------- phases

def pod(name, image, command, labels=None, env=None, volumes=None, mounts=None, ports=None):
    c = {"name": "main", "image": image, "imagePullPolicy": "IfNotPresent", "command": command,
         "env": env or [], "volumeMounts": mounts or [],
         "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
         "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}}}
    if ports:
        c["ports"] = ports
    return {"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": NS, "labels": {"app.kubernetes.io/part-of": "pharos-e2e",
                                                                    **(labels or {})}},
            "spec": {"securityContext": {"runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001,
                                         "runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
                     "terminationGracePeriodSeconds": 2, "containers": [c], "volumes": volumes or []}}


def phase_fixture():
    files = {}
    for repo, tree in (("alpha", ALPHA), ("beta", BETA)):
        for rel, text in tree.items():
            files[f"{repo}__" + rel.replace("/", "__")] = text
    apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "e2e-fixture-files", "namespace": NS},
           "data": files})
    apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "e2e-fixture-scripts", "namespace": NS},
           "data": {"make-repos.sh": MAKE_REPOS, "commit.sh": COMMIT}})
    image = f"{IMAGE_REPO}:{VERSION}"
    git = pod("e2e-git", image, ["sh", "/fixture/make-repos.sh"], labels={"app": "e2e-git"},
              volumes=[{"name": "files", "configMap": {"name": "e2e-fixture-files"}},
                       {"name": "scripts", "configMap": {"name": "e2e-fixture-scripts"}},
                       {"name": "srv", "emptyDir": {}}],
              mounts=[{"name": "files", "mountPath": "/fixture/files"},
                      {"name": "scripts", "mountPath": "/fixture/scripts"},
                      {"name": "srv", "mountPath": "/srv"}],
              ports=[{"name": "http", "containerPort": 8000}])
    # the scripts are run as /fixture/<name>: link them next to the files
    git["spec"]["containers"][0]["command"] = ["sh", "-c", "mkdir -p /srv/git /srv/work && "
                                               "exec sh /fixture/scripts/make-repos.sh"]
    git["spec"]["containers"][0]["readinessProbe"] = {"tcpSocket": {"port": 8000}, "periodSeconds": 2}
    apply(git)
    apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "e2e-git", "namespace": NS},
           "spec": {"selector": {"app": "e2e-git"}, "ports": [{"port": 8000, "targetPort": 8000}]}})
    # probe: an unlabelled pod (no pharos.dev/cluster), for DNS and the NetworkPolicy's refusal
    apply(pod("e2e-probe", image, ["sleep", "86400"]))
    wait_for("the git server", lambda: "e2e-git" in ready_names(), 180, 2)
    out = kubectl("exec", "e2e-probe", "--", "git", "ls-remote", f"{GIT_URL}/alpha.git", "main",
                  check_rc=False)
    check("fixture: git server answers ls-remote over HTTP", "refs/heads/main" in out, out.strip()[:60])


def ready_names():
    pods = kjson("get", "pods", "-l", "app.kubernetes.io/part-of=pharos-e2e")
    return {p["metadata"]["name"] for p in pods["items"]
            if any(c["type"] == "Ready" and c["status"] == "True"
                   for c in (p.get("status") or {}).get("conditions") or [])}


def cluster_resource():
    resource = {
        "apiVersion": "pharos.dev/v1alpha1", "kind": "PharosCluster",
        "metadata": {"name": NAME, "namespace": NS},
        "spec": {
            "version": VERSION,
            "storage": {"minio": {"storage": "2Gi"}},
            # NoOp embeddings: no model in the cluster (the brief's second project still registers
            # with embeddings on; see docs/superpowers/gates/2026-11-22-operator.md)
            "embeddings": {"model": "none"},
            "projects": [{"name": n, "git": {"url": f"{GIT_URL}/{n}.git", "ref": "main"}, "embed": e}
                         for n, e in PROJECTS],
            "writers": {"min": WRITERS_MIN, "max": WRITERS_MAX, "workVolume": "1Gi",
                        "resources": {"requests": {"cpu": "200m", "memory": "640Mi"}, "limits": {"memory": "900Mi"}}},
            "readers": {"min": READERS_MIN, "max": READERS_MAX, "mode": "lazy", "cacheVolume": "1Gi",
                        "resources": {"requests": {"cpu": "200m", "memory": "512Mi"}, "limits": {"memory": "768Mi"}}},
            "router": {"replicas": 1, "service": {"type": "NodePort"}},
            "autoscaling": {
                "enabled": True, "cooldownSeconds": COOLDOWN, "maxStepUp": MAX_STEP_UP, "maxStepDown": MAX_STEP_DOWN,
                "triggers": {
                    "readers": {"scaleUp": {"warmP95Millis": 5000, "qpsPerReader": QPS_PER_READER, "forSeconds": 30},
                                "scaleDown": {"loadBelowPercent": 20, "forSeconds": 60}},
                    "writers": {"scaleUp": {"backlogShards": 5, "oldestPendingSeconds": 3600, "forSeconds": 600},
                                "scaleDown": {"idleForSeconds": 3600}}}},
            "pharosConfig": {"writer.pollSeconds": 10},
        }}
    if EMBEDDINGS:
        spec = resource["spec"]
        spec["embeddings"] = {"model": EMBEDDINGS}
        # the model runs off-heap (ONNX Runtime): no limit below the heap plus ~1.5 GB of native memory
        big = {"requests": {"cpu": "500m", "memory": "1536Mi"}, "limits": {"memory": "4Gi"}}
        spec["writers"].update({"min": 1, "max": 1, "resources": big, "workVolume": "3Gi"})
        spec["readers"].update({"min": 1, "max": 1, "resources": big, "cacheVolume": "3Gi"})
    return resource


def phase_cluster():
    apply(cluster_resource())
    log("PharosCluster e2e applied")


def projects_published(c):
    ps = (c.get("status") or {}).get("projects") or []
    return len(ps) == len(PROJECTS) and all(p.get("generation") for p in ps)


def phase_ready():
    def ready():
        c = cluster()
        st = c.get("status") or {}
        if st.get("phase") == "Ready" and projects_published(c):
            return c
        return None
    t = time.time()
    try:
        c = wait_for("phase Ready with every project published", ready, 2400 if EMBEDDINGS else 900, 10)
    except Abort:
        c = cluster()
        log("status at timeout: " + json.dumps(c.get("status"), indent=1)[:3000])
        raise
    check("ready: phase Ready, every project has a LATEST generation", True,
          f"{time.time() - t:.0f}s; " + ", ".join(f"{p['name']}=g{p['generation']}" for p in c["status"]["projects"]))
    check("ready: ClusterStatusAvailable=True (operator reads /cluster/status through the NetworkPolicy)",
          condition(c, "ClusterStatusAvailable").get("status") == "True",
          condition(c, "ClusterStatusAvailable").get("message", ""))


def phase_infra():
    # DNS: the operator's form (<svc>.<ns>.svc) and each pod's advertised host
    script = ("python3 - <<'EOF'\nimport socket, json\nout = {}\n"
              "for h in %s:\n"
              "    try:\n        out[h] = sorted({a[4][0] for a in socket.getaddrinfo(h, 7070)})\n"
              "    except Exception as e:\n        out[h] = str(e)\n"
              "print(json.dumps(out))\nEOF")
    pods = kjson("get", "pods", "-l", f"pharos.dev/cluster={NAME}")["items"]
    ips = {}
    for p in pods:
        comp = p["metadata"]["labels"].get("app.kubernetes.io/component")
        if comp in ("reader", "writer") and p.get("status", {}).get("podIP"):
            ips.setdefault(comp, {})[p["metadata"]["name"]] = p["status"]["podIP"]
    hosts = [f"{NAME}-readers-hl.{NS}.svc", f"{NAME}-writers-hl.{NS}.svc"]
    for comp, m in ips.items():
        for podname in m:
            hosts.append(f"{podname}.{NAME}-{comp}s-hl.{NS}.svc.cluster.local")
    res = json.loads(pod_exec("e2e-probe", script % json.dumps(hosts)))
    for comp, m in ips.items():
        svc = f"{NAME}-{comp}s-hl.{NS}.svc"
        check(f"dns: {svc} resolves to every {comp} pod", res.get(svc) == sorted(m.values()), f"{res.get(svc)}")
        for podname, ip in m.items():
            h = f"{podname}.{NAME}-{comp}s-hl.{NS}.svc.cluster.local"
            check(f"dns: {podname}'s advertised host resolves to its pod IP", res.get(h) == [ip], f"{res.get(h)}")

    # NetworkPolicy: the unlabelled probe is refused on 7070, but reaches the router
    probe = ("python3 - <<'EOF'\nimport socket\nfor h, p in %s:\n"
             "    s = socket.socket(); s.settimeout(4)\n"
             "    try:\n        s.connect((h, p)); print(h, 'open')\n"
             "    except Exception as e:\n        print(h, 'refused', type(e).__name__)\n"
             "    finally:\n        s.close()\nEOF")
    reader0 = f"{NAME}-readers-0.{NAME}-readers-hl.{NS}.svc.cluster.local"
    out = pod_exec("e2e-probe", probe % json.dumps([[reader0, 7070], [f"{NAME}-router.{NS}.svc", 7080]]))
    check("networkpolicy: an unlabelled pod cannot reach a reader's 7070", f"{reader0} refused" in out,
          out.strip().replace("\n", "; "))
    check("networkpolicy: the router's port stays open", f"{NAME}-router.{NS}.svc open" in out)
    c = cluster()
    check("networkpolicy: the operator's /cluster/status reads are admitted (ClusterStatusAvailable)",
          condition(c, "ClusterStatusAvailable").get("status") == "True")

    # StatefulSet fields under server-side apply: the operator owns replicas and its annotations; the
    # controller's revision fields are present and consistent
    for comp in ("readers", "writers"):
        s = sts(comp)
        st = s.get("status") or {}
        check(f"sts {comp}: currentRevision == updateRevision, all replicas updated and ready",
              st.get("currentRevision") and st.get("currentRevision") == st.get("updateRevision")
              and st.get("updatedReplicas") == s["spec"]["replicas"] == st.get("readyReplicas"),
              f"current={st.get('currentRevision')} update={st.get('updateRevision')} "
              f"updated={st.get('updatedReplicas')} ready={st.get('readyReplicas')}")
        full = kjson("get", "statefulset", f"{NAME}-{comp}", "--show-managed-fields")
        managers = [m.get("manager") for m in full["metadata"].get("managedFields") or []
                    if m.get("operation") == "Apply"]
        check(f"sts {comp}: applied by server-side apply (field managers {managers})", bool(managers))


def reference_node(name="e2e-ref"):
    """A single-node pharos pod (and NodePort Service) indexing the repos' current commits; its base URL."""
    image = f"{IMAGE_REPO}:{VERSION}"
    clone = " && ".join(f"git clone -q {GIT_URL}/{n}.git /data/src/{n}" for n, _ in PROJECTS)
    index = " && ".join(f"pharos-entrypoint index /data/src/{n} --project {n}"
                        + ("" if EMBEDDINGS and e else " --no-embed") for n, e in PROJECTS)
    ref = pod(name, image, ["sh", "-c", f"export HOME=/tmp; mkdir -p /data/src && {clone} && {index} "
                                        "&& exec pharos-entrypoint"],
              labels={"app": name},
              env=[{"name": "PHAROS_ROLE", "value": "all"}, {"name": "PHAROS_HOME", "value": "/data/pharos"},
                   {"name": "PHAROS_CONFIG", "value": "/etc/ref/config.json" if EMBEDDINGS else "/data/none.json"}],
              volumes=[{"name": "data", "emptyDir": {}}, {"name": "tmp", "emptyDir": {}}]
              + ([{"name": "ref-config", "configMap": {"name": "e2e-ref-config"}}] if EMBEDDINGS else []),
              mounts=[{"name": "data", "mountPath": "/data"}, {"name": "tmp", "mountPath": "/tmp"}]
              + ([{"name": "ref-config", "mountPath": "/etc/ref"}] if EMBEDDINGS else []),
              ports=[{"name": "http", "containerPort": 7070}])
    if EMBEDDINGS:
        # single node with exactly the embedding block the operator rendered for the cluster's readers
        rendered = json.loads(kjson("get", "configmap", f"{NAME}-config-reader")["data"]["config.json"])
        rendered.pop("cluster", None)
        apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "e2e-ref-config", "namespace": NS},
               "data": {"config.json": json.dumps(rendered, indent=1)}})
    ref["spec"]["containers"][0]["readinessProbe"] = {"httpGet": {"path": "/health", "port": 7070},
                                                      "periodSeconds": 3}
    ref["spec"]["containers"][0]["resources"] = (
        {"requests": {"cpu": "500m", "memory": "1536Mi"}, "limits": {"memory": "4Gi"}} if EMBEDDINGS
        else {"requests": {"cpu": "200m", "memory": "512Mi"}, "limits": {"memory": "900Mi"}})
    apply(ref)
    apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": name, "namespace": NS},
           "spec": {"type": "NodePort", "selector": {"app": name}, "ports": [{"port": 7070, "targetPort": 7070}]}})
    wait_for(f"the reference node {name}", lambda: name in ready_names(), 1800 if EMBEDDINGS else 600, 5)
    return f"http://{minikube_ip()}:{node_port(name)}"


def phase_reference():
    ST.ref_base = reference_node()
    same = differ = empty = 0
    for q, project in QUERIES:
        r = search(ST.ref_base, q, project)
        c = search(router_base(), q, project)
        ST.expected[(q, project)] = r
        if not r:
            empty += 1
        if r == c:
            same += 1
        else:
            differ += 1
            log(f"MISMATCH [{project}] {q!r}\n   reference: {r}\n   cluster:   {c}")
    check("reference: top-10 identical to a single-node pharos for every query",
          differ == 0 and same == len(QUERIES), f"{same} identical, {differ} differ, {empty} empty in the reference")
    check("reference: every query has results", empty == 0)
    if EMBEDDINGS:
        for pipeline in ("vector", "hybrid"):
            same = differ = empty = 0
            embedded = [(q, p) for q, p in QUERIES if dict(PROJECTS)[p]]
            for q, project in embedded:
                r = search(ST.ref_base, q, project, timeout=60, pipeline=pipeline)
                c = search(router_base(), q, project, timeout=60, pipeline=pipeline)
                empty += 0 if (r and c) else 1
                if r == c:
                    same += 1
                else:
                    differ += 1
                    log(f"MISMATCH {pipeline} [{project}] {q!r}\n   reference: {r}\n   cluster:   {c}")
            check(f"reference: {pipeline} search returns results for every embedded-project query", empty == 0,
                  f"{len(embedded) - empty}/{len(embedded)} non-empty")
            check(f"reference: {pipeline} top-10 identical to the single node", differ == 0,
                  f"{same} identical, {differ} differ")
    kubectl("delete", "pod", "e2e-ref", "--wait=false", check_rc=False)


def proposals(c):
    return (c.get("status") or {}).get("proposals") or []


def apply_owns(comp, field):
    """Whether an Apply field manager of the StatefulSet owns metadata.annotations[field]."""
    full = kjson("get", "statefulset", f"{NAME}-{comp}", "--show-managed-fields")
    for m in full["metadata"].get("managedFields") or []:
        if m.get("operation") == "Apply" and field in json.dumps(
                ((m.get("fieldsV1") or {}).get("f:metadata") or {}).get("f:annotations") or {}):
            return True
    return False


def save_operator_logs(tag):
    OUT.mkdir(parents=True, exist_ok=True)
    text = kubectl("logs", "deploy/pharos-operator", "--tail=-1", check_rc=False, timeout=60)
    (OUT / f"operator-{tag}-{int(time.time())}.log").write_text(text)
    return text


def phase_load():
    if ST.timeline is None:
        ST.timeline = Timeline().start()
    start = time.time()
    load = Load(router_base(), LOAD_QPS, "load").start()
    ST.loads.append(load)
    log(f"load: {LOAD_QPS} qps through the router")
    try:
        wait_for("readers 1 -> 2", lambda: (replicas("readers") or 0) >= 2, 600, 3)
        first = time.time()
        c = cluster()
        applied = [p for p in proposals(c) if p.get("kind") == "add-readers" and p.get("applied")]
        check("load: add-readers raised and applied (status.proposals)", bool(applied),
              json.dumps(applied)[:300])
        ann = (sts("readers")["metadata"].get("annotations") or {}).get("pharos.dev/last-scale")
        check("load: pharos.dev/last-scale on the readers StatefulSet after the apply", bool(ann), str(ann)[:200])
        check("load: the last-scale annotation is owned by the operator's server-side apply",
              apply_owns("readers", "f:pharos.dev/last-scale"), "managedFields")
        # operator restart inside the cooldown: the annotation must keep it from applying again early
        save_operator_logs("before-restart")
        kubectl("delete", "pod", "-l", "app.kubernetes.io/name=pharos-operator", "--wait=false")
        log("operator pod deleted (restart inside the cooldown)")
        wait_for("readers 2 -> 3", lambda: (replicas("readers") or 0) >= 3, 600, 3)
        second = time.time()
        check("load: the in-force proposal is re-applied once per cooldown, across an operator restart",
              second - first >= COOLDOWN - 3, f"{second - first:.0f}s between the two applications")
        ann2 = (sts("readers")["metadata"].get("annotations") or {}).get("pharos.dev/last-scale")
        check("load: last-scale updated by the second application", ann2 and ann2 != ann, str(ann2)[:200])
        wait_for("3 readers ready", lambda: len(ready_pods("reader")) >= 3, 300, 3)
        # stays at max while the proposal is still in force
        time.sleep(COOLDOWN + 15)
        check("load: readers stay at max while add-readers stays in force", replicas("readers") == READERS_MAX,
              f"replicas {replicas('readers')}")
    finally:
        load.stop()
    ok, failed, errors = load.snapshot()
    check("load: 0 failed queries", failed == 0 and ok > 0, f"{ok} ok, {failed} failed {errors[:3]}")
    ups = ST.timeline.changes("reader", start)
    check("load: every reader step within maxStepUp and [min, max]",
          all(0 < b - a <= MAX_STEP_UP and READERS_MIN <= b <= READERS_MAX for _, a, b in ups),
          f"{[(a, b) for _, a, b in ups]}")


def phase_scaledown():
    if ST.timeline is None:
        ST.timeline = Timeline().start()
    start = time.time()
    trickle = Load(router_base(), TRICKLE_QPS, "trickle").start(threads=1)
    ST.loads.append(trickle)
    ST.trickle = trickle
    log(f"load removed; trickle of {TRICKLE_QPS} qps")
    wait_for("readers back to min", lambda: replicas("readers") == READERS_MIN
             and len(ready_pods("reader")) == READERS_MIN, 900, 5)
    downs = ST.timeline.changes("reader", start)
    log(f"reader steps down: {[(round(t - start), a, b) for t, a, b in downs]}")
    check("scaledown: readers back to min", replicas("readers") == READERS_MIN)
    check("scaledown: every step within maxStepDown and [min, max]",
          all(0 < a - b <= MAX_STEP_DOWN and READERS_MIN <= b <= READERS_MAX for _, a, b in downs), f"{downs}")
    gaps = [downs[i][0] - downs[i - 1][0] for i in range(1, len(downs))]
    check("scaledown: at most one change per cooldown", all(g >= COOLDOWN - 3 for g in gaps),
          f"gaps {[round(g) for g in gaps]}")
    ok, failed, errors = trickle.snapshot()
    check("scaledown: 0 failed queries so far", failed == 0, f"{ok} ok, {failed} failed {errors[:3]}")


def lease_holders(status):
    out = {}
    for s in (status.get("layout") or {}).get("shards") or []:
        lease = s.get("lease") or {}
        if lease.get("writer") and not lease.get("released") and not lease.get("expired"):
            out[s["shard"]] = lease["writer"]
    return out


def trickle_running():
    if getattr(ST, "trickle", None) is None or ST.trickle.stop_flag.is_set():
        ST.trickle = Load(router_base(), TRICKLE_QPS, "trickle").start(threads=1)
        ST.loads.append(ST.trickle)
    return ST.trickle


def phase_takeover():
    trickle_running()
    status = wait_for("both shards leased", lambda: (lambda h: h if len(h) == len(PROJECTS) else None)(
        lease_holders(cluster_status())), 300, 5)
    shard, victim = sorted(status.items())[0]
    gen_before = shard_generation(cluster_status(), shard)["generation"]
    started = {w["node"]: w.get("startedAt") for w in (cluster_status()["layout"].get("writers") or [])}
    log(f"leases {status}; force-deleting {victim} (holds {shard}, generation {gen_before})")
    kubectl("delete", "pod", victim, "--force", "--grace-period=0")
    commit(shard, "takeover")

    def taken_over():
        st = cluster_status()
        holder = lease_holders(st).get(shard)
        live = {w["node"]: w for w in (st["layout"].get("writers") or []) if not w.get("stopping")}
        if holder is None or holder not in live:
            return None
        # a different writer, or the same pod name with a new process (StatefulSet re-creates it)
        if holder == victim and live[holder].get("startedAt") == started.get(victim):
            return None
        return holder
    holder = wait_for(f"a live writer holding {shard}'s lease again", taken_over, 300, 5)
    g = wait_published(shard, gen_before + 1)
    wait_marker_searchable(shard, ST.commits[shard])
    check("takeover: lease taken over by a live writer and the next generation published",
          True, f"{shard}: {victim} -> {holder}, generation {gen_before} -> {g}")
    ok, failed, errors = ST.trickle.snapshot()
    check("takeover: 0 failed queries so far", failed == 0, f"{ok} ok, {failed} failed {errors[:3]}")


def patch_cluster(patch):
    kubectl("patch", "pharoscluster", NAME, "--type=merge", "-p", json.dumps(patch))


def phase_writerdown():
    trickle_running()
    if ST.timeline is None:
        ST.timeline = Timeline().start()
    start = time.time()
    st = cluster_status()
    gens = {p: shard_generation(st, p)["generation"] for p, _ in PROJECTS}
    readers_ann = (sts("readers")["metadata"].get("annotations") or {}).get("pharos.dev/last-scale")
    for p, _ in PROJECTS:
        commit(p, "writer scale-down")
    patch_cluster({"spec": {"writers": {"min": 1, "max": 1}}})
    log("spec.writers 2 -> 1 (both shards have a new commit pending)")
    wait_for("writers 2 -> 1", lambda: replicas("writers") == 1, 300, 2)
    wait_for(f"pod {NAME}-writers-1 gone", lambda: get_or_none("pod", f"{NAME}-writers-1") is None, 300, 3)

    def drained():
        s = cluster_status()
        nodes = {w["node"] for w in (s["layout"].get("writers") or [])}
        holders = lease_holders(s)
        if f"{NAME}-writers-1" in nodes or f"{NAME}-writers-1" in holders.values():
            return None
        return holders if len(holders) == len(PROJECTS) else None
    holders = wait_for("writers-1's heartbeat gone and its leases moved", drained, 300, 5)
    for p, _ in PROJECTS:
        g = wait_published(p, gens[p] + 1)
        wait_marker_searchable(p, ST.commits[p])
        log(f"{p}: generation {gens[p]} -> {g}")
    check("writerdown: the removed writer drained and every lease moved to the remaining writer",
          set(holders.values()) == {f"{NAME}-writers-0"}, f"{holders}")
    wc = ST.timeline.changes("writer", start)
    check("writerdown: writers stepped down by one", all(a - b == 1 for _, a, b in wc) and bool(wc), f"{wc}")
    after = (sts("readers")["metadata"].get("annotations") or {}).get("pharos.dev/last-scale")
    check("writerdown: the readers' last-scale annotation survives later server-side applies of the spec change",
          readers_ann is not None and after == readers_ann and apply_owns("readers", "f:pharos.dev/last-scale"),
          str(after)[:120])
    # put the spec back to two writers for the rest of the run
    patch_cluster({"spec": {"writers": {"min": WRITERS_MIN, "max": WRITERS_MAX}}})
    wait_for("writers back to 2 ready", lambda: replicas("writers") == 2 and len(ready_pods("writer")) == 2, 400, 5)
    ST.trickle.stop()
    total_ok = total_failed = 0
    errors = []
    for l in ST.loads:
        ok, failed, errs = l.snapshot()
        total_ok += ok
        total_failed += failed
        errors += errs
    check("steps 5-7: 0 failed queries throughout (load, scale-down, takeover, writer scale-down)",
          total_failed == 0 and total_ok > 0, f"{total_ok} ok, {total_failed} failed {errors[:5]}")
    allrows = ST.timeline.rows
    rs = [r.get("reader") for r in allrows if r.get("reader") is not None]
    ws = [r.get("writer") for r in allrows if r.get("writer") is not None]
    check("scaling stayed within [min, max] throughout", rs and ws and min(rs) >= READERS_MIN
          and max(rs) <= READERS_MAX and min(ws) >= 1 and max(ws) <= WRITERS_MAX,
          f"readers {min(rs)}..{max(rs)}, writers {min(ws)}..{max(ws)}")


def ensure_mc():
    """The mc pod: reads (and in the pre-flight phase tampers with) the bucket, with the bucket Job's env."""
    if "e2e-mc" in ready_names():
        return
    apply(pod("e2e-mc", MC_IMAGE, ["/bin/sh", "-c", "sleep 86400"],
              env=[{"name": "MINIO_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": f"{NAME}-minio",
                                                                              "key": "accessKey"}}},
                   {"name": "MINIO_SECRET_KEY", "valueFrom": {"secretKeyRef": {"name": f"{NAME}-minio",
                                                                              "key": "secretKey"}}},
                   {"name": "MC_HOST_pharos",
                    "value": f"http://$(MINIO_ACCESS_KEY):$(MINIO_SECRET_KEY)@{NAME}-minio.{NS}.svc:9000"},
                   {"name": "HOME", "value": "/tmp"}, {"name": "MC_CONFIG_DIR", "value": "/tmp/.mc"}],
              volumes=[{"name": "tmp", "emptyDir": {}}], mounts=[{"name": "tmp", "mountPath": "/tmp"}]))
    wait_for("the mc pod", lambda: "e2e-mc" in ready_names(), 180, 2)


def mc(cmd, stdin=None):
    ensure_mc()
    return pod_exec("e2e-mc", cmd, stdin=stdin, container="main")


def manifests(shard):
    names = mc(f"mc ls --json {BUCKET_ROOT}/shards/{shard}/manifests/")
    out = {}
    for line in names.splitlines():
        if not line.strip():
            continue
        key = json.loads(line).get("key", "")
        m = re.fullmatch(r"gen-(\d+)\.json", key)
        if m:
            out[int(m.group(1))] = json.loads(mc(f"mc cat {BUCKET_ROOT}/shards/{shard}/manifests/{key}"))
    return out


def latest(shard):
    return mc(f"mc cat {BUCKET_ROOT}/shards/{shard}/LATEST").strip()


def phase_generations():
    for p, _ in PROJECTS:
        ms = manifests(p)
        gens = sorted(ms)
        chain_ok = all(ms[g].get("generation") == g for g in gens)
        problems = []
        for i, g in enumerate(gens):
            parent = ms[g].get("parentGeneration")
            want = gens[i - 1] if i else None
            if parent != want:
                problems.append(f"gen {g} parent {parent}, expected {want}")
        lt = latest(p)
        check(f"generations {p}: one parentGeneration chain, no gap or duplicate, LATEST is the last",
              chain_ok and not problems and gens and json.loads(lt).get("generation") == gens[-1]
              and gens == list(range(1, len(gens) + 1)),
              f"generations {gens}, LATEST {lt!r}" + (f", {problems}" if problems else ""))


def workload_images():
    out = {}
    for kind, n in (("statefulset", "readers"), ("statefulset", "writers"), ("deployment", "router")):
        o = get_or_none(kind, f"{NAME}-{n}")
        out[n] = next(c["image"] for c in o["spec"]["template"]["spec"]["containers"] if c["name"] == "pharos")
    return out


def preflight_case(version, reason, job_reason):
    images = workload_images()
    patch_cluster({"spec": {"version": version}})
    log(f"spec.version -> {version}")

    def blocked():
        c = cluster()
        cond = condition(c, "UpgradeBlocked")
        return c if cond.get("status") == "True" and cond.get("reason") == reason else None
    try:
        c = wait_for(f"UpgradeBlocked {reason}", blocked, 420, 5)
    except Abort:
        log("UpgradeBlocked at timeout: " + json.dumps(condition(cluster(), "UpgradeBlocked")))
        log(kubectl("get", "jobs", "-l", "app.kubernetes.io/component=preflight", "-o", "wide", check_rc=False))
        raise
    jobs = kjson("get", "jobs", "-l", "app.kubernetes.io/component=preflight")["items"]
    failed = [j for j in jobs if any(x["type"] == "Failed" and x["status"] == "True"
                                     for x in (j.get("status") or {}).get("conditions") or [])]
    reasons = [next(x.get("reason") for x in j["status"]["conditions"] if x["type"] == "Failed") for j in failed]
    check(f"preflight {version}: UpgradeBlocked={reason}", True, condition(c, "UpgradeBlocked").get("message", "")[:200])
    check(f"preflight {version}: the Job failed with reason {job_reason}", job_reason in reasons,
          f"jobs {[j['metadata']['name'] for j in jobs]} failed reasons {reasons}")
    check(f"preflight {version}: no workload changed", workload_images() == images, f"{workload_images()}")


def phase_preflight():
    shard = PROJECTS[0][0]
    lt = latest(shard)
    gen = json.loads(lt)["generation"]
    key = f"{BUCKET_ROOT}/shards/{shard}/manifests/gen-{gen}.json"
    original = mc(f"mc cat {key}")
    ST.original_manifest = (key, original)
    try:
        m = json.loads(original)
        m.setdefault("indexFormat", {})["format"] = 999
        mc(f"mc pipe {key} >/dev/null", stdin=json.dumps(m))
        log(f"{key}: index format set to 999")
        preflight_case(V_REFUSED, "PreflightRefused", "PodFailurePolicy")
        mc(f"mc pipe {key} >/dev/null", stdin="{ this is not a manifest")
        log(f"{key}: made unreadable")
        preflight_case(V_FAILED, "PreflightFailed", "BackoffLimitExceeded")
    finally:
        mc(f"mc pipe {key} >/dev/null", stdin=original)
        check("preflight: the tampered manifest is restored byte for byte", mc(f"mc cat {key}") == original)


def phase_upgrade():
    before = workload_images()
    # what the cluster answers now (every commit published), to compare with after the roll
    answers = {(q, project): search(router_base(), q, project) for q, project in QUERIES}
    patch_cluster({"spec": {"version": V_UPGRADE}})
    log(f"spec.version -> {V_UPGRADE}")
    order = []
    seen = set()

    def done():
        imgs = workload_images()
        for n, img in imgs.items():
            if img.endswith(":" + V_UPGRADE) and n not in seen:
                seen.add(n)
                order.append(n)
                log(f"upgrade: {n} moved to {V_UPGRADE}")
        c = cluster()
        cond = condition(c, "UpgradeBlocked")
        return c if len(seen) == 3 and cond.get("reason") == "UpToDate" and c["status"].get("phase") == "Ready" \
            else None
    try:
        wait_for("the upgrade to finish (UpToDate, Ready)", done, 1500, 3)
    except Abort:
        log("upgrade at timeout: " + json.dumps((cluster().get("status") or {}).get("upgrade")) + " "
            + json.dumps(condition(cluster(), "UpgradeBlocked")))
        raise
    check("upgrade: rolled readers, then writers, then the router", order == ["readers", "writers", "router"],
          f"order {order}, from {sorted(set(before.values()))}")
    for comp in ("readers", "writers"):
        st = sts(comp).get("status") or {}
        check(f"upgrade: sts {comp} currentRevision == updateRevision after the roll",
              st.get("currentRevision") == st.get("updateRevision") and st.get("currentRevision"),
              f"{st.get('currentRevision')}")
    jobs = kjson("get", "jobs", "-l", "app.kubernetes.io/component=preflight")["items"]
    check("upgrade: every pre-flight Job deleted once up to date",
          all(j["metadata"].get("deletionTimestamp") for j in jobs) or not jobs,
          f"{[j['metadata']['name'] for j in jobs]}")
    ST.base = None
    differ = 0
    for (q, project), want in answers.items():
        got = search(router_base(), q, project)
        if got != want:
            differ += 1
            log(f"MISMATCH after upgrade [{project}] {q!r}\n   want: {want}\n   got:  {got}")
    check("upgrade: top-10 of every query identical before and after the upgrade", differ == 0,
          f"{differ} differ of {len(answers)}")


RESHARD_PROJECT = "alpha"
RESHARD_SPEC = "[project, ring(path, shards=2)]"


def status_project(status, project):
    return next((p for p in status.get("projects") or [] if p.get("project") == project), None)


def phase_reshard():
    """Integration step: a live reshard under the operator (P6-R15: the operator reads projects[])."""
    p = RESHARD_PROJECT
    before = status_project(cluster_status(), p)
    check(f"reshard: {p} starts unsplit and published", before and before.get("specVersion") is None
          and before.get("published") and len(before.get("shards") or []) == 1, json.dumps(before)[:300])
    writer = ready_pods("writer")[0]
    out = pod_exec(writer, f"JAVA_OPTS=-Xmx256m pharos-entrypoint reshard {p} --spec '{RESHARD_SPEC}'",
                   container="pharos", timeout=240)
    log(out.strip().splitlines()[-1] if out.strip() else "(no output)")
    check(f"reshard: requested in writer pod {writer}", f"requested: project {p}" in out, out.strip()[-300:])

    seen = {"phases": [], "unpublished": []}

    def flipped():
        c = cluster()
        cond = condition(c, "ProjectsPublished")
        if cond.get("status") != "True":
            seen["unpublished"].append(cond.get("message", ""))
        st = cluster_status()
        cur = status_project(st, p)
        if not cur:
            return None
        phase = (cur.get("reshard") or {}).get("phase")
        if phase and (not seen["phases"] or seen["phases"][-1] != phase):
            seen["phases"].append(phase)
            log(f"reshard {p}: {phase} ({json.dumps(cur.get('reshard'))[:200]})")
        done = (cur.get("specVersion") is not None and (cur.get("spec") or "").startswith("[project, ring(path, shards=2")
                and cur.get("published") and not cur.get("staged") and len(cur.get("shards") or []) == 2
                and (cur.get("reshard") is None or (cur.get("reshard") or {}).get("phase") == "flipped"))
        return cur if done else None
    try:
        after = wait_for(f"{p} flipped to {RESHARD_SPEC}", flipped, 900, 5)
    except Abort:
        log("reshard at timeout: " + json.dumps(status_project(cluster_status(), p))[:2000])
        raise
    check(f"reshard: /cluster/status lists {p} once, published, on 2 ring shards with the same docCount",
          after["published"] and after.get("docCount") == before.get("docCount")
          and all("~ring=" in sh for sh in after["shards"]),
          f"shards {after['shards']}, docCount {before.get('docCount')} -> {after.get('docCount')}, phases {seen['phases']}")

    def operator_sees():
        c = cluster()
        mine = next((x for x in (c.get("status") or {}).get("projects") or [] if x.get("name") == p), None)
        cond = condition(c, "ProjectsPublished")
        return (c, mine) if mine and mine.get("generation") is not None and mine.get("docCount") == after.get("docCount") \
            and cond.get("status") == "True" else None
    c, mine = wait_for("the operator's status of the split project", operator_sees, 300, 5)
    check(f"reshard: the operator reports {p} published with the summed docCount (projects[], P6-R15)", True,
          f"generation {mine.get('generation')}, docCount {mine.get('docCount')}")
    check("reshard: ProjectsPublished stayed True during the reshard", not seen["unpublished"],
          f"{seen['unpublished'][:3]}")

    ref = reference_node("e2e-ref2")
    differ = 0
    for q, project in QUERIES:
        r = search(ref, q, project)
        got = search(router_base(), q, project)
        if r != got:
            differ += 1
            log(f"MISMATCH after reshard [{project}] {q!r}\n   reference: {r}\n   cluster:   {got}")
    check("reshard: top-10 of every query identical to a single-node reference at the current commits",
          differ == 0, f"{differ} differ of {len(QUERIES)}")
    kubectl("delete", "pod", "e2e-ref2", "--wait=false", check_rc=False)
    kubectl("delete", "service", "e2e-ref2", "--wait=false", check_rc=False)


def load_secrets():
    for secret, keys in ((f"{NAME}-token", ["token"]), (f"{NAME}-minio", ["accessKey", "secretKey"])):
        s = get_or_none("secret", secret)
        if s is None:
            continue
        for k in keys:
            v = base64.b64decode(s["data"][k]).decode().strip()
            if len(v) >= 8:
                ST.secrets[f"{secret}/{k}"] = v


def phase_secrets():
    load_secrets()
    check("secrets: token and MinIO keys found to scan for", len(ST.secrets) == 3, f"{sorted(ST.secrets)}")
    needles = {}
    for name, v in ST.secrets.items():
        needles[name] = [v, base64.b64encode(v.encode()).decode()]
    kinds = ("pharoscluster,configmap,service,statefulset,deployment,job,pod,pvc,networkpolicy,"
             "poddisruptionbudget,event,replicaset,controllerrevision,serviceaccount,role,rolebinding")
    yaml_text = kubectl("get", kinds, "-o", "yaml", timeout=120)
    events = kubectl("get", "events.events.k8s.io", "-o", "yaml", check_rc=False)
    logs = save_operator_logs("final")
    for f in OUT.glob("operator-*.log"):
        logs += f.read_text()
    for where, text in (("kubectl get -o yaml (every non-Secret object)", yaml_text + events),
                        ("operator logs", logs)):
        hits = [n for n, vs in needles.items() if any(v in text for v in vs)]
        check(f"secrets: none in {where}", not hits, f"found {hits}" if hits else f"{len(text)} bytes scanned")
    ST.secrets.clear()


def phase_reapply():
    answers = {(q, project): search(router_base(), q, project) for q, project in QUERIES}
    gens = {p["name"]: p.get("generation") for p in (cluster().get("status") or {}).get("projects") or []}
    kept = {kind: get_or_none(kind, f"{NAME}-minio")["metadata"]["uid"] for kind in ("pvc", "secret")}
    kubectl("delete", "pharoscluster", NAME, "--wait=true", "--timeout=180s", timeout=200)
    log("PharosCluster deleted")
    wait_for("the owned workloads garbage-collected",
             lambda: get_or_none("statefulset", f"{NAME}-minio") is None
             and get_or_none("statefulset", f"{NAME}-writers") is None
             and get_or_none("secret", f"{NAME}-token") is None, 240, 3)
    after = {kind: (get_or_none(kind, f"{NAME}-minio") or {}).get("metadata", {}).get("uid") for kind in kept}
    check("reapply: the MinIO PVC and credentials Secret survive kubectl delete of the PharosCluster",
          after == kept, f"{after}")
    resource = cluster_resource()
    resource["spec"]["version"] = V_UPGRADE if "upgrade" in os.environ.get("E2E_PHASES", "upgrade") else VERSION
    apply(resource)
    log(f"PharosCluster applied again ({resource['spec']['version']})")

    def ready():
        c = cluster()
        return c if (c.get("status") or {}).get("phase") == "Ready" and projects_published(c) else None
    c = wait_for("phase Ready again", ready, 900, 10)
    now = {p["name"]: p.get("generation") for p in c["status"]["projects"]}
    check("reapply: Ready with the existing generations (the bucket was adopted, nothing re-indexed from scratch)",
          all(now.get(n) is not None and gens.get(n) is not None and now[n] >= gens[n] for n, _ in PROJECTS),
          f"before {gens}, after {now}")
    check("reapply: the adopted PVC and Secret are the same objects, still without an owner reference",
          all((get_or_none(k, f"{NAME}-minio") or {}).get("metadata", {}).get("uid") == kept[k]
              and not (get_or_none(k, f"{NAME}-minio") or {}).get("metadata", {}).get("ownerReferences")
              for k in kept))
    ST.base = None
    wait_for("the router answering", lambda: search(router_base(), *QUERIES[0]), 300, 5)
    differ = sum(1 for (q, project), want in answers.items() if search(router_base(), q, project) != want)
    check("reapply: top-10 of every query identical to before the delete", differ == 0,
          f"{differ} differ of {len(answers)}")


def phase_embedcache():
    """A writer that loses its pod and everything on its work volume restores its writer state (file
    state and embed cache) from the bucket: the next run is incremental, so only the new file is parsed
    and embedded. pharos logs a FULL run at WARN (the pods' stderr shows WARN and above), with its reason."""
    project = next(n for n, e in PROJECTS if e)
    writer = f"{NAME}-writers-0"
    gen = shard_generation(cluster_status(), project)["generation"]
    # empty the work volume (index, file state, embed cache, model, checkouts), then kill the pod; deleting
    # the claim instead races minikube's hostpath provisioner, which can re-create the dir root-owned 0755
    pod_exec(writer, "rm -rf /data/pharos /data/work", container="pharos")
    kubectl("delete", "pod", writer, "--force", "--grace-period=0")
    log(f"{writer}'s work volume emptied and the pod deleted")
    wait_for(f"{writer} ready again", lambda: writer in ready_pods("writer"), 1200, 5)
    started = time.time()
    commit(project, "embed cache")
    wait_published(project, gen + 1, timeout=900)
    wait_marker_searchable(project, ST.commits[project], timeout=600)
    log(f"{project}: generation {gen} -> {gen + 1} in {time.time() - started:.0f}s after the commit")
    text = kubectl("logs", writer, "-c", "pharos", "--tail=-1", check_rc=False, timeout=60)
    full = [l for l in text.splitlines() if "running a FULL index" in l]
    check("embedcache: after losing its pod and volume, the writer restores its state and indexes incrementally"
          " (no FULL run, so nothing unchanged is re-embedded)", not full, "; ".join(full)[:300] or "no FULL run")
    for pipeline in ("vector", "hybrid"):
        ids = search(router_base(), "quokka marker", project, timeout=60, pipeline=pipeline)
        check(f"embedcache: {pipeline} search finds the new file after the restart",
              any("change_" in (i or "") for i in ids), f"{ids[:3]}")


def phase_teardown():
    if ST.timeline:
        ST.timeline.stop()
        OUT.mkdir(parents=True, exist_ok=True)
        (OUT / "timeline.json").write_text(json.dumps(ST.timeline.rows))
    kubectl("delete", "pharoscluster", NAME, "--wait=true", "--timeout=180s", check_rc=False, timeout=200)
    kubectl("delete", "pod,service,configmap", "-l", "app.kubernetes.io/part-of=pharos-e2e", "--wait=false",
            check_rc=False)
    kubectl("delete", "pod", "e2e-git", "e2e-mc", "e2e-probe", "e2e-ref", "e2e-ref2", "--wait=false", "--ignore-not-found",
            check_rc=False)
    kubectl("delete", "service", "e2e-git", "e2e-ref", "e2e-ref2", "--ignore-not-found", check_rc=False)
    kubectl("delete", "configmap", "e2e-fixture-files", "e2e-fixture-scripts", "e2e-ref-config", "--ignore-not-found",
            check_rc=False)

    def gone():
        left = kubectl("get", "statefulset,deployment,service,configmap,job,secret,networkpolicy,"
                              "poddisruptionbudget", "-l", f"pharos.dev/cluster={NAME}", "-o", "name", check_rc=False)
        return True if not left.strip() else None
    # the volume claims and the MinIO credentials outlive the cluster by design (the operator never
    # deletes data): the documented full teardown deletes them by label
    kubectl("delete", "pvc,secret", "-l", f"pharos.dev/cluster={NAME}", "--wait=false", check_rc=False)
    try:
        wait_for("the cluster's objects garbage-collected", gone, 240, 5)
        check("teardown: every object of the PharosCluster is garbage-collected", True)
    except Abort as e:
        check("teardown: every object of the PharosCluster is garbage-collected", False, str(e))


PHASE_FUNCS = {n: globals()["phase_" + n] for n in PHASES_ALL + ["embedcache"]}


def kubeconfig_problem():
    """Why this run must not start, or None: it needs its own kubeconfig (operator-e2e.sh exports
    $E2E_OUT/kubeconfig) whose current context is the e2e profile, never the user's ~/.kube/config."""
    kc = os.environ.get("KUBECONFIG", "")
    if not kc or os.pathsep in kc:
        return "KUBECONFIG must name the run's own kubeconfig (operator-e2e.sh exports $E2E_OUT/kubeconfig)"
    if Path(kc).expanduser().resolve() == (Path.home() / ".kube" / "config").resolve():
        return "KUBECONFIG points at ~/.kube/config; the e2e never uses the user's kubeconfig"
    p = subprocess.run(["kubectl", "config", "current-context"], capture_output=True, text=True)
    if p.returncode != 0 or p.stdout.strip() != PROFILE:
        return f"the current context of {kc} is {p.stdout.strip() or 'unset'!r}, not {PROFILE!r}"
    return None


def main():
    problem = kubeconfig_problem()
    if problem:
        print(f"operator_e2e: refusing to start: {problem}", file=sys.stderr)
        return 2
    phases = os.environ.get("E2E_PHASES", ",".join(PHASES_EMBEDDINGS if EMBEDDINGS else PHASES_ALL)).split(",")
    unknown = [p for p in phases if p not in PHASE_FUNCS]
    if unknown:
        print(f"unknown phases {unknown}; known: {PHASES_ALL}", file=sys.stderr)
        return 2
    OUT.mkdir(parents=True, exist_ok=True)
    aborted = None
    for p in phases:
        log(f"=== phase {p}")
        try:
            PHASE_FUNCS[p]()
        except Abort as e:
            aborted = f"{p}: {e}"
            check(f"{p}: completed", False, str(e))
            break
        except Exception as e:
            aborted = f"{p}: {type(e).__name__}: {e}"
            check(f"{p}: completed", False, f"{type(e).__name__}: {str(e)[:300]}")
            break
    if aborted and "teardown" in phases and os.environ.get("E2E_KEEP_ON_FAIL", "1") != "1":
        phase_teardown()
    failed = [c for c in CHECKS if not c[1]]
    print()
    print(f"checks: {len(CHECKS) - len(failed)} passed, {len(failed)} failed; {time.time() - T0:.0f}s")
    for n, _, d in failed:
        print(f"  FAILED {n}: {d}")
    if failed:
        print(f"E2E RESULT: FAIL ({len(failed)} of {len(CHECKS)} checks failed" + (f"; aborted in {aborted}" if aborted else "") + ")")
        return 1
    print(f"E2E RESULT: PASS ({len(CHECKS)} checks, phases {','.join(phases)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
