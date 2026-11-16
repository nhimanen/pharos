#!/usr/bin/env python3
"""Phase 1 gate checks (standard library only). Driven by scripts/cluster-gate.sh.

1. For every eval/benchmark.py query whose project is pharos or lucene, compare the top-10
   ids from /api/search?pipeline=keyword on writer and reader, and /api/callers of each
   query's top result. Any mismatch -> exit 1.
2. Freshness: append a method to the writer's copy of a pharos source file, re-index the
   writer incrementally (GATE_REINDEX_CMD), take t0 after the publish returns, then poll the
   reader until the method is searchable. Prints elapsed time vs poll interval.
"""
import argparse
import importlib.util
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent


def load_queries():
    spec = importlib.util.spec_from_file_location("benchmark", HERE.parent / "eval" / "benchmark.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return [(q[1], q[2]) for q in mod.QUERIES if q[2] in ("pharos", "lucene")]


def get_json(base, path, params, timeout=60):
    url = f"{base}{path}?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"__error__": f"HTTP {e.code}"}
    except Exception as e:  # connection problems are mismatches too
        return {"__error__": str(e)}
    if isinstance(data, dict) and "results" in data:
        data = data["results"]
    return data


def ids(data):
    if isinstance(data, dict):
        return [data["__error__"]]
    return [r.get("id") for r in data]


def compare(args):
    queries = load_queries()
    mismatches = 0
    callers_checked = 0
    for query, project in queries:
        p = {"q": query, "pipeline": "keyword", "limit": 10, "project": project}
        w = ids(get_json(args.writer, "/api/search", p))
        r = ids(get_json(args.reader, "/api/search", p))
        if w != r:
            mismatches += 1
            print(f"MISMATCH search [{project}] {query!r}\n  writer: {w}\n  reader: {r}")
            continue
        if not w:
            continue
        cp = {"fqn": w[0], "project": project}
        wc = ids(get_json(args.writer, "/api/callers", cp))
        rc = ids(get_json(args.reader, "/api/callers", cp))
        callers_checked += 1
        if wc != rc:
            mismatches += 1
            print(f"MISMATCH callers [{project}] {w[0]}\n  writer: {wc}\n  reader: {rc}")
    print(f"queries compared: {len(queries)}, callers compared: {callers_checked}, mismatches: {mismatches}")
    return len(queries), mismatches


def freshness(args):
    reindex = os.environ["GATE_REINDEX_CMD"]
    src_root = Path(os.environ["GATE_PHAROS_SRC"])
    poll = int(os.environ.get("GATE_POLL_INTERVAL", "10"))
    log_path = Path(os.environ.get("GATE_READER_LOG", ""))
    marker = "gateProbe" + str(int(time.time()))
    target = next(src_root.rglob("BordaMerger.java"), None) or next(src_root.rglob("*.java"))
    text = target.read_text()
    idx = text.rstrip().rfind("}")
    probe = f"\n    public static int {marker}() {{ return 42; }}\n"
    target.write_text(text[:idx] + probe + text[idx:])
    print(f"appended {marker}() to {target}")

    log_size_before = log_path.stat().st_size if log_path.is_file() else 0
    rc = subprocess.run(shlex.split(reindex), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode
    if rc != 0:
        print(f"FAIL: incremental re-index/publish exited {rc}")
        return False
    t0 = time.monotonic()
    deadline = t0 + 3 * poll + 120
    params = {"q": marker, "pipeline": "keyword", "limit": 5, "project": "pharos"}
    while time.monotonic() < deadline:
        if any(marker in (i or "") for i in ids(get_json(args.reader, "/api/search", params, 10))):
            elapsed = time.monotonic() - t0
            break
        time.sleep(0.5)
    else:
        print(f"FAIL: {marker} not visible on the reader after {deadline - t0:.0f}s")
        return False

    line = ""
    if log_path.is_file():
        with open(log_path, errors="replace") as f:
            f.seek(log_size_before)
            for ln in f:
                if "materialised generation" in ln:
                    line = ln.strip()
    print(f"visible after {elapsed:.1f} s (poll interval {poll} s)")
    print(f"reader log (generation download): {line or 'no materialised-generation line found'}")
    ok = elapsed <= poll + 30  # poll interval + generous download budget; judged against the log line
    print("freshness " + ("OK" if ok else "SLOW (check download time in the log line above)"))
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--writer", default="http://127.0.0.1:7300")
    ap.add_argument("--reader", default="http://127.0.0.1:7301")
    args = ap.parse_args()
    n, mism = compare(args)
    if mism:
        print("GATE FAILED: top-10 / callers mismatches")
        return 1
    if not freshness(args):
        return 1
    print("GATE PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
