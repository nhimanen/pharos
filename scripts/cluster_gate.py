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
import re
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


class FetchError(Exception):
    pass


def ids(data):
    if isinstance(data, dict):
        raise FetchError(data["__error__"])
    return [r.get("id") for r in data]


def compare(args):
    queries = load_queries()
    st = {"compared": 0, "non_empty": 0, "empty": 0, "errors": 0, "mismatches": 0, "callers": 0}
    empties = []
    for query, project in queries:
        p = {"q": query, "pipeline": "keyword", "limit": 10, "project": project}
        try:
            w = ids(get_json(args.writer, "/api/search", p))
            r = ids(get_json(args.reader, "/api/search", p))
        except FetchError as e:
            st["errors"] += 1
            print(f"ERROR search [{project}] {query!r}: {e}")
            continue
        st["compared"] += 1
        if w != r:
            st["mismatches"] += 1
            print(f"MISMATCH search [{project}] {query!r}\n  writer: {w}\n  reader: {r}")
            continue
        if not w:
            st["empty"] += 1
            empties.append(f"[{project}] {query!r}")
            continue
        st["non_empty"] += 1
        cp = {"fqn": w[0], "project": project}
        try:
            wc = ids(get_json(args.writer, "/api/callers", cp))
            rc = ids(get_json(args.reader, "/api/callers", cp))
        except FetchError as e:
            st["errors"] += 1
            print(f"ERROR callers [{project}] {w[0]}: {e}")
            continue
        st["callers"] += 1
        if wc != rc:
            st["mismatches"] += 1
            print(f"MISMATCH callers [{project}] {w[0]}\n  writer: {wc}\n  reader: {rc}")
    for e in empties:
        print(f"EMPTY top-10 on both: {e}")
    print(f"queries total: {len(queries)}, compared: {st['compared']}, non-empty: {st['non_empty']}, "
          f"empty: {st['empty']}, callers compared: {st['callers']}, errors: {st['errors']}, "
          f"mismatches: {st['mismatches']}")
    ok = (st["errors"] == 0 and st["mismatches"] == 0 and st["compared"] == len(queries)
          and st["non_empty"] >= 0.9 * st["compared"] and st["compared"] > 0)
    if not ok and st["errors"] == 0 and st["mismatches"] == 0:
        print("FAIL: fewer than 90% of queries returned a non-empty top-10 (vacuous comparison)")
    return ok


def freshness(args):
    reindex = os.environ["GATE_REINDEX_CMD"]
    src_root = Path(os.environ["GATE_PHAROS_SRC"])
    poll = int(os.environ.get("GATE_POLL_INTERVAL", "10"))
    log_path = Path(os.environ.get("GATE_READER_LOG", ""))
    reindex_log = os.environ.get("GATE_REINDEX_LOG", os.devnull)
    marker = "gateProbe" + str(int(time.time()))
    target = next(src_root.rglob("BordaMerger.java"), None) or next(src_root.rglob("*.java"))
    text = target.read_text()
    idx = text.rstrip().rfind("}")
    probe = f"\n    public static int {marker}() {{ return 42; }}\n"
    target.write_text(text[:idx] + probe + text[idx:])
    print(f"appended {marker}() to {target}")

    log_size_before = log_path.stat().st_size if log_path.is_file() else 0
    with open(reindex_log, "w") as lf:
        rc = subprocess.run(shlex.split(reindex), stdout=lf, stderr=subprocess.STDOUT).returncode
    if rc != 0:
        print(f"FAIL: incremental re-index/publish exited {rc} (see {reindex_log})")
        return False
    t0 = time.monotonic()
    deadline = t0 + 3 * poll + 120
    params = {"q": marker, "pipeline": "keyword", "limit": 5, "project": "pharos"}
    elapsed = None
    while time.monotonic() < deadline:
        d = get_json(args.reader, "/api/search", params, 10)
        if not isinstance(d, dict) and any(marker in (r.get("id") or "") for r in d):
            elapsed = time.monotonic() - t0
            break
        time.sleep(0.5)
    if elapsed is None:
        print(f"FAIL: {marker} not visible on the reader after {deadline - t0:.0f}s")
        return False

    lines = []
    if log_path.is_file():
        with open(log_path, errors="replace") as f:
            f.seek(log_size_before)
            lines = [ln.strip() for ln in f if "materialised generation" in ln]
    ms = [int(m.group(1)) for ln in lines for m in [re.search(r" in (\d+) ms", ln)] if m]
    if not ms:
        print("FAIL: no 'materialised generation ... in N ms' line in the reader log; cannot judge")
        return False
    download = sum(ms) / 1000.0
    bound = poll + download + 5
    ok = elapsed <= bound
    print(f"visible after {elapsed:.1f} s; poll interval {poll} s; generation download {download:.1f} s; "
          f"bound (poll + download + 5 s slack) {bound:.1f} s -> {'OK' if ok else 'TOO SLOW'}")
    for ln in lines:
        print(f"reader log: {ln}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--writer", default="http://127.0.0.1:7300")
    ap.add_argument("--reader", default="http://127.0.0.1:7301")
    args = ap.parse_args()
    if not compare(args):
        print("GATE FAILED: comparison")
        return 1
    if not freshness(args):
        print("GATE FAILED: freshness")
        return 1
    print("GATE PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
