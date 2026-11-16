#!/usr/bin/env python3
"""Phase 1 gate checks (standard library only). Driven by scripts/cluster-gate.sh.

1. For every eval/benchmark.py query whose project is pharos or lucene, compare the top-10
   ids from /api/search?pipeline=keyword on writer and reader, and for each query's top result
   /api/callers (Lucene calledMethods) plus the graph-backed /api/usages and /api/impact
   (which on the reader come from the compact graph; compared as sets, since reverse-edge order
   may differ). Any mismatch -> exit 1; no non-empty graph answer at all -> exit 1 (vacuous).
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
    sys.dont_write_bytecode = True  # don't leave eval/__pycache__ behind in the repo
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
        if "__error__" in data:
            raise FetchError(data["__error__"])
        raise FetchError("unexpected dict response: " + json.dumps(data)[:200])
    if not isinstance(data, list):
        raise FetchError("unexpected response: " + repr(data)[:200])
    return [r.get("id") for r in data]


def fqn_of(result_id, project):
    """Search ids are '<project>:<fqn>'; graph endpoints take the bare FQN."""
    prefix = project + ":"
    return result_id[len(prefix):] if result_id and result_id.startswith(prefix) else result_id


def check_dict(data):
    if isinstance(data, dict) and "__error__" in data:
        raise FetchError(data["__error__"])
    if not isinstance(data, dict):
        raise FetchError("unexpected response: " + repr(data)[:200])
    return data


def usage_sets(data):
    """/api/usages: every list-valued kind as a set (order is not part of the contract)."""
    d = check_dict(data)
    return {k: frozenset(v) for k, v in d.items() if isinstance(v, list)}


def impact_set(data):
    """/api/impact: the (fqn, depth) callers as a set, plus the totals."""
    d = check_dict(data)
    entries = set()
    for e in d.get("callers") or []:
        entries.add(json.dumps(e, sort_keys=True))
    return {"totalCallers": d.get("totalCallers"), "truncated": d.get("truncated"),
            "callers": frozenset(entries)}


def compare_graph(args, project, fqn, st):
    """Graph-backed endpoints for one FQN. Returns False on error/mismatch."""
    try:
        wu = usage_sets(get_json(args.writer, "/api/usages", {"fqn": fqn, "kind": "all"}))
        ru = usage_sets(get_json(args.reader, "/api/usages", {"fqn": fqn, "kind": "all"}))
        wi = impact_set(get_json(args.writer, "/api/impact", {"fqn": fqn, "depth": 3}))
        ri = impact_set(get_json(args.reader, "/api/impact", {"fqn": fqn, "depth": 3}))
    except FetchError as e:
        st["errors"] += 1
        print(f"ERROR graph [{project}] {fqn}: {e}")
        return False
    st["graph"] += 1
    if any(wu.values()) or wi["callers"]:
        st["graph_non_empty"] += 1
    ok = True
    if wu != ru:
        st["mismatches"] += 1
        ok = False
        diff = {k: (sorted(wu.get(k, ())), sorted(ru.get(k, ())))
                for k in set(wu) | set(ru) if wu.get(k) != ru.get(k)}
        print(f"MISMATCH usages [{project}] {fqn}: kind -> (writer, reader): {json.dumps(diff)[:2000]}")
    if wi != ri:
        st["mismatches"] += 1
        ok = False
        print(f"MISMATCH impact [{project}] {fqn}\n  writer: total={wi['totalCallers']} "
              f"{sorted(wi['callers'])[:20]}\n  reader: total={ri['totalCallers']} {sorted(ri['callers'])[:20]}")
    return ok


def compare(args):
    queries = load_queries()
    st = {"compared": 0, "non_empty": 0, "empty": 0, "errors": 0, "mismatches": 0, "callers": 0,
          "graph": 0, "graph_non_empty": 0}
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
        fqn = fqn_of(w[0], project)
        compare_graph(args, project, fqn, st)
        cp = {"fqn": fqn, "project": project}
        try:
            wc = ids(get_json(args.writer, "/api/callers", cp))
            rc = ids(get_json(args.reader, "/api/callers", cp))
        except FetchError as e:
            st["errors"] += 1
            print(f"ERROR callers [{project}] {fqn}: {e}")
            continue
        st["callers"] += 1
        if wc != rc:
            st["mismatches"] += 1
            print(f"MISMATCH callers [{project}] {fqn}\n  writer: {wc}\n  reader: {rc}")
    for e in empties:
        print(f"EMPTY top-10 on both: {e}")
    print(f"queries total: {len(queries)}, compared: {st['compared']}, non-empty: {st['non_empty']}, "
          f"empty: {st['empty']}, callers compared: {st['callers']}, graph compared: {st['graph']} "
          f"(non-empty: {st['graph_non_empty']}), errors: {st['errors']}, mismatches: {st['mismatches']}")
    search_ok = st["non_empty"] >= 0.9 * st["compared"] and st["compared"] > 0
    graph_ok = st["graph_non_empty"] > 0
    ok = (st["errors"] == 0 and st["mismatches"] == 0 and st["compared"] == len(queries)
          and search_ok and graph_ok)
    if st["errors"] == 0 and st["mismatches"] == 0:
        if not search_ok:
            print("FAIL: fewer than 90% of queries returned a non-empty top-10 (vacuous comparison)")
        if not graph_ok:
            print("FAIL: no top result had a non-empty graph answer (vacuous graph comparison)")
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
    try:
        pub = re.findall(r"Published shard (\S+) generation (\d+)", Path(reindex_log).read_text(errors="replace"))
    except OSError:
        pub = []
    pub = [x for x in pub if "pharos" in x[0]] or pub
    if not pub:
        print(f"FAIL: no 'Published shard ... generation N' line in {reindex_log}; cannot identify the probe generation")
        return False
    shard, gen = pub[-1]
    print(f"writer published shard {shard} generation {gen}")
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

    ms, lines = [], []
    if log_path.is_file():
        with open(log_path, errors="replace") as f:
            f.seek(log_size_before)
            for ln in f:
                m = re.search(r"Shard (\S+): materialised generation (\d+) .* in (\d+) ms", ln)
                if m and m.group(1) == shard and m.group(2) == gen:
                    ms.append(int(m.group(3)))
                    lines = [ln.strip()]
    if not ms:
        print(f"FAIL: no 'materialised generation {gen} ... in N ms' line for shard {shard} in the reader log; cannot judge")
        return False
    download = ms[-1] / 1000.0
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
