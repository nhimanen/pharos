#!/usr/bin/env python3
"""
Synonym source ablation test.

For each configuration, strips auto-mined rules, re-mines with the selected
sources, restarts the daemon, and measures MRR@10 on the 200-query benchmark.

Source bitmasks (matching ConceptMiner.SRC_* constants):
  1 = class name tokens + bigrams + trigrams
  2 = javadoc prose bigrams (top-PPMI)
  4 = acronym / initialism expansion
  8 = per-class method wormhole

Usage:
  python3 eval/synonym_ablation.py
"""

import json, os, shutil, subprocess, sys, time, urllib.parse, urllib.request
from pathlib import Path

REPO     = Path(__file__).parent.parent
SYN_FILE = Path.home() / ".pharos" / "synonyms.txt"
JAR      = sorted((REPO / "target").glob("pharos-*.jar"), reverse=True)
JAR      = JAR[0] if JAR else None
BASE_URL = "http://localhost:7171"
PROJECTS = ["lucene", "solr", "vespa", "pharos"]

PIPELINES = ["keyword", "auto", "hybrid"]

CONFIGS = [
    ("no_synonyms",       None),            # manual rules only
    ("src1_class_names",  0b0001),
    ("src2_javadoc",      0b0010),
    ("src3_acronyms",     0b0100),
    ("src4_wormhole",     0b1000),
    ("src1+2",            0b0011),
    ("src1+2+3",          0b0111),
    ("all_sources",       0b1111),
]


# ── Synonym file helpers ──────────────────────────────────────────────────────

def load_manual_header(syn_file: Path) -> str:
    """Return only the hand-written (non-auto-mined) portion of synonyms.txt."""
    lines, keep = syn_file.read_text().splitlines(keepends=True), []
    for line in lines:
        if "Auto-mined from" in line:
            break
        keep.append(line)
    return "".join(keep)


def apply_sources(mask: int | None, manual_header: str):
    """Write manual header + fresh auto-mined rules for the given source mask."""
    SYN_FILE.write_text(manual_header)
    if mask is None:
        return
    if JAR is None:
        print("ERROR: JAR not found in target/. Run ./setup.sh.", file=sys.stderr)
        sys.exit(1)
    jvm = [
        "java",
        "--enable-native-access=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio.channels.spi=ALL-UNNAMED",
        "--add-modules", "jdk.incubator.vector",
        "-Dpolyglot.engine.WarnInterpreterOnly=false",
        "-jar", str(JAR),
    ]
    for proj in PROJECTS:
        r = subprocess.run(
            jvm + ["mine", proj, f"--sources={mask}", "--write"],
            capture_output=True, text=True, cwd=REPO
        )
        if r.returncode != 0:
            print(f"  [WARN] mine {proj} failed: {r.stderr[:150]}", file=sys.stderr)
        else:
            # Extract rule count from output
            for line in r.stdout.splitlines():
                if "synonym rules" in line.lower():
                    print(f"    {proj}: {line.strip()}")


def restart_daemon():
    subprocess.run(["pharos", "daemon", "restart"], capture_output=True)
    for _ in range(25):
        try:
            with urllib.request.urlopen(f"{BASE_URL}/health", timeout=1): return
        except: time.sleep(1)
    print("  [WARN] daemon did not come up after 25s")


# ── Benchmark helpers ─────────────────────────────────────────────────────────

def api_search(query, project, pipeline, limit=10):
    for proj in [project, None]:
        params = {"q": query, "pipeline": pipeline, "limit": limit}
        if proj: params["project"] = proj
        url = f"{BASE_URL}/api/search?" + urllib.parse.urlencode(params)
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                data = json.loads(r.read())
                if isinstance(data, dict): data = data.get("results", [])
                if proj is None and project:
                    data = [x for x in data if x.get("project") == project]
                return data
        except:
            if proj is not None: continue
            return []
    return []


def hit_rank(results, gt_id):
    for i, r in enumerate(results):
        rid = r.get("id", "")
        if rid.startswith(gt_id) or gt_id.startswith(rid): return i+1
        if ":" in gt_id and ":" in rid:
            if gt_id.split(":")[1].split("#")[0] == rid.split(":")[1].split("#")[0]: return i+1
    return None


def mrr(ranks): return sum(1/r for r in ranks if r) / len(ranks) if ranks else 0.0


def run_benchmark(QUERIES):
    ranks = {pl: [] for pl in PIPELINES}
    for idx, query, project, cat, gt_id, conf in QUERIES:
        for pl in PIPELINES:
            ranks[pl].append(hit_rank(api_search(query, project, pl), gt_id))
    return {pl: mrr(ranks[pl]) for pl in PIPELINES}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Load query set from benchmark.py
    bench_code = (Path(__file__).parent / "benchmark.py").read_text()
    ns = {}
    exec(bench_code.split("def _query_fingerprint")[0], ns)
    QUERIES = ns["QUERIES"]

    manual_header = load_manual_header(SYN_FILE)
    backup = SYN_FILE.with_suffix(".bak_ablation")
    shutil.copy(SYN_FILE, backup)
    print(f"Backed up synonyms.txt → {backup.name}\n")

    results = {}
    try:
        for i, (name, mask) in enumerate(CONFIGS, 1):
            print(f"[{i}/{len(CONFIGS)}] {name}  (mask={'None' if mask is None else bin(mask)})")
            apply_sources(mask, manual_header)
            print(f"  → restarting daemon…")
            restart_daemon()
            print(f"  → running {len(QUERIES)} queries × {len(PIPELINES)} pipelines…")
            results[name] = run_benchmark(QUERIES)
            print(f"  → MRR: {results[name]}\n")
    finally:
        print("Restoring original synonyms.txt…")
        shutil.copy(backup, SYN_FILE)
        restart_daemon()
        backup.unlink(missing_ok=True)

    # ── Report ────────────────────────────────────────────────────────────────
    print("\n" + "="*70)
    print("SYNONYM SOURCE ABLATION — MRR@10\n")
    header = f"{'Configuration':<25} " + "  ".join(f"{p:>8}" for p in PIPELINES)
    print(header)
    print("-" * len(header))
    baseline_mrrs = results.get("all_sources", {})
    for name, mask in CONFIGS:
        mrrs = results.get(name, {})
        row = f"{name:<25} " + "  ".join(f"{mrrs.get(p,0):>8.3f}" for p in PIPELINES)
        # Mark improvement vs no_synonyms baseline
        vs_no = results.get("no_synonyms", {})
        gains = [mrrs.get(p,0) - vs_no.get(p,0) for p in PIPELINES]
        if any(g > 0.005 for g in gains):
            row += f"  ↑ {max(gains):+.3f}"
        print(row)

    out = Path(__file__).parent / "synonym_ablation_results.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"\nResults saved to {out}")


if __name__ == "__main__":
    main()
