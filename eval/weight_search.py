#!/usr/bin/env python3
"""
Grid search over Borda fusion weights using cached keyword + vector results.

No server calls — reads eval/benchmark_cache.json, simulates Borda locally,
measures MRR@10 for each weight combination, and reports the optimal config
per category and per intent.

Usage:
  python3 eval/weight_search.py
"""
import json, os, re
from collections import defaultdict

CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "benchmark_cache.json")
AGREEMENT_BONUS = 1.5

# ── Import queries and helpers from benchmark.py ─────────────────────────────
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
exec(open(os.path.join(os.path.dirname(__file__), "benchmark.py")).read().split("def _query_fingerprint")[0])

# ── Load cache ────────────────────────────────────────────────────────────────
cache = json.load(open(CACHE_FILE))

def cached(idx, pipeline):
    entry = cache.get(f"{idx}:{pipeline}", {})
    return entry.get("results", [])

# ── Intent classification (mirrors FstQueryClassifier logic) ─────────────────
STOP_WORDS = {"how","where","what","when","why","which","who","the","a","an","is",
              "are","was","were","be","all","for","to","of","in","with","that",
              "this","by","and","or","not","find","get","show","give","fetch"}

FST_INTENTS = [
    ("find code that",   "BEHAVIORAL"), ("find the code that", "BEHAVIORAL"),
    ("how to",           "BEHAVIORAL"), ("how do i",           "BEHAVIORAL"),
    ("what does",        "BEHAVIORAL"), ("interface for",      "INTERFACE"),
    ("interface to",     "INTERFACE"),  ("interface that",     "INTERFACE"),
    ("abstract class for","INTERFACE"), ("method that",        "JAVADOC"),
    ("returns the",      "JAVADOC"),    ("returns a",          "JAVADOC"),
    ("returns true",     "JAVADOC"),    ("configure",          "CONFIG"),
    ("set maximum",      "CONFIG"),     ("set the",            "CONFIG"),
    ("tune",             "CONFIG"),     ("enable",             "CONFIG"),
    ("handle corrupt",   "LIFECYCLE"),  ("handle failed",      "LIFECYCLE"),
    ("recover from",     "LIFECYCLE"),  ("graceful shutdown",  "LIFECYCLE"),
    ("close all",        "LIFECYCLE"),  ("replica recovery",   "LIFECYCLE"),
    ("node crash",       "LIFECYCLE"),  ("daemon startup",     "LIFECYCLE"),
    ("lock factory",     "LIFECYCLE"),
]

def classify(query):
    q = query.strip().lower()
    for phrase, intent in FST_INTENTS:
        if q.startswith(phrase) or f" {phrase}" in q:
            return intent
    if " " not in query: return "KEYWORD"
    if "#" in query: return "KEYWORD"
    for t in query.split():
        if re.search(r'[a-z][A-Z]', t) or re.match(r'^[A-Z][a-z]+[A-Z]', t) or re.match(r'^[A-Z0-9]{3,}', t):
            return "KEYWORD"
    tokens = q.split()
    has_stop = any(t in STOP_WORDS for t in tokens)
    if not has_stop and len(tokens) >= 4: return "KEYWORD_TECHNICAL"
    if has_stop and len(tokens) >= 4: return "HYBRID"
    if all(any(c.isupper() for c in t) for t in query.split()): return "KEYWORD"
    return "HYBRID"

# ── Local Borda simulation ────────────────────────────────────────────────────
def local_borda(kw_results, vec_results, kw_w, vec_w, limit=10):
    scores = {}
    by_id  = {}
    in_kw, in_vec = set(), set()
    n_kw, n_vec = len(kw_results), len(vec_results)
    for i, r in enumerate(kw_results):
        rid = r.get("id","")
        scores[rid] = scores.get(rid, 0.0) + kw_w * (n_kw - i)
        by_id.setdefault(rid, r)
        in_kw.add(rid)
    for i, r in enumerate(vec_results):
        rid = r.get("id","")
        scores[rid] = scores.get(rid, 0.0) + vec_w * (n_vec - i)
        by_id.setdefault(rid, r)
        in_vec.add(rid)
    for rid in list(scores):
        if rid in in_kw and rid in in_vec:
            scores[rid] *= AGREEMENT_BONUS
    return [by_id[rid] for rid, _ in sorted(scores.items(), key=lambda x: -x[1])[:limit]]

def hit_rank_local(results, gt_id):
    for i, r in enumerate(results):
        rid = r.get("id","")
        if rid.startswith(gt_id) or gt_id.startswith(rid): return i+1
        if ":" in gt_id and ":" in rid:
            if gt_id.split(":")[1].split("#")[0] == rid.split(":")[1].split("#")[0]: return i+1
    return None

def mrr(ranks): return sum(1/r for r in ranks if r) / len(ranks) if ranks else 0.0

# ── Grid search ───────────────────────────────────────────────────────────────
KW_GRID = [round(w, 2) for w in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]]

# Current baseline: CamelCase → 0.8, else → 0.5
def current_kw(query): return 0.8 if any(
    t.matches if hasattr(t,'matches') else
    bool(re.match(r'[A-Z][a-zA-Z0-9]*[a-z][a-zA-Z0-9]*', t) or re.match(r'[A-Z]{3,}', t))
    for t in query.split()) else 0.5

def is_camel(query):
    for t in query.split():
        if re.match(r'[A-Z][a-zA-Z0-9]*[a-z][a-zA-Z0-9]*', t): return True
        if re.match(r'[A-Z]{3,}', t): return True
    return False

print("Loading results from cache...")

# Gather MRR per weight per query
query_results = []  # (idx, query, project, cat, gt_id, conf, intent, kw_ranks, vec_ranks)
for idx, query, project, cat, gt_id, conf in QUERIES:
    kw_res  = cached(idx, "keyword")
    vec_res = cached(idx, "vector")
    intent  = classify(query)
    query_results.append((idx, query, project, cat, gt_id, conf, intent, kw_res, vec_res))

# ── 1. Best overall weight ────────────────────────────────────────────────────
print("\n## Overall MRR by kw_weight (uniform across all queries)\n")
print(f"{'kw_w':>6} {'vec_w':>6} {'MRR@10':>8}")
print("-" * 25)
best_overall_mrr, best_overall_w = 0, 0.5
for kw_w in KW_GRID:
    vec_w = round(1 - kw_w, 2)
    ranks = [hit_rank_local(local_borda(kwr, vr, kw_w, vec_w), gt)
             for _, _, _, _, gt, _, _, kwr, vr in query_results]
    m = mrr(ranks)
    marker = " ← current baseline" if kw_w in (0.8, 0.5) else ""
    if m > best_overall_mrr: best_overall_mrr, best_overall_w = m, kw_w
    print(f"{kw_w:>6.1f} {vec_w:>6.1f} {m:>8.3f}{marker}")
print(f"\nBest overall: kw={best_overall_w:.1f}/vec={1-best_overall_w:.1f}  MRR={best_overall_mrr:.3f}")

# ── 2. Best weight per category ───────────────────────────────────────────────
print("\n## Best kw_weight per category\n")
print(f"{'Cat':<4} {'Label':<28} {'current_MRR':>11} {'best_MRR':>9} {'best_kw':>8} {'gain':>6}")
print("-" * 75)
for cat in sorted(CAT_LABELS):
    subset = [(kwr, vr, gt) for _, _, _, c, gt, _, _, kwr, vr in query_results if c == cat]
    # current (mixed 0.8 for camel, 0.5 for NL)
    curr_ranks = [hit_rank_local(local_borda(kwr, vr,
                  0.8 if is_camel(q) else 0.5, 0.2 if is_camel(q) else 0.5), gt)
                  for (_, q, _, c, gt, _, _, kwr, vr) in query_results if c == cat]
    curr_m = mrr(curr_ranks)
    best_m, best_w = 0, 0.5
    for kw_w in KW_GRID:
        vec_w = round(1 - kw_w, 2)
        ranks = [hit_rank_local(local_borda(kwr, vr, kw_w, vec_w), gt) for kwr, vr, gt in subset]
        m = mrr(ranks)
        if m > best_m: best_m, best_w = m, kw_w
    gain = best_m - curr_m
    marker = f"  +{gain:.3f}" if gain > 0.005 else ""
    print(f"{cat:<4} {CAT_LABELS[cat]:<28} {curr_m:>11.3f} {best_m:>9.3f} {best_w:>8.1f}{marker}")

# ── 3. Best weight per intent ─────────────────────────────────────────────────
print("\n## Best kw_weight per intent\n")
print(f"{'Intent':<20} {'n':>4} {'current_MRR':>11} {'best_MRR':>9} {'best_kw':>8} {'gain':>6}")
print("-" * 65)
intents = sorted(set(intent for _, _, _, _, _, _, intent, _, _ in query_results))
for intent in intents:
    subset_q = [(q, kwr, vr, gt) for _, q, _, _, gt, _, i, kwr, vr in query_results if i == intent]
    if not subset_q: continue
    curr_ranks = [hit_rank_local(local_borda(kwr, vr,
                  0.8 if is_camel(q) else 0.5, 0.2 if is_camel(q) else 0.5), gt)
                  for q, kwr, vr, gt in subset_q]
    curr_m = mrr(curr_ranks)
    best_m, best_w = 0, 0.5
    for kw_w in KW_GRID:
        vec_w = round(1 - kw_w, 2)
        ranks = [hit_rank_local(local_borda(kwr, vr, kw_w, vec_w), gt) for _, kwr, vr, gt in subset_q]
        m = mrr(ranks)
        if m > best_m: best_m, best_w = m, kw_w
    gain = best_m - curr_m
    marker = f"  +{gain:.3f}" if gain > 0.005 else ""
    print(f"{intent:<20} {len(subset_q):>4} {curr_m:>11.3f} {best_m:>9.3f} {best_w:>8.1f}{marker}")

# ── 4. Show full grid per intent (for tuning) ─────────────────────────────────
print("\n## MRR grid per intent (rows=intent, cols=kw_weight)\n")
header = f"{'Intent':<20} " + " ".join(f"{w:.1f}" for w in KW_GRID)
print(header)
print("-" * (21 + 6 * len(KW_GRID)))
for intent in intents:
    subset_q = [(kwr, vr, gt) for _, _, _, _, gt, _, i, kwr, vr in query_results if i == intent]
    if not subset_q: continue
    row = f"{intent:<20}"
    best_m = 0
    for kw_w in KW_GRID:
        vec_w = round(1 - kw_w, 2)
        ranks = [hit_rank_local(local_borda(kwr, vr, kw_w, vec_w), gt) for kwr, vr, gt in subset_q]
        m = mrr(ranks)
        best_m = max(best_m, m)
        marker = "*" if m == best_m else " "
        row += f" {m:.3f}"
    print(row)
