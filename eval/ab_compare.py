#!/usr/bin/env python3
"""
Paired comparison of two `eval/benchmark.py --json-out` result files (model A = baseline, model B = candidate).

Per pipeline: MRR@10, P@1 and Recall@10 for both models, the paired difference (B - A) and a
paired bootstrap 95% CI (percentile, 10k resamples over queries, fixed seed), overall, per category
and per project.  Then the decision rule from
docs/superpowers/specs/2026-11-16-embedding-pooling-design.md ("Decision gate (A/B)") is evaluated
mechanically:

  SWITCH only if B's MRR@10 >= A's on hybrid-reranked AND on hybrid, and the paired-bootstrap CI of
  the vector MRR@10 difference (B - A) lies entirely above 0.  Otherwise: OPT-IN ONLY.

Usage: ab_compare.py A.json B.json [--label-a jina] [--label-b granite] [--out report.md]
                     [--resamples 10000] [--seed 20261116] [--json-out decision.json]
Exit code: 0 always when the comparison ran (the decision is in the output); 2 on bad input.
"""
import argparse, json, math, random, sys

METRICS = ["MRR@10", "P@1", "Recall@10"]
GATE_PIPELINES = ["hybrid-reranked", "hybrid", "vector"]


def value(metric, rank):
    if metric == "MRR@10":
        return 1.0 / rank if rank and rank <= 10 else 0.0
    if metric == "P@1":
        return 1.0 if rank == 1 else 0.0
    if metric == "Recall@10":
        return 1.0 if rank and rank <= 10 else 0.0
    raise ValueError(metric)


def bootstrap_diff(diffs, resamples, seed):
    """Percentile 95% CI of mean(diffs) under resampling queries with replacement."""
    n = len(diffs)
    if n == 0:
        return (float("nan"), float("nan"))
    try:
        import numpy as np
        rng = np.random.default_rng(seed)
        arr = np.asarray(diffs, dtype=float)
        idx = rng.integers(0, n, size=(resamples, n))
        means = arr[idx].mean(axis=1)
        lo, hi = np.percentile(means, [2.5, 97.5])
        return (float(lo), float(hi))
    except ImportError:
        rng = random.Random(seed)
        means = sorted(sum(diffs[rng.randrange(n)] for _ in range(n)) / n for _ in range(resamples))
        return (means[int(0.025 * resamples)], means[min(resamples - 1, int(0.975 * resamples))])


def load(path):
    with open(path) as f:
        d = json.load(f)
    for k in ("queries", "ranks", "pipelines"):
        if k not in d:
            raise ValueError(f"{path}: missing key '{k}' (not a benchmark.py --json-out file)")
    return d


def compare(a, b, resamples, seed):
    qa = {q["idx"]: q for q in a["queries"]}
    qb = {q["idx"]: q for q in b["queries"]}
    if set(qa) != set(qb):
        raise ValueError(f"query sets differ: only-A={sorted(set(qa) - set(qb))[:5]} only-B={sorted(set(qb) - set(qa))[:5]}")
    pipelines = [p for p in a["pipelines"] if p in b["pipelines"]]
    idxs = sorted(qa)
    cats = sorted({q["category"] for q in qa.values()})
    projs = sorted({q["project"] for q in qa.values()})
    groups = [("overall", None)] + [("category", c) for c in cats] + [("project", p) for p in projs]

    out = {}  # (pipeline, kind, name, metric) -> dict
    for pl in pipelines:
        for kind, name in groups:
            sel = [i for i in idxs
                   if kind == "overall"
                   or (kind == "category" and qa[i]["category"] == name)
                   or (kind == "project" and qa[i]["project"] == name)]
            for m in METRICS:
                va = [value(m, a["ranks"][pl].get(str(i))) for i in sel]
                vb = [value(m, b["ranks"][pl].get(str(i))) for i in sel]
                diffs = [y - x for x, y in zip(va, vb)]
                n = len(sel)
                ma = sum(va) / n if n else float("nan")
                mb = sum(vb) / n if n else float("nan")
                # small groups: skip the (slow, meaningless) bootstrap only if n < 2
                lo, hi = bootstrap_diff(diffs, resamples, seed) if n >= 2 else (float("nan"), float("nan"))
                out[(pl, kind, name, m)] = dict(n=n, a=ma, b=mb, diff=mb - ma, lo=lo, hi=hi)
    return pipelines, groups, out


def decide(pipelines, out):
    need = [p for p in GATE_PIPELINES if p not in pipelines]
    if need:
        return "INCONCLUSIVE", [f"missing pipelines for the decision rule: {need}"], {}
    g = lambda pl: out[(pl, "overall", None, "MRR@10")]
    hr, hy, ve = g("hybrid-reranked"), g("hybrid"), g("vector")
    c1 = hr["b"] >= hr["a"]
    c2 = hy["b"] >= hy["a"]
    c3 = ve["lo"] > 0
    lines = [
        f"hybrid-reranked MRR@10: A={hr['a']:.4f} B={hr['b']:.4f} diff={hr['diff']:+.4f} -> B >= A: {c1}",
        f"hybrid          MRR@10: A={hy['a']:.4f} B={hy['b']:.4f} diff={hy['diff']:+.4f} -> B >= A: {c2}",
        f"vector          MRR@10: A={ve['a']:.4f} B={ve['b']:.4f} diff={ve['diff']:+.4f} "
        f"CI95=[{ve['lo']:+.4f}, {ve['hi']:+.4f}] -> CI excludes 0 on positive side: {c3}",
    ]
    detail = {"hybrid-reranked_ge": c1, "hybrid_ge": c2, "vector_ci_positive": c3}
    return ("SWITCH" if (c1 and c2 and c3) else "OPT-IN ONLY"), lines, detail


def fmt_row(name, r):
    sig = "" if math.isnan(r["lo"]) else (" *" if (r["lo"] > 0 or r["hi"] < 0) else "")
    return (f"| {name} | {r['n']} | {r['a']:.3f} | {r['b']:.3f} | {r['diff']:+.3f} | "
            f"[{r['lo']:+.3f}, {r['hi']:+.3f}]{sig} |")


def render(la, lb, pipelines, groups, out, decision, dlines, meta):
    L = [f"# A/B: {la} (A) vs {lb} (B)", "",
         f"Paired bootstrap: {meta['resamples']} resamples, seed {meta['seed']}, 95% percentile CI of (B - A). "
         "`*` = CI excludes 0.", ""]
    for pl in pipelines:
        L += [f"## Pipeline `{pl}`", ""]
        for m in METRICS:
            L += [f"### {m}", "", f"| group | n | {la} | {lb} | diff | 95% CI |", "|---|---|---|---|---|---|"]
            for kind, name in groups:
                label = "overall" if kind == "overall" else f"{kind}: {name}"
                L.append(fmt_row(label, out[(pl, kind, name, m)]))
            L.append("")
    L += ["## Decision gate", ""] + [f"- {x}" for x in dlines] + ["", f"**DECISION: {decision}**", ""]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("a_json"); ap.add_argument("b_json")
    ap.add_argument("--label-a", default="A"); ap.add_argument("--label-b", default="B")
    ap.add_argument("--out"); ap.add_argument("--json-out")
    ap.add_argument("--resamples", type=int, default=10000)
    ap.add_argument("--seed", type=int, default=20261116)
    args = ap.parse_args()
    try:
        a, b = load(args.a_json), load(args.b_json)
        pipelines, groups, out = compare(a, b, args.resamples, args.seed)
    except (OSError, ValueError, KeyError) as e:
        print(f"ab_compare: {e}", file=sys.stderr)
        sys.exit(2)
    for lab, d in ((args.label_a, a), (args.label_b, b)):
        bad = {pl: n for pl, n in d.get("empty_result_queries", {}).items() if n}
        if bad:
            print(f"WARNING: {lab} has queries with EMPTY results (daemon error/timeouts?): {bad}", file=sys.stderr)
    decision, dlines, detail = decide(pipelines, out)
    text = render(args.label_a, args.label_b, pipelines, groups, out, decision, dlines,
                  dict(resamples=args.resamples, seed=args.seed))
    print(text)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text)
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump({"decision": decision, "rule": detail, "numbers": dlines}, f, indent=1)


if __name__ == "__main__":
    main()
