# Embedding A/B gate: jina-embeddings-v2-base-code vs granite-embedding-english-r2

Date: 2026-11-17 (run finished 02:52). Spec: `docs/superpowers/specs/2026-11-16-embedding-pooling-design.md`.

## Verdict

- Pre-registered rule verdict: **OPT-IN ONLY**. hybrid-reranked MRR@10 is a tie (-0.009, B < A, so the
  rule's first clause fails); hybrid is +0.068; vector is +0.049 with 95% CI [-0.017, +0.116] (does not
  exclude 0).
- **User decision (overrides the rule): granite-r2 becomes the built-in default for NEW installs only.**
  Existing configs that name jina (old-style `embeddingModelUrl`, migrated to modelId `legacy`, or explicit
  `embeddingProviders` entries) keep jina; no forced re-embed.
- Reasoning: the `auto` search type mostly dispatches to `hybrid-rrf`, where granite is significantly better
  (MRR@10 +0.056, CI [+0.002, +0.112]); plain `hybrid` is also significantly better (+0.068, CI [+0.008, +0.129]).
  The only pipeline where the rule fails (hybrid-reranked) is a tie within noise.

## Finding: the cross-encoder lowers MRR for both models

jina hybrid 0.596 -> hybrid-reranked 0.578; granite hybrid 0.664 -> hybrid-reranked 0.569. The reranker is
active (checked), so this is not a wiring failure. It is why the rule's hybrid-reranked clause fails (granite
loses more from reranking than jina). Investigated separately; not part of this change.

## Run metadata

- pharos source: `/home/nhimanen/projects/pharos` @ `6400137b9ce8c5a908eb882cf0cb3954f22f3646`
- lucene source: `/home/nhimanen/projects/lucene/lucene` @ `b0900bd176f42e01500ad04660ed2c4e98fddf01`
- jar: `target/pharos-1.4.0.jar` built from worktree `df67b88c62ba9a9b19b0fa8c73e3116a4c7101d2`
- models: jina, granite; projects: pharos, lucene; pipelines: vector, hybrid, hybrid-rrf, hybrid-reranked
- 99 queries (pharos 45, lucene 54), categories A-H
- durations: jina index 0 s (served from its existing embedding cache; benchmark 45 s); granite index
  39369 s (10.9 h on this CPU; benchmark 50 s); total 39464 s
- Peak RSS is higher for granite (~2.8 GB vs ~2.3 GB) and CPU embedding is much slower.

## Validity checks

- 0 empty results on both sides.
- pharos and lucene source trees unchanged during the run.
- jina embedding cache reused (262.5 -> 275.5 MiB, i.e. not rebuilt).
- Reranker active in the hybrid-reranked pipeline.
- `~/.pharos` and `~/.djl.ai` untouched (marker check: OK).

## Results


Paired bootstrap: 10000 resamples, seed 20261116, 95% percentile CI of (B - A). `*` = CI excludes 0.

## Pipeline `vector`

### MRR@10

| group | n | jina | granite | diff | 95% CI |
|---|---|---|---|---|---|
| overall | 99 | 0.548 | 0.597 | +0.049 | [-0.017, +0.116] |
| category: A | 18 | 0.849 | 1.000 | +0.151 | [+0.037, +0.297] * |
| category: B | 12 | 0.630 | 0.669 | +0.039 | [-0.039, +0.125] |
| category: C | 11 | 0.500 | 0.461 | -0.039 | [-0.193, +0.120] |
| category: D | 20 | 0.375 | 0.492 | +0.117 | [-0.058, +0.304] |
| category: E | 10 | 0.583 | 0.767 | +0.183 | [-0.067, +0.400] |
| category: F | 10 | 0.400 | 0.333 | -0.067 | [-0.233, +0.100] |
| category: G | 8 | 0.417 | 0.391 | -0.026 | [-0.307, +0.188] |
| category: H | 10 | 0.525 | 0.408 | -0.117 | [-0.325, +0.075] |
| project: lucene | 54 | 0.370 | 0.468 | +0.098 | [+0.018, +0.180] * |
| project: pharos | 45 | 0.762 | 0.753 | -0.009 | [-0.116, +0.100] |

### P@1

| group | n | jina | granite | diff | 95% CI |
|---|---|---|---|---|---|
| overall | 99 | 0.455 | 0.485 | +0.030 | [-0.061, +0.121] |
| category: A | 18 | 0.778 | 1.000 | +0.222 | [+0.056, +0.444] * |
| category: B | 12 | 0.583 | 0.583 | +0.000 | [+0.000, +0.000] |
| category: C | 11 | 0.364 | 0.364 | +0.000 | [-0.273, +0.273] |
| category: D | 20 | 0.300 | 0.400 | +0.100 | [-0.150, +0.350] |
| category: E | 10 | 0.400 | 0.600 | +0.200 | [-0.200, +0.600] |
| category: F | 10 | 0.200 | 0.100 | -0.100 | [-0.300, +0.000] |
| category: G | 8 | 0.375 | 0.250 | -0.125 | [-0.375, +0.000] |
| category: H | 10 | 0.500 | 0.200 | -0.300 | [-0.600, +0.000] |
| project: lucene | 54 | 0.278 | 0.370 | +0.093 | [-0.000, +0.204] |
| project: pharos | 45 | 0.667 | 0.622 | -0.044 | [-0.200, +0.111] |

### Recall@10

| group | n | jina | granite | diff | 95% CI |
|---|---|---|---|---|---|
| overall | 99 | 0.707 | 0.788 | +0.081 | [+0.010, +0.152] * |
| category: A | 18 | 1.000 | 1.000 | +0.000 | [+0.000, +0.000] |
| category: B | 12 | 0.833 | 0.833 | +0.000 | [-0.250, +0.250] |
| category: C | 11 | 0.727 | 0.727 | +0.000 | [-0.273, +0.273] |
| category: D | 20 | 0.500 | 0.650 | +0.150 | [+0.000, +0.300] |
| category: E | 10 | 0.800 | 1.000 | +0.200 | [+0.000, +0.500] |
| category: F | 10 | 0.600 | 0.600 | +0.000 | [-0.300, +0.300] |
| category: G | 8 | 0.500 | 0.750 | +0.250 | [+0.000, +0.625] |
| category: H | 10 | 0.600 | 0.700 | +0.100 | [+0.000, +0.300] |
| project: lucene | 54 | 0.556 | 0.648 | +0.093 | [-0.019, +0.204] |
| project: pharos | 45 | 0.889 | 0.956 | +0.067 | [-0.022, +0.156] |

## Pipeline `hybrid`

### MRR@10

| group | n | jina | granite | diff | 95% CI |
|---|---|---|---|---|---|
| overall | 99 | 0.596 | 0.664 | +0.068 | [+0.008, +0.129] * |
| category: A | 18 | 0.880 | 0.972 | +0.093 | [+0.000, +0.204] |
| category: B | 12 | 0.576 | 0.669 | +0.093 | [-0.022, +0.250] |
| category: C | 11 | 0.524 | 0.712 | +0.189 | [-0.051, +0.447] |
| category: D | 20 | 0.443 | 0.612 | +0.169 | [+0.060, +0.296] * |
| category: E | 10 | 0.608 | 0.708 | +0.100 | [-0.025, +0.246] |
| category: F | 10 | 0.625 | 0.581 | -0.044 | [-0.221, +0.123] |
| category: G | 8 | 0.525 | 0.238 | -0.287 | [-0.618, -0.026] * |
| category: H | 10 | 0.507 | 0.531 | +0.024 | [-0.011, +0.068] |
| project: lucene | 54 | 0.450 | 0.547 | +0.097 | [-0.000, +0.193] |
| project: pharos | 45 | 0.771 | 0.804 | +0.034 | [-0.031, +0.100] |

### P@1

| group | n | jina | granite | diff | 95% CI |
|---|---|---|---|---|---|
| overall | 99 | 0.485 | 0.566 | +0.081 | [+0.000, +0.162] |
| category: A | 18 | 0.778 | 0.944 | +0.167 | [+0.000, +0.333] |
| category: B | 12 | 0.417 | 0.500 | +0.083 | [+0.000, +0.250] |
| category: C | 11 | 0.455 | 0.545 | +0.091 | [-0.182, +0.364] |
| category: D | 20 | 0.350 | 0.550 | +0.200 | [+0.050, +0.400] * |
| category: E | 10 | 0.400 | 0.600 | +0.200 | [+0.000, +0.500] |
| category: F | 10 | 0.500 | 0.500 | +0.000 | [-0.300, +0.300] |
| category: G | 8 | 0.500 | 0.125 | -0.375 | [-0.750, -0.125] * |
| category: H | 10 | 0.400 | 0.400 | +0.000 | [+0.000, +0.000] |
| project: lucene | 54 | 0.333 | 0.426 | +0.093 | [-0.037, +0.222] |
| project: pharos | 45 | 0.667 | 0.733 | +0.067 | [-0.022, +0.178] |

### Recall@10

| group | n | jina | granite | diff | 95% CI |
|---|---|---|---|---|---|
| overall | 99 | 0.818 | 0.879 | +0.061 | [+0.000, +0.121] |
| category: A | 18 | 1.000 | 1.000 | +0.000 | [+0.000, +0.000] |
| category: B | 12 | 0.917 | 0.917 | +0.000 | [-0.250, +0.250] |
| category: C | 11 | 0.818 | 0.909 | +0.091 | [-0.182, +0.364] |
| category: D | 20 | 0.650 | 0.750 | +0.100 | [+0.000, +0.250] |
| category: E | 10 | 0.900 | 1.000 | +0.100 | [+0.000, +0.300] |
| category: F | 10 | 0.800 | 0.800 | +0.000 | [+0.000, +0.000] |
| category: G | 8 | 0.625 | 0.750 | +0.125 | [+0.000, +0.375] |
| category: H | 10 | 0.800 | 0.900 | +0.100 | [+0.000, +0.300] |
| project: lucene | 54 | 0.685 | 0.796 | +0.111 | [+0.000, +0.222] |
| project: pharos | 45 | 0.978 | 0.978 | +0.000 | [+0.000, +0.000] |

## Pipeline `hybrid-rrf`

### MRR@10

| group | n | jina | granite | diff | 95% CI |
|---|---|---|---|---|---|
| overall | 99 | 0.605 | 0.662 | +0.056 | [+0.002, +0.112] * |
| category: A | 18 | 0.907 | 0.972 | +0.065 | [-0.046, +0.185] |
| category: B | 12 | 0.628 | 0.777 | +0.149 | [+0.024, +0.292] * |
| category: C | 11 | 0.556 | 0.697 | +0.141 | [-0.023, +0.327] |
| category: D | 20 | 0.473 | 0.597 | +0.124 | [+0.017, +0.256] * |
| category: E | 10 | 0.645 | 0.651 | +0.006 | [-0.205, +0.229] |
| category: F | 10 | 0.478 | 0.458 | -0.020 | [-0.060, +0.000] |
| category: G | 8 | 0.375 | 0.239 | -0.136 | [-0.359, +0.008] |
| category: H | 10 | 0.625 | 0.608 | -0.017 | [-0.159, +0.125] |
| project: lucene | 54 | 0.440 | 0.525 | +0.085 | [+0.012, +0.162] * |
| project: pharos | 45 | 0.804 | 0.826 | +0.022 | [-0.056, +0.105] |

### P@1

| group | n | jina | granite | diff | 95% CI |
|---|---|---|---|---|---|
| overall | 99 | 0.495 | 0.576 | +0.081 | [+0.000, +0.162] |
| category: A | 18 | 0.833 | 0.944 | +0.111 | [-0.111, +0.333] |
| category: B | 12 | 0.417 | 0.667 | +0.250 | [+0.000, +0.500] |
| category: C | 11 | 0.455 | 0.636 | +0.182 | [+0.000, +0.455] |
| category: D | 20 | 0.400 | 0.500 | +0.100 | [+0.000, +0.250] |
| category: E | 10 | 0.500 | 0.500 | +0.000 | [-0.300, +0.300] |
| category: F | 10 | 0.400 | 0.400 | +0.000 | [+0.000, +0.000] |
| category: G | 8 | 0.250 | 0.125 | -0.125 | [-0.375, +0.000] |
| category: H | 10 | 0.500 | 0.500 | +0.000 | [-0.300, +0.300] |
| project: lucene | 54 | 0.315 | 0.426 | +0.111 | [+0.000, +0.222] |
| project: pharos | 45 | 0.711 | 0.756 | +0.044 | [-0.067, +0.156] |

### Recall@10

| group | n | jina | granite | diff | 95% CI |
|---|---|---|---|---|---|
| overall | 99 | 0.808 | 0.838 | +0.030 | [-0.020, +0.081] |
| category: A | 18 | 1.000 | 1.000 | +0.000 | [+0.000, +0.000] |
| category: B | 12 | 1.000 | 1.000 | +0.000 | [+0.000, +0.000] |
| category: C | 11 | 0.818 | 0.818 | +0.000 | [-0.273, +0.273] |
| category: D | 20 | 0.600 | 0.750 | +0.150 | [+0.000, +0.300] |
| category: E | 10 | 0.900 | 1.000 | +0.100 | [+0.000, +0.300] |
| category: F | 10 | 0.700 | 0.600 | -0.100 | [-0.300, +0.000] |
| category: G | 8 | 0.625 | 0.625 | +0.000 | [+0.000, +0.000] |
| category: H | 10 | 0.800 | 0.800 | +0.000 | [+0.000, +0.000] |
| project: lucene | 54 | 0.685 | 0.741 | +0.056 | [-0.019, +0.148] |
| project: pharos | 45 | 0.956 | 0.956 | +0.000 | [-0.067, +0.067] |

## Pipeline `hybrid-reranked`

### MRR@10

| group | n | jina | granite | diff | 95% CI |
|---|---|---|---|---|---|
| overall | 99 | 0.578 | 0.569 | -0.009 | [-0.057, +0.040] |
| category: A | 18 | 0.740 | 0.722 | -0.018 | [-0.081, +0.023] |
| category: B | 12 | 0.597 | 0.542 | -0.056 | [-0.153, +0.014] |
| category: C | 11 | 0.591 | 0.644 | +0.053 | [-0.212, +0.288] |
| category: D | 20 | 0.427 | 0.475 | +0.048 | [-0.090, +0.202] |
| category: E | 10 | 0.591 | 0.574 | -0.017 | [-0.142, +0.083] |
| category: F | 10 | 0.533 | 0.500 | -0.033 | [-0.100, +0.000] |
| category: G | 8 | 0.500 | 0.396 | -0.104 | [-0.250, +0.000] |
| category: H | 10 | 0.650 | 0.633 | -0.017 | [-0.050, +0.000] |
| project: lucene | 54 | 0.457 | 0.462 | +0.005 | [-0.067, +0.077] |
| project: pharos | 45 | 0.725 | 0.698 | -0.027 | [-0.086, +0.033] |

### P@1

| group | n | jina | granite | diff | 95% CI |
|---|---|---|---|---|---|
| overall | 99 | 0.444 | 0.414 | -0.030 | [-0.091, +0.030] |
| category: A | 18 | 0.611 | 0.556 | -0.056 | [-0.167, +0.000] |
| category: B | 12 | 0.333 | 0.250 | -0.083 | [-0.250, +0.000] |
| category: C | 11 | 0.455 | 0.545 | +0.091 | [-0.182, +0.364] |
| category: D | 20 | 0.350 | 0.350 | +0.000 | [-0.200, +0.200] |
| category: E | 10 | 0.500 | 0.400 | -0.100 | [-0.300, +0.000] |
| category: F | 10 | 0.400 | 0.400 | +0.000 | [+0.000, +0.000] |
| category: G | 8 | 0.375 | 0.250 | -0.125 | [-0.375, +0.000] |
| category: H | 10 | 0.500 | 0.500 | +0.000 | [+0.000, +0.000] |
| project: lucene | 54 | 0.315 | 0.315 | +0.000 | [-0.093, +0.093] |
| project: pharos | 45 | 0.600 | 0.533 | -0.067 | [-0.156, +0.022] |

### Recall@10

| group | n | jina | granite | diff | 95% CI |
|---|---|---|---|---|---|
| overall | 99 | 0.808 | 0.838 | +0.030 | [-0.020, +0.081] |
| category: A | 18 | 1.000 | 1.000 | +0.000 | [+0.000, +0.000] |
| category: B | 12 | 1.000 | 1.000 | +0.000 | [+0.000, +0.000] |
| category: C | 11 | 0.818 | 0.818 | +0.000 | [-0.273, +0.273] |
| category: D | 20 | 0.600 | 0.750 | +0.150 | [+0.000, +0.300] |
| category: E | 10 | 0.900 | 1.000 | +0.100 | [+0.000, +0.300] |
| category: F | 10 | 0.700 | 0.600 | -0.100 | [-0.300, +0.000] |
| category: G | 8 | 0.625 | 0.625 | +0.000 | [+0.000, +0.000] |
| category: H | 10 | 0.800 | 0.800 | +0.000 | [+0.000, +0.000] |
| project: lucene | 54 | 0.685 | 0.741 | +0.056 | [-0.019, +0.148] |
| project: pharos | 45 | 0.956 | 0.956 | +0.000 | [-0.067, +0.067] |

## Decision gate (script output)

- hybrid-reranked MRR@10: A=0.5785 B=0.5690 diff=-0.0095 -> B >= A: False
- hybrid          MRR@10: A=0.5957 B=0.6637 diff=+0.0681 -> B >= A: True
- vector          MRR@10: A=0.5481 B=0.5975 diff=+0.0494 CI95=[-0.0171, +0.1162] -> CI excludes 0 on positive side: False

**DECISION: OPT-IN ONLY**
