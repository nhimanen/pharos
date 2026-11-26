#!/usr/bin/env bash
# Runs scripts/cpudet/CpuDet.java under each Lucene arithmetic configuration and compares the exact bits of
# the scores, so two machines (or one machine pretending to be older hardware) can be checked for
# bitwise-identical vector scoring. Prints one line per configuration; the *_sha columns must match
# between configurations that are meant to be equivalent.
#
#   scripts/cpu-determinism.sh <lucene-core.jar> <work-dir> [quick]
#
# quick: only the pairs and a 768-dim KNN check (no 384-dim index, smaller corpus), for a second machine.
# The index is built once, under the default flags, and every configuration searches that same index:
# in a cluster the writer builds the segments and every reader searches copies of them.
set -euo pipefail
jar="$1"; work="$2"; mode="${3:-full}"
src="$(cd "$(dirname "$0")" && pwd)/cpudet/CpuDet.java"
mkdir -p "$work"
: > "$work/results.txt"
heap="${CPUDET_HEAP:-2g}"
base=(-Xmx"$heap" -XX:+UseParallelGC --enable-native-access=ALL-UNNAMED)
vec=(--add-modules jdk.incubator.vector)
off=(-Dlucene.useScalarFMA=false -Dlucene.useVectorFMA=false)
on=(-Dlucene.useScalarFMA=true -Dlucene.useVectorFMA=true)

# name | JVM flags. "deterministic" is the setting of ruling CPU-R1 (Lucene's scalar code, FMA off); the
# *-cpu entries make this AMD AVX-512 machine behave like older hardware (AVX2 only, no FMA, AVX1, SSE only),
# and *-again repeat a configuration in a fresh JVM (run-to-run reproducibility).
declare -a names flags
add() { names+=("$1"); flags+=("$2"); }
det="${off[*]}"
add default                    "${vec[*]}"
add fma-on                     "${vec[*]} ${on[*]}"
add fma-off                    "${vec[*]} ${off[*]}"
add deterministic              "$det"
add panama256-fma-off          "${vec[*]} ${off[*]} -XX:MaxVectorSize=32"
add panama256-fma-on           "${vec[*]} ${on[*]} -XX:MaxVectorSize=32"
add panama256-fma-off-avx2cpu  "${vec[*]} ${off[*]} -XX:MaxVectorSize=32 -XX:UseAVX=2"
add panama256-fma-off-nofmacpu "${vec[*]} ${off[*]} -XX:MaxVectorSize=32 -XX:-UseFMA"
add panama128-fma-off          "${vec[*]} ${off[*]} -XX:MaxVectorSize=16"
add default-avx2cpu            "${vec[*]} -XX:UseAVX=2"
add fma-on-nofmacpu            "${vec[*]} ${on[*]} -XX:-UseFMA"
add deterministic-avx2cpu      "$det -XX:UseAVX=2"
add deterministic-avx1-nofmacpu "$det -XX:UseAVX=1 -XX:-UseFMA"
add deterministic-sse-only     "$det -XX:UseAVX=0"
add deterministic-c1-only      "$det -XX:TieredStopAtLevel=1"
add deterministic-no-superword "$det -XX:-UseSuperWord"
add scalar-fma-on              "${on[*]}"
add default-again              "${vec[*]}"
add panama256-fma-off-again    "${vec[*]} ${off[*]} -XX:MaxVectorSize=32"
add deterministic-again        "$det"
# the configurations whose indexing throughput is measured (indexes 0-4 above)
builds=(0 1 2 3 4)

run() { # name flags args...
    local name="$1" f="$2"; shift 2
    # shellcheck disable=SC2086
    java "${base[@]}" $f -cp "$jar" "$src" "$@" 2> "$work/$name.$1.err"
}

dims=(768 384); n=50000; nq=1000
[ "$mode" = quick ] && { dims=(768); n=20000; nq=500; }

for dim in "${dims[@]}"; do
    # indexing throughput under the four candidate settings; searches below all use the default-built one
    for i in "${builds[@]}"; do
        echo "== build $dim-dim, ${names[$i]}"
        out="$work/idx$dim"; [ "$i" -gt 0 ] && out="$work/idx$dim-${names[$i]}"
        # an index already there (e.g. copied from another machine) is searched as it is
        if [ "$i" -eq 0 ] && [ -d "$out" ]; then echo "   (reusing $out)"; continue; fi
        [ "$mode" = quick ] && [ "$i" -gt 0 ] && continue
        run "${names[$i]}" "${flags[$i]}" build "$out" "$dim" "$n"
    done
done

for i in "${!names[@]}"; do
    name="${names[$i]}"; f="${flags[$i]}"
    printf '\n## %s: %s\n' "$name" "$f"
    run "$name" "$f" info
    grep -h "preferredBitSize\|not enabled\|not readable" "$work/$name.info.err" | sed 's/^/  lucene: /' | head -2 || true
    for dim in "${dims[@]}"; do
        run "$name" "$f" pairs "$dim" "$work/$name.pairs$dim.bin" | sed "s/^/$name /" | tee -a "$work/results.txt"
        run "$name" "$f" search "$work/idx$dim" "$dim" "$nq" 10 "$work/$name.search$dim.bin" \
            | sed "s/^/$name /" | tee -a "$work/results.txt"
    done
done
printf '\n## software Math.fma (-XX:-UseFMA) vs hardware\n'
run fma-sw "-XX:-UseFMA" fma
run fma-hw "" fma

# Verdict: every deterministic* configuration must reproduce the bits of "deterministic" exactly, cold and warm.
printf '\n## verdict (bits compared with "deterministic")\n'
python3 - "$work/results.txt" <<'PY'
import re, sys
rows = {}
for line in open(sys.argv[1]):
    name, kind, rest = line.split(" ", 2)
    kv = dict(re.findall(r"(\w+)=(\S+)", rest))
    rows.setdefault(name, {})[(kind, kv["dim"])] = kv
ref = rows["deterministic"]
bad = 0
for name, r in rows.items():
    same = []
    for key, kv in sorted(r.items()):
        fields = ["cos_sha", "dot_sha"] if key[0] == "PAIRS" else ["knn_sha", "exact_sha"]
        same.append(all(kv[f] == ref[key][f] for f in fields))
        cold = kv.get("cold_vs_warm_diff", "?")
        if not cold.startswith("0/"): same[-1] = same[-1] and False
    verdict = "IDENTICAL" if all(same) else "differs"
    if name.startswith("deterministic") and not all(same):
        bad = 1
    print(f"{name:30s} {verdict}")
sys.exit(bad)
PY
