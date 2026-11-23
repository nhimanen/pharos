#!/usr/bin/env bash
# Runs a gate command under one hard memory cap, with a watchdog that stops it on a shared host.
#
#   scripts/gate-watchdog.sh --state-dir DIR [options] -- command [args...]
#   scripts/gate-watchdog.sh --self-test        (dry run: stops dummy jobs, starts no gate)
#
# The cap: with a working `systemd-run --user --scope` the whole command runs in one transient scope
# with MemoryMax=<cap> and MemorySwapMax=0 (the kernel OOM-kills inside the scope, never outside it).
# Without it (no systemd user manager, no memory controller delegated) the cgroup-free fallback
# enforces the same cap from the watchdog: every poll sums VmRSS + VmSwap over the recorded process
# tree and stops the gate when the sum exceeds the cap.
#
# The watchdog polls every second. It records every descendant of the command as "pid starttime" in
# DIR/watchdog.pids (cumulative: a process that was reparented after it was recorded is still
# stopped). It stops the gate when MemAvailable drops below --min-available, when the fallback cap is
# exceeded, or when DIR/watchdog.stop exists (manual stop). Stopping sends SIGTERM to every recorded
# PID that still runs with its recorded start time (so a recycled PID is never signalled), waits up
# to --grace seconds, then sends SIGKILL to the survivors. SIGINT is never used: non-interactive bash
# ignores it in background jobs. When the command exits on its own, recorded leftovers are stopped
# the same way, and so is the gate when the watchdog itself gets SIGINT, SIGTERM or SIGHUP.
#
# Options (env in parentheses):
#   --state-dir DIR        pid file, logs and the stop file (GATE_WD_STATE_DIR; required)
#   --mem-max SIZE         hard cap for the whole tree, K/M/G suffix (GATE_MEM_MAX; default 4G)
#   --min-available MIB    stop below this MemAvailable (GATE_MIN_AVAILABLE_MIB; default 3072)
#   --grace SECONDS        SIGTERM to SIGKILL delay (GATE_WD_GRACE; default 10)
#   --no-cgroup            use the cgroup-free fallback even when systemd-run works
#   --self-test            run the dry-run self-test and exit (0 = pass)
# Exit status: the command's, or 137 when the watchdog stopped it.
set -euo pipefail

usage() { sed -n '2,29p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

STATE="${GATE_WD_STATE_DIR:-}"
MEM_MAX="${GATE_MEM_MAX:-4G}"
MIN_AVAIL="${GATE_MIN_AVAILABLE_MIB:-3072}"
GRACE="${GATE_WD_GRACE:-10}"
CGROUP=auto
SELF_TEST=0
while (( $# )); do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --state-dir) STATE="$2"; shift 2 ;;
    --mem-max) MEM_MAX="$2"; shift 2 ;;
    --min-available) MIN_AVAIL="$2"; shift 2 ;;
    --grace) GRACE="$2"; shift 2 ;;
    --no-cgroup) CGROUP=no; shift ;;
    --self-test) SELF_TEST=1; shift ;;
    --) shift; break ;;
    *) echo "gate-watchdog: unknown argument $1" >&2; exit 2 ;;
  esac
done

to_kib() {  # 4G / 512M / 100K / bytes -> KiB
  local v="${1^^}"
  case "$v" in
    *G) echo $(( ${v%G} * 1024 * 1024 )) ;;
    *M) echo $(( ${v%M} * 1024 )) ;;
    *K) echo $(( ${v%K} )) ;;
    *[0-9]) echo $(( v / 1024 )) ;;
    *) echo "gate-watchdog: bad size $1" >&2; return 2 ;;
  esac
}
mem_available_mib() { awk '/^MemAvailable:/{print int($2/1024)}' /proc/meminfo; }

# Field 22 of /proc/PID/stat (start time in clock ticks); empty when PID is gone or a zombie. The
# comm field may hold spaces and parentheses, so fields are counted after the last ')'.
starttime() {  # $1 = PID, $2 = "any" to include a zombie
  local s any="${2:-}"
  s="$(cat "/proc/$1/stat" 2>/dev/null)" || return 0
  s="${s##*) }"
  # shellcheck disable=SC2086
  set -- $s
  [[ "$any" == any || ( "${1:-}" != Z && "${1:-}" != X ) ]] || return 0
  echo "${20:-}"
}

# True when PID runs (not a zombie) with the start time it was recorded with.
same_process() { [[ -n "$2" && "$(starttime "$1")" == "$2" ]]; }

# ---------------------------------------------------------------- self-test
self_test() {
  local tmp rc=0 fails=0
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/gate-watchdog-selftest.XXXXXX")"
  local me="${BASH_SOURCE[0]}"
  # A dummy gate: a driver with two background jobs. One is a plain sleep (a background job, so it
  # ignores SIGINT); the other also ignores SIGTERM, so only the SIGKILL after the grace stops it.
  cat > "$tmp/dummy.sh" <<'DUMMY'
sleep 600 &
echo "pid $!"
bash -c 'trap "" TERM INT; while :; do sleep 1; done' &
echo "pid $!"
trap '' INT
wait
DUMMY
  # A command that orphans a job and exits before the first poll: found by its session id.
  printf '%s\n' 'nohup sleep 600 > /dev/null 2>&1 &' 'echo "pid $!"' 'exit 5' > "$tmp/orphan.sh"
  local mode
  for mode in cgroup fallback-trigger fallback-cap orphan-cgroup orphan-fallback; do
    local sd="$tmp/$mode" args=() t0 status=0 want=137 reason='stopping the gate'
    mkdir -p "$sd"
    case "$mode" in
      cgroup|orphan-cgroup) args=(--mem-max 256M) ;;
      fallback-trigger|orphan-fallback) args=(--no-cgroup --mem-max 256M) ;;
      fallback-cap)     args=(--no-cgroup --mem-max 64M) ;;
    esac
    t0=$SECONDS
    case "$mode" in
      fallback-cap)
        # Over the cap: python allocates 200 MiB and holds it; the fallback must stop the tree.
        GATE_WD_STATE_DIR="$sd" bash "$me" "${args[@]}" -- bash -c 'python3 -I -c "import time; b=bytearray(200<<20)
for i in range(0, len(b), 4096): b[i] = 1
time.sleep(600)" & echo "pid $!"; exec bash "$1"' _ "$tmp/dummy.sh" > "$sd/out" 2>&1 || status=$? ;;
      orphan-*)
        want=5; reason='leaving recorded processes'
        GATE_WD_STATE_DIR="$sd" bash "$me" "${args[@]}" -- bash "$tmp/orphan.sh" > "$sd/out" 2>&1 || status=$? ;;
      *)
        # Trigger the stop path through the real MemAvailable check: a threshold above any host.
        ( sleep 4; echo "self-test: arming the MemAvailable trigger" > "$sd/arm" ) &
        GATE_WD_STATE_DIR="$sd" GATE_WD_TEST_ARM="$sd/arm" bash "$me" "${args[@]}" -- bash "$tmp/dummy.sh" \
          > "$sd/out" 2>&1 || status=$? ;;
    esac
    if [[ "$mode" == *cgroup && "$(cat "$sd/mode" 2>/dev/null)" != cgroup ]]; then
      echo "self-test $mode: SKIP (systemd-run --user --scope with MemoryMax is not usable here)"
      continue
    fi
    # The dummy jobs print "pid N"; every one of them, and every recorded PID, must be gone.
    local left=() jobs=() pid st
    while read -r pid st; do
      [[ -n "$pid" ]] || continue
      if same_process "$pid" "$st"; then left+=("$pid"); fi
    done < "$sd/watchdog.pids"
    while read -r _ pid; do
      jobs+=("$pid")
      if [[ -n "$(starttime "$pid")" ]]; then left+=("$pid"); fi
    done < <(grep -E '^pid [0-9]+$' "$sd/out")
    local why
    why="$(grep -m1 "$reason" "$sd/watchdog.log" || true)"
    if (( ${#left[@]} == 0 && ${#jobs[@]} > 0 && status == want )) && [[ -n "$why" ]] \
        && { [[ "$mode" == orphan-* ]] || grep -q 'KILL' "$sd/watchdog.log"; }; then
      echo "self-test $mode: PASS (dummy jobs ${jobs[*]} and $(wc -l < "$sd/watchdog.pids") recorded PIDs" \
           "gone after $((SECONDS - t0)) s; exit $status; ${why#* })"
    else
      echo "self-test $mode: FAIL (dummy jobs ${jobs[*]:-none}; still running: ${left[*]:-none}; exit $status," \
           "expected $want)"
      sed 's/^/    /' "$sd/watchdog.log" 2>/dev/null || true
      for pid in "${left[@]}"; do kill -KILL "$pid" 2>/dev/null || true; done
      fails=$((fails + 1))
    fi
  done
  echo "self-test logs in $tmp"
  (( fails == 0 )) || rc=1
  return "$rc"
}

if (( SELF_TEST )); then
  self_test
  exit $?
fi

# ---------------------------------------------------------------- run
(( $# )) || { echo "gate-watchdog: no command (see --help)" >&2; exit 2; }
[[ -n "$STATE" ]] || { echo "gate-watchdog: --state-dir is required" >&2; exit 2; }
CAP_KIB="$(to_kib "$MEM_MAX")"
mkdir -p "$STATE"
PIDS_FILE="$STATE/watchdog.pids"
LOG="$STATE/watchdog.log"
MEM_LOG="$STATE/watchdog.mem.log"
STOP_FILE="$STATE/watchdog.stop"
: > "$PIDS_FILE"
rm -f "$STOP_FILE"
log() { echo "$(date +%T) $*" >> "$LOG"; }

avail="$(mem_available_mib)"
if (( avail < MIN_AVAIL )); then
  echo "gate-watchdog: MemAvailable $avail MiB is already below $MIN_AVAIL MiB; not starting" >&2
  exit 3
fi

PREFIX=()
if [[ "$CGROUP" == auto ]] && command -v systemd-run > /dev/null \
    && systemd-run --user --scope -q -p MemoryMax="$MEM_MAX" -p MemorySwapMax=0 -- true 2>/dev/null; then
  PREFIX=(systemd-run --user --scope -q -p MemoryMax="$MEM_MAX" -p MemorySwapMax=0 --)
  echo cgroup > "$STATE/mode"
  log "cap $MEM_MAX via systemd-run --user --scope (MemoryMax, MemorySwapMax=0)"
else
  echo fallback > "$STATE/mode"
  log "cap $MEM_MAX via the cgroup-free fallback (watchdog sums VmRSS+VmSwap of the recorded tree)"
fi
log "stop when MemAvailable < $MIN_AVAIL MiB; SIGTERM, SIGKILL after $GRACE s; now $avail MiB"

# The command gets its own session (setsid execs: a background job is no process group leader), so a
# job it orphans before a poll sees it is still found by its session id. systemd-run --scope execs the
# command too, so $! is the command's PID and its session id in both modes.
if command -v setsid > /dev/null; then
  setsid --wait "${PREFIX[@]}" "$@" &
else
  "${PREFIX[@]}" "$@" &
fi
ROOT=$!
declare -A REC=()   # pid -> starttime, everything recorded so far
ROOT_ST="$(starttime "$ROOT" any)"  # our child: its /proc entry stays until we wait for it
ROOT_ST="${ROOT_ST:-0}"
REC[$ROOT]="$ROOT_ST"
echo "$ROOT $ROOT_ST" > "$PIDS_FILE"
echo "$ROOT" > "$STATE/watchdog.root.pid"
log "started root pid $ROOT: $*"

record_tree() {  # adds every live descendant of a recorded process, and the root's session, to REC
  local pid ppid sid st
  local -A kids=()
  local queue=() p
  while read -r pid ppid sid; do
    kids[$ppid]+="$pid "
    if [[ "$sid" == "$ROOT" && -z "${REC[$pid]:-}" ]]; then
      st="$(starttime "$pid")"
      # same session id and started after the root: never a recycled PID
      if [[ -n "$st" ]] && (( st >= ROOT_ST )); then
        REC[$pid]="$st"
        echo "$pid $st" >> "$PIDS_FILE"
      fi
    fi
  done < <(ps -e -o pid= -o ppid= -o sid=)
  for p in "${!REC[@]}"; do if same_process "$p" "${REC[$p]}"; then queue+=("$p"); fi; done
  while (( ${#queue[@]} )); do
    p="${queue[0]}"; queue=("${queue[@]:1}")
    for pid in ${kids[$p]:-}; do
      [[ -n "${REC[$pid]:-}" ]] && continue
      st="$(starttime "$pid")"
      [[ -n "$st" ]] || continue
      REC[$pid]="$st"
      echo "$pid $st" >> "$PIDS_FILE"
      queue+=("$pid")
    done
  done
}

tree_kib() {  # VmRSS + VmSwap over the live recorded processes
  local p total=0 v
  for p in "${!REC[@]}"; do
    same_process "$p" "${REC[$p]}" || continue
    v="$(awk '/^(VmRSS|VmSwap):/{s+=$2} END{print s+0}' "/proc/$p/status" 2>/dev/null || echo 0)"
    total=$((total + ${v:-0}))
  done
  echo "$total"
}

live_recorded() {
  local p
  for p in "${!REC[@]}"; do if same_process "$p" "${REC[$p]}"; then echo "$p"; fi; done
  return 0
}

stop_all() {  # $1 = reason
  local p left
  log "$1: stopping the gate"
  record_tree
  for p in $(live_recorded); do
    local cmd
    cmd="$(tr '\0' ' ' 2>/dev/null < "/proc/$p/cmdline" | cut -c1-120 || true)"
    if kill -TERM "$p" 2>/dev/null; then log "  TERM $p $cmd"; fi
  done
  local end=$((SECONDS + GRACE))
  while (( SECONDS < end )); do
    left="$(live_recorded)"
    [[ -n "$left" ]] || break
    sleep 0.5
  done
  for p in $(live_recorded); do
    if kill -KILL "$p" 2>/dev/null; then log "  KILL $p (alive ${GRACE} s after TERM)"; fi
  done
  sleep 0.5
  left="$(live_recorded | tr '\n' ' ')"
  if [[ -n "${left// /}" ]]; then log "still running after KILL: $left"; else log "all recorded processes are gone"; fi
  echo "$1" > "$STATE/watchdog.stopped"
}

# Stopping the watchdog itself (Ctrl-C, kill) stops the gate first: it never leaves the gate unwatched.
on_signal() {
  trap - INT TERM HUP
  stop_all "watchdog got SIG$1"
  wait "$ROOT" 2> /dev/null || true
  exit 137
}
trap 'on_signal INT' INT
trap 'on_signal TERM' TERM
trap 'on_signal HUP' HUP

STOPPED=0
while same_process "$ROOT" "$ROOT_ST"; do
  record_tree
  avail="$(mem_available_mib)"
  threshold="$MIN_AVAIL"
  # self-test hook: once the arm file exists, use a threshold above any host so the real check fires
  if [[ -n "${GATE_WD_TEST_ARM:-}" && -f "$GATE_WD_TEST_ARM" ]]; then threshold=$((1 << 40)); fi
  used=0
  if [[ "$(cat "$STATE/mode")" == fallback ]]; then used="$(tree_kib)"; fi
  echo "$(date +%T) avail=${avail}MiB tree=$((used / 1024))MiB" >> "$MEM_LOG"
  if (( avail < threshold )); then
    stop_all "MemAvailable $avail MiB < $threshold MiB"; STOPPED=1; break
  fi
  if (( used > CAP_KIB )); then
    stop_all "tree uses $((used / 1024)) MiB > cap $MEM_MAX"; STOPPED=1; break
  fi
  if [[ -f "$STOP_FILE" ]]; then
    stop_all "stop file $STOP_FILE"; STOPPED=1; break
  fi
  sleep 1
done

rc=0
wait "$ROOT" || rc=$?
if (( ! STOPPED )); then
  record_tree  # orphans of a command that exited between two polls are still in its session
  if [[ -n "$(live_recorded)" ]]; then stop_all "command exited ($rc) leaving recorded processes"; fi
  log "command exited $rc"
  exit "$rc"
fi
log "command stopped by the watchdog (its exit $rc)"
exit 137
