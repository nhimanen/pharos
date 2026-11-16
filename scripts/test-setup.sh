#!/usr/bin/env bash
# Tests for setup.sh release mode. Every case runs with a throwaway HOME; the real
# ~/.pharos and ~/.local/bin are never touched. Run: bash scripts/test-setup.sh
set -uo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
SETUP="${SETUP:-$REPO/setup.sh}"
JAR="$(ls "$REPO"/target/pharos-*.jar 2>/dev/null | grep -v original | sort -V | tail -1 || true)"
if [ -z "$JAR" ]; then
  (cd "$REPO" && mvn -q package -DskipTests) || { echo "build failed"; exit 1; }
  JAR="$(ls "$REPO"/target/pharos-*.jar | grep -v original | sort -V | tail -1)"
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
TAG="v9.9.9"
FAILED=0

# Fake release dir: $WORK/release/{latest,v9.9.9/<assets>}
REL="$WORK/release"
mkdir -p "$REL/$TAG" "$WORK/skillsrc"
cp "$JAR" "$REL/$TAG/pharos-9.9.9.jar"
(cd "$REL/$TAG" && sha256sum pharos-9.9.9.jar > pharos-9.9.9.jar.sha256)
cp "$REPO/pharos" "$REL/$TAG/pharos"
cp "$SETUP" "$REL/$TAG/setup.sh"
cp -r "$REPO/.claude/skills/pharos" "$WORK/skillsrc/pharos"
tar -czf "$REL/$TAG/pharos-skill.tar.gz" -C "$WORK/skillsrc" pharos
echo "$TAG" > "$REL/latest"
GOOD_SHA="$(cut -d' ' -f1 "$REL/$TAG/pharos-9.9.9.jar.sha256")"

# make_variant TAG VER JARFILE SKILLTAR  -> extra fake release dir with its own checksum
make_variant() {
  local tag="$1" ver="$2" jar="$3" skill="$4"
  mkdir -p "$REL/$tag"
  cp "$jar" "$REL/$tag/pharos-$ver.jar"
  (cd "$REL/$tag" && sha256sum "pharos-$ver.jar" > "pharos-$ver.jar.sha256")
  cp "$REPO/pharos" "$REL/$tag/pharos"
  cp "$SETUP" "$REL/$tag/setup.sh"
  cp "$skill" "$REL/$tag/pharos-skill.tar.gz"
}
echo "not a jar" > "$WORK/fake.jar"
make_variant v9.9.8 9.9.8 "$WORK/fake.jar" "$REL/$TAG/pharos-skill.tar.gz"
mkdir -p "$WORK/badskill/other" && echo x > "$WORK/badskill/other/f"
tar -czf "$WORK/badskill.tar.gz" -C "$WORK/badskill" other
make_variant v9.9.7 9.9.7 "$JAR" "$WORK/badskill.tar.gz"

new_home() { mktemp -d "$WORK/home.XXXXXX"; }

# register_project HOME NAME FORMAT|none  -> registry.json entry + index dir (stamp optional)
register_project() {
  local home="$1" name="$2" fmt="$3"
  local idx="$home/.pharos/indexes/$name"
  mkdir -p "$idx" "$home/src/$name"
  if [ "$fmt" != none ]; then
    printf '{"format": %s, "pharosVersion": "test", "writtenAt": "2026-01-01T00:00:00Z"}\n' "$fmt" > "$idx/index-format.json"
  fi
  head -c 2048 /dev/zero > "$idx/dummy.bin"
  python3 - "$home/.pharos/registry.json" "$name" "$home/src/$name" "$idx" <<'PY'
import json, os, sys
path, name, root, idx = sys.argv[1:5]
d = json.load(open(path)) if os.path.exists(path) else {}
d[name] = {"name": name, "rootPath": root, "indexPath": idx,
           "lastIndexed": "2026-11-12T10:00:00Z", "methodCount": 1, "classCount": 1,
           "fileCount": 1, "knownPackages": [], "linkedProjects": [], "unresolvedRefs": []}
json.dump(d, open(path, "w"))
PY
}

# preinstall HOME -> a pre-existing old install with marker contents
preinstall() {
  mkdir -p "$1/.pharos/bin"
  echo "OLD-JAR" > "$1/.pharos/bin/pharos.jar"
}

run_setup() { # HOME args...
  local home="$1"; shift
  HOME="$home" PHAROS_NO_DAEMON=1 PHAROS_PORT=17999 PHAROS_RELEASE_BASE_URL="file://$REL" \
    bash "$SETUP" "$@" </dev/null >"$home/out.txt" 2>&1
  echo $? > "$home/rc"
}
rc() { cat "$1/rc"; }

check() { # name condition-exit-status detail
  if [ "$2" -eq 0 ]; then echo "PASS $1"; else echo "FAIL $1 ($3)"; FAILED=1; fi
}

t_release_install_verifies_and_installs() {
  local h; h="$(new_home)"
  run_setup "$h" --release latest
  local got; got="$(sha256sum "$h/.pharos/bin/pharos.jar" 2>/dev/null | cut -d' ' -f1)"
  [ "$(rc "$h")" = 0 ] && [ "$got" = "$GOOD_SHA" ] && [ -f "$h/.pharos/bin/setup.sh" ] \
    && [ -f "$h/.claude/skills/pharos/SKILL.md" ] && [ -L "$h/.local/bin/pharos" ]
  check release_install_verifies_and_installs $? "rc=$(rc "$h"); $(tail -5 "$h/out.txt")"
}

t_corrupt_checksum_aborts_without_touching_install() {
  local h; h="$(new_home)"; preinstall "$h"
  local good="$REL/$TAG/pharos-9.9.9.jar.sha256"
  cp "$good" "$good.bak"
  echo "0000000000000000000000000000000000000000000000000000000000000000  pharos-9.9.9.jar" > "$good"
  run_setup "$h" --release "$TAG"
  mv "$good.bak" "$good"
  [ "$(rc "$h")" = 3 ] && [ "$(cat "$h/.pharos/bin/pharos.jar")" = "OLD-JAR" ]
  check corrupt_checksum_aborts_without_touching_install $? "rc=$(rc "$h")"
}

# Blocking path: stamp format -1 is two behind INDEX_FORMAT=1 (MIN_READABLE_FORMAT=0) -> outdated.
t_warning_lists_outdated_projects_and_declines_noninteractive() {
  local h; h="$(new_home)"; preinstall "$h"
  register_project "$h" brokenproj -1
  run_setup "$h" --release "$TAG"
  [ "$(rc "$h")" = 2 ] && grep -q brokenproj "$h/out.txt" && grep -q -- '--upgrade-indexes' "$h/out.txt" \
    && [ "$(cat "$h/.pharos/bin/pharos.jar")" = "OLD-JAR" ]
  check warning_lists_outdated_projects_and_declines_noninteractive $? "rc=$(rc "$h"); $(cat "$h/out.txt")"
}

t_yes_installs_despite_outdated() {
  local h; h="$(new_home)"; preinstall "$h"
  register_project "$h" brokenproj -1
  run_setup "$h" --release "$TAG" --yes
  local got; got="$(sha256sum "$h/.pharos/bin/pharos.jar" | cut -d' ' -f1)"
  [ "$(rc "$h")" = 0 ] && [ "$got" = "$GOOD_SHA" ] && grep -q brokenproj "$h/out.txt"
  check yes_installs_despite_outdated $? "rc=$(rc "$h"); $(tail -8 "$h/out.txt")"
}

# N-1 (unstamped = format 0 = upgrade-available): listed as searchable, no prompt needed.
t_searchable_only_installs_without_prompt() {
  local h; h="$(new_home)"; preinstall "$h"
  register_project "$h" oldish none
  run_setup "$h" --release "$TAG"
  [ "$(rc "$h")" = 0 ] && grep -q oldish "$h/out.txt" && grep -qi 'searchable' "$h/out.txt"
  check searchable_only_installs_without_prompt $? "rc=$(rc "$h"); $(tail -8 "$h/out.txt")"
}

t_upgrade_indexes_flag() {
  local h; h="$(new_home)"
  run_setup "$h" --release "$TAG"
  register_project "$h" oldish none
  run_setup "$h" --upgrade-indexes --yes
  # Project root is an empty dir so the re-index may fail; we only require the command ran.
  grep -qi 'upgrad\|re-index\|reindex\|oldish' "$h/out.txt"
  check upgrade_indexes_flag $? "rc=$(rc "$h"); $(tail -8 "$h/out.txt")"
}

t_index_status_failure_fails_closed() {
  local h; h="$(new_home)"; preinstall "$h"
  register_project "$h" someproj 1
  run_setup "$h" --release v9.9.8
  local rc1; rc1="$(rc "$h")"
  local msg=0; grep -qi 'could not determine' "$h/out.txt" || msg=1
  local jar1; jar1="$(cat "$h/.pharos/bin/pharos.jar")"
  run_setup "$h" --release v9.9.8 --yes
  [ "$rc1" = 2 ] && [ "$msg" = 0 ] && [ "$jar1" = "OLD-JAR" ] && [ "$(rc "$h")" = 0 ] \
    && [ "$(cat "$h/.pharos/bin/pharos.jar")" = "not a jar" ]
  check index_status_failure_fails_closed $? "rc1=$rc1 msg=$msg rc2=$(rc "$h"); $(tail -6 "$h/out.txt")"
}

t_bad_skill_bundle_leaves_install_untouched() {
  local h; h="$(new_home)"; preinstall "$h"
  mkdir -p "$h/.claude/skills/pharos"; echo OLD-SKILL > "$h/.claude/skills/pharos/SKILL.md"
  run_setup "$h" --release v9.9.7 --yes
  [ "$(rc "$h")" != 0 ] && [ "$(cat "$h/.pharos/bin/pharos.jar")" = "OLD-JAR" ] \
    && [ "$(cat "$h/.claude/skills/pharos/SKILL.md")" = "OLD-SKILL" ]
  check bad_skill_bundle_leaves_install_untouched $? "rc=$(rc "$h"); $(tail -4 "$h/out.txt")"
}

t_upgrade_indexes_without_yes_does_not_crash_on_empty_array() {
  local h; h="$(new_home)"
  run_setup "$h" --release "$TAG"
  run_setup "$h" --upgrade-indexes
  ! grep -q 'unbound variable' "$h/out.txt"
  check upgrade_indexes_without_yes_no_unbound_var $? "$(tail -4 "$h/out.txt")"
}

t_release_install_verifies_and_installs
t_corrupt_checksum_aborts_without_touching_install
t_warning_lists_outdated_projects_and_declines_noninteractive
t_yes_installs_despite_outdated
t_searchable_only_installs_without_prompt
t_upgrade_indexes_flag
t_index_status_failure_fails_closed
t_bad_skill_bundle_leaves_install_untouched
t_upgrade_indexes_without_yes_does_not_crash_on_empty_array
exit "$FAILED"
