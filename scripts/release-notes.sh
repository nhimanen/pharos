#!/usr/bin/env bash
# Print the body of the "## [<version>]" section of CHANGELOG.md (heading excluded,
# up to the next "## [" heading). Exit 1 if the section is missing or has no
# "**Index format:**" line. Usage: scripts/release-notes.sh <version> [changelog]
set -u
VERSION="${1:-}"
[ -n "$VERSION" ] || { echo "usage: $0 <version> [changelog]" >&2; exit 2; }
CHANGELOG="${2:-$(cd "$(dirname "$0")/.." && pwd)/CHANGELOG.md}"
[ -f "$CHANGELOG" ] || { echo "release-notes: $CHANGELOG not found" >&2; exit 1; }

BODY="$(awk -v v="$VERSION" '
  /^## \[/ {
    if (found) exit
    if (index($0, "## [" v "]") == 1) { found = 1; next }
  }
  found { print }
' "$CHANGELOG")"

if ! awk -v v="$VERSION" 'index($0, "## [" v "]") == 1 { f = 1 } END { exit !f }' "$CHANGELOG"; then
  echo "release-notes: no '## [$VERSION]' section in $CHANGELOG" >&2
  exit 1
fi
if ! printf '%s\n' "$BODY" | grep -q '^\*\*Index format:\*\*'; then
  echo "release-notes: section [$VERSION] has no '**Index format:**' line" >&2
  exit 1
fi
printf '%s\n' "$BODY"
