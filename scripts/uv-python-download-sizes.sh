#!/usr/bin/env bash
# Total up the download size of every Python build uv can install.
#
# Sends one HEAD request per build (nothing is downloaded) and reads the
# Content-Length of the final response after redirects.
#
# Usage:
#   scripts/uv-python-download-sizes.sh [pythons.json]
#
# If the JSON file does not exist it is generated with:
#   uv python list --all-versions --all-platforms --all-arches \
#     --only-downloads --output-format json
#
# Environment:
#   JOBS=16     parallel requests
#   LIMIT=0     only check the first N builds (0 = all), handy for a dry run
#   OUT=python-download-sizes.tsv   per-build results
#   OS=""       only these OSes, comma-separated (e.g. linux,windows)
#   SKIP_ARCH="" leave out these architectures, comma-separated (e.g. aarch64,armv7)
#
# Example: Linux and Windows only, no ARM
#   OS=linux,windows SKIP_ARCH=aarch64,armv7 scripts/uv-python-download-sizes.sh
#
# Requires: curl, jq, xargs, awk, and uv (only if the JSON must be generated).

set -euo pipefail

JSON="${1:-pythons.json}"
JOBS="${JOBS:-16}"
LIMIT="${LIMIT:-0}"
OUT="${OUT:-python-download-sizes.tsv}"
OS="${OS:-}"
SKIP_ARCH="${SKIP_ARCH:-}"

for cmd in curl jq xargs awk; do
  command -v "$cmd" >/dev/null || { echo "error: $cmd is not installed" >&2; exit 1; }
done

if [[ ! -f "$JSON" ]]; then
  command -v uv >/dev/null || { echo "error: $JSON not found and uv is not installed" >&2; exit 1; }
  echo "Generating $JSON with uv..." >&2
  uv python list --all-versions --all-platforms --all-arches \
    --only-downloads --output-format json > "$JSON"
fi

# One row per build: key, implementation, os, url
rows=$(jq -r --arg os "$OS" --arg skip "$SKIP_ARCH" '
  ($os   | split(",") | map(select(. != ""))) as $oses
  | ($skip | split(",") | map(select(. != ""))) as $skips
  | .[]
  | select(($oses | length) == 0 or (.os as $o | $oses | index($o)))
  | select(.arch as $a | $skips | index($a) | not)
  | [.key, .implementation, .os, .url] | @tsv' "$JSON")
if [[ -z "$rows" ]]; then
  echo "error: no builds match OS=\"$OS\" SKIP_ARCH=\"$SKIP_ARCH\"" >&2
  exit 1
fi
if [[ "$LIMIT" -gt 0 ]]; then
  rows=$(head -n "$LIMIT" <<<"$rows")
fi
total=$(wc -l <<<"$rows" | tr -d ' ')
echo "Checking $total builds with $JOBS parallel requests..." >&2

# Prints: key  implementation  os  bytes   (bytes = ERR when the size is unknown)
check_one() {
  local key="$1" impl="$2" os="$3" url="$4" headers status len
  for attempt in 1 2 3; do
    if headers=$(curl -sSIL --max-time 30 "$url" 2>/dev/null); then
      # With -L there is one header block per hop; keep the last one.
      status=$(awk 'toupper($1) ~ /^HTTP\// {s=$2} END {print s}' <<<"$headers")
      len=$(awk 'tolower($1)=="content-length:" {l=$2} toupper($1) ~ /^HTTP\// {l=""} END {print l}' <<<"$headers" | tr -d '\r')
      if [[ "$status" == 200 && -n "$len" ]]; then
        printf '%s\t%s\t%s\t%s\n' "$key" "$impl" "$os" "$len"
        return
      fi
    fi
    sleep "$attempt"
  done
  printf '%s\t%s\t%s\tERR\n' "$key" "$impl" "$os"
}
export -f check_one

tr '\t' '\0' <<<"$rows" | tr '\n' '\0' \
  | xargs -0 -n 4 -P "$JOBS" bash -c 'check_one "$@"' _ \
  > "$OUT"

awk -F'\t' '
  function gb(b) { return sprintf("%8.2f GB", b / 1e9) }
  $4 == "ERR" { failed++; next }
  { ok++; sum += $4; impl[$2] += $4; implN[$2]++; os[$3] += $4; osN[$3]++ }
  END {
    print "\nBy implementation"
    for (k in impl) printf "  %-12s %5d builds  %s\n", k, implN[k], gb(impl[k])
    print "\nBy OS"
    for (k in os) printf "  %-12s %5d builds  %s\n", k, osN[k], gb(os[k])
    printf "\nTotal: %d builds  %s  (compressed archives, as downloaded)\n", ok, gb(sum)
    if (failed) printf "Missing: %d builds had no size; see rows marked ERR\n", failed
  }' "$OUT"

echo "Per-build sizes written to $OUT" >&2
