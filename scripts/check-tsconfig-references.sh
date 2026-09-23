#!/usr/bin/env bash

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

refs="$(
  grep -v '^[[:space:]]*//' tsconfig.json \
    | grep -oE '"path"[[:space:]]*:[[:space:]]*"[^"]+"' \
    | sed -E 's/.*"([^"]+)"$/\1/; s#^\./##; s#/$##'
)"

missing=()

check() {
  printf '%s\n' "$refs" | grep -qxF "$1" || missing+=("$1")
}

shopt -s nullglob
for dir in services/libs/* services/apps/* services/archetypes/* backend; do
  if [[ -f "$dir/tsconfig.check.json" ]]; then
    check "$dir/tsconfig.check.json"
  elif [[ -f "$dir/tsconfig.json" ]]; then
    check "$dir"
  fi

  for file in "$dir"/tsconfig.*.json; do
    [[ "$file" == */tsconfig.check.json ]] || check "$file"
  done
done
shopt -u nullglob

if (( ${#missing[@]} )); then
  printf 'Missing from tsconfig.json references:\n'
  printf '  %s\n' "${missing[@]}"
  exit 1
fi

printf 'All packages are in tsconfig.json references.\n'
