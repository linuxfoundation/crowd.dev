#!/usr/bin/env bash

set -eo pipefail
CLI_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $CLI_HOME/utils.sh

if ! command -v jq &>/dev/null; then
  error "jq is required but is not installed. Install it: brew install jq (macOS) / apt-get install jq (Debian/Ubuntu) / dnf install jq (Fedora)."
  exit 1
fi

REQUIRED_SCRIPTS=("tsc-check")
FAILED=0

check_pkg_json () {
  local pkg_json="$1"
  local label="$2"
  local missing=()

  for script in "${REQUIRED_SCRIPTS[@]}"; do
    if [ "$(jq -r --arg s "$script" '.scripts[$s] // empty' "$pkg_json")" == "" ]; then
      missing+=("$script")
    fi
  done

  if [ ${#missing[@]} -gt 0 ]; then
    error "$label is missing required script(s): ${missing[*]}"
    FAILED=1
  fi
}

check_dir () {
  local group_dir="$1"
  local group_label="$2"

  for pkg_dir in "$group_dir"*/; do
    local pkg_json="${pkg_dir}package.json"
    if [ -f "$pkg_json" ]; then
      check_pkg_json "$pkg_json" "$group_label $(basename "$pkg_dir")"
    fi
  done
}

check_dir "$CLI_HOME/../libs/" "Library"
check_dir "$CLI_HOME/../archetypes/" "Archetype"
check_dir "$CLI_HOME/../apps/" "App"
check_pkg_json "$CLI_HOME/../../backend/package.json" "Backend"

if [ "$FAILED" -eq 1 ]; then
  error "One or more packages are missing required scripts (${REQUIRED_SCRIPTS[*]})."
  exit 1
fi

say "All services packages define the required scripts."
