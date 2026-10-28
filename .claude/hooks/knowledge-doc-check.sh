#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to CDP.
# SPDX-License-Identifier: MIT

set -euo pipefail

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty' 2>/dev/null || true)
[ -z "$FILE_PATH" ] && exit 0

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || echo "")
[ -z "$REPO_ROOT" ] && exit 0

FILE_PATH="${FILE_PATH#./}"
FILE_PATH="${FILE_PATH#"$REPO_ROOT"/}"

KNOWLEDGE_DIR="$REPO_ROOT/docs/knowledge"
[ -d "$KNOWLEDGE_DIR" ] || exit 0

[[ "$FILE_PATH" == docs/knowledge/* ]] && exit 0

note_sources() {
  awk '
    NR == 1 && $0 == "---" { in_fm = 1; next }
    in_fm && $0 == "---" { exit }
    in_fm && /^sources:[[:space:]]*$/ { in_src = 1; next }
    in_fm && /^sources:[[:space:]]*\[/ {
      list = $0
      sub(/^sources:[[:space:]]*\[/, "", list)
      sub(/\][[:space:]]*$/, "", list)
      n = split(list, items, ",")
      for (i = 1; i <= n; i++) { gsub(/^[[:space:]"\x27]+|[[:space:]"\x27]+$/, "", items[i]); if (items[i] != "") print items[i] }
      next
    }
    in_fm && in_src && /^[[:space:]]+-[[:space:]]+/ { sub(/^[[:space:]]+-[[:space:]]+/, ""); print; next }
    in_fm && in_src { in_src = 0 }
  ' "$1"
}

matches=()
for note in "$KNOWLEDGE_DIR"/*.md; do
  [ -e "$note" ] || continue
  while IFS= read -r source; do
    [ -z "$source" ] && continue
    if [[ "$FILE_PATH" == "$source" ]] || { [[ "$source" == */ ]] && [[ "$FILE_PATH" == "$source"* ]]; }; then
      matches+=("docs/knowledge/$(basename "$note")")
      break
    fi
  done < <(note_sources "$note")
done

[ ${#matches[@]} -eq 0 ] && exit 0

notes=$(printf ', %s' "${matches[@]}")
notes="${notes:2}"
message="Knowledge check: $FILE_PATH is a listed source of $notes. If this change makes the note wrong, fix the note in the same pull request."

jq -n --arg ctx "$message" '{hookSpecificOutput: {hookEventName: "PostToolUse", additionalContext: $ctx}}'
exit 0
