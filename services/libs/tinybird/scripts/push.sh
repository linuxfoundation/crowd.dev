#!/bin/bash

PIPES_FOLDER="pipes"

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.." || exit 1

show_help() {
  cat << EOF
Usage: push.sh [OPTIONS]

Push Tinybird pipe files using "tb push".

OPTIONS:
  --help          Show this help message and exit
  --match PATTERN Match files containing PATTERN in their name (wildcard match)

EXAMPLES:
  push.sh
    Push all pipe files

  push.sh --match leaderboards_
    Push only files containing "leaderboards_" in their name

EOF
  exit 0
}

# Parse command line arguments
MATCH_PATTERN=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --help|-h)
      show_help
      ;;
    --match)
      if [ -z "$2" ] || [[ "$2" == --* ]]; then
        echo "Error: --match requires a pattern argument"
        echo ""
        show_help
      fi
      MATCH_PATTERN="$2"
      shift 2
      ;;
    *)
      echo "Error: Unknown option: $1"
      echo ""
      show_help
      ;;
  esac
done

for file in "$PIPES_FOLDER"/*; do
  if [ -f "$file" ]; then
    local_basename=$(basename "$file")

    # Skip file if pattern is set and doesn't match
    if [ -n "$MATCH_PATTERN" ] && [[ ! "$local_basename" == *$MATCH_PATTERN* ]]; then
      continue
    fi

    tb push "$PIPES_FOLDER/$local_basename" --force --yes
  fi
done
