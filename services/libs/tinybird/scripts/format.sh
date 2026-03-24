#!/bin/bash

PIPES_FOLDER="../pipes"
DATA_SOURCES_FOLDER="../datasources"

show_help() {
  cat << EOF
Usage: format_all.sh [OPTIONS]

Format Tinybird files in pipes and datasources folders.

OPTIONS:
  --help          Show this help message and exit
  --sequential    Run formatting sequentially instead of in parallel
  --match PATTERN Match files containing PATTERN in their name (wildcard match)

EXAMPLES:
  format_all.sh
    Format all files in parallel

  format_all.sh --match cdp_member
    Format only files containing "cdp_member" in their name

  format_all.sh --match cdp_organization --sequential
    Format files containing "cdp_organization" sequentially

EOF
  exit 0
}

# Parse command line arguments
SEQUENTIAL=false
MATCH_PATTERN=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --help|-h)
      show_help
      ;;
    --sequential)
      SEQUENTIAL=true
      shift
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

format_files_in_folder() {
  local folder="$1"
  for file in "$folder"/*; do
    if [ -f "$file" ]; then
      local basename=$(basename "$file")

      # Skip file if pattern is set and doesn't match
      if [ -n "$MATCH_PATTERN" ] && [[ ! "$basename" == *$MATCH_PATTERN* ]]; then
        continue
      fi

      if [ "$SEQUENTIAL" = true ]; then
        tb fmt --yes "$file"
      else
        tb fmt --yes "$file" &
      fi
    fi
  done
}

format_files_in_folder "$PIPES_FOLDER"
format_files_in_folder "$DATA_SOURCES_FOLDER"

# Only wait for background processes in parallel mode
[ "$SEQUENTIAL" = false ] && wait
