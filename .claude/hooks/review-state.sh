#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to CDP.
# SPDX-License-Identifier: MIT
#
# Markers are keyed by the content of the change against its merge base, not by
# commit sha, so a commit made after the review keeps the review valid.

set -euo pipefail

cmd="${1:-status}"
base_ref="${CDP_REVIEW_BASE:-origin/main}"
open_important='[.findings[] | select(.tag == "Important" and (.declined // false | not))] | length'

usage() {
  cat >&2 <<'EOF'
usage: review-state.sh <command>
  key                 print the key of the current change (or "empty")
  base                print the merge-base sha
  status              print the marker for the current change as JSON
  last                print the branch's most recent marker as JSON
  mark < marker.json  store a marker for the current change
  decline <id> <why>  decline an Important finding with a reason
  gate                PreToolUse hook: block `git push` without a clean review
EOF
}

load_state() {
  local where="${1:-.}"
  root=$(git -C "$where" rev-parse --show-toplevel)
  dir="$(git -C "$root" rev-parse --path-format=absolute --git-common-dir)/cdp-review"
  mkdir -p "$dir/branches"
  branch=$(git -C "$root" rev-parse --abbrev-ref HEAD)
  branch_file="$dir/branches/${branch//\//__}"
  base=$(git -C "$root" merge-base "$base_ref" HEAD 2>/dev/null) || return 1
}

fail_base() {
  echo "review state: cannot resolve base ref '$base_ref' (set CDP_REVIEW_BASE or fetch origin main)" >&2
  exit "$1"
}

diff_is_empty() { git -C "$root" diff --quiet "$base" -- .; }
tree_matches_head() { git -C "$root" diff --quiet HEAD -- .; }
sha256() { if command -v sha256sum >/dev/null; then sha256sum; else shasum -a 256; fi; }
diff_key() {
  if diff_is_empty; then echo "empty"; return; fi
  git -C "$root" diff "$base" -- . | sha256 | cut -c1-16
}
marker_path() { echo "$dir/$1.json"; }

write_marker_atomically() {
  local target="$1" tmp
  tmp=$(mktemp "$dir/.marker.XXXXXX")
  cat > "$tmp"
  mv "$tmp" "$target"
}

refuse() {
  printf 'REVIEW GATE: %s\n' "$@" >&2
  exit 2
}

tokenize() {
  awk '
    function emit() {
      if (!have) return
      gsub(/\n/, " ", tok)
      if (pending) { delim = tok; pending = 0 } else if (operand) operand = 0; else print "T" tok
      tok = ""; have = 0
    }
    function sep(s) { emit(); operand = 0; print "S" s }
    function redirect(width) { if (have && tok ~ /^[0-9]+$/) { tok = ""; have = 0 } else emit(); operand = 1; i += width }
    BEGIN { q = ""; tok = ""; have = 0; delim = ""; dash = 0; body_exec = 0; pending = 0; cont = 0; operand = 0 }
    {
      line = $0
      if (delim != "" && q == "") {
        t = line; if (dash) sub(/^\t+/, "", t)
        if (t == delim) { delim = ""; next }
        if (!body_exec) next
      }
      if (!cont && q == "" && delim == "") body_exec = (line ~ /^[[:space:]]*((sudo|env|command|exec)[[:space:]]+)*(\/[^[:space:]]*\/)?(ba|z|da|k)?sh([[:space:]]|$)/) && (line !~ /\.claude\/hooks\/review-state\.sh/)
      cont = 0
      n = length(line); i = 1
      while (i <= n) {
        c = substr(line, i, 1)
        if (q == "\047") { if (c == "\047") q = ""; else tok = tok c; have = 1; i++; continue }
        if (q == "\"") {
          if (c == "\\" && i < n) { tok = tok substr(line, i + 1, 1); i += 2; continue }
          if (c == "\"") q = ""; else tok = tok c
          have = 1; i++; continue
        }
        if (c == "\\") { if (i == n) { cont = 1; break } tok = tok substr(line, i + 1, 1); have = 1; i += 2; continue }
        if (c == "\047" || c == "\"") { q = c; have = 1; i++; continue }
        if (c == " " || c == "\t") { emit(); i++; continue }
        two = substr(line, i, 2); three = substr(line, i, 3)
        if (three == "<<<") { emit(); i += 3; continue }
        if (two == "<<") { emit(); pending = 1; if (substr(line, i + 2, 1) == "-") { dash = 1; i += 3 } else { dash = 0; i += 2 }; continue }
        if (two == ">&" || two == "<&" || two == ">>") { redirect(2); continue }
        if (two == "&&" || two == "||" || two == "$(") { sep(two); i += 2; continue }
        if (c == ";" || c == "|" || c == "&" || c == "(" || c == ")" || c == "{" || c == "}" || c == "`") { sep(c); i++; continue }
        if (c == ">" || c == "<") { redirect(1); continue }
        tok = tok c; have = 1; i++
      }
      if (q != "") { tok = tok "\n"; next }
      if (cont) next
      sep("\n")
    }
    END { if (q != "" || pending) print "U" }
  ' <<< "$1"
}

segment_text() { local IFS=' '; printf '%s' "$*"; }
looks_like_push() { printf '%s' "$1" | grep -Eq '(^|[^[:alnum:]_.-])git[[:space:]]+([^[:space:]]+[[:space:]]+)*push([^[:alnum:]_-]|$)'; }
runs_review_state() {
  local arg
  for arg in "$@"; do
    [[ "$arg" == -* ]] && continue
    [[ "$arg" == .claude/hooks/review-state.sh || "$arg" == */.claude/hooks/review-state.sh ]]
    return
  done
  return 1
}

resolve_dir() {
  local from="$1" target="$2"
  case "$target" in
    "" | "~") printf '%s' "$HOME" ;;
    "~/"*) printf '%s' "$HOME/${target#\~/}" ;;
    *[\$\`]* | -) return 1 ;;
    /*) (cd "$target" 2>/dev/null && pwd -P) ;;
    *) [[ "$from" != "?" ]] && (cd "$from" 2>/dev/null && cd "$target" 2>/dev/null && pwd -P) ;;
  esac
}

WRAPPERS=' if then do else elif while until ! time nohup command exec nice sudo env timeout xargs stdbuf caffeinate script builtin '
TEXT_COMMANDS=' echo printf cat grep rg sed awk jq tee head tail wc sort uniq diff cut tr less more '
INTERPRETERS=' sh bash zsh dash ksh eval '

classify_segment() {
  local -a t=("$@")
  local i=0 first text skip=0 git_dir
  [[ ${#t[@]} -gt 0 ]] || return 0
  text=$(segment_text "${t[@]}")
  while :; do
    while [[ $i -lt ${#t[@]} && "${t[$i]}" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]; do
      [[ "${t[$i]}" == "CDP_SKIP_REVIEW=1" ]] && skip=1
      [[ "${t[$i]}" == GIT_DIR=* || "${t[$i]}" == GIT_WORK_TREE=* || "${t[$i]}" == GIT_COMMON_DIR=* ]] && work_dir="?"
      i=$((i + 1))
    done
    [[ $i -lt ${#t[@]} ]] || return 0
    first="${t[$i]##*/}"
    [[ "$WRAPPERS" == *" $first "* ]] || break
    local wrapper="$first"
    i=$((i + 1))
    while [[ $i -lt ${#t[@]} && "${t[$i]}" == -* ]]; do i=$((i + 1)); done
    [[ "$wrapper" == "timeout" && $i -lt ${#t[@]} ]] && i=$((i + 1))
  done
  case "$first" in
    cd)
      work_dir=$(resolve_dir "$work_dir" "${t[$((i + 1))]:-}") || work_dir="?"
      return 0 ;;
    pushd)
      dir_stack+=("$work_dir")
      work_dir=$(resolve_dir "$work_dir" "${t[$((i + 1))]:-}") || work_dir="?"
      return 0 ;;
    popd)
      if [[ ${#dir_stack[@]} -gt 0 ]]; then work_dir="${dir_stack[$((${#dir_stack[@]} - 1))]}"; unset "dir_stack[$((${#dir_stack[@]} - 1))]"; else work_dir="?"; fi
      return 0 ;;
    git)
      if [[ "$next_sep" == '$(' || "$next_sep" == '`' ]] && looks_like_push "$whole_command"; then
        refuse "a git command whose arguments come from a command substitution cannot be checked: $text" "Run the push with literal arguments."
      fi ;;
    *)
      if [[ "$INTERPRETERS" == *" $first "* ]]; then
        runs_review_state "${t[@]:$((i + 1))}" && return 0
        { looks_like_push "$text" || { [[ "$prev_sep" == "|" ]] && looks_like_push "$whole_command"; }; } && refuse "a push wrapped in '$first' cannot be checked: $text" "Run the push as a plain 'git push ...' command."
      elif [[ "$TEXT_COMMANDS" != *" $first "* ]]; then
        looks_like_push "$text" && refuse "a push behind '$first' cannot be checked: $text" "Run the push as a plain 'git push ...' command."
      fi
      return 0 ;;
  esac
  i=$((i + 1))
  git_dir="$work_dir"
  while [[ $i -lt ${#t[@]} && "${t[$i]}" == -* ]]; do
    case "${t[$i]}" in
      -C) git_dir=$(resolve_dir "$git_dir" "${t[$((i + 1))]:-}") || git_dir="?"; i=$((i + 2)) ;;
      -C*) git_dir=$(resolve_dir "$git_dir" "${t[$i]#-C}") || git_dir="?"; i=$((i + 1)) ;;
      --git-dir=* | --work-tree=*) git_dir="?"; i=$((i + 1)) ;;
      --git-dir | --work-tree) git_dir="?"; i=$((i + 2)) ;;
      -c)
        [[ "${t[$((i + 1))]:-}" == alias.* ]] && refuse "a git alias cannot be checked: $text" "Run the push as a plain 'git push ...' command."
        i=$((i + 2)) ;;
      -c*) [[ "${t[$i]#-c}" == alias.* ]] && refuse "a git alias cannot be checked: $text" "Run the push as a plain 'git push ...' command."; i=$((i + 1)) ;;
      --namespace | --exec-path | --config-env) i=$((i + 2)) ;;
      *) i=$((i + 1)) ;;
    esac
  done
  [[ $i -lt ${#t[@]} && "${t[$i]}" == "push" ]] || return 0
  [[ "$git_dir" != "?" ]] || refuse "cannot tell which repository this push targets: $text" "Run the push from the repository root with a plain 'git push ...' command."
  push_dirs+=("$git_dir"); push_skips+=("$skip")
  local args; args=$(printf '%s\n' "${t[@]:$((i + 1))}")
  push_args+=("$args")
}

collect_pushes() {
  local rec
  local -a seg=()
  push_dirs=(); push_args=(); push_skips=(); dir_stack=()
  work_dir="$PWD"; whole_command="$1"; unparsable=0; prev_sep=""; next_sep=""
  while IFS= read -r rec; do
    case "$rec" in
      T*) seg+=("${rec#T}") ;;
      S*) next_sep="${rec#S}"; classify_segment "${seg[@]+"${seg[@]}"}"; seg=(); prev_sep="$next_sep" ;;
      U) unparsable=1 ;;
    esac
  done < <(tokenize "$1")
  next_sep=""
  classify_segment "${seg[@]+"${seg[@]}"}"
  [[ $unparsable -eq 1 ]] && looks_like_push "$1" && refuse "cannot parse this command well enough to check the review state (unterminated quote)." "Run the push as a plain 'git push ...' command."
  return 0
}

pushes_only_head() {
  local head arg src sha first=1
  head=$(git -C "$root" rev-parse HEAD)
  while IFS= read -r arg; do
    [[ -z "$arg" ]] && continue
    case "$arg" in
      --delete | -d) return 0 ;;
      --all | --mirror | --branches) echo "$arg"; return 1 ;;
      -*) continue ;;
    esac
    if [[ $first -eq 1 ]]; then first=0; continue; fi
    src="${arg%%:*}"; src="${src#+}"
    [[ -z "$src" ]] && continue
    case "$src" in *[\$\`]*) echo "$arg"; return 1 ;; esac
    sha=$(git -C "$root" rev-parse --verify --quiet "${src}^{commit}") || { echo "$arg"; return 1; }
    [[ "$sha" == "$head" ]] || { echo "$arg"; return 1; }
  done
  return 0
}

check_push() {
  local push_dir="$1" args="$2"
  load_state "$push_dir" || fail_base 2
  if ! other=$(printf '%s\n' "$args" | pushes_only_head); then
    refuse "the push names '$other', which is not the checked-out HEAD or does not resolve locally, so its review state cannot be checked." \
      "Check out the branch you want to push, run /review there, then push it as HEAD."
  fi
  diff_is_empty && return 0
  local key f open
  key=$(diff_key); f=$(marker_path "$key")
  if [[ ! -f "$f" ]]; then
    refuse "no local review covers the current change in $root (key $key, base $base_ref)." \
      "Run the /review skill, fix or decline every Important finding, then push again." \
      "A deliberate bypass the user asked for: prefix the command with CDP_SKIP_REVIEW=1."
  fi
  open=$(jq -r '.important_open // 1' "$f")
  if [[ "$open" != "0" ]]; then
    refuse "the last review (round $(jq -r .round "$f")) left $open Important finding(s) open:" \
      "$(jq -r '.findings[] | select(.tag == "Important" and (.declined // false | not)) | "  [\(.id)] \(.file):\(.line) \(.summary)"' "$f")" \
      "Fix them (or decline with a reason via: bash .claude/hooks/review-state.sh decline <id> \"<reason>\"), run /review again, then push."
  fi
  tree_matches_head || refuse "the reviewed change has uncommitted edits in $root; the push would ship HEAD without them." \
    "Commit the working tree (the review stays valid, its key is content-based), then push."
}

case "$cmd" in
  key) load_state || fail_base 1; diff_key ;;
  base) load_state || fail_base 1; echo "$base" ;;
  status)
    load_state || fail_base 1
    key=$(diff_key)
    f=$(marker_path "$key")
    if [[ "$key" == "empty" ]]; then echo '{"status":"empty"}'
    elif [[ -f "$f" ]]; then jq '. + {status: (if .important_open == 0 then "clean" else "open" end)}' "$f"
    else jq -n --arg key "$key" '{status:"unreviewed", key:$key}'
    fi
    ;;
  last)
    load_state || fail_base 1
    if [[ -f "$branch_file" ]] && [[ -f "$(marker_path "$(cat "$branch_file")")" ]]; then
      cat "$(marker_path "$(cat "$branch_file")")"
    else
      echo '{"round":0}'
    fi
    ;;
  mark)
    load_state || fail_base 1
    key=$(diff_key)
    [[ "$key" == "empty" ]] && { echo "nothing to mark: no change against $base_ref" >&2; exit 1; }
    input=$(cat)
    printf '%s' "$input" | jq -e 'has("important_open") and has("round") and has("findings")' >/dev/null \
      || { echo "marker needs important_open, round and findings" >&2; exit 1; }
    derived=$(printf '%s' "$input" | jq "$open_important")
    supplied=$(printf '%s' "$input" | jq -r .important_open)
    [[ "$derived" == "$supplied" ]] \
      || { echo "important_open is $supplied but the findings hold $derived open Important finding(s)" >&2; exit 1; }
    printf '%s' "$input" | jq --arg key "$key" --arg branch "$branch" --arg head "$(git -C "$root" rev-parse HEAD)" \
      --arg base "$base" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      '. + {key:$key, branch:$branch, head:$head, base:$base, reviewed_at:$at}' \
      | write_marker_atomically "$(marker_path "$key")"
    echo "$key" > "$branch_file"
    echo "marked $key (round $(printf '%s' "$input" | jq -r .round), important_open $derived)"
    ;;
  decline)
    load_state || fail_base 1
    id="${2:?finding id}"; why="${3:?reason}"
    key=$(diff_key); f=$(marker_path "$key")
    [[ -f "$f" ]] || { echo "no review marker for the current change; run /review first" >&2; exit 1; }
    jq -e --arg id "$id" '.findings[] | select(.id == $id and .tag == "Important" and (.declined // false | not))' "$f" >/dev/null \
      || { echo "no open Important finding with id $id" >&2; exit 1; }
    jq --arg id "$id" --arg why "$why" "
      (.findings[] | select(.id == \$id)) |= (. + {declined: true, declined_reason: \$why})
      | .important_open = ($open_important)
    " "$f" | write_marker_atomically "$f"
    echo "declined $id; important_open now $(jq -r .important_open "$f")"
    ;;
  gate)
    input=$(cat)
    case "$input" in *push*) ;; *) exit 0 ;; esac
    command -v jq >/dev/null || refuse "jq is required to check the review state and is not on PATH"
    command=$(printf '%s' "$input" | jq -r '.tool_input.command // empty') || refuse "could not read the hook payload"
    [[ -n "$command" ]] || exit 0
    trap 'echo "REVIEW GATE: internal error while checking the review state, refusing the push" >&2; exit 2' ERR
    collect_pushes "$command"
    [[ ${#push_dirs[@]} -gt 0 ]] || exit 0
    for n in "${!push_dirs[@]}"; do
      if [[ "${push_skips[$n]}" == "1" ]]; then
        echo "review gate bypassed for this push (CDP_SKIP_REVIEW=1)" >&2
        continue
      fi
      check_push "${push_dirs[$n]}" "${push_args[$n]}"
    done
    exit 0
    ;;
  -h | --help) usage; exit 0 ;;
  *) echo "unknown command: $cmd" >&2; usage; exit 1 ;;
esac
