#!/usr/bin/env bash
set -euo pipefail

: "${SLACK_WEBHOOK_URL:?}"
: "${RUN_URL:?}"
: "${EVENT_NAME:?}"
: "${ALERT_TITLE:?}"

failed_step() {
  local pairs=(
    "${OUTCOME_RESOLVE_TAG}:Resolve deploy image tag"
    "${OUTCOME_DEPLOY}:Deploy api-e2e"
    "${OUTCOME_HEALTH}:Wait for api-e2e service to be ready"
    "${OUTCOME_E2E}:Run e2e tests"
  )
  local pair outcome label
  for pair in "${pairs[@]}"; do
    outcome="${pair%%:*}"
    label="${pair#*:}"
    if [[ "$outcome" == "failure" ]]; then
      printf '%s\n' "$label"
      return
    fi
  done
  printf 'unknown\n'
}

read_summary() {
  local path="${E2E_SUMMARY_PATH:-e2e.summary}"
  results=""
  failures=""

  [[ -f "$path" ]] || return 0

  local passed failed
  passed="$(awk -F= '/^passed=/{print $2; exit}' "$path")"
  failed="$(awk -F= '/^failed=/{print $2; exit}' "$path")"
  if [[ -n "${passed}" || -n "${failed}" ]]; then
    results="Passed: ${passed:-?} / Failed: ${failed:-?}"
  fi
  failures="$(awk '/^FAIL /{print; if (++n == 10) exit}' "$path")"
}

sha_short="${DEPLOY_TAG:-unknown}"
sha_short="${sha_short:0:12}"

read_summary

lines=(
  ":rotating_light: *${ALERT_TITLE}*"
  "*Event:* \`${EVENT_NAME}\`"
  "*Failed step:* \`$(failed_step)\`"
  "*Deploy SHA:* \`${sha_short}\`"
  "*Run:* <${RUN_URL}|View run>"
)

[[ -n "${results}" ]] && lines+=("*Results:* ${results}")

if [[ -n "${failures}" ]]; then
  lines+=("*Failed tests:*" $'```\n'"${failures}"$'\n```')
fi

text="$(printf '%s\n' "${lines[@]}")"
payload="$(jq -n --arg text "$text" '{text: $text}')"

curl -fsS -X POST "$SLACK_WEBHOOK_URL" \
  -H 'Content-Type: application/json' \
  -d "$payload"
