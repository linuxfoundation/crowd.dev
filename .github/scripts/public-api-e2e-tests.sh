#!/usr/bin/env bash
# End-to-end tests for the Public API (scheduled regression suite).
#
# The suite seeds its own test data and communicates with the API over HTTP only.
# Database setup/reset is handled externally.
#
# Required environment variables (same names as GitHub Actions secrets/vars):
#   AUTH0_STAGING_AUDIENCE
#   AUTH0_STAGING_ISSUER
#   AUTH0_STAGING_API_E2E_CLIENT_ID
#   AUTH0_STAGING_API_E2E_CLIENT_SECRET
#   CDP_API_E2E_BASE_URL
#
# Do not enable `set -x`; requests include sensitive credentials.
#
# Structure:
#   - Shared helpers (api, check, require)
#   - Seed data
#   - One test suite per resource
#
# Each test case is an api call followed by check. Stateful suites are a short
# workflow. Register new suites in main.

set -euo pipefail

if [[ $- == *x* ]]; then
  echo "error: refusing to run with xtrace (set -x); credentials would leak" >&2
  exit 1
fi

for cmd in curl jq; do
  command -v "$cmd" >/dev/null || {
    echo "error: '$cmd' is required" >&2
    exit 1
  }
done

: "${AUTH0_STAGING_API_E2E_CLIENT_ID:?set AUTH0_STAGING_API_E2E_CLIENT_ID}"
: "${AUTH0_STAGING_API_E2E_CLIENT_SECRET:?set AUTH0_STAGING_API_E2E_CLIENT_SECRET}"
: "${AUTH0_STAGING_AUDIENCE:?set AUTH0_STAGING_AUDIENCE}"
: "${AUTH0_STAGING_ISSUER:?set AUTH0_STAGING_ISSUER}"
: "${CDP_API_E2E_BASE_URL:?set CDP_API_E2E_BASE_URL}"

VERIFIED_BY="${VERIFIED_BY:-jordan.lee+e2e@example.com}"
RUN_ID="${GITHUB_RUN_ID:-$(date +%s)}-${RANDOM}"
SOURCE="lfxOne-api-e2e"

PASS=0
FAIL=0
FAILURE_LINES=()
TOKEN=""
HTTP_CODE=""
BODY=""

die() {
  echo "error: $*" >&2
  exit 1
}

CURL_CONNECT_TIMEOUT=10
CURL_MAX_TIME=30

api() {
  local version=$1 method=$2 path=$3 body=${4-}
  local -a args=(-sS --connect-timeout "$CURL_CONNECT_TIMEOUT" --max-time "$CURL_MAX_TIME"
    -w '\n%{http_code}' -X "$method" "${CDP_API_E2E_BASE_URL}/${version}${path}"
    -H 'Content-Type: application/json'
    -H "Authorization: Bearer ${TOKEN}")
  [[ -n $body ]] && args+=(-d "$body")

  local response
  response="$(curl "${args[@]}")"
  HTTP_CODE="$(printf '%s' "$response" | tail -n1)"
  BODY="$(printf '%s' "$response" | sed '$d')"
}

# Print a short, non-secret snippet of the last res on failure (never tokens).
log_fail_body() {
  local snippet
  snippet="$(jq -c 'if type == "object" then del(.access_token, .refresh_token, .id_token, .client_secret) else . end' <<<"$BODY" 2>/dev/null || printf '%s' "$BODY")"
  snippet="${snippet:0:500}"
  printf '  body: %s\n' "$snippet"
}

record_failure() {
  local line=$1
  FAILURE_LINES+=("$line")
  printf '  %s\n' "$line"
}

check() {
  local name=$1 expected=$2
  shift 2
  if [[ $HTTP_CODE != "$expected" ]]; then
    record_failure "FAIL  ${name} — expected HTTP ${expected}, got ${HTTP_CODE}"
    log_fail_body
    FAIL=$((FAIL + 1))
    return
  fi
  local expr
  for expr in "$@"; do
    if ! jq -e "$expr" >/dev/null 2>&1 <<<"$BODY"; then
      record_failure "FAIL  ${name} (${expr})"
      log_fail_body
      FAIL=$((FAIL + 1))
      return
    fi
  done
  printf '  PASS  %s (HTTP %s)\n' "$name" "$HTTP_CODE"
  PASS=$((PASS + 1))
}

write_summary() {
  local path="${E2E_SUMMARY_PATH:-}"
  [[ -n "$path" ]] || return 0
  {
    printf 'passed=%s\n' "$PASS"
    printf 'failed=%s\n' "$FAIL"
    printf '%s\n' "${FAILURE_LINES[@]+"${FAILURE_LINES[@]}"}"
  } >"$path"
}

require() {
  local expected=$1 name=$2
  [[ $HTTP_CODE == "$expected" ]] || die "$name — expected HTTP $expected, got $HTTP_CODE"
}

json() {
  jq -n "$@"
}

# Soft-read a field from response BODY
body_field() {
  jq -r --arg k "$1" 'if type == "object" then .[$k] // empty else empty end' <<<"$BODY" 2>/dev/null || true
}

fetch_token() {
  local response
  local issuer="${AUTH0_STAGING_ISSUER%/}"
  response="$(curl -sS --connect-timeout "$CURL_CONNECT_TIMEOUT" --max-time "$CURL_MAX_TIME" \
    -X POST "${issuer}/oauth/token" \
    -H 'Content-Type: application/json' \
    -d "$(json \
      --arg client_id "$AUTH0_STAGING_API_E2E_CLIENT_ID" \
      --arg client_secret "$AUTH0_STAGING_API_E2E_CLIENT_SECRET" \
      --arg audience "$AUTH0_STAGING_AUDIENCE" \
      '{grant_type:"client_credentials",client_id:$client_id,client_secret:$client_secret,audience:$audience}')")"

  TOKEN="$(jq -r '.access_token // empty' <<<"$response")"
  [[ -n $TOKEN ]] || die "Auth0 token failed: $(jq -c '{error,error_description}' <<<"$response" 2>/dev/null || echo unavailable)"
}

# ── fixtures ─────────────────────────────────────────────────────────────────

ACME_NAME="Acme-${RUN_ID}"
ACME_DOMAIN="acme-${RUN_ID}.example.com"
ACME_ID=""

GLOBEX_NAME="Globex-${RUN_ID}"
GLOBEX_DOMAIN="globex-${RUN_ID}.example.com"
GLOBEX_ID=""

PERSON_NAME="Jordan Lee"
PERSON_LFID="jordan.lee-${RUN_ID}"
PERSON_ID=""

seed() {
  echo "=== seed (${RUN_ID}) ==="

  api v1 POST /organizations "$(json \
    --arg name "$ACME_NAME" --arg domain "$ACME_DOMAIN" --arg source "$SOURCE" \
    '{name:$name, domain:$domain, source:$source}')"
  require 201 "create Acme"
  ACME_ID="$(jq -r '.id' <<<"$BODY")"

  api v1 POST /organizations "$(json \
    --arg name "$GLOBEX_NAME" --arg domain "$GLOBEX_DOMAIN" --arg source "$SOURCE" \
    '{name:$name, domain:$domain, source:$source}')"
  require 201 "create Globex"
  GLOBEX_ID="$(jq -r '.id' <<<"$BODY")"

  api v1 POST /members "$(json \
    --arg lfid "$PERSON_LFID" --arg by "$VERIFIED_BY" --arg name "$PERSON_NAME" '{
      displayName: $name,
      identities: [{
        value: $lfid,
        platform: "lfid",
        type: "username",
        source: "lfxOne",
        verified: true,
        verifiedBy: $by
      }]
    }')"
  require 201 "create Jordan"
  PERSON_ID="$(jq -r '.memberId' <<<"$BODY")"
  [[ -n $PERSON_ID && $PERSON_ID != null ]] || die "create Jordan returned no memberId"

  echo "  acme=$ACME_ID"
  echo "  globex=$GLOBEX_ID"
  echo "  person=$PERSON_ID"
}

# ── /organizations ───────────────────────────────────────────────────────────

suite_organizations() {
  echo
  echo "=== /organizations ==="

  local name_q
  name_q="$(jq -nr --arg n "$ACME_NAME" '$n|@uri')"

  api v1 GET "/organizations?domain=${ACME_DOMAIN}"
  check "GET find Acme by domain" 200 \
    ".id == \"$ACME_ID\"" \
    ".name == \"$ACME_NAME\""

  api v1 GET "/organizations?name=${name_q}"
  check "GET find Acme by name" 200 ".id == \"$ACME_ID\""

  api v1 GET "/organizations?name=${name_q}&domain=${ACME_DOMAIN}"
  check "GET find Acme by name+domain" 200 ".id == \"$ACME_ID\""

  api v1 GET "/organizations?name=${name_q}&domain=example.com"
  check "GET name/domain mismatch" 404

  api v1 GET "/organizations"
  check "GET missing params" 400

  api v1 GET "/organizations?domain=missing-${RUN_ID}.example.com"
  check "GET unknown domain" 404

  api v1 GET "/organizations?domain=not-a-valid-domain"
  check "GET invalid domain" 400

  api v1 POST /organizations "$(json \
    --arg name "$ACME_NAME" --arg domain "$ACME_DOMAIN" --arg source "$SOURCE" \
    '{name:$name, domain:$domain, source:$source}')"
  check "POST recreate Acme is idempotent" 201 ".id == \"$ACME_ID\""

  api v1 POST /organizations "$(json \
    --arg name "Bad-${RUN_ID}" --arg source "$SOURCE" \
    '{name:$name, domain:"not-a-valid-domain", source:$source}')"
  check "POST reject invalid domain" 400
}

# ── /members ─────────────────────────────────────────────────────────────────

suite_members() {
  echo
  echo "=== /members ==="

  local sam_lfid="sam.rivera-${RUN_ID}"
  local sam_id

  api v1 POST /members "$(json \
    --arg lfid "$sam_lfid" --arg by "$VERIFIED_BY" '{
      displayName: "Sam Rivera",
      identities: [{
        value: $lfid,
        platform: "lfid",
        type: "username",
        source: "lfxOne",
        verified: true,
        verifiedBy: $by
      }]
    }')"
  check "POST create Sam" 201 'has("memberId")'
  sam_id="$(body_field memberId)"

  api v1 POST /members "$(json '{displayName:"No Identities", identities:[]}')"
  check "POST reject empty identities" 400

  api v1 POST /members/resolve "$(json --arg lfid "$PERSON_LFID" '{lfids:[$lfid]}')"
  check "POST resolve Jordan by lfid" 200 ".memberId == \"$PERSON_ID\""

  api v1 POST /members/resolve "$(json --arg lfid "$sam_lfid" '{lfids:[$lfid]}')"
  check "POST resolve Sam by lfid" 200 ".memberId == \"$sam_id\""

  api v1 POST /members/resolve "$(json --arg lfid "nobody-${RUN_ID}" '{lfids:[$lfid]}')"
  check "POST resolve unknown lfid" 404

  api v1 POST /members/resolve "$(json '{lfids:[]}')"
  check "POST resolve reject empty lfids" 400
}

# ── /members/:id/identities ──────────────────────────────────────────────────

suite_member_identities() {
  echo
  echo "=== /members/:id/identities ==="

  local email="jordan.lee+${RUN_ID}@example.com"
  local identity_id

  api v1 GET "/members/${PERSON_ID}/identities"
  check "GET lists seeded lfid" 200 \
    '.identities | type == "array"' \
    ".identities | map(select(.platform == \"lfid\" and .value == \"$PERSON_LFID\")) | length == 1"

  api v1 POST "/members/${PERSON_ID}/identities" "$(json \
    --arg email "$email" --arg by "$VERIFIED_BY" '{
      value: $email,
      platform: "email",
      type: "email",
      source: "lfxOne",
      verified: false,
      verifiedBy: $by
    }')"
  check "POST add email identity" 201 \
    ".value == \"$email\"" \
    '.verified == false'
  identity_id="$(body_field id)"

  api v1 POST "/members/${PERSON_ID}/identities" "$(json \
    --arg email "$email" '{
      value: $email,
      platform: "email",
      type: "email",
      source: "lfxOne",
      verified: false
    }')"
  check "POST same identity is idempotent" 200 ".id == \"$identity_id\""

  api v1 GET "/members/${PERSON_ID}/identities"
  check "GET includes email" 200 \
    ".identities | map(select(.id == \"$identity_id\")) | length == 1"

  api v1 PATCH "/members/${PERSON_ID}/identities/${identity_id}" \
    "$(json --arg by "$VERIFIED_BY" '{verified:true, verifiedBy:$by}')"
  check "PATCH verify email" 200 '.verified == true'

  api v1 PATCH "/members/${PERSON_ID}/identities/${identity_id}" \
    "$(json --arg by "$VERIFIED_BY" '{verified:false, verifiedBy:$by}')"
  check "PATCH reject email deletes unused identity" 204

  api v1 GET "/members/${PERSON_ID}/identities"
  check "GET email gone after reject" 200 \
    ".identities | map(select(.id == \"$identity_id\")) | length == 0"
}

# ── /members/:id/work-experiences ────────────────────────────────────────────

suite_member_work_experiences() {
  echo
  echo "=== /members/:id/work-experiences ==="

  local acme_we globex_we doomed_we

  api v1 POST "/members/${PERSON_ID}/work-experiences" "$(json \
    --arg org "$ACME_ID" --arg by "$VERIFIED_BY" '{
      organizationId: $org,
      jobTitle: "Platform Engineer",
      verified: false,
      verifiedBy: $by,
      source: "lfxOne",
      startDate: "2024-01-01T00:00:00.000Z",
      endDate: "2024-12-31T00:00:00.000Z"
    }')"
  check "POST create Acme stint" 201 \
    'has("id") and (has("workExperiences") | not)' \
    ".organizationName == \"$ACME_NAME\"" \
    ".organizationDomains | index(\"$ACME_DOMAIN\") != null"
  acme_we="$(body_field id)"

  api v1 POST "/members/${PERSON_ID}/work-experiences" "$(json \
    --arg org "$GLOBEX_ID" --arg by "$VERIFIED_BY" '{
      organizationId: $org,
      jobTitle: "Software Engineer",
      verified: false,
      verifiedBy: $by,
      source: "lfxOne",
      startDate: "2020-01-01T00:00:00.000Z",
      endDate: "2022-06-01T00:00:00.000Z"
    }')"
  check "POST create Globex stint" 201
  globex_we="$(body_field id)"

  api v1 POST "/members/${PERSON_ID}/work-experiences" "$(json \
    --arg org "$GLOBEX_ID" --arg by "$VERIFIED_BY" '{
      organizationId: $org,
      jobTitle: "ignored title",
      verified: false,
      verifiedBy: $by,
      source: "email-domain",
      startDate: "2021-01-01T00:00:00.000Z",
      endDate: "2023-01-01T00:00:00.000Z"
    }')"
  check "POST overlapping Globex email-domain row" 201

  api v1 GET "/members/${PERSON_ID}/work-experiences"
  check "GET groups to two visible rows" 200 \
    '.workExperiences | length == 2' \
    ".workExperiences[] | select(.id == \"$globex_we\") | .jobTitle == \"Software Engineer\"" \
    ".workExperiences[] | select(.id == \"$globex_we\") | .source | test(\"lfxOne\")" \
    ".workExperiences[] | select(.id == \"$globex_we\") | .source | test(\"email-domain\")" \
    ".workExperiences[] | select(.id == \"$globex_we\") | .startDate | startswith(\"2020-01-01\")" \
    ".workExperiences[] | select(.id == \"$globex_we\") | .endDate | startswith(\"2023-01-01\")" \
    ".workExperiences[] | select(.organizationId == \"$GLOBEX_ID\") | .id == \"$globex_we\""

  api v1 PUT "/members/${PERSON_ID}/work-experiences/${globex_we}" "$(json \
    --arg org "$GLOBEX_ID" --arg by "$VERIFIED_BY" '{
      organizationId: $org,
      jobTitle: "Senior Software Engineer",
      verified: false,
      verifiedBy: $by,
      source: "lfxOne",
      startDate: "2020-01-01T00:00:00.000Z",
      endDate: "2022-06-01T00:00:00.000Z"
    }')"
  check "PUT update Globex title" 200 '.jobTitle == "Senior Software Engineer"'

  api v1 GET "/members/${PERSON_ID}/work-experiences"
  check "GET shows updated title and submitted dates" 200 \
    ".workExperiences[] | select(.id == \"$globex_we\") | .jobTitle == \"Senior Software Engineer\"" \
    ".workExperiences[] | select(.id == \"$globex_we\") | .endDate | startswith(\"2022-06-01\")" \
    ".workExperiences[] | select(.id == \"$globex_we\") | .source | test(\"email-domain\") | not"

  api v1 PATCH "/members/${PERSON_ID}/work-experiences/${globex_we}" \
    "$(json --arg by "$VERIFIED_BY" '{verified:true, verifiedBy:$by}')"
  check "PATCH verify Globex" 200 \
    '.verified == true' \
    ".id == \"$globex_we\""

  api v1 GET "/members/${PERSON_ID}/work-experiences"
  check "GET shows Globex verified" 200 \
    ".workExperiences[] | select(.id == \"$globex_we\") | .verified == true"

  api v1 PATCH "/members/${PERSON_ID}/work-experiences/${acme_we}" \
    "$(json --arg by "$VERIFIED_BY" '{verified:false, verifiedBy:$by}')"
  check "PATCH reject Acme" 200 '.verified == false'

  api v1 GET "/members/${PERSON_ID}/work-experiences"
  check "GET reject hides Acme" 200 \
    '.workExperiences | length == 1' \
    ".workExperiences[0].id == \"$globex_we\""

  api v1 PATCH "/members/${PERSON_ID}/work-experiences/${globex_we}" \
    "$(json --arg by "$VERIFIED_BY" '{verified:false, verifiedBy:$by}')"
  check "PATCH reject Globex" 200

  api v1 GET "/members/${PERSON_ID}/work-experiences"
  check "GET all rejects leave empty list" 200 '.workExperiences | length == 0'

  api v1 POST "/members/${PERSON_ID}/work-experiences" "$(json \
    --arg org "$ACME_ID" --arg by "$VERIFIED_BY" '{
      organizationId: $org,
      jobTitle: "Contractor",
      verified: false,
      verifiedBy: $by,
      source: "lfxOne",
      startDate: "2025-01-01T00:00:00.000Z",
      endDate: "2025-06-01T00:00:00.000Z"
    }')"
  check "POST create stint for delete" 201
  doomed_we="$(body_field id)"

  api v1 DELETE "/members/${PERSON_ID}/work-experiences/${doomed_we}" \
    "$(json --arg by "$VERIFIED_BY" '{deletedBy: $by}')"
  check "DELETE work experience" 204

  api v1 GET "/members/${PERSON_ID}/work-experiences"
  check "GET empty after delete" 200 '.workExperiences | length == 0'

  api v1 POST "/members/${PERSON_ID}/work-experiences" "$(json \
    --arg org "$GLOBEX_ID" --arg by "$VERIFIED_BY" '{
      organizationId: $org,
      jobTitle: "hidden row",
      verified: false,
      verifiedBy: $by,
      source: "email-domain",
      startDate: "2019-01-01T00:00:00.000Z",
      endDate: "2024-01-01T00:00:00.000Z"
    }')"
  check "POST create hidden Globex email-domain row first" 201

  api v1 POST "/members/${PERSON_ID}/work-experiences" "$(json \
    --arg org "$GLOBEX_ID" --arg by "$VERIFIED_BY" '{
      organizationId: $org,
      jobTitle: "Platform Architect",
      verified: false,
      verifiedBy: $by,
      source: "lfxOne",
      startDate: "2020-01-01T00:00:00.000Z",
      endDate: "2022-06-01T00:00:00.000Z"
    }')"
  check "POST create authoritative Globex stint drops hidden row" 201 \
    '.jobTitle == "Platform Architect"' \
    '.endDate | startswith("2022-06-01")' \
    '.source | test("email-domain") | not'

  api v1 GET "/members/${PERSON_ID}/work-experiences"
  check "GET shows submitted dates and no hidden source" 200 \
    '.workExperiences | length == 1' \
    ".workExperiences[0].jobTitle == \"Platform Architect\"" \
    '.workExperiences[0].endDate | startswith("2022-06-01")' \
    '.workExperiences[0].source | test("email-domain") | not'
}

# ── /members/:id/maintainer-roles ────────────────────────────────────────────

suite_member_maintainer_roles() {
  echo
  echo "=== /members/:id/maintainer-roles ==="

  api v1 GET "/members/${PERSON_ID}/maintainer-roles"
  check "GET empty roles for fresh member" 200 \
    '.maintainerRoles | type == "array"' \
    '.maintainerRoles | length == 0'
}

# ── /members/:id/project-affiliations ────────────────────────────────────────
# PATCH needs a project the member already belongs to (activity-backed). Without
# that fixture we only cover GET + unknown-project 404.

suite_member_project_affiliations() {
  echo
  echo "=== /members/:id/project-affiliations ==="

  local missing_project="00000000-0000-4000-8000-000000000099"

  api v1 GET "/members/${PERSON_ID}/project-affiliations"
  check "GET empty affiliations for fresh member" 200 \
    '.projectAffiliations | type == "array"' \
    '.projectAffiliations | length == 0'

  api v1 PATCH "/members/${PERSON_ID}/project-affiliations/${missing_project}" \
    "$(json --arg by "$VERIFIED_BY" --arg org "$ACME_ID" '{
      verifiedBy: $by,
      affiliations: [{
        organizationId: $org,
        dateStart: "2024-01-01T00:00:00.000Z",
        dateEnd: null
      }]
    }')"
  check "PATCH unknown project" 404
}

main() {
  echo "Public API e2e tests"
  echo "  run:  $RUN_ID"
  echo "  base: $CDP_API_E2E_BASE_URL"
  echo

  fetch_token
  echo "Authenticated."

  seed
  suite_organizations
  suite_members
  suite_member_identities
  suite_member_work_experiences
  suite_member_maintainer_roles
  suite_member_project_affiliations

  echo
  echo "=== Results ==="
  printf 'Passed: %s\n' "$PASS"
  printf 'Failed: %s\n' "$FAIL"
  write_summary
  [[ $FAIL -eq 0 ]]
}

main "$@"
