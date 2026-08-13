# Load tests — `POST /members/resolve`

Validates that the endpoint handles 200 req/min with p99 latency < 2 s, matching the Auth0 `cdp_uuid` action's SLO.

## Prerequisites

```bash
brew install k6
```

## Required env vars

| Variable | Description |
|---|---|
| `AUTH0_TOKEN_URL` | Auth0 `/oauth/token` endpoint — e.g. `https://linuxfoundation.auth0.com/oauth/token` |
| `AUTH0_CLIENT_ID` | M2M client ID |
| `AUTH0_CLIENT_SECRET` | M2M client secret |
| `AUTH0_AUDIENCE` | API audience — staging: `https://lf-staging.crowd.dev/api/`, prod: `https://cm.lfx.dev/api/` |
| `API_BASE_URL` | Target base URL (default: `https://lf-staging.crowd.dev`) |

Optional:

| Variable | Description |
|---|---|
| `PROFILE` | `steady` (default), `burst`, or `soak` |
| `LFIDS_FILE` | Path to JSON file containing array of LFIDs (relative to script dir) — e.g. `tmp/lfids.json` |
| `ALLOW_PROD` | Set to `1` to enable production runs (requires coordination window) |

## Running

```bash
# Steady state — 200 req/min for 10 min (staging)
k6 run \
  -e AUTH0_TOKEN_URL=https://linuxfoundation.auth0.com/oauth/token \
  -e AUTH0_CLIENT_ID=... \
  -e AUTH0_CLIENT_SECRET=... \
  -e AUTH0_AUDIENCE=https://lf-staging.crowd.dev/api/ \
  backend/load-tests/members-resolve.k6.js

# With real LFIDs from staging DB (avoids shell arg limit)
k6 run \
  -e AUTH0_TOKEN_URL=https://linuxfoundation.auth0.com/oauth/token \
  -e AUTH0_CLIENT_ID=... \
  -e AUTH0_CLIENT_SECRET=... \
  -e AUTH0_AUDIENCE=https://lf-staging.crowd.dev/api/ \
  -e LFIDS_FILE=tmp/lfids.json \
  backend/load-tests/members-resolve.k6.js

# Burst — ramps to 400 req/min to confirm 429 behaviour
k6 run -e PROFILE=burst ... backend/load-tests/members-resolve.k6.js

# Soak — 200 req/min for 60 min (pool exhaustion / event-loop drift)
k6 run -e PROFILE=soak ... backend/load-tests/members-resolve.k6.js

# Production (requires explicit opt-in)
k6 run \
  -e API_BASE_URL=https://cm.lfx.dev \
  -e ALLOW_PROD=1 \
  -e AUTH0_AUDIENCE=https://cm.lfx.dev/api/ \
  ... backend/load-tests/members-resolve.k6.js
```

## Pass criteria

| Metric | Threshold |
|---|---|
| p95 latency (200 responses) | < 500 ms |
| p99 latency (200 responses) | < 2 000 ms |
| Error rate (non-200/404/409/429) | < 1 % |
| `rate_limited_429` during `steady` | Expected: 0 |
| `rate_limited_429` during `burst` | Expected: > 0 (limiter working) |

## LFID fixture

For realistic latency numbers, supply real LFIDs from staging:

```sql
-- run on staging replica, pipe output into tmp/lfids.json
SELECT json_agg(username)
FROM (SELECT username FROM "memberIdentities" WHERE platform = 'lfid' LIMIT 1000) t;
```

```bash
k6 run -e LFIDS_FILE=tmp/lfids.json ... backend/load-tests/members-resolve.k6.js
```

`backend/load-tests/tmp/` is gitignored.
