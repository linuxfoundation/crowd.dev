# crowdmail — Mailing List Integration

Ingests public mailing lists (e.g. lore.kernel.org / LKML-style) into CDP,
following the same worker pattern as `services/apps/git_integration`:
FastAPI lifespan + async poll worker + asyncpg + aiokafka.

Status: hardcoded lists only, no onboarding UI (CM-1318). See the plan/step
tracker referenced in the PR for build progress.

## Architecture (planned)

- `worker/list_worker.py` polls `mailinglist.listProcessing` for lists to
  process, mirrors the list via `public-inbox-clone`/`public-inbox-fetch`,
  parses new messages, writes activities to `integration.results`, and emits
  Kafka messages to the `data-sink-worker` topic — same plumbing as
  git_integration, with `platform=mailinglist`.
- `services/mirror/` wraps the external `public-inbox` CLI (Perl tool,
  installed system-wide, not vendored).
- `services/parse/` is the ported email parser (originally `noteren.py`),
  producing crowd.dev-shaped activity payloads.
- `services/queue/` is the Kafka producer.

## Running locally

```bash
make run          # from services/apps/mailing_list_integration
```

This builds `scripts/services/docker/Dockerfile.mailing_list_integration` (installs
`public-inbox` for mirroring) and starts `mailing-list-integration-dev` via
`scripts/services/mailing-list-integration.yaml`, exposing the FastAPI health
endpoint on host port `8086`.

Other targets: `make lint`, `make format`, `make test`, `make rebuild`. See
`make help` for the full list.

## Onboarding a list (dev, hardcoded)

There is no UI yet — a list is onboarded by inserting its rows directly.
`dev/seed.sql` does this for one example list (`lore.kernel.org/git`):

```bash
psql "$CROWD_DB_WRITE_URI" -f dev/seed.sql
```

It creates a `public.integrations` row (`platform='mailinglist'`), a
`mailinglist.lists` row pointing at an existing segment, and a
`mailinglist."listProcessing"` row in state `pending` — which the worker's
`acquire_onboarding_list()` will pick up on its next poll. Edit the `name`
and `"sourceUrl"` values in the file to onboard a different lore list.

## End-to-end verification

1. Apply `backend/src/database/migrations/V1784048135__mailinglist-schema.sql`
   and run `dev/seed.sql` against the target Postgres.
2. Start Kafka and a `data_sink_worker` consumer on the `data-sink-worker` topic.
3. `make run` (or `uv run uvicorn crowdmail.server:app`) — worker polls, mirrors
   the seeded list via `public-inbox-clone`, parses new commits, and:
   - inserts rows into `integration.results` with `state='pending'` and
     `data.type='activity'`,
   - emits one Kafka message per result (`process_integration_result`,
     `platform=mailinglist`),
   - advances `mailinglist."listProcessing"."lastProcessedHeads"` and sets
     `state='completed'`.
4. Confirm `data_sink_worker` consumes the message, resolves the integration's
   `platform='mailinglist'`, and creates the member (email-based verified
   `username`+`email` identities) and activity.
5. `GET http://localhost:8086/` returns the FastAPI health check.

## Out of scope (CM-1318)

- Onboarding UI to create `integrations` + `mailinglist.lists` rows (the
  `mailing-list-connect` backend endpoint exists; `dev/seed.sql` remains
  useful for onboarding lists without going through the UI).
- Whether `member_join`/`member_leave` activities are derivable from lore, or
  only `message` initially.
