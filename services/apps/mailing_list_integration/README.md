# crowdmail — Mailing List Integration

Ingests public mailing lists (e.g. lore.kernel.org / LKML-style) into CDP,
following the same worker pattern as `services/apps/git_integration`:
FastAPI lifespan + async poll worker + asyncpg + aiokafka.

Status: under construction (CM-1318). See the plan/step tracker referenced
in the PR for build progress.

## Architecture (planned)

- `worker/list_worker.py` polls `mailinglist.listProcessing` for lists to
  process, mirrors the list via `public-inbox-clone`/`public-inbox-fetch`,
  parses new messages, writes activities to `integration.results`, and emits
  Kafka messages to the `data-sink-worker` topic — same plumbing as
  git_integration, with `platform=groupsio`.
- `services/mirror/` wraps the external `public-inbox` CLI (Perl tool,
  installed system-wide, not vendored).
- `services/parse/` is the ported email parser (originally `noteren.py`),
  producing crowd.dev-shaped activity payloads.
- `services/queue/` is the Kafka producer.

## Running locally

TODO — filled in once the worker (Step 6) and Dockerfile (Step 7) land.
