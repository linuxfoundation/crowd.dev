# ADR-0037: Import past Sched events with a re-runnable crowd.dev script

**Date**: 2026-11-04
**Status**: proposed
**Deciders**: Joana Maia (pending review by a CDP teammate)

## Context

LF events move from Sched to Sessionize in 2027, so CDP will not build an ongoing Sched connector, but the history of LF's past Sched events (658 that LF staff administer, 2016–2026) is still wanted for identity resolution and company data ([spec](https://linuxfoundation-dxwx.dsp.so/4hh3fAU8-sched-connector-scope)). Every Sched event is its own site with its own API key, shown only on that event's Control Panel; the API allows under 30 calls a minute and revokes keys that go over, and the largest registration call returns 48–56 MB in one unpaged response. Neither ingestion path we have fits: Sched data is not in Snowflake, and the v2 connector framework runs on a cadence with one environment credential per platform ([`credentials.ts`](../../services/libs/connectors/src/credentials.ts)).

## Decision

Import Sched history with a one-time, re-runnable crowd.dev script that reads each event from the Sched API and sends activities to the data sink with `DataSinkWorkerEmitter.createAndProcessActivityResult`, the path the Snowflake connectors use; from there activities and relations reach Tinybird and Snowflake as described in [activity-pipeline.md](../design/activity-pipeline.md), with events and their CDP projects in a reviewed mapping file in the repo. Each event's key is kept in one 1Password item, one concealed field per subdomain, and reaches the job only through a Kubernetes secret loaded for the run and deleted after it. Source ids include the event (`{subdomain}:{username}:{sessionKey}` for `registered-session`, `{subdomain}:speaker:{username}:{sessionId}` for `spoke-at-session`) and the channel is the subdomain, so a re-run updates activities in place as long as timestamp, type, source id, channel and segment stay the same.

## Alternatives Considered

### Load Sched into Snowflake, then add a `sched` source to `snowflake_connectors`

- **Pros**: reuses the source query, transformer and project resolver; the raw data stays in the warehouse.
- **Cons**: needs a separate loader into Snowflake first; exports run daily and only pick up rows since the last job, and the manual trigger runs every enabled platform with no filters.
- **Why not**: two systems to build for a one-off load, filtered re-runs are awkward, and keeping a raw copy was ruled out.

### A v2 `sched` connector, run once

- **Pros**: reuses the framework's run tracking, watermarks and `emit()`.
- **Cons**: needs per-event credential storage the framework doesn't have, and it has no run-once mode: units reschedule on their cadence, so a one-off means seeding units and pausing them by hand.
- **Why not**: the largest build, for a connector we decided not to keep.

### Keys in Oracle Vault, one secret per event

- **Pros**: sits where CDP runs; the job reads each key with its own identity, so no key is stored in the cluster.
- **Cons**: nothing in crowd.dev uses Oracle Vault yet, so the vault, access policy and workload identity would all be new, plus one secret per event.
- **Why not**: that setup outweighs a one-time import, and the team already keeps credentials in 1Password.

### The job reads 1Password directly with a service account

- **Pros**: no operator step and no Kubernetes secret, even for a short time.
- **Cons**: needs a 1Password admin to create the service account, and its token still has to reach the cluster.
- **Why not**: not available to the team today; a secret that lives only for the run gives the same result. The job reads a mounted secret either way, so switching later changes no code.

## Consequences

### Positive

- No new long-running service: deduplication, member matching by verified email and email-domain organizations work as they do for every source.
- Runs can be filtered by event, date range and data type, with a key check, a dry run and per-event resume; repeated runs update instead of duplicating.
- No new infrastructure for keys: they sit in the 1Password vault the team already uses, and they are in the cluster only while a run lasts.
- Keys are collected once by a browser script that reads each event's existing key from its Control Panel; it never generates or changes a key.

### Negative

- A new `sched` platform: `PlatformType` entry, activity-type migration and scores.
- Progress tracking, pacing and streaming the 50 MB response are the script's own code.
- Each run needs an operator with 1Password and cluster access to load the secret (piping `op item get` into `kubectl create secret`, so no key touches disk) and delete it afterwards.
- A new event's key has to be added to the 1Password item before it can be imported.
- Re-running after 2027 depends on Sched sites and keys still working; no raw copy is kept.

### Risks

- Going over the rate limit revokes the key, and its scope (key, event or IP) is unknown: pace all keys as one budget (12 calls a minute, as in the POC) and stop on the first 429.
- Bad keys return HTTP 200 with `ERR: Authorization failed.`: check the body, not only the status.
- `site/sync` echoes the API key in its response: never log raw responses.
- A timestamp that changes between runs, or an event moved to another project, creates duplicates: the registration-time choice is still an open product question.
- Read/Write keys can change event data: the script calls read endpoints only, the Kubernetes secret is deleted after each run, and the 1Password item is archived once the import is done.
- Organizations and the product sign-off (key type, activity types and scores, registration time, project per event) are open with Jonathan and Nirav; this ADR covers the technical approach only.
