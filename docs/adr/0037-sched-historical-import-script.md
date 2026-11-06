# ADR-0037: Import past Sched events with a re-runnable crowd.dev script

**Date**: 2026-11-04
**Status**: proposed
**Deciders**: Joana Maia (pending review by a CDP teammate)

## Context

LF events move from Sched to Sessionize in 2027, so CDP will not build an ongoing Sched connector, but the history of LF's 112 Sched events is still wanted for identity resolution and company data ([spec](https://linuxfoundation-dxwx.dsp.so/4hh3fAU8-sched-connector-scope)). Every Sched event is its own site with its own API key, the API allows under 30 calls a minute and revokes keys that go over, and the largest registration call returns 48–56 MB in one unpaged response. Neither ingestion path we have fits: Sched data is not in Snowflake, and the v2 connector framework runs on a cadence with one environment credential per platform ([`credentials.ts`](../../services/libs/connectors/src/credentials.ts)). CDP runs on Oracle Cloud, but nothing in crowd.dev reads from Oracle Vault yet.

## Decision

Import Sched history with a one-time, re-runnable crowd.dev script that reads each event from the Sched API and sends activities to the data sink with `DataSinkWorkerEmitter.createAndProcessActivityResult`, the path the Snowflake connectors use; from there activities and relations reach Tinybird and Snowflake as described in [activity-pipeline.md](../design/activity-pipeline.md). Events and their CDP projects live in a reviewed mapping file in the repo, and each event's key is an Oracle Vault secret named `sched-<subdomain>`, read at run time by the job's own identity and deleted after the import. Source ids include the event (`{subdomain}:{username}:{sessionKey}` for `registered-session`, `{subdomain}:speaker:{username}:{sessionId}` for `spoke-at-session`) and the channel is the subdomain, so a re-run updates activities in place as long as timestamp, type, source id, channel and segment stay the same.

## Alternatives Considered

### Load Sched into Snowflake, then add a `sched` source to `snowflake_connectors`

- **Pros**: reuses the source query, transformer and project resolver; the raw data stays in the warehouse.
- **Cons**: needs a separate loader into Snowflake first; exports run daily and only pick up rows since the last job, and the manual trigger runs every enabled platform with no filters.
- **Why not**: two systems to build for a one-off load, filtered re-runs are awkward, and keeping a raw copy was ruled out.

### A v2 `sched` connector, run once

- **Pros**: reuses the framework's run tracking, watermarks and `emit()`.
- **Cons**: needs per-event credential storage the framework doesn't have, and it has no run-once mode: units reschedule on their cadence, so a one-off means seeding units and pausing them by hand.
- **Why not**: the largest build, for a connector we decided not to keep.

### Keys in a Kubernetes secret or AWS Secrets Manager, mounted as a file

- **Pros**: a familiar mount; `lfx-secrets-management` already delivers to AWS ([ADR-0016](./0016-akrites-cdp-public-api-authentication.md)).
- **Cons**: CDP runs on Oracle Cloud; all 112 keys would sit in one hand-edited file for the life of the job.
- **Why not**: Oracle Vault sits where CDP runs, takes keys one at a time as they are collected, and lets the job read each key by identity without writing it to disk.

## Consequences

### Positive

- No new long-running service: deduplication, member matching by verified email and email-domain organizations work as they do for every source.
- Runs can be filtered by event, date range and data type, with a key check, a dry run and per-event resume; repeated runs update instead of duplicating.
- The first Oracle Vault use gives CDP a pattern for per-integration secrets, which the connector framework lacks today.

### Negative

- A new `sched` platform: `PlatformType` entry, activity-type migration and scores.
- Progress tracking, pacing and streaming the 50 MB response are the script's own code.
- We own the Vault setup (vault, access policy, workload identity for the job).
- Re-running after 2027 depends on Sched sites and keys still working; no raw copy is kept.

### Risks

- Going over the rate limit revokes the key, and its scope (key, event or IP) is unknown: pace all keys as one budget (12 calls a minute, as in the POC) and stop on the first 429.
- Bad keys return HTTP 200 with `ERR: Authorization failed.`: check the body, not only the status.
- `site/sync` echoes the API key in its response: never log raw responses.
- A timestamp that changes between runs, or an event moved to another project, creates duplicates: the registration-time choice is still an open product question.
- Read/Write keys can change event data: the script calls read endpoints only, and secrets are deleted after the import.
- Organizations and the product sign-off (key type, activity types and scores, registration time, project per event) are open with Jonathan and Nirav; this ADR covers the technical approach only.
