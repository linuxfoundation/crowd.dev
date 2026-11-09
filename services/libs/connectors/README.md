# @crowd/connectors

The connector framework (integrations v2). This library holds one manifest per platform (`src/connectors/<platform>/`) and the framework around it: emit, HTTP client and token pool, credentials, registry. `services/apps/connectors_worker` dispatches and runs the syncs. Where it sits in CDP: [`docs/knowledge/sys-connectors.md`](../../../docs/knowledge/sys-connectors.md).

Read this before adding a connector, a sync, or anything that writes `integration.sync_units`.

## Why

The [Integration Builder v2 architecture](https://linuxfoundation-dxwx.dsp.so/CQLF4CI1-integration-builder-v2-nango-deprecation) ([lfx-cdp-insights-ops#28](https://github.com/linuxfoundation/lfx-cdp-insights-ops/issues/28)) gives every integration one model: a connector, an integration, sync units, runs and activities, with `integration.sync_units` holding one row per (channel, sync). The rules below keep it that way.

- **No mapping tables.** A connector that routes records through its own table rebuilds, outside `sync_units`, everything the framework already does per unit: find-or-create of integrations, locking, reconciliation, deactivation, backfill tracking. Each new connector would add another set.
- **Fan-out is an exception.** [ADR-0033](../../../docs/adr/0033-connector-emit-per-call-segment.md) lets one unit emit to many segments for Octolens, a single org-wide feed whose shared 500 requests/hour budget can't afford one poll per project. It is not a general pattern.
- **Channels never move in place.** The sink dedupes on (platform, type, `sourceId`, channel, segment, timestamp), so a record re-emitted under another segment becomes a second activity.
- **The dedupe key is exact.** The sink compares `channel` and `sourceId` with `!==`, and `activityRelations` in Tinybird is a `ReplacingMergeTree` sorted on (`segmentId`, `timestamp`, `type`, `platform`, `channel`, `sourceId`). A channel that differs only in letter case, or a `sourceId` rebuilt from a value that changed, is a different row in both, and Tinybird can't retract it (rule 20).

## Model

| Concept     | Where                                  | What it is                                                                                                                                                              |
| ----------- | -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Destination | `public.integrations`                  | One live row per (platform, subproject segment). Every activity lands on one.                                                                                           |
| Credential  | `getCredential(qx, integrationId)`     | Who we are at the provider. Resolved per platform, identical for every integration of that platform unless the provider issues per-install credentials.                 |
| Channel     | `sync_units.channelId` / `channelName` | The smallest scope the provider API can filter by (repo, template, board, …). `channelId` is the provider's stable id; `channelName` is the human-readable name or URL. |
| Sync        | `Manifest.syncs[]`                     | One kind of data pulled from a channel (issues, comments, badges, …), with its own cadence and watermark.                                                               |
| Sync unit   | `integration.sync_units`               | One row per (integration, channel, sync). An integration owns channels × syncs units. Unique on `(integrationId, channelId, syncName)`.                                 |

**The channel is the mapping.** A record goes to the integration and segment of the unit that fetched it. Onboarding a channel under an integration _is_ mapping it to that segment; there is nothing else to keep in step.

## Routing

1. Never add a table, a `settings` key or a cache that maps provider keys to integrations or segments. If you need one, the provider key is a channel: create units for it under the destination integration.
2. `sync_units` is the source of truth for what an integration syncs. `integration.settings` may mirror it for the UI, never drive it. GitHub's `settings` and `public.repositories` predate this rule and stay (Insights reads `repositories`); don't copy that pattern.
3. Call `emit()` without `segmentId`/`integrationId` overrides. Only a connector that declares `emitsOwnSegment` (one unit emitting to many segments, ADR-0033) passes them, on every call, and that is allowed only when the provider can't filter requests by channel, or a shared org-wide budget can't afford one poll per channel. It needs a segment-less anchor integration and sign-off before any code.
4. A channel has one owning integration per platform. In the transaction that creates the units, take `lockSyncUnitChannel`, check that no other live integration of the same platform owns the channel, and skip with a reason when one does. `hasActiveSyncUnitsForChannelElsewhere` is the GitHub check and only looks at `github-nango` integrations: give it a platform parameter before another connector uses it, don't copy it.
5. Moving a channel to another integration means decommissioning the old units and onboarding new ones (see `onboardGithubRepo`). Never rewrite `integrationId`, `channelId` or `segmentId` on units that have emitted.
6. Rules or review that decide _whether_ a channel is synced (matchers, overrides, a held queue) live outside the emit path. Their only output is units created or decommissioned; reviewer decisions belong in code or config, not in a routing table.

## Integrations

7. Connectors whose integrations nobody connects by hand are listed in `AUTO_MANAGED_INTEGRATION_PLATFORMS`; the backend refuses UI edits to them.
8. `public.integrations` has no unique key on (platform, segmentId). Find or create under a lock, reuse the platform's existing function, and never create a second live row for the same pair.
9. Integrations are created on subproject segments only. Resolve the segment and check it is a subproject before creating.

## Integrations and sync units change together

An integration and its units are one thing seen at two levels: the integration says where data lands, its units say what is fetched for it. Every write to one is matched by a write to the other in the same flow.

| Integration event                                   | Its sync units                                                          |
| --------------------------------------------------- | ----------------------------------------------------------------------- |
| Created or connected                                | Units onboarded for each of its channels                                |
| Channels added or removed (UI, discovery, resolver) | New channels onboarded, removed ones decommissioned                     |
| Soft-deleted                                        | All its units decommissioned (`decommissionAllSyncUnits`), then drained |
| Restored                                            | Its channels onboarded again                                            |

10. Never write one side alone: an integration change that leaves its units stale, or units created outside an integration's onboarding or lifecycle flow, is drift. `reconcileGithubIntegration` is the reference flow.
11. The dispatcher skips units whose integration is soft-deleted; that is a safety net, not the delete path. Deleting an integration still decommissions its units.
12. A live integration on the connectors pipeline owns units for every channel it syncs (any status but `decommissioned`). The only exception is a fan-out destination (rule 3), which owns none and is soft-deleted once nothing routes to it. Integrations still on the Nango or legacy pipelines are outside this rule until they switch.

## Credentials

13. Credentials identify the account, never the scope: no channel lists, filters or segment ids in a credential.
14. New credential kinds extend the `Credential` union in `types.ts`; syncs read them through `ctx.getCredential()` or the token pool (`preparePool` / `mintToken`), not from env directly.

## Sync units and lifecycle

15. Create units only through `upsertSyncUnits`. On an existing unit it keeps `emitEnabled`, status and schedule, rewrites `channelName`, and replaces the watermark when you pass a non-null one, so pass none unless you mean to reset progress. Set `emitEnabled` explicitly on insert; `false` means records go to `sync_shadow_records` and nothing reaches the sink.
16. Stop a unit by changing its status (`paused`, `dead_letter`, `decommissioned`), never by deleting it. A channel that disappears or moves is decommissioned; only the lifecycle drain (`deleteDrainedSyncUnits`) removes decommissioned units, once no run holds them.
17. How far back a first run goes is a deliberate per-sync choice (full history for GitHub, a lookback or `since` for feeds). Extra history later is a backfill carried in the unit's watermark, not a second unit.
18. Discovery (`Manifest.discover` or a lifecycle sweep) lists channels; it must page to the end, and a listing it can't finish must not decommission anything.

## Syncs

19. A sync is `{ name, cadenceMinutes, schema, run }`. `run` reads `ctx.watermark`, calls `ctx.commitWatermark` after each durable step, and returns `{ complete }`. Return `complete: false` before `ctx.hasRunBudget()` runs out; the next run resumes from the committed watermark.
20. Watermarks are per unit and must be safe to resume from at any committed point: re-emitting a record is fine, skipping one is not. The sink dedupes on (platform, type, `sourceId`, channel, segment, timestamp), so a record re-emitted to another segment, under another channel or with a changed timestamp becomes a duplicate activity. Every part of that key is case-sensitive and is also the sorting key of `activityRelations` in Tinybird, which can't retract a row once it lands. So:
    - Store `channelName` byte for byte as the channel the repo's existing activities carry. For GitHub that is the live `repositories.url`, the only channel the sink routed Nango records under; never lowercase it and never swap in the provider's own spelling (GitHub's `html_url` can differ in case). Most connector records carry no channel of their own and inherit `channelName` (`emit.ts`), so a unit whose casing differs duplicates every record it replays. Look channels up case-insensitively; store the value you found.
    - Build `sourceId` only from immutable provider ids. Synthetic ids that embed a mutable value, such as a GitHub login in `gen-CE_`, `gen-AE_` and `gen-RRE_`, produce a second activity for the same event after a rename, so a full-history replay over data another pipeline ingested duplicates those events.
    - Before replaying history that is already ingested (switching pipelines, re-onboarding, resetting a watermark), check both: the channel must match the stored activities byte for byte, and the replayed record types must have stable `sourceId`s.
21. Records are validated against the sync's Zod `schema` in `emit()`; a failing record is a `connector.code` error, not something to drop silently.
22. Make provider calls through `ctx.http` inside a sync so the token pool, budget probe and `interpretResponse` apply. Only a manifest without `preparePool`/`mintToken` (Octolens) calls its own client with `ctx.getCredential()`.
23. When a request filters by channel, assert every returned record belongs to that channel. Some providers silently ignore unknown filters and return the whole account.
24. Per-run shared state (DB lookups, caches) goes through `createDeps` (ADR-0034), built once per run; syncs don't open their own DB connections.

## Errors

25. Throw the `ConnectorError` subclasses from `http/errors.ts`; the class decides what happens:
    - `provider.rate_limit`, `provider.unavailable`: the unit is parked until `resumeAt` and keeps its committed progress. Pass `resumeAt` when the provider says when.
    - `provider.auth`: dead-letter after 5 consecutive failures; `provider.access_denied`: after 1.
    - `provider.contract`: the provider answered something we don't understand. Fail loudly; never coerce it into an empty result.
    - `connector.code`: our bug.
26. Never turn an error into a partial or empty success. A run that couldn't read everything says so (`complete: false` or an error), and nothing downstream may treat its output as complete.

## Before adding a connector or sync

- Pick the channel: what is the smallest scope the provider filters by, and is one poll per channel affordable within the provider's budget at the sync's cadence? If not, stop and get sign-off for a fan-out design.
- Register the manifest, add the platform to `PlatformType`, add activity types, and decide whether the platform is auto-managed.
- Write the onboarding path that creates units for a channel under its destination integration, and the lifecycle path that decommissions them.
