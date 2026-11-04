# ADR-0036: Connector routing — the sync unit's channel is the mapping

**Date**: 2026-11-04
**Status**: proposed
**Deciders**: Mouad BANI

## Context

Integration Builder v2 ([lfx-cdp-insights-ops#28](https://github.com/linuxfoundation/lfx-cdp-insights-ops/issues/28),
[architecture doc](https://linuxfoundation-dxwx.dsp.so/CQLF4CI1-integration-builder-v2-nango-deprecation),
[tech spec](https://linuxfoundation-dxwx.dsp.so/R446nfTq-integration-builder-tech-spec), POC
[#4463](https://github.com/linuxfoundation/crowd.dev/pull/4463)) set one model for every
integration: a connector, an integration, sync units, runs and activities, with
`integration.sync_units` as the framework's one durable table, one row per (channel, sync), and
`discover()` listing an integration's channels. "Every integration uses one uniform model" is a
success criterion of the epic. ADR-0033 then added per-call `segmentId`/`integrationId` overrides
on `emit()` so Octolens could run one org-wide poll under a 500 requests/hour budget shared by
every project. Credly reused that path because one OAuth client covers the whole organization,
and each fan-out connector grew its own routing outside `sync_units`: a mapping table
(`octolensKeywordMappings`, `credlyTemplateMappings`), a find-or-create of per-segment integration
rows under its own advisory lock, deactivation and reconcile logic, and for Credly a separate
release-backfill table. Every new connector following that path adds another such set.

## Decision

A record is routed by the sync unit that fetched it: `sync_units.integrationId` →
`integrations.segmentId`. The channel is the smallest scope the provider API can filter requests
by, and mapping a provider key to a segment means onboarding that key as a channel under the
segment's integration. Connectors add no mapping tables and keep no routing in
`integration.settings`; ADR-0033's overrides stay only for providers that cannot filter by channel
or whose shared budget cannot afford one poll per channel at the sync's cadence.

### Channels

| Connector | Channel            | Units per integration           | Routing                                   |
| --------- | ------------------ | ------------------------------- | ----------------------------------------- |
| GitHub    | repo `node_id`     | repos × 8 syncs                 | unit's integration                        |
| Credly    | badge template id  | mapped templates × 2 syncs      | unit's integration (moves off fan-out)    |
| Octolens  | one global channel | 1 unit on a segment-less anchor | fan-out: keyword → subproject integration |

Octolens stays fan-out: a poll per subproject at the 5-minute cadence costs 12 requests/hour each
before any results, so the shared 500/hour budget runs out with the project count, and a mention
matching several keywords would be fetched once per unit.

### Rules

- **Destination**: one live `public.integrations` row per (platform, subproject segment). An
  integration owns channels × syncs units, unique on `(integrationId, channelId, syncName)`.
- **Integrations and units change together**: creating or connecting an integration onboards
  its channels, adding or removing channels onboards or decommissions their units, and
  soft-deleting it decommissions all its units, in the same flow (`reconcileGithubIntegration` is
  the reference). Neither side is written alone.
- **Source of truth**: `sync_units` says what an integration syncs. `integration.settings` may
  mirror it for display, never drive it. GitHub's `settings` and `public.repositories` predate this
  and stay (`repositories` is part of the Insights contract).
- **Credentials identify the account, not the scope**: the same platform credential serves every
  channel; channel lists and filters never go into a credential.
- **One owner per channel**: units are created under `lockSyncUnitChannel` after
  `hasActiveSyncUnitsForChannelElsewhere`, and another integration's channel is skipped with a
  reason.
- **Moving a channel is decommission + onboard, never an `integrationId` rewrite**: the sink
  dedupes on (platform, type, `sourceId`, channel, segment, timestamp), so a record re-emitted
  under another segment becomes a second activity.
- **Filters are asserted**: a sync that filters a request by channel checks every returned record
  belongs to it, since providers such as Credly ignore unknown filter keys and return the whole
  account.
- **Matching and review decide only whether a channel exists**: resolvers, overrides and held
  queues stay outside the emit path; their output is units created or decommissioned, and reviewer
  decisions live in code or config.
- **Fan-out needs sign-off**: a connector meeting the exception runs on a segment-less anchor
  integration with `emitsOwnSegment`, and its routing table is reviewed before it is built.

The full rule set for connector authors is in
[`services/libs/connectors/README.md`](../../services/libs/connectors/README.md).

## Consequences

### Positive

- One routing path for every channel-filterable connector; onboarding, lifecycle, renames,
  dead-lettering and per-unit observability (`emittedCount`, `lastErrorClass`) apply per channel
  without connector-specific tables.
- Credly drops its anchor poll, `credlyTemplateReleases` and the routing reads of
  `credlyTemplateMappings`: releasing a held template becomes creating its units with a start
  watermark.

### Negative

- More units: Credly moves from 2 units to 2 per mapped template, and each channel costs its own
  request per cadence even when nothing changed.
- A channel's history is tied to its integration; moving it re-fetches under the new integration
  instead of re-pointing rows.

### Risks

- A provider that silently ignores a channel filter would emit another channel's records under
  this segment; the per-sync filter assertion turns that into a `provider.contract` failure.
- Fan-out connectors keep their own routing tables; the sign-off gate is what stops the exception
  from becoming the default again.
