---
type: system
slug: sys-connectors
name: Connector framework (integrations v2)
repo: crowd.dev
owner: CDP team
depends_on: [sys-cdp, flow-activities]
used_by: []
sources:
  - services/libs/connectors/
  - services/apps/connectors_worker/
  - services/libs/data-access-layer/src/connectors/
  - services/libs/data-access-layer/src/octolens/
  - services/libs/data-access-layer/src/credly/
---

# Connector framework (integrations v2)

The Temporal-based framework that pulls activities from source platforms and hands them to
`data_sink_worker`. Its model and the rules every connector follows are in
[`services/libs/connectors/README.md`](../../services/libs/connectors/README.md). The short
version: an integration owns one sync unit per channel and sync, and the channel is the mapping
to a segment.

## Where things are

| What                                   | Where                                                                                                     |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Manifests (GitHub, Octolens, Credly)   | `services/libs/connectors/src/connectors/<platform>/`                                                     |
| Framework types, emit, registry        | `services/libs/connectors/src/` (`types.ts`, `emit.ts`, `registry.ts`)                                    |
| HTTP client, error classes, token pool | `services/libs/connectors/src/http/`, `src/pool/`                                                         |
| Credentials                            | `services/libs/connectors/src/credentials.ts`                                                             |
| Registration, dispatcher, sync runs    | `services/apps/connectors_worker/src/` (`main.ts`, `workflows/`, `activities/`)                           |
| GitHub onboarding, lifecycle, renames  | `services/apps/connectors_worker/src/` (`onboardGithubRepo.ts`, `githubLifecycle.ts`, `githubRenames.ts`) |
| Sync unit, shadow and completeness DAL | `services/libs/data-access-layer/src/connectors/`                                                         |
| Platform-specific DAL                  | `services/libs/data-access-layer/src/octolens/`, `src/credly/`                                            |

## Tables

| Table                                                                                | Holds                                                                 |
| ------------------------------------------------------------------------------------ | --------------------------------------------------------------------- |
| `public.integrations`                                                                | One destination per (platform, subproject segment)                    |
| `integration.sync_units`                                                             | One row per (integration, channel, sync): status, schedule, watermark |
| `integration.sync_shadow_records`                                                    | Records from units with `emitEnabled = false`                         |
| `integration."octolensKeywordMappings"`                                              | Octolens keyword → subproject routing (fan-out exception)             |
| `integration."credlyTemplateMappings"`, `"credlyBadges"`, `"credlyTemplateReleases"` | Credly resolver decisions, badge state, release backfills             |

## Decisions

- [Integration Builder v2 architecture](https://linuxfoundation-dxwx.dsp.so/CQLF4CI1-integration-builder-v2-nango-deprecation)
  ([lfx-cdp-insights-ops#28](https://github.com/linuxfoundation/lfx-cdp-insights-ops/issues/28)):
  one model for every integration, one sync unit per (channel, sync).
- [ADR-0033](../adr/0033-connector-emit-per-call-segment.md): per-call segment overrides on
  `emit()`, the fan-out exception Octolens uses.
- [ADR-0034](../adr/0034-connector-sync-deps-bag.md): syncs reach the database through a typed
  per-connector deps bag.
