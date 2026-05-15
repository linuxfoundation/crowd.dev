# 2. Automatic Projects Discovery Worker

Date: 2026-05-15

## Status

Accepted

## Context

The Linux Foundation needs to discover open-source projects that are candidates for onboarding into the CDP platform. Candidate projects come from heterogeneous external sources with different formats and update frequencies. The list of sources is expected to grow over time.

Two sources are active at the time of writing:

1. **LF Criticality Score API** — a scored list of open-source repositories maintained by the Linux Foundation. Updated daily. Provides a criticality score per repo.
2. **Insights GitHub Discussions** — the `linuxfoundation/insights` GitHub repository has a discussion category (`project-onboardings`) where projects are manually proposed for onboarding. This source carries stronger intent signal than a criticality score alone.

We needed a design that:
- Ingests from multiple sources into a single canonical table (`projectCatalog`).
- Preserves the origin of each record so downstream jobs can reason about it.
- Encodes a lifecycle state per record that survives re-ingestion without losing human decisions.
- Can be extended with new sources without changing the core pipeline.

## Decision

We will run a dedicated Temporal worker (`automatic_projects_discovery_worker`) that executes a daily scheduled workflow (`discoverProjects`). The workflow fans out over all registered sources, fetches their data as streams, and bulk-upserts into `projectCatalog`.

**Source abstraction (`IDiscoverySource`):** each source implements a common interface — `listAvailableDatasets`, `fetchDatasetStream`, `parseRow` — so the pipeline is source-agnostic. Adding a new source means adding a new class; the workflow does not change.

**`source` field:** populated by each provider with its own identifier (`'lf-criticality-score'`, `'insights-discussions'`). Tracks where a record came from. Indexed.

**`action` field:** encodes the lifecycle state of a candidate project. Four values:

| Value | Meaning |
|---|---|
| `auto` | Discovered automatically; no human signal. Default. |
| `evaluate` | Flagged for priority evaluation by the next job. Set by `insights-discussions` at ingestion time; can also be assigned by the evaluate job to `auto` records. |
| `onboard` | Confirmed for onboarding. Set by a downstream job or manually. |
| `unsure` | Evaluated but outcome is uncertain. Set by a downstream job or manually. |

**`action` precedence on upsert:** `insights-discussions` sets `action = 'evaluate'`, which takes precedence over `auto`. Human decisions (`onboard`, `unsure`) are sticky — a re-ingestion from any source will not overwrite them. This ensures the pipeline can run daily without resetting decisions made downstream.

**`evaluate` as a priority signal:** records with `action = 'evaluate'` are processed with higher priority by the downstream evaluate job. The discovery worker pre-populates this state for `insights-discussions` records because they carry explicit human intent. However, the evaluate job is responsible for deciding priority across the full catalog — it may also promote `auto` records to `evaluate` based on its own logic when no discussion-sourced candidates are available.

**Incremental vs full mode:** the workflow supports two modes. `incremental` processes only the latest dataset from each source (daily default). `full` reprocesses all historical datasets, useful for backfills.

## Consequences

- New discovery sources can be added by implementing `IDiscoverySource` and registering the class — no changes to workflow or activities.
- The `source` field allows per-source analytics and filtering without joining other tables.
- The `action` state machine is intentionally simple. If lifecycle complexity grows, the four-value enum may need extension via a new ADR.
- `insights-discussions` records act as high-priority signals. If the discussion category changes name or moves to a different repo, `InsightsDiscussionsSource` must be updated manually.
- The `onboard` and `unsure` values are currently set by downstream jobs (not yet implemented at time of writing). Until those jobs exist, these states can only be set manually.
