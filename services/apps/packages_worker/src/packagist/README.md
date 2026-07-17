# Packagist Worker — Workflows

The Packagist worker keeps the PHP/Composer slice of the packages database fresh
by crawling **packagist.org directly** — deps.dev has no Packagist coverage, so
unlike npm/maven/pypi there is no BigQuery universe to import from; the registry
crawl *is* the universe source. It runs **four Temporal workflows** on the
`packagist-worker` task queue: three cron schedules registered at worker boot
(`src/bin/packagist-worker.ts`), plus the metadata drain, which the seed chains
as a child workflow on completion.

Identity: ecosystem `packagist`, purls `pkg:composer/{vendor}/{name}`
(namespace = vendor). Audit tag: `packagist` in `audit_field_changes`.

Shared design notes:

- **Two registry endpoints.** The *dynamic* endpoint
  (`packagist.org/packages/{vendor}/{name}.json`, server-cached ~12h) carries
  package info and live counters: downloads, dependents, description, repo URL,
  abandoned flag, maintainers. The *static* p2 endpoint
  (`repo.packagist.org/p2/{vendor}/{name}.json`, CDN-served, the same file
  Composer clients resolve) carries the version manifests: every tagged
  release with its `require`/`require-dev`, licenses, homepage.
- **Politeness.** Bounded concurrency per Packagist's own published etiquette
  guideline ([packagist.org/apidoc](https://packagist.org/apidoc)): max 10
  concurrent requests, 20 if static-file-only. Their docs state this as "be a
  good citizen," not a documented enforcement mechanism — we don't have a
  citation for what happens above it, so treat 10 as the number to respect,
  not a proven "or else". Since every lane's ingest starts with a dynamic
  fetch, all lanes run at that stricter cap anyway (env-tunable downward,
  hard-capped at 10); the p2 fetches inside the metadata lane inherit it,
  well under p2's own 20. User-Agent carries a `mailto=` contact
  (`CROWD_PACKAGES_PACKAGIST_MAILTO`) — their docs say a missing one risks an
  IP block. Cron minutes are deliberately off `:00` (their docs also flag
  hourly traffic peaks at `XX:00`). p2 fetches replay the stored
  `Last-Modified` as `If-Modified-Since`; a `304` records the run without
  re-downloading or re-parsing.
- **Watermark-driven drains.** Each lane tracks its own per-purl watermark +
  run result in `packagist_package_state` (`metadata_last_run_at` /
  `downloads_30d_last_run_at` / `daily_downloads_last_run_at`, each with a
  `*_run_result` JSONB). Workflows drain in `continueAsNew` rounds (20 × 50),
  so an interrupted run resumes where it left off on the next launch.
- **Error handling.** 404 or malformed body/shape → 3 fast in-lane retries
  (1s linear backoff), then mark the lane's watermark with an error result and
  move on — `isClientError()` treats 404 the same as any other non-429 4xx, so
  there's no zero-retry fast path for it specifically. 429/5xx/network/timeout
  → throw and ride Temporal's exponential activity retry (5 attempts, lockstep
  with the per-item give-up so one bad package can never stall a drain).
- **Tier scoping.** Discovery, package info, repo links, versions, and
  dependencies cover **all** packages (deliberate divergence from pypi — see
  ADR-0009). Only `downloads_daily` and maintainers are
  **critical-slice-only**, matching npm/pypi.

---

## 1. `seedPackagistPackages` — universe discovery

**Schedule:** `packagist-seed`, weekly Sunday `02:17` UTC.
**Targets:** the full catalog (~500K names) — criticality not consulted.

**What it does** (`runPackagistPackageSeed`): one GET of
`packagist.org/packages/list.json`, validates/lowercases/dedupes the names,
inserts identity rows in 5,000-row chunks, then **chain-starts the metadata
drain** (child workflow, `ParentClosePolicy.ABANDON`).

**Populates:** `packages` only — `purl`, `ecosystem='packagist'`,
`namespace` (vendor), `name`, `registry_url`, `status='active'`,
`ingestion_source='packagist-registry'`. `ON CONFLICT (purl) DO NOTHING`:
re-runs only add newly published packages and never touch enriched rows.

---

## 2. `ingestPackagistMetadata` — the enrichment drain (both endpoints)

**Schedule:** none of its own — **chained off the seed** (effectively weekly,
Sunday right after `02:17`), so newly published packages exist as rows before
dependency targets are resolved. Runs independently once started; a failed
seed skips that week's drain (recover with
`pnpm trigger-packagist:local metadata`).
**Targets:** **all** packagist packages due for a refresh —
`CROWD_PACKAGES_PACKAGIST_RUN_ONLY_FOR_CRITICAL` defaults to `false`
(`true` narrows to the critical slice). Due = never scanned or
`metadata_last_run_at` older than 7 days (`…_METADATA_REFRESH_DAYS`).

**What it does:** per package, **two fetches** — the dynamic endpoint (≤10
concurrent) and the p2 endpoint (with `If-Modified-Since`; steady state is
mostly 304s). A package the dynamic endpoint 404s is marked errored without
touching p2.

**Populates, per scanned package:**

- From the **dynamic endpoint**:
  - `packages` — `description`, `declared_repository_url`, `repository_url`
    (canonicalized), `status` (`abandoned` → `deprecated`, else `active`),
    `downloads_last_30d` (rolling monthly), `total_downloads`,
    `dependent_count` (registry-reported direct dependents — a ranking input),
    `last_synced_at`. Counters COALESCE: a missing value never wipes a known one.
  - `repos` + `package_repos` — **all packages**: canonicalized repo URL linked
    with `source='declared'`, `confidence=0.8` (the manifest-declared convention
    shared with npm/pypi/maven/cargo); feeds the shared github-repos-enricher.
    Skipped when no URL or it can't be canonicalized.
  - `maintainers` + `package_maintainers` — **critical only**: usernames from
    the dynamic endpoint (`role='maintainer'`).
- From the **p2 endpoint**:
  - `versions` — one row per tagged release (`number`, `published_at`,
    `is_prerelease`, `is_latest`, `licenses`); dev branches skipped.
  - `package_dependencies` — direct edges only (`require` → `'direct'`,
    `require-dev` → `'dev'`), declared constraints verbatim, platform packages
    excluded, `depends_on_version_id` NULL (resolved at query time — ADR-0009).
  - `packages` aggregates — `versions_count`, `latest_version`,
    `first_release_at`, `latest_release_at`, `licenses`, `homepage` — all
    written non-destructively (COALESCE / keep-on-zero).
- `packagist_package_state` — `metadata_last_run_at`, `metadata_run_result`,
  and `metadata_last_modified` (the p2 `Last-Modified`, replayed as
  `If-Modified-Since` next run).
- `audit_field_changes` — every changed field above, worker tag `packagist`.

---

## 3. `ingestPackagistDownloads30d` — monthly rolling-window capture

**Schedule:** `packagist-downloads-30d`, monthly on the **1st** at `03:53` UTC —
anchored on the month boundary so the observed rolling-30d value approximates
the calendar month.
**Targets:** **all** packagist packages, npm-style breadth: the run fixes a
`cutoff` timestamp once, and every package whose `downloads_30d_last_run_at`
is older (or null) is due exactly once per run.

**What it does:** fetches the dynamic endpoint per package and records
`downloads.monthly` as the month's window.

**Populates:** one `downloads_last_30d` row per purl per month
(`end_date` = the 1st, `start_date` = end − 30d), **mirrored to
`packages.downloads_last_30d`**. A window already recorded for the month is
never overwritten — the first observation (closest to the boundary) wins.
Packagist keeps no history, so unlike npm there is no backfill lane; a missed
month stays missed. State: `downloads_30d_last_run_at` + `downloads_30d_run_result`.

Like the daily lane (see below), the write-date is pinned to the run's fixed
cutoff and threaded through every batch, so the labeled window stays correct
even if the drain runs long.

---

## 4. `ingestPackagistDownloadsDaily` — daily capture (critical slice)

**Schedule:** `packagist-downloads-daily`, daily `22:23` UTC — late in the day
on purpose: Packagist's `daily` figure is `today_so_far + yesterday_total ×
dayRatio`, with `dayRatio` decaying toward 0 as the day goes on, so asking
late gets a figure that's mostly today's real data instead of mostly
borrowed from yesterday.
**Targets:** `is_critical = TRUE` only; no-ops (guard) while zero packagist
rows are critical — i.e. until the first `rank_packages()` pass. Same cutoff
watermark pattern as the 30d lane.

**What it does:** fetches the dynamic endpoint per critical package and records
the registry's rolling daily figure.

**Populates:** one `downloads_daily` row per package per day (`package_id`,
run date, count), with the run date pinned once from the run's `cutoff` so
every row from one run shares the same label even if the drain runs long.
That only fixes the *label*, though — Packagist computes `daily` live at
fetch time, so for a long-running drain the underlying value can drift
forward from what the label implies for whichever packages are processed
last. State: `daily_downloads_last_run_at` + `daily_downloads_run_result`.

---

## What this worker deliberately does NOT write

- `transitive_dependent_count`, `dependent_repos_count` — not computable from
  the registry; needs a reverse-closure over our stored direct edges
  (future work, see ADR-0009 risks).
- Advisories — the OSV worker owns security data platform-wide.
- `is_critical` / `criticality_score` / ranking columns — the shared
  criticality worker; this worker only *reads* `is_critical` for scoping.
- Repo stars/forks/scorecard — the shared github-repos-enricher, fed by the
  `package_repos` links this worker creates.
- `keywords`, `versions.is_yanked`, `depends_on_version_id` — see above.

## Configuration

All optional; defaults in `activities.ts` / `fetchPackage.ts`:

| Env var | Default | Meaning |
|---|---|---|
| `CROWD_PACKAGES_PACKAGIST_MAILTO` | `oss-packages@linuxfoundation.org` | Contact in the crawl User-Agent |
| `CROWD_PACKAGES_PACKAGIST_STATS_CONCURRENCY` | 10 (hard cap 10) | Parallel package ingests across all lanes — every lane starts with a dynamic-endpoint fetch, so the stricter 10-limit bounds everything |
| `CROWD_PACKAGES_PACKAGIST_METADATA_REFRESH_DAYS` | 7 | Metadata refresh window |
| `CROWD_PACKAGES_PACKAGIST_RUN_ONLY_FOR_CRITICAL` | `false` | `true` narrows metadata to the critical slice |
| `CROWD_PACKAGES_PACKAGIST_STOP_AFTER_FIRST_PAGE` | `false` | Debug: one drain page, no continueAsNew |

## Running it

```bash
# local worker (hot reload; needs packages-db + temporal from scripts/scaffold.yaml)
DEV=1 ./scripts/cli service packagist-worker up

# trigger on demand instead of waiting for the crons
cd services/apps/packages_worker
pnpm trigger-packagist:local seed             # discovery (chain-starts metadata!)
pnpm trigger-packagist:local metadata         # enrichment: info + versions + deps
pnpm trigger-packagist:local downloads-30d    # monthly rolling-window capture
pnpm trigger-packagist:local downloads-daily  # daily capture, critical slice
```

Note: triggering `seed` also chain-starts the full `metadata` drain (set
`CROWD_PACKAGES_PACKAGIST_STOP_AFTER_FIRST_PAGE=true` locally to bound it).
Local smoke order: seed → metadata → (rank) → downloads lanes for
critical-scoped writes. State lives in `packagist_package_state`
(migration `V1783382400__packagist_worker.sql`); design decisions in
`docs/adr/0009-packagist-worker-design-decisions.md`.
