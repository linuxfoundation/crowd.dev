# npm Worker — Workflows

The npm worker keeps the npm slice of the packages database fresh: package
metadata (packuments), per-day download counts, and rolling 30-day download
windows. It does this with **four Temporal workflows**, all running on the
`npm-worker` task queue and all registered as cron schedules when the
worker boots (`src/bin/npm-worker.ts`).

Shared design notes:

- **Lanes.** Concurrency is expressed as _lanes_ — one per configured proxy IP
  when `CROWD_PACKAGES_PROXIES_ENABLED=true`, otherwise a single direct lane
  (`src/npm/proxies.ts`). Each lane egresses through its own IP so the work stays
  under npm's per-IP rate limits. Lane count is read at runtime via
  `getLaneCount()`.
- **`continueAsNew`.** Each workflow processes a bounded number of rounds per
  run, then continues-as-new so history stays small and a long backfill never
  blocks the feed.
- **Error handling.** 4xx/404 (e.g. bogus deps.dev names that leaked into
  `packages`) → skip the package, mark it processed/scanned, don't retry.
  5xx/network → throw and ride Temporal's exponential activity retry. A
  downloads API 429 pauses the shared per-egress limiter
  (`src/npm/downloadsThrottle.ts`), which backs off and retries; after 20
  minutes of only 429s the lane yields and the workflow sleeps 1 hour, then
  continues as new. Metadata ingest still throws on 429. The limiter lives in
  the worker process, so run a single npm-worker replica.
- **purl is the key.** The purl from the DB row is the source of truth; the npm
  registry name used for HTTP fetches is derived from it.

---

## 1. `ingestNpmPackages` — package metadata ingest

**Schedule:** `npm-registry-ingest`, daily at `03:15` UTC · 1h execution timeout.

**What it does** (`src/npm/workflows.ts`):

1. **Drains unscanned packages.** Keyset-paginates through `packages` rows that
   have never been metadata-scanned, fanning each round (`lanes × 50` purls)
   round-robin across the lanes. Up to 25 rounds per run.
2. **Polls the npm `_changes` feed** once per run. New/changed package names are
   filtered down to purls that exist in `packages` and re-ingested across lanes.
   On a cold start it bootstraps the `_changes` cursor from `replicate.npmjs.com`
   instead of replaying all history.
3. **Advances the `_changes` cursor** only _after_ the page's packages are
   ingested, so the cursor never moves past unprocessed work.

Continues-as-new while either the backfill or the feed still has work. Each
package fetch is a packument upsert + audit-field-change log + scan watermark.

---

## 2. `backfillDailyDownloads` — per-day download counts (critical slice)

**Schedule:** `npm-daily-downloads-backfill`, daily at `03:30` UTC · 6h timeout.

**What it does:** Self-healing fill of `downloads_daily` for the critical slice
(`packages`). Each run first asks npm for its newest published day
(`/downloads/point/last-day`); npm publishes a day's counts hours after it ends,
so that day, not yesterday, is the upper bound. Each round runs one
self-selecting lane per proxy IP; lanes drain disjoint **hash-shards**
(`laneIndex / laneCount`) of the packages due for backfill (`lanes × 128` per
round, up to 50 rounds per run). Per package it fetches the _missing_ days
between the first release (clamped to `NPM_EARLIEST`) and the bound, plus the 7
days ending at the newest stored day, and upserts them: a different nonzero
count overwrites the stored one, so a day stored too early gets corrected.

Unscoped packages run every day and share bulk requests (up to 128 names, the
last 365 days); older gaps and scoped (`@scope/...`) packages take one request
per range of up to 540 days. Scoped packages are due weekly. A round where
every lane fetches nothing means done; otherwise it continues-as-new with a
fixed `cutoff`.

**Repair:** start it with `refetchFrom` to refetch and upsert every day from
that date for every critical package (the weekly scoped rule is off for that
run). The repair and the scheduled run share the per-package watermark, so a
package the scheduled run marks first would skip the repair: pause the schedule
and let any open daily run finish (or terminate it) before starting, then
unpause once the repair completes. A date within the last 365 days keeps
unscoped packages in bulk requests.

```bash
temporal schedule toggle --schedule-id npm-daily-downloads-backfill --pause \
  --reason "daily downloads repair"
temporal workflow start \
  --task-queue npm-worker \
  --type backfillDailyDownloads \
  --workflow-id npm-daily-downloads-repair \
  --run-timeout 24h \
  --input '{"refetchFrom":"YYYY-MM-DD"}'
# once the repair has completed
temporal schedule toggle --schedule-id npm-daily-downloads-backfill --unpause \
  --reason "daily downloads repair done"
```

---

## 3. last-30d rolling windows (whole universe) — **breadth then depth**

`downloads_last_30d` is filled across all npm rows in `packages` by **two
cooperating workflows** keyed on two watermarks in `npm_package_universe_state`:

| Watermark                             | Meaning                         | Set by       |
| ------------------------------------- | ------------------------------- | ------------ |
| `downloads_30d_last_run_at`           | current 30-day window refreshed | breadth (3a) |
| `downloads_30d_history_backfilled_at` | full older history filled       | depth (3b)   |

The split makes the work **breadth-first**: the latest 30-day number (the
denormalized `packages.downloads_last_30d`) lands for _every_ package before any
deep history is fetched, and the history backfill can never block the
latest-window refresh.

Both share one fetch engine (`processLast30dWindowPlans`): work is grouped **by
window**, most recent first; unscoped packages are bulk-fetched 128 at a time per
window, scoped (`@scope/...`) individually; an upsert mirrors to
`packages.downloads_last_30d` only when the window is the latest. Client errors
(404/4xx) skip the package and record an error; 5xx/network errors throw so
Temporal retries the lane — and because progress lives in `downloads_last_30d`, a
retry skips every already-stored window instead of re-fetching. All three
download lanes (daily included) run with a 1 hour activity timeout and a 2 minute
heartbeat timeout; they **heartbeat** before every fetch and every 30s while the
limiter waits, so a stall is detected in minutes. Each download lane returns after a
20 minute budget with progress saved, and the workflow starts another round. A lane whose own
requests keep getting 429s while other flows succeed on the same egress returns the same way,
20 minutes past its budget.

### 3a. `refreshLatestLast30dDownloads` — breadth (latest window)

**Schedule:** `npm-last-30d-latest-refresh`, monthly on the 1st at `04:00` UTC ·
6h timeout. `LATEST_PER_LANE` (128) packages per lane per round so each round
fills the bulk-128 call; up to 50 rounds, then continue-as-new.

Selects packages due for the current window (`downloads_30d_last_run_at IS NULL
OR < cutoff`) and fetches **just the latest window** for each, mirroring the count
to `packages.downloads_last_30d`. The breadth watermark is bumped per package as soon as its
window is processed, so the monthly run touches each package exactly once.

### 3b. `backfillLast30dHistory` — depth (older history)

**Schedule:** `npm-last-30d-history-backfill`, **daily** at `04:30` UTC · 6h
timeout, `overlap: SKIP`. Independent of 3a, so a long initial backfill never
blocks the monthly latest-window refresh. Daily cadence gives ~180h/month of
throughput vs a monthly run's 6h (`workflowExecutionTimeout` caps the whole
continue-as-new chain at 6h per fire).

Selects packages whose latest window is present (`downloads_30d_last_run_at IS NOT
NULL`) but whose history is not yet filled (`downloads_30d_history_backfilled_at
IS NULL`) — so history is never fetched before the latest window. For each, the
still-missing **older** windows are derived from the stored rows and capped at
`HISTORY_WINDOWS_PER_ROUND` (6) newest-first per round (`HISTORY_PER_LANE` = 50).
This cap is the key bound: a brand-new package has ~140 monthly windows back to
`NPM_EARLIEST`, and without it a single lane tried to fetch all of them for its
whole batch in one shot and never finished inside the activity timeout. The depth
watermark is bumped only once a package's whole backlog drains (or it 4xx's); a
partially-backfilled package stays unmarked and is re-selected next round until it
converges.

---

## How to run

All four are **cron-scheduled automatically** — just start the worker and the
schedules register themselves (idempotently):

```bash
# from services/apps/packages_worker
pnpm dev:npm-worker:local     # local: loads backend/.env.*.local, hot-reload
pnpm start:npm-worker         # prod-style start
```

To run one **on demand** (outside its cron), trigger the workflow against the
`npm-worker` task queue with the Temporal CLI or client — they all take no
args and default their internal state:

```bash
temporal workflow start \
  --task-queue npm-worker \
  --type ingestNpmPackages \
  --workflow-id npm-ingest-manual

# or: backfillDailyDownloads / refreshLatestLast30dDownloads / backfillLast30dHistory
```

### Relevant environment variables

| Variable                            | Default | Purpose                                                                           |
| ----------------------------------- | ------- | --------------------------------------------------------------------------------- |
| `CROWD_PACKAGES_PROXIES_ENABLED`    | `false` | Master switch for the proxy/lane layer. Off = single direct lane.                 |
| `CROWD_PACKAGES_PROXIES`            | —       | Comma-separated `host:port:user:pass` proxy list; lane count = number of entries. |
| `CROWD_PACKAGES_INGEST_SLEEP_MS`    | `1200`  | Per-package throttle in the metadata ingest lane.                                 |
| `CROWD_PACKAGES_DOWNLOAD_SLEEP_MS`  | `1000`  | Minimum spacing between downloads API requests per egress (shared limiter floor). |
| `CROWD_PACKAGES_TEMPORAL_NAMESPACE` | —       | Temporal namespace the worker connects to.                                        |
