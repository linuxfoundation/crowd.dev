# Maven POM Fetcher

Worker that syncs Maven package metadata from Maven Central into the `packages` DB.
Lives in `packages_worker` and can run in two ways:

- **Temporal (production, incremental):** entry point `bin/packages-worker.ts` registers the
  `maven-critical` Temporal schedule. Each tick runs a **single batch** as a Temporal activity,
  and **skips POM extraction when the version is unchanged**. See [Scheduling](#scheduling).
- **One-shot backfill:** entry point `bin/maven-backfill.ts` (`pnpm backfill:maven`) drains the
  Tier 2 critical queue once with **full extraction** (no version short-circuit), then exits.
  Run it `exec`ed into the `packages-worker` container for the initial fill / periodic full
  refresh. It is resumable — the DB state is the cursor, so re-running picks up where it left off.

---

## Architecture: Two-Tier Fetch

Both phases pull candidates from `packages_universe` (filtered by `is_critical`, ordered by
`rank_in_ecosystem`) and write into `packages`. A package is a candidate when it is not yet
in `packages` (`last_synced_at IS NULL`) or its `last_synced_at` is older than
`POM_FETCHER_REFRESH_DAYS`. The two phases run as **separate Temporal schedules** (or, in the
standalone loop, only the critical phase runs).

### Non-Critical phase (`is_critical = false`)

DB-only. Copies universe stats (criticality score, downloads, dependent counts) into
`packages` with `ingestion_source = 'packages_universe'`. **No HTTP.** Fast (~1000 pkg/sec).

### Critical phase (`is_critical = true`)

Full POM extraction from Maven Central with parent-chain resolution (max 8 hops). Populates
description, homepage, SCM/repo, licenses, maintainers and the full version list.

Whether the version short-circuit applies is fixed per **entry point** (not a runtime flag):

| Entry point | Mode | Behaviour |
|-------------|------|-----------|
| Standalone `bin/maven.ts` | **backfill** | Always runs full POM extraction for every selected critical package, regardless of version. Use for the initial fill / periodic full refresh. |
| Temporal `mavenCriticalWorkflow` | **incremental** | If the upstream release version equals the stored `latest_version`, skips the POM fetch and only bumps `last_synced_at` (status `unchanged`). Full extraction runs only for new packages or when the version changed. |

This is passed as the `forceFullExtraction` argument to `processBatch` — `true` from the
standalone loop, `false` from the Temporal activity. There is no env variable for it.

**Why two tiers?** Parent POM resolution is the expensive part — multiple HTTP requests per
package (up to 8 extra fetches). Running it on millions of non-critical packages is not
feasible; for them the DB-only universe stats are enough. The extra cost is reserved for
critical packages, where data quality matters.

### Parent POM cache

Parent POMs are shared across many artifacts of the same namespace (`org.apache:apache`,
`org.springframework.boot:spring-boot-starter-parent`, `com.google.cloud:google-cloud-shared-config`, …).
Because the queue is ordered by `rank_in_ecosystem`, those siblings are processed close
together. A module-level, coordinate-keyed in-process cache in `extract.ts` collapses the
repeated parent fetches into a **single** HTTP request, and also removes the redundant second
fetch of each artifact's own POM (`extractArtifact` fetches the leaf, then
`resolveWithInheritance` would fetch it again at depth 0). This is the **single biggest lever
against Maven Central rate limiting** — and it works *because* of the namespace clustering, so
shuffling the batch (which the queue's `rank` ordering produces) would be counter-productive.

- **Only successful fetches are cached.** `fetchPom` returns `null` for both a real 404 and a
  transient failure (throttle/timeout), so caching `null` would poison the cache — it is never
  done. Missing/failed POMs are simply re-fetched on the next pass.
- **No TTL.** Maven coordinates are immutable, so a cached POM never goes stale. The cache is
  bounded by an LRU size cap (`POM_CACHE_MAX_ENTRIES`, default 5000) purely to cap memory.
- **Request coalescing.** Concurrent fetches for the same coordinates share a single in-flight
  request instead of issuing duplicates.
- **Observability.** `getPomCacheStats()` returns `{ size, hits, coalesced, misses, evictions,
  hitRate }`; the critical batch logs it once per batch under message **`POM cache`**, so you
  can watch the hit rate climb as the cache warms.

> The cache lives for the lifetime of the worker process. Under Temporal it persists **across
> batches/ticks** (same process), so the hit rate keeps improving across the run; in the
> standalone loop it persists across passes until the process is restarted.

---

## Coverage Matrix (critical packages — full POM extraction)

The matrix below describes the **critical** path (full POM + parent resolution).
Non-critical packages are DB-only: they receive just the universe-stat columns
(`criticality_score`, `dependent_packages_count`, `dependent_repos_count`,
`downloads_last_month`) plus `purl`/`namespace`/`name`/`registry_url`/`last_synced_at`;
all POM-derived columns stay null for them.

### packages

| Column | Source | Coverage |
|--------|--------|----------|
| purl | packages_universe | ✅ all |
| ecosystem | hardcoded `'maven'` | ✅ all |
| namespace | packages_universe.namespace (= groupId) | ✅ all |
| name | packages_universe.name (= artifactId) | ✅ all |
| registry_url | `https://central.sonatype.com/artifact/{ns}/{name}` | ✅ all |
| latest_version | maven-metadata.xml `<release>` | ✅ all |
| ingestion_source | see table below | ✅ all |
| last_synced_at | NOW() | ✅ all |
| description | POM `<description>` | ✅ best-effort¹ |
| homepage | POM `<url>` | ✅ best-effort¹ |
| declared_repository_url | POM `<scm><url\|connection>` raw | ✅ best-effort¹ |
| repository_url | normalized from declared_repository_url | ✅ best-effort¹ |
| licenses / licenses_raw | POM `<licenses>` | ✅ best-effort¹ / ✅ full for critical² |
| status | Sonatype: deprecated flag | 🔜 Sonatype |
| versions_count | Sonatype: COUNT of releases | 🔜 Sonatype |
| first_release_at | Sonatype: MIN release timestamp | 🔜 Sonatype |
| latest_release_at | Sonatype: MAX release timestamp | 🔜 Sonatype |
| keywords | not in Maven POM | ❌ |
| dist_tags_* | N/A — Maven ecosystem | ❌ |
| dependent_packages_count | not in Maven registry API | ❌ |
| dependent_repos_count | not in Maven registry API | ❌ |
| criticality_score | set by ranking function | ❌ |
| is_critical | set by ranking function | ❌ |
| last_rank_pass_at | set by ranking function | ❌ |

### versions

| Column | Source | Coverage |
|--------|--------|----------|
| package_id | FK from packages upsert | ✅ all |
| ecosystem | hardcoded `'maven'` | ✅ all |
| number | maven-metadata.xml `<versions><version>` | ✅ all |
| is_latest | `number === <release>` | ✅ all |
| is_prerelease | regex on version string³ | ✅ all |
| last_synced_at | NOW() | ✅ all |
| licenses | package-level license applied to all versions⁴ (stored as a single-element `text[]`) | ✅ best-effort¹ |
| published_at | Sonatype: release timestamp | 🔜 Sonatype |
| is_yanked | no yank mechanism in Maven | ❌ |
| download_count | no public per-version API | ❌ |

### maintainers / package_maintainers

| Column | Source | Coverage |
|--------|--------|----------|
| ecosystem | hardcoded `'maven'` | ✅ all |
| username | POM `<developer><id>` | ✅ best-effort¹ |
| display_name | POM `<developer><name>` | ✅ best-effort¹ |
| email_hash | SHA-256(`<developer><email>`) — GDPR | ✅ best-effort¹ |
| url | POM `<developer><url>` | ✅ best-effort¹ |
| role | `'author'` from `<developers>`, `'maintainer'` from `<contributors>` | ✅ best-effort¹ |
| github_login | requires identity resolution | ❌ |

### repos / package_repos

| Column | Source | Coverage |
|--------|--------|----------|
| repos.url | `repository_url` (normalized from POM `<scm>`) | ✅ best-effort¹ |
| repos.host | derived from URL (`github` / `gitlab` / `bitbucket` / `other`) | ✅ best-effort¹ |
| repos.owner | URL path segment | ✅ best-effort¹ |
| repos.name | URL path segment | ✅ best-effort¹ |
| repos.description / stars / forks / … | GitHub enricher | filled by github-repos-enricher |
| package_repos.source | `'declared'` (from POM `<scm>`) | ✅ best-effort¹ |
| package_repos.confidence | `0.80` | ✅ best-effort¹ |

The POM fetcher seeds `repos` with URL-derivable fields only. The GitHub enricher then fills the rest (description, stars, forks, language, topics, etc.) because the repo row already exists. On conflict the `repos` upsert uses `COALESCE` — richer data from other enrichers is never overwritten.

`package_repos.confidence` is updated with `GREATEST(new, existing)` so a higher-confidence link from deps.dev (`0.90`) is never downgraded by our `0.80` write.

### Not supported (no Maven source)

`package_funding_links` — no funding concept in Maven POM.
`package_name_history` — Maven coordinates are immutable; rename history does not exist.
`downloads_daily` — no public per-day download API from Maven Central.
`downloads_last_30d` — 🔜 Sonatype.

---

**Notes:**

> ¹ **best-effort**: field is populated only when present in the resolved POM chain. Non-critical packages skip POM fetching entirely (DB-only), so these columns are always null for them.
>
> ² **full resolution for critical**: parent chain is followed (max 8 hops), so inherited fields are resolved correctly.
>
> ³ **prerelease regex**: matches `-SNAPSHOT`, `-alpha`, `-beta`, `-rc`, `-M[0-9]+` (case-insensitive).
>
> ⁴ **license per version**: the package-level license (first entry from POM `<licenses>`) is applied to all versions. Per-version POM fetches are not performed. This is an approximation — Maven licenses rarely change between versions.

---

## `ingestion_source` Values

| Value | Meaning |
|-------|---------|
| `maven-registry` | Critical — full POM + parent resolution succeeded |
| `packages_universe` | Non-critical (DB-only) — only universe stats copied, no POM fetch |
| `maven_not_on_central` | `maven-metadata.xml` not found on `repo1.maven.org` — artifact is hosted on a third-party repository (e.g. WSO2 Nexus, JBoss, Atlassian). Universe data came from an aggregator (deps.dev, OSV). |
| `maven_no_version` | `maven-metadata.xml` found but `<release>` is empty — artifact has no stable release |
| `maven_error` | `maven-metadata.xml` has a release version but the `.pom` file for that version is a 404. Typical cause: partial deploy to Maven Central (metadata updated, artifact not uploaded) or Eclipse P2 feature artifacts that don't publish a standard POM. |

> On a 403/429 rate-limit or a transient network error, **no sentinel record is written**:
> the batch counts the package as an error and it is simply retried on the next tick/pass.

---

## Known Exceptions

### WSO2 (`org.wso2.*`)

WSO2 publishes some artifacts exclusively to their own Nexus at `maven.wso2.org`. A subset of their artifacts appear in `packages_universe` (sourced from deps.dev/OSV which aggregates all Maven repositories) but are **not** available on `repo1.maven.org/maven2`.

Affected pattern: `org.wso2.carbon.*` — specifically `.feature` Eclipse P2 artifacts and `.stub` artifacts. These are written with `ingestion_source = 'maven_not_on_central'` and are not retried until the next `nonCriticalPomRefreshDays` window.

### Eclipse P2 Feature Artifacts (`*.feature`)

Eclipse/OSGi feature artifacts (e.g. `org.wso2.carbon.identity.xacml.server.feature`) are packaged as `.zip` files, not `.jar`. Some publishers update `maven-metadata.xml` on Central without uploading the corresponding `.pom`. These land in `maven_error`. No fix is possible without the publisher correcting their CI/CD pipeline.

### Maven Central 403 rate limiting

Maven Central (`repo1.maven.org`) restituisce 403 come meccanismo di throttle oltre al canonico 429. Il comportamento è gestito a due livelli:

1. **Retry con backoff esponenziale** — 403 e 429 vengono ritentati fino a 3 volte (2s base, ×2 per tentativo). Gestito in `getWithRetry` (extract.ts) e `resolveVersionsList` (metadata.ts).

2. **Retry al prossimo pass** — se tutti i retry esauriscono, il batch conta il pacchetto come errore (nessun record sentinel viene scritto su `packages`) e lo riprenderà al tick/pass successivo, quando l'IP è di nuovo freddo.

**Causa root dei 403 persistenti:** `packages_universe` è ordinato per `rank_in_ecosystem`, quindi pacchetti dello stesso namespace (es. `com.google.apis`, `org.wso2`, `software.amazon.awssdk`) si raggruppano nel batch e colpiscono lo stesso CDN node di Maven Central in rapida successione. Il rate limit scatta sistematicamente dopo ~150–200 pacchetti processati.

**Mitigazione applicata, in ordine di efficacia:**

1. **Cache in-process dei parent POM** (vedi [Parent POM cache](#parent-pom-cache)) — sfrutta il clustering per namespace per collassare i fetch dei parent condivisi e il doppio fetch del leaf POM, riducendo il **volume totale** di richieste. È la leva principale: il throttle è volume-based per IP, quindi meno richieste = meno 403.
2. Un delay configurabile tra i gruppi concorrenti (`POM_FETCHER_GROUP_DELAY_MS`) + `POM_FETCHER_CONCURRENCY` basso (≤5) → abbassano il rate istantaneo.
3. Backoff di retry con jitter (`±500ms`, vedi `extract.ts` / `metadata.ts`) → evita retry sincronizzati.

> Nota: **lo shuffle dei batch non aiuta** — riordina gli stessi N request nella stessa finestra temporale (stesso volume → stesso throttle) e in più romperebbe la località che rende efficace la cache dei parent.

Namespace noti per triggerare il rate limit a causa dell'alta densità di artefatti: `com.google.apis`, `software.amazon.awssdk`, `org.wso2.*`.

**IP caldo durante i test locali:** run ripetute sulla stessa macchina accumulano request history sull'IP. Maven Central usa finestre di throttle lunghe (1–4 ore), quindi anche a concurrency=3 + delay=400ms l'IP può rimanere in stato di throttle per tutta la sessione di test. In produzione questo non accade perché le run sono distanziate di 24 ore e l'IP è sempre freddo tra un pass e l'altro. Per verificare se l'IP è throttlato: `curl -I https://repo1.maven.org/maven2/org/wso2/carbon/identity/framework/application-mgt/maven-metadata.xml` — risposta 403 immediata conferma il throttle.

### Partial Maven Central Deploys

Occasionally a publisher's CI/CD updates `<release>` in `maven-metadata.xml` before the `.pom` is fully propagated to all Central mirrors. These appear as `maven_error` on the first pass and usually resolve on the next periodic refresh.

---

## Configuration Reference

**All variables are required** — `getMavenConfig()` (`config.ts`) calls `requireEnv` for each,
so the worker throws on startup if any is missing. Suggested values shown.

| Env var | Suggested | Description |
|---------|-----------|-------------|
| `POM_FETCHER_BATCH_SIZE` | `50` | Packages per batch — critical phase |
| `POM_FETCHER_CONCURRENCY` | `5` | Concurrent fetches — critical phase |
| `POM_FETCHER_NON_CRITICAL_BATCH_SIZE` | `500` | Packages per batch — non-critical phase |
| `POM_FETCHER_NON_CRITICAL_CONCURRENCY` | `20` | Concurrent writes — non-critical DB-only phase |
| `POM_FETCHER_REFRESH_DAYS` | `1` | Staleness window — re-sync a package once its `last_synced_at` is older than N days (applies to both phases) |
| `POM_FETCHER_GROUP_DELAY_MS` | `200`–`400` | Delay between concurrent groups in the critical phase (rate-limit mitigation) |

### Sync source (Temporal critical path)

These select **where the critical sync gets its work from**. They affect only the Temporal
`processMavenCriticalBatch` activity — the standalone backfill loop is unaffected. All are
optional; unset/invalid values fall back to the current universe-polling behaviour.

| Env var | Default | Description |
|---------|---------|-------------|
| `MAVEN_SYNC_SOURCE` | `maven` | `maven` = poll `packages_universe` by staleness (current behaviour). `api` = enrich only what the delta feed reports. `both` = run both passes per tick. |
| `MAVEN_DELTA_API_URL` | — | Base URL of our delta feed (e.g. the Railway deployment). **Required** when source is `api` or `both`. |
| `MAVEN_DELTA_API_TOKEN` | — | Optional bearer token for the delta feed. |
| `MAVEN_DELTA_API_PAGE_SIZE` | `100` | Page size for `/api/changes` pagination. |
| `MAVEN_DELTA_API_LOOKBACK_MINUTES` | `15` | Rolling window size: each tick fetches `[now-N, now)`. Overlaps the cron interval on purpose — re-processing is safe (idempotent upserts). |
| `MAVEN_DELTA_API_INCLUDE_PRERELEASE` | `false` | Forwarded as `includePrerelease` to the feed. |

The delta-API path always runs **full extraction** (the feed is an explicit "this changed"
signal) and only enriches packages that are `is_critical` in `packages_universe`; non-critical
purls in the feed are dropped.

**Concurrency guidance:** Maven Central handles 10–15 concurrent requests per IP without throttling. Retry logic with exponential backoff handles 429/403s. Keep `POM_FETCHER_CONCURRENCY` ≤ 5 locally — repeated local runs heat the IP (see [Known Exceptions](#maven-central-403-rate-limiting)).

---

## Performance

Observed on ~2K packages (local dev, Maven Central over the network):

| Phase | Mode | Throughput | Notes |
|-------|------|------------|-------|
| Non-critical | DB-only | ~1000 pkg/sec | Pure DB writes, no HTTP |
| Non-critical | direct-pom | ~25 pkg/sec | 2 HTTP requests/pkg: metadata.xml + POM |
| Critical | full-pom | ~15–25 pkg/sec | Faster when packages share parent POMs (CDN cache warm) |

**Estimated time for ~800K packages (≈18% critical):**

| Phase | Packages | Estimated time |
|-------|----------|---------------|
| Non-critical (DB-only) | ~670K | ~12 min |
| Critical (full POM, first extraction) | ~150K | several hours |

The first critical extraction is the expensive part — run it with the standalone backfill
loop. Afterwards the Temporal schedules keep things incremental: non-critical re-syncs cheaply
every `POM_FETCHER_REFRESH_DAYS`, and critical packages are re-fetched only when a new release
is published (the Temporal path skips unchanged versions)
or once their refresh window elapses.

Under Temporal **each tick processes one batch** within its workflow timeout (15 min critical /
5 min non-critical); the backlog is drained over many ticks, not in one long pass. To go
faster, raise `POM_FETCHER_BATCH_SIZE` / `POM_FETCHER_CONCURRENCY` (keep concurrency ≤ 15 to
avoid Maven Central throttling) or trigger the schedule manually.

---

## Scheduling

Two Temporal schedules are registered on startup of `bin/packages-worker.ts`
(see `maven/schedule.ts`):

| Schedule ID | Cron | Workflow | Activity | Workflow timeout |
|-------------|------|----------|----------|------------------|
| `maven-critical` | `*/5 * * * *` (every 5 min) | `mavenCriticalWorkflow` | `processMavenCriticalBatch` → one critical batch | 15 min |
| `maven-non-critical` | `*/10 * * * *` (every 10 min) | `mavenNonCriticalWorkflow` | `processMavenNonCriticalBatch` → one non-critical batch | 5 min |

Both: overlap policy `SKIP` (a tick is dropped if the previous run is still active),
catchup window 1 hour, retry 3× (30s initial, 2× backoff).

**Each tick processes a single batch** (`POM_FETCHER_BATCH_SIZE` /
`POM_FETCHER_NON_CRITICAL_BATCH_SIZE`), not a full pass — the queue is drained incrementally
across ticks.

To run a batch on demand instead of waiting for the cron, trigger the schedule from the
Temporal UI (the schedule's **Trigger** button) or the CLI:

```bash
temporal schedule trigger --schedule-id maven-critical
```

---

## Known Data Anomalies

### High version counts

Maven packages released via automated CI/CD pipelines (every commit or every day) accumulate thousands of versions on Central. Observed examples on a 10K sample:

| Package | Versions |
|---------|----------|
| io.joern/x2cpg_3 | ~2 166 |
| org.cdk8s/cdk8s | ~1 749 |
| io.joern/semanticcpg_3 | ~2 077 |
| org.janusgraph/* (×15 artifacts) | ~795 each |

`maven-metadata.xml` `<versions><version>` lists **every version ever published**, including each snapshot, alpha, RC, and automated patch. On a 10K package run this produced ~3.8M rows in the `versions` table (~1 375 versions/package on average).

This is correct data, not a bug. The high cardinality is expected given Maven's publishing model and is useful for `versions_count`, `first_release_at`, and `is_prerelease` derivation. `published_at` (pending Sonatype) will complete the picture.

To inspect the distribution:

```sql
SELECT
  width_bucket(cnt, 0, 3000, 10) AS bucket,
  min(cnt)                        AS min_versions,
  max(cnt)                        AS max_versions,
  count(*)                        AS packages
FROM (SELECT package_id, count(*) AS cnt FROM versions GROUP BY package_id) t
GROUP BY bucket ORDER BY bucket;
```

### Low repo coverage for non-critical packages

On the same 10K sample only ~3–4% of packages produced a `repos` row. The root cause is that `<scm>` is frequently absent from the direct POM and inherited from a parent POM instead (Apache parent, Spring parent, Google parent, etc.). Since non-critical packages use direct-POM fetch without parent resolution, those SCM URLs are null and no repo row is written.

Coverage breakdown by ingestion source:

```sql
SELECT p.ingestion_source, count(p.id) AS packages, count(pr.id) AS with_repo
FROM packages p
LEFT JOIN package_repos pr ON pr.package_id = p.id
WHERE p.ecosystem = 'maven'
GROUP BY p.ingestion_source
ORDER BY packages DESC;
```

Expected behaviour:
- `maven` (critical, full resolution) → high repo coverage
- `maven_direct` (non-critical, no parent resolution) → low repo coverage
- `maven_not_on_central` / `maven_error` → no repo (no POM data)

Repo coverage will grow naturally as the critical package set expands and as non-critical packages hit their 30-day POM refresh window.

---

## Pending: Sonatype Integration

The following fields require data from the Sonatype API and are not yet populated:

- `packages.status` — deprecated flag
- `packages.versions_count` — count of published releases
- `packages.first_release_at` — timestamp of first release
- `packages.latest_release_at` — timestamp of most recent release
- `versions.published_at` — per-version release timestamp
- `downloads_last_30d` — 30-day rolling download count
