# Maven POM Fetcher

Worker that syncs Maven package metadata from Maven Central into the `packages` DB.
Runs as a standalone entry point inside `packages_worker` on a daily Temporal schedule (4am UTC).

---

## Architecture: Two-Tier Fetch

All Maven packages in `packages_universe` are processed in two sequential phases per pass.

### Phase 1 — Non-Critical

| Mode | Trigger | What happens |
|------|---------|-------------|
| `POM_FETCHER_DIRECT_POM_FOR_ALL=false` (default) | `last_synced_at > 1 day` | DB-only: copies universe stats (criticality, downloads, dependents) into `packages`. No HTTP. |
| `POM_FETCHER_DIRECT_POM_FOR_ALL=true` | `last_synced_at > 30 days` | Fetches `maven-metadata.xml` + root POM (no parent chain). Populates description, homepage, SCM, maintainers, versions. |

### Phase 2 — Critical

Always active. Full POM extraction with parent chain resolution (max 5 hops). Runs when:
- Package not yet in `packages` table
- `ingestion_source IN ('maven_index', 'packages_universe')` — not yet POM-enriched
- New version released (`latest_release_at > last_synced_at`)
- Periodic full refresh (`last_synced_at > 90 days`)

**Why two tiers?** Parent POM resolution is the expensive part — it requires multiple HTTP requests per package (up to 5 extra fetches). Running it on millions of non-critical packages every day is not feasible. For non-critical packages a single direct-POM fetch is sufficient; for critical packages the extra cost is justified by data quality.

---

## Coverage Matrix (`POM_FETCHER_DIRECT_POM_FOR_ALL=true`)

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
| license | package-level license applied to all versions⁴ | ✅ best-effort¹ |
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

> ¹ **best-effort**: field is populated when declared in the direct POM. If inherited from a parent POM (common for `licenses`, `<scm>` in Apache/Spring/Google projects), it is null for non-critical packages.
>
> ² **full resolution for critical**: parent chain is followed (max 5 hops), so inherited fields are resolved correctly.
>
> ³ **prerelease regex**: matches `-SNAPSHOT`, `-alpha`, `-beta`, `-rc`, `-M[0-9]+` (case-insensitive).
>
> ⁴ **license per version**: the package-level license (first entry from POM `<licenses>`) is applied to all versions. Per-version POM fetches are not performed. This is an approximation — Maven licenses rarely change between versions.

---

## `ingestion_source` Values

| Value | Meaning |
|-------|---------|
| `pom_fetcher` | Critical — full POM + parent resolution succeeded |
| `pom_fetcher_direct` | Non-critical — direct POM fetch succeeded (no parent resolution) |
| `pom_fetcher_not_on_central` | `maven-metadata.xml` not found on `repo1.maven.org` — artifact is hosted on a third-party repository (e.g. WSO2 Nexus, JBoss, Atlassian). Universe data came from an aggregator (deps.dev, OSV). |
| `pom_fetcher_no_version` | `maven-metadata.xml` found but `<release>` is empty — artifact has no stable release |
| `pom_fetcher_error` | `maven-metadata.xml` has a release version but the `.pom` file for that version is a 404. Typical cause: partial deploy to Maven Central (metadata updated, artifact not uploaded) or Eclipse P2 feature artifacts that don't publish a standard POM. |
| `pom_fetcher_rate_limited` | Maven Central returned 403/429 on all retry attempts. Package will be retried on the next pass. |
| `packages_universe` | Non-critical, DB-only mode (`POM_FETCHER_DIRECT_POM_FOR_ALL=false`) — only universe stats copied |

---

## Known Exceptions

### WSO2 (`org.wso2.*`)

WSO2 publishes some artifacts exclusively to their own Nexus at `maven.wso2.org`. A subset of their artifacts appear in `packages_universe` (sourced from deps.dev/OSV which aggregates all Maven repositories) but are **not** available on `repo1.maven.org/maven2`.

Affected pattern: `org.wso2.carbon.*` — specifically `.feature` Eclipse P2 artifacts and `.stub` artifacts. These are written with `ingestion_source = 'pom_fetcher_not_on_central'` and are not retried until the next `nonCriticalPomRefreshDays` window.

### Eclipse P2 Feature Artifacts (`*.feature`)

Eclipse/OSGi feature artifacts (e.g. `org.wso2.carbon.identity.xacml.server.feature`) are packaged as `.zip` files, not `.jar`. Some publishers update `maven-metadata.xml` on Central without uploading the corresponding `.pom`. These land in `pom_fetcher_error`. No fix is possible without the publisher correcting their CI/CD pipeline.

### Maven Central 403 rate limiting

Maven Central (`repo1.maven.org`) restituisce 403 come meccanismo di throttle oltre al canonico 429. Il comportamento è gestito a due livelli:

1. **Retry con backoff esponenziale** — 403 e 429 vengono ritentati fino a 3 volte (2s base, ×2 per tentativo). Gestito in `getWithRetry` (extract.ts) e `resolveVersionsList` (metadata.ts).

2. **Fallback su DB** — se tutti i retry esauriscono, il pacchetto viene scritto con `ingestion_source = 'pom_fetcher_rate_limited'` e `last_synced_at = NOW()`, evitando loop infiniti. Verrà ritentato al prossimo ciclo di refresh.

**Causa root dei 403 persistenti:** `packages_universe` è ordinato per `rank_in_ecosystem`, quindi pacchetti dello stesso namespace (es. `com.google.apis`, `org.wso2`, `software.amazon.awssdk`) si raggruppano nel batch e colpiscono lo stesso CDN node di Maven Central in rapida successione. Il rate limit scatta sistematicamente dopo ~150–200 pacchetti processati.

**Fix applicato:** i batch HTTP vengono shufflati prima dell'esecuzione (`[...packages].sort(() => Math.random() - 0.5)`) per distribuire i namespace uniformemente nei gruppi concorrenti. Un delay configurabile tra i gruppi (`POM_FETCHER_GROUP_DELAY_MS`, default 200ms) riduce ulteriormente il rate di richieste.

Namespace noti per triggerare il rate limit a causa dell'alta densità di artefatti: `com.google.apis`, `software.amazon.awssdk`, `org.wso2.*`.

**IP caldo durante i test locali:** run ripetute sulla stessa macchina accumulano request history sull'IP. Maven Central usa finestre di throttle lunghe (1–4 ore), quindi anche a concurrency=3 + delay=400ms l'IP può rimanere in stato di throttle per tutta la sessione di test. In produzione questo non accade perché le run sono distanziate di 24 ore e l'IP è sempre freddo tra un pass e l'altro. Per verificare se l'IP è throttlato: `curl -I https://repo1.maven.org/maven2/org/wso2/carbon/identity/framework/application-mgt/maven-metadata.xml` — risposta 403 immediata conferma il throttle.

### Partial Maven Central Deploys

Occasionally a publisher's CI/CD updates `<release>` in `maven-metadata.xml` before the `.pom` is fully propagated to all Central mirrors. These appear as `pom_fetcher_error` on the first pass and usually resolve on the next periodic refresh.

---

## Configuration Reference

All variables are optional — defaults are shown.

| Env var | Default | Description |
|---------|---------|-------------|
| `POM_FETCHER_DIRECT_POM_FOR_ALL` | `false` | `true` = direct POM fetch for all packages + full resolution for critical |
| `POM_FETCHER_BATCH_SIZE` | `50` | Packages per batch — critical phase |
| `POM_FETCHER_CONCURRENCY` | `5` | Concurrent fetches — critical phase |
| `POM_FETCHER_FULL_REFRESH_DAYS` | `90` | Re-sync critical packages after N days |
| `POM_FETCHER_NON_CRITICAL_BATCH_SIZE` | `500` | Packages per batch — non-critical phase |
| `POM_FETCHER_NON_CRITICAL_CONCURRENCY` | `20` | Concurrent writes — non-critical DB-only mode |
| `POM_FETCHER_NON_CRITICAL_POM_CONCURRENCY` | `5` | Concurrent fetches — non-critical direct-pom mode |
| `POM_FETCHER_NON_CRITICAL_REFRESH_DAYS` | `1` | Re-sync non-critical stats after N days (DB-only) |
| `POM_FETCHER_NON_CRITICAL_POM_REFRESH_DAYS` | `30` | Re-sync non-critical POM data after N days (direct-pom) |
| `POM_FETCHER_IDLE_SLEEP_SEC` | `3600` | Sleep between passes |

**Concurrency guidance:** Maven Central handles 10–15 concurrent requests per IP without throttling. Retry logic with exponential backoff handles 429s. Keep `POM_FETCHER_CONCURRENCY` + `POM_FETCHER_NON_CRITICAL_POM_CONCURRENCY` ≤ 15 in production.

---

## Performance

Observed on ~2K packages (local dev, Maven Central over the network):

| Phase | Mode | Throughput | Notes |
|-------|------|------------|-------|
| Non-critical | DB-only | ~1000 pkg/sec | Pure DB writes, no HTTP |
| Non-critical | direct-pom | ~25 pkg/sec | 2 HTTP requests/pkg: metadata.xml + POM |
| Critical | full-pom | ~15–25 pkg/sec | Faster when packages share parent POMs (CDN cache warm) |

**Estimated time for 800K packages (10% critical) at default settings:**

| Phase | Packages | Estimated time |
|-------|----------|---------------|
| Non-critical (DB-only, first pass) | 720K | ~12 min |
| Non-critical (direct-pom, first pass) | 720K | ~8 h |
| Critical (full-pom, first pass) | 80K | ~3–4 h |

First pass is the expensive one. Subsequent daily passes are incremental:
- Non-critical DB-only: re-syncs all packages daily (~12 min)
- Non-critical direct-pom: re-syncs after 30 days (~8 h every 30 days)
- Critical: only packages with new versions or approaching 90-day refresh window

The daily Temporal schedule has a **12-hour workflow timeout**. With `POM_FETCHER_DIRECT_POM_FOR_ALL=true`, the first pass is within budget (~11–12 h); increase `POM_FETCHER_NON_CRITICAL_POM_CONCURRENCY` to 10 to halve non-critical time if needed.

---

## Scheduling

Runs daily at **4am UTC** via Temporal schedule `maven-pom-fetcher`.
Overlap policy: `SKIP` — if a previous run is still active, the new trigger is dropped.
Catchup window: 1 hour.
Workflow timeout: 12 hours.
Max retries: 3 (30s initial, 2× backoff).

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
- `pom_fetcher` (critical, full resolution) → high repo coverage
- `pom_fetcher_direct` (non-critical, no parent resolution) → low repo coverage
- `pom_fetcher_not_on_central` / `pom_fetcher_error` → no repo (no POM data)

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
