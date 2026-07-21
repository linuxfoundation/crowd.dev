# ADR-0009: Security contacts — tiered extraction, confidence scoring, and Temporal batch ingestion

**Date**: 2026-07-21
**Status**: accepted
**Deciders**: Mouad BANI

_Consolidated ADR for the security-contacts worker — record further security-contacts decisions here rather than opening new ADRs._

## Context

The Akrites effort needs a single, confidence-rated answer to "who do we contact about a
vulnerability in this package?" for every repo linked to an `is_critical` package. Today that
information is scattered across registry manifests, repo files (`SECURITY.md`,
`SECURITY-INSIGHTS.yml`, `SECURITY_CONTACTS`, `security.txt`), and GitHub API state (private
vulnerability reporting), with no cross-ecosystem standard. Much of it is noise: RFC 2606
placeholder emails, templated SECURITY.md files linking to generic GitHub docs, registry usernames
that are only *guessed* to be GitHub logins, and bot/AI-agent accounts among top committers. The
data is consumed by the public `/v1/akrites/packages/detail` endpoint, so it must carry enough
provenance and confidence signal for a downstream security team to trust — or discount — each
contact. The worker follows the ADR-0001 §Worker architecture pattern
(`services/apps/packages_worker/src/security-contacts/`, own entrypoint, shared image).

## Decision

Build the security-contacts sub-worker as a **Temporal-scheduled batch ingestion** (daily cron at
06:00, `ScheduleOverlapPolicy.SKIP`, one 500-repo batch per activity, `continueAsNew` until a batch
comes back empty, 24 h execution timeout across the chain), plus a separate **on-demand
single-purl workflow** invoked by the akrites API when it hits a never-evaluated repo. Each repo
runs a **tiered extractor hierarchy (A > B > D)**; results are reconciled, scored into a `[0, 1]`
confidence with a banded label, and persisted to `security_contacts` (keyed by `repo_id`,
soft-delete semantics) plus policy columns on `repos`.

### Tiered extractor hierarchy

| Tier | Source | Extractor |
| ---- | ------ | --------- |
| A1 | `SECURITY-INSIGHTS.yml` (root, `.github/`, `.gitlab/`) | `securityInsights.ts` |
| A2 | GitHub private vulnerability reporting status (authed) | `pvr.ts` |
| A3 | `SECURITY_CONTACTS` / `OWNERS` (k8s-style) | `securityContactsFile.ts` |
| A4 | RFC 9116 `security.txt` on the project homepage | `securityTxt.ts` |
| B1 | `SECURITY.md` (root, `.github/`, `docs/`) | `securityMd.ts` |
| B2 | Registry manifests — npm, PyPI, Maven, Cargo, NuGet, RubyGems, Composer | `extractors/registry/` |
| D | Top-3 committers (last 90 days) + repo owner profile; static Go ecosystem fallback | `topCommitters.ts`, `repoOwner.ts`, `registry/go.ts` |

Supporting decisions:

- **One git-tree fetch per repo** (`gitTree.ts`) is shared by all file-probing extractors, instead
  of each extractor blind-probing well-known paths with 404s.
- **A2 vetoes B1's PVR guess**: when the authed API authoritatively reports PVR disabled, a
  "use GitHub PVR" mention parsed out of SECURITY.md is dropped. The PVR call is skipped entirely
  for known-archived repos (GitHub rejects them with 422).
- **Maven walks the parent-POM chain, but only group-related parents**: developer emails often
  live only in a shared parent POM (`jackson-bom`, `commons-parent`, …), so a leaf POM that yields
  none triggers traversal up the parent chain — restricted to parents in a related groupId.
  Generic convenience parents (`oss-parent`, `spring-boot-starter-parent`) are excluded: they
  would donate maintainers unrelated to the actual project. Apache-convention obfuscated emails
  ("ggregory at apache.org") are deobfuscated on the way.
- **Tier D is a gated last resort**: committers and owner are fetched only when no usable
  (non-junk, reachable) higher-tier contact exists. Bot and AI-agent committer accounts
  (`dependabot`, `renovate`, `github-actions`, `claude`, `copilot`, …, plus `[bot]` suffixes) are
  excluded.
- **Go packages get a static ecosystem fallback**: Go has no registry manifest carrying contact
  metadata (the module proxy and pkg.go.dev expose none), so the Go fetcher makes no HTTP call and
  always emits two tier-D `security-team` contacts per the Go security policy —
  `security@golang.org` and the `https://g.co/vulnz` report form. At tier D they never outrank a
  real repo-level contact, and they don't suppress the committers/owner gate (which only checks
  for usable contacts above tier D), so a Go repo is never left with an empty answer.
- **Tier C (CODEOWNERS / repo admins / org owners) is not implemented** — the type system reserves
  it, but tier D's committers + owner covered the "no contact at all" gap first. C remains a
  follow-up if precision between B and D proves insufficient.
- Extractors run under `Promise.allSettled`; one failing source never sinks the repo
  (`status: 'partial'` changes the write semantics — see below).

### Scoring

`score = 0.55·tier + 0.20·channel + 0.15·freshness + 0.10·corroboration − penalties`, computed by
the pure function `score.ts` and banded by `securityContactConfidenceBand` (PRIMARY ≥ 0.80,
SECONDARY ≥ 0.55, FALLBACK ≥ 0.30, else NONE).

- Tier: A = 1.0, B = 0.7, C = 0.4, D = 0.2.
- Channel: `security@`-style local-parts = 1.0, PVR = 0.95, generic local-parts = 0.7, individual
  email = 0.6, URL/web-form = 0.5, bare GitHub handle = 0.4.
- Freshness: linear 1.0 (≤ 90 d) → 0.0 (≥ 730 d) on `declaredAt ?? fetchedAt`.
- Corroboration: 2 independent extractors = 0.5, 3+ = 1.0. CDP identity lookups never count —
  they re-attest the same value, they are not independent sources.
- Penalties: handle-only −0.05; CDP-unverified-email-only −0.25 plus a 0.35 channel quality.

Weights are the spec's starting point; calibration against a hand-labeled set is a post-rollout
follow-up.

### Reconciliation and noise filtering

`reconcile.ts` runs: junk filter (RFC 2606 placeholder domains, generic hosts like
`docs.github.com`/`dependabot.com`, localhost) → exact-match merge on `channel + normalized value`
(provenance concatenated, highest role/tier wins) → identity-link merge collapsing a bare
`github-handle` into an email that carries the same explicit `handle` field (never matched by
display name — two people can share a name) → provenance dedup → score → stable sort
(score, role priority, tier, value). Email contacts additionally carry a
`reachable`/`reachability_reason` classification from `@crowd/common`.

### Handle verification and CDP email resolution

Two distinct trust problems, two mechanisms:

- **Registry usernames are only candidates.** RubyGems/NuGet owner names are *guessed* GitHub
  logins; `verifyHandleCandidates.ts` confirms a candidate only when the same login owns the repo
  or appears in its top-100 contributors. Unconfirmed candidates are dropped entirely.
- **Confirmed handles are resolved to emails through CDP's identity graph**
  (`resolveCdpEmails.ts`, read-only connection to the CDP database): verified emails are emitted
  with source `cdp-verified`; unverified ones with `cdp-unverified` and the scoring penalty above.
  GitHub noreply emails (`…@users.noreply.github.com`) are parsed back into handles first so they
  ride the same resolution path.

### GitHub API access layer

All GitHub calls go through one rate-limit-aware gateway (`githubToken.ts`), reusing the
enricher's GitHub App auth and `InstallationPool`:

- An **app-wide semaphore caps concurrent GitHub requests at 50** — GitHub's secondary limit
  rejects bursts above ~100 per app, and repo-level concurrency (100) would otherwise multiply
  into far more in-flight calls.
- **Primary rate limits park the offending installation** until its reset and rotate to another;
  **secondary limits are waited out** (`retry-after`), since they are app-wide and switching
  installations cannot help.
- **Absent is not an error**: 404/410/422/451 return a null body so extractors treat "file/repo
  not there" as a normal outcome instead of a failure that marks the repo `partial`.
- **No App configured → unauthenticated fallback** with a warning, so local/dev runs still work
  at the unauthenticated quota.

### Write semantics and refresh cadence

- **Soft-delete, not replace**: a full pass marks the repo's active rows `deleted_at = NOW()`,
  then bulk-upserts on `(repo_id, channel, value)` with `deleted_at = NULL` — rediscovered
  contacts are revived in place, disappeared ones stay soft-deleted. Readers filter
  `deleted_at IS NULL`.
- **Partial passes merge only**: when an extractor failed, the soft-delete step is skipped — a
  source that wasn't consulted cannot wipe contacts it didn't see. Stale rows are cleaned on the
  next fully-successful pass.
- **Batched, chunked persistence**: extraction results for the whole batch are collected in memory
  and written in chunks of 100 repos, one transaction per chunk. Per-repo transactions at repo
  concurrency 100 exhausted the packages-db pool and were the measured sweep bottleneck. A failing
  chunk only re-extracts its own 100 repos next sweep; remaining chunks are still attempted.
- **Cadence via `repos.contacts_last_refreshed`**: never evaluated → always eligible; evaluated
  with no contacts → retry after 20 h (just under the daily tick); has contacts → refresh after
  156 h (just under weekly). Failed repos are marked attempted so the sweep always advances.

### Scheduling and the on-demand path

The batch activity heartbeats on a fixed 30 s cadence (a single slow repo can outlast the 2-minute
heartbeat timeout even while all slots are busy) and checks the Temporal cancellation signal so a
superseded attempt stops instead of racing its retry. The on-demand workflow
(`ingestSecurityContactsForPurlWorkflow`) uses a short 45 s activity timeout — a caller awaiting an
API cache miss must not hang. It selects the same best-repo the read side surfaces (mirroring the
`getPackageDetailByPurl` LATERAL), applies **no `is_critical` filter** (non-critical purls are
exactly what the path exists for) and **no host filter** (non-GitHub repos degrade gracefully;
filtering would leave them permanently NULL and re-trigger the path on every request).

On the read side, the API contract distinguishes **`securityContacts: null`** (repo never
evaluated — `contacts_last_refreshed IS NULL`, which is what triggers the on-demand ingest) from
**`[]`** (evaluated, nothing found — no re-ingest until the daily retry). `packageConfidence` is
derived at read time as the band of the highest contact score, not stored.

## Alternatives Considered

### Alternative 1: Standalone polling-loop worker (the `github-repos-enricher` pattern)
- **Pros**: simplest runtime; the original implementation plan specified it; proven pattern in
  this service; no Temporal coupling.
- **Cons**: scheduling, retry, overlap protection, and run observability all hand-rolled; no
  natural home for the synchronous on-demand single-purl path the API needs.
- **Why not**: ADR-0001 already designates Temporal (`workflows.ts`/`activities.ts`/`schedule.ts`,
  `SKIP` overlap) as the standard for ingestion sub-workers, with the enricher's loop explicitly
  marked as legacy-to-migrate. The on-demand purl workflow settled it — it needs a client-invocable,
  awaitable execution, which Temporal gives for free and a polling loop does not.

### Alternative 2: Hard `DELETE` + `INSERT` per repo (the plan's original write model)
- **Pros**: simplest idempotent recompute; no `deleted_at` filtering for readers.
- **Cons**: a partial pass (one failed extractor) wipes contacts the failed source discovered
  earlier; row identity churns every sweep; per-repo transactions at concurrency 100 overwhelm a
  pool sized for far fewer connections.
- **Why not**: an extractor outage (e.g. a registry API down for a day) would silently erase good
  contacts fleet-wide. Soft-delete + upsert keeps history, lets partial passes merge safely, and
  the chunked batch write removed the measured persistence bottleneck.

### Alternative 3: Trust registry owner usernames as GitHub handles directly
- **Pros**: no extra GitHub API call; more contacts surfaced.
- **Cons**: a RubyGems/NuGet username and a GitHub login are separate namespaces — an unrelated
  person or bot can hold the same name.
- **Why not**: emitting a wrong person as a *security contact* is worse than emitting nothing.
  Corroboration against the repo's contributors/owner costs one API call per repo and removes the
  collision class entirely.

### Alternative 4: Always emit tier D committers and owner
- **Pros**: maximal coverage; no gating logic.
- **Cons**: floods well-documented repos with low-confidence individual contacts; surfaces
  individuals who never volunteered for security contact duty; wastes two GitHub calls per repo.
- **Why not**: tier D exists only to avoid an empty answer. When a usable A/B contact exists, the
  committers add noise, not signal — the gate keeps D rows out of exactly the repos that don't
  need them.

### Alternative 5: Resolve handles via GitHub public profile email only (no CDP lookup)
- **Pros**: no cross-database read into CDP; single data source.
- **Cons**: most GitHub profiles expose no public email, so most confirmed handles would remain
  handle-only contacts (channel quality 0.4) that a security team cannot actually write to.
- **Why not**: CDP's identity graph already links GitHub handles to verified emails at meaningful
  coverage. Both paths are used — public profile email where present, CDP resolution on top —
  with the verified/unverified distinction preserved in provenance and scoring.

## Consequences

### Positive
- One queryable, confidence-banded contact source per repo, with full per-contact provenance
  (source, path, fetch/declared timestamps) — consumers can audit why any contact exists.
- Idempotent, self-advancing sweep: failed repos are marked attempted, failed chunks re-extract in
  isolation, and re-runs converge instead of duplicating.
- The on-demand path fills coverage for non-critical purls lazily, exactly when the API needs them.
- Pure scoring/reconcile functions are unit-tested in isolation (`__tests__/`).

### Negative
- GitHub API budget: tree fetch, PVR check, contributor verification, and tier D lookups consume
  the shared GitHub App token pool alongside the enricher.
- Readers must remember `deleted_at IS NULL`; the soft-delete convention is enforced only by review.
- Cross-database coupling: CDP email resolution needs a read connection to the CDP database; an
  outage degrades (logged, contacts kept unresolved) but coverage silently drops.
- Scoring weights and the 20 h/156 h cadences are judgment values, not calibrated ones.
- Tier C is a hole in the hierarchy: repos with CODEOWNERS but no security files skip straight
  from B to committers.

### Risks
- **Wrong-person contact despite corroboration** — a top committer or repo owner is not
  necessarily a security contact. Mitigated by tier D's 0.2 tier score, the `committer`/`org-owner`
  roles, and confidence bands that push these to FALLBACK; consumers are expected to respect bands.
- **Single-writer assumption** — `writeContacts` takes no lock; correctness relies on the Temporal
  schedule (SKIP overlap) and heartbeat-based supersession keeping one writer per repo. A future
  second caller (e.g. a backfill script) must respect this or add locking.
- **Source-format drift** — SECURITY-INSIGHTS schema versions, registry API shapes, and GitHub's
  `stats/contributors` 202-polling behavior all change over time. Extractor isolation limits blast
  radius to one source; fixture-based tests catch parser regressions.
