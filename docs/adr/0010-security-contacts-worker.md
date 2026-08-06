# ADR-0010: Security contacts — tiered extraction, confidence scoring, and Temporal batch ingestion

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

Computed by the pure function `score.ts`, after reconciliation — so a merged contact is scored
with its highest surviving tier and role and the union of its provenance:

```
raw   = 0.55·tier + 0.20·channel + 0.15·freshness + 0.10·corroboration
        − 0.25 (if CDP-unverified)  − 0.05 (if channel is github-handle)
score = round(clamp(raw, 0, 1) · 1000) / 1000        -- stored as NUMERIC(4,3)
```

**Tier** — A = 1.0, B = 0.7, C = 0.4, D = 0.2.

**Channel quality** — evaluated in this order:

1. A CDP-unverified contact (see penalty rules below) gets a flat **0.35** regardless of channel.
2. `email`: local-part in the security set (`security`, `secure`, `psirt`, `sirt`, `cert`, `cve`,
   `abuse`, `vuln`, `vulnerability`, `vulnerabilities`, `disclosure`) *or any local-part starting
   with `security`* = **1.0**; local-part in the generic set (`info`, `team`, `contact`, `hello`,
   `hi`, `support`, `admin`, `help`, `maintainers`, `dev`, `devs`, `opensource`, `open-source`,
   `office`, `mail`) = **0.7**; anything else (an individual's address) = **0.6**. Matching is on
   the lowercased, trimmed local-part.
3. `github-pvr` = **0.95**; `url` / `web-form` = **0.5**; `github-handle` = **0.4**.

**Freshness** — takes the **most recent** `declaredAt ?? fetchedAt` across all provenance
entries: 1.0 at ≤ 90 days, 0.0 at ≥ 730 days, linear in between. No parseable timestamp at all
scores **0** — unknown age is treated as stale, not fresh.

**Corroboration** — counts **distinct provenance `source` identifiers**, so it measures
independent extractors, not the same file fetched twice: 3+ sources = 1.0, exactly 2 = 0.5,
1 = 0. `cdp-*` sources are excluded from the count: a CDP identity lookup re-attests a value we
already had, it is not an independent attestation.

**Penalties**:

- **Handle-only −0.05** (`channel === 'github-handle'`): a bare handle is not directly
  contactable; the penalty keeps it just below an equivalent resolved email for the same person.
- **CDP-unverified −0.25** (plus the 0.35 channel quality above): applies only when **every**
  provenance entry is `cdp-unverified`. A single `cdp-verified` resolution — or any real
  extractor source, e.g. the same email independently found in a manifest — lifts the penalty
  entirely.

**Confidence band** (`securityContactConfidenceBand` in the data-access-layer, applied to the
rounded score): PRIMARY ≥ 0.80 > SECONDARY ≥ 0.55 > FALLBACK ≥ 0.30 > NONE. Both `score` and the
band are stored per contact; `packageConfidence` on the API is the band of the max score.

Worked examples:

- `security@project.org` from a fresh `SECURITY-INSIGHTS.yml`, corroborated by `security.txt`:
  0.55·1.0 + 0.20·1.0 + 0.15·1.0 + 0.10·0.5 = **0.95 → PRIMARY**.
- An individual maintainer email from a 1-year-old npm manifest, single source:
  0.55·0.7 + 0.20·0.6 + 0.15·0.57 + 0 ≈ **0.59 → SECONDARY**.
- A tier-D committer's email resolved only via an unverified CDP identity, fresh, no
  corroboration: 0.55·0.2 + 0.20·0.35 + 0.15·1.0 + 0 − 0.25 = **0.08 → NONE**.

**Reachability is orthogonal to the score**: `reachable` / `reachability_reason` (from
`classifyEmailReachability` in `@crowd/common`) are stored alongside but never feed the formula —
they gate the tier-D fallback (an unreachable email doesn't count as a "usable" higher-tier
contact) and let consumers filter, without hiding that a source declared the address.

Weights, penalties, and band cut-offs are the spec's starting point; calibration against a
hand-labeled set is a post-rollout follow-up.

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


## Addendum (2026-07-29): Vulnerability reporting protocol

Adds a sister data model answering "**how** does this project expect external vulnerability
reporting?" per repo — distinct from security contacts, which answer *who*. The source of truth
is what the project itself declared: security files from the enricher's `repo_well_known_files`
inventory, the pages they link to, and the authoritative `pvr_enabled` flag. Inferred contacts
from `security_contacts` never blend in as if declared; they appear only as clearly-labeled
fallback when nothing was declared.

### Volume and parser split (prod analysis, 2026-07-28)

Of 114,045 critical GitHub repos, 10,349 (9.1%) have a security file: 10,495 files collapsing
to 6,125 distinct blobs (top-20 shared blobs cover ~1,900 repos of boilerplate). A probe over
all 6,120 reachable blobs showed **69.2% deterministically resolvable** (a single declared
method, or several with exactly one preference-cued), **14% pointer-only** (the file is just a
link to an external policy page), **21.6% with conditional routing** ("only email if a GHSA is
not possible"), **53% with negation language** ("do NOT open a public issue"), 2.3% GitHub
default template. Volume is not the constraint; precision is — hence **hybrid,
deterministic-first**: the classifier fully settles clean blobs, an LLM handles the residue and
prose fields, and a deterministic validator gates every LLM write.

### Data model

- **`security_policy_parses`** — content-keyed parse cache. PK `blob_oid` (git blob oid for
  files; sha256 of the URL for linked pages, so two URLs with identical content stay
  independently joinable from `linked_urls`), `source_kind`
  (`security-file`/`linked-page`), `url` (linked-page rows), `parser`
  (`deterministic`/`llm`), `parser_version`, `status` (`ok`/`template`/`degraded`), `parsed`
  JSONB (methods + guidelines), `linked_urls`. Identical content across repos is parsed once,
  ever; a `parser_version` bump is a targeted re-parse, not a migration.
- **`repo_reporting_protocols`** — assembled per-repo answer. PK `repo_id`, `declared`,
  `methods` JSONB (ordered array of `{type, status, endpoint, condition, confidence,
  provenance}`), `guidelines` JSONB, `sources` JSONB, `assembled_at`. Method `type` ∈
  github-pvr | email | web-form | bounty-platform | security-txt | mailing-list; `status` ∈
  preferred | accepted | fallback | prohibited (`prohibited` captures negation language);
  `confidence` ∈ declared | inferred. Plain upsert — fully derived and recomputable, no
  soft delete.

### Parse stage (blob-driven)

`repo_well_known_files` is the work queue (live `security` rows for critical GitHub repos whose
`blob_oid` lacks a parse at the current version); this pipeline never probes repos for files.
Blobs are fetched once by oid through the shared GitHub gateway. The classifier (same
windowing family as the B1 extractor) emits a `clean` verdict — single usable method, or
exactly one preference-cued among several, no conditional language, negation on a method's own
line marks it `prohibited` — which is stored as-is. Residue goes to the LLM; the validator
requires every emitted endpoint to appear in the source (URLs verbatim; emails also via
deobfuscation normalization — "security at python dot org"), valid enums, and at most one
`preferred` — failures are stored `status='degraded'` (classifier partials, no guidelines).
The LLM can never invent a channel. Pointer-only parses record up to 3 linked URLs; each
linked page is fetched once per URL (SSRF-guarded: http(s) only, private/loopback/link-local
and metadata hosts blocked, redirects revalidated per hop, body capped at 500 KB while
streaming) and parsed as a `linked-page` row. For a pointer-only blob the file row is written
only after every linked page has a parse row, so a transient page failure leaves the blob
unmarked and the next daily sweep retries the whole unit. Batches are drawn in random order so
permanently failing blobs cannot starve the queue.

### Assembly

Repos are re-assembled when inputs change (no protocol row, `contacts_last_refreshed` newer
than `assembled_at`, or a newer parse for one of their blobs). Merge rules: `ok`/`template`
parses contribute methods and guidelines with provenance — **`degraded` parses contribute
nothing**; `pvr_enabled = true` adds a `github-pvr` method when the files are silent, and
`pvr_enabled = false` **vetoes** a declared github-pvr method (the A2-vetoes-B1 rule applied
to the protocol); github-pvr sentinel endpoints are rewritten per repo to
`…/security/advisories/new`; dedup on type+endpoint; at most one `preferred`; sort preferred >
accepted > fallback > prohibited. Only when nothing is declared: up to 3 `inferred`/`fallback`
methods derived from live `security_contacts` (email, github-pvr, web-form channels, by score).
Every repo in the population gets a row — `declared=false` with an empty `methods` array for
the ~89 no-signal repos.

### LLM contract

Direct AWS Bedrock calls (`@aws-sdk/client-bedrock-runtime`, module-local in `llmExtract.ts`)
— deliberately **not** the legacy class-based `LlmService` in `common_services` (class pattern
+ prompt-history DB coupling) and **not** a shared provider-agnostic lib speaking to a LiteLLM
proxy (built during implementation, then dropped: no LiteLLM infra today; revisit if CDP
standardizes multi-provider LLM infrastructure — schema and prompt carry over unchanged).
Existing `CROWD_AWS_BEDROCK_ACCESS_KEY_ID`/`CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY` credentials;
default `LlmModelType.CLAUDE_HAIKU_4_5` with region from `LLM_MODEL_REGION_MAP`. The JSON
schema is embedded in the system prompt (Bedrock InvokeModel has no structured-output mode);
`parseLlmJson` parses the answer. Missing credentials or any failure → `degraded` parse, never
a thrown error. No prompt-history persistence.

### Scheduling

Own Temporal schedule `reporting-protocol-ingestion` (daily 07:00, `SKIP` overlap, 24 h
execution timeout) inside the security-contacts worker, independent of the contacts schedule so
a slow LLM pass never stalls contact ingestion. The workflow drains parsing first
(`continueAsNew` while a batch parsed anything; an all-failed batch falls through to assembly
instead of recursing — failed blobs get no row and retry on the next daily tick), then drains
assembly. Batch sizes: 200 blobs (parse), 2,000 repos (assemble). Both activities ride the
shared 30-minute proxy and heartbeat on a fixed 30 s cadence under its 2-minute
`heartbeatTimeout`.

### Deferred

Other interaction-profile domains (contribution intake, governance, maintainer roster,
communication channels, code of conduct — the content-keyed cache and section pattern extend
to them); org-level `.github` default files (GitHub serves them for repos without their own
SECURITY.md; the inventory doesn't capture them — measure the gap first); non-GitHub declared
parsing (no file inventory; such repos assemble as `declared=false` + inferred fallback); API
exposure on the akrites endpoints.
