# ADR-0021: Secondary manifest repository signal

**Date**: 2026-09-01
**Status**: accepted
**Deciders**: Joana Maia

## Context

Most registry writers only created a `package_repos` row when the ecosystem's
canonical repository field parsed — npm `repository`, cargo `repository`,
rubygems `source_code_uri`, NuGet `<repository>`, POM `<scm><url>`. PyPI and
NuGet already had ad hoc fallbacks onto `homepage`/`projectUrl`, but wrote them
as `primary` with no record of which field won. Everywhere else, a package
that left the canonical field empty while publishing the same repo URL in
`homepage`, `bugs.url`, `bug_tracker_uri`, or the POM `<url>` ended up with no
repo link at all — invisible to criticality, blast radius, and Insights.

Simply widening each writer to accept any of those fields would trade
under-coverage for wrong links: fallback fields are free-form, so
`https://example.com/docs/getting-started` canonicalizes into a plausible
`owner/repo` shape without being a repository. Fallback links also should not
rank equally with a declared one.
[ADR-0020](./0020-package-repo-confidence-scoring.md) already reserved the
`signal` column and its −0.10 penalty for exactly this.

## Decision

One shared helper, `resolveManifestRepo(candidates)`
(`packages_worker/src/utils/resolveManifestRepo.ts`), resolves a package's repo
from an ordered candidate list — the first candidate is `primary`, every later
one is `secondary` — and each writer persists the returned `signal` on the link.

### Chains

| Ecosystem | Chain                                                  |
| --------- | ------------------------------------------------------ |
| npm       | `repository` → `homepage` → `bugs.url`                 |
| pypi      | Source/Code project URL → `Homepage` → bug tracker URL |
| cargo     | `repository` → `homepage`                              |
| rubygems  | `source_code_uri` → `homepage_uri` → `bug_tracker_uri` |
| packagist | `support.source` → `homepage`                          |
| nuget     | `<repository>` → `projectUrl`                          |
| maven     | POM `<scm><url>` → POM `<url>`                         |

### Host gate

Candidates go through the shared `canonicalizeRepoUrl`. A `secondary` candidate
is rejected when canonicalization yields `host === 'other'` — recognized VCS
hosts only. The `primary` candidate keeps its historical behaviour and still
accepts `other`, so existing links to self-hosted Gitea, cgit, and SVN are
unaffected. Packagist already applied this gate locally; it is now the shared
rule.

Cargo is the exception on mechanics, not on policy: its pipeline is set-based
SQL over a dump, so `normalizeRepos` stages both `declared_repository_url` and
`homepage` into `repo_norm`, and a new `repo_choice` table applies the same
first-wins-with-host-gate rule in SQL. `documentation` is not staged — it is
almost always docs.rs, which the host gate rejects anyway.

## Alternatives Considered

### Alternative 1: Widen each writer's existing extractor in place

- **Pros**: no new module; smallest diff per ecosystem.
- **Cons**: seven copies of the fallback order and the host gate, which is how
  the current per-ecosystem divergence arose in the first place; the `signal`
  value would be derived independently in each writer.
- **Why not**: the whole point is one rule; nine implementations of "which
  field won" is the defect, not the fix.

### Alternative 2: Accept fallback URLs on any host, like the primary field does

- **Pros**: maximum coverage; no URL is discarded.
- **Cons**: a documentation site or a marketing page with two path segments
  becomes a repo link, creating a `repos` row and an incorrect
  `packages_published` attribution — the exact failure this epic exists to fix.
- **Why not**: coverage gained by inventing repos is negative value; the
  primary field at least carries the publisher's explicit claim.

### Alternative 3: Score fallback links lower directly in the writers instead of adding `signal`

- **Pros**: no schema column; visible in one place.
- **Cons**: reintroduces per-writer confidence literals, and the penalty could
  not be retuned or audited afterwards — nothing records _why_ a row scored
  lower.
- **Why not**: ADR-0020 makes the stored score derivable from stored evidence;
  `signal` is that evidence.

### Alternative 4: Backfill a separate pass that mines fallback fields for packages with no link

- **Pros**: zero risk to the existing write paths; can be re-run at will.
- **Cons**: a second code path that has to re-fetch or re-read every manifest,
  and it goes stale the moment a package is re-ingested.
- **Why not**: the data is already in hand at write time; the write path is
  the cheapest place to fix coverage.

## Consequences

### Positive

- Packages that only publish their repo in a secondary field now get a link,
  and the link is honestly labelled as weaker.
- The fallback order and the host gate exist once, so adding an ecosystem means
  declaring a candidate list.
- `package_repos.signal` is directly queryable (`GROUP BY signal`) after merge, making
  the primary-vs-secondary coverage split measurable against the pre-merge baseline.

### Negative

- Secondary links are, by construction, less certain than declared ones; some
  will be wrong even with the host gate.
- Maven and cargo needed local restructuring (maven's POM-specific
  `normalizeScmUrl` feeds the same `package_repos` write path; cargo's
  `repo_choice` applies the host-gate rule in SQL) to reach the same behaviour.
- Row counts in `package_repos` grow, and the dedup/keep-highest path now sees
  more competing links per package.

### Risks

- **A secondary link can outrank a genuine one when the declared field is
  missing on the true repo but present on a fork.** Currently mitigated only
  by ADR-0020's fork and archived penalties. Ownership evidence that would
  penalise the fork's owner mismatch far more heavily than the secondary
  penalty is planned under CM-1394, not yet implemented.
- **Recognized-host gating rejects legitimate self-hosted repos found in a
  fallback field.** Accepted deliberately: an unrecognized host in a free-form
  field carries no signal that it is a repository at all. Rejected candidates
  aren't persisted anywhere, so revisiting this needs an ad hoc sample of
  homepage/bug_tracker fields against unrecognized hosts, not a live counter.
- **Coverage growth is hard to attribute after the fact.** Mitigation: record
  per-ecosystem `package_repos` row counts before merge, and compare against
  post-merge `signal = 'secondary'` counts from the same table.
- **Rows written by the pre-signal PyPI/NuGet fallback logic are backfilled as
  `primary`, overstating their confidence until re-ingestion.** Accepted for
  now: those packages get re-scored naturally on their next registry sync;
  no separate backfill migration is planned.
- **Maven's unchanged-version fast path (`runMavenEnrichmentLoop.ts`) skips
  POM re-extraction, so a stable package already on the old writer never
  gains a homepage-derived secondary link until it next changes version or a
  forced full extraction runs.** Accepted: forcing full extraction for every
  unchanged package on rollout would multiply POM-fetch volume; existing
  packages backfill gradually as they publish new versions.
