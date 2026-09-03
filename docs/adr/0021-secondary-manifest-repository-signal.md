# ADR-0021: Secondary manifest repository signal

**Date**: 2026-09-01
**Status**: accepted
**Deciders**: Joana Maia

## Context

Every registry writer only created a `package_repos` row when the ecosystem's
canonical repository field parsed — npm `repository`, cargo `repository`,
rubygems `source_code_uri`, NuGet `<repository>`, POM `<scm><url>`. A large
share of packages leave that field empty while publishing the same repo URL in
`homepage`, `bugs.url`, `projectUrl`, `bug_tracker_uri`, or the POM `<url>`, and
those packages ended up with no repo link at all — invisible to criticality,
blast radius, and Insights.

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
from an ordered candidate list. The first candidate is the ecosystem's canonical
field and resolves as `primary`; every later candidate resolves as `secondary`.
The result carries `{ repo, signal }`, and each writer persists the
returned `signal` on the link. No writer computes a confidence value.

### Chains

| Ecosystem | Chain |
| --- | --- |
| npm | `repository` → `homepage` → `bugs.url` |
| pypi | Source/Code project URL → `Homepage` → bug tracker URL |
| cargo | `repository` → `homepage` |
| rubygems | `source_code_uri` → `homepage_uri` → `bug_tracker_uri` |
| packagist | `support.source` → `homepage` |
| nuget | `<repository>` → `projectUrl` |
| maven | POM `<scm><url>` → POM `<url>` |

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

### Per-ecosystem fallback (no shared helper)

Each registry writer would duplicate its own fallback chain — npm already tried
`bugs.url`, packagist already tried `homepage`.

**Pros:** no new abstraction; each writer stays self-contained.
**Cons:** seven writers meant seven copies of the same ordered-candidate logic
and seven copies of the host-gate rule; adding a new ecosystem requires knowing
which copy to follow.
**Why not:** the duplication already caused drift (packagist's host gate existed
alone; npm had no gate at all); a single helper enforces the rule once.

### Accept all fallback fields at `primary` confidence

Widen each writer to try secondary fields but record every link as `primary`.

**Pros:** simpler persistence — no signal column needed.
**Cons:** free-form fields (`homepage`, `projectUrl`) are noisy; ranking a
documentation site equal to an explicit `<scm>` declaration inflates wrong
links and corrupts the confidence score distribution.
**Why not:** ADR-0020 already reserved the `signal` column and its −0.10 penalty
specifically to express this distinction; a flat approach would abandon that
mechanism before it could be used.

## Consequences

### Positive

- Packages that only publish their repo in a secondary field now get a link,
  and the link is honestly labelled as weaker.
- The fallback order and the host gate exist once, so adding an ecosystem means
  declaring a candidate list.
- Per-run counters (`primary_field_hit`, `fallback_hit_by_field`, `no_signal`)
  make the coverage uplift measurable against the pre-merge baseline.

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
  missing on the true repo but present on a fork.** Mitigation: ADR-0020's
  fork and archived penalties, plus ADR-0022's ownership evidence, which
  penalises the fork's owner mismatch far more heavily than the secondary
  penalty.
- **Recognized-host gating rejects legitimate self-hosted repos found in a
  fallback field.** Accepted deliberately: an unrecognized host in a free-form
  field carries no signal that it is a repository at all. Revisit if the
  `no_signal` counters show a material self-hosted tail.
- **Coverage growth is hard to attribute after the fact.** Mitigation: record
  per-ecosystem `package_repos` row counts before merge, and compare against
  the `fallback_hit_by_field` counters afterwards.
