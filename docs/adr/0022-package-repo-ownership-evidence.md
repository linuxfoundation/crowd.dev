# ADR-0022: Ownership evidence for package→repo links

**Date**: 2026-09-01
**Status**: accepted
**Deciders**: Joana Maia

## Context

A package can name any repository it likes. Nothing in the ingestion path ever
checked whether the entity publishing the package has anything to do with the
entity owning the repo, so a package declaring `github.com/torvalds/linux`
produced a link indistinguishable from the kernel's own. That is how unrelated
packages inflate a repo's `packages_published` count and, through it, every
aggregate computed from those counts.

The evidence needed for the check is already ingested: npm scopes, Maven
groupIds, packagist vendors, Go module paths, and maintainer/owner/author logins
for most registries. [ADR-0020](./0020-package-repo-confidence-scoring.md)
reserved the `ownership_match` column and priced it — `unmatched` −0.25,
`no_evidence` −0.10 — so all that is missing is a producer for the value.

## Decision

`matchOwnership({ namespace, maintainers, repoOwner })`
(`packages_worker/src/utils/ownershipMatch.ts`) returns `matched`,
`unmatched`, or `no_evidence`, and every declared writer calls it before
persisting a link. Namespace evidence is checked first, maintainer logins are
the fallback. `no_evidence` is a distinct outcome from `unmatched`: a registry
that exposes nothing to compare must not be treated as a failed comparison.

### Normalisation

Identities are compared after `normalizeIdentity`: trim, lowercase, strip a
leading `@`, strip one trailing vanity suffix from `-ai`, `-io`, `-team`,
`-labs`, `-oss`, `-dev` (only when a suffix strip leaves more than one
character), then drop all non-alphanumerics. Reverse-DNS namespaces expand into
candidates — `org.projectlombok` yields its segments plus the joined form, so
`io.github.resilience4j` matches `resilience4j`. Two identities match on
equality or on prefix when the shorter is at least 4 characters, which is what
makes `tokio-rs` → `tokio` and `langchain-ai` → `langchain` match while `ab`
against `abcdef` stays `unmatched`.

### Per-ecosystem evidence

| Ecosystem | Namespace evidence | Maintainer evidence |
| --- | --- | --- |
| npm | package scope | `maintainers` |
| pypi | — | maintainer/author names |
| packagist | vendor from `name` | maintainers |
| nuget | — | owners + authors |
| maven | groupId | developer/contributor usernames |
| cargo | — | maintainer GitHub logins |
| go | module path owner, VCS hosts only | — |
| rubygems | — | none at the link-writing loop |

Go derives an owner only for module paths rooted at a known VCS host
(`github.com`, `gitlab.com`, `bitbucket.org`, `codeberg.org`, `gitea.com`,
`git.sr.ht`); a vanity module path would otherwise produce a false `unmatched`.
Rubygems stays `no_evidence` for now — owners are fetched in the critical loop,
not in the core loop that writes the link. Maven's backfill caller passes no
evidence and therefore yields `no_evidence`, not `unmatched`.

Cargo's pipeline is set-based SQL over a dump, so it gets a SQL twin of the
matcher — `package_repo_owner_key(text)` (IMMUTABLE) and
`package_repo_owner_match(repo_owner, candidates[])` — applied as a lateral
join against the staged maintainer logins, with the repo owner carried on
`cargo_sync.repo_choice` (see
[ADR-0021](./0021-secondary-manifest-repository-signal.md)).

## Alternatives Considered

### Alternative 1: Verify ownership against the forge (GitHub API) instead of registry metadata

- **Pros**: authoritative — the repo's actual owner, org membership, and
  whether the publisher can push.
- **Cons**: an API call per candidate link across millions of packages, with
  rate limits, and a hard dependency on the enricher having run first.
- **Why not**: the registry metadata is already in hand at write time and is
  enough to separate the obvious cases; forge verification can raise
  `matched` to a stronger tier later without changing this contract.

### Alternative 2: Treat "no evidence available" as `unmatched`

- **Pros**: one fewer state; simpler scoring table.
- **Cons**: every rubygems link and every Maven backfill row would be
  penalised as if a comparison had failed, which is a data-availability
  artefact, not a quality signal.
- **Why not**: it would push whole ecosystems below the medium label for
  reasons that have nothing to do with the link's correctness.

### Alternative 3: Reject `unmatched` links instead of scoring them down

- **Pros**: squatting links never enter the table at all.
- **Cons**: legitimate cases fail the heuristic — a package published under a
  personal account for a repo owned by a foundation, a rename, a monorepo
  vendor split — and those packages would silently lose their only link.
- **Why not**: a ranked link degrades gracefully; a dropped one is
  unrecoverable and invisible.

### Alternative 4: Implement the matcher only in TypeScript and stream cargo rows through Node

- **Pros**: a single implementation, unit-tested, no SQL duplication.
- **Cons**: the cargo enrich phase would round-trip millions of staged rows
  through the worker purely to compute a three-valued enum.
- **Why not**: cost is unacceptable for the bulk path. The SQL twin was
  validated read-only against packages-db case-by-case, including the `-io`
  suffix edge case, to confirm it agrees with `normalizeIdentity`.

## Consequences

### Positive

- Squatting links are separated from legitimate ones by score alone — the read
  side needs no filtering, and `ORDER BY confidence DESC LIMIT 1` starts
  returning the right repo for the epic's named cases.
- `no_evidence` carries a smaller penalty (−0.10) than `unmatched` (−0.25), so ecosystems with thin metadata are not scored as if they had actively mismatched the owner.
- The matcher is a pure function with standalone unit tests; the SQL twin
  mirrors it explicitly rather than approximating it.

### Negative

- Two implementations of one rule (TypeScript and SQL) that must stay in sync.
- Heuristic matching produces false `unmatched` for legitimate publisher/owner
  splits, costing those links 0.25.
- Rubygems and the Maven backfill contribute `no_evidence` until their loops
  are extended, so their links carry a −0.10 they could avoid.

### Risks

- **The TS and SQL normalisers drift, so cargo scores differently from every
  other ecosystem.** Mitigation: both were validated against the same inputs
  before merge; any change to the vanity-suffix list or the prefix rule must
  touch both, and the cargo integration path is the canary.
- **Vanity-suffix stripping over-matches, e.g. two distinct orgs collapsing to
  the same key.** Mitigation: the 4-character floor on prefix matching, and
  the penalty is a score adjustment rather than a hard rejection.
- **The `unmatched` distribution is unknown until the migration is applied.**
  Mitigation: record the matched/unmatched/no_evidence baseline per ecosystem
  before the rescore, so an unexpectedly large `unmatched` share is caught
  before the scores reach downstream consumers.
