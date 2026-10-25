# Review guide

How code review works in this repository and what a reviewer, human or agent, is expected to find. The `/review` skill reads this file before every review, and the local push gate refuses to push a change that has not passed one.

## Prerequisites

The `/review` skill and the push gate run inside Claude Code. The gate intercepts the pushes Claude runs through its Bash tool; a push from a terminal, an IDE or another agent is not gated, and the human review tiers below apply to every pull request regardless.

- Claude Code 2.1.295 or newer (the gate hook relies on `onFailure: block`)
- `jq` on `PATH` (the gate refuses every push without it)
- git 2.31 or newer
- A `main` branch on the `origin` remote, or `CDP_REVIEW_BASE=<remote>/main` exported when working from a fork

## What failure looks like here

CDP ingests millions of activities a day and spends most of its effort cleaning that data. A change fails when it:

- **Corrupts or loses data.** A wrong merge or unmerge, an identity attached to the wrong member or organization, activities dropped or duplicated, a write that is not idempotent and gets replayed by a Temporal or Kafka retry.
- **Does not scale.** A query that scans without an index, an N+1 against PostgreSQL or OpenSearch, an unbounded `IN` list or result set, per-row round trips where a batch exists, a Temporal activity that cannot finish inside its timeout at production volume.
- **Fails silently.** A caught error that is logged and swallowed, partial success reported as success, a Kafka message or Temporal activity acknowledged without doing the work, a zero count returned from a path that stopped reporting.
- **Breaks a contract.** A public API response shape, an activity type name, a Tinybird sorting key, an edit to an applied migration, a change to a shared `data-access-layer` or `common` function whose other callers were not checked.
- **Reintroduces a pattern in transition** (see `AGENTS.md`): Sequelize, class-based services or repositories, multi-tenant branching beyond `DEFAULT_TENANT_ID`, legacy JWT auth, a public endpoint without a Zod schema and `validateOrThrow`.
- **Exposes something.** A secret in code or logs, SQL built from strings, an auth check bypassed, a user-supplied URL fetched server-side.
- **Fixes the symptom, not the cause.** On a bug fix, the reviewer asks whether the root cause is addressed or only the reported instance.

## Tags

Every finding carries exactly one tag:

- **Important.** Must be fixed, or explicitly declined with a reason, before the change is pushed. Blocks the push gate.
- **Nit.** Worth fixing, never blocks. At most 5 per review; the rest are dropped, not deferred.
- **Pre-existing.** The defect lives in code the change did not introduce (it is present at the merge base). Reported once, never blocks, never repeated in later rounds.

## Evidence

A finding without a file and line is not a finding. Each one states the concrete scenario: these inputs or this state lead to this wrong outcome. "Could be a problem" is not a scenario. Style a linter or formatter would catch is not a finding. A finding that only restates a rule from `AGENTS.md` without showing the violating line is not a finding.

## Depth by path

The deepest tier touched by any changed file sets the depth of the whole review.

| Depth    | Paths                                                                                                                                                                                                                                                                                                 | Why                                        |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| deep     | `services/libs/data-access-layer/**`, `services/libs/common/**`, `backend/src/database/migrations/**`, `services/apps/*/migrations/**`, `services/apps/data_sink_worker/**`, anything named `*merge*` or `*unmerge*`, `backend/src/api/**`, `backend/src/services/auth/**`, `backend/src/security/**` | One missed bug costs days of data cleanup. |
| quick    | `*.md`, `docs/**`, `.claude/**`, `.github/**`, `scripts/**`, `**/src/tmp/**`, test-only changes                                                                                                                                                                                                       | Low blast radius.                          |
| standard | Everything else                                                                                                                                                                                                                                                                                       |                                            |

Depth controls how many hunters run and how many verifiers vote on each finding (see the `/review` skill). `--depth` overrides it.

## Human review by risk

The local review decides whether a change may leave the machine. It never replaces a human on the pull request, and an agent's approval never counts as a human's.

| Risk   | Examples                                                                                                                         | Human approvals on the PR                                     |
| ------ | -------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| Low    | Docs, copy, `.claude/**`, tests only, additive changes behind a flag                                                             | One reviewer, light read                                      |
| Medium | Ordinary product code in `backend/`, `frontend/`, `services/apps/**`                                                             | One reviewer                                                  |
| High   | Migrations, shared `data-access-layer` and `common` functions, merge and unmerge, auth, `.github/workflows/**`, deploy manifests | One reviewer plus a code owner (`.github/CODEOWNERS`), always |

The change sets the tier, not the author. An approval covers the commit it was given on; new commits need a fresh look.

## Skip

Reviewers do not read or report on:

- `pnpm-lock.yaml`, generated files, vendored code, snapshot files
- `**/src/tmp/**`, `docs/superpowers/**`
- Pure formatting diffs (the output of `pnpm format`)

## Convergence

- Round 1 reviews the whole change. Rounds 2 and later review the whole current change again with earlier findings suppressed, and check that every earlier Important finding is fixed or declined.
- A finding already reported is never repeated. A defect that recurs across the change is reported once, as a class ("every caller of X"), so all instances get fixed together.
- After 3 rounds with Important findings still open, the reviewer stops and asks the author to reconsider the approach instead of producing round 4.

## Summary format

The first line of every review is the count:

```text
Review: 2 Important, 1 Nit, 1 Pre-existing (6 raised, 2 dropped by verifiers)
```

When nothing needs fixing, say exactly that:

```text
Review: nothing needs fixing (4 raised, 4 dropped by verifiers)
```

Each finding that follows has its tag, `file:line`, the scenario, and the suggested fix.

## When the reviewer keeps flagging the same thing

Fix it at the source: a lint rule, a line in `AGENTS.md`, a skill, or this file. Not another review comment.
