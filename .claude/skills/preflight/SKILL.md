---
name: preflight
description: >
  Pre-PR validation — working tree status, format, lint, TypeScript check, and
  protected file check. Workspace-aware: detects which of backend/, frontend/,
  services/apps/*, services/libs/* were changed and runs only the relevant
  scripts. Use before submitting any PR, to check if code is ready, or to
  validate a branch before review.
allowed-tools: Bash, Read, Glob, Grep, AskUserQuestion
---

# Pre-Submission Preflight Check

Run each check in order. Report results clearly and help fix any issues found.

---

## Check 0: Working Tree Status

```bash
git fetch origin main
git status
git diff --stat origin/main...HEAD
git log --format="%h %s%n%b" origin/main...HEAD
```

Fetch first — every check below diffs against `origin/main`, so a stale local copy of it would give wrong commit/diff counts here and a wrong changed-package scope in Check 2–4.

**Evaluate:**

- **Uncommitted changes?** — Ask the contributor: commit now or stash?
- **No commits ahead of main?** — The branch has nothing to validate. Ask if they're on the right branch.
- **Commit messages missing JIRA key?** — Flag commits that don't include a `CM-XXX` reference.
- **Commits missing `--signoff`?** — Flag any commits without `Signed-off-by:` lines.

Resolve any issues before proceeding.

---

## Check 1: Detect Changed Workspaces

```bash
git diff --name-only origin/main...HEAD
```

Note whether `backend/`, `services/**`, and/or `frontend/` have changed files — used only to decide which of Checks 2–4 apply.

---

## Check 2–4: Lint, Format, TypeScript

**Backend + services** — one command covers lint, format-check, and tsc-check, with dependency-graph-aware fan-out (packages that depend on a changed lib are checked too) and automatic fallback to a full workspace check when shared config (eslint/prettier/tsconfig base, lockfile, workspace yaml) changed:

```bash
./scripts/cli lint-changed
```

Run this whenever `backend/` or `services/**` has changed files. Skip if neither changed.

The three checks run in order (lint → format-check → tsc-check) and stop at the first failure — same fail-fast behavior as the CI job. If lint fails, format/tsc did not run yet; fix and re-run to see the next check, same as watching CI re-run per push.

Fix any errors it reports before proceeding:
- Lint errors — unused imports, missing types, rule violations
- Format violations — fix with `pnpm format` (from the relevant workspace) then `git add -p`
- TypeScript errors — fix all before proceeding

**Frontend** — no dependency-graph tooling exists for it yet; run manually if `frontend/` has changed files:

```bash
cd frontend && npm run lint
```

Frontend has no `format-check` or `tsc-check` script — skip both.

---

## Check 5: Protected Files Check

```bash
git diff --name-only origin/main...HEAD
```

Flag any changes to these protected files — they should NOT be modified without code owner approval:

- `services/libs/data-access-layer/**` — shared DAL, high blast radius
- `services/libs/common/**` — shared utilities, affects all services
- `backend/src/database/migrations/**` — append-only Flyway migrations
- `scripts/cli`, `scripts/scaffold.yaml`, `scripts/scaffold.insights.yaml`
- `.husky/*`, `commitlint.config.js`
- `.github/workflows/**`, `.github/actions/**`
- `tsconfig*.json`, `.eslintrc*`, `.prettierrc*`
- `pnpm-lock.yaml`, `package.json`, `*/package.json`
- `CLAUDE.md`, `.claude/settings.json`

If protected files appear in the diff, warn the contributor and ask them to revert or get code owner approval.

---

## Check 6: Commit Verification

```bash
git log --format="%h %s%n%b" origin/main...HEAD
```

- **All changes committed?** — If not, remind them to commit.
- **Commit messages follow conventions?** — `type: description (CM-XXX)` format per `commit-workflow.md`.
- **`--signoff` on all commits?** — Every commit must have `Signed-off-by:`.
- **JIRA key referenced?** — PR title must contain a JIRA key (validated by CI at `.github/workflows/pr-title-jira-key-lint.yml`).

---

## Check 7: Change Summary

```bash
git diff --stat origin/main...HEAD
```

List:
1. **New files created** — with their purpose
2. **Modified files** — with what changed
3. **API changes** — any new or modified routes in `backend/src/api/`
4. **Migration changes** — any new Flyway migrations

---

## Results Report

Present a clear report:

```text
PREFLIGHT RESULTS
─────────────────────────────────────────────────
✓ Working tree        — Clean, N commits ahead of main
✓ Lint                — backend: OK | frontend: OK | services/...: OK
✓ Format check        — backend: OK
✓ TypeScript          — backend: OK
✓ Protected files     — None modified
✓ Commits             — Conventions followed, signed off, CM-XXX present
─────────────────────────────────────────────────
READY FOR PR
```

Or if there are issues:

```text
PREFLIGHT RESULTS
─────────────────────────────────────────────────
✓ Working tree        — Clean, N commits ahead of main
✗ Lint                — backend: 3 errors (see above)
✓ Format check        — backend: OK
✗ TypeScript          — backend: 1 error (see above)
✓ Protected files     — None modified
✗ Commits             — Missing Signed-off-by on 2 commits (run /dco)
─────────────────────────────────────────────────
ISSUES FOUND — Fix before submitting
```

### If All Checks Pass

Suggest creating the PR:

> "All preflight checks passed! Ready to create a PR. Would you like me to create it with `gh pr create`?"
