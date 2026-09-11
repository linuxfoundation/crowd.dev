---
name: fix-vulns-git-integration
description: >
  Automated vulnerability triage and dependency bumps for the git_integration
  worker (Python via uv + the two vendored Go modules). Fetches open Dependabot
  alerts scoped to git_integration manifests, correlates them with open
  Dependabot PRs, classifies each fix as safe or needs-human, validates safe
  fixes locally on the PR branches merged with latest main (uv sync / lint /
  pytest + full git-integration image build, which compiles both Go modules),
  updates safe PR branches server-side so they are one click from merge, then
  posts a "safe to merge" review summary to Slack and opens PRs for alerts
  Dependabot has no PR for. NEVER merges PRs, never dismisses alerts, never
  deploys without explicit user confirmation. Use when the user says "fix vulns",
  "bump deps", "dependabot triage", or asks about git_integration
  vulnerabilities.
allowed-tools: Bash, Read, Glob, Grep, Agent, AskUserQuestion, WebFetch, ToolSearch
---

<!-- Copyright The Linux Foundation and each contributor to LFX. -->
<!-- SPDX-License-Identifier: MIT -->

# Fix Vulnerabilities — git_integration

Triage Dependabot alerts for `services/apps/git_integration`, validate the
safe fixes locally, and hand humans a reviewed merge list via Slack.

**v1 scope — only these manifests:**

| Manifest | Ecosystem | Toolchain |
|---|---|---|
| `services/apps/git_integration/uv.lock` (+ `pyproject.toml`) | pip | uv |
| `services/apps/git_integration/src/crowdgit/services/vulnerability_scanner/go.mod` | go | go |
| `services/apps/git_integration/src/crowdgit/services/software_value/go.mod` | go | go |

Alerts on any other manifest (pnpm-lock.yaml, frontend, etc.) are out of
scope: mention the count in the final report, do nothing else with them.

## Hard rules

- **NEVER merge any PR.** Not Dependabot's, not ones this skill opens. The
  output for merge-ready PRs is a Slack review message, the merge is a
  human's.
- **NEVER dismiss or modify Dependabot alerts.**
- **Never trust repo CI as validation for this worker** — repo CI does not
  run the Python worker's tests. A green Dependabot PR proves nothing here;
  local validation is the only gate.
- **Uncertainty is always `needs-human`, never `safe`.**
- **No major version bumps in the safe tier**, ever.
- Deploy dispatch only on explicit user confirmation, and staging before
  production.
- Requires a clean `git status --porcelain` for the git_integration paths
  before applying any local fix.

## Phase 0 — Preconditions & scope

1. `gh auth status` works and `git status --porcelain` is clean for
   `services/apps/git_integration/`.
2. `uv` is on PATH (`uv --version`) and Docker is running (`docker info`) —
   Go and image-level validation happen by building the real git-integration
   image, no local go toolchain needed. If Docker is down, packages can
   still be triaged/classified but anything not fully validated goes in the
   Slack message under needs-human with "validation not run", never under
   safe to merge.
3. Ask the user (AskUserQuestion, multiselect): which severities to address —
   critical, high, medium/low — and which Slack channel the review summary
   should go to (free text via "Other" is fine). Default severities:
   critical + high.

## Phase 1 — Fetch alerts and Dependabot PRs

```bash
gh api 'repos/linuxfoundation/crowd.dev/dependabot/alerts?state=open&per_page=100' --paginate \
  -q '.[] | {n:.number, sev:.security_advisory.severity, ghsa:.security_advisory.ghsa_id,
      cve:.security_advisory.cve_id, eco:.dependency.package.ecosystem,
      pkg:.dependency.package.name, manifest:.dependency.manifest_path,
      range:.security_vulnerability.vulnerable_version_range,
      patched:.security_vulnerability.first_patched_version.identifier}'
```

Filter to the three in-scope manifests and the selected severities. Dedupe by
(package, manifest) with the package name lowercased — alerts mix casings
(`GitPython` vs `gitpython`). Keep every GHSA but plan one fix per package —
the target version is the highest `first_patched_version` across its alerts;
alerts with no patched version are `needs-human` immediately.

Before triaging a package, check main's current lockfile/go.mod: if the
installed version already satisfies the patched version, the alert is stale
(Dependabot hasn't rescanned since a merge) — drop it from the run and note
it in the report.

```bash
gh pr list --repo linuxfoundation/crowd.dev --author app/dependabot --state open \
  --limit 100 --json number,title,headRefName,url
```

Match PRs to packages via `headRefName` (patterns:
`dependabot/uv/services/apps/git_integration/<pkg>-<ver>`,
`dependabot/go_modules/services/apps/git_integration/src/crowdgit/services/<module>/<pkg>-<ver>`).
A package may have a PR whose version is older than the needed patched
version — note that; the PR still counts as "exists" but the gap goes in the
report.

## Phase 2 — Classify each package

Determine for every in-scope package:

- **direct vs transitive** — pip: listed in `pyproject.toml` dependencies or
  only in `uv.lock`; go: in the module's `go.mod` require block without
  `// indirect`.
- **bump size** — patch/minor vs major (current version from `uv.lock` /
  `go.mod`, target = patched version).
- **usage surface** — grep `services/apps/git_integration/src/crowdgit` for
  imports/usage of the package. For >3 packages, fan out one Explore agent
  per package in parallel; each must return positive evidence, not absence
  of doubt: where it's used, whether the changelog between current and target
  versions (WebFetch the release notes) mentions breaking changes or changed
  APIs the code touches.

Verdicts:

- `safe`: patch/minor bump, patched version exists, and either transitive or
  direct with no breaking-change signal touching our usage.
- `needs-human`: major bump, no patched release, breaking-change signal,
  heavy direct usage of changed APIs (expect GitPython and aiohttp to land
  here often), or any uncertainty.

## Phase 3 — Validate safe candidates locally

Work through safe packages sequentially. All commands run from
`services/apps/git_integration` (Python) or the Go module dir (Go).

**Existing Dependabot PRs**: validate them together, not one image build per
PR. Create a throwaway local branch off latest main, then for each safe
candidate PR merge its head into it
(`git fetch origin <headRefName> && git merge FETCH_HEAD --no-edit`) —
Dependabot branches are often weeks behind and validating the stale base
tests the wrong code; the combined merge also catches cross-bump conflicts
that per-PR validation misses. On merge conflict, drop that PR from the
combined branch, mark it `needs-human` ("PR needs rebase"), and continue.
Run the full validation suite once on the combined state; if it fails,
bisect by re-validating PRs individually (still merged with main). Then
return to the original branch and delete the throwaway branch. Never push
local commits to any PR branch — local merges are validation-only.

For PRs that end up in the safe tier: first re-check the PR is still open
(`gh pr view <n> --json state` — a teammate may have merged it mid-run),
then make it one click from merge by updating the branch server-side:
`gh api -X PUT repos/linuxfoundation/crowd.dev/pulls/<n>/update-branch`
(equivalent to the "Update branch" button; a 422 "head ref does not exist"
usually means the PR was just merged — re-check its state).

**No PR** (typical for transitive pip deps — Dependabot often only alerts):
apply the fix on a new branch off main:

- pip transitive: `uv lock --upgrade-package '<pkg>'` (add
  `--upgrade-package '<pkg>==<target>'` if it overshoots into a major).
- pip direct: bump the constraint in `pyproject.toml`
  (`uv add '<pkg>>=<target>'`), then `uv lock`.
- go: `go get <pkg>@v<target> && go mod tidy` in the module dir.

Verify the vulnerable version is gone from the lockfile/go.mod, then validate.

**Validation suite**

- Python: `uv sync --group dev`, `uv run ruff check src/`, and
  `uv run pytest src/test/ -v` — skip/ignore tests that require live repos,
  credentials, or network (anything reading `TEST_REPO_NAME`/env creds);
  collect-only first (`--collect-only`) to see what's runnable. Add
  `--ignore=<path>` for every untracked file under `src/test/` (from
  `git status --porcelain`) — local WIP tests can import code that doesn't
  exist on the branch under validation and fail collection spuriously.
- Container build (validates Go and the shipped Python env on the real
  toolchain): from `services/apps/git_integration`, run

  ```bash
  docker compose -f ../../../scripts/services/git-integration.yaml build git-integration-dev
  ```

  (`make rebuild`). The Dockerfile compiles both Go modules from the bumped
  go.mod/go.sum and does `uv sync --frozen` from the bumped uv.lock — a
  failed build fails the package. This is the only Go gate; Go-only bumps
  don't need the host pytest suite, but always run the container build even
  for pip-only bumps.

On failure: for a local fix, revert that package's changes
(`git checkout -- .` scoped to the touched files) and mark it
`failed-validation`; for a PR checkout, just mark it. Continue the batch —
never let one failure abort the rest, never leave a half-applied fix in the
tree.

**Ship local fixes**: one branch per run, `chore/git-integration-vuln-bumps`
(suffix with date if taken), one commit per package
(`chore(deps): bump <pkg> from <old> to <new> in git_integration`), push and
open a PR titled `chore(deps): git_integration vulnerability bumps` whose
body lists GHSA/CVE per package and the validation evidence. Do not enable
auto-merge.

## Phase 4 — Slack review summary

Load Slack tooling with ToolSearch (`select:mcp__claude_ai_Slack__slack_send_message`).
Show the drafted message in the terminal, get user approval, then send to the
channel chosen in Phase 0. Never send without that approval, and if a
summary covering substantially the same findings was already posted (a
re-run, or iterating on the skill), don't post again — terminal report only.
Before listing a PR as safe to merge, re-confirm it is still open. Structure:

- Header: git_integration vuln triage, date, alert counts by severity.
- **Safe to merge** — each validated Dependabot PR: PR link, package
  old→new, GHSA/CVE + severity, one-line evidence (validation green,
  transitive/patch-only, usage summary).
- **New PR opened** — link to the batch PR from Phase 3, same detail.
- **Needs human** — package, severity, why (major bump / breaking signal /
  failed validation / no patched release).
- Footer: out-of-scope alert count remaining on other manifests.

## Phase 5 — Deploy (optional, gated)

Only if the user, after merges happened (they merge, not this skill), asks to
deploy: `gh workflow run lf-oracle-staging-deploy.yaml -f services="git-integration"`.
Production (`lf-oracle-production-deploy.yaml`) only on a second explicit
confirmation after staging is verified.

## Final report (terminal)

| GHSA/CVE | Package | Manifest | Old → New | Verdict | Action | Validation |
|---|---|---|---|---|---|---|

Plus: Slack message link/status, PR opened (if any), needs-human reasoning,
and the count of out-of-scope alerts left on other manifests.
