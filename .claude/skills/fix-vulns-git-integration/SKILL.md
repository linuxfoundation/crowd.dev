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

**Non-interactive invocation**: only when the invocation itself explicitly
passes both parameters (e.g.
`/fix-vulns-git-integration non-interactive severities=critical,high channel=#my-channel`),
skip the questions and use them; the Slack summary is then the run's output
and is sent without interactive approval. Every other gate still holds: no
merging, no deploys, and if a summary covering the same findings was already
posted, post nothing. This skill ships with no schedule and no default
channel — anyone who wants periodic runs sets up their own scheduler passing
their own parameters.

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
(`GitPython` vs `gitpython`). Keep every GHSA but plan one fix per
(package, manifest) pair — the same package can be vulnerable in both Go
modules at different versions and each go.mod needs its own bump; the target
version is the highest `first_patched_version` across that pair's alerts.
Alerts with no patched version are `needs-human` immediately.

Before triaging a package, check main's current lockfile/go.mod: if the
installed version already satisfies the patched version, the alert is stale
(Dependabot hasn't rescanned since a merge) — drop it from the run and note
it in the report.

```bash
gh api 'repos/linuxfoundation/crowd.dev/pulls?state=open&per_page=100' --paginate \
  -q '.[] | select(.head.ref | startswith("dependabot/"))
      | {number, title, headRefName: .head.ref, headSha: .head.sha, url: .html_url}'
```

(`gh pr list` truncates at its `--limit`; paginate so old git_integration PRs
— often the stalest — are never missed and mistaken for "no PR". Record each
PR's `headSha`: it is the revision validation applies to.)

Match PRs to packages via `headRefName` (patterns:
`dependabot/uv/services/apps/git_integration/<pkg>-<ver>`,
`dependabot/go_modules/services/apps/git_integration/src/crowdgit/services/<module>/<pkg>-<ver>`).
Read the version each PR actually resolves from its diff (go.mod / uv.lock),
not from the branch name alone. A PR resolving below the required patched
version can never be `safe` — the alert would stay open; it still counts as
"exists" but goes in the report as a gap. A PR resolving above the advisory's
minimum shifts the review window: Phase 2 classification and changelog review
must cover current → the PR's resolved version, not just the advisory
minimum.

## Phase 2 — Classify each package

Determine for every in-scope package:

- **direct vs transitive** — pip: direct if listed anywhere in
  `pyproject.toml` — `[project.dependencies]` **or** `[dependency-groups]`
  (dev deps are still direct); transitive only if it appears solely in
  `uv.lock`. go: direct if required without `// indirect`, whether inside a
  `require (...)` block or a standalone single-line `require`.
- **bump size** — patch/minor vs major, measured from the current version
  (`uv.lock` / `go.mod`) to the version the fix actually lands: the PR's
  resolved version when a PR exists, else the advisory's patched version. A
  PR that lands on a major is a major bump — never safe — regardless of how
  small the advisory's minimum patched version is.
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
that per-PR validation misses. On merge conflict, don't write the PR off:
drop it from the combined branch and validate it individually against main
instead — two pip PRs both rewrite `uv.lock`, so a conflict between PRs is
expected and meaningless; only a PR that also conflicts with main alone is
`needs-human` ("PR needs rebase"). Run the full validation suite once on the
combined state; if it fails, bisect by re-validating PRs individually (still
merged with main). Then return to the original branch and delete the
throwaway branch. Never push local commits to any PR branch — local merges
are validation-only.

A combined green validates the PRs as a set. That is what the Slack message
must present: "safe to merge together". If reviewers may cherry-pick only
some of them, either validate those individually first or say in the message
that the batch was validated jointly.

For PRs that end up in the safe tier: re-check the PR is still open and its
head is still the revision that was validated
(`gh pr view <n> --json state,headRefOid` — a teammate may have merged it,
or Dependabot may have force-pushed a new revision mid-run; if the head SHA
changed, revalidate before calling it safe). Then make it one click from
merge by updating the branch server-side:
`gh api -X PUT repos/linuxfoundation/crowd.dev/pulls/<n>/update-branch -f expected_head_sha=<validated sha>`
(equivalent to the "Update branch" button; a 422 "head ref does not exist"
usually means the PR was just merged, and a 422 mentioning the expected head
SHA means the head moved — re-check state and revalidate). The update creates
a new head: a merge of the validated SHA with main. That is the same state
validated locally **only if main has not advanced since the validation
fetch** — record main's SHA when creating the throwaway branch, then right
before calling update-branch run `git fetch origin main` (fetching only the
PR head does not refresh `origin/main`, and without this fetch the
comparison always trivially matches) and compare the recorded SHA against
the fresh `origin/main`; if main moved, re-merge and revalidate first. The Slack message must reference the
validated head SHA and note that the branch was then updated with that same
main.

**No PR** (typical for transitive pip deps — Dependabot often only alerts):
apply the fix on a new branch off main:

- pip transitive: `uv lock --upgrade-package '<pkg>==<target>'` — pin to the
  reviewed target; classification (changelog review) only covered versions up
  to it.
- pip direct: bump the constraint in `pyproject.toml`
  (`uv add '<pkg>>=<target>'`), then `uv lock`.
- go (no host toolchain — run in the image the Dockerfile pins):

  ```bash
  docker run --rm -v "$PWD":/w -w /w golang:1.25-alpine \
    sh -c 'go get <pkg>@v<target> && go mod tidy'
  ```

Verify the resolved version in the lockfile/go.mod is exactly the reviewed
target — if the resolver landed on anything newer, re-run Phase 2
classification against that version before treating the fix as safe.

**Validation suite**

- Python: `uv sync --frozen --group dev` (frozen so validation never rewrites
  the branch's committed `uv.lock` — an inconsistent lockfile must fail, not
  be silently regenerated before the image build), `uv run ruff check src/`,
  and
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
(suffix with date if taken), one commit per package following the repo's
commit workflow — `git commit --signoff -S` (DCO + signing are required, the
Probot DCO check blocks unsigned commits) with message
`chore(deps): bump <pkg> from <old> to <new> in git_integration (CM-XXX)`.
Ask the user for the JIRA key — the commit workflow requires a `CM` ticket
even for untracked work, so if none exists ask the user to create one. With
a key, open the PR titled
`chore(deps): git_integration vulnerability bumps (CM-XXX)`. Without one
(user declines, or non-interactive run), push the branch but do **not** open
the PR — report it as "branch pushed, PR pending JIRA ticket"; never open a
draft to sidestep the title lint. The PR body lists GHSA/CVE per package and the
validation evidence. Do not enable auto-merge.

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
