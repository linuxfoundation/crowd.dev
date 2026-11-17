# Release, dependency and security automation for crowd.dev and insights

Status: draft for approval · 2026-11-17 · Applies to `linuxfoundation/crowd.dev` and `linuxfoundation/insights`

## Intent

We want releases, dependency updates and security findings to be handled by automation instead of ad-hoc manual work. The process should stay structured (versions, release notes, changelog, tracked security work), and it must not slow the team down.

The LF release-engineering tooling (`lfreleng-actions/*`) and the LFX v2 services are the reference. We follow their practices where they fit a TypeScript monorepo that deploys many services from one `main` branch. We don't copy them where they would add process. Examples: per-service tags, interactive release CLIs, GitOps bump PRs.

## Where we are today

|                 | crowd.dev                                                                                                                            | insights                                                                                                          |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------- |
| Tags / releases | Last tag `v0.49.0` (2023). Stale draft releases (`v0.50.0`, 2× `v0.1.0`). `release-drafter.yml` exists but no workflow uses it       | None                                                                                                              |
| Deploy          | Manual `workflow_dispatch` (`lf-oracle-{staging,production}-deploy.yaml`) with a free-text service list. About 180 prod runs a month | `public-api-deploy.yaml` deploys to prod on every merge (with change detection). Staging and prod are also manual |
| Migrations      | Flyway, applied by hand through crowd-kube                                                                                           | Flyway, applied by hand through crowd-kube                                                                        |
| Dependabot      | No `dependabot.yml`; security updates only. 355 open alerts (4 critical, 143 high)                                                   | No `dependabot.yml`. 207 open alerts (1 critical, 60 high)                                                        |
| Code scanning   | CodeQL default setup: 30 open (6 critical, 11 high). 1 open secret-scanning alert                                                    | CodeQL default setup: 8 open (4 high)                                                                             |
| Claude Security | Not wired                                                                                                                            | Findings arrive as `security` issues in the ops repo and are fixed with the `fix-security-finding` skill          |
| Changelog       | None                                                                                                                                 | API docs changelog page only                                                                                      |
| PR conventions  | Squash merge; Conventional Commits title lint (`linuxfoundation/lfx-ui/_pr-title-lint.yml`). CODEOWNERS covers `/frontend/` only     | Same title lint. No CODEOWNERS, no PR template                                                                    |

lfx-changelog (`linuxfoundation/lfx-changelog`) has a GitHub App that ingests GitHub Releases from linked repos and drafts entries with AI. Our existing `changelog-automatic-publishing` cron publishes Insights and CDP drafts daily.

## Goals

1. Every merge to `main` gets a version tag. Every user-facing merge also gets a GitHub Release, published once its code is in production.
2. You can see which version of each service is running in staging and in production.
3. Released changes reach the LFX changelog with no manual step.
4. Dependency updates arrive grouped and on a predictable schedule. Low-risk ones merge themselves.
5. New critical or high vulnerabilities are caught on the PR. Existing ones are burned down and tracked against an SLA.
6. Security work from Claude Security (insights only, for now) and from Dependabot is tracked the same way.

## Non-goals (this project)

- **Changing how we deploy or migrate.** Deploys stay as the existing GitHub Actions workflows. Migrations stay manual through crowd-kube. No staging-on-merge, prod approvals, GitOps/ArgoCD or migration Jobs. That is a separate, later project.
- **Per-service versioning.**
- **Image signing, SBOM and SLSA provenance.** These are listed as a later hardening phase only.

## Anti-goals (failure even if it "works")

- Merging or deploying needs a new manual step or takes longer.
- Developers have to do anything per PR beyond writing a Conventional Commits title, which they already do.
- Bots merge dependency changes that skipped required checks or code-owner review.
- The changelog fills with chore and dependency noise.
- A pile of automated issues nobody reads.

## Constraints

- Both repos squash-merge and lint PR titles as Conventional Commits. So one merge is one commit with a typed title, and that is the only input versioning relies on.
- Commits must be DCO-signed. The team's commits are GPG-signed. Bot-created tags and releases come from GitHub Actions, not people.
- Work is tracked as issues in `linuxfoundation/lfx-cdp-insights-ops` (project 30).
- insights includes crowd.dev as a submodule (`submodules/crowd.dev`), so some insights Dependabot alerts belong to crowd.dev.
- Both repos are public, so reusable workflows can be called across them.

## Approaches considered

**Release unit**

- **Chosen: one repo-wide semver tag per merge.** Shared code in `services/libs/*` touches many of the 38 builder images at once. lfx-changelog links products to repos, not services. And the services ship together from one branch.
- **Rejected: per-service tags** (`<service>@vX.Y.Z`). A shared-lib change would create about 20 near-identical tags. It doesn't help the changelog either.
- **Rejected: scheduled snapshot releases** (weekly). The team already ships on every merge, and a weekly release would lump unrelated changes together.

**Cutting the release**

- **Chosen: a small workflow on push to `main`.** It reads the merged PR's type and bumps from the latest tag.
- **Rejected: release-drafter.** It is label-driven and built for batched releases. The existing config is superseded.
- **Rejected: semantic-release** (the `automatic-releases` proof of concept). It commits a CHANGELOG back to `main`, which fights branch protection and adds noise at our merge rate.
- **Rejected: porting `lfx-release-tools`.** It is an interactive CLI tied to the LFX v2 / ArgoCD flow.

**When a release is published**

- **Chosen: at production deploy.** The release is created as a draft at merge and published by the prod deploy workflow, so the changelog never announces something that isn't live.
- **Rejected: publishing at merge.** In crowd.dev, prod deploys are separate from merges.

**Dependency updates**

- **Chosen: native `dependabot.yml`** with grouping and a cooldown, plus a workflow that approves and merges a narrow low-risk class once CI is green.
- **Deferred: `lfreleng-actions/dependamerge`** to a later phase. It's useful for merging the same bump in both repos at once. Its default `--force=code-owners` bypasses code-owner review, so it would run with `--force=none` behind a protected environment.

**Security**

- **Chosen:** a PR gate (`dependency-review-action`), a weekly digest (`lfreleng-actions/github-security-report-action`), a one-time burn-down, and one SLA covering both Dependabot and Claude Security.
- **Deferred:** the full lfreleng hardening lane (harden-runner egress blocking, zizmor, pinned SHAs, Grype, SBOM, cosign), to a later phase.

## Design

### 1. Versioning and tags (both repos)

A new workflow `release-tag.yml` runs on `push` to `main`, under `concurrency: release-tag`, without cancelling runs in progress.

1. Find the PR for the pushed commit (`GET /repos/{repo}/commits/{sha}/pulls`). Take the Conventional Commits type and the breaking marker from its title.
2. Work out the next version from the latest `v*` tag:

   | PR type                                                                       | Bump                                          |
   | ----------------------------------------------------------------------------- | --------------------------------------------- |
   | `feat`                                                                        | minor                                         |
   | `fix`, `perf`                                                                 | patch                                         |
   | `chore`, `ci`, `docs`, `refactor`, `build`, `test`, `style`, `revert`, `deps` | patch                                         |
   | any type with `!`                                                             | major. While we're on `0.x`, treated as minor |

3. Create the annotated tag `vX.Y.Z` on the commit.
4. If the type is `feat`, `fix` or `perf`, also create a **draft** GitHub Release for that tag. Its body:
   ```
   feat(sched): import social identities (#5344) — @author
   Services: members-enrichment-worker, script-executor
   ```
   - The `Services:` line lists what the commit touches, computed from changed paths. insights already has change detection to reuse. crowd.dev needs a mapping from paths to builder services. A machine-readable copy of the list is kept in the body (for example an HTML comment) so the publish step can read it.
   - Other types get a tag only, no release.
5. Starting versions:
   - crowd.dev continues at `v0.50.0`. The stale drafts `v0.50.0` and `v0.1.0` are deleted first.
   - insights starts at `v0.1.0`.
6. Delete `.github/release-drafter.yml` in crowd.dev.

Tags must increase in commit order. The concurrency group plus "bump from the latest tag reachable from this commit" guarantees that.

### 2. Publishing releases on production deploy

After a successful deploy, the production deploy workflows run a new final step (ideally a reusable workflow or composite action, called once per repo):

- crowd.dev: `lf-oracle-production-deploy.yaml`
- insights: `public-api-deploy.yaml` and `production-deploy.yaml`

The step:

1. Records a GitHub Deployment per deployed service: environment `production`, ref `github.sha`, payload `{service, version}`, status `success`. This happens for branch deploys too (about 1 in 5 crowd.dev prod deploys run from a branch). The version is then the `git describe` form, for example `v0.51.0-2-gabc123`.
   - insights already records Deployments in per-service environments that its change detection reads. Reuse those instead of adding one `production` environment.
2. Stops here without publishing if `github.sha` is not on `main`.
3. For each draft release whose tag commit is an ancestor of `github.sha`, oldest first, publish it if:
   - its service list overlaps the deployed services, or
   - the list is empty or unknown, or
   - the draft is older than 3 days. This catches code that went live from a branch and whose service wasn't redeployed from `main`.

   The newest published release is marked `latest`.

The staging deploy workflows record `staging` Deployments in the same way, but publish nothing.

In insights, the tag workflow and the auto-deploy start from the same push. The publish step must wait for the tag workflow (or look the draft up again) so a deploy never runs before its draft exists.

This is the only change to the deploy workflows. They aren't restructured, and nothing about migrations changes.

### 3. Version visibility

- Builds set the version to `git describe --tags --match 'v*'` for the commit, for example `v0.51.0` or `v0.51.0-2-gabc123` when the tag isn't there yet.
- They pass it as the OCI label `org.opencontainers.image.version` and as the env var `APP_VERSION`.
- Services that already have a health or info endpoint return `APP_VERSION`.
- The frontends show it in a footer or about area. That one is optional.
- Together with GitHub Deployments, this answers "what version is service X on in prod?"

### 4. Changelog

- Link `crowd.dev` and `insights` to their products (CDP and Insights) in lfx-changelog. Published GitHub Releases then become draft entries there.
- The existing `changelog-automatic-publishing` cron publishes those drafts, and lfx-changelog posts to Slack on publish.
- **To verify:** lfx-changelog ingests published releases (`release.published`), not drafts, and doesn't make one entry per raw merged PR in a way that duplicates the release entries.

### 5. Dependabot (both repos)

Add a `.github/dependabot.yml`:

- **Ecosystems:**
  - `npm` at `/` (pnpm workspace)
  - `github-actions` at `/`
  - `docker` for the Dockerfile directories
  - crowd.dev also gets `pip`/`uv` for `services/apps/git_integration` and `gomod` for its vendored Go modules
- **Schedule:** weekly, Monday.
- **Cooldown:** default 7 days, major 30, minor 7, patch 3. Each value must be at least pnpm's `minimum-release-age` for that repo. Otherwise Dependabot opens PRs that fail at `pnpm install` with `ERR_PNPM_MINIMUM_RELEASE_AGE_VIOLATION`, which already happens on crowd.dev today.
- **Groups per ecosystem:** production minor+patch in one group, development minor+patch in one group, majors as individual PRs.
- **Security updates:** grouped separately (`applies-to: security-updates`) and not held by the cooldown.
- **Commit message prefix:** `chore(deps)` / `chore(deps-dev)`, so PR titles pass the existing title lint.
- **`open-pull-requests-limit`:** 10.

Add `dependabot-automerge.yml`, using `dependabot/fetch-metadata`:

- **Scope:** only `semver-patch` updates to development dependencies, and GitHub Actions patch updates. Everything else waits for a person. The `review-dependabot-pr` skill gives a per-PR verdict.
- **Waits for CI itself.** The workflow waits until every check on the PR has finished. It continues only if all of them passed or were skipped by path filters. Any failure, and it stops and leaves the PR for a person.
- **Then approves and merges.** It approves the PR with `GITHUB_TOKEN` (both repos allow Actions to approve PRs), which meets the ruleset's 1-approval rule, then squash-merges it.
- **Frontend PRs.** A PR that touches `/frontend/` in crowd.dev still needs a frontend code owner, because the bot isn't one. The workflow approves it and leaves the merge to GitHub once a frontend owner approves.

Rulesets don't change. `DCO` stays the only required check. Because the workflow does the CI wait itself, there's no new required check for everyone.

dependamerge (`lfreleng-actions/dependamerge`) isn't used in this phase. Later, it can run as a CLI with `--force=none` to merge the same bump in both repos at once.

### 6. Security handling (both repos)

1. **PR gate.** Every PR already runs an enterprise-level "License compliance check" (a dependency-review policy).
   - First check whether that policy already fails on vulnerability severity. If it does, don't add a second gate.
   - Otherwise add `actions/dependency-review-action` on `pull_request`, with `fail-on-severity: high` and a PR comment summary on failure. It runs report-only (not a required check) for two weeks, then becomes required.
2. **Weekly digest.** `lfreleng-actions/github-security-report-action` runs Monday on a schedule, scoped to the two repos. It covers Dependabot, code scanning and secret scanning, and posts a Slack digest to the team channel.
   - Org mode needs a fine-grained PAT with read-only security scopes. Repo mode needs only `GITHUB_TOKEN`, but its output is a job summary, so Slack is posted separately.
3. **SLA**, the same for every source (Dependabot, CodeQL, Claude Security):

   | Severity     | Fix within                    |
   | ------------ | ----------------------------- |
   | Critical     | 7 days                        |
   | High         | 30 days                       |
   | Medium / low | best effort, reviewed monthly |

   The weekly digest reports what is overdue.

   Security updates skip Dependabot's cooldown but still wait out pnpm's minimum release age, which fits inside the high SLA. A critical fix that can't wait gets a one-package exemption from the age check, added by hand.

4. **Where work is tracked.**
   - Dependabot and CodeQL alerts stay in the GitHub security tab, which is the source of truth. We don't create one issue per alert.
   - Burn-down and fix work is tracked as ops issues labelled `security`, grouped by package or root cause.
   - Claude Security findings keep arriving as ops `security` issues, as insights does today. Claude Security stays insights-only in this project; adding crowd.dev is out of scope for now.
5. **One-time burn-down.** Close the 1 open crowd.dev secret-scanning alert immediately. Then work critical → high across Dependabot and CodeQL. In insights, alerts that come from the crowd.dev submodule are fixed in crowd.dev, not duplicated.

### 7. Repo housekeeping

- insights gets a PR template matching crowd.dev's. CODEOWNERS stays as it is; this project adds no new code-owner paths.
- Delete the unused crowd.dev composite actions: `.github/actions/{build-docker-image,deploy-service,slack-notify}`.

## Phases

- **Track A** (sections 5, 6, 7) and **Track B** (sections 1–4) are independent and can run in parallel.
- **Phase 3 (later): hardening.**
  - SHA-pin all actions, checked by `lfreleng-actions/pinned-versions-action`
  - `zizmor-scan-action`
  - harden-runner, in audit mode first
  - SBOM and cosign on images, via `lfreleng-actions/docker-workflows`
  - dependamerge across both repos
- **Separate project (later):** deploy and migration redesign. That covers staging on merge, prod promotion with approvals, migrations as part of the deploy, and possibly GitOps.

Each phase becomes ops issues under one epic in `lfx-cdp-insights-ops`.

## Prerequisites and permissions

- **Workflow token:** `contents: write` (tags, releases) and `deployments: write`. Check that the org or repo default workflow permissions and any tag rulesets allow this.
- **lfx-changelog:** admin access to link both repos to products.
- **Slack:** a bot token and channel ID for the security digest.
- **Security report:** a fine-grained PAT, only if org mode is used. It needs org-admin approval.
- **Repo settings:** none beyond what's already on. Actions are already allowed to approve PRs, and rulesets stay unchanged.

## Decided by Joana

- Releases are per merge. There is one repo-wide version, not per service.
- A release is published when its code reaches production.
- Deploys and migrations stay as they are. Their redesign is a later project.
- Same approach for both repos.
- Self-merging Dependabot PRs are approved by a bot after CI is green, scoped to dev-dependency patches and Actions patches. Rulesets stay unchanged.
- CODEOWNERS stays as is. No new code-owner paths.
- Security fixes wait out pnpm's minimum release age. Critical ones get a manual exemption.
- Claude Security stays insights-only for now.

## My calls (please check)

- semver, not CalVer. `!` is held to minor while on `0.x`. crowd.dev starts at `v0.50.0`, insights at `v0.1.0`.
- Releases only for `feat`/`fix`/`perf`; tags for everything.
- Publish when a deployed service overlaps the release's services. If the service list is unknown, publish on any prod deploy. Drafts older than 3 days publish on the next `main` deploy.
- Branch deploys record a Deployment but never publish.
- The cooldown values and the grouping.
- dependency-review is report-only for two weeks, then required.
- The SLA values (critical 7d, high 30d).
- No issue per alert. The security tab plus grouped ops issues.

## Left to the builder

- Whether the shared logic lives as reusable workflows in crowd.dev called by insights (pinned by SHA), or as small copies in each repo.
- How crowd.dev maps paths to builder services (derived from `scripts/builders/*.env`, or an explicit map).
- Which existing services expose `APP_VERSION`, and where.
- Exact Docker directories and Go module paths for Dependabot.
