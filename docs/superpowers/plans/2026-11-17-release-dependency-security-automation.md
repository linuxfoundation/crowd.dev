# Release, Dependency and Security Automation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every merge to `main` in crowd.dev and insights gets a semver tag. User-facing merges get a GitHub Release that is published when the code reaches production. Dependabot updates arrive grouped and low-risk ones merge themselves. Security findings get a weekly digest with SLA tracking.

**Architecture:**

- **One shared composite action.** `crowd.dev/.github/actions/repo-automation` holds all the logic, as dependency-free TypeScript run by Node 24 with native type stripping. Each command is a pure core with tests plus a thin `git`/`gh` IO layer.
- **Workflows in both repos call it.** crowd.dev uses `./.github/actions/repo-automation`. insights uses `linuxfoundation/crowd.dev/.github/actions/repo-automation@<sha>`.
- **Dependabot is config only**, plus one gate workflow that calls the same action.

**Tech Stack:** GitHub Actions, Node 24 (`node --test`, type stripping), `gh` CLI, git, Dependabot, `dependabot/fetch-metadata`, `lfreleng-actions/github-security-report-action`, `actions/dependency-review-action`.

**Spec:** `docs/design/2026-11-17-release-automation-design.md`. Executors read both.

## Global Constraints

**Tags and releases**

- Tags are `vX.Y.Z`. crowd.dev starts at `v0.50.0` and insights at `v0.1.0`.
- Bumps:
  - `feat` → minor
  - `fix` and `perf` → patch
  - every other type, or a title that isn't Conventional Commits → patch
  - `!` → major, held to minor while major is `0`
- Only `feat`, `fix` and `perf` merges get a GitHub Release, created as a draft. Every merge gets a tag.
- A draft is published by a prod deploy from `main` when one of these holds:
  - its services overlap the deployed services, or
  - its service list is unknown (`*`), or
  - it is more than 3 days old.
- Branch deploys never publish.

**Out of scope**

- No changes to rulesets, CODEOWNERS, deploy structure or migrations.
- Claude Security stays insights-only.

**Dependabot and security**

- Dependabot runs weekly on Monday.
- Cooldown: default 7 days, semver-major 30, semver-minor 7, semver-patch 3.
- `open-pull-requests-limit: 10`.
- Commit prefixes: `chore(deps)` and `chore(deps-dev)`.
- Auto-merge covers only `semver-patch` updates to `direct:development` dependencies and `github_actions` `semver-patch` updates. It runs only after every other check passed or was skipped.
- SLA: critical 7 days, high 30 days.

**Code**

- The action has no npm dependencies, only Node built-ins plus the `git` and `gh` binaries.
- Relative imports use explicit `.ts` extensions, and type-only imports use `import type`.
- No `enum` and no constructor parameter properties, because type stripping rejects them.
- Third-party actions are pinned to a commit SHA with a `# vX.Y.Z` comment.

**Commits and issues**

- Every commit uses `git commit -s -S` with a Conventional Commits subject. There are no ops-issue refs: Joana asked to keep this work local, with no ops tracking issues. Ignore any `Refs …#<…>` in the commit commands below.
- Work stays local: no `git push`, no PRs.
- crowd.dev work happens in the worktree `crowd.dev.ci-release-dependency-automation` (branch `ci/release-dependency-automation`). insights work happens in its own `wt` worktree.
- Task 0 is skipped.

## Review Focus

1. **Merge bursts.** GitHub concurrency keeps only one pending run, so queued `release-tag` runs get cancelled. Every untagged first-parent commit since the last tag must still get its own tag. Owned by Task 3, test `tags every untagged commit since last tag`.
2. **Re-runs.** Re-running the tag workflow, or a commit that is already tagged, must not create duplicate tags or releases. Owned by Task 3, test `skips commits that already have a tag`.
3. **Odd titles.** A title that isn't Conventional Commits (`Revert "…"`, a merge commit with no PR) gets a patch tag, no release, and no crash. Owned by Task 2, test `non-conventional title is patch without release`.
4. **Deploys from a branch.** A sha that isn't on `main` records Deployments but publishes nothing. Owned by Task 4, test `publishes nothing when sha is not on main`.
5. **Deploy before its tag exists.** In insights, the deploy can start before `release-tag` has finished. After waiting it out, older eligible drafts still publish, and the run never fails because of the missing tag. Owned by Task 4, test `own draft missing still publishes older eligible drafts`.

---

## Task 0: Tracking issues (no code)

- [ ] **Step 1:** Run `/new-issue` to create an epic "Release, dependency and security automation" in `linuxfoundation/lfx-cdp-insights-ops`. Then create child issues:
  - (A) Dependabot and security, crowd.dev
  - (B) Dependabot and security, insights
  - (C) Release automation, crowd.dev
  - (D) Release automation, insights
  - (E) Version visibility, both repos
- [ ] **Step 2:** Record the issue numbers. Each task below says which issue it refs and lands in.

**PR boundaries:**

- crowd.dev: PR-A = Tasks 1, 6, 7, 8, 9 (crowd.dev parts). PR-C = Tasks 2, 3, 4, 5. PR-E = Task 10.
- insights: PR-B = the insights parts of 7, 8, 9. PR-D = the insights part of 5. Both come after PR-C merges, since insights pins the action's SHA from it.

---

## Task 1: `repo-automation` action skeleton + CI (issue C)

**Files:**

- Create: `.github/actions/repo-automation/action.yml`
- Create: `.github/actions/repo-automation/package.json`
- Create: `.github/actions/repo-automation/src/cli.ts`
- Create: `.github/actions/repo-automation/src/gh.ts`
- Create: `.github/actions/repo-automation/src/cli.test.ts`
- Create: `.github/workflows/repo-automation-tests.yml`

**Interfaces:**

- **Produces:** action inputs
  - `command` (required: `tag` | `deployed` | `dependabot-gate` | `security-sla`)
  - `token` (default `${{ github.token }}`)
  - `config` (path, default `.github/release-services.json`)
  - `initial-version`
  - `services` (space separated)
  - `environment`
  - `publish` (`'true'`/`'false'`)
  - `deployment-environment` (empty means don't record Deployments)
  - `tag-wait-seconds` (default `300`)
  - `pr-number`
  - `self-check` (the gate's own check name)
- **Produces:** action output `slack_text`, mapped from the CLI step's `GITHUB_OUTPUT`.
- **Produces:** `src/gh.ts` exports
  - `run(cmd: string, args: string[]): string`, which trims stdout and throws on a non-zero exit
  - `ghJson<T>(args: string[]): T`, which runs `gh` with `GH_TOKEN` from the env
- **Produces:** `src/cli.ts` exports `main(argv: string[], env: NodeJS.ProcessEnv): Promise<number>`. It dispatches on `argv[0]` to the command modules from later tasks, and returns exit code `2` with the message `unknown command: <x>` for anything else.

- [ ] **Step 1: Write the failing test** in `src/cli.test.ts`

```ts
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { main } from './cli.ts'

test('unknown command exits 2', async () => {
  assert.equal(await main(['nope'], {}), 2)
})
```

- [ ] **Step 2: Run it and check it fails.** Run `node --test .github/actions/repo-automation/src/`. Expected: FAIL, cannot find `./cli.ts`.

- [ ] **Step 3: Implement** `cli.ts` (`main` plus a bottom-of-file `if (import.meta.main)` runner that calls `process.exit(await main(process.argv.slice(2), process.env))`) and `gh.ts` (wrapping `execFileSync`).

  `action.yml` is `runs: using: composite` with two steps:
  1. `actions/setup-node` pinned SHA `# v4`, `node-version: 24`
  2. `node "$GITHUB_ACTION_PATH/src/cli.ts" "${{ inputs.command }}"`, with every input exported as an `INPUT_*` env var and `GH_TOKEN: ${{ inputs.token }}`

  `package.json` is `{ "private": true, "type": "module", "scripts": { "test": "node --test src/" } }`. It has no dependencies and is not added to `pnpm-workspace.yaml`.

- [ ] **Step 4: Add the workflow** `repo-automation-tests.yml`:
  - Trigger: `pull_request` on paths `.github/actions/repo-automation/**`, `.github/release-services.json`, `scripts/builders/**`
  - One job, `test`: checkout, setup-node 24, `node --test .github/actions/repo-automation/src/`

- [ ] **Step 5: Run the tests.** Same command as Step 2. Expected: PASS, 1 test.

- [ ] **Step 6: Commit**

```bash
git add .github/actions/repo-automation .github/workflows/repo-automation-tests.yml
git commit -s -S -m "ci: add repo-automation action skeleton" -m "Refs linuxfoundation/lfx-cdp-insights-ops#<C>"
```

---

## Task 2: Version, title and service mapping core (issue C)

**Files:**

- Create: `.github/actions/repo-automation/src/version.ts`
- Create: `.github/actions/repo-automation/src/services.ts`
- Create: `.github/actions/repo-automation/src/version.test.ts`
- Create: `.github/actions/repo-automation/src/services.test.ts`
- Create: `.github/release-services.json`
- Create: `.github/actions/repo-automation/src/release-services.config.test.ts`

**Interfaces:**

- **Produces** (`version.ts`):
  - `type Bump = 'major' | 'minor' | 'patch'`
  - `parseTitle(title: string): { type: string; breaking: boolean; description: string } | null`. Matches `^(\w+)(\([^)]*\))?(!)?: (.+)$`.
  - `bumpFor(title: string): Bump`
  - `isReleaseType(title: string): boolean`. True only for `feat`, `fix` and `perf`.
  - `nextVersion(latest: string | null, bump: Bump, initial: string): string`. Returns `initial` when `latest` is null. Keeps the `v` prefix.
- **Produces** (`services.ts`):
  - `type Rule = { prefix: string; services: string[] | '*' }`
  - `loadRules(path: string): Rule[]`
  - `servicesForPaths(paths: string[], rules: Rule[]): string[] | null`
  - Rules:
    - For each path, the first rule whose `prefix` it starts with applies.
    - If any matched rule is `'*'`, the result is `null` (unknown).
    - Otherwise the result is the sorted union of the matched services.
    - Paths that match no rule are ignored.
    - If nothing matches at all, the result is `[]`.

- [ ] **Step 1: Write the failing tests**

```ts
// version.test.ts
test('feat is minor', () =>
  assert.equal(nextVersion('v0.50.0', bumpFor('feat: x'), 'v0.1.0'), 'v0.51.0'))
test('fix is patch', () =>
  assert.equal(nextVersion('v0.50.3', bumpFor('fix(api): x'), 'v0.1.0'), 'v0.50.4'))
test('breaking on 0.x is minor', () =>
  assert.equal(nextVersion('v0.50.3', bumpFor('feat!: x'), 'v0.1.0'), 'v0.51.0'))
test('breaking on 1.x is major', () =>
  assert.equal(nextVersion('v1.2.3', bumpFor('refactor!: x'), 'v0.1.0'), 'v2.0.0'))
test('no previous tag uses initial', () =>
  assert.equal(nextVersion(null, 'minor', 'v0.50.0'), 'v0.50.0'))
test('non-conventional title is patch without release', () => {
  assert.equal(bumpFor('Revert "feat: x"'), 'patch')
  assert.equal(isReleaseType('Revert "feat: x"'), false)
})
test('chore is not a release type', () => assert.equal(isReleaseType('chore(deps): bump x'), false))

// services.test.ts
const rules = [
  { prefix: 'services/apps/data_sink_worker/', services: ['data-sink-worker'] },
  { prefix: 'services/libs/', services: '*' },
  { prefix: 'backend/', services: ['backend'] },
]
test('maps app dir to its service', () =>
  assert.deepEqual(servicesForPaths(['services/apps/data_sink_worker/src/a.ts'], rules), [
    'data-sink-worker',
  ]))
test('shared lib makes it unknown', () =>
  assert.equal(servicesForPaths(['backend/x.ts', 'services/libs/common/y.ts'], rules), null))
test('docs only maps to nothing', () =>
  assert.deepEqual(servicesForPaths(['docs/a.md'], rules), []))
```

- [ ] **Step 2: Run the tests and check they fail.** Run `node --test .github/actions/repo-automation/src/`. Expected: FAIL, missing modules.

- [ ] **Step 3: Implement** `version.ts` and `services.ts` to the signatures above.

- [ ] **Step 4: Generate `.github/release-services.json` for crowd.dev.**
  - For each `scripts/builders/<name>.env`:
    - read `DOCKERFILE`
    - if it is `./services/docker/Dockerfile.<snake>` and `services/apps/<snake>/` exists, emit `{ "prefix": "services/apps/<snake>/", "services": ["<name>"] }`
    - a builder whose Dockerfile builds `backend/` (`backend.env`) gets the prefix `backend/`
    - the frontend builder gets `frontend/`
  - Then add catch-all `'*'` rules for `services/libs/`, `services/archetypes/`, `package.json`, `pnpm-lock.yaml` and `pnpm-workspace.yaml`.
  - Builders with no source directory (infra images such as db, redis, nginx) get no rule. Leave them out.

- [ ] **Step 5: Write the config consistency test** `release-services.config.test.ts`. It runs only when `scripts/builders` exists, otherwise it skips. For every `scripts/builders/*.env` whose `DOCKERFILE` matches `Dockerfile.<snake>` with `services/apps/<snake>/` present, assert that some rule lists that builder name. This catches new services that aren't mapped.

- [ ] **Step 6: Run all tests.** Expected: PASS.

- [ ] **Step 7: Commit**: `feat: add version and service mapping for release automation`.

---

## Task 3: `tag` command (issue C)

**Files:**

- Create: `.github/actions/repo-automation/src/tag.ts`
- Create: `.github/actions/repo-automation/src/release-body.ts`
- Create: `.github/actions/repo-automation/src/tag.test.ts`
- Create: `.github/actions/repo-automation/src/release-body.test.ts`
- Modify: `.github/actions/repo-automation/src/cli.ts` (dispatch `tag`)

**Interfaces:**

- **Consumes:** `bumpFor`, `isReleaseType`, `nextVersion`, `servicesForPaths`, `loadRules` (Task 2), and `run`, `ghJson` (Task 1).
- **Produces** (`release-body.ts`):
  - `renderReleaseBody(i: { title: string; number: number | null; author: string | null; services: string[] | null }): string`. Line 1 is `<title> (#<number>) — @<author>`, leaving out the parts that are null. Line 2 is `Services: <comma list>`, or `Services: all (shared code)` when `services` is null, and is omitted when `services` is `[]`. The last line is `<!-- release-services: <JSON array | "*"> -->`.
  - `parseReleaseServices(body: string): string[] | null`. Returns null when the marker is `*` or missing.
- **Produces** (`tag.ts`):
  - `type CommitInfo = { sha: string; title: string; number: number | null; author: string | null; paths: string[]; existingTag: string | null }`
  - `type PlannedTag = { sha: string; version: string; release: boolean; title: string; body: string }`
  - `planTags(commits: CommitInfo[], latestTag: string | null, initial: string, rules: Rule[]): PlannedTag[]`. `commits` are oldest first. It skips any commit with an `existingTag` (and continues the sequence from that tag), and chains versions across the commits it plans.
  - `runTag(env): Promise<number>`, the IO layer:
    1. `git fetch --tags`.
    2. `latest = git describe --tags --abbrev=0 --match 'v[0-9]*.[0-9]*.[0-9]*' $GITHUB_SHA`. If that fails, `latest = null`.
    3. List the commits: `git rev-list --first-parent --reverse <latest>..$GITHUB_SHA`, or just `[$GITHUB_SHA]` when `latest` is null.
    4. For each commit:
       - get its PR from `gh api repos/$GITHUB_REPOSITORY/commits/<sha>/pulls` (the first one with `merged_at` set)
       - title is the PR title, falling back to the commit subject
       - paths come from `git diff-tree --no-commit-id --name-only -r <sha>`
       - `existingTag` comes from `git tag --points-at <sha> --list 'v*'`
    5. Run `planTags`.
    6. For each planned tag, `git tag -a <v> -m "<title>" <sha>`.
    7. One `git push origin <tags…>`.
    8. Then, for each planned tag with `release: true`, run `gh release create <v> --draft --title "<v> — <title>" --notes <body> --verify-tag`.

- [ ] **Step 1: Write the failing tests**

```ts
// tag.test.ts
const c = (sha: string, title: string, extra = {}) => ({
  sha,
  title,
  number: 1,
  author: 'a',
  paths: ['docs/x.md'],
  existingTag: null,
  ...extra,
})
test('tags every untagged commit since last tag', () => {
  const p = planTags(
    [c('a', 'fix: x'), c('b', 'feat: y'), c('c', 'chore: z')],
    'v0.50.0',
    'v0.50.0',
    [],
  )
  assert.deepEqual(
    p.map((t) => t.version),
    ['v0.50.1', 'v0.51.0', 'v0.51.1'],
  )
  assert.deepEqual(
    p.map((t) => t.release),
    [true, true, false],
  )
})
test('skips commits that already have a tag', () => {
  const p = planTags(
    [c('a', 'fix: x', { existingTag: 'v0.50.1' }), c('b', 'fix: y')],
    'v0.50.0',
    'v0.50.0',
    [],
  )
  assert.deepEqual(
    p.map((t) => [t.sha, t.version]),
    [['b', 'v0.50.2']],
  )
})
test('first ever run uses initial version', () =>
  assert.equal(planTags([c('a', 'feat: x')], null, 'v0.50.0', [])[0].version, 'v0.50.0'))

// release-body.test.ts
test('round-trips services', () =>
  assert.deepEqual(
    parseReleaseServices(
      renderReleaseBody({ title: 'feat: x', number: 5, author: 'a', services: ['api'] }),
    ),
    ['api'],
  ))
test('unknown services render as *', () =>
  assert.equal(
    parseReleaseServices(
      renderReleaseBody({ title: 'feat: x', number: null, author: null, services: null }),
    ),
    null,
  ))
```

- [ ] **Step 2: Run the tests.** Expected: FAIL, missing modules.
- [ ] **Step 3: Implement** `release-body.ts`, `planTags` and `runTag`, and wire `tag` into `cli.ts`.
- [ ] **Step 4: Run the tests.** Expected: PASS.
- [ ] **Step 5: Commit**: `feat: add tag command to repo-automation`.

---

## Task 4: `deployed` command (issue C)

**Files:**

- Create: `.github/actions/repo-automation/src/deployed.ts`
- Create: `.github/actions/repo-automation/src/deployed.test.ts`
- Modify: `.github/actions/repo-automation/src/cli.ts` (dispatch `deployed`)

**Interfaces:**

- **Consumes:** `parseReleaseServices` (Task 3), and `run`, `ghJson` (Task 1).
- **Produces:**
  - `type Draft = { tag: string; sha: string; createdAt: string; services: string[] | null }`
  - `selectDraftsToPublish(i: { drafts: Draft[]; deployed: string[]; onMain: boolean; isAncestor: (sha: string) => boolean; now: Date; maxAgeDays: number }): Draft[]`
    - Returns oldest first.
    - Returns `[]` when `!onMain`.
    - A draft is eligible when `isAncestor(sha)` holds and one of these is true: `services === null`, its services intersect `deployed`, or `now - createdAt > maxAgeDays`.
  - `runDeployed(env): Promise<number>`, the IO layer:
    1. Parse `INPUT_SERVICES` (split on whitespace).
    2. If `INPUT_DEPLOYMENT_ENVIRONMENT` is non-empty, create one Deployment per service:
       - `POST /repos/{repo}/deployments` with `ref=$GITHUB_SHA`, `environment=<env>`, `task=deploy:<service>`, `auto_merge=false`, `required_contexts=[]`, `payload={service, version}`. The version is `git describe --tags --match 'v[0-9]*' --always`.
       - Then post a `success` status.
    3. If `INPUT_PUBLISH !== 'true'`, return 0.
    4. `onMain` comes from `git merge-base --is-ancestor $GITHUB_SHA origin/main` (after `git fetch origin main --tags`).
    5. If `onMain`, poll every 15 s until `git tag --points-at $GITHUB_SHA` is non-empty, up to `INPUT_TAG_WAIT_SECONDS`. On timeout, log a warning and continue.
    6. List drafts with `gh api repos/{repo}/releases --paginate`, filtered to `draft: true`. The sha comes from `git rev-list -n1 <tag>` and services from `parseReleaseServices(body)`.
    7. Publish each selected draft with `gh release edit <tag> --draft=false --latest=false`. The newest one gets `--latest`.
    8. Errors while publishing are logged and the step still exits 0. A failed publish must never fail a deploy.

- [ ] **Step 1: Write the failing tests**

```ts
const now = new Date('2026-11-20T00:00:00Z')
const d = (tag: string, services: string[] | null, createdAt = '2026-11-19T00:00:00Z') => ({
  tag,
  sha: tag,
  createdAt,
  services,
})
const base = { deployed: ['api'], onMain: true, isAncestor: () => true, now, maxAgeDays: 3 }
test('publishes overlapping and unknown, skips others', () =>
  assert.deepEqual(
    selectDraftsToPublish({
      ...base,
      drafts: [d('v1', ['api']), d('v2', ['worker']), d('v3', null)],
    }).map((x) => x.tag),
    ['v1', 'v3'],
  ))
test('publishes drafts older than 3 days regardless of service', () =>
  assert.deepEqual(
    selectDraftsToPublish({ ...base, drafts: [d('v1', ['worker'], '2026-11-16T00:00:00Z')] }).map(
      (x) => x.tag,
    ),
    ['v1'],
  ))
test('publishes nothing when sha is not on main', () =>
  assert.deepEqual(
    selectDraftsToPublish({ ...base, onMain: false, drafts: [d('v1', ['api'])] }),
    [],
  ))
test('ignores drafts not contained in the deployed sha', () =>
  assert.deepEqual(
    selectDraftsToPublish({
      ...base,
      isAncestor: (s) => s !== 'v2',
      drafts: [d('v1', ['api']), d('v2', ['api'])],
    }).map((x) => x.tag),
    ['v1'],
  ))
test('own draft missing still publishes older eligible drafts', () =>
  assert.deepEqual(
    selectDraftsToPublish({ ...base, drafts: [d('v1', ['api'])] }).map((x) => x.tag),
    ['v1'],
  ))
```

- [ ] **Step 2: Run the tests.** Expected: FAIL.
- [ ] **Step 3: Implement** `selectDraftsToPublish` and `runDeployed`, and wire them into `cli.ts`.
- [ ] **Step 4: Run the tests.** Expected: PASS.
- [ ] **Step 5: Commit**: `feat: add deployed command to publish releases on prod deploy`.

---

## Task 5: Wire release workflows (crowd.dev = issue C, insights = issue D)

**Files (crowd.dev):**

- Create: `.github/workflows/release-tag.yml`
- Modify: `.github/workflows/lf-oracle-production-deploy.yaml` (after the builder step)
- Modify: `.github/workflows/lf-oracle-staging-deploy.yaml` (after the builder step)
- Delete: `.github/release-drafter.yml`

**Files (insights):**

- Create: `.github/workflows/release-tag.yml`
- Create: `.github/release-services.json`
- Modify: `.github/workflows/public-api-deploy.yaml` (new final job)
- Modify: `.github/workflows/production-deploy.yaml` (final step)

**Interfaces:**

- **Consumes:** action commands `tag` and `deployed` (Tasks 3 and 4).
- `uses:` lines:
  - crowd.dev: `./.github/actions/repo-automation`
  - insights: `linuxfoundation/crowd.dev/.github/actions/repo-automation@<PR-C merge sha> # repo-automation`

- [ ] **Step 1: crowd.dev `release-tag.yml`**
  - `on: push: branches: [main]`
  - `concurrency: { group: release-tag, cancel-in-progress: false }`
  - `permissions: { contents: write }`
  - Steps: checkout with `fetch-depth: 0`, then the action with `command: tag` and `initial-version: v0.50.0`.

- [ ] **Step 2: crowd.dev deploy workflows.**
  - Add `permissions: { contents: write, deployments: write }`. Neither file has a permissions block today.
  - Change checkout to `fetch-depth: 0`.
  - After the builder step, add the action with:
    - `command: deployed`
    - `services: ${{ github.event.inputs.services }}`
    - `deployment-environment: production` (or `staging`)
    - `publish: 'true'` in production, `'false'` in staging
  - Before writing this, confirm in `.github/actions/node/src/utils.ts` that the `services` input contains builder names (the `scripts/builders/<name>.env` file names). If it holds k8s service names instead, map them to builder names through each env file's `SERVICES=` before calling the action.

- [ ] **Step 3: Delete `.github/release-drafter.yml`.** Run `actionlint .github/workflows/*.y*ml`. Expected: no errors.

- [ ] **Step 4: Commit** crowd.dev: `ci: tag every merge and publish releases on prod deploy`. Open PR-C. After it merges:
  - Watch the first `release-tag` run create `v0.50.0`.
  - Check that a later `feat`/`fix` merge creates a draft.
  - Check that the next prod deploy publishes it (`gh release list -R linuxfoundation/crowd.dev --limit 5`).

- [ ] **Step 5: insights `release-services.json`.** Hand-written:

  | prefix                                                                          | services                                                   |
  | ------------------------------------------------------------------------------- | ---------------------------------------------------------- |
  | `api/`                                                                          | `["api"]`                                                  |
  | `workers/api-gateway/`                                                          | `["api-gateway"]`                                          |
  | `frontend/`                                                                     | `["app"]`                                                  |
  | `workers/temporal/<dir>/`                                                       | `["<dir with _ → ->"]`, one rule for each of the 3 workers |
  | `libs/`, `submodules/`, `package.json`, `pnpm-lock.yaml`, `pnpm-workspace.yaml` | `'*'`                                                      |

  The worker names must match the `service` values in `public-api-deploy.yaml`.

- [ ] **Step 6: insights workflows.**
  - `release-tag.yml`: same as Step 1, with `initial-version: v0.1.0`.
  - `public-api-deploy.yaml`: add a final job `publish-releases`:
    - `needs: [changes, deploy-api, deploy-api-gateway, deploy-app, deploy-workers]`
    - `if: always() && !cancelled() && github.ref == 'refs/heads/main'`
    - `permissions: { contents: write }`
    - steps: checkout with `fetch-depth: 0`, then the action with `command: deployed`, `publish: 'true'`, an empty `deployment-environment` (the jobs already record Deployments), and `services` built from the `changes` outputs, keeping only the deploy jobs whose `result == 'success'`.
  - `production-deploy.yaml`: add a final step, `command: deployed` with `services: ${{ inputs.service }}`, mapping `frontend` → `app`.

- [ ] **Step 7: Run `actionlint` and commit** insights: `ci: tag every merge and publish releases on prod deploy`. Open PR-D.

---

## Task 6: Dependabot gate command (issue A)

**Files:**

- Create: `.github/actions/repo-automation/src/dependabot-gate.ts`
- Create: `.github/actions/repo-automation/src/dependabot-gate.test.ts`
- Modify: `.github/actions/repo-automation/src/cli.ts`

**Interfaces:**

- **Produces:**
  - `type Check = { name: string; bucket: 'pass' | 'fail' | 'pending' | 'skipping' | 'cancel' }`. This is the shape of `gh pr checks --json name,bucket`.
  - `checksVerdict(checks: Check[], selfName: string): 'pending' | 'pass' | 'fail'`. It ignores `selfName`. Any `fail` or `cancel` means `fail`, any `pending` means `pending`, otherwise `pass`. Zero checks counts as `pending`, because checks may not have registered yet.
  - `runDependabotGate(env): Promise<number>`, the IO layer:
    1. Poll `gh pr checks $INPUT_PR_NUMBER --json name,bucket` every 30 s, for up to 45 minutes.
    2. On `fail` or timeout, log it and exit 0 without approving.
    3. On `pass`: `gh pr review --approve --body "Patch dev-dependency update; all checks green."`, then `gh pr merge --squash`.
    4. If the merge fails (for example a `/frontend/` path that needs a code owner), log it and exit 0.
  - The workflow sets `selfName` through `INPUT_SELF_CHECK`.

- [ ] **Step 1: Write the failing tests**

```ts
test('ignores own check', () =>
  assert.equal(
    checksVerdict(
      [
        { name: 'gate', bucket: 'pending' },
        { name: 'test', bucket: 'pass' },
      ],
      'gate',
    ),
    'pass',
  ))
test('skipped path-filtered checks count as pass', () =>
  assert.equal(
    checksVerdict(
      [
        { name: 'build', bucket: 'skipping' },
        { name: 'DCO', bucket: 'pass' },
      ],
      'gate',
    ),
    'pass',
  ))
test('any failure fails', () =>
  assert.equal(checksVerdict([{ name: 'test', bucket: 'fail' }], 'gate'), 'fail'))
test('no checks yet is pending', () => assert.equal(checksVerdict([], 'gate'), 'pending'))
```

- [ ] **Step 2: Run the tests.** Expected: FAIL.
- [ ] **Step 3: Implement** the command and wire it into `cli.ts`.
- [ ] **Step 4: Run the tests.** Expected: PASS.
- [ ] **Step 5: Commit**: `feat: add dependabot gate command`.

---

## Task 7: Dependabot config + gate workflow (crowd.dev = issue A, insights = issue B)

**Files:**

- Create in both repos: `.github/dependabot.yml`, `.github/workflows/dependabot-automerge.yml`

**Shared values (every update block):**

- `schedule: { interval: weekly, day: monday }`
- `cooldown: { default-days: 7, semver-major-days: 30, semver-minor-days: 7, semver-patch-days: 3 }`
- `open-pull-requests-limit: 10`
- `commit-message: { prefix: chore, include: scope }`. This yields `chore(deps)` / `chore(deps-dev)`.
- Groups:
  - `prod-minor-patch` with `{ dependency-type: production, update-types: [minor, patch] }`
  - `dev-minor-patch` with `{ dependency-type: development, update-types: [minor, patch] }`
  - `security` with `{ applies-to: security-updates, patterns: ['*'] }`
  - Majors aren't grouped.

**crowd.dev update blocks:**

| Ecosystem        | Directories                                                                                                                                                                                                                                                                                     |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `npm`            | `/` and `/services/cronjobs/archived_repositories`                                                                                                                                                                                                                                              |
| `github-actions` | `/`                                                                                                                                                                                                                                                                                             |
| `docker`         | `/scripts/services/docker`, `/backend/src/database`, `/backend/src/osspckgs`, `/backend/src/product`, `/scripts/packages-db`, `/scripts/scaffold/kafka-connect`, `/scripts/scaffold/nginx`, `/scripts/scaffold/redis`, `/scripts/scaffold/temporal`, `/services/cronjobs/archived_repositories` |
| `uv`             | `/services/apps/git_integration`, `/services/apps/mailing_list_integration`                                                                                                                                                                                                                     |
| `pip`            | `/services/libs/tinybird`                                                                                                                                                                                                                                                                       |
| `gomod`          | `/services/apps/git_integration/src/crowdgit/services/software_value`, `/services/apps/git_integration/src/crowdgit/services/vulnerability_scanner`                                                                                                                                             |

**insights update blocks:**

| Ecosystem        | Directories / settings                                                                                                                                           |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `npm`            | `/`, with `exclude-paths: ['submodules/**']`                                                                                                                     |
| `github-actions` | `/`                                                                                                                                                              |
| `docker`         | `/api`, `/frontend`, `/workers/temporal/package_downloads_worker`, `/workers/temporal/search_volume_worker`, `/workers/temporal/vulnerability_enrichment_worker` |

- [ ] **Step 1: Check pnpm's minimum release age against the cooldown.** In each repo, run `pnpm config get minimum-release-age`. Expected: a value of at most 4320 minutes (3 days); that is, `undefined`, `0` or `1440`. If it's higher, raise `semver-patch-days` to match and note it in the PR.

- [ ] **Step 2: Write `dependabot.yml`** from the tables above.

- [ ] **Step 3: Validate it.** Run `uvx check-jsonschema --builtin-schema vendor.dependabot .github/dependabot.yml`. Expected: `ok -- validation done`.

- [ ] **Step 4: Write `dependabot-automerge.yml`.**
  - `on: pull_request`
  - `permissions: { contents: write, pull-requests: write, checks: read }`
  - job id `gate`, with `if: github.event.pull_request.user.login == 'dependabot[bot]'`
  - Steps:
    1. `dependabot/fetch-metadata`, pinned SHA `# v2`
    2. The action with `command: dependabot-gate`, `pr-number: ${{ github.event.pull_request.number }}` and `self-check: gate`, under this `if:` condition:

```yaml
if: >-
  steps.meta.outputs.update-type == 'version-update:semver-patch' &&
  (steps.meta.outputs.dependency-type == 'direct:development' ||
   steps.meta.outputs.package-ecosystem == 'github_actions')
```

- insights uses the cross-repo `uses:` line from Task 5.

- [ ] **Step 5: Run `actionlint` and commit** in each repo: `ci: add dependabot config and patch auto-merge gate`.

- [ ] **Step 6: Watch the first Monday run.** Expected:
  - grouped PRs titled `chore(deps): bump the …` that pass `pr-title-lint`
  - a dev-patch PR gets approved by `github-actions[bot]` and merged once it's green
  - a red PR stays open with no approval

---

## Task 8: Security gate, digest and SLA (crowd.dev = issue A, insights = issue B)

**Files:**

- Create: `.github/actions/repo-automation/src/security-sla.ts`
- Create: `.github/actions/repo-automation/src/security-sla.test.ts`
- Modify: `.github/actions/repo-automation/src/cli.ts`
- Create in both repos: `.github/workflows/security-report.yml`
- Conditional, in both repos: `.github/workflows/dependency-review.yml`

**Interfaces:**

- **Produces:**
  - `type Alert = { source: 'dependabot' | 'code-scanning'; severity: 'critical' | 'high' | 'medium' | 'low'; createdAt: string; url: string }`
  - `overdue(alerts: Alert[], now: Date, sla: { critical: number; high: number }): { critical: Alert[]; high: Alert[] }`
  - `runSecuritySla(env)`:
    1. Fetch open alerts:
       - `gh api repos/{repo}/dependabot/alerts?state=open --paginate`, using `security_advisory.severity`
       - `gh api repos/{repo}/code-scanning/alerts?state=open --paginate`, using `rule.security_severity_level`
    2. Write the counts and up to 5 overdue links per severity to `$GITHUB_STEP_SUMMARY`.
    3. Set output `slack_text`.
    4. SLA is `{ critical: 7, high: 30 }`.

- [ ] **Step 1: Write the failing tests**

```ts
const now = new Date('2026-11-30T00:00:00Z')
const a = (severity: Alert['severity'], createdAt: string) => ({
  source: 'dependabot' as const,
  severity,
  createdAt,
  url: 'u',
})
test('critical older than 7 days is overdue', () =>
  assert.equal(
    overdue([a('critical', '2026-11-20T00:00:00Z'), a('critical', '2026-11-25T00:00:00Z')], now, {
      critical: 7,
      high: 30,
    }).critical.length,
    1,
  ))
test('high within 30 days is not overdue', () =>
  assert.equal(
    overdue([a('high', '2026-11-05T00:00:00Z')], now, { critical: 7, high: 30 }).high.length,
    0,
  ))
test('medium is never overdue', () =>
  assert.deepEqual(overdue([a('medium', '2020-01-01T00:00:00Z')], now, { critical: 7, high: 30 }), {
    critical: [],
    high: [],
  }))
```

- [ ] **Step 2: Run the tests, check they fail, implement, run them again until they pass. Commit**: `feat: add security sla command`.

- [ ] **Step 3: Write `security-report.yml`** in each repo.
  - `on: schedule: cron '0 8 * * 1'` and `workflow_dispatch`
  - `permissions: { contents: read, security-events: read, actions: read, issues: read, pull-requests: read }`
  - Steps:
    1. `lfreleng-actions/github-security-report-action`, pinned to the SHA of tag `v0.1.0` (get it with `gh api repos/lfreleng-actions/github-security-report-action/git/ref/tags/v0.1.0 -q .object.sha`), with `scope: repo` and `token: ${{ secrets.SECURITY_REPORT_TOKEN || github.token }}`
    2. The action with `command: security-sla`, same token
    3. `slackapi/slack-github-action`, pinned SHA `# v2`, method `chat.postMessage`, channel `${{ vars.SECURITY_SLACK_CHANNEL_ID }}`, text from the report's Slack summary plus `slack_text`, token `${{ secrets.SLACK_BOT_TOKEN }}`

- [ ] **Step 4: Test it by hand.** Run `gh workflow run security-report.yml`, then check the summary. Expected: Dependabot counts that match the Security tab (355 for crowd.dev, 207 for insights, or whatever the current numbers are).
  - If the Dependabot section is empty or returns 403, `GITHUB_TOKEN` can't read Dependabot alerts. Ask the user to create a fine-grained PAT with Dependabot alerts read and Code scanning alerts read, stored as `SECURITY_REPORT_TOKEN`. Re-run.

- [ ] **Step 5: Decide on the dependency-review gate.**
  - Open the latest "License compliance check" run on any PR in each repo and see whether it lists or fails on vulnerabilities.
  - If it does, skip this step and note it in the PR.
  - Otherwise create `dependency-review.yml`:
    - `on: pull_request`
    - `actions/dependency-review-action`, pinned SHA `# v4`, with `fail-on-severity: high`, `comment-summary-in-pr: on-failure`, `license-check: false`
  - Making it required after two weeks needs a ruleset change by an admin. List that as a follow-up in the issue, not in this PR.

- [ ] **Step 6: Run `actionlint` and commit**: `ci: add weekly security report and dependency review`.

---

## Task 9: Housekeeping and SLA docs (crowd.dev = issue A, insights = issue B)

**Files:**

- Delete (crowd.dev): `.github/actions/build-docker-image/`, `.github/actions/deploy-service/`, `.github/actions/slack-notify/`
- Create (insights): `.github/PULL_REQUEST_TEMPLATE.md`. Copy crowd.dev's and drop the `## Frontend checklist` section.
- Modify: `SECURITY.md` in insights. Create it in crowd.dev if it's missing.

- [ ] **Step 1: Confirm the dead actions are unused.** Run `grep -rn "actions/build-docker-image\|actions/deploy-service\|actions/slack-notify" .github/`. Expected: no matches in crowd.dev. Then delete them.

- [ ] **Step 2: Add a "Vulnerability handling" section to `SECURITY.md`.** It covers:
  - SLA: critical 7 days, high 30 days, medium/low best effort and reviewed monthly
  - the Security tab is the source of truth, and there's no issue per alert
  - fix work is tracked as `security` ops issues
  - security fixes wait out pnpm's minimum release age, and critical fixes get a manual exemption
  - in insights only: Claude Security findings arrive as ops `security` issues

- [ ] **Step 3: Commit**: `docs: add vulnerability handling sla and repo housekeeping`.

---

## Task 10: Version visibility (issue E)

**Files:**

- Modify (crowd.dev): `scripts/cli:397-418` (`build_and_publish`)
- Modify (crowd.dev): the runner stage of each `scripts/services/docker/Dockerfile.<snake>`
- Modify (crowd.dev): the `/health` handlers at `backend/src/api/index.ts:188`, `services/apps/kafka_connect_monitor/src/main.ts:14`, `services/apps/nango_webhook_api/src/main.ts:24`, `services/apps/search_sync_api/src/main.ts:47`, `services/apps/webhook_api/src/main.ts:45`
- Modify (insights): `.github/actions/build-docker-image/action.yaml`, `api/Dockerfile`, the workers' and frontend Dockerfiles, `api/src/health.ts:17-19`

**Interfaces:**

- **Produces:** the env var `APP_VERSION` inside every service image, and the OCI labels `org.opencontainers.image.version` and `org.opencontainers.image.revision`.
- **Consumes:** tags from Task 5. Until they exist, `git describe --always` falls back to the short sha.

- [ ] **Step 1: crowd.dev `scripts/cli`.**
  - Compute `APP_VERSION=$(git describe --tags --match 'v[0-9]*' --always)`.
  - Add `--build-arg APP_VERSION="$APP_VERSION" --label org.opencontainers.image.version="$APP_VERSION" --label org.opencontainers.image.revision="$(git rev-parse HEAD)"` to the `docker build` at `:410`.
  - The deploy builder checks out with depth 1, so tags aren't available there. Add `fetch-depth: 0` to the checkout in both `lf-oracle-*-deploy.yaml` files. Task 5 Step 2 already does this if it has landed.

- [ ] **Step 2: crowd.dev Dockerfiles.** In each `Dockerfile.<snake>` runner stage (the last `FROM`), add `ARG APP_VERSION=unknown` and `ENV APP_VERSION=$APP_VERSION`.

- [ ] **Step 3: crowd.dev health handlers.** Add `version: process.env.APP_VERSION ?? 'unknown'` to each JSON body. Keep the status code and existing fields unchanged, because k8s probes depend on them.

- [ ] **Step 4: Verify crowd.dev.** Run `cd scripts && bash cli build data-sink-worker test`, then `docker inspect --format '{{ index .Config.Labels "org.opencontainers.image.version" }}' <image>`. Expected: `v0.5x.y` or a short sha. Then run `docker run --rm <image> printenv APP_VERSION`. Expected: the same value.

- [ ] **Step 5: insights.**
  - `build-docker-image/action.yaml`: compute `APP_VERSION` with the same `git describe` command, append `APP_VERSION=<v>` to `build-args`, and add a `labels:` input to `docker/build-push-action` with the two OCI labels.
  - Each Dockerfile gets `ARG APP_VERSION=unknown` and `ENV APP_VERSION=$APP_VERSION`.
  - `api/src/health.ts` returns `version`.
  - Every checkout that feeds this action needs `fetch-depth: 0`.

- [ ] **Step 6: Run `pnpm lint` and `pnpm tsc-check` (crowd.dev), and `pnpm lint` (insights). Commit**: `feat: expose app version in images and health endpoints`.

---

## Task 11: One-time operations (issue C/D, no code; the user runs the destructive steps)

- [ ] **Step 1: Clean up stale drafts.** List the stale crowd.dev drafts with `gh release list -R linuxfoundation/crowd.dev --exclude-pre-releases --limit 20`. Ask the user to confirm, then delete the drafts `v0.50.0` and both `v0.1.0` drafts (`gh release delete <tag> -R linuxfoundation/crowd.dev`). This must happen before PR-C merges, so `v0.50.0` is free.
- [ ] **Step 2: Link the repos in lfx-changelog.** In the lfx-changelog admin, link `linuxfoundation/crowd.dev` to the CDP product and `linuxfoundation/insights` to the Insights product. This needs changelog admin access; the user does it or asks the owner.
- [ ] **Step 3: Check what lfx-changelog ingests.** After the first published release, confirm that one draft changelog entry appears per published release, that draft GitHub Releases don't create entries, and that there are no duplicates from merged-PR ingestion. If there are duplicates, raise it with the lfx-changelog owners before more releases go out.
- [ ] **Step 4: Fix the secret alert.** Resolve the one open crowd.dev secret-scanning alert (rotate the secret, then close the alert). The user does this.
- [ ] **Step 5: Plan the burn-down.** Create the burn-down ops issues under the epic, labelled `security` and grouped by package or root cause: critical first, then high. Alerts in insights that come from `submodules/crowd.dev` go to the crowd.dev issues.
