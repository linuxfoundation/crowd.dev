---
name: tinybird
description: >
  Update, deploy, or roll out a Tinybird pipe or datasource to staging or
  production. Use for "update a pipe", "push to Tinybird", "tb push", "tb
  pull", "change a datasource", "add a field to a pipe", or anything touching
  the `lfx_insights` / `lfx_insights_stg` Tinybird workspaces.
allowed-tools: Bash, Read, Edit, Glob, Grep, AskUserQuestion
---

# Tinybird Pipe / Datasource Update

Tinybird resources (`.pipe`, `.datasource`) live at `services/libs/tinybird/` in this repo. This
skill drives the edit-and-deploy cycle; interactive or destructive steps are handed back to you.

**Out of scope** — local Docker testing, backfills, schema iteration, backup datasources. Those
stay in `services/libs/tinybird/README.md` (and its siblings `dataflow.md`,
`lambda-architecture.md`, `bucketing-architecture.md`) — read that file, don't attempt those flows
here.

## Guardrails

- Never run `tb push` against `lfx_insights` (prod) without showing the diff and the exact command
  and getting an explicit yes first.
- Never delete a datasource, and never run `tb push --populate` — those are the
  downtime-causing / backfill paths documented in the README, out of scope for this skill.
- Never print `.tinyb` wholesale or echo a token — it holds a live admin credential. Only report
  `host` / `name` / `user_email` from it, and never open it with `Read` — that puts the whole
  file, token included, into the transcript. Extract fields with `jq` instead (see step 2).

## 1. Environment

```bash
cd services/libs/tinybird
source .venv/bin/activate
tb --version   # expect 5.x — ignore the "upgrade to 6.x" nag, that's a different product line
```

If `.venv` doesn't exist yet:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Auth — hand back to the user

`tb auth` is interactive; you cannot drive it. Check whether a session already exists by
extracting only the safe fields — never open `.tinyb` with `Read` or `cat`, the file holds a
live admin token:

```bash
jq -r '"host=\(.host) workspace=\(.name) user=\(.user_email)"' .tinyb
```

If not authenticated, ask the user to run it themselves via the `!` prefix:

```
! cd services/libs/tinybird && source .venv/bin/activate && tb auth
```

## 3. Select the workspace

```bash
tb workspace ls
tb workspace use lfx_insights_stg   # staging/dev
tb workspace use lfx_insights       # production
```

`.tinyb` persists the last-selected workspace across sessions — **always echo the current
workspace back before any mutating command**, don't assume it's still pointed where you left it.

## 4. Get the change into files

Either edit the `.pipe` / `.datasource` file directly under `pipes/` or `datasources/`, or — if the
resource was authored/edited in the staging UI first — pull it while pointed at
`lfx_insights_stg`:

```bash
tb pull --force --match <resource_name>
```

## 5. Format

Must be run from `scripts/` (it uses relative `../pipes` / `../datasources`). Use `--match` so the
diff stays scoped to the resource you touched:

Run it in a subshell so the working directory stays at the Tinybird project root afterward:

```bash
(cd scripts && ./format.sh --match <resource_name>)
```

Then confirm scope: `git diff` should touch only the intended resource.

## 6. Deploy to staging

`.tinyb` may still be pointed at production from a previous session — select and verify staging
immediately before pushing:

```bash
tb workspace use lfx_insights_stg
tb workspace ls                    # confirm lfx_insights_stg is current
tb push pipes/<name>.pipe          # or datasources/<name>.datasource
# add --force if overwriting an existing resource — call this out explicitly when you do
```

## 7. PR

Branch and commit following this repo's `.claude/rules/commit-workflow.md` conventions: branch
`type/CM-<number>-short-description`, commit `type: description (CM-XXX)` with the JIRA key
in parens at the end, `--signoff -S`, and the PR title must carry the CM key (CI-enforced by
`pr-title-jira-key-lint.yml`). Note: this repo's Tinybird CI (`tinybird-ci.yml`) only checks
`tb fmt --diff` — a green CI does not mean the SQL itself is correct, say so if asked.

## 8. Deploy to production

Only after the PR from step 7 is merged — the README's own workflow gates production on
this (`services/libs/tinybird/README.md`, "once changes are merged, now point to production").
Before switching workspaces, make sure the local checkout is on the merged target branch,
pulled, and clean:

```bash
git status   # must be clean
git pull
tb workspace use lfx_insights
```

Show the resource diff and the exact `tb push` command, and get an explicit yes before running it.

## Reference

- Full doc: `services/libs/tinybird/README.md` — architecture overview (Lambda vs Bucketing),
  local Docker testing, data iteration, backup datasources.
- Workspaces: `lfx_insights_stg` (staging), `lfx_insights` (production), host
  `https://api.us-west-2.aws.tinybird.co`.
