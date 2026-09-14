---
description: Commit conventions, branch naming, PR format, PR size guidelines, sign-off + GPG signing, and JIRA tracking workflow
paths:
  - '*'
---

# Commit & PR Workflow

## Commit Conventions

- Follow the [Conventional Commits](https://www.conventionalcommits.org/) format: `type: description`
- A scope is **optional** — most commits in this repo do not use one
- Valid types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`
- Use present tense, imperative mood: "add feature" not "added feature"
- Include the JIRA ticket **inline at the end** of the description, in parentheses
- Examples:
  - `feat: add github discussions integration (CM-1164)`
  - `fix: improve member merge suggestions (CM-1137)`
  - `chore: update stale dependency versions`

## Commit Signing

All commits must be both DCO-signed and GPG-signed:

- **DCO sign-off (`--signoff`)** — required by LF governance; validated by the Probot DCO check in CI. The `Signed-off-by: Name <email>` trailer is appended automatically when you pass `--signoff` (or `-s`).
- **GPG signature (`-S`)** — required by repo policy. Configure a signing key once:

  ```bash
  git config --global user.signingkey <KEY_ID>
  git config --global commit.gpgsign true
  ```

Standard commit command:

```bash
git commit --signoff -S -m "type: description (CM-XXX)"
```

If signing fails, fix the underlying issue — do not push unsigned commits. To verify signature status on a branch's commits:

```bash
git log --format='%G? %h %s' origin/main..HEAD
```

Acceptable `%G?` codes: `G` (good signature) or `U` (good signature, key not in local trust db). Codes `N`, `B`, or `E` need investigation.

## Branch Naming

- Format: `type/CM-<number>-short-description` (e.g. `feat/CM-1164-github-discussions`)
- The JIRA key in the branch name lets `/commit` include it automatically in the message

## PR Titles

- PR titles must contain a JIRA key — validated by CI (`.github/workflows/pr-title-jira-key-lint.yml`)
- Format: `type: description (CM-XXX)` — Conventional Commits format with the JIRA key in parens at the end
- Example: `feat: add github discussions source (CM-1164)`

## PR Size & Focus

- **Target under 1000 lines of diff** — one feature, one bug fix, or one refactor per PR
- **Don't bundle unrelated changes** — keeps reviews focused and rollbacks clean

## JIRA Tracking

Before starting any work:

1. Check if there is a JIRA ticket in the `CM` project
2. Create one if untracked work
3. Include `CM-XXX` in commit messages and PR title
