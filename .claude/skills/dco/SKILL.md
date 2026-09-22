---
name: dco
description: >
  Recover from missing DCO sign-off on commits. Handles the single-commit
  amend, older-commit recovery via interactive rebase or cherry-pick,
  and explains the Probot DCO check that blocks PRs without sign-off.
  Use when a PR fails the DCO check, when a commit needs a Signed-off-by
  trailer added retroactively, or when sign-off was forgotten during
  rebase / cherry-pick / amend.
allowed-tools: Bash, Read, Edit
---

# DCO Sign-off Recovery

All commits in this repo must carry a `Signed-off-by:` trailer (Developer Certificate of Origin). The Probot DCO check on PRs blocks merge until every commit is signed.

The standard way to add it is with `--signoff` at commit time. This skill handles the cases where that was missed.

## Case 1: Last commit only

```bash
git commit --amend --signoff -S --no-edit
git push --force-with-lease
```

`--force-with-lease` is preferred over plain `--force` — it refuses if someone else has pushed to the branch since you last fetched.

## Case 2: Older commit on the same branch

Find the commit hash of the oldest unsigned commit:

```bash
git log --pretty='%h %s %(trailers:key=Signed-off-by,valueonly)' origin/main..HEAD
# Any line with an empty trailing field is unsigned
```

Then interactively rebase from one commit _before_ that:

```bash
git rebase -i <parent-of-unsigned-commit>
# In the editor, change `pick` to `edit` for each unsigned commit
# Save and exit; the rebase stops at each `edit` line so you can amend
```

For each commit in the rebase, replace it with a signed version:

```bash
git commit --amend --signoff -S --no-edit
git rebase --continue
```

When done, force-push:

```bash
git push --force-with-lease
```

## Case 3: Cherry-pick / merge brought in unsigned commits

Cherry-pick with sign-off baked in:

```bash
git cherry-pick --signoff <sha>
```

If you already cherry-picked without it, fall back to Case 1 or Case 2 above.

## Verifying the whole branch

Before pushing, verify every commit ahead of origin/main has both DCO and GPG:

```bash
git log --format='%G? %(trailers:key=Signed-off-by,valueonly,separator=%x20) %h %s' origin/main..HEAD
```

Each line must start with `G` or `U` (good GPG signature) AND carry a non-empty `Signed-off-by` value before the SHA. Codes `N` / `B` / `E` need investigation. See `.claude/rules/commit-workflow.md` for the canonical signing policy.

## What the Probot DCO check looks for

The check passes when every commit message includes a `Signed-off-by: Name <email>` trailer matching the commit author's email. The `--signoff` (or `-s`) flag adds this trailer automatically using your `user.name` and `user.email` git config.

A failing DCO check on a PR will show a "Details" link explaining which commits are missing sign-off.
