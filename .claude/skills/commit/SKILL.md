---
name: commit
description: Generate a commit message and commit staged changes using git commit -s -S.
allowed-tools: Bash, AskUserQuestion
---

Commit the current staged changes.

1. Run `git diff --staged`. If empty, tell the user there is nothing staged and stop.
2. Run `git log --oneline -5` to understand this repo's commit message style.
3. Run `git branch --show-current` to get the branch name. If it contains a `CM-XXX` ticket number, include it at the end of the subject in parentheses (e.g. `feat: add token refresh (CM-1234)`).
4. Generate a single commit message following Conventional Commits format: `type: description`. Subject max 72 characters. No scope required unless meaningful. No body, no extra trailers, no Co-Authored-By. (The `Signed-off-by` trailer is added automatically by `-s`.)
   - Valid types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`
5. Run `git commit -s -S -m "<message>"` using exactly the generated message.
6. Output only the commit hash and subject line from the result.
