---
name: review-pr
description: >
  Review a pull request against CDP architecture standards — fetches PR diff,
  verifies previous comments are addressed, validates PR metadata (title,
  branch, issue reference, size), runs a code-standards enforcer against every file
  in `.claude/rules/` and `.claude/hooks/guard-protected-files.sh`, and drafts
  inline review comments with suggested fixes. NEVER auto-posts comments or
  submits reviews — always presents a draft in the terminal for user approval
  before any comment lands on the PR. Use for someone else's pull request:
  "review PR #123", "check this PR", "draft comments on #123", "is this PR
  ready to merge?". For the current branch or "review this", use /review.
allowed-tools: Bash, Read, Glob, Grep, Agent, AskUserQuestion, Skill
---

# CDP PR Review

You are reviewing a pull request against the CDP (Community Data Platform) architecture standards and project conventions. Walk through each phase in order.

The review is backed by these living sources of truth — always pull current contents rather than relying on memory:

- `.claude/rules/*.md` — all project rules
- `.claude/hooks/guard-protected-files.sh` — the authoritative protected-files list
- `AGENTS.md` — project conventions, patterns in transition

---

## Phase 1: Input & Context Gathering

### Parse arguments

The args string follows this format: `<PR number> [extra instructions]`.

- First token is the PR number if numeric.
- Everything after it is extra instructions (e.g. "focus on backend", "check that previous comments were addressed").
- If no PR number is provided, use **AskUserQuestion** to ask for one.

### Determine repository

```bash
gh repo view --json nameWithOwner --jq '.nameWithOwner'
```

### Fetch PR metadata (parallel)

Run all of the following in a single turn:

```bash
# PR details
gh pr view <N> --json title,body,headRefName,baseRefName,author,files,additions,deletions,state,number

# Full diff
gh pr diff <N>

# Previous inline review comments
gh api repos/{owner}/{repo}/pulls/{N}/comments --paginate

# Previous review summaries
gh api repos/{owner}/{repo}/pulls/{N}/reviews --paginate

# Commit messages
gh api repos/{owner}/{repo}/pulls/{N}/commits --paginate --jq '.[].commit.message'

# Fetch both branches for merge-base check
git fetch origin <baseRefName> <headRefName>
```

If the diff is too large, save it to `/tmp/pr-<N>.diff` and read only changed `.ts`, `.vue`, `.sql`, `.md` files with `Read`.

### Load all project rules (dynamic — do not hardcode)

Glob `.claude/rules/*.md` and read every rule file. New rules added in the future must be picked up automatically; never maintain a hand-kept list here.

### Load the protected-files hook

Read `.claude/hooks/guard-protected-files.sh`. Parse its `case` statements and `if` conditions to build the authoritative protected-files list. Never maintain it by hand — parse the hook so it stays in sync.

---

## Phase 2: Launch Code Enforcer (background) and the Bug Hunt

Spawn a background **Agent** subagent (`code-standards-enforcer`) with `run_in_background: true`. Proceed to Phase 3 immediately while it runs in parallel.

Then invoke the `review` skill with the PR number (`<N>`) via the Skill tool. It fetches the PR head, runs the parallel bug hunters and verifiers per `REVIEW.md` (not the standards enforcer, which this skill already runs), and returns tagged findings (Important / Nit / Pre-existing) without writing a push-gate marker. Its findings join the enforcer's in Phase 5: Important maps to CRITICAL or SHOULD_FIX by the verifier's scenario, Nit maps to NIT, Pre-existing is mentioned once in the review body and never as an inline comment.

Prompt for the agent:

> You are a code-standards enforcer for the CDP (Community Data Platform) codebase. Your job is to read every changed file on the PR branch and flag violations of project conventions.
>
> **Branch:** `origin/<headRefName>`
> **Changed files:** (include the full list from Phase 1)
>
> For each file, read it with `git show origin/<headRefName>:<path>` and check against:
>
> 1. `.claude/rules/*.md` — glob and read all rule files
> 2. `AGENTS.md` — project conventions and patterns-in-transition
> 3. Domain checklists:
>    - Backend files (`backend/**`) → `.claude/skills/review-pr/references/backend-checklist.md`
>    - Frontend files (`frontend/**`) → `.claude/skills/review-pr/references/frontend-checklist.md`
>    - Service files (`services/apps/**`, `services/libs/**`) → `.claude/skills/review-pr/references/services-checklist.md`
>    - SQL / migrations (`backend/src/database/migrations/**`, any `.sql`) → `.claude/skills/review-pr/references/sql-checklist.md`
>
> Also read `.claude/hooks/guard-protected-files.sh` and parse its `case`/`if` patterns. For every changed file matching a protected pattern, emit a NIT finding with the hook's warning reason.
>
> **Severity calibration:**
>
> - **CRITICAL** — runtime bugs, security issues, new Sequelize usage in non-legacy files, new public endpoint without `validateOrThrow`/Zod schema, multi-tenant logic beyond `DEFAULT_TENANT_ID`, secrets hardcoded
> - **SHOULD_FIX** — documented style/structure violations (new class-based service/repo, `any` types in new code, DAL function added without checking for existing equivalents, missing license headers)
> - **NIT** — minor improvements, naming, protected-file awareness
>
> Return findings as JSON:
> `[{ "file": "...", "line": N, "severity": "CRITICAL|SHOULD_FIX|NIT", "rule": "<source>:<section>", "message": "...", "suggestion": "..." }]`
>
> **If you cannot quote the rule from a loaded rule file, checklist, or AGENTS.md, drop the finding. Hallucinated rules are worse than missed ones.**

---

## Phase 3: Verify Previous Review Comments

Check whether previously raised review comments were actually addressed in code. **Do NOT trust "resolved" status — read the actual code.**

1. Gather all inline comments and review bodies from Phase 1.
2. Skip trivial comments: nits, "+1", bot auto-comments, purely informational remarks.
3. For every **CRITICAL** or **SHOULD FIX** comment:
   1. Read the file on the PR branch: `git show origin/<headRefName>:<file>`
   2. Compare current code against what the comment requested.
   3. Classify: **FIXED** / **NOT FIXED** / **PARTIALLY FIXED** / **N/A**
4. Build a markdown table:

```markdown
| #   | Comment Summary                     | File                                   | Status    | Evidence                       |
| --- | ----------------------------------- | -------------------------------------- | --------- | ------------------------------ |
| 1   | Use queryExecutor not Sequelize     | services/libs/data-access-layer/foo.ts | FIXED     | Line 12 now uses queryExecutor |
| 2   | Missing validateOrThrow on endpoint | backend/src/api/members.ts             | NOT FIXED | Route still has no Zod schema  |
```

If no previous review comments, note "No previous review comments found" and move on.

---

## Phase 4: PR Metadata Validation

Validates PR metadata against `commit-workflow.md`.

### Checks

1. **PR title format** — must follow Conventional Commits format: `type: description`, lowercase description with no trailing period (checked by CI at `.github/workflows/pr-title-lint.yml`). The title must not carry an issue reference.
   - Valid types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`

2. **Branch name format** — should match `type/issue-<issue-number>-short-description` (e.g. `feat/issue-123-github-discussions`). Flag as NIT if non-conforming but otherwise well-formed.

3. **Issue reference** — the PR body should link the tracking issue with `Closes` or `Refs`. Strip HTML comments from the body first (template placeholders live there), then extract with `grep -oE '(Closes|Refs) linuxfoundation/lfx-cdp-insights-ops#[0-9]+'`. If none, flag SHOULD FIX. If the body uses a bare `Closes #<number>` / `Refs #<number>` instead, flag SHOULD FIX: it points at a `crowd.dev` issue, not the tracker. Other bare `#<number>` mentions (e.g. related crowd.dev PRs) are fine. Commit-body references are checked by `/preflight`.

4. **Branch rebased on main**:

   ```bash
   git merge-base --is-ancestor origin/main origin/<headRefName>
   ```

   If non-zero exit code, flag SHOULD FIX: branch needs a rebase.

5. **PR size** — if `additions > 1000`, note per `commit-workflow.md`'s 1000-line target.

6. **Commit signing** — at least one commit should have `Signed-off-by:` trailer (DCO).

Build a findings table:

```markdown
| Check           | Status | Detail                                         |
| --------------- | ------ | ---------------------------------------------- |
| PR title format | PASS   | `feat: add github discussions source`          |
| Branch name     | PASS   | `feat/issue-123-github-discussions`            |
| Issue reference | PASS   | Found linuxfoundation/lfx-cdp-insights-ops#123 |
| Branch rebased  | PASS   | origin/main is an ancestor                     |
| PR size         | PASS   | 342 additions                                  |
| DCO sign-off    | PASS   | All commits signed                             |
```

---

## Phase 5: Compile Context

Wait for the Phase 2 enforcer Agent to complete. Then compile all findings.

### Apply false-positive filter

Before surfacing any finding, drop it if:

- The `rule` field cannot be matched by string search in the loaded rule files, checklists, or AGENTS.md
- It relates to patterns from other codebases (Angular, Nuxt, Go-specific rules, etc.)
- It flags legacy Sequelize/class usage in files that are clearly already legacy (`backend/src/database/repositories/`, `backend/src/services/`) — only flag NEW usage

### Assemble the context block

1. **Previous comment verification** — Phase 3 table (or "No previous review comments found")
2. **PR metadata validation** — Phase 4 table
3. **Protected files touched** — list any matching `.claude/hooks/guard-protected-files.sh`, with the hook's warning reason
4. **Code enforcer findings** — filtered JSON results from Phase 2
   4b. **Bug hunt findings** — the `/review` findings from Phase 2, already verified; do not re-filter them
5. **Domain checklists applied** — note which checklists were used
6. **Extra user instructions** — any additional instructions from the args

---

## Phase 6: Present Draft Review for Approval (NEVER auto-post)

**You MUST NOT post inline comments, submit a review, or request changes without the user's explicit approval. Always present the draft first and wait for a clear go-ahead.** This applies every time, with no exceptions.

### Step 1 — Show the draft

Print the compiled context as a draft review summary:

1. **PR summary** — number, title, author, size, branch
2. **Phase 3 table** — previous comments and whether they were addressed
3. **Phase 4 table** — PR metadata validation
4. **Protected files touched** — list with hook reasons
5. **Proposed inline comments** — one block per finding: file:line, severity, rule citation, message, suggested fix. Number them so the user can reference individual items.
6. **Proposed review body** — summary text for the top of the review
7. **Proposed review verdict** — COMMENT / APPROVE / REQUEST_CHANGES, with reasoning

### Step 2 — Ask for approval

Use **AskUserQuestion** with options:

- "Post all comments as drafted"
- "Post with changes — I'll tell you which comments to drop or edit"
- "Don't post — just keep the summary here"

Do NOT proceed until the user explicitly picks an option. Treat silence or ambiguous replies as "don't post".

### Step 3 — Only after approval: post with `gh`

Once the user approves (with or without edits), apply their edits and submit one review carrying every approved inline comment. Write the payload as a JSON file with the Write tool (so quotes, backslashes and multi-line suggestion blocks are encoded by you as JSON, never pasted raw into a shell string), then post it:

```json
{
  "event": "COMMENT",
  "body": "<approved review body>",
  "comments": [
    {
      "path": "services/apps/foo/src/bar.ts",
      "line": 42,
      "side": "RIGHT",
      "body": "<approved comment>"
    }
  ]
}
```

```bash
jq -e . <scratchpad>/review-<N>.json >/dev/null && gh api --method POST repos/{owner}/{repo}/pulls/{N}/reviews --input <scratchpad>/review-<N>.json
```

Keep `--method POST` first: the project allow list pre-approves `gh api repos*` for the read-only calls above, and this spelling makes the one mutating call go through a permission prompt.

Use `REQUEST_CHANGES` or `APPROVE` as `event` only when the user chose that verdict. If the user said "don't post", stop here — do not run any PR-mutating `gh` command.

---

## Additional Rules

### PR size check

If `additions > 1000`, include in the review body:

> **Note:** This PR has {additions} additions, which exceeds the recommended 1000-line target per `commit-workflow.md`. Consider splitting into smaller, independently reviewable PRs.

### New contributor awareness

```bash
gh pr list --author <author> --state merged --limit 5 --json number | jq 'length'
```

If the author has fewer than 5 merged PRs to this repo, be more educational in inline comments — explain the **why** behind each rule, not just the **what**.

### Extra instructions

If the user passed extra instructions after the PR number, prioritize those areas but still execute the full review pipeline.
