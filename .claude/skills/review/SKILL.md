---
name: review
description: >
  Use when changes need a code review before they leave the machine: before any
  git push, when the push gate refuses a push, after fixing review findings,
  after a bot or human review round on a PR, when a workflow skill says to
  dispatch a code reviewer, when the user says "review this", "review my
  changes", or asks whether a branch is ready. The only reviewer for this repo;
  it never posts to GitHub. For someone else's PR, /review-pr calls it.
allowed-tools: Bash, Read, Glob, Grep, Agent, Skill, ReportFindings, Edit, Write
---

# CDP local code review

Parallel bug hunters in fresh contexts, independent verifiers that vote on every finding, a report that starts with a count, and a marker the push gate reads. `REVIEW.md` at the repository root is the contract; read it first on every run.

## Arguments

`[target] [--depth quick|standard|deep] [--fix] [--fresh]`

| Target         | Meaning                                                                             |
| -------------- | ----------------------------------------------------------------------------------- |
| none           | The current change: everything in the working tree and commits since the merge base |
| `<PR number>`  | A pull request, read via `gh`; reviewed in a fetched ref, no marker is written      |
| `<ref>..<ref>` | A commit range; no marker is written                                                |
| `<path> [...]` | Only these paths of the current change; no marker is written                        |

Only a review of the whole current change (no target) writes the marker the push gate reads. `--fix` fixes Important findings after the report and runs the next round. Without it the skill reports, writes the marker when allowed, and stops. The base is `origin/main`; a session that exports `CDP_REVIEW_BASE` (see `REVIEW.md`, Prerequisites) uses that ref for both the review and the gate.

## Phase 0: Scope

Decide the target first. The status, round and marker steps below apply only to the whole current change; for a PR number, a range, or a path subset skip them and go straight to the diff.

**Whole current change:**

```bash
git fetch origin main --quiet
bash .claude/hooks/review-state.sh status   # unreviewed | clean | open | empty, plus the marker if any
bash .claude/hooks/review-state.sh last     # previous round on this branch: round number and findings
BASE=$(bash .claude/hooks/review-state.sh base)
```

- `status` is `clean` and the user did not ask for a re-review: say so in one line and stop. The gate will let the push through.
- `status` is `empty`: nothing to review, stop.
- New files you created this session that are untracked belong to the change: `git add -N <those files>` so the diff and the key include them. Do not add unrelated untracked files.
- Round = `last.round + 1` when `last` is for this branch and either its key differs from the current one or it still has open Important findings; otherwise 1. Round 4 is not allowed: stop, show the still-open Important findings, and tell the author the approach needs reconsidering (REVIEW.md, Convergence). After that reconsideration the author starts a new cycle with `--fresh`, which treats `last` as round 0; `--fresh` is typed by the user, never added by you.
- A previous `t1` (failing test) closes when the Phase 1 rerun passes and is re-raised when it still fails; it never goes through the verifiers.

Write the diff once and share it with every agent:

```bash
SCRATCH=<your scratchpad directory>
git diff $BASE -- . ':(exclude)pnpm-lock.yaml' > $SCRATCH/review.diff
git diff $BASE --name-only -- . > $SCRATCH/review.files
```

**PR number:** `gh pr view <N> --json title,body,headRefName,baseRefName`, `git fetch origin pull/<N>/head:refs/remotes/origin/pr/<N>`, set `BASE=$(git merge-base origin/<baseRefName> origin/pr/<N>)`, and produce the diff with `git diff $BASE origin/pr/<N>`. Tell the hunters and verifiers to read head code with `git show origin/pr/<N>:<path>` instead of the working tree. `ALREADY_REPORTED` is `[]`.

**Range or paths:** `BASE` is the range start (or the merge base for paths); produce the diff with `git diff <start> <end>` or `git diff $BASE -- <paths>`. `ALREADY_REPORTED` is `[]`.

Drop files on the `REVIEW.md` skip list from `review.files`. Depth: the deepest tier in `REVIEW.md` hit by any remaining file, unless `--depth` is given.

Intent, in this order: the PR body, then the commit messages since `BASE`, then what the user said in this session. Write it as two or three sentences for the agents.

## Phase 1: Verification evidence

Skip at quick depth. Skip for a PR number, and for a range or path subset whose end is not HEAD: the code under review is not checked out, so record "not run: head not checked out" instead. Otherwise run the tests that exercise the change and keep the real output:

```bash
pnpm exec vitest run <changed test files, and test files beside changed source files>
```

If the tests need infrastructure that is not up, record "not run: needs <what>" with the command. A failing test is finding `t1`, tagged Important, with the failing assertion as its scenario; it skips verification, goes straight into the report and the marker as an open Important finding, and the hunters still run.

## Phase 2: Hunters, in parallel

One `bug-hunter` Agent per lens, all launched in a single message, each with `run_in_background: true` and its lens as the `description`:

| Depth    | Lenses                                                                      |
| -------- | --------------------------------------------------------------------------- |
| quick    | generalist                                                                  |
| standard | correctness, data-and-scale, concurrency-and-failure, security              |
| deep     | correctness, data-and-scale, concurrency-and-failure, security, adversarial |

At standard and deep for the whole current change, a range, or a path subset, also launch the `code-standards-enforcer` agent on the changed files, exactly as `/review-pr` Phase 2 does; its CRITICAL findings enter the pool as `important`, SHOULD_FIX and NIT as `nit`. For a PR number skip it: `/review-pr` already runs the enforcer itself.

If a session does not list `bug-hunter` as an agent type, launch `general-purpose` with the full body of `.claude/agents/bug-hunter.md` as the prompt preamble.

Hunter prompt, filled in:

```text
LENS: <lens>
BASE: <sha>
DIFF_FILE: <SCRATCH>/review.diff
CHANGED_FILES: <one per line>
INTENT: <two or three sentences>
ALREADY_REPORTED: <JSON array from last.findings, or []>
DEPTH: <quick|standard|deep>
Repository root: <absolute path>. Read REVIEW.md and AGENTS.md first. Return only the JSON described in your instructions.
```

Wait for all of them. Parse each JSON result; a hunter that returns prose instead of JSON is re-run once with "Return only the JSON object" appended. A lens that still has no parsable result (error, timeout, prose twice) is a missing lens: report it as finding `m1`, tagged Important, with the lens name as the scenario, and skip Phase 6 so the gate stays closed. A review with a missing lens is never reported as clean.

## Phase 3: Merge the pool

Give each finding an id `h<n>`. Then:

- Drop anything on a skipped file.
- Drop anything whose `file:line` is within 3 lines of a finding in `ALREADY_REPORTED` with the same claim.
- Merge duplicates across lenses (same file, lines within 3, same claim): keep the fuller scenario, note both lenses.
- Keep the hunters' `root_cause_note` for the report when the change is a bug fix.

Count `raised` = pool size after merging.

## Phase 4: Verifiers, in parallel

Group the pool by file. One `finding-verifier` Agent per group, at most 8 agents per round (merge small groups when there are more files). At deep depth, two verifiers per group with no shared context. Pass `PREVIOUS_IMPORTANT` (open Important findings from `last`) to the group whose files they touch, or to the first group; when `PREVIOUS_IMPORTANT` is not empty and the pool is empty, launch one verifier anyway with `FINDINGS: []` so every previous finding gets a `fixed` verdict.

Verifier prompt, filled in:

```text
BASE: <sha>
DIFF_FILE: <SCRATCH>/review.diff
INTENT: <same as hunters>
FINDINGS: <JSON array for this group>
PREVIOUS_IMPORTANT: <JSON array or []>
Repository root: <absolute path>. Return only the JSON described in your instructions.
```

Resolve each finding:

| Verifier votes              | Result                                         |
| --------------------------- | ---------------------------------------------- |
| CONFIRMED (one verifier)    | kept, verifier's severity and pre_existing     |
| CONFIRMED, CONFIRMED (deep) | kept, verifier's severity and pre_existing     |
| CONFIRMED, REJECTED (deep)  | kept as Nit, note "1 of 2 verifiers confirmed" |
| REJECTED                    | dropped                                        |

Tag: `pre_existing` wins and becomes **Pre-existing**; otherwise `important` becomes **Important** and `nit` becomes **Nit**. Cap Nits at 5, keeping the worst outcomes. A previous Important finding stays open and is carried into this round's list with its original id unless a verifier reported it `fixed: true`; no verdict means still open.

## Phase 5: Report

First line, always, in the `REVIEW.md` summary format:

```text
Review: <I> Important, <N> Nit, <P> Pre-existing (<raised> raised, <dropped> dropped by verifiers)
Review: nothing needs fixing (<raised> raised, <raised> dropped by verifiers)
```

Then, when the `ReportFindings` tool is available, report every kept finding through it (Important first, `verdict: CONFIRMED`, `category` from the lens, `short_summary` under 60 characters) and print nothing else about them. Without the tool, print one block per finding: tag, `file:line`, scenario, suggested fix. Add the root-cause note for bug fixes, the Phase 1 evidence line, the depth, and the round number.

## Phase 6: Marker

Whole current change only: never for a PR number, a range, or a path subset, and never when a lens is missing. Important = kept Important findings that are not Pre-existing, plus `t1` when the tests failed, plus previous Important findings still open. `mark` recounts `important_open` from the findings and refuses a marker whose count disagrees. The gate also requires the working tree to match HEAD before a push, so after a review of uncommitted work the author commits and the review stays valid.

```bash
bash .claude/hooks/review-state.sh mark <<'EOF'
{
  "round": <n>,
  "depth": "<depth>",
  "important_open": <count>,
  "raised": <raised>,
  "dropped": <dropped>,
  "findings": [
    {"id": "r<round>-1", "tag": "Important", "file": "...", "line": 42, "summary": "...", "lens": "correctness"}
  ]
}
EOF
```

Spell the hook path literally in every command, as above: the gate treats a heredoc on a line that names `.claude/hooks/review-state.sh` as data, but it cannot see through a shell variable. Carried-over findings keep their original id. The author declines an Important finding with a reason through `bash .claude/hooks/review-state.sh decline <id> "<reason>"`; record the reason in the report when that happens.

## Phase 7: Fix loop (`--fix` only)

1. Fix every Important finding in the working tree, smallest correct change each, Nits only when the fix is one obvious line. Do not commit.
2. Run the relevant tests again and show the output.
3. If the fix step changed nothing (`bash .claude/hooks/review-state.sh key` prints the same key as before), stop: the open findings need a decision from the author, a fix or a decline with a reason. Do not start another round on an unchanged change.
4. Start the next round at Phase 0. Rounds 2 and 3 review the whole current diff but hunters receive `ALREADY_REPORTED`, so only new defects and the resolution of old ones come back.
5. Stop when `important_open` is 0 or round 3 ends with findings still open; in the second case say so and hand the decision to the author.

## Rules

- Never post to GitHub from this skill. `/review-pr` owns comments and only after the user approves a draft.
- Never commit. The marker survives the user's commit because the key is content-based.
- Never bypass the gate on the user's behalf. `CDP_SKIP_REVIEW=1` is typed by the user for one push.
- Never pad the report. A clean result is a result.
- When the same finding comes back across branches, propose the source fix (lint rule, `AGENTS.md` line, `REVIEW.md` line) instead of restating it.
