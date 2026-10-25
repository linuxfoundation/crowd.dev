---
description: Local code review is the default review workflow; /review runs before every push and nothing is posted to GitHub without approval
paths:
  - '*'
---

# Code Review Workflow

- **`/review` is the reviewer for this repository.** Use it whenever a review of code changes is called for: before pushing, when a workflow skill (superpowers `requesting-code-review`, `finishing-a-development-branch`, `subagent-driven-development`, `verification-before-completion`) says to dispatch a reviewer, after a bot or human round on a PR, or when the user asks for a review. Do not substitute an ad-hoc read of the diff or a generic reviewer subagent; those have neither the verifiers nor the marker.
- **`REVIEW.md`** defines what counts as a finding, the tags (Important, Nit, Pre-existing), review depth by path, the human-approval tiers, and the summary format. Humans and agents follow the same file.
- **The push gate** (`.claude/hooks/review-state.sh gate`, a `PreToolUse` hook on Bash) blocks `git push` until the current change has a local review with zero open Important findings. When it blocks: run `/review`, fix or decline each Important finding (`bash .claude/hooks/review-state.sh decline <id> "<reason>"`), run `/review` again, then push. `CDP_SKIP_REVIEW=1` exists for the user to type for one push; never add it yourself.
- **`/review-pr`** is for reviewing someone else's pull request and drafting GitHub comments. It runs `/review <N>` for the bug hunt and never posts without the user's explicit approval of the draft.
- **Recurring findings** across reviews get fixed at the source: a lint rule, a line in `AGENTS.md` or `REVIEW.md`, or a skill. Not another review comment.
