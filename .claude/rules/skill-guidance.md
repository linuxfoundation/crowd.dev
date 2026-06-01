---
description: Guides Claude to suggest the right skill based on user intent
paths:
  - '*'
---

# Available Skills

This project has guided skills for common workflows. **Proactively suggest the relevant skill** when a user's request matches one of these:

| Skill         | When to Suggest                                                                                     |
| ------------- | --------------------------------------------------------------------------------------------------- |
| `/commit`     | Commit staged changes, generate a commit message, "commit this", "make a commit"                   |
| `/preflight`  | Before submitting a PR, check if code is ready, validate changes, verify a branch, run all checks  |
| `/dco`        | PR failing DCO check, missing Signed-off-by, sign-off forgotten on a commit                        |
| `/review-pr`  | Review a PR, audit code changes, check PR quality, validate a PR against standards                  |
| `/adr`        | Record an architecture decision, choose between frameworks/libraries/patterns, query past decisions  |
| `/scaffold-snowflake-connector` | Add a new Snowflake-connector data source or integration                          |
| `/packages-worker-setup` | First-time setup of packages-db and github-repos-enricher for a new engineer    |
| `/packages-worker-add-entrypoint` | Scaffold a new sibling worker inside packages_worker (npm, OSV, scorecard, etc.) |

## Trigger Phrases

**`/preflight`** — match any of these intents:
- "Ready for PR", "Check my code", "Validate changes", "Before I submit"
- "Is my branch ready?", "Run checks", "Lint and build", "Pre-PR validation"
- Any indication that development work is finished

**`/dco`** — match any of these intents:
- "DCO check failing", "Missing sign-off", "Signed-off-by"
- "DCO bot blocked my PR", "forgot --signoff"
- Any mention of the DCO Probot check

**`/review-pr`** — match any of these intents:
- "Review this PR", "Check PR quality", "Audit code changes"
- "Review #123", "Is this PR ready to merge?"
- Any mention of reviewing or auditing a pull request

**`/adr`** — match any of these intents:
- "Record this decision", "Let's write an ADR", "ADR for…"
- "Why did we choose X?", "What was the reason for…"
- "Should we use X or Y?" when the choice is architectural (framework, database, pattern, API style)
- "Document this trade-off", "Capture this as an ADR"
- Any moment where a significant technical alternative was considered and rejected
- Discussions about the patterns in transition (Sequelize → pg-promise, classes → functions, etc.)

**`/scaffold-snowflake-connector`** — match any of these intents:
- "Add a new Snowflake connector", "New integration for [platform]"
- "Scaffold a new data source", anything about adding a platform to `snowflake_connectors`

**`/packages-worker-setup`** — match any of these intents:
- "Set up packages worker", "how do I run the enricher", "first time on this branch"
- "Get packages-db running", "packages-db won't start", "ENRICHER_GITHUB_TOKENS"
- Any first-time setup question specific to `packages_worker` or `packages-db`

**`/packages-worker-add-entrypoint`** — match any of these intents:
- "Add a new packages worker", "scaffold a sibling worker", "new entry point in packages_worker"
- "Add npm ingestion", "add OSV worker", "add scorecard runner"
- Any request to create a new `src/bin/*.ts` worker inside `packages_worker`
