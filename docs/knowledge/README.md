# Knowledge notes

Short, typed notes that describe how CDP is built, for agents and for people new to the code.
Start from the repository's `AGENTS.md`; these notes go one level deeper.

| Note                                       | What it covers                                                                  |
| ------------------------------------------ | ------------------------------------------------------------------------------- |
| [`sys-cdp.md`](sys-cdp.md)                 | Every service under `services/apps/`, grouped by what it does                   |
| [`data-flow.md`](data-flow.md)             | How an activity travels from a source platform to Tinybird and LFX Insights     |
| [`member-identity.md`](member-identity.md) | How members, organizations and their identities are stored, resolved and merged |

## Conventions

Each note starts with frontmatter:

```yaml
---
type: system | data-flow | data-model | runbook | contract
slug: sys-cdp
name: Human-readable title
repo: crowd.dev
owner: CDP team
depends_on: [slugs of notes or systems this relies on]
used_by: [slugs of notes or systems that rely on this]
sources: [code paths this note is grounded in]
---
```

The slugs follow the prefixes used by the LFX knowledge graph (`sys-`, `proj-`, `dec-` and so
on), so these notes can move into it without renaming.

Notes are maps, not manuals. Where a deeper document already exists, such as an ADR or
`services/libs/tinybird/*.md`, a note links to it rather than repeating it. When code changes
make a note wrong, fix the note in the same pull request. `.claude/hooks/knowledge-doc-check.sh`
reminds Claude Code when it edits a file that a note lists under `sources`; a source ending in
`/` covers everything below it.
