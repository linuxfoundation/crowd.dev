---
description: Enforce Nygard ADR template structure on files written under docs/adr/
paths:
  - 'docs/adr/**/*.md'
---

# ADR Format Enforcement

When writing or editing any `.md` file under `docs/adr/`, enforce these rules:

## Exempt files

`README.md` and `template.md` are index/template files — they are **not** ADRs
and are exempt from the section requirements below.

## Mandatory sections for ADR files

Every file matching `docs/adr/[0-9]*.md` **must** contain all of the following,
in this order:

1. `# ADR-NNNN: [Title]` — H1 heading with 4-digit sequential ID
2. `**Date**:` — ISO date (YYYY-MM-DD)
3. `**Status**:` — one of: `proposed`, `accepted`, `deprecated`, `superseded by ADR-NNNN`
4. `**Deciders**:` — who was involved in the decision
5. `## Context` — 2–5 sentences on the situation and forces
6. `## Decision` — 1–3 sentences stating the change
7. `## Alternatives Considered` — at least one alternative with Pros, Cons, Why not
8. `## Consequences` — with sub-sections `### Positive`, `### Negative`, `### Risks`

If any mandatory section is missing, **stop and ask the user** to provide the
missing information before writing the file. Do not write a partial ADR.

## Numbering

- IDs are zero-padded to 4 digits: `0001`, `0002`, `0003`, …
- Assign the next sequential number by scanning existing files with `Glob docs/adr/[0-9]*.md`.
- Never reuse a number, even if a previous ADR is deprecated.

## File naming

`docs/adr/NNNN-kebab-case-title.md` — all lowercase, words separated by hyphens.

## Index maintenance

After writing or changing the status of an ADR, update the index table in
`docs/adr/README.md`:

```markdown
| [ADR-NNNN](./NNNN-kebab-title.md) | Title | accepted | YYYY-MM-DD |
```

Remove the `_none yet_` placeholder row once the first real ADR is added.
