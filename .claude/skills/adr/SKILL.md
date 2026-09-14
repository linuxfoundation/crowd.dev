---
name: adr
description: >
  Record an architecture decision as an ADR in docs/adr/. Use when choosing
  between frameworks, libraries, databases, or architectural patterns; stating
  a decision with reasoning ("we decided X instead of Y because..."); or
  querying past decisions ("why did we choose X?").
allowed-tools: Read, Write, Edit, Glob, Grep, AskUserQuestion
---

# Architecture Decision Records

You are recording or retrieving an Architecture Decision Record (ADR) for this
project. ADRs live in `docs/adr/` at the repo root.

## When to record

**Record these decisions:**
- Technology selections (frameworks, libraries, databases, cloud providers)
- Architectural patterns (state management, caching strategy, API design)
- Data modeling choices (schema design, indexing, query approach)
- Infrastructure and deployment models
- Security, authentication, or testing strategy changes
- Patterns in transition: any documented migration (e.g. Sequelize → pg-promise, classes → functions, multi-tenant → single tenant) is a textbook ADR candidate

**Skip:** trivial choices (variable naming, formatting, minor refactors).

## ADR template

Every ADR file must include all of these sections:

```markdown
# ADR-NNNN: [Decision Title]

**Date**: YYYY-MM-DD
**Status**: proposed | accepted | deprecated | superseded by ADR-NNNN
**Deciders**: [who was involved]

## Context
[2–5 sentences describing the situation, constraints, and forces at play]

## Decision
[1–3 sentences stating the change clearly and unambiguously]

## Alternatives Considered

### Alternative 1: [Name]
- **Pros**: [benefits]
- **Cons**: [drawbacks]
- **Why not**: [specific rejection reason]

## Consequences

### Positive
- [benefit 1]

### Negative
- [trade-off 1]

### Risks
- [risk and mitigation]
```

## Workflow — recording a new ADR

1. **Scan existing ADRs** — `Glob docs/adr/[0-9]*.md` to find the highest existing number.
2. **Assign next ID** — next sequential 4-digit number (e.g., `0003`).
3. **Gather context** — ask the user for any missing details: who the deciders were, what alternatives were seriously considered, and what the consequences are.
4. **Draft the ADR** — populate all mandatory sections from the template above.
5. **Present the draft** — show it to the user for review before writing any file.
6. **Write the file** — `docs/adr/NNNN-kebab-title.md` (kebab-case title, all lowercase).
7. **Update the index** — append a new row to the `| ADR | Title | Status | Date |` table in `docs/adr/README.md`.

## Workflow — reading / querying ADRs

1. Check if `docs/adr/README.md` exists. If not, offer to start the ADR directory.
2. Read the README index table and find entries relevant to the user's question.
3. Read the matching ADR file and summarise the **Context** and **Decision** sections.
4. If no ADR matches, suggest recording one now.

## Quality standards

- Each ADR should be readable in under 2 minutes.
- Every rejected alternative must include a **Why not** reason.
- When a decision is superseded, update the old ADR's **Status** field to `superseded by ADR-NNNN` and create the new ADR with a back-reference in its Context.
- Keep one decision per ADR.
