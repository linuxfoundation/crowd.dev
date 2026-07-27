---
name: write-unit-tests
description: >
  Write focused Vitest unit tests. Use when adding or improving unit tests for
  business logic, data access, common services, or other server modules.
allowed-tools: Bash, Read, Glob, Grep, Edit, Write, AskUserQuestion
---

# Write unit tests

Write focused unit tests that follow the project's testing conventions.

## When to use

- User asks to add or improve unit tests.
- A change touches high-blast-radius logic (affiliations, merges, identity resolution, timelines, inference).
- A PR needs confidence in a pure or Postgres-backed function.

## When not to use

- Public or HTTP contract coverage → use `write-api-e2e-tests`.
- `packages_worker` (excluded from `pnpm test:server` for now; own vitest/packages-db).
- Temporal, Redis, or OpenSearch fixtures (not available yet).
- Broad "increase coverage %" requests without a clear unit under test.

## Source of truth

Read before writing. Follow these ADRs; do not invent a parallel testing style.

- [ADR-0008](../../../docs/adr/0008-how-we-write-unit-tests.md) — scenarios, `describe` grouping, assertions, mocking, and shared setup.
- [ADR-0007](../../../docs/adr/0007-test-factory-primitives-and-defaults.md) — factories and defaults.

## Workflow

1. Identify the unit under test (one function or decision path). Colocate tests as `<file>.test.ts`.
2. Read ADR-0007 and ADR-0008. Skim the nearest existing test in the same area if one exists.
3. Compose fixtures using `@crowd/test-kit` (`withQx` for Postgres-backed tests; factories and opt-in defaults per ADR-0007).
4. Write focused scenarios following ADR-0008 (grouping, naming, assertions, and mocking).
5. Run the affected tests and fix failures until green.
6. If production code is difficult to test, suggest a small testability seam and ask before changing production code.

## Run

Start the test database when needed:

```bash
./scripts/cli scaffold up-test
```

Run a focused test file:

```bash
pnpm test:server -- path/to/file.test.ts
```

Optional:

```bash
pnpm test:changed
pnpm test:watch -- path/to/file.test.ts
```

## Guardrails

- Prefer critical behaviours over trivial getters, setters, and thin wrappers.
- Keep production behaviour unchanged unless the user explicitly asks for a testability improvement.

## Output

- Scenarios covered
- How to re-run
- Optional testability suggestions
