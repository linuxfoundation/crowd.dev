---
name: write-api-e2e-tests
description: >
  Write API end-to-end tests. Use when adding or changing API endpoints, or when
  the user asks for API e2e, smoke, or contract tests.
allowed-tools: Bash, Read, Glob, Grep, Edit, Write, AskUserQuestion
---

# Write API end-to-end tests

Write or extend API end-to-end tests.

## When to use

- New or changed API endpoints.
- User asks for API e2e, smoke, or contract tests.
- Critical API behaviour needs regression coverage.

## When not to use

- Domain or SQL correctness → use `write-unit-tests`.
- Temporal, OpenSearch, or other eventual side effects outside the documented API e2e scope.
- Creating a new testing framework or suite style.

## Source of truth

Read before writing. Follow these ADRs and existing suite structure; do not
invent a parallel testing style.

- [ADR-0012](../../../docs/adr/0012-api-e2e-test-architecture.md) — runtime, isolation, supported surfaces, scope, and assertions.
- [ADR-0013](../../../docs/adr/0013-api-e2e-test-suite-design.md) — suite organisation, helpers, and conventions.

Current default entrypoint:

- `.github/scripts/public-api-e2e-tests.sh`

## Workflow

1. Identify the API surface. Default to Public API unless the user specifies otherwise.
2. Read ADR-0012 and ADR-0013.
3. Add or extend the appropriate suite and register it if required.
4. Run the affected suite locally and fix failures until green.
5. If required fixtures cannot be created through the API, prefer testing supported scenarios and explain any coverage gaps instead of seeding the database directly.
6. Suggest production testability improvements only when they make the API easier to test, and ask before changing production code.

## Run

Export the environment variables required by the suite entrypoint.

```bash
bash .github/scripts/public-api-e2e-tests.sh
```

Refer to ADR-0012 and the suite entrypoint for environment setup, reset behaviour,
and local development workflows.

## Guardrails

- Keep tests focused on observable API behaviour.

## Output

- Suites and cases added
- How to re-run
- Coverage gaps, if any
- Optional testability suggestions
