# ADR-0013: API e2e test suite design

**Date**: 2026-07-24
**Status**: accepted
**Deciders**: Yeganathan S

## Context

API e2e tests need a maintainable HTTP suite pattern that can grow as endpoints are added. Without shared structure, suites tend to split by HTTP method, accumulate one-off asserts, and become hard to extend or review.

We want a thin, boring pattern: shared helpers, clear grouping, and an obvious place to add the next resource. This is how we write durable e2e cases until a fuller PR-time API e2e harness exists (backlog).

This ADR is the **suite design** (how tests are written). Runtime, isolation, scheduled CI, which surfaces we cover, and ops live in [ADR-0012](./0012-api-e2e-test-architecture.md).

## Decision

Implement API e2e tests as HTTP bash script(s) with this structure. Current entrypoint: [`.github/scripts/public-api-e2e-tests.sh`](../../.github/scripts/public-api-e2e-tests.sh). New suites should reuse this pattern (same script or additional scripts), not invent a parallel style.

The suite is **thin**: assert the HTTP contract and critical flows so regressions show up early. Leave exhaustive edge-case matrices to unit or focused contract tests.

| Layer | Role |
| --- | --- |
| Helpers | `api <version> <method> <path> [body]` (call), `check` (soft status + body preds), `require` (hard fail for seed) |
| Seed | Create shared fixtures once per run over HTTP |
| Suites | One `suite_*` per **resource path**, not per HTTP method |
| Cases | One exchange: `api` then `check`. Stateful resources stay ordered as a short story |
| Registration | Call each suite from `main` |

**Rules of thumb**

- Group by resource (`/organizations`, `/members/:id/identities`, …). Put GET and POST for the same path in the same suite.
- Prefer one meaningful `check` per request over many micro-asserts.
- Cover the contract and critical flows. Leave deep normalization / matrix cases to unit or focused contract tests.
- Keep seed HTTP-only. Suites that need non-API fixtures (e.g. activity-backed projects) may cover the route with empty/error paths until a fixture exists.
- Name people and orgs like real data so failures read naturally.

## Alternatives Considered

### Alternative 1: One suite per HTTP method (`suite_get_*`, `suite_post_*`)

- **Pros**: Mirrors OpenAPI operation IDs one-to-one.
- **Cons**: Splits one resource across files/functions; harder to see the full surface; encourages duplicate setup.
- **Why not**: Resource path is the stable unit as the API grows.

### Alternative 2: Flat table of request/response cases only

- **Pros**: Very uniform; easy code-gen later.
- **Cons**: Poor fit for stateful flows (create → list → update → verify).
- **Why not**: We need both independent cases and short ordered stories.

### Alternative 3: No shared structure (each contributor invents a style)

- **Pros**: Maximum local freedom.
- **Cons**: Inconsistent quality; expensive to review and extend.
- **Why not**: A thin shared pattern scales better than ad-hoc scripts.

## Consequences

### Positive

- Adding an endpoint is: open (or create) the resource suite, add `api` + `check`, register in `main` if new.
- Reviews focus on cases, not harness inventiveness.
- Soft `check` keeps the run going so one failure does not hide the rest.

### Negative

- Stateful suites are order-dependent; a mid-story failure can cascade.
- Bash is less ergonomic than a typed test runner for large suites.

### Risks

- Script grows past a comfortable size → split helpers vs suites, or one file per resource / surface, without changing the grouping rule.
- Pressure to turn e2e tests into a full matrix → push depth to unit/contract tests; keep this suite thin.
