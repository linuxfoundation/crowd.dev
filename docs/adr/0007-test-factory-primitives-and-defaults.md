# ADR-0007: Test factory primitives and defaults

**Date**: 2026-07-10
**Status**: accepted
**Deciders**: Yeganathan S

## Context

Shared test factories are easy to turn into hidden scenario builders: they invent data the test never declared, blur what is under assertion, and drift from the real insert shapes used in production code (see ADR-0006).

We need a simple, durable rule for how test data is created — what the factory does, what defaults may fill, and what the test must own — so the pattern does not divert as more factories are added.

## Decision

Treat test setup as **composition of sharp primitives**, not smart world-builders. Keep three layers separate.

### 1. Primitives

- One job: **persist what they are given**, preferably in bulk, via the DAL.
- Input stays close to schema insert types (`*DbInsert` and small, explicit extensions when a factory accepts related children in one call).
- No faker inside primitives. No silent graph scaffolding. No behavioral guesses.
- When a primitive accepts structured input (for example a hierarchy), it may **derive structural fields** from that shape (links, type/level, denormalized parent labels). That derivation is part of the primitive’s contract — not a default.

### 2. Defaults (opt-in)

- Separate helpers (`withXDefaults`) fill an **allowlist** of missing label / harmless system fields **on rows the caller already wrote**.
- Call sites opt in: `createX(qx, withXDefaults([...]))`. Defaults are never baked into primitives.
- Semantics:
  - **Allowlist only** — unlisted fields are untouched.
  - **Explicit always wins** — a provided value is never overwritten.
  - **Only** `undefined` **is missing** — omitted / `undefined` may be filled; `null` **is intentional and sticks**.
  - **Labels / harmless flags only** — e.g. generated `id`, display names, join timestamps, inactive-path status flags. Ask per field: *if we invent this, could we change which production branch the test hits?* If yes, it does not belong in defaults.
  - **Caller owns scenario identity** when the value defines the case (names, slugs, platforms, dates, relationship targets, and any field that makes two cases different).

Defaults do **not** build a realistic entity or graph. They only patch allowlisted gaps (e.g. `displayName`, `joinedAt`). Identities, org stints, activities, and other scenario data stay with the test.

### 3. Test composition

- The suite owns behavioral fields and graph shape.
- Prefer inline fixtures and spreads in the test file.
- File-local seed helpers only after real repetition; promote into the shared test kit only when a second suite needs the same helper.
- Do not grow suite-specific “build the whole world” APIs in the shared kit.

This stays aligned with ADR-0006: factories may widen partial write payloads and fill defaults; they do not redefine row or DAL contracts.

## Alternatives Considered

### Alternative 1: Always-on smart factories that scaffold graphs

- **Pros**: Shortest test setup; one call builds a “ready” world.
- **Cons**: Hides scenario data; easy to pass for the wrong reason; hard to see what a test asserts.
- **Why not**: Tests must declare behavior. Defaults are ergonomics, not fixtures.

### Alternative 2: No shared defaults — every test passes full insert rows

- **Pros**: Maximum explicitness; zero magic.
- **Cons**: Noisy boilerplate for ids/labels/flags that rarely matter to the assertion.
- **Why not**: Opt-in allowlisted defaults keep noise down without hiding scenario data.

### Alternative 3: Defaults baked into every primitive call

- **Pros**: Slightly shorter call sites.
- **Cons**: Forces defaults even when a test wants raw inserts; harder to see the boundary between persistence and convenience.
- **Why not**: Opt-in composition keeps primitives sharp and defaults obvious at the call site.

## Consequences

### Positive

- Factories stay readable, composable, and reviewable; scenario intent stays in the test.
- Same overwrite semantics everywhere (`undefined` fill, `null` preserved, allowlist only).
- New factories have a clear checklist instead of reinventing “helpful” behavior.
- Stays aligned with ADR-0006’s insert/row types.

### Negative

- Callers must compose `withXDefaults` explicitly.
- Suites carry more scenario data in the test file than a mega-helper would.

### Risks

- **Defaults creep** — mitigate by reviewing allowlists against “could this change the branch we hit?” and rejecting behavioral defaults in review.
- **Scenario helpers leaking into the shared kit** — mitigate by keeping suite-specific seeds file-local until a second suite needs them.
