# ADR-0008: How we write unit tests

**Date**: 2026-07-13
**Status**: accepted
**Deciders**: Yeganathan S

## Context

Unit tests should stay easy to read and cheap to change. Without a shared style,
suites either bury the case in setup and weak assertions, or grow a private
layer of helpers that obscures what is under test.

We need a small set of principles for designing unit tests: what a case is,
what it asserts, and what may be shared.

## Decision

Treat each unit test as a **named scenario**. Compose data at the call site,
assert the outcome that proves the behavior, and extract helpers only when
reuse is real.

### Scenarios

- Name the real situation under test — one decision or path per case.
- Setup should make that story obvious (concrete labels, dates, relationships).
- Do not combine unrelated behaviors in a single test.
- Prefer fewer tests that fail clearly over many that pass for the wrong reason.

### Assertions

- Assert the result the unit owns (shape, exact values, exact counts).
- Prefer exact expectations when the outcome is deterministic.
- Avoid loose probes (`some`, `greaterThan`, optional finds) unless the claim
  truly is approximate.

### What to share

| Layer | Where | Belongs |
| --- | --- | --- |
| Scenario | Inside the test | Data that defines *this* case |
| File-local | Top of the suite file | Constants or tiny helpers used more than once in this file |
| Shared kit | `@crowd/test-kit` | Helpers needed by a second suite |

**Rules of thumb**

- Start inline. Extract to file-local only after real repetition.
- Promote to the shared kit only when another suite needs the same helper —
  and keep it sharp (create / fill), not a full scenario builder.
- Shared fixtures at the top of a file should be stable shapes, not multipurpose
  objects that different tests reinterpret by index.
- A few duplicated lines that tell the story beat a shared fixture that hides it.
- Reuse production types and enums. Do not invent test-only mirrors.
- Name helpers after what they do, not after a vague dump of domain data.
- A short list of clear file-local helpers is fine; wrappers that only pass data
  through are not.

## Alternatives Considered

### Alternative 1: Maximize reuse via shared scenario builders

- **Pros**: Short call sites.
- **Cons**: Hides what each test owns; failures are harder to diagnose.
- **Why not**: Readability of the scenario matters more than shorter setup.

### Alternative 2: No shared guidance — each suite invents its own style

- **Pros**: Maximum local freedom.
- **Cons**: Inconsistent quality; hard to review or extend.
- **Why not**: A thin shared philosophy scales better than ad-hoc patterns.

## Consequences

### Positive

- Tests read as stories; failures point at a clear decision.
- Sharing stays deliberate.

### Negative

- Some tests look more verbose than a “build everything” helper.

### Risks

- File-local helpers slowly become a private framework.
  **Mitigation:** extract only on repetition; prefer deleting a wrapper over
  stacking another.
