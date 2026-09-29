# ADR-0031: Frontend modernization decisions

**Date**: 2026-09-29
**Status**: accepted
**Deciders**: Gašper Grom and the frontend team

## Context

A 2026-09-22 audit of `frontend/` found that no type check has ever run in CI, that Vuex and Pinia coexist as two state-management systems, and that Element Plus and the LF ui-kit coexist as two component systems. The same audit counted 206 JavaScript files, zero tests, 15 dependencies stalled multiple majors behind current, and 52 real circular imports. Every new feature has to route around these forks instead of building on one system, and nothing catches a broken type or a reintroduced import cycle before it reaches `main`.

## Decision

Modernize the frontend incrementally on `main`, in small ratcheted PRs, rather than a rewrite or a long-lived branch:

- **State**: migrate Vuex stores to Pinia setup stores.
- **Components**: migrate Element Plus usage to the LF ui-kit, with no globally registered components going forward — each component imports what it uses.
- **Language**: TypeScript everywhere, using `<script setup lang="ts">` with `defineOptions` for component options; TypeScript 5.9 now, with TS 7 (the native compiler) evaluated in a separate spike rather than adopted here.
- **Tests**: Vitest as the test runner.
- **Styles**: Sass `@use` over `@import`.
- **Linting**: keep ESLint for the frontend, since oxlint has no Vue template rules, paired with a formatter that runs on staged files only.
- **Process**: one PR per Jira ticket, all targeting `main`, tracked with ratchets (type-check error count, import-cycle allowlist, legacy-file lists) shrunk over time instead of a big-bang rewrite.

## Alternatives Considered

### Alternative 1: Long-lived modernization branch

- **Pros**: Freedom to make sweeping changes without touching `main` on every step.
- **Cons**: Months of merge drift against a `main` that keeps shipping, with no incremental value until the branch lands.
- **Why not**: The team ships from `main` weekly; a long-lived branch would fall behind faster than it could be kept in sync.

### Alternative 2: Keep Vuex + Element Plus and only add type checking

- **Pros**: Less short-term churn, with no component or state migration to review.
- **Cons**: Two state-management systems and two component systems remain forever, and the audit already counts 507 `any` usages that type checking alone would not remove.
- **Why not**: Every new feature would keep paying the double-system tax indefinitely.

### Alternative 3: Adopt oxlint for the frontend too

- **Pros**: One linter across the whole repo, and oxlint runs roughly 100x faster than ESLint.
- **Cons**: oxlint has no `eslint-plugin-vue` template rules, which the ESLint ratchets that lint new `.vue` files and the stricter Vue template rules depend on.
- **Why not**: Template linting is the point of keeping ESLint on the frontend.

### Alternative 4: Rewrite the frontend on Nuxt

- **Pros**: A modern, batteries-included framework with SSR built in.
- **Cons**: The frontend has no SSR requirement, and a rewrite would discard roughly 60k lines of working, shipped UI.
- **Why not**: No SSR need, and 60k lines of working UI make a rewrite a worse trade than incremental modernization.

## Consequences

### Positive

- Type safety is enforced in CI instead of never having run.
- One state-management system (Pinia) and one component system (LF ui-kit) instead of two of each.
- A smaller bundle once Element Plus is fully replaced.
- Faster CI feedback from Vitest and a staged-files-only formatter.

### Negative

- Roughly 290 small PRs' worth of review load spread across the team.
- Temporary duplication while Vuex/Pinia and Element Plus/ui-kit coexist mid-migration.
- Legacy-file lists (ratchet allowlists) need to be maintained and shrunk over time.

### Risks

- Ratchet lists (type-check error count, cycle allowlist, legacy-file lists) can drift if not enforced; CI enforcement keeps them shrinking rather than growing.
- Storybook/pnpm peer-dependency issues are a known risk, which is why that work is sequenced last.
- TS 7's native compiler may change type-checking semantics; it is evaluated in a separate spike rather than adopted alongside this decision.
