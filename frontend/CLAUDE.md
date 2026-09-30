# frontend/CLAUDE.md

Conventions specific to `frontend/` (the CDP Vue UI). The root `AGENTS.md`
still applies here, including its "no comments" rule, and commits follow
`CONTRIBUTING.md`.

## Components

- `<script setup lang="ts">` only. No Options API.
- Use `defineOptions({ name: 'ComponentName' })` for the component name instead
  of a second, non-setup `<script>` block.
- Typed `defineProps` / `defineEmits` — no untyped prop objects.
- No globally registered components going forward: each component explicitly
  imports what it uses.
- Prefer `Lf*` ui-kit components over Element Plus and raw HTML primitives.
  Element Plus (`el-*`) is acceptable only where no ui-kit equivalent exists
  yet.
- Every new ui-kit component ships a Storybook story.

## Data

- No raw HTTP calls in components. Server state goes through TanStack Vue Query,
  whose query functions call `*.api.service.ts` modules instead of `authAxios`
  directly.
- Pinia setup stores for shared client state.
- Vuex is legacy — do not add new stores or actions to it.

## Types

- No `any`. Use proper types, `unknown` with narrowing, or generics.
- Extend and reuse existing types instead of redefining them.
- Use `import type` for type-only imports.

## Styling

- Tailwind tokens; `gap-*` for stacking, not `space-y-*`.
- No hard-coded hex colors in templates.
- Scoped styles; Sass `@use` over `@import`.

## Checks

- `npm run lint` — must pass with 0 warnings.
- `npm run build:localhost` — must succeed.
- The legacy-file list (`.eslint/legacy-files.json`) only shrinks, never
  grows.

## Ownership

- Decisions and rationale: [ADR-0031](../docs/adr/0031-frontend-modernization.md).
- Local dev environment setup: [`frontend/README.md`](README.md).
