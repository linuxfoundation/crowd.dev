# CDP Frontend

The web UI for the LFX Community Data Platform (CDP). Vue 3 + Vite + TypeScript, using Pinia for
client state, TanStack Vue Query for server state, Tailwind for utility styling, and Auth0 for
authentication. Element Plus is the current component library; it's being replaced incrementally
by the LF `ui-kit` (`src/ui-kit/`) as part of the frontend modernization epic (see
[Modernization plan](#modernization-plan)).

This app is a thin client over the CDP backend (`../backend/`) — it does not talk to the database
directly. In the browser it does call a few third parties itself: Auth0 for login, plus Segment,
Datadog RUM and Hotjar for analytics and telemetry when their keys are configured.

## Prerequisites

- **Node 24** — the repo root `.nvmrc` pins this. Run `nvm use` from the repo root before
  installing.
- **npm** — the frontend is intentionally outside the root pnpm workspace and uses its own
  `package-lock.json`. Don't run `pnpm install` in this directory.
- **Docker** and `docker-compose` — the backend and its infra (Postgres, Redis, etc.) run via
  `../scripts/cli`, which needs Docker.

## Setup

1. From the repo root, get on Node 24 and install:

   ```shell
   nvm use
   cd frontend
   npm ci
   ```

2. Env files: `.env.dist.local` is tracked and holds the shared local defaults (backend URL,
   websocket URL, Auth0-adjacent flags, etc.). `.env.override.local` is your personal, gitignored
   override file — it's created empty for you by `scripts/cli` the first time you run it, and
   `start:dev:local` sources both files in order (override last, so it wins).

   See [`docs/environment.md`](docs/environment.md) for the full list of `VUE_APP_*` keys, where
   each is read, and which are substituted at container start.

3. Start the backend and its infra from the repo root:

   ```shell
   cd scripts
   ./cli start-be
   ```

   Use `start-be`, not `start`: `start` also launches a `frontend` container that binds port 8081,
   which would clash with the dev server in the next step. `start-be` also creates
   `frontend/.env.override.local` if it doesn't exist yet.

4. Back in `frontend/`, start the dev server:

   ```shell
   npm run start:dev:local
   ```

   This runs Vite on port 8081 and proxies `/api` requests to `BACKEND_URL` (defaults to
   `http://localhost:8080`, which is where `scripts/cli start-be` runs the backend).

5. Open http://localhost:8081.

If you only need the frontend build tools (lint, build) without running the app, `npm ci` is
enough — you don't need the backend running for those.

## Scripts

All scripts run from `frontend/` via `npm run <script>`.

| Script | What it does |
|---|---|
| `lint` | ESLint over `src/**/*.{js,ts,vue}`, fails on any warning (`--max-warnings=0`) |
| `lint:fix` | Same as `lint`, with `--fix` |
| `typecheck` | `vue-tsc --noEmit -p tsconfig.json`. Exits non-zero while existing type errors remain; CI does not run it yet |
| `lint:cycles` | Fails on any cycle madge reports that is not listed in `cycles-allowlist.json`, and on any listed cycle madge no longer reports |
| `lint:cycles:update` | Regenerates `cycles-allowlist.json`; run it after breaking a cycle and commit the result |
| `format` | Prettier `--write` over the frontend (`.vue` files are ignored by Prettier) |
| `format:check` | Prettier `--check`, without writing |
| `build` | `vite build` (Vite's default `production` mode); the Dockerfile and CI use `build:production` |
| `start` | `vite --host` — Vite dev server with defaults, no env sourcing |
| `start:dev` | Alias for `start` |
| `start:dev:local` | Sources `.env.dist.local` + `.env.override.local`, then runs Vite on port 8081 in `localhost` mode — the normal way to run the app locally |
| `preview` | `vite preview` — serves the built `dist/` locally; run `build` first. Without `VUE_APP_*` set at build time the bundle keeps `CROWD_VUE_APP_*` placeholders (see `docs/environment.md`) |
| `build:localhost` | `vite build --mode localhost` |
| `build:production` | `vite build --mode prod` |
| `build:staging` | `vite build --mode staging` |
| `analyze` | `ANALYZE=1 vite build --mode localhost` — also writes the bundle treemap to `analyse.html` (gitignored) |
| `docs:tailwind` | Opens the Tailwind config viewer |
| `docs:storybook` | Runs Storybook dev server on port 6006 |
| `docs:storybook:build` | Builds the static Storybook site |
| `docs:storybook:ci` | Same as `docs:storybook:build`, with `--quiet` for less log output |
| `docs` | Runs `docs:tailwind` and `docs:storybook` together |

## Checks before you push

- `npm run lint` — must pass with 0 warnings.
- `npm run lint:cycles` — must pass; break a new circular import instead of adding it to
  `cycles-allowlist.json`. madge lists cycles from a single depth-first walk, so it does not
  enumerate every cycle: a new import between modules that are already in cycles can slip through,
  and removing an import can re-route other entries. Run `npm run lint:cycles:update` and review
  the diff.
- `npm run build:localhost` (or `build:staging`/`build:production`) — must succeed.

CI (`.github/workflows/frontend-checks.yml`) runs `npm run lint`, `npm run lint:cycles`, a
production build and a Storybook build on pull requests that touch `frontend/**`. It does not yet
run a type check.

The root pre-commit hook (`.husky/pre-commit`) runs `npx lint-staged` inside `frontend/` whenever
a staged file matches `frontend/.+\.(js|ts|vue|scss|html|css|json|md|yml|yaml)$`. `lint-staged`
(configured in `package.json`) runs Prettier and then `eslint --fix` on staged `.js`/`.ts` files,
`eslint --fix` on `.vue` files, and Prettier on `.scss`/`.css`/`.json`/`.md`/`.yml`/`.yaml`
files. The hook is installed by
the root `pnpm install` (via `husky`). Run `npm ci` in `frontend/` before committing frontend
changes so `lint-staged` can run; a commit with no matching frontend files skips that step and
doesn't need `frontend/node_modules`.

## Architecture map

```
src/
  main.ts              entry point
  app.vue              root component
  router/              vue-router setup
  modules/<feature>/   one folder per feature area (member, organization, integration, ...):
                          routes, a Pinia or Vuex store, pages, components, and an
                          *.api.service.ts for backend calls
  shared/               cross-feature code: axios instance (auth-axios.js), generic model,
                          form/field helpers, dialogs, layout components, filters
  ui-kit/               the LF ui-kit — Lf-prefixed components (LfButton, LfCard, ...)
  config/               integrations/ and identities/ config, plus permissions and links
  config.js             environment-derived app config (reads VUE_APP_* env vars)
  assets/               images and scss
config/styles/          shared SCSS: tokens, variables, component-level styles
.storybook/              Storybook config
```

Feature modules under `src/modules/` are the unit of ownership: a change to one feature should
stay inside its module plus whatever it needs from `shared/` or `ui-kit/`.

## State and data

- **Client state** — Pinia, via `defineStore` setup stores per feature (e.g.
  `modules/organization/store`). New state should be Pinia, not Vuex.
- **Server state** — TanStack Vue Query wraps calls to `*.api.service.ts` modules, which in turn
  use the shared `authAxios` instance (`src/shared/axios/auth-axios.js`).
- **Vuex** (`src/store/`) is legacy and being phased out as part of CM-1482 in this epic. Don't
  add new Vuex modules; migrate a module to Pinia when you're touching it anyway.

## How to add an integration

1. Add `src/config/integrations/<key>/config.ts` and a `components/` folder for any
   connect/settings UI the integration needs.
2. Register the new config in `src/config/integrations/index.ts`.
3. If the integration also needs identity handling, add
   `src/config/identities/<key>/config.ts`.
4. Add the integration's logo under `src/assets/images/integrations/`.

Look at an existing integration (e.g. `github-nango` or `gitlab`) for the shape of `config.ts` and
`IntegrationConfig`.

## How to add a ui-kit component

See [`docs/UI-Kit-Structure.md`](docs/UI-Kit-Structure.md) for where a component lives and how
it's exported, and [`docs/Storybook-Guide.md`](docs/Storybook-Guide.md) for the story format. A
story is mandatory for every new ui-kit component — it's how the component gets reviewed and how
other engineers discover it.

## Storybook

```shell
npm run docs:storybook
```

Opens on http://localhost:6006. Stories live next to the component they document
(`*.stories.ts`).

## Conventions

- The Element Plus → ui-kit / structural decisions behind this modernization:
  [ADR-0031](../docs/adr/0031-frontend-modernization.md) (note that `docs/adr/0030-*.md` in this
  repo is an unrelated, earlier ADR about the docs-readiness worker, not the frontend — this one is
  0031).
- Commit format: `type: description (CM-XXX)`, signed with `--signoff -S`. See
  [Commit Message Guidelines](../CONTRIBUTING.md#commit-message-guidelines) for the full convention.
- One ticket per PR — don't bundle unrelated frontend changes into the same PR.

## Modernization plan

This README, and the frontend build/CI/tooling work generally, are part of a larger frontend
modernization effort tracked in Jira as 10 epics (CM-1480 through CM-1489, label
`frontend-modernization`). CM-1480 itself covers build, CI, and tooling.
