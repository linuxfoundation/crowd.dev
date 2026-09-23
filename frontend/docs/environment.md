# Frontend environment variables

How `VUE_APP_*` variables get from a config file to a running page, across the three environments this app runs in.

## The three paths

### 1. Local dev

`pnpm run start:dev:local` runs:

```
set -a && . ./.env.dist.local && . ./.env.override.local && set +a && vite --mode localhost --port 8081
```

`.env.dist.local` (tracked) and `.env.override.local` (personal, gitignored, created by `scripts/cli`) are sourced as shell files with `set -a`, so every variable they define is exported into the process environment before Vite starts. This is plain shell sourcing, not Vite's own `.env.<mode>` loading — the file names here don't match that convention, so Vite itself reads no `.env.*` file in this path.

Only variables prefixed `VUE_APP_` are exposed to the app via `import.meta.env` (see `envPrefix` in `vite.config.js`). `src/config.js`'s `defaultConfig` reads them directly, e.g. `import.meta.env.VUE_APP_BACKEND_URL`.

### 2. CI / Docker build

`Dockerfile` runs `npm run build:production` with no `VUE_APP_*` variables set. `src/config.js` picks its export based on `defaultConfig.backendUrl`:

```js
const config = defaultConfig.backendUrl ? defaultConfig : composedConfig;
```

With no `VUE_APP_BACKEND_URL`, `defaultConfig.backendUrl` is `undefined`, so the build bundles `composedConfig` instead. `composedConfig` doesn't read `import.meta.env` — its values are literal `CROWD_VUE_APP_*` placeholder strings baked into the built JS. `index.html` uses the same placeholder convention directly for `VUE_APP_SEGMENT_KEY` (as `%VUE_APP_SEGMENT_KEY%`, which Vite's HTML transform leaves as the literal fallback `CROWD_VUE_APP_SEGMENT_KEY` when the env var isn't set at build time).

### 3. Container start

`scripts/docker-entrypoint.sh` runs before nginx starts. For each name in its `ENV_VARIABLES` array, it `sed`s the placeholder `CROWD_<KEY>` to that variable's actual value (read from the container's own environment) across `dist/assets/*.js` and `dist/index.html`. The container's environment comes from the `frontend-config` configmap, sourced from `crowd-kube/<env>/.frontend.env.enc` (encrypted; decrypt with `SECRET_ENCRYPTION_KEY` to inspect).

A key only reaches the running app in production if it's in **both** places: `ENV_VARIABLES` in `docker-entrypoint.sh` (so the placeholder gets substituted) and `composedConfig` in `src/config.js` or `index.html` (so there's a placeholder to substitute). Booleans are plain strings compared with `=== 'true'` (see `config.isGitIntegrationEnabled` etc.) — a placeholder left unsubstituted is never the string `'true'`, so it silently evaluates to `false`.

## Key reference

| Key | Read in | Substituted at container start | In `.env.dist.local` | Status |
|---|---|---|---|---|
| `VUE_APP_AUTH0_CLIENT_ID` | `config.js` | yes | no | active |
| `VUE_APP_AUTH0_DATABASE` | `config.js` | yes | no | active |
| `VUE_APP_AUTH0_DOMAIN` | `config.js` | yes | no | active |
| `VUE_APP_BACKEND_URL` | `config.js` | yes | yes | active |
| `VUE_APP_COMMUNITY_PREMIUM` | `config.js` | yes | yes | active, owned by N-01/N-02 |
| `VUE_APP_DATADOG_RUM_APPLICATION_ID` | `config.js` | yes | no | active |
| `VUE_APP_DATADOG_RUM_CLIENT_TOKEN` | `config.js` | yes | no | active |
| `VUE_APP_DISCORD_INSTALLATION_URL` | `config.js` | yes | yes | active |
| `VUE_APP_EDITION` | `config.js` | yes | yes | active, owned by N-01/N-02 |
| `VUE_APP_ENV` | `config.js` | yes | yes | active |
| `VUE_APP_FRONTEND_HOST` | `config.js` | yes | yes | active |
| `VUE_APP_FRONTEND_PROTOCOL` | `config.js` | yes | yes | active |
| `VUE_APP_GITHUB_INSTALLATION_URL` | `config.js` | yes | yes | active |
| `VUE_APP_HOTJAR_KEY` | `config.js` | yes | no | active, owned by N-01/N-02 |
| `VUE_APP_INTERCOM_APP_ID` | `config.js` | yes | yes | active |
| `VUE_APP_IS_CONFLUENCE_ENABLED` | `config.js` | no | no | local-only; always `false` in prod; N-05 (CM-1643) decides honour-or-delete |
| `VUE_APP_IS_GERRIT_ENABLED` | `config.js` | no | no | local-only; always `false` in prod; N-05 (CM-1643) decides honour-or-delete |
| `VUE_APP_IS_GIT_ENABLED` | `config.js` | yes | no | active |
| `VUE_APP_IS_GROUPSIO_ENABLED` | `config.js` | yes | no | active |
| `VUE_APP_IS_TWITTER_ENABLED` | `config.js` | yes | no | active |
| `VUE_APP_LF_TENANT_ID` | `config.js` | yes | no | active |
| `VUE_APP_NANGO_URL` | `config.js` | yes | yes | active |
| `VUE_APP_SEGMENT_KEY` | `index.html` | yes | no | active; moves into `config.js` after A-05 |
| `VUE_APP_STRIPE_CUSTOMER_PORTAL_LINK` | `config.js` | yes | no | active, owned by N-01/N-02 |
| `VUE_APP_STRIPE_GROWTH_PLAN_PAYMENT_LINK` | `config.js` | yes | no | active, owned by N-01/N-02 |
| `VUE_APP_STRIPE_PUBLISHABLE_KEY` | `config.js` | yes | no | active, owned by N-01/N-02 |
| `VUE_APP_TEAM_USER_IDS` | `config.js` | yes | no | active |
| `VUE_APP_TYPEFORM_ID` | `config.js` (`composedConfig` only) | no | no | local-only; scheduled for removal by N-03 (CM-1641) |
| `VUE_APP_TYPEFORM_TITLE` | `config.js` (`composedConfig` only) | no | no | local-only; scheduled for removal by N-03 (CM-1641) |
| `VUE_APP_WEBSOCKETS_URL` | `config.js` | yes | yes | active |

"owned by N-01/N-02" keys stay in the entrypoint and `composedConfig` until those tickets land — not touched here.

## Adding a new key

Say the new key is `VUE_APP_FOO`. Four places need it:

1. `src/config.js` — a `defaultConfig` entry reading `import.meta.env.VUE_APP_FOO`, and a matching `composedConfig` entry set to the placeholder string `'CROWD_VUE_APP_FOO'`.

   ```js
   // defaultConfig
   foo: import.meta.env.VUE_APP_FOO,
   // composedConfig
   foo: 'CROWD_VUE_APP_FOO',
   ```

2. `scripts/docker-entrypoint.sh` — add `"VUE_APP_FOO"` to the `ENV_VARIABLES` array.

3. `.env.dist.local` — add `VUE_APP_FOO=<local value>` so local dev has something to read.

4. The relevant `crowd-kube/<env>/.frontend.env.enc` — add `VUE_APP_FOO=<env value>` so the container has something to substitute in production/staging.

Skip step 2 if the key is only ever read locally (never needs a production value) — but then it must stay documented above as local-only, since a `config.js`/`index.html` consumer with no entrypoint entry never resolves in production.
