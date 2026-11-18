---
name: packages-worker-setup
description: >
  Get packages_worker running locally — first time or resuming after a break.
  Spins up packages-db if not running, applies any pending migrations, and starts
  the worker. All steps are safe to re-run.
  Use when: "set up packages worker", "start packages worker", "resume packages worker",
  "get packages-db running", "packages-db stopped", "restart the worker".
allowed-tools: Read, Bash, Edit, AskUserQuestion
---

# packages-worker

Get `packages_worker` running locally. Every step can be re-run, but
Step 1's `scaffold up` does more than start containers (it rebuilds images and runs every local
migration), so when resuming use the lighter command in Step 1.

## Prerequisites check

```bash
docker info --format '{{.ServerVersion}}'
pnpm --version
```

## Step 1 — Start packages-db and Temporal

`./scripts/cli scaffold up` creates the `crowd-bridge` network and starts the local
infrastructure, including `packages` and `temporal`, under the `crowd` Compose project. It is not
a no-op on a running stack: it also builds missing images and runs every pending local migration
(CDP Postgres, product DB, `packages-db` and Tinybird) plus the Nango setup. Running
`docker compose -f scripts/scaffold.yaml` directly, without `-p crowd`, would start a second stack
under another project name.

When resuming on a machine where `scaffold up` has run before, start only what this worker needs
instead: `docker compose -p crowd -f scripts/scaffold.yaml up -d packages temporal db`. `db` is the
CDP database, which `bq-dataset-ingest` (Scorecard jobs) and `security-contacts-worker` read.

```bash
./scripts/cli scaffold up
until docker compose -p crowd -f scripts/scaffold.yaml exec packages pg_isready -U postgres; do sleep 1; done
echo "packages-db is ready"
```

## Step 2 — Apply pending migrations (only if needed)

`scaffold up` already applies the `packages-db` migrations, so skip this step after it succeeds.
Run it to apply a migration you added later without re-running `scaffold up`. Flyway skips
already-applied migrations, so it is safe to re-run.

```bash
arch=$(uname -m)
[ "$arch" = "arm64" ] && PLATFORM="--platform=linux/arm64/v8" || PLATFORM="--platform=linux/amd64"
docker build $PLATFORM -t packages_flyway \
  -f backend/src/osspckgs/Dockerfile.flyway backend/src/osspckgs --load

docker run --rm --network crowd-bridge \
  -e PGHOST=packages \
  -e PGPORT=5432 \
  -e PGUSER=postgres \
  -e PGPASSWORD=example \
  -e PGDATABASE=packages-db \
  packages_flyway
```

To create a new migration:

```bash
./scripts/cli scaffold create-packages-migration <descriptive_name>
```

## Step 3 — Start the worker

```bash
DEV=1 ./scripts/cli service <entry-point> up
```

`<entry-point>` is one of the files in `scripts/services/` for this worker, such as
`npm-worker` or `bq-dataset-ingest`; the full list is in `services/apps/packages_worker/README.md`.

Dev mode uses hot reload — edits to `services/apps/packages_worker/src/` and
`services/libs/*/src/` are picked up immediately without restarting.

`criticality-worker` has no service file; run it with `pnpm dev:criticality-worker:local`.

### Without Docker

Any entry point can also run directly with `pnpm dev:<entry-point>:local` from
`services/apps/packages_worker`. These scripts load `backend/.env.dist.local` and
`backend/.env.override.local` after your shell, so a `CROWD_TEMPORAL_NAMESPACE` set in the shell
is overwritten and the worker polls the namespace those files set.

### On-demand jobs from a local API

The API starts blast radius and member contributor jobs with
`CROWD_PACKAGES_TEMPORAL_SERVER_URL` and `CROWD_PACKAGES_TEMPORAL_NAMESPACE`, read from the backend
env files. The worker must poll the same Temporal instance and namespace, and where its namespace
comes from depends on how it was started:

- **In Docker** (`./scripts/cli service <entry-point> up`): the service file sets
  `CROWD_TEMPORAL_NAMESPACE` to `${CROWD_PACKAGES_TEMPORAL_NAMESPACE:-default}`. Compose fills
  that in from your shell when you run the command, not from the backend env files, so the worker
  polls `default` unless you export `CROWD_PACKAGES_TEMPORAL_NAMESPACE` first.
- **With `pnpm dev:<entry-point>:local`**: the worker uses `CROWD_TEMPORAL_NAMESPACE` from the
  backend env files.

Then point the API at that namespace and Temporal instance:

- set `CROWD_PACKAGES_TEMPORAL_NAMESPACE` in `backend/.env.override.local` to the namespace the
  worker polls;
- set `CROWD_PACKAGES_TEMPORAL_SERVER_URL` to the address the API can reach: `localhost:7233`
  when the API runs on the host, or `temporal:7233` in `backend/.env.override.composed` when it
  runs in Docker (the Docker API loads that file last).

Do not change the `CROWD_TEMPORAL_*` values in the env files: every local CDP service reads them.
`pnpm trigger-bootstrap:local` connects through `CROWD_TEMPORAL_*`, like a `pnpm` worker.

## Day-to-day commands

A worker started with `DEV=1` runs as the `<entry-point>-dev` container, so keep `DEV=1` on
the commands that act on it:

```bash
# Follow logs
DEV=1 ./scripts/cli service <entry-point> logs

# Stop (removes both the dev and non-dev containers)
./scripts/cli service <entry-point> down

# Restart
DEV=1 ./scripts/cli service <entry-point> restart

# Check status: `status` only looks at the non-dev container, so for a dev worker use
docker ps --filter name=<entry-point>-dev
```

## Going further

- Add a new sub-worker (npm-sync, osv-sync, etc.): `/packages-worker-add-entrypoint`
- Record an architecture decision: `/adr`
- Before opening a PR: `/preflight`
- Commit with DCO sign-off: `/commit`

## Troubleshooting

| Symptom                             | Likely cause       | Fix                         |
| ----------------------------------- | ------------------ | --------------------------- |
| `Connection refused` on packages-db | Docker not running | `./scripts/cli scaffold up` |
| `permission denied: scripts/cli`    | CLI not executable | `chmod +x scripts/cli`      |
