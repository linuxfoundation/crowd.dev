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

Get `packages_worker` running locally. All steps are idempotent — safe to run
whether this is your first time or you're resuming after a break.

## Prerequisites check

```bash
git branch --show-current        # should be feat/track-packages
docker info --format '{{.ServerVersion}}'
pnpm --version
```

If the branch is wrong: `git checkout feat/track-packages && pnpm i`.

## Step 1 — Start packages-db

No-op if already running.

```bash
docker compose -f scripts/scaffold.yaml up -d packages
until docker compose -f scripts/scaffold.yaml exec packages pg_isready -U postgres; do sleep 1; done
echo "packages-db is ready"
```

## Step 2 — Apply pending migrations

Flyway skips already-applied migrations, so this is safe to re-run.

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
DEV=1 ./scripts/cli service packages-worker up
```

Dev mode uses hot reload — edits to `services/apps/packages_worker/src/` and
`services/libs/*/src/` are picked up immediately without restarting.

## Day-to-day commands

```bash
# Follow logs
./scripts/cli service packages-worker logs

# Stop
./scripts/cli service packages-worker down

# Restart
./scripts/cli service packages-worker restart

# Check status
./scripts/cli service packages-worker status
```

## Going further

- Add a new sub-worker (npm-sync, osv-sync, etc.): `/packages-worker-add-entrypoint`
- Record an architecture decision: `/adr`
- Before opening a PR: `/preflight`
- Commit with DCO sign-off: `/commit`

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Connection refused` on packages-db | Docker not running | `docker compose -f scripts/scaffold.yaml up -d packages` |
| `permission denied: scripts/cli` | CLI not executable | `chmod +x scripts/cli` |
