# Software value

This tool analyzes a source code repository and saves data about it to the Insights database: the
estimated cost to develop the project, and statistics about the programming languages it uses.

It is a Go binary that wraps [scc](https://github.com/boyter/scc). The git integration worker
calls it as a subprocess.

## How it runs

`software_value_service.py` runs the binary at `/usr/local/bin/software-value` once per repository
processing run, on the first clone batch (`is_first_batch` in `crowdgit/worker/repository_worker.py`). That
batch is a depth-1 clone of the default branch, so scc measures the files at the branch head.

The worker picks up a repository again when its last processing is older than
`REPOSITORY_UPDATE_INTERVAL_HOURS` (default 24, in `crowdgit/settings.py`), so the numbers refresh
at that rate.

Before it calls the binary, the service:

- skips repositories listed in `_SOFTWARE_VALUE_EXCLUDED_REPO_IDS`;
- measures the clone with `du -sb` and, at 10 GB or more, passes `--no-large`, which makes scc skip
  files larger than 100 MB to avoid running out of memory.

The binary always exits with code 0 and prints a JSON status on stdout:

```json
{ "status": "failure", "error_code": "NO_COST_FOUND", "error_message": "..." }
```

The service records the status, error code and duration as a service execution with operation type
`SOFTWARE_VALUE`. Error codes are defined in `types.go`.

## What it computes

For the repository at the path given as the only argument, the binary:

1. Reads the `origin` remote with `git remote get-url origin` and rewrites `git@`, `ssh://git@`
   and `http://` URLs to `https://`. This URL is the key in both tables.
2. Runs `scc --format=short` and parses the COCOMO line "Estimated Cost to Develop" into a
   dollar amount. A cost of zero fails the run with `NO_COST_FOUND` and saves nothing.
3. Runs `scc --format=json` for per-language counts: bytes, code bytes, lines, code, comment and
   blank lines, complexity, file count and weighted complexity.
4. Upserts the cost into `project_costs` and one row per language into `language_stats`.

## Data model

The tables live in the Insights database, not the CDP database. `db.sql` defines them:

| Table            | Key                         | Holds                                   |
| ---------------- | --------------------------- | --------------------------------------- |
| `project_costs`  | `repo_url`                  | `estimated_cost` (whole dollars)        |
| `language_stats` | `(repo_url, language_name)` | scc's per-language counts for that repo |

`language_stats.repo_url` references `project_costs` with `ON DELETE CASCADE`.

## Where the numbers go

`project_costs` is replicated to Tinybird as the datasource
`services/libs/tinybird/datasources/softwareValueProjectCosts.datasource`. From there:

- `repositories_populated_copy.pipe` exposes the cost per repository as `softwareValue`;
- `segments_aggregates_with_ids_copy.pipe` sums the cost of a project's repositories into the
  project's `softwareValue`.

`language_stats` has no Tinybird datasource in this repository.

## Configuration

The binary reads the Insights database connection from environment variables:

| Variable                 | Use                             |
| ------------------------ | ------------------------------- |
| `INSIGHTS_DB_WRITE_HOST` | Host                            |
| `INSIGHTS_DB_PORT`       | Port                            |
| `INSIGHTS_DB_USERNAME`   | User                            |
| `INSIGHTS_DB_PASSWORD`   | Password                        |
| `INSIGHTS_DB_DATABASE`   | Database name                   |
| `INSIGHTS_DB_SSLMODE`    | `sslmode` for the connection    |
| `INSIGHTS_DB_POOL_MAX`   | Maximum connections in the pool |
| `INSIGHTS_DB_READONLY`   | `true` opens read-only sessions |

It finds `scc` on `PATH`.

## Build

`scripts/services/docker/Dockerfile.git_integration` builds the binary in a Go stage, installs scc
v3.5.0 with `go install`, and copies both to `/usr/local/bin` in the git integration image. The
parser depends on scc's output format, so test before changing the scc version.

## Running locally

Requirements: Go (the version in `go.mod`), PostgreSQL with the tables from `db.sql`, and scc
v3.5.0 on `PATH`.

```bash
go mod download
export INSIGHTS_DB_WRITE_HOST=localhost INSIGHTS_DB_PORT=5432 INSIGHTS_DB_USERNAME=... \
  INSIGHTS_DB_PASSWORD=... INSIGHTS_DB_DATABASE=... INSIGHTS_DB_SSLMODE=disable INSIGHTS_DB_POOL_MAX=4
go run ./ [--no-large] /path/to/cloned/repository
```

The repository must be a git clone with an `origin` remote.
