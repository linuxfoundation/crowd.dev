---
type: runbook
slug: runbook-performance-profiling
name: Profiling CDP services
repo: crowd.dev
owner: CDP team
depends_on: [sys-cdp]
used_by: []
sources:
  - services/libs/database/src/connection.ts
  - services/libs/logging/src/utility.ts
  - services/libs/logging/src/logger.ts
  - services/libs/common/src/env.ts
  - services/apps/data_sink_worker/src/queue/index.ts
  - services/apps/data_sink_worker/src/service/
  - services/libs/opensearch/src/service/member.sync.service.ts
  - services/libs/data-access-layer/src/organizations/base.ts
  - scripts/services/
  - scripts/cli
---

# Profiling CDP services

CDP services have two profiling switches, both turned on with environment variables and both
written to the service log:

- **Query profiling** times every Postgres query.
- **Operation timing** times the blocks of code wrapped in `logExecutionTimeV2`.

Both are off by default. Turn them on for one service at a time, and turn them off when you are
done: query profiling logs SQL parameter values, which can include personal data.

## Query profiling

`getDbConnection` in `services/libs/database/src/connection.ts` wraps every query on the
connections it creates. Any service that opens Postgres through `@crowd/database` gets it.

| Variable                                        | Effect                                                        |
| ----------------------------------------------- | ------------------------------------------------------------- |
| `CROWD_POSTGRESQL_PROFILE_QUERIES`              | Turns query profiling on when set, whatever its value         |
| `CROWD_POSTGRESQL_PROFILE_QUERIES_MIN_DURATION` | Logs only queries that take at least this many ms (default 0) |

Each logged query is a `warn` line with the message `PostgreSQL query duration profiling!` and
these fields:

- `durationSeconds`: the query time, to two decimals
- `query`: the SQL text
- `values`: the parameter values

Set a threshold such as 100 or 500 ms. Without one, every query is logged.

## Operation timing

`logExecutionTimeV2` in `services/libs/logging/src/utility.ts` runs a function and logs how long
it took. When timing is off, it calls the function and adds nothing.

| Variable                                | Effect                                                           |
| --------------------------------------- | ---------------------------------------------------------------- |
| `CROWD_LOG_EXECUTION_TIME`              | Turns operation timing on when set to a non-empty value          |
| `CROWD_LOG_EXECUTION_TIME_MIN_DURATION` | Logs only operations that take at least this many ms (default 0) |
| `CROWD_LOG_EXECUTION_START`             | Also logs a line when each operation starts                      |

Each operation logs `info` lines:

- `Starting process <name>...`, only with `CROWD_LOG_EXECUTION_START`
- `Process <name> took <seconds> seconds!` on success
- `Process <name> failed after <seconds> seconds!` on error

`<name>` follows the pattern `service -> method -> call`, for example
`activityService -> processActivities -> insertActivities`, so it points at the code that ran.
The wrapped operations are in:

- `data_sink_worker`: queue handling, result processing, member and activity services
- `services/libs/opensearch`: member sync
- `services/libs/data-access-layer`: organization queries in `organizations/base.ts`

To time other code, wrap it in `logExecutionTimeV2` with a name in the same pattern.

## Where the output goes

Both switches write through the service logger in `services/libs/logging/src/logger.ts`, a
Bunyan logger that writes to stdout. In local development and tests the lines are short and
human-readable; everywhere else they are JSON, one object per line, with the service name in
`name`.

`LOG_LEVEL` sets the lowest level written and defaults to `info`. Query profiling needs `warn` or
lower, and operation timing needs `info` or lower.

## Running it locally

1. Add the variables to `backend/.env.override.local` (and `backend/.env.override.composed` for
   services run in Docker). The service definitions in `scripts/services/` load both files, and
   git ignores them.
2. Restart the service from `scripts/`: `./cli service <name> restart`.
3. Follow its log: `./cli service <name> logs`.

On a deployed service, set the same variables on that service's deployment and restart it.

## Reading the output

- **One slow query**: sort the query lines by `durationSeconds` and run the slowest `query` with
  its `values` under `EXPLAIN ANALYZE`.
- **A slow step in a worker**: compare `took` times across operation names. A slow outer
  operation with fast inner ones points at code between the wrapped calls.
- **Both together**: turn on query profiling with a threshold alongside operation timing, with
  `CROWD_LOG_EXECUTION_START` set. The start and end lines bracket the slow queries in time. The
  lines share no request ID, so when a service handles several messages at once, match them by
  timestamp with care.
