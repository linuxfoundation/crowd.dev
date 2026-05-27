# ADR-0004: Standalone bin vs Temporal for batch sub-workers in packages_worker

**Date**: 2026-05-27
**Status**: accepted
**Deciders**: CDP/Insights team

## Context

ADR-0002 codifies that `packages_worker` is a single npm package hosting many sub-workers, each with its own `src/bin/<worker>.ts` entry point. What ADR-0002 does **not** answer is which *execution shape* each sub-worker should adopt: a Temporal worker registered against the `packages-worker` task queue (the original `bin/packages-worker.ts` pattern) or a standalone bin running an internal control loop (the `bin/github-repos-enricher.ts` pattern, repeated in this slice for `bin/osv-sync.ts`).

The sub-workers we have, or expect to add in the coming weeks, fall into two clusters by workload shape:

- **Small, periodic batches.** OSV (two daily zips, ~226k records total), OpenSSF Scorecard runs (per-repo, idempotent), deps.dev metadata refresh for a fixed set of packages.
- **High-volume, incremental, with retries.** npm package sync (~3M packages, a continuous `_changes` stream, per-package retries, per-package activity backfill).

These differ on every axis Temporal's value props address: parallel workflow fan-out, retries with persistent state, signal-driven coordination, cross-process visibility. The cost of those props (Temporal namespace + task queue + activity registration + worker lifecycle) is the same regardless of whether they're used.

## Decision

Sub-workers whose work is a small periodic batch use the standalone-bin pattern (an entry point that opens its own DB connection, runs a control loop with an `isShuttingDown()` check and an `OSV_SYNC_INTERVAL_HOURS`-style sleep, and exits on SIGINT/SIGTERM). `bin/github-repos-enricher.ts` and `bin/osv-sync.ts` are the reference implementations.

Sub-workers whose work is high-volume, incremental, or needs cross-process retry/signal coordination use Temporal, registering against the existing `packages-worker` task queue. The forthcoming npm-package-sync worker is expected to take this shape.

The decision is per sub-worker. There is no global default — picking the right shape upfront matters, but switching shapes later for one sub-worker is local (it's just a different `bin/<worker>.ts`).

## Alternatives Considered

### Alternative 1: Use Temporal for every sub-worker

- **Pros**: One execution model to reason about across the service. Retry, scheduling, and observability surface uniformly.
- **Cons**: Temporal adds a namespace, task queue, activity/workflow registration, and a workflow-versioning discipline per sub-worker. For a daily 6-second Maven download and 2.5-minute npm download with idempotent UPSERTs, the operational burden — and the cognitive overhead for anyone reading the code — buys nothing.
- **Why not**: The retry/coordination features Temporal exists for are unused on small batches. The empty Temporal shape becomes scaffolding-as-documentation rather than load-bearing infrastructure.

### Alternative 2: Use plain cron (e.g. Kubernetes CronJob) for batch work, no service at all

- **Pros**: Even less infrastructure than a long-running container. Scheduling is owned by the cluster.
- **Cons**: Loses the in-process derivation step (`deriveCriticalFlag` must run after the ingest pass in the same process; a cron pattern would force it into a separate job with its own scheduling). Also fights the existing pattern: `packages_worker` is already a long-running service per ADR-0002, and adding a parallel CronJob deployment shape per sub-worker doubles the deploy surface.
- **Why not**: The integration with the rest of `packages_worker` (shared `config.ts`, shared `db.ts`, shared Dockerfile) is the value of being a sub-worker. A cron job would have none of that.

### Alternative 3: One Temporal workflow per ecosystem instead of a single bin loop

- **Pros**: Per-ecosystem retry policies and cancellation.
- **Cons**: The retry policy we need (exponential backoff on download failure, give up after N) is ~15 lines of TypeScript. Cancellation we get from the SIGINT handler. There's no cross-process coordination needed.
- **Why not**: We'd be paying Temporal's complexity tax to replicate behavior the standalone bin already does correctly.

## Consequences

### Positive

- Each sub-worker uses the simplest shape that does the job. Reviewers can read `osv-sync.ts` start-to-finish without holding Temporal concepts in their head.
- `packages_worker` keeps one Dockerfile and one image; each sub-worker is just a different entry-point command, regardless of which execution shape it uses.
- Switching a sub-worker from standalone to Temporal (or back) when its scale changes is a local change to one `bin/<worker>.ts` — no cross-cutting refactor.

### Negative

- Two execution shapes coexist in the same service. Anyone debugging "what is `packages_worker` doing right now?" has to know which sub-worker uses which shape. Mitigation: the `bin/<worker>.ts` filename and `start:<worker>` script in `package.json` are the canonical source of truth; the shape is visible in 20 lines of code.
- The standalone-bin pattern repeats some plumbing across workers (DB connection, signal handlers, logger setup). That's ~30 lines of boilerplate per worker; small enough that DRYing it up isn't worth the abstraction yet. Revisit if a fourth standalone bin lands and the duplication becomes annoying.

### Risks

- A sub-worker is picked for the wrong shape and outgrows it. Concretely: a standalone bin that grows into needing per-record retries with persistent state, or signal-driven coordination across processes, will hit the limits of its `while (!isShuttingDown())` loop. Mitigation: the rewrite to Temporal is bounded to a single `bin/<worker>.ts` plus a small set of activities; no other sub-worker is affected.
- The "what counts as small batch?" line is judgment. Reasonable disagreement is possible (e.g. Scorecard is a small batch per repo but 27k repos make it large in aggregate). Mitigation: the per-sub-worker scope of this decision means the team can debate it once per new entry point, not as a one-time global commitment.
