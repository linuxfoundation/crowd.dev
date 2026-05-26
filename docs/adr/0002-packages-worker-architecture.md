# ADR-0002: Single-service, multi-entry-point architecture for packages_worker

**Date**: 2026-05-25
**Status**: accepted
**Deciders**: CDP/Insights team

## Context

The packages domain requires several independent data-ingestion workers (GitHub repository enrichment, npm package sync, deps.dev dependency data, OSV advisories, OpenSSF Scorecard, and others). Each worker has distinct external API dependencies, rate-limit profiles, and scheduling needs. The platform already demonstrates this pattern in `backend/`, where `api.ts` and `job-generator.ts` are two separate processes built from the same Dockerfile and npm package.

## Decision

All packages_worker sub-workers live in a single npm package (`services/apps/packages_worker`) and are built from one Dockerfile (`scripts/services/docker/Dockerfile.packages-worker`). Each sub-worker is a self-contained directory under `services/apps/packages_worker/src/{worker}/` with its own logic, types, and database access. Each is launched as a separate container using a different entry point command, sharing the same image. Config helpers (`requireEnv`, `requireEnvInt`) and the packages-db connection are shared across all entry points.

```
services/apps/packages_worker/
  src/
    bin/
      packages-worker.ts      ← parent / health-check stub
      github-repos-enricher.ts
    github/                   ← github-repos-enricher logic
    npm/                      ← npm worker (future)
    deps-dev/                 ← deps.dev worker (future)
    osv/                      ← OSV worker (future)
    config.ts                 ← shared requireEnv / requireEnvInt
    db.ts                     ← shared packages-db connection
```

## Alternatives Considered

### Alternative 1: One npm package per worker (matching the pattern of other workers in `services/apps/`)

- **Pros**: Full isolation; each worker has its own `package.json`, Dockerfile, and deploy lifecycle.
- **Cons**: Most packages-domain workers share the same DB connection, config shape, and type definitions. Duplicating these across N packages creates maintenance overhead that grows with each new data source.
- **Why not**: The packages domain has a clear shared foundation (one DB, one config pattern, one set of domain types). A monorepo sub-package per worker is the right split when workers diverge significantly in dependencies or deploy cadence — that is not the case here.

### Alternative 2: One monolithic process running all workers

- **Pros**: Simpler deployment — one container.
- **Cons**: Workers have different rate-limit profiles and external API dependencies. A failure or resource spike in one worker affects all others. Independent scaling is impossible.
- **Why not**: Workers must be deployed and scaled independently. A single-process monolith would require internal concurrency management that replicates what separate processes give for free.

## Consequences

### Positive

- One Dockerfile and one build to maintain regardless of how many sub-workers are added.
- Shared `config.ts` enforces fail-fast env-var validation (`requireEnv`/`requireEnvInt`) across all workers — no silent `undefined`/`NaN` tuning values.
- Each worker can be deployed, scaled, and restarted independently.
- Adding a new data source means adding `src/{worker}/` and a new compose service entry — no new npm package, no new Dockerfile.

### Negative

- All workers in the service share the same npm dependency tree. A dependency needed by only one worker adds to the image size for all.
- A breaking change to shared code (`config.ts`, `db.ts`) affects all entry points simultaneously.
