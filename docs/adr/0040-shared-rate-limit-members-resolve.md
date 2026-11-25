# ADR-0040: Shared Redis store for the `POST /members/resolve` rate limit

**Date**: 2026-11-23
**Status**: accepted
**Deciders**: Yeganathan S

## Context

ADR-0018 gave `POST /v1/members/resolve` a per-client limit of 200 requests a minute, counted in each API pod's memory, and deferred a Redis store until the limit was seen being exceeded. It was exceeded on Oct 29 2026: during a burst one client got 426 successful lookups in one minute, while 20 of its requests were rejected by whichever pods had reached 200 first. The 2,248 rate-limited lookups of Aug 13 came from the old IP-keyed global limiter, an hour before ADR-0018 shipped. The endpoint is now called by the Auth0 `cdp_uuid` action and by other LFX services, each with its own client id (linuxfoundation/lfx-cdp-insights-ops#491).

## Decision

Count the resolve limit in Redis with `rate-limit-redis`, so each client gets one 200 requests a minute budget shared by every API pod. The limiter keeps its key (`req.actor.id`), window and headers, and uses the backend's existing Redis client under the `rate-limit:members-resolve:` key prefix. This supersedes ADR-0018.

## Alternatives Considered

### Alternative 1: Keep the in-memory store

- **Pros**: no Redis call per request.
- **Cons**: the real limit is 200 × the number of pods, changes with scaling, and rejects requests unevenly depending on which pod they land on.
- **Why not**: it no longer meets the requirement that a client's limit is the same regardless of instance count.

### Alternative 2: Let requests through when Redis errors

- **Pros**: a Redis failure would not reject lookups.
- **Cons**: needs a store wrapper or an `express-rate-limit` v7 upgrade (`passOnStoreError`).
- **Why not**: the backend creates its Redis client with `exitOnError`, so a lost connection already restarts the pod; there is no state in which this endpoint runs without Redis.

## Consequences

### Positive

- One limit per client, independent of how many pods are running.
- Callers see consistent `RateLimit-*` and `Retry-After` headers across pods.

### Negative

- One Redis `EVALSHA` per resolve request, about 11 a minute on average and a few hundred at peak.
- A burst that the per-pod counters used to absorb is now rejected after 200 requests in the window.

### Risks

- Callers that relied on the higher effective limit will see more 429s. The Auth0 action backs off on `Retry-After` and never blocks sign-in; tell the owners of the other callers before rollout.
