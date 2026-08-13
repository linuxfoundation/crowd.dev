# ADR-0018: Per-client rate limiting for `POST /members/resolve` using in-memory store

**Date**: 2026-08-12
**Status**: accepted
**Deciders**: Joana Maia

## Context

`POST /v1/members/resolve` is called by the Auth0 `cdp_uuid` Action on every login to resolve a CDP member UUID. The existing global rate limiter (200 req/min, IP-keyed) collapses to a single bucket behind the ingress, making it impossible to give each M2M consumer an isolated budget. Auth0 scales workers horizontally; aggregate volume can exceed what any single instance tracks via its own pacer (`PACER_WINDOW_LIMIT = 60/min`). The endpoint required per-client isolation before the Auth0 action ships (linuxfoundation/workspace-segments#15).

## Decision

Add a dedicated `express-rate-limit` middleware on `POST /v1/members/resolve` keyed by `req.actor.id` (the M2M client `sub` from the verified JWT, populated by `oauth2Middleware`). Limit: 200 req/min per client. The global IP-keyed limiter is configured to skip this exact route via an exact method + path check. The store is the default in-memory `MemoryStore`; a Redis-backed distributed store is deferred.

## Alternatives Considered

### Alternative 1: Redis-backed distributed store (`rate-limit-redis`)
- **Pros**: Accurate counter across all k8s replicas; true 200 req/min ceiling regardless of HPA scale-out.
- **Cons**: New npm dependency; Redis connection at rate-limit middleware layer; added operational surface.
- **Why not**: The Auth0 Action has its own per-instance pacer. Overshoot is bounded by replica count, which is stable under normal HPA conditions. Deferred until the endpoint is live and replica behaviour is observable under real load.

### Alternative 2: Raise global IP limit, rely on per-instance pacer
- **Pros**: No new middleware.
- **Cons**: No isolation between M2M consumers; any single client can exhaust the shared ceiling.
- **Why not**: Does not satisfy the per-client isolation requirement in workspace-segments#15.

## Consequences

### Positive
- Each M2M consumer gets an isolated 200 req/min budget keyed on a stable identity that survives credential rotation.
- `Retry-After` and `RateLimit-*` headers are emitted on 429, giving callers actionable backoff signals.
- Global IP limiter remains unchanged for all other endpoints.

### Negative
- Effective per-client limit is `200 × replica_count` under HPA scale-out — not a hard ceiling.

### Risks
- If replica count grows significantly before a Redis store is added, the soft ceiling may be exceeded. Monitor replica count alongside the Datadog resolve-rate dashboard; add `rate-limit-redis` if ceiling violations are observed.
