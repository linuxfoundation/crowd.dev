# ADR-0019: Match builder and runner libc; Debian for Temporal TypeScript workers

**Date**: 2026-08-27
**Status**: accepted
**Deciders**: Yeganathan S

## Context

Our Node.js services use multi-stage Docker builds. Dependencies are installed in the builder stage and `node_modules` is copied into the runner stage.

Some Node.js dependencies contain native binaries (`.node` files). These binaries are built against a specific libc:

* `node:<major>-alpine` uses **musl**
* `node:<major>-bookworm-slim` uses **glibc**

Because the runner uses the `node_modules` produced by the builder, both stages must use the same libc. Mixing Alpine and Debian can result in native modules failing at runtime — the image builds, then the process dies on boot (`Bindings not found`, missing `ld-linux-x86-64.so.2`, and similar).

This became apparent after upgrading to pnpm 11. Older pnpm often installed optional natives for more than one libc, so a mixed Alpine builder and Debian runner could still start. pnpm 11 only installs natives for the OS it is running on, so the mismatch is no longer hidden. The upgrade exposed an existing Docker issue; it did not create the libc incompatibility.

Temporal TypeScript workers have an additional requirement. `@temporalio/core-bridge` ships a glibc Linux binary only. Temporal does not support Alpine / musl for TypeScript workers.

## Decision

**Builder and runner must always use the same Node.js image family.**

* **Temporal TypeScript workers:** use `node:<major>-bookworm-slim` for both builder and runner.
* **Other services:** Alpine is fine if all dependencies support musl. Use Alpine for both builder and runner.
* **Any other glibc-only native dependency:** use Debian for both stages.

Never mix Alpine and Debian between builder and runner.

For Temporal workers, the Dockerfile should follow this pattern:

```dockerfile
FROM node:24-bookworm-slim AS builder

RUN apt-get update \
    && apt-get install -y python3 make g++ --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# install dependencies

FROM node:24-bookworm-slim AS runner

RUN apt-get update \
    && apt-get install -y ca-certificates --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# copy node_modules from builder
```

Debian slim leaves out `ca-certificates`. Temporal still needs those certs, even without TLS to Temporal itself.

For services that are compatible with musl:

```dockerfile
FROM node:24-alpine AS builder

RUN apk add --no-cache python3 make g++

# install dependencies

FROM node:24-alpine AS runner

# copy node_modules from builder
```

Temporal's Docker guidance: [Run a TypeScript Worker process](https://docs.temporal.io/develop/typescript/workers/run-worker-process#run-a-worker-on-docker) — glibc image required, [do not use Alpine](https://docs.temporal.io/develop/typescript/workers/run-worker-process#do-not-use-alpine), and install `ca-certificates` on `node:*-slim`.

## Alternatives Considered

### Mix Alpine builder and Debian runner

* **Pros**: Smaller build stage; runner can stay Debian.
* **Cons**: Natives in `node_modules` do not match the process that loads them. Failures show up at container start, not at `docker build`.
* **Why not**: Relies on the package manager installing extras it no longer installs. Builder and runner must use the same libc.

### Alpine for Temporal workers

* **Pros**: Smaller images; one OS for the fleet.
* **Cons**: Temporal's TypeScript native (Rust core / `@temporalio/core-bridge`) requires glibc. There is no supported musl build.
* **Why not**: Temporal documents this as unsupported. Compiling the bridge yourself or adding `gcompat` is a workaround, not a platform we want to maintain.

### Debian for every service

* **Pros**: One image family; no "does this need glibc?" check.
* **Cons**: Debian slim is larger than Alpine (more disk, slower pulls on a cold node). More OS packages, so scanners can report a different / larger CVE set. `apt` vs `apk` in Dockerfiles.
* **Why not**: Services that already run on Alpine do not need to pay that cost. Only glibc-required services should be Debian.

## Consequences

### Positive

* Native dependencies are built and executed against the same libc.
* Temporal workers use a supported runtime.
* Alpine remains available for services that do not require glibc.
* The rule holds even if pnpm's optional-dep behavior changes again.

### Negative

* The fleet intentionally uses both Alpine and Debian.
* Developers need to keep builder and runner on the same image family.
* Debian-based Temporal images are larger than Alpine would be, so they cost a bit more registry storage and pull time. That is accepted for Temporal compatibility.

### Risks

* Someone copies an Alpine Dockerfile for a new Temporal TypeScript worker because most services are Alpine. If the process loads `@temporalio/worker` / `core-bridge`, both stages must be Debian.

## Rule of Thumb

When changing or creating a Node.js Dockerfile:

**Same libc in builder and runner.**

**Temporal TypeScript worker = Debian.**
