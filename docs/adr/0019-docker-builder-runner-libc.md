# ADR-0019: Align Docker build and runtime images; use Debian for Temporal workers

**Date**: 2026-08-27
**Status**: accepted
**Deciders**: Yeganathan S

## Context

Our Node.js services use multi-stage Docker builds. Dependencies are installed in a builder stage, then `node_modules` is copied into the final runner image.

Some dependencies include native binaries compiled for a specific operating system, CPU architecture, and C standard library. Alpine uses musl, while Debian uses glibc. Copying native dependencies between these environments can produce an image that builds successfully but fails when the application starts.

The pnpm 11 upgrade exposed this existing mismatch. pnpm installs platform-specific optional dependencies for the environment where installation runs. Dependencies installed in an Alpine builder therefore cannot be assumed to work in a Debian runner, or the other way around. The package manager change exposed the problem; it did not create the underlying incompatibility.

Temporal TypeScript workers have an additional constraint: `@temporalio/core-bridge` requires glibc. Temporal explicitly does not support running TypeScript workers on Alpine.

## Decision

Builder and runner stages must use the same Node.js base image family and operating system.

- Temporal TypeScript workers use `node:<major>-bookworm-slim` for both stages.
- Services whose dependencies support musl may use `node:<major>-alpine` for both stages.
- Services with another glibc-only native dependency use Debian for both stages.
- Alpine and Debian must not be mixed when `node_modules` is copied between stages.

For example:

```dockerfile
FROM node:24-bookworm-slim AS builder
# Install dependencies

FROM node:24-bookworm-slim AS runner
# Copy dependencies from the builder
```

When using a Debian slim image for a Temporal worker, install `ca-certificates` in the runner as required by Temporal.

See [Temporal’s Docker guidance for TypeScript workers](https://docs.temporal.io/develop/typescript/workers/run-worker-process#run-a-worker-on-docker), particularly the sections on slim images and why Alpine is unsupported.

## Alternatives Considered

### Continue mixing Alpine builders and Debian runners

- **Pros**: Requires fewer changes to existing Dockerfiles.
- **Cons**: Native dependencies installed against musl may not load under glibc, and vice versa. Failures may only appear when the container starts.
- **Why not**: The result depends on incidental package-manager behavior and is not a reliable deployment model.

### Use Alpine for Temporal workers

- **Pros**: Smaller base images and one image family for more services.
- **Cons**: Temporal’s TypeScript SDK does not support musl because its Rust core requires glibc.
- **Why not**: Compatibility layers or custom musl builds would be unsupported workarounds with additional maintenance and runtime risk.

### Use Debian for every Node.js service

- **Pros**: One image family across the fleet and broader compatibility with native dependencies.
- **Cons**: Debian slim images are larger than Alpine images, take more registry storage and network transfer, and may produce more operating-system packages and security-scanner findings.
- **Why not**: Services that are already verified on Alpine do not need to pay those costs.

## Consequences

### Positive

- Native dependencies are installed and executed in compatible environments.
- Temporal TypeScript workers run on an officially supported platform.
- Docker behavior no longer depends on a package manager installing binaries for multiple platforms.
- Services without glibc requirements can continue using smaller Alpine images.
- Future Dockerfiles have a clear default: use the same image family in both stages.

### Negative

- The service fleet intentionally uses both Alpine and Debian images.
- Debian-based worker images are larger and may take longer to pull on a new Kubernetes node.
- Debian and Alpine use different package managers, so Dockerfile setup commands differ.
- Image security scans may report different findings for each distribution.

### Risks

- A service running on Alpine may later add a glibc-only native dependency. Image builds and staging startup checks should verify that the service still boots successfully.
- Builder and runner images may drift during future Dockerfile changes. Reviews should treat matching image families as a required invariant.
- Changing a Node major version or Debian release in one stage without changing the other can reintroduce compatibility problems.

## Rule of Thumb

When a Docker build copies `node_modules` from builder to runner:

**Use the same Node.js image family in both stages.**

**Temporal TypeScript worker means Debian with glibc.**
