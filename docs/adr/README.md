# Architecture Decision Records

Architecture Decision Records (ADRs) capture significant technical decisions made in this project, including context, alternatives considered, and consequences.

Use the `/adr` skill in Claude Code to record new ADRs or query past decisions.

## Index

| ADR | Title | Status | Date |
| --- | ----- | ------ | ---- |
<<<<<<< HEAD
| [ADR-0001](./0001-oss-packages-design-decisions.md) | OSS packages — design decisions (living) | living | 2026-05-27 |
=======
| [ADR-0001](./0001-packages-database.md) | Separate physical database for the packages domain | accepted | 2026-05-26 |
| [ADR-0002](./0002-packages-worker-architecture.md) | Single-service, multi-entry-point architecture for packages_worker | accepted | 2026-05-25 |
| [ADR-0003](./0003-has-critical-vulnerability-semantics.md) | Semantics of `packages.has_critical_vulnerability` | accepted | 2026-05-27 |
>>>>>>> ff8ded2ca (feat: osv advisories ingestion)

## Why ADRs?

The codebase is in active transition across several axes (see `CLAUDE.md`). ADRs provide a durable record of:
- Why old patterns are being replaced (e.g. Sequelize → pg-promise)
- What alternatives were considered before choosing the current approach
- What trade-offs were accepted

New contributors can understand constraints without needing to ask — the reasoning is in the ADRs.
