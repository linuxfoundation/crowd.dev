# Architecture Decision Records

Architecture Decision Records (ADRs) capture significant technical decisions made in this project, including context, alternatives considered, and consequences.

Use the `/adr` skill in Claude Code to record new ADRs or query past decisions.

## Index

| ADR | Title | Status | Date |
| --- | ----- | ------ | ---- |
| [ADR-0001](./0001-oss-packages-design-decisions.md) | OSS packages — design decisions (living) | living | 2026-05-27 |
| [ADR-0003](./0003-has-critical-vulnerability-semantics.md) | Semantics of `packages.has_critical_vulnerability` | accepted | 2026-05-27 |
| [ADR-0004](./0004-standalone-bin-vs-temporal-for-batch-sub-workers.md) | Standalone bin vs Temporal for batch sub-workers in packages_worker | accepted | 2026-05-27 |
| [ADR-0005](./0005-cvss-scoring-strategy.md) | CVSS scoring strategy for OSV ingestion (inline v3.1, defer v4) | accepted | 2026-05-27 |

## Why ADRs?

The codebase is in active transition across several axes (see `CLAUDE.md`). ADRs provide a durable record of:
- Why old patterns are being replaced (e.g. Sequelize → pg-promise)
- What alternatives were considered before choosing the current approach
- What trade-offs were accepted

New contributors can understand constraints without needing to ask — the reasoning is in the ADRs.
