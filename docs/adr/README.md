# Architecture Decision Records

Architecture Decision Records (ADRs) capture significant technical decisions made in this project, including context, alternatives considered, and consequences.

Use the `/adr` skill in Claude Code to record new ADRs or query past decisions.

## Index

| ADR                                                 | Title                                    | Status | Date       |
| --------------------------------------------------- | ---------------------------------------- | ------ | ---------- |
| [ADR-0001](./0001-oss-packages-design-decisions.md) | OSS packages — design decisions (living) | living | 2026-05-27 |
| [ADR-0003](./0003-deps-bq-table-selection.md) | Use DependencyGraphEdgesLatest for deps ingestion; defer DependenciesLatest until NUGET or GO needed | accepted | 2026-05-29 |
| [ADR-0004](./0004-go-nuget-transitive-dependent-counts.md) | Compute GO/NUGET transitive dependent counts via exact reverse closure (over HLL approximation) | accepted | 2026-06-23 |
| [ADR-0005](./0005-pypi-downloads-bigquery-merge-scoping.md) | PyPI downloads via BigQuery bulk export, scoped in the Postgres merge | accepted | 2026-07-01 |
| [ADR-0006](./0006-database-schema-types-as-source-of-truth.md) | Database schema types as the source of truth | accepted | 2026-07-09 |
| [ADR-0007](./0007-test-factory-primitives-and-defaults.md) | Test factory primitives and defaults | accepted | 2026-07-10 |
| [ADR-0008](./0008-how-we-write-unit-tests.md) | How we write unit tests | accepted | 2026-07-13 |

## Why ADRs?

The codebase is in active transition across several axes (see `CLAUDE.md`). ADRs provide a durable record of:

- Why old patterns are being replaced (e.g. Sequelize → pg-promise)
- What alternatives were considered before choosing the current approach
- What trade-offs were accepted

New contributors can understand constraints without needing to ask — the reasoning is in the ADRs.
