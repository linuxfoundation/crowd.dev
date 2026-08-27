# Architecture Decision Records

Architecture Decision Records (ADRs) capture significant technical decisions made in this project, including context, alternatives considered, and consequences.

Use the `/adr` skill in Claude Code to record new ADRs or query past decisions.

## Index

| ADR                                                            | Title                                                                                                | Status   | Date       |
| -------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | -------- | ---------- |
| [ADR-0001](./0001-oss-packages-design-decisions.md)            | OSS packages — design decisions (living)                                                             | living   | 2026-05-27 |
| [ADR-0003](./0003-deps-bq-table-selection.md)                  | Use DependencyGraphEdgesLatest for deps ingestion; defer DependenciesLatest until NUGET or GO needed | accepted | 2026-05-29 |
| [ADR-0004](./0004-go-nuget-transitive-dependent-counts.md)     | Compute GO/NUGET transitive dependent counts via exact reverse closure (over HLL approximation)      | accepted | 2026-06-23 |
| [ADR-0005](./0005-pypi-downloads-bigquery-merge-scoping.md)    | PyPI downloads via BigQuery bulk export, scoped in the Postgres merge                                | accepted | 2026-07-01 |
| [ADR-0006](./0006-database-schema-types-as-source-of-truth.md) | Database schema types as the source of truth                                                         | accepted | 2026-07-09 |
| [ADR-0007](./0007-test-factory-primitives-and-defaults.md)     | Test factory primitives and defaults                                                                 | accepted | 2026-07-10 |
| [ADR-0008](./0008-how-we-write-unit-tests.md)                  | How we write unit tests                                                                              | accepted | 2026-07-13 |
| [ADR-0009](./0009-packagist-worker-design-decisions.md)        | Packagist worker — design decisions                                                                  | accepted | 2026-07-13 |
| [ADR-0010](./0010-security-contacts-worker.md)                 | Security contacts — tiered extraction, confidence scoring, and Temporal batch ingestion              | accepted | 2026-07-21 |
| [ADR-0011](./0011-mailinglist-skip-unparseable-dates.md)       | Skip mailing list activities with unparseable/implausible dates                                      | accepted | 2026-07-19 |
| [ADR-0012](./0012-api-e2e-test-architecture.md)                | API e2e test architecture                                                                            | accepted | 2026-07-25 |
| [ADR-0013](./0013-api-e2e-test-suite-design.md)                | API e2e test suite design                                                                            | accepted | 2026-07-24 |
| [ADR-0014](./0014-collaboration-track-record-signal.md)        | Collaboration track record signal                                                                    | accepted | 2026-07-28 |
| [ADR-0015](./0015-how-cdp-stores-member-identities.md)         | How CDP stores member identities                                                                     | accepted | 2026-07-28 |
| [ADR-0016](./0016-akrites-cdp-public-api-authentication.md)    | Akrites → CDP public API authentication                                                              | proposed | 2026-07-22 |
| [ADR-0017](./0017-blast-radius-pipeline-architecture.md)       | Blast radius analysis pipeline — multi-ecosystem architecture                                        | accepted | 2026-08-11 |
| [ADR-0018](./0018-per-client-rate-limiting-members-resolve.md) | Per-client rate limiting for `POST /members/resolve` using in-memory store                           | accepted | 2026-08-12 |
| [ADR-0019](./0019-docker-builder-runner-libc.md)               | Use matching Node image families in Docker builds                                                    | accepted | 2026-08-27 |

## Why ADRs?

The codebase is in active transition across several axes (see `CLAUDE.md`). ADRs provide a durable record of:

- Why old patterns are being replaced (e.g. Sequelize → pg-promise)
- What alternatives were considered before choosing the current approach
- What trade-offs were accepted

New contributors can understand constraints without needing to ask — the reasoning is in the ADRs.
