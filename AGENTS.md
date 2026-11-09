# CDP — Community Data Platform

CDP is a community data platform by the Linux Foundation. It ingests millions of activities and events daily from platforms like GitHub, GitLab, and many others (not just code hosting). Open-source projects get onboarded by connecting integrations, and data flows continuously at scale.

The ingested data is often messy. A big part of what CDP does is improve data quality: deduplicating member and organization profiles through merge and unmerge operations, enriching data via third-party providers, and resolving identities across sources. The cleaned data powers analytics and insights for LFX products.

The codebase started as crowd.dev, an open-source startup later acquired by the Linux Foundation. Speed was prioritized over standards, but the platform is now stable. The focus has shifted to maintainable patterns, scalability, and good developer experience. Performance matters at this scale, even small inefficiencies compound across millions of data points.

Deeper notes on the services, how data flows and the member identity model are in
[`docs/knowledge/`](docs/knowledge/README.md). Architecture diagrams are in
[`docs/design/`](docs/design/README.md), and decisions in [`docs/adr/`](docs/adr/README.md).
`docs/design/README.md` explains which of the three a new doc belongs in.

## Tech stack

TypeScript, Node.js, Express, PostgreSQL (pg-promise), Temporal, Kafka, Redis, OpenSearch, Zod, Bunyan, AWS S3.

Vue 3, Vite, Tailwind CSS, Element Plus, Pinia, TanStack Vue Query, Axios.

Package manager is **pnpm**. Monorepo managed via pnpm workspaces.

## Codebase structure

```
backend/          -> APIs (public endpoints for LFX products + internal for CDP UI)
frontend/         -> CDP Platform UI
services/apps/    -> Microservices — Temporal workers, Node.js workers, webhook APIs
services/libs/    -> Shared libraries used across services
```

`services/libs/common` holds shared utilities, error classes, and helpers. If a piece of logic is reusable (not business logic), it belongs there.

`services/libs/data-access-layer` holds all database query functions. Check here before writing new ones — duplicates are already a problem.

Typecheck is `pnpm tsc-check` (`tsc -b` on the root `tsconfig.json`). A new `services/` package must be added to that file's `references`, and its own tsconfig must reference the `@crowd` packages it imports. Say so when you add one.

## Linting and formatting

- After making code changes, run `pnpm lint:fix`, then run `pnpm format`.
- Before finishing, run `pnpm lint --deny-warnings --format=agent`.

## Patterns in transition

Old and new patterns coexist. Always use the new pattern.

- **Sequelize -> pg-promise**: Sequelize is legacy (backend only). Use `queryExecutor` from `@crowd/data-access-layer` for all new database code.
- **Classes -> functions**: Class-based services and repos are legacy. Write plain functions — composable, modular, easy to test.
- **Multi-tenancy -> single tenant**: Multi-tenancy is being phased out. The tenant table still exists. Code uses `DEFAULT_TENANT_ID` from `@crowd/common`. Don't add new multi-tenant logic.
- **Legacy auth -> Auth0**: Auth0 is the current auth system. Ignore old JWT patterns.
- **Zod for validation**: Public API endpoints use Zod schemas with `validateOrThrow`. Follow this pattern for all new endpoints.
- **Connector routing -> the channel is the mapping**: a connector record goes to the integration and segment of the sync unit that fetched it. Don't add mapping tables or routing in `integration.settings`. Read [`services/libs/connectors/README.md`](services/libs/connectors/README.md) before touching connectors or `sync_units`.
- **Activity identity is exact**: the sink and the `activityRelations` sorting key match `channel` and `sourceId` byte for byte. Store channel names verbatim from the provider (never lowercased), build `sourceId` from immutable ids only, and check both before replaying history another pipeline already ingested. Details in the connectors README, rule 20.

## Working with the database

Millions of rows. Every query matters.

- Look up the table schema and indexes before writing any query. Don't select or touch columns blindly.
- Check existing functions in `data-access-layer` before writing new ones. Weigh the blast radius of modifying a shared function — sometimes a new function is safer.
- Write queries with performance in mind. Think about what indexes exist, what the query plan looks like, and whether you're scanning more rows than needed.

## Downstream consumer: LFX Insights

`linuxfoundation/insights` depends on this repository at four points, none of them a versioned
API. Changing any of them can break Insights production with no compile-time signal on this
side.

- **Libraries.** Insights vendors a sparse subset of `services/libs/*` and
  `services/archetypes/*` as a git submodule and links them as `@crowd/*` packages. Its deploys
  resolve that submodule to this repo's `main` at build time, so a merge here can reach Insights
  production without an Insights commit.
- **Postgres.** Insights connects to this product's database directly. It reads `repositories`
  and `insightsProjects`, and reads and writes `collections`, `collectionsInsightsProjects`,
  `collectionsRepositories`, `collectionLikes` and `insightsSsoUsers`. Community collections,
  collection likes and SSO user records are created by Insights and stored here.
- **Temporal.** Insights starts two workflows that run on workers in this repo:
  `upsertOSPSBaselineSecurityInsights` on the `security-best-practices` queue
  (`security_best_practices_worker`) and `processProjectDocsReadiness` on the `docs-readiness`
  queue (`docs_readiness_worker`). Insights keeps its own copies of their names, task queues and
  argument types, and its own implementation of the payload encryption codec.
- **Tinybird.** Insights appends community mentions to the `mentions` datasource defined in
  `services/libs/tinybird/datasources/mentions.datasource`, alongside the Octolens connector here,
  using a hand-copied list of its fields. Rows that stop matching the schema are quarantined
  rather than rejected, so a break there is silent.

Treat these as an external API: the exports of the vendored libraries, the schema of those
tables and of `mentions`, and the names, queues, arguments and codec of those two workflows.
What exactly is shared, and what counts as a breaking change, is recorded in
https://github.com/linuxfoundation/insights/blob/main/docs/knowledge/cross-repo-contract.md —
check it before changing any of these, and tell the Insights team.

## Code quality

- Functional and modular. Code should be easy to plug in, pull out, and test independently.
- Think about performance at scale, even for small changes.
- Define types properly — extend and reuse existing types. Don't sprinkle `any`.
- Don't touch working code outside the scope of the current task.
- Prefer doing less over introducing risk. Weigh trade-offs before acting.
- No comments. Code must be self-explanatory: name things well and extract functions instead of explaining them.
- A comment is allowed ONLY if one of these is true, and the reason cannot be expressed in code:
  - workaround for an external bug/API quirk (link it)
  - non-obvious invariant or ordering the caller must respect
  - performance/concurrency constraint that looks removable but isn't
  - genuinely convoluted logic (e.g. inherited/legacy complexity) that can't be simplified or extracted right now — prefer refactoring over commenting
  - `TODO(linuxfoundation/lfx-cdp-insights-ops#123):` with a GitHub issue
- Never comment: what the code does, section headers, JSDoc on obvious functions, notes about the change you just made.
- When allowed: 2 lines max.
