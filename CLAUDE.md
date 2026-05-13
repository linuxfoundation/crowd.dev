# CDP — Community Data Platform

CDP is a community data platform by the Linux Foundation. It ingests millions of
activities and events daily from platforms like GitHub, GitLab, and many others
(not just code hosting). Open-source projects get onboarded by connecting
integrations, and data flows continuously at scale.

The ingested data is often messy. A big part of what CDP does is improve data quality: deduplicating member and organization profiles through merge and unmerge operations, enriching data via third-party providers, and resolving identities across sources. The cleaned data powers analytics and insights for LFX products.

The codebase started as crowd.dev, an open-source startup later acquired by the Linux Foundation. Speed was prioritized over standards, but the platform is now stable. The focus has shifted to maintainable patterns, scalability, and good developer experience. Performance matters at this scale, even small inefficiencies compound across millions of data points.

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

`services/libs/common` holds shared utilities, error classes,
and helpers. If a piece of logic is reusable (not business logic), it belongs there.

`services/libs/data-access-layer` holds all
database query functions. Check here before writing new ones — duplicates are
already a problem.

## Patterns in transition

Old and new patterns coexist. Always use the new pattern.

- **Sequelize -> pg-promise**: Sequelize is legacy (backend only). Use
  `queryExecutor` from `@crowd/data-access-layer` for all new database code.
- **Classes -> functions**: Class-based services and repos are legacy. Write
  plain functions — composable, modular, easy to test.
- **Multi-tenancy -> single tenant**: Multi-tenancy is being phased out. The
  tenant table still exists. Code uses `DEFAULT_TENANT_ID` from `@crowd/common`.
  Don't add new multi-tenant logic.
- **Legacy auth -> Auth0**: Auth0 is the current auth system. Ignore old JWT
  patterns.
- **Zod for validation**: Public API endpoints use Zod schemas with
  `validateOrThrow`. Follow this pattern for all new endpoints.

## Working with the database

Millions of rows. Every query matters.

- Look up the table schema and indexes before writing any query. Don't select
  or touch columns blindly.
- Check existing functions in `data-access-layer` before writing new ones.
  Weigh the blast radius of modifying a shared function — sometimes a new
  function is safer.
- Write queries with performance in mind. Think about what indexes exist, what
  the query plan looks like, and whether you're scanning more rows than needed.

## Code quality

- Functional and modular. Code should be easy to plug in, pull out, and test
  independently.
- Think about performance at scale, even for small changes.
- Define types properly — extend and reuse existing types. Don't sprinkle `any`.
- Don't touch working code outside the scope of the current task.
- Prefer doing less over introducing risk. Weigh trade-offs before acting.
