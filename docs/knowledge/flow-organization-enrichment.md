---
type: data-flow
slug: flow-organization-enrichment
name: How organizations are enriched
repo: crowd.dev
owner: CDP team
depends_on: [model-member-identity, sys-cdp]
used_by: []
sources:
  - services/apps/organizations_enrichment_worker/
  - services/libs/data-access-layer/src/organizations/enrichment/
  - services/libs/data-access-layer/src/organizations/attributesConfig.ts
  - services/libs/types/src/enums/enrichment.ts
  - backend/src/database/migrations/V1758292471__organizationEnrichment.sql
  - backend/src/database/migrations/R__organizationEnrichmentMaterializedViews.sql
---

# How organizations are enriched

`organizations_enrichment_worker` (Temporal, task queue `organizations-enrichment`) adds profile
attributes and identities to organizations from an external source, once a day. It has one source
today: `lfx-internal-api`, an LFX-run HTTP service that returns an organization profile for a name
and a list of domains. Its client is
`services/apps/organizations_enrichment_worker/src/sources/lfx-internal-api/service.ts`, and the
endpoint and key come from `CROWD_ORGANIZATION_ENRICHMENT_INTERNAL_API_URL` and
`CROWD_ORGANIZATION_ENRICHMENT_INTERNAL_API_KEY`. New sources plug in through
`OrganizationEnrichmentSourceServiceFactory` in `src/factory.ts`.

Member enrichment is a separate worker; see [`member-identity.md`](member-identity.md).

## Trigger

On start, the worker registers the Temporal schedule `organizations-enrichment` (06:00 daily,
every two minutes in dev and test), which starts `triggerOrganizationsEnrichment` with
`perRunLimit: 500`.

## Which organizations qualify

`fetchOrganizationsForEnrichment` in
`services/libs/data-access-layer/src/organizations/enrichment/index.ts` selects organizations that:

- are not deleted;
- have at least one verified `primary-domain` identity;
- have more than 10 activities in the `organizationsGlobalActivityCount` materialized view;
- have no `organizationEnrichmentCache` row for the source, or one older than 90 days.

Results are ordered by activity count, highest first.

`organizationsGlobalActivityCount` sums `activityCount` from `organizationSegmentsAgg` over
project segments (those with both `parentId` and `grandparentId`). Its definition is in
`R__organizationEnrichmentMaterializedViews.sql`. The worker has a
`refreshOrganizationEnrichmentMaterializedViews` workflow and a 05:00 schedule for it in
`src/schedules/refreshOrganizationEnrichmentMaterializedViews.ts`, but `src/main.ts` does not
register that schedule.

## The workflows

`triggerOrganizationsEnrichment` fetches a batch, then runs one child `enrichOrganization`
workflow per organization (ID `organization-enrichment/<organizationId>`), a few at a time: the
batch size is the smallest `maxConcurrentRequests` across sources, 4 for `lfx-internal-api`. It
then calls `continueAsNew` until no organization qualifies.

`enrichOrganization` (`src/workflows/enrichOrganization.ts`):

1. Exits if the organization no longer exists or its cache is less than 90 days old.
2. Builds the input from the display name and verified primary domains, deduplicated by value.
3. If there is more than one domain, asks an LLM to pick the one that matches the organization
   (`selectMostRelevantDomainWithLLM` in `src/activities/llm.ts`, query type
   `select_most_relevant_domain`). This guards against enriching from a wrong domain left by a bad
   merge.
4. Calls the source. A 404 or 422 means no data; 401, 403 and 400 fail without retry.
5. Writes `organizationEnrichmentCache`: creates it, replaces `data` when it changed, or only
   bumps `updatedAt` when nothing new came back.
6. Applies the data to the organization only when it differs from the cache, through
   `applyEnrichmentToOrganization` in `src/activities/enrichment.ts`, and sets
   `organizationEnrichments.lastUpdatedAt`.
7. Sets `organizationEnrichments.lastTriedAt` as its last step. A run that fails earlier, for
   example on a source error, does not set it.

## What enrichment writes

The source's `normalize` maps the profile to attributes (`name`, `headline`, `location`, `type`,
`logo`, `founded`, `employees`, `revenue`, `revenueRange`, `ultimateParent`) and identities
(primary and alternative domains, emails, and social usernames such as LinkedIn and Crunchbase).
All enriched identities are written unverified, with source `enrichment`.

Attributes are stored with source `enrichment-lfx-internal-api`. In
`ORG_DB_ATTRIBUTE_SOURCE_PRIORITY` (`services/libs/data-access-layer/src/organizations/attributesConfig.ts`)
that source ranks below `custom`, `lfx-serve`, `salesforce`, `system` and `llm`, so enrichment never
overrides a manual edit or those sources' values.

## Tables

| Table                              | Holds                                                                                                      |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `organizationEnrichmentCache`      | Raw source payload per organization and `source`; `updatedAt` drives the 90-day refresh                    |
| `organizationEnrichments`          | `lastTriedAt` (attempts that reached the end) and `lastUpdatedAt` (attempts that changed the organization) |
| `organizationsGlobalActivityCount` | Materialized view: activity count per organization, used for eligibility and ordering                      |

## Where to look

- Is enrichment running: compare recent `lastTriedAt` and `lastUpdatedAt` in
  `organizationEnrichments`, and check the `enrichOrganization` runs in Temporal: a failed run
  leaves no `lastTriedAt`, so an old timestamp does not prove nothing ran. Rows with `lastTriedAt`
  and no `lastUpdatedAt` were tried but changed nothing.
- Why an organization is skipped: check its verified primary domain, its row in
  `organizationsGlobalActivityCount`, and its cache age.
- Why a domain was chosen: the LLM answer is in `llmPromptHistory` (type
  `select_most_relevant_domain`, `entityId` is the organization ID).
