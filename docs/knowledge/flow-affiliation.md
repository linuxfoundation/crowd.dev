---
type: data-flow
slug: flow-affiliation
name: How CDP affiliates activities with organizations
repo: crowd.dev
owner: CDP team
depends_on: [model-member-identity, flow-activities, design-affiliation]
used_by: []
sources:
  - services/libs/common_services/src/services/common.member.service.ts
  - services/libs/common_services/src/services/member/update.ts
  - services/libs/common_services/src/services/member-organization.ts
  - services/libs/data-access-layer/src/member-organization-affiliation/
  - services/libs/data-access-layer/src/affiliations/
  - services/libs/data-access-layer/src/member_segment_affiliations/
  - services/libs/data-access-layer/src/members/segments.ts
  - services/apps/data_sink_worker/src/service/activity.service.ts
  - services/apps/data_sink_worker/src/service/member.service.ts
  - services/apps/profiles_worker/src/workflows/member/memberUpdate.ts
  - services/apps/profiles_worker/src/workflows/organization/organizationUpdate.ts
  - services/apps/profiles_worker/src/activities/organization/fakeAnalysis.ts
  - services/apps/cron_service/src/jobs/inferMemberOrganizationStintChanges.job.ts
  - services/apps/git_integration/src/crowdgit/services/affiliation/
  - services/apps/script_executor_worker/src/workflows/recalculate-member-affiliations.ts
  - backend/src/api/public/v1/affiliations/
  - backend/src/api/public/v1/members/work-experiences/
  - backend/src/api/public/v1/members/project-affiliations/
  - backend/src/api/member/affiliation/
  - backend/src/services/member/
---

# How CDP affiliates activities with organizations

An affiliation says which organization a member worked for, or contributed on behalf of, during
a period. CDP stores the member's work history and overrides in Postgres, picks one organization
per activity, and writes it to `activityRelations.organizationId`. Tinybird and LFX Insights count
organization contributions from that column.

The tables and the two resolvers' priority orders are in
[`member-identity.md`](member-identity.md#affiliation). The architecture diagrams, including the
data model, are in [`docs/design/affiliation.md`](../design/affiliation.md). The full selection
rules for each path are next to the code, in
[`member-organization-affiliation/README.md`](../../services/libs/data-access-layer/src/member-organization-affiliation/README.md).

## Inputs

| Input                                    | Written by                                                                                                                                                                                                                                                                                                                                                   |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `memberOrganizations` (work history)     | UI and public API (`source` `ui` or caller-supplied), `members_enrichment_worker` (`enrichment-*`), `data_sink_worker` (`email-domain`, plus organizations carried in the activity payload, such as `github` from the GitHub integration or `cvent`, `tnc`, `meetings` and `committees` from `snowflake_connectors`), `git_integration` (`project-registry`) |
| `memberSegmentAffiliations`              | Per-project affiliations: the UI (`backend/src/api/member/affiliation/`), `PATCH /v1/members/{memberId}/project-affiliations/{projectId}`, and `git_integration`                                                                                                                                                                                             |
| `memberOrganizationAffiliationOverrides` | `allowAffiliation` (use this work experience or not) and `isPrimaryWorkExperience`, set from the UI, or by blocking an organization                                                                                                                                                                                                                          |
| `organizations.isAffiliationBlocked`     | Set from the UI, by `profiles_worker`'s fake-organization analysis, or by a `script_executor_worker` script. Blocking sets `allowAffiliation = false` on every work experience at that organization and deletes its project affiliations                                                                                                                     |

The `Individual - No Account` organization (`INDIVIDUAL_NO_ACCOUNT_ORGANIZATION_NAME` in
`services/libs/common/src/organization.ts`) stands for contributions made without an employer.

**Email-domain stints.** When `data_sink_worker` sees an email identity on a non-excluded domain,
it links the member to the organization that owns that domain (`source = 'email-domain'`) and
queues the activity dates in Redis. Every five minutes `cron_service` runs
`infer-member-organization-stint-changes`, which turns those dates into dated
`memberOrganizations` rows and signals a recalculation.

**Repository affiliation files.** `git_integration`'s `AffiliationService` finds a
contributor-to-organization mapping file in a repository, parses it with an LLM, and writes
`memberOrganizations` rows (`project-registry`) and `memberSegmentAffiliations` rows for that
repository's project. `git."repoAffiliationRegistry"` tracks the file and the last run. It does
not start a recalculation; activities ingested afterwards use the new rows.

## Computing it

| When              | Where                                                                                                                                                | What it writes                                                               |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| Ingest            | `data_sink_worker` calls `findAffiliation` in `common.member.service.ts`                                                                             | `organizationId` on each new activity; bots get `null`                       |
| Recalculation     | `profiles_worker` workflow `memberUpdate`, activity `updateMemberAffiliations` → `refreshMemberOrganizationAffiliations`                             | Rewrites `activityRelations.organizationId` for all of a member's activities |
| Organization-wide | `profiles_worker` workflow `organizationUpdate` with `recalculateAffiliations`                                                                       | Signals `memberUpdate` for every member of the organization, 500 per run     |
| Bulk repair       | `script_executor_worker`: `recalculateMemberAffiliations` workflow, `recalculate-all-affiliations` and `recalculate-enrichment-affiliations` scripts | Same as recalculation, for many members                                      |

Every recalculation goes through `signalMemberUpdate`
(`services/libs/common_services/src/services/member/update.ts`). It signal-starts one
`memberUpdate/<memberId>` workflow on the `profiles` task queue, so concurrent requests for a
member coalesce into one pass. Callers include work-history and project-affiliation edits in
`backend`, member updates, enrichment, the stint job, and `entity_merging_worker` after a merge or
unmerge.

The recalculation builds a timeline from work history, per-project affiliations and email
domains, then updates `activityRelations` in batches of 5,000. It changes only Postgres; Sequin
replicates the rows to Tinybird (see [`data-flow.md`](data-flow.md)). `memberOrganizations` itself
is not replicated to Tinybird.

## Serving it

| Endpoint                                      | Auth                                         | Returns                                                                                     |
| --------------------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `POST /v1/affiliations`                       | Static API key, scope `read:affiliations`    | Up to 100 GitHub handles per request, paginated: name, verified emails, affiliation periods |
| `GET /v1/affiliations/{githubHandle}`         | Static API key, scope `read:affiliations`    | The same for one handle                                                                     |
| `/v1/members/{memberId}/work-experiences`     | OAuth2, `read:`/`write:work-experiences`     | Work history CRUD and verification; writes trigger a recalculation                          |
| `/v1/members/{memberId}/project-affiliations` | OAuth2, `read:`/`write:project-affiliations` | Per-project affiliations                                                                    |

The affiliations endpoints match members by verified GitHub `username` identities and resolve
periods with `resolveAffiliationsByMemberIds` in
`services/libs/data-access-layer/src/affiliations/index.ts`. Static keys are stored as SHA-256
hashes in `apiKeys` and checked by `staticApiKeyMiddleware`. The OpenAPI spec is
`backend/src/api/public/openapi.yaml`. `packages_worker` reads current affiliations from the same
module (`resolveCurrentAffiliationsByMemberIds`).

## Where to look

- **Wrong organization on a new activity**: `findAffiliation`, and whether the activity's
  username is an email on an organization's verified primary domain.
- **Old activities keep a stale organization**: check that the change signalled `memberUpdate`,
  then the workflow's history in Temporal.
- **API and dashboards disagree**: the API and the recalculation use different selection code;
  see the comparison in
  [`member-organization-affiliation/README.md`](../../services/libs/data-access-layer/src/member-organization-affiliation/README.md#how-the-three-paths-differ).
- **Organization still shows after blocking**: Tinybird pipes filter on
  `organizations.isAffiliationBlocked`; check the `organizations` datasource.
