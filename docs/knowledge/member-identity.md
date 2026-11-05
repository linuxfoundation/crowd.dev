---
type: data-model
slug: model-member-identity
name: Members, organizations and identity
repo: crowd.dev
owner: CDP team
depends_on: []
used_by: [flow-activities, sys-cdp]
sources:
  - services/libs/types/src/members.ts
  - services/libs/types/src/organizations.ts
  - services/libs/types/src/enums/members.ts
  - services/libs/types/src/enums/organizations.ts
  - services/apps/data_sink_worker/src/service/member.service.ts
  - services/libs/data-access-layer/src/members/
  - services/libs/data-access-layer/src/github-email-lookups/
  - services/libs/common_services/src/services/member/unmerge.ts
  - services/libs/common_services/src/services/common.member.service.ts
  - services/libs/data-access-layer/src/affiliations/index.ts
  - services/libs/types/src/enums/merging.ts
  - services/libs/common/src/organization.ts
  - docs/adr/0015-how-cdp-stores-member-identities.md
---

# Members, organizations and identity

CDP's central job is deciding that a commit author, a GitHub login and a mailing list address
are the same person, and which organization that person worked for at the time. This note maps
the tables and the services that make those decisions.

How identity values are compared and cased is decided in
[ADR-0015](../adr/0015-how-cdp-stores-member-identities.md); read it before touching any identity
write path.

## Tables

| Table                                    | Holds                                                                                                                                             |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `members`                                | One row per person                                                                                                                                |
| `memberIdentities`                       | How a member appears on each platform: `platform`, `type`, `value`, `verified`, `verifiedBy`, `verifiedAt`, `source`, `sourceId`, `integrationId` |
| `organizations`                          | One row per organization                                                                                                                          |
| `organizationIdentities`                 | How an organization appears: domains, usernames, emails, affiliated profiles                                                                      |
| `memberOrganizations`                    | Work history: which organization a member was at, with `dateStart` and `dateEnd`                                                                  |
| `memberSegmentAffiliations`              | Per-project affiliation, used when a member contributes to a project on behalf of a specific organization                                         |
| `memberOrganizationAffiliationOverrides` | Per-work-experience overrides, such as marking one as the member's primary work experience                                                        |
| `memberToMerge`, `organizationToMerge`   | Merge suggestions waiting for a decision                                                                                                          |
| `memberNoMerge`                          | Pairs that must not be suggested again                                                                                                            |
| `mergeActions`                           | Merge and unmerge history: `type` (`member` or `org`), `primaryId`, `secondaryId`, `state` and `step`                                             |
| `githubEmailLookups`                     | Git author emails already looked up on GitHub, with the account found and what was done with it                                                   |

Identity types are small enums: members have `username` and `email`; organizations have
`username`, `primary-domain`, `alternative-domain`, `affiliated-profile` and `email`. Members also
carry a bot classification (`confirmed_bot`, `suspected_bot`, `not_bot`), and activity from bots
is stored without an organization.

## Verified and unverified identities

The rule the whole model rests on: **a verified identity belongs to exactly one member.** The
unique index `uix_memberIdentities_platform_type_lower_value_verified` enforces it, on
`(platform, type, lower(value))` for verified rows.

Unverified identities can appear on several members. They are hints, not claims.

## How ingest resolves a member

`data_sink_worker` (`member.service.ts`) handles incoming identities like this:

1. Deduplicate the payload on `(platform, type, lower(value))`, preferring the verified copy.
2. Drop unverified identities that another member already holds.
3. Insert the rest. If a verified identity collides with the unique index, another member already
   owns it: look up that owner and, when merging is allowed, merge the two members.

So ingest can merge members on its own, and does so only on a verified collision. Everything
else goes through suggestions.

## Merging and unmerging

- `merge_suggestions_worker` generates suggestions into `memberToMerge` and
  `organizationToMerge`, including suggestions checked by an LLM.
- A merge combines the records; `entity_merging_worker` then finishes the job by moving dependent
  data, including `activityRelations` through `moveActivityRelationsToAnotherMember`, and deleting
  orphans. `mergeActions` records each merge's `state` (`in-progress`, `merged`,
  `unmerged`, `error`) and its finer-grained `step` (`merge-started` through `merge-done`, and
  the matching `unmerge-*` steps); both enums are in `services/libs/types/src/enums/merging.ts`.
- Unmerging is in `services/libs/common_services/src/services/member/unmerge.ts`, also finished by
  `entity_merging_worker`.
- `script_executor_worker` holds bulk fixes, such as merging members whose identities differ only
  in capitalization.
- `profiles_worker` links git-only members to GitHub every hour: it asks GitHub who authored one
  of their commits, then merges them into the member that owns that GitHub account (unless the
  pair is in `memberNoMerge` or the stored GitHub ID differs), or adds the login as a verified
  identity. Each email is looked up once, recorded in `githubEmailLookups`.

Because activity relations live in Postgres, the rewrite replicates to Tinybird. Activity content is untouched; see [`data-flow.md`](data-flow.md).

## Enrichment

`members_enrichment_worker` and `organizations_enrichment_worker` add attributes, identities and
work history from external sources. Member sources are under
`services/apps/members_enrichment_worker/src/sources/`: Clearbit, Crustdata, LFID, ProgAI and
SERP. Enrichment follows the same rule as ingest: it does not add an unverified identity that
another member already has.

Placeholder employer labels such as "Freelance", "Self-employed" or "Student"
(`isPlaceholderEmployerName` in `services/libs/common/src/organization.ts`) are never organizations:
`findOrCreateOrganization` skips them on every path. Enrichment records such a role as a work
experience at the `Individual - No Account` organization instead.

## Affiliation

Two resolvers decide which organization a member is credited to, and they do not use the same
order.

**At ingest**, `data_sink_worker` calls `findAffiliation` in
`services/libs/common_services/src/services/common.member.service.ts`. This is what sets
`organizationId` on an activity:

1. a manual per-project affiliation from `memberSegmentAffiliations` always wins;
2. otherwise, if the activity's username is an email address on a non-excluded domain, a work
   experience at an organization whose verified primary domain matches it;
3. otherwise, work history active at the activity's timestamp;
4. otherwise, the most recent work experiences with unknown dates;
5. otherwise, any undated organization tied to the member.

When step 3 finds several organizations, it first drops universities if any company also matches
(`preferCompanyOverUniversityWhenOverlapping` in
`services/libs/data-access-layer/src/organizations/identities.ts`). The check only covers
organizations that have a verified primary domain: for those, "university" anywhere in the display
name (case-insensitive) or a `.edu` domain marks it as a university. An organization with no
verified primary domain is always treated as a company, so a university without one is never
dropped, whatever its name. So a university
marked primary can still lose to a non-primary company. Among the candidates left at any step,
the one marked primary through `memberOrganizationAffiliationOverrides` is preferred. Step 2 means an email-domain match can beat
a different, date-matched primary work experience.

**When affiliations are read back**, through the public v1 affiliations API in
`backend/src/api/public/v1/affiliations/` or by `packages_worker`, `selectPrimaryWorkExperience`
in `services/libs/data-access-layer/src/affiliations/index.ts` picks among the organizations
active on a date:

1. a manual per-project affiliation, which always wins (the longest date range if there are
   several);
2. otherwise, a work experience marked primary, preferring one with a start date;
3. otherwise, the only work experience with a start date, if there is exactly one;
4. otherwise, the best source among dated rows: `ui`, then `project-registry`, then
   `email-domain`, then enrichment (`getMemberOrganizationSourceRank` in
   `services/libs/common/src/member.ts`);

with further tie-breaks after that in the same file.

When a credited organization looks wrong on an activity, debug `findAffiliation`; when the
affiliations API disagrees with the activity data, the two resolvers' different orders are the
first thing to check. Bot members get no organization at all.
