---
type: design
slug: design-affiliation
name: Affiliation across CDP services and stores
repo: crowd.dev
owner: CDP team
depends_on: [model-member-identity, flow-activities]
used_by: [flow-affiliation]
sources:
  - services/libs/common_services/src/services/common.member.service.ts
  - services/libs/common_services/src/services/member/update.ts
  - services/libs/data-access-layer/src/member-organization-affiliation/index.ts
  - services/libs/data-access-layer/src/affiliations/index.ts
  - services/libs/data-access-layer/src/members/segments.ts
  - services/libs/common/src/member.ts
  - services/apps/data_sink_worker/src/service/activity.service.ts
  - services/apps/data_sink_worker/src/service/member.service.ts
  - services/apps/profiles_worker/src/workflows/member/memberUpdate.ts
  - services/apps/profiles_worker/src/workflows/organization/organizationUpdate.ts
  - services/apps/cron_service/src/jobs/inferMemberOrganizationStintChanges.job.ts
  - services/apps/git_integration/src/crowdgit/services/affiliation/
  - backend/src/api/public/v1/affiliations/
  - backend/src/database/migrations/
---

# Affiliation across CDP services and stores

How CDP decides which organization each activity is credited to, and how that decision reaches
Tinybird and the public API. For where each piece lives, start from the note
[`flow-affiliation.md`](../knowledge/flow-affiliation.md).

## Where affiliation data comes from

Four tables feed every decision: work history (`memberOrganizations`), per-project affiliations
(`memberSegmentAffiliations`), per-work-experience overrides
(`memberOrganizationAffiliationOverrides`) and the organization-level block flag
(`organizations.isAffiliationBlocked`). The `source` column on `memberOrganizations` records which
writer created each row.

```mermaid
flowchart LR
  ui["UI and backend API"]
  pub["Public API v1<br/>work-experiences,<br/>project-affiliations"]
  enr["members_enrichment_worker"]
  sink["data_sink_worker"]
  cron["cron_service<br/>stint inference"]
  git["git_integration<br/>AffiliationService"]
  fake["profiles_worker<br/>fake-org analysis"]
  redis[("Redis<br/>stint dates")]

  mo[("memberOrganizations")]
  msa[("memberSegmentAffiliations")]
  ovr[("memberOrganizationAffiliationOverrides")]
  org[("organizations<br/>isAffiliationBlocked")]

  ui -->|"source ui"| mo
  ui --> msa
  ui --> ovr
  ui --> org
  pub --> mo
  pub --> msa
  enr -->|"enrichment-*"| mo
  sink -->|"email-domain,<br/>payload orgs"| mo
  sink --> redis
  redis --> cron
  cron -->|"dated email-domain rows"| mo
  git -->|"project-registry"| mo
  git --> msa
  fake --> org
  org -.->|"block: allowAffiliation false"| ovr
```

Blocking an organization, from the UI, from fake-organization analysis or from a script, sets
`allowAffiliation = false` on every work experience at that organization and deletes its
per-project affiliations.

## Recalculation and propagation

A change to any input signals the member's `memberUpdate` workflow. The workflow rebuilds the
member's timeline and rewrites `activityRelations`; Postgres replication carries the change to
Tinybird. The rules each path uses to pick an organization are in
[`member-organization-affiliation/README.md`](../../services/libs/data-access-layer/src/member-organization-affiliation/README.md).

```mermaid
flowchart LR
  subgraph triggers["Triggers"]
    be["backend: work history,<br/>project affiliations,<br/>member and org updates"]
    ew["members_enrichment_worker"]
    em["entity_merging_worker<br/>merge and unmerge"]
    cj["cron_service<br/>stint inference"]
    se["script_executor_worker<br/>bulk recalculation"]
  end

  ou["organizationUpdate<br/>(profiles queue)"]
  sig["signalMemberUpdate"]
  mu["memberUpdate/memberId<br/>(profiles queue)"]
  ref["refreshMemberOrganizationAffiliations"]
  ar[("Postgres<br/>activityRelations")]
  sq["Sequin"]
  k["Kafka"]
  kc["Kafka Connect"]
  tb[("Tinybird<br/>activityRelations")]
  os[("OpenSearch")]

  be --> sig
  ew --> sig
  em --> sig
  cj --> sig
  se --> sig
  be -->|"org update with<br/>recalculation"| ou
  em -->|"org merge"| ou
  ou -->|"each member,<br/>500 per run"| sig
  sig --> mu
  mu --> ref
  ref -->|"UPDATE organizationId<br/>in batches of 5,000"| ar
  mu -.->|"if syncToOpensearch"| os
  ar --> sq --> k --> kc --> tb
```

Signals that arrive while a pass is running are merged into one follow-up pass, so a member
never has two recalculations racing.

## Serving affiliations

The public affiliations API does not read `activityRelations`. It rebuilds periods from the same
tables on each request.

```mermaid
sequenceDiagram
  participant C as Client
  participant B as backend public v1
  participant P as Postgres
  C->>B: POST /v1/affiliations (Bearer key)
  B->>P: apiKeys lookup by SHA-256 hash
  B->>B: require scope read:affiliations
  B->>P: members by verified GitHub username
  B->>P: verified emails
  B->>P: work history, overrides, per-project affiliations
  B->>B: resolveAffiliationsByMemberIds
  B-->>C: contributors, affiliation periods, notFound
```

## Data model

Only the columns that take part in affiliation are shown.

```mermaid
erDiagram
  members ||--o{ memberOrganizations : "has work history"
  organizations ||--o{ memberOrganizations : "employs"
  memberOrganizations ||--o| memberOrganizationAffiliationOverrides : "overridden by"
  members ||--o{ memberOrganizationAffiliationOverrides : "owns"
  members ||--o{ memberSegmentAffiliations : "has"
  segments ||--o{ memberSegmentAffiliations : "scopes"
  organizations ||--o{ memberSegmentAffiliations : "credited"
  organizations ||--o{ organizationIdentities : "has"
  organizations ||--o{ organizationSegmentsAgg : "aggregated"
  members ||--o{ activityRelations : "performs"
  organizations ||--o{ activityRelations : "credited"
  segments ||--o{ activityRelations : "contains"

  members {
    uuid id PK
    text displayName
    jsonb attributes
  }
  organizations {
    uuid id PK
    text displayName
    boolean isAffiliationBlocked
  }
  organizationIdentities {
    uuid organizationId FK
    text platform
    varchar type
    text value
    boolean verified
  }
  memberOrganizations {
    uuid id PK
    uuid memberId FK
    uuid organizationId FK
    timestamptz dateStart
    timestamptz dateEnd
    varchar title
    varchar source
    boolean verified
    varchar verifiedBy
    timestamptz verifiedAt
    timestamptz createdAt
    timestamptz updatedAt
    timestamp deletedAt
    varchar deletedBy
  }
  memberOrganizationAffiliationOverrides {
    uuid id PK
    uuid memberId FK
    uuid memberOrganizationId FK
    boolean allowAffiliation
    boolean isPrimaryWorkExperience
  }
  memberSegmentAffiliations {
    uuid id PK
    uuid memberId FK
    uuid segmentId FK
    uuid organizationId FK
    timestamptz dateStart
    timestamptz dateEnd
    boolean verified
    varchar verifiedBy
    timestamptz deletedAt
  }
  segments {
    uuid id PK
  }
  organizationSegmentsAgg {
    uuid organizationId
    uuid segmentId
    bigint memberCount
  }
  activityRelations {
    uuid activityId PK
    uuid memberId FK
    uuid organizationId FK
    uuid segmentId FK
    timestamptz timestamp
    text username
  }
```

- `memberOrganizationAffiliationOverrides` is unique on `(memberId, memberOrganizationId)`. A
  missing row, or a `null` value, means `allowAffiliation = true` and
  `isPrimaryWorkExperience = false`.
- `members.attributes` carries the bot flag that makes ingest skip affiliation.
- `activityRelations.username` is what the email-domain rules match on.
