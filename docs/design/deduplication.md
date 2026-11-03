---
type: design
slug: design-deduplication
name: How deduplication works across services
repo: crowd.dev
owner: CDP team
depends_on: [model-member-identity, sys-cdp]
used_by: [flow-deduplication]
sources:
  - services/apps/merge_suggestions_worker/
  - services/apps/entity_merging_worker/
  - services/libs/common_services/src/services/common.member.service.ts
  - services/libs/common_services/src/services/member/unmerge.ts
  - services/libs/data-access-layer/src/member_merge/
  - services/libs/data-access-layer/src/org_merge/
  - services/libs/data-access-layer/src/old/apps/merge_suggestions_worker/
  - backend/src/services/organizationService.ts
  - backend/src/services/memberService.ts
  - backend/src/api/member/
  - backend/src/api/organization/
---

# How deduplication works across services

These diagrams show how CDP finds duplicate members and organizations, decides on them, and
merges or unmerges them. [`flow-deduplication.md`](../knowledge/flow-deduplication.md) lists the
files, tables and workflows by name; the ADRs linked there explain why the flow works this way.

## Components

The API and the two Temporal workers share Postgres. The suggestion generator reads candidates
from OpenSearch, and merges keep that index up to date through `search_sync_api`. Merge and
unmerge rewrite `activityRelations` in Postgres, which replicates to Tinybird.

```mermaid
flowchart LR
    UI[Frontend] --> API[Backend API]
    API --> PG[(Postgres)]
    API -->|start workflows| T[Temporal]
    T --> MSW[merge_suggestions_worker]
    T --> EMW[entity_merging_worker]
    MSW --> PG
    MSW -->|candidate search| OS[(OpenSearch)]
    MSW -->|LLM review| BR[Amazon Bedrock]
    MSW -->|org merge| API
    EMW --> PG
    EMW --> SSA[search_sync_api]
    SSA --> OS
    EMW -->|affiliations| PW[profiles_worker]
    EMW -->|"Redis pub/sub"| WS[API websocket]
    WS --> UI
    PG -.->|replication| TB[(Tinybird)]
```

## Generating suggestions

Two schedules run every two hours, one for members and one for organizations. Each run pages
through records updated since the last run, scores candidates, and stores the pairs. How a pair
is scored is in
[`merge_suggestions_worker/README.md`](../../services/apps/merge_suggestions_worker/README.md). A third,
daily schedule looks for duplicates inside recently onboarded projects
([ADR-0025](../adr/0025-subproject-member-merge-suggestions.md)).

```mermaid
sequenceDiagram
    participant S as Temporal schedule
    participant W as merge_suggestions_worker
    participant PG as Postgres
    participant OS as OpenSearch
    S->>W: generateMemberMergeSuggestions (every 2h)
    W->>PG: read memberMergeSuggestionsLastGeneratedAt
    loop pages of 50 members updated since then
        W->>PG: load member and identities
        W->>OS: search candidates (identities, display name)
        W->>W: score each pair
        W->>PG: upsert memberToMergeRaw (all pairs)
        W->>PG: upsert memberToMerge (score above 0.75)
        W->>PG: insert mergeSuggestionSources
        W->>W: continueAsNew with next page
    end
    W->>PG: touch memberMergeSuggestionsLastGeneratedAt
```

Organizations follow the same sequence with `generateOrganizationMergeSuggestions`, pages of 25,
and the `organization*` tables. Pairs that already have a valid no-merge row are excluded from
the candidate search. In a member pair, the member with more identities becomes the primary; on a
tie, the one with more activities.

## LLM review

`mergeMembersWithLLM` and `mergeOrganizationsWithLLM` are started on demand with a similarity
range. Each run takes a small batch of pairs from the raw table that have no verdict yet, asks a
Claude model on Amazon Bedrock for a structured yes or no, stores the verdict, and acts on it.
The member prompt tells the model never to merge two profiles with different values on the same
platform, never to merge a bot with a human, and never to merge on display name alone. Identities
that look like emails are removed from members before the call.

```mermaid
sequenceDiagram
    participant W as merge_suggestions_worker
    participant PG as Postgres
    participant BR as Amazon Bedrock
    participant API as Backend API
    loop until no pairs are left
        W->>PG: pairs in raw table, in range, no verdict
        W->>PG: load both profiles
        W->>BR: prompt with submit_merge_decision tool
        BR-->>W: decision and reason
        W->>PG: insert llmSuggestionVerdicts
        alt same entity
            Note over W,API: members merge in-process, organizations through the API
            W->>W: CommonMemberService.merge
            W->>API: PUT /organization/:id/merge
        else different
            W->>PG: delete pair from both suggestion tables
            W->>PG: insert no-merge row
        end
    end
```

## Member merge

A person merges from the UI with `PUT /member/:id/merge`; the LLM review calls the same
`CommonMemberService.merge`. The synchronous part runs in one transaction and returns to the
caller. `entity_merging_worker` then does the slow part. Only merges started by a user notify
the UI.

```mermaid
sequenceDiagram
    participant UI as Frontend
    participant API as Backend API
    participant PG as Postgres
    participant EMW as entity_merging_worker
    participant SSA as search_sync_api
    UI->>API: PUT /member/:id/merge
    API->>PG: auditLogAction, mergeActions (merge-started)
    API->>PG: move identities, affiliations, orgs
    API->>PG: merge fields into primary
    API->>PG: remove suggestions with secondary
    API->>PG: mergeActions (merge-sync-done)
    API->>EMW: start finishMemberMerging
    API-->>UI: success
    EMW->>PG: move activityRelations to primary
    EMW->>EMW: signal memberUpdate (affiliations)
    EMW->>SSA: sync primary, remove secondary
    EMW->>PG: delete secondary member
    EMW->>PG: mergeActions (merged, merge-done)
    EMW-->>UI: websocket member-merge
```

## Member unmerge

Unmerge takes one identity off a member. The caller can either split only that identity into a
new member or, when a merge backup exists, restore both members as they were before the merge
(`revertPreviousMerge`). `POST /member/:id/unmerge/preview` shows the result first.

```mermaid
sequenceDiagram
    participant UI as Frontend
    participant API as Backend API
    participant PG as Postgres
    participant EMW as entity_merging_worker
    UI->>API: POST /member/:id/unmerge
    API->>PG: auditLogAction
    API->>PG: create secondary with the identities
    API->>PG: mergeActions (unmerge-started)
    API->>PG: restore orgs, affiliations, fields
    API->>PG: insert memberNoMerge
    API->>EMW: start finishMemberUnmerging
    API-->>UI: success
    EMW->>PG: move matching activityRelations to secondary
    EMW->>EMW: sync both, recalculate affiliations
    EMW->>PG: mergeActions (unmerged, unmerge-done)
    EMW-->>UI: websocket member-unmerge
```

## Organization merge and unmerge

Organizations follow the same split between a synchronous step in `organizationService.ts` and a
background workflow. The background merge also moves Insights projects to the primary
organization and resyncs every member of the primary organization in OpenSearch.

```mermaid
sequenceDiagram
    participant UI as Frontend
    participant API as Backend API
    participant PG as Postgres
    participant EMW as entity_merging_worker
    participant PW as profiles_worker
    UI->>API: PUT /organization/:id/merge
    API->>PG: auditLogAction, mergeActions (merge-started)
    API->>PG: move identities and members, merge fields
    API->>PG: mergeActions (merge-sync-done)
    API->>EMW: start finishOrganizationMerging
    API-->>UI: success
    EMW->>PG: move activityRelations, insightsProjects
    EMW->>PW: organizationUpdate (if affiliations changed)
    EMW->>EMW: sync primary and its members
    EMW->>PG: delete secondary organization
    EMW->>PG: mergeActions (merged, merge-done)
    EMW-->>UI: websocket notification
```

An organization unmerge restores the secondary organization and its member roles from
`unmergeBackup`, writes `organizationNoMerge`, and starts `finishOrganizationUnmerging`, which
recalculates affiliations and resyncs both organizations.
