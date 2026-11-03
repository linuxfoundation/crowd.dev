---
type: data-flow
slug: flow-deduplication
name: Deduplication of members and organizations
repo: crowd.dev
owner: CDP team
depends_on: [model-member-identity, sys-cdp, design-deduplication]
used_by: []
sources:
  - services/apps/merge_suggestions_worker/
  - services/apps/entity_merging_worker/
  - services/libs/common_services/src/services/common.member.service.ts
  - services/libs/common_services/src/services/member/unmerge.ts
  - services/libs/data-access-layer/src/member_merge/
  - services/libs/data-access-layer/src/org_merge/
  - services/libs/data-access-layer/src/old/apps/merge_suggestions_worker/
  - services/libs/data-access-layer/src/merge-suggestion-sources/
  - services/libs/data-access-layer/src/llm-suggestion-verdicts/
  - services/libs/data-access-layer/src/mergeActions/
  - services/libs/data-access-layer/src/auditLogs/
  - services/libs/types/src/enums/suggestions.ts
  - services/libs/types/src/enums/merging.ts
  - backend/src/services/organizationService.ts
  - backend/src/api/member/
  - backend/src/api/organization/
  - backend/src/api/merge-suggestions/
  - backend/src/api/mergeAction/
---

# Deduplication of members and organizations

CDP collects the same person or organization from many sources, so duplicates are normal.
Deduplication finds likely duplicate pairs (merge suggestions), decides on them (a person in the
UI, or an LLM review), and merges or unmerges the records. Every merge can be undone.

The diagrams for this flow are in
[`docs/design/deduplication.md`](../design/deduplication.md). How a pair is scored is in
[`merge_suggestions_worker/README.md`](../../services/apps/merge_suggestions_worker/README.md). How identities are stored, and the
one merge that ingest does on its own (a verified identity collision), are in
[`member-identity.md`](member-identity.md).

## Who does what

| Part                                                        | Role                                                                                                                                                                         |
| ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `merge_suggestions_worker` (task queue `merge-suggestions`) | Generates suggestions on Temporal schedules and runs the LLM review workflows                                                                                                |
| `entity_merging_worker` (task queue `entity-merging`)       | Finishes merges and unmerges in the background: moves `activityRelations`, syncs OpenSearch, deletes the secondary record, notifies the UI                                   |
| `CommonMemberService.merge` (`common_services`)             | Synchronous part of a member merge, used by the API and by the LLM review                                                                                                    |
| `member/unmerge.ts` (`common_services`)                     | Unmerge preview and synchronous member unmerge                                                                                                                               |
| `backend/src/services/organizationService.ts`               | Synchronous organization merge, unmerge and no-merge                                                                                                                         |
| `backend/src/api/`                                          | `PUT /member/:id/merge`, `/unmerge`, `/unmerge/preview`, `/no-merge`, the same for `/organization/:id`, `POST /membersToMerge`, `/organizationsToMerge`, `GET /mergeActions` |
| `search_sync_api`                                           | Updates OpenSearch after a merge; the suggestion generator searches that index                                                                                               |
| `profiles_worker` (task queue `profiles`)                   | Recalculates activity affiliations after a merge (`memberUpdate`, `organizationUpdate`)                                                                                      |
| Amazon Bedrock                                              | Model behind the LLM review; calls only happen when `CROWD_ENABLE_LLM` is `true`                                                                                             |

## Workflows

| Workflow                                               | Trigger                                                             |
| ------------------------------------------------------ | ------------------------------------------------------------------- |
| `generateMemberMergeSuggestions`                       | Schedule `member-merge-suggestions`, every 2 hours                  |
| `generateOrganizationMergeSuggestions`                 | Schedule `organization-merge-suggestions`, every 2 hours            |
| `spawnSubprojectMemberMergeSuggestions`                | Schedule `spawn-subproject-member-merge-suggestions`, daily 06:00   |
| `mergeMembersWithLLM`, `mergeOrganizationsWithLLM`     | Started on demand with a similarity range; no schedule in this repo |
| `finishMemberMerging`, `finishOrganizationMerging`     | Started by the synchronous merge                                    |
| `finishMemberUnmerging`, `finishOrganizationUnmerging` | Started by the synchronous unmerge                                  |

## Tables

| Table                                        | Holds                                                                                                              |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `memberToMergeRaw`, `organizationToMergeRaw` | Every scored pair, whatever the score. The LLM review reads from here                                              |
| `memberToMerge`, `organizationToMerge`       | Pairs scored above 0.75, shown in the UI                                                                           |
| `mergeSuggestionSources`                     | Which flow created a pair; kept after the suggestion is gone ([ADR-0035](../adr/0035-merge-suggestion-sources.md)) |
| `memberNoMerge`, `organizationNoMerge`       | Pairs judged different. Member rows carry an `evidenceHash` ([ADR-0027](../adr/0027-nomerge-is-evidence-bound.md)) |
| `llmSuggestionVerdicts`                      | Every LLM decision, with prompt, response and token counts                                                         |
| `mergeActions`                               | Merge and unmerge progress (`state`, `step`) and the `unmergeBackup` of both records                               |
| `auditLogAction`                             | Who changed what, with old and new state                                                                           |
| `tenants`                                    | `memberMergeSuggestionsLastGeneratedAt`, `organizationMergeSuggestionsLastGeneratedAt`                             |

## What depends on what

- Suggestion generation pages through members or organizations updated since the last run and
  searches OpenSearch for candidates, so it is only as fresh as the search index. Identity writes
  bump `members.updatedAt` so changed members re-enter the generator (ADR-0027).
- Suggestions come from four sources (`MergeSuggestionSource`): global similarity, subproject
  similarity ([ADR-0025](../adr/0025-subproject-member-merge-suggestions.md)), identity conflicts
  found by the public API, and enrichment conflicts.
- A merge rewrites `activityRelations` in Postgres, which then replicates to Tinybird; see
  [`data-flow.md`](data-flow.md). Activity content is not touched.
- Rejecting a suggestion, an LLM "no", and every unmerge write a no-merge row. A member no-merge
  row stops applying when the members' evidence changes (ADR-0027). An organization no-merge row
  has no evidence check: the pair stays excluded until the row is deleted.
