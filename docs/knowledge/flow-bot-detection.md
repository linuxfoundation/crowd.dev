---
type: data-flow
slug: flow-bot-detection
name: How CDP detects bot members
repo: crowd.dev
owner: CDP team
depends_on: [model-member-identity, sys-cdp]
used_by: [flow-activities]
sources:
  - services/libs/common_services/src/services/bot.service.ts
  - services/libs/common/src/bot.ts
  - services/libs/common/src/constants/bots.ts
  - services/apps/data_sink_worker/src/service/member.service.ts
  - services/apps/data_sink_worker/src/service/activity.service.ts
  - backend/src/database/repositories/memberRepository.ts
  - services/libs/common_services/src/services/member/unmerge.ts
  - services/apps/profiles_worker/src/workflows/member/processMemberBotAnalysisWithLLM.ts
  - services/apps/profiles_worker/src/activities/member/botSuggestion.ts
  - services/apps/profiles_worker/src/utils.ts
  - services/libs/data-access-layer/src/members/bot.ts
  - backend/src/services/member/memberAttributesService.ts
  - backend/src/api/member/memberBotSuggestionsList.ts
  - backend/src/bin/scripts/process-bot-members.ts
  - frontend/src/modules/data-quality/
  - frontend/src/modules/member/components/member-bot-suggestions.vue
---

# How CDP detects bot members

Bots such as Dependabot or CI accounts would inflate contributor counts and skew affiliations, so
CDP flags them on the member. A bot member is one whose `attributes.isBot` has any value set to
`true`. Activities from a bot member are stored with `isBotActivity: true` and no
`organizationId` (`activity.service.ts`), as described in
[`member-identity.md`](member-identity.md).

Detection has three stages: a heuristic check when a member is created, an LLM check for
suspected bots, and manual review of low-confidence results.

## 1. Heuristic check on member creation

`BotDetectionService.isMemberBot` in `services/libs/common_services/src/services/bot.service.ts`
looks at the display name and every identity value and returns a `MemberBotDetection` value:

- `confirmed_bot` when the integration already flagged the member (`isFlaggedAsBot`: GitHub,
  GitLab and Discord set `isBot` in their `processData.ts`), when a value is in the known-bots list
  (`isKnownBot`, list in `services/libs/common/src/constants/bots.ts`), or when a value matches a
  strong pattern such as `[bot]`;
- `suspected_bot` when a value matches a common pattern, such as a `bot`, `ci`, `deploy` or
  `runner` token;
- `not_bot` otherwise.

The check runs only where a member is created: in `data_sink_worker` (`MemberService.create` in
`member.service.ts`), in the backend (`MemberRepository.create`), and for the member split off by
an unmerge (`unmerge.ts`, which only sets the confirmed flag). It does not run when an existing
member is updated.

A confirmed bot gets `isBot: { default: true, system: true }` unless an active flag already exists,
and `data_sink_worker` skips organization creation for it. A suspected bot is created as normal and
an LLM check is started.

## 2. LLM check

`processMemberBotAnalysisWithLLM` runs in `profiles_worker` (task queue `profiles`), with workflow
ID `member-bot-analysis-with-llm/<memberId>`. It sends the display name, bio and up to 30 identities
to an LLM through `LlmService` (query type `member_bot_validation`, model and settings in
`services/libs/types/src/llm.ts`). The model returns `isBot` and per-field signal strengths;
`calculateMemberBotConfidence` in `profiles_worker/src/utils.ts` turns those into a score.

- Not a bot: the member is recorded in `memberNoBot`, so it is not picked up again.
- Bot with confidence of at least 0.99: the workflow sets `isBot` through the backend member
  attributes API, deletes the member's organizations and refreshes its affiliations.
- Bot with lower confidence: the workflow inserts a row into `memberBotSuggestions` with the
  confidence.

## 3. Manual review

`GET /member/bot-suggestions` (`backend/src/api/member/memberBotSuggestionsList.ts`) lists
suggestions per project, highest confidence first. Reviewers work through them in the Data Quality
Copilot's bot suggestions tab (`frontend/src/modules/data-quality/`) and on the member profile
(`frontend/src/modules/member/components/member-bot-suggestions.vue`). "Mark as bot" and "Ignore
suggestion" both set a `custom` value on `isBot` through the member attributes API.
`memberAttributesService.ts` keeps the tracking tables consistent: setting `isBot` to `true` clears
the member's `memberBotSuggestions` and `memberNoBot` rows; setting it to `false` clears the
suggestion and, if the system had flagged the member, adds a `memberNoBot` row.

## Flag precedence

When new activity arrives for an existing member, `data_sink_worker` (`memberAttValue` in
`activity.service.ts`) uses the first truthy value among `custom` (set by a person), `system` (set
by bot detection) and the incoming platform value, then falls back to `default`. A manual `true`
and a system flag survive later platform data, but a manual `custom: false` does not stop a true
system flag or platform value from winning.

## Existing members

`backend/src/bin/scripts/process-bot-members.ts` backfills the LLM check for existing members.
`fetchBotCandidateMembers` in `services/libs/data-access-layer/src/members/bot.ts` selects members
whose identities or display name match a broad set of bot-like patterns, whose `isBot.default` is
not `true`, and that have no suggestion and no `memberNoBot` row; the script starts the workflow
for each. A member whose suggestion was ignored can qualify again, because ignoring adds a
`memberNoBot` row only when the system had flagged the member.

## Where to look

- A real person flagged as a bot: check `attributes.isBot` for `system` versus `custom`, and the
  LLM answer in `llmPromptHistory` (type `member_bot_validation`, `entityId` is the member ID).
- A bot not flagged: the member predates detection or was only updated; run the backfill script
  or add the handle to `knownBots`.
- `merge_suggestions_worker` uses `isKnownBot` in two narrow places in
  `memberSimilarityCalculator.ts`: a known-bot display name keeps a Git email clash counting as a
  clash, and known-bot names are not used as a matching Git local part. Bot pairs can still be
  scored high and suggested.
