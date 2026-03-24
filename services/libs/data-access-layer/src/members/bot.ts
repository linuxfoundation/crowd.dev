import { PageData } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

import { IDbMemberBotSuggestionBySegment, IDbMemberBotSuggestionInsert } from './types'

export async function insertMemberBotSuggestion(
  qx: QueryExecutor,
  suggestion: IDbMemberBotSuggestionInsert,
): Promise<void> {
  await qx.result(
    `INSERT INTO "memberBotSuggestions" ("memberId", "confidence", "createdAt") 
     VALUES ($(memberId), $(confidence), now())
     ON CONFLICT DO NOTHING`,
    suggestion,
  )
}

export async function deleteMemberBotSuggestion(
  qx: QueryExecutor,
  memberId: string,
): Promise<void> {
  await qx.result(`DELETE FROM "memberBotSuggestions" WHERE "memberId" = $(memberId)`, { memberId })
}

export async function insertMemberNoBot(qx: QueryExecutor, memberId: string): Promise<void> {
  await qx.result(
    `INSERT INTO "memberNoBot" ("memberId", "createdAt") VALUES ($(memberId), now())
     ON CONFLICT DO NOTHING`,
    { memberId },
  )
}

export async function deleteMemberNoBot(qx: QueryExecutor, memberId: string): Promise<void> {
  await qx.result(`DELETE FROM "memberNoBot" WHERE "memberId" = $(memberId)`, { memberId })
}

export async function fetchBotCandidateMembers(qx: QueryExecutor, limit = 100): Promise<string[]> {
  const rows = await qx.select(
    `
    SELECT DISTINCT m.id AS "memberId"
    FROM "memberIdentities" mi
    JOIN "members" m ON mi."memberId" = m.id
    WHERE m."deletedAt" IS NULL
      AND mi."deletedAt" IS NULL
      AND (
        mi.value ~* '(^|[-_/\\s])(bot|robot)($|[-_/\\s])'
        OR mi.value ~* '(^|[-_/])(bot|automation|ci|cd|deploy|build|release)[-_]?\\d+$'
        OR mi.value ~* '(^|[-_/])(github|gitlab|bitbucket|azure|aws|gcp|k8s|openshift)[-_](bot|ci|actions?|pipeline|runner|automation)($|[-_/])'
        OR mi.value ~* '^(bot[-_/\\s\\d]*|.*[-_/\\s\\d]bot)$'

        OR mi.value ~* '.*\\[bot\\].*'
        OR mi.value ~* '.*[-_]ci$|^ci-.*'
        OR mi.value ~* '.*dependa.*'
        OR mi.value ~* '.*renovate.*'
        OR mi.value ~* '.*coderabbit.*'

        OR mi.value ~* '.*-actions$|.*actions.*bot.*'
        OR mi.value ~* '.*-automation$|.*automation.*bot.*'
        OR mi.value ~* '.*(commit|commits).*(bot|auto|ci|queue).*'
        OR mi.value ~* '.*(bot|auto|ci|queue).*(commit|commits).*'

        OR mi.value ~* '^auto[-_].*'
        OR mi.value ~* '.*[-_]auto$'

        OR mi.value ~* '^(k8s|knative|istio|openshift|okd)-.*bot$'
        OR mi.value ~* '^(azure|aws|gcp|google)-.*bot$'
        OR mi.value ~* '^(cncf|lf|linuxfoundation)-.*bot$'
        OR mi.value ~* '^(microsoft|redhat|ibm|intel|nvidia)-.*bot$'
        OR mi.value ~* '^(tensorflow|pytorch|onnx)-.*bot$'
        OR mi.value ~* '^(react|angular|vue|svelte)-.*bot$'
        OR mi.value ~* '^(rust|go|python|java|flutter)-.*bot$'

        OR mi.value ~* 'botify'

        OR mi.value ~* '^(claassistant|licenseeye|mergify|homu|bors|bors-ng|pull|release-drafter|auto-assign|sider|allcontributors|kodiak|probot|bors-servo|jenkins|teamcity|bamboo|concourse-ci)$'

        OR m."displayName" ~* '(^|[-_/\\s])(bot|robot)($|[-_/\\s])'
        OR m."displayName" ~* '(^|[-_/])(bot|automation|ci|cd|deploy|build|release)[-_]?\\d+$'
        OR m."displayName" ~* '(^|[-_/])(github|gitlab|bitbucket|azure|aws|gcp|k8s|openshift)[-_](bot|ci|actions?|pipeline|runner|automation)($|[-_/])'
        OR m."displayName" ~* '^(bot[-_/\\s\\d]*|.*[-_/\\s\\d]bot)$'

        OR m."displayName" ~* '.*\\[bot\\].*'
        OR m."displayName" ~* '.*[-_]ci$|^ci-.*'
        OR m."displayName" ~* '.*dependa.*'
        OR m."displayName" ~* '.*renovate.*'
        OR m."displayName" ~* '.*coderabbit.*'

        OR m."displayName" ~* '.*-actions$|.*actions.*bot.*'
        OR m."displayName" ~* '.*-automation$|.*automation.*bot.*'
        OR m."displayName" ~* '.*(commit|commits).*(bot|auto|ci|queue).*'
        OR m."displayName" ~* '.*(bot|auto|ci|queue).*(commit|commits).*'

        OR m."displayName" ~* '^auto[-_].*'
        OR m."displayName" ~* '.*[-_]auto$'

        OR m."displayName" ~* '^(k8s|knative|istio|openshift|okd)-.*bot$'
        OR m."displayName" ~* '^(azure|aws|gcp|google)-.*bot$'
        OR m."displayName" ~* '^(cncf|lf|linuxfoundation)-.*bot$'
        OR m."displayName" ~* '^(microsoft|redhat|ibm|intel|nvidia)-.*bot$'
        OR m."displayName" ~* '^(tensorflow|pytorch|onnx)-.*bot$'
        OR m."displayName" ~* '^(react|angular|vue|svelte)-.*bot$'
        OR m."displayName" ~* '^(rust|go|python|java|flutter)-.*bot$'

        OR m."displayName" ~* 'botify'

        OR m."displayName" ~* '^(claassistant|licenseeye|mergify|homu|bors|bors-ng|pull|release-drafter|auto-assign|sider|allcontributors|kodiak|probot|bors-servo|jenkins|teamcity|bamboo|concourse-ci)$'
      )
      AND (
        m.attributes IS NULL
        OR m.attributes->'isBot' IS NULL
        OR (m.attributes->'isBot'->>'default')::boolean IS NOT TRUE
      )
      AND NOT EXISTS (SELECT 1 FROM "memberBotSuggestions" WHERE "memberId" = m.id)
      AND NOT EXISTS (SELECT 1 FROM "memberNoBot" WHERE "memberId" = m.id)
    LIMIT $(limit);
    `,
    { limit },
  )

  return rows.map((r) => r.memberId)
}

export async function fetchMemberBotSuggestionsBySegment(
  qx: QueryExecutor,
  segmentId: string,
  limit: number,
  offset: number,
): Promise<PageData<IDbMemberBotSuggestionBySegment>> {
  const params = { segmentId, limit, offset }

  const createQuery = (fields: string) => `
    SELECT
      ${fields}
    FROM "memberBotSuggestions" mbs
    INNER JOIN "memberSegmentsAgg" msa ON mbs."memberId" = msa."memberId"
    AND msa."segmentId" = $(segmentId)
    INNER JOIN "members" m ON mbs."memberId" = m.id
  `

  const countQuery = createQuery('COUNT(*)')

  const dataQuery = `
    ${createQuery(`
      mbs."memberId",
      mbs.confidence,
      msa."activityCount",
      m."displayName",
      m.attributes -> 'avatarUrl' ->> 'default' AS "avatarUrl",
      m.attributes
    `)}
    ORDER BY mbs.confidence DESC, msa."activityCount" DESC
    LIMIT $(limit) OFFSET $(offset)
  `

  const results = await Promise.all([
    qx.select(dataQuery, params),
    qx.selectOne(countQuery, params),
  ])

  return {
    rows: results[0],
    count: parseInt(results[1].count, 10),
    limit,
    offset,
  }
}
