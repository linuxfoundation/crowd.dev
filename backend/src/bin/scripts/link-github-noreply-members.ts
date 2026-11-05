import commandLineArgs from 'command-line-args'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'
import { getSearchSyncApiClient } from '@/utils/apiClients'
import { Error404, Error409 } from '@crowd/common'
import { CommonMemberService } from '@crowd/common_services'
import {
  QueryExecutor,
  getMemberNoMerge,
  insertMemberIdentities,
  pgpQx,
} from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { getTemporalClient } from '@crowd/temporal'
import { MemberIdentityType, PlatformType } from '@crowd/types'

const log = getServiceLogger()

interface GithubNoreplyMemberToLink {
  memberId: string
  integrationId: string | null
  githubUsername: string
  ownerId: string | null
  cursor: string
}

async function fetchGithubNoreplyMembersToLink(
  qx: QueryExecutor,
  { afterCursor, limit }: { afterCursor: string; limit: number },
): Promise<GithubNoreplyMemberToLink[]> {
  return qx.select(
    `
      WITH candidates AS (
        SELECT
          mi."memberId",
          mi."integrationId",
          gh.username AS "githubUsername",
          lower(mi.value) AS cursor
        FROM "memberIdentities" mi
        CROSS JOIN LATERAL (
          SELECT regexp_replace(
            mi.value, '^([^+]*\\+)?(.*)@users\\.noreply\\.github\\.com$', '\\2', 'i'
          ) AS username
        ) gh
        WHERE mi.platform = 'git'
          AND mi.type = 'username'
          AND mi.verified = true
          AND mi."deletedAt" IS NULL
          AND lower(mi.value) > $(afterCursor)
          AND lower(mi.value) LIKE '%@users.noreply.github.com'
          AND gh.username <> ''
          AND NOT EXISTS (
            SELECT 1
            FROM "memberIdentities" linked
            WHERE linked."memberId" = mi."memberId"
              AND linked.platform = 'github'
              AND linked.type = 'username'
              AND linked."deletedAt" IS NULL
              AND lower(linked.value) = lower(gh.username)
          )
        ORDER BY lower(mi.value)
        LIMIT $(limit)
      )
      SELECT c.*, owner."memberId" AS "ownerId"
      FROM candidates c
      LEFT JOIN "memberIdentities" owner
        ON owner.platform = 'github'
        AND owner.type = 'username'
        AND owner.verified = true
        AND owner."deletedAt" IS NULL
        AND lower(owner.value) = lower(c."githubUsername")
      ORDER BY c.cursor
    `,
    { afterCursor, limit },
  )
}

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Run in test mode (stop after the first 10 members).',
  },
]

const parameters = commandLineArgs(options)

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const BATCH_SIZE = testRun ? 10 : 100

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)
  const temporal = await getTemporalClient(TEMPORAL_CONFIG)
  const memberService = new CommonMemberService(qx, temporal, log)
  const searchSyncApi = await getSearchSyncApiClient()

  log.info({ testRun, BATCH_SIZE }, 'Running script with the following parameters!')

  let totalLinked = 0
  let totalMerged = 0
  let processedInPass = 0

  do {
    processedInPass = 0
    let afterCursor = ''
    let candidates: GithubNoreplyMemberToLink[] = []

    do {
      candidates = await fetchGithubNoreplyMembersToLink(qx, { afterCursor, limit: BATCH_SIZE })

      // a member, owner or username can only be touched once per batch, the rest waits for the next pass
      const seen = new Set<string>()
      const batch = candidates.filter(({ memberId, ownerId, githubUsername }) => {
        const keys = [memberId, ownerId ?? `github:${githubUsername.toLowerCase()}`]
        if (keys.some((key) => seen.has(key))) {
          return false
        }

        keys.forEach((key) => seen.add(key))
        return true
      })

      const toLink = batch.filter(({ ownerId }) => !ownerId)
      const toMerge = batch.filter(({ ownerId }) => ownerId)

      const linkedIdentities = await insertMemberIdentities(
        qx,
        toLink.map(({ memberId, integrationId, githubUsername }) => ({
          memberId,
          integrationId,
          platform: PlatformType.GITHUB,
          value: githubUsername,
          type: MemberIdentityType.USERNAME,
          verified: true,
          source: 'integration',
        })),
        false,
        true,
      )

      // the identity is already written, so a rerun would skip this member and never sync it
      await Promise.all(
        linkedIdentities.map(async ({ memberId }) => {
          try {
            await searchSyncApi.triggerMemberSync(memberId)
          } catch (err) {
            log.error({ err, memberId }, 'Failed to sync member to OpenSearch!')
          }
        }),
      )

      const noMergePairs = await getMemberNoMerge(
        qx,
        toMerge.flatMap(({ memberId, ownerId }) => [memberId, ownerId]),
        { includeExpired: true },
      )

      const mergeResults = await Promise.all(
        toMerge.map(async ({ memberId, ownerId }) => {
          const isNoMerge = noMergePairs.some(
            (pair) =>
              (pair.memberId === memberId && pair.noMergeId === ownerId) ||
              (pair.memberId === ownerId && pair.noMergeId === memberId),
          )

          if (isNoMerge) {
            log.warn({ memberId, ownerId }, 'Skipping members marked as no-merge')
            return false
          }

          try {
            await memberService.merge(ownerId, memberId)
            await temporal.workflow.result(`finishMemberMerging/${ownerId}/${memberId}`)
            return true
          } catch (err) {
            if (err instanceof Error404 || err instanceof Error409) {
              log.warn(
                { memberId, ownerId },
                'Skipping members, one is gone or already being merged',
              )
            } else {
              log.error({ err, memberId, ownerId }, 'Failed to merge members!')
            }
            return false
          }
        }),
      )

      const linked = linkedIdentities.length
      const merged = mergeResults.filter(Boolean).length
      processedInPass += linked + merged
      totalLinked += linked
      totalMerged += merged
      afterCursor = candidates[candidates.length - 1]?.cursor ?? afterCursor

      log.info(
        { linked, merged, skipped: candidates.length - linked - merged, totalLinked, totalMerged },
        'Processed a batch of members!',
      )

      if (testRun) {
        log.info({ toLink, toMerge }, 'Test run - stopping after first batch!')
        process.exit(0)
      }
    } while (candidates.length > 0)
  } while (processedInPass > 0)

  log.info({ totalLinked, totalMerged }, 'Done! No more GitHub noreply members to link.')
  process.exit(0)
})
