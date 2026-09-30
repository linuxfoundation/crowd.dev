import commandLineArgs from 'command-line-args'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'
import { Error404, Error409 } from '@crowd/common'
import { CommonMemberService } from '@crowd/common_services'
import {
  ILlmSuggestionVerdictPair,
  findLlmApprovedMemberPairsToMerge,
  pgpQx,
  removeMemberNoMerge,
} from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { getTemporalClient } from '@crowd/temporal'

const log = getServiceLogger()

const FIRST_ID = '00000000-0000-0000-0000-000000000000'

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Run in test mode (stop after the first 10 pairs).',
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

  log.info({ testRun, BATCH_SIZE }, 'Running script with the following parameters!')

  let totalMerged = 0
  let mergedInPass = 0

  do {
    mergedInPass = 0
    let afterId = FIRST_ID
    let candidates: ILlmSuggestionVerdictPair[] = []

    do {
      candidates = await findLlmApprovedMemberPairsToMerge(qx, { afterId, limit: BATCH_SIZE })

      const seenMemberIds = new Set<string>()
      const pairs = candidates.filter(({ primaryId, secondaryId }) => {
        if (seenMemberIds.has(primaryId) || seenMemberIds.has(secondaryId)) {
          return false
        }

        seenMemberIds.add(primaryId)
        seenMemberIds.add(secondaryId)
        return true
      })

      const results = await Promise.all(
        pairs.map(async ({ primaryId, secondaryId }) => {
          try {
            await memberService.merge(primaryId, secondaryId)
            await removeMemberNoMerge(qx, [{ memberId: primaryId, noMergeId: secondaryId }])
            await temporal.workflow.result(`finishMemberMerging/${primaryId}/${secondaryId}`)
            return true
          } catch (err) {
            if (err instanceof Error404 || err instanceof Error409) {
              log.warn(
                { primaryId, secondaryId },
                'Skipping members, one is gone or already being merged',
              )
            } else {
              log.error({ err, primaryId, secondaryId }, 'Failed to merge members!')
            }
            return false
          }
        }),
      )

      const merged = results.filter(Boolean).length
      mergedInPass += merged
      totalMerged += merged
      afterId = candidates[candidates.length - 1]?.id ?? afterId

      log.info(
        { merged, skipped: candidates.length - merged, totalMerged },
        'Processed a batch of members!',
      )

      if (testRun) {
        log.info('Test run - stopping after first batch!')
        process.exit(0)
      }
    } while (candidates.length > 0)
  } while (mergedInPass > 0)

  log.info({ totalMerged }, 'Done! No more LLM approved members to merge.')
  process.exit(0)
})
