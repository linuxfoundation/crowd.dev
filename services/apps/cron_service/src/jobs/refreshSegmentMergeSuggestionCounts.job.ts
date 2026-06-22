import CronTime from 'cron-time-generator'

import {
  READ_DB_CONFIG,
  WRITE_DB_CONFIG,
  getDbConnection,
} from '@crowd/data-access-layer/src/database'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import {
  calculateSegmentMemberMergeSuggestionsCount,
  calculateSegmentOrganizationMergeSuggestionsCount,
  fetchProjectGroupSegmentIds,
  getSegmentMergeSuggestionCounts,
  upsertSegmentMergeSuggestionCounts,
} from '@crowd/data-access-layer/src/segments'

import { IJobDefinition } from '../types'

const job: IJobDefinition = {
  name: 'refresh-segment-merge-suggestion-counts',
  cronTime: CronTime.every(4).hours(),
  timeout: 2 * 60 * 60,
  process: async (ctx) => {
    const readDb = await getDbConnection(READ_DB_CONFIG(), 3, 0)
    const writeDb = await getDbConnection(WRITE_DB_CONFIG(), 1, 0)

    const readQx = pgpQx(readDb)
    const writeQx = pgpQx(writeDb)

    const segmentIds = await fetchProjectGroupSegmentIds(readQx)
    const BATCH_SIZE = 5

    const failedSegmentIds: string[] = []

    ctx.log.info({ segmentCount: segmentIds.length }, 'Refreshing segment merge suggestion counts')

    for (const batch of chunkArray(segmentIds, BATCH_SIZE)) {
      await Promise.all(
        batch.map(async (segmentId) => {
          try {
            const [memberMergeSuggestionsCount, organizationMergeSuggestionsCount] =
              await Promise.all([
                calculateSegmentMemberMergeSuggestionsCount(readQx, segmentId),
                calculateSegmentOrganizationMergeSuggestionsCount(readQx, segmentId),
              ])

            const smsc = await getSegmentMergeSuggestionCounts(readQx, segmentId)

            if (
              smsc?.memberMergeSuggestionsCount !== memberMergeSuggestionsCount ||
              smsc?.organizationMergeSuggestionsCount !== organizationMergeSuggestionsCount
            ) {
              await upsertSegmentMergeSuggestionCounts(writeQx, segmentId, {
                memberMergeSuggestionsCount,
                organizationMergeSuggestionsCount,
              })
            }

            ctx.log.debug({ segmentId }, 'Refreshed segment merge suggestion counts')
          } catch (err) {
            failedSegmentIds.push(segmentId)
            ctx.log.error(err, { segmentId }, 'Segment merge suggestion count refresh failed')
          }
        }),
      )
    }

    const failedCount = failedSegmentIds.length

    if (failedCount > 0) {
      throw new Error(
        `Segment merge suggestion counts refresh failed for ${failedCount}/${segmentIds.length} segments (${failedSegmentIds.join(', ')})`,
      )
    }

    ctx.log.info('Segment merge suggestion counts refresh finished')
  },
}

export default job
