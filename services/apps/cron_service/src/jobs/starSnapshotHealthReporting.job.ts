import CronTime from 'cron-time-generator'

import { IS_DEV_ENV, IS_PROD_ENV } from '@crowd/common'
import {
  findDeadLetteredStarBackfillFailures,
  findRepoIdsWithStarSnapshotGaps,
  findReposForStarSnapshot,
} from '@crowd/data-access-layer'
import { READ_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { REDIS_CONFIG, getRedisClient } from '@crowd/redis'
import {
  SlackChannel,
  SlackMessageSection,
  SlackPersona,
  sendSlackNotificationAsync,
} from '@crowd/slack'

import { IJobDefinition } from '../types'

const LAST_DEAD_LETTER_REPORTED_AT_KEY =
  'star-snapshot-health-reporting:last-dead-letter-reported-at'
const SAMPLE_SIZE = 20

const job: IJobDefinition = {
  name: 'star-snapshot-health-reporting',
  cronTime: IS_DEV_ENV ? CronTime.every(15).minutes() : CronTime.everyDayAt(8, 45),
  timeout: 10 * 60,
  enabled: async () => IS_PROD_ENV,
  process: async (ctx) => {
    ctx.log.info('Running star-snapshot-health-reporting job...')

    const dbConnection = await getDbConnection(READ_DB_CONFIG(), 3, 0)
    const qx = pgpQx(dbConnection)
    const redis = await getRedisClient(REDIS_CONFIG())

    const since = await redis.get(LAST_DEAD_LETTER_REPORTED_AT_KEY)

    const [newlyDeadLettered, allDeadLettered, allRepos] = await Promise.all([
      findDeadLetteredStarBackfillFailures(qx, since),
      findDeadLetteredStarBackfillFailures(qx, null),
      findReposForStarSnapshot(qx),
    ])

    const repoUrlById = new Map(allRepos.map((repo) => [repo.repositoryId, repo.repoUrl]))
    const gappedRepoIds = await findRepoIdsWithStarSnapshotGaps(
      qx,
      allRepos.map((repo) => repo.repositoryId),
    )

    // Opaque high-water mark - passed straight back as `since` next run, never reparsed
    // locally as a JS Date (see findDeadLetteredStarBackfillFailures).
    if (newlyDeadLettered.length > 0) {
      await redis.set(LAST_DEAD_LETTER_REPORTED_AT_KEY, newlyDeadLettered[0].deadLetteredAt)
    }

    const sections: SlackMessageSection[] = [
      {
        title: 'Star Snapshot Health Summary',
        text: [
          `🪦 Newly dead-lettered (self-heal): *${newlyDeadLettered.length}*`,
          `📉 Total dead-lettered (self-heal): *${allDeadLettered.length}*`,
          `📅 Repos with a snapshot gap right now: *${gappedRepoIds.length}*`,
        ].join('\n'),
      },
    ]

    if (newlyDeadLettered.length > 0) {
      const shown = newlyDeadLettered.slice(0, SAMPLE_SIZE)
      const lines = shown.map((failure) => {
        const url = repoUrlById.get(failure.repositoryId) ?? failure.repositoryId
        return `• \`${url}\` - ${failure.lastErrorClass ?? 'unknown error'} (${failure.consecutiveFailures} consecutive failures)`
      })
      sections.push({
        title: `Newly Dead-Lettered (top ${shown.length} of ${newlyDeadLettered.length})`,
        text: lines.join('\n'),
      })
    }

    if (gappedRepoIds.length > 0) {
      const shown = gappedRepoIds.slice(0, SAMPLE_SIZE)
      const lines = shown.map(
        (repositoryId) => `• \`${repoUrlById.get(repositoryId) ?? repositoryId}\``,
      )
      sections.push({
        title: `Snapshot Gaps (top ${shown.length} of ${gappedRepoIds.length})`,
        text: lines.join('\n'),
      })
    }

    const persona =
      newlyDeadLettered.length > 0 || gappedRepoIds.length > 0
        ? SlackPersona.WARNING_PROPAGATOR
        : SlackPersona.INFO_NOTIFIER

    await sendSlackNotificationAsync(
      SlackChannel.CDP_INTEGRATIONS_ALERTS,
      persona,
      'Star Snapshot Health Report',
      sections,
    )

    ctx.log.info(
      `Star snapshot health report sent: newlyDeadLettered=${newlyDeadLettered.length}, totalDeadLettered=${allDeadLettered.length}, gaps=${gappedRepoIds.length}`,
    )
  },
}

export default job
