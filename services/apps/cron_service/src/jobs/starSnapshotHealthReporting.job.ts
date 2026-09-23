import CronTime from 'cron-time-generator'

import { IS_DEV_ENV, IS_PROD_ENV } from '@crowd/common'
import {
  IRepoStarSnapshotGapDays,
  countDeadLetteredStarBackfillFailures,
  findAllRepoIdsWithStarSnapshotGaps,
  findDeadLetteredStarBackfillFailures,
  findReposForStarSnapshot,
  findStarSnapshotGapDaysForRepos,
  getDeadLetterReportCursor,
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
const GAP_DAYS_BATCH_SIZE = 5_000

const job: IJobDefinition = {
  name: 'star-snapshot-health-reporting',
  // cron_service runs jobs on Europe/Berlin time (main.ts); noon Berlin trails both the UTC-based
  // captureStarSnapshots (08:00 UTC) and selfHealStarBackfill (09:00 UTC) schedules year-round.
  cronTime: IS_DEV_ENV ? CronTime.every(15).minutes() : CronTime.everyDayAt(12, 0),
  timeout: 10 * 60,
  enabled: async () => IS_PROD_ENV,
  process: async (ctx) => {
    ctx.log.info('Running star-snapshot-health-reporting job...')

    const dbConnection = await getDbConnection(READ_DB_CONFIG(), 3, 0)
    const qx = pgpQx(dbConnection)
    const redis = await getRedisClient(REDIS_CONFIG())

    const since = await redis.get(LAST_DEAD_LETTER_REPORTED_AT_KEY)
    // Captured before the read below, not after gap-checks/Slack send - those can run long
    // enough to erode the safety margin if the watermark were taken at persist time instead.
    const nextCursor = await getDeadLetterReportCursor(qx)

    const [newlyDeadLettered, totalDeadLettered, allRepos] = await Promise.all([
      findDeadLetteredStarBackfillFailures(qx, since),
      countDeadLetteredStarBackfillFailures(qx),
      findReposForStarSnapshot(qx),
    ])

    const repoUrlById = new Map(allRepos.map((repo) => [repo.repositoryId, repo.repoUrl]))
    const allRepoIds = allRepos.map((repo) => repo.repositoryId)
    const gappedRepoIds = await findAllRepoIdsWithStarSnapshotGaps(qx, allRepoIds)

    const gapDays: IRepoStarSnapshotGapDays[] = []
    for (let i = 0; i < gappedRepoIds.length; i += GAP_DAYS_BATCH_SIZE) {
      const batch = gappedRepoIds.slice(i, i + GAP_DAYS_BATCH_SIZE)
      gapDays.push(...(await findStarSnapshotGapDaysForRepos(qx, batch)))
    }
    const missingDaysByRepoId = new Map(gapDays.map((gap) => [gap.repositoryId, gap.missingDays]))
    const totalMissingDays = gapDays.reduce((sum, gap) => sum + gap.missingDays, 0)
    // The two queries above run seconds apart, so a repo can close its gap in between and
    // come back with 0 missing days - drop those instead of over-reporting the gap count.
    const currentlyGappedRepoIds = gapDays
      .filter((gap) => gap.missingDays > 0)
      .map((gap) => gap.repositoryId)

    const sections: SlackMessageSection[] = [
      {
        title: 'Star Snapshot Health Summary',
        text: [
          `🪦 New repos GitHub gave up retrying (3 failures in a row, excl. repo-gone/IP-allowlist): *${newlyDeadLettered.length}*`,
          `📉 Total repos GitHub gave up retrying: *${totalDeadLettered}*`,
          `📅 Repos with a snapshot gap right now: *${currentlyGappedRepoIds.length}*`,
          `📆 Total missing snapshot-days across those repos: *${totalMissingDays}*`,
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
        title: `Repos No Longer Retried (top ${shown.length} of ${newlyDeadLettered.length})`,
        text: lines.join('\n'),
      })
    }

    if (currentlyGappedRepoIds.length > 0) {
      const sortedByMissingDays = [...currentlyGappedRepoIds].sort(
        (a, b) => (missingDaysByRepoId.get(b) ?? 0) - (missingDaysByRepoId.get(a) ?? 0),
      )
      const shown = sortedByMissingDays.slice(0, SAMPLE_SIZE)
      const lines = shown.map((repositoryId) => {
        const url = repoUrlById.get(repositoryId) ?? repositoryId
        const days = missingDaysByRepoId.get(repositoryId) ?? 0
        return `• \`${url}\` - missing ${days} day${days === 1 ? '' : 's'}`
      })
      sections.push({
        title: `Snapshot Gaps (top ${shown.length} of ${currentlyGappedRepoIds.length}, most days missing first)`,
        text: lines.join('\n'),
      })
    }

    const persona =
      newlyDeadLettered.length > 0 || currentlyGappedRepoIds.length > 0
        ? SlackPersona.WARNING_PROPAGATOR
        : SlackPersona.INFO_NOTIFIER

    const sent = await sendSlackNotificationAsync(
      SlackChannel.CDP_INTEGRATIONS_ALERTS,
      persona,
      'Star Snapshot Health Report',
      sections,
    )

    if (sent) {
      await redis.set(LAST_DEAD_LETTER_REPORTED_AT_KEY, nextCursor)
    } else {
      ctx.log.warn('star snapshot health report failed to send, will retry next run')
    }

    ctx.log.info(
      `Star snapshot health report sent: newlyDeadLettered=${newlyDeadLettered.length}, totalDeadLettered=${totalDeadLettered}, gaps=${currentlyGappedRepoIds.length}`,
    )
  },
}

export default job
