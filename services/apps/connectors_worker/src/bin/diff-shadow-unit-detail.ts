import { parseRepoChannel } from '@crowd/connectors/src/connectors/github/paging'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { getNangoMappingForRepo } from '@crowd/data-access-layer/src/integrations'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { initNangoCloudClient } from '@crowd/nango'

import { diffUnit, diffableRecordKey, resolveDiffWindow } from '../shadowDiffUnit'

const log = getServiceLogger()

function usage(): never {
  log.error('Usage: diff-shadow-unit-detail --unit-id <id> [--day <YYYY-MM-DD>]')
  process.exit(1)
}

function takeFlag(argv: string[], flag: string): string | undefined {
  const flagIndex = argv.indexOf(flag)
  if (flagIndex === -1) {
    return undefined
  }
  const value = argv[flagIndex + 1]
  argv.splice(flagIndex, 2)
  return value
}

function parseArgs(rawArgv: string[]): { unitId: string; day?: string } {
  const argv = rawArgv.filter((arg) => arg !== '--')
  const unitId = takeFlag(argv, '--unit-id')
  const day = takeFlag(argv, '--day')
  if (!unitId) {
    usage()
  }
  return { unitId, day }
}

setImmediate(async () => {
  try {
    const { unitId, day: dayArg } = parseArgs(process.argv.slice(2))

    const db = await getDbConnection(WRITE_DB_CONFIG())
    const qx = pgpQx(db)

    const unit = await qx.selectOneOrNone(
      `SELECT id, "integrationId", "channelName", "syncName"
       FROM integration.sync_units
       WHERE id = $(unitId)`,
      { unitId },
    )
    if (!unit) {
      throw new Error(`no sync_units row found for id ${unitId}`)
    }

    const { owner, repo } = parseRepoChannel(unit.channelName)
    const mapping = await getNangoMappingForRepo(qx, unit.integrationId, owner, repo)
    if (!mapping) {
      throw new Error(
        `no nango_mapping found for ${unit.channelName} (integrationId ${unit.integrationId})`,
      )
    }

    const { day, windowStart, windowEnd } = resolveDiffWindow(dayArg)
    log.info(
      { unitId, channelName: unit.channelName, syncName: unit.syncName, day },
      'diffing unit',
    )

    await initNangoCloudClient()

    const { mismatches, nangoRecordsByKey } = await diffUnit(
      qx,
      unit,
      mapping.connectionId,
      windowStart,
      windowEnd,
    )

    log.info({ mismatchCount: mismatches.length }, 'diff complete')
    for (const mismatch of mismatches) {
      const nangoRecord = nangoRecordsByKey.get(diffableRecordKey(mismatch))
      process.stdout.write(`${JSON.stringify({ ...mismatch, nangoRecord })}\n`)
    }

    process.exit(0)
  } catch (err) {
    log.error(err, 'diff-shadow-unit-detail failed')
    process.exit(1)
  }
})
