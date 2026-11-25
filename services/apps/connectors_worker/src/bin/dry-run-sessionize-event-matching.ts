import { readFileSync } from 'node:fs'

import { getPlatformCredential } from '@crowd/connectors'
import { createSessionizeSessionHttp } from '@crowd/connectors/src/connectors/sessionize/sessionHttp'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { SnowflakeClient } from '@crowd/snowflake'

import { diffAgainstBaseline, formatDifference, parseBaseline } from '../sessionizeBaselineDiff'
import { computeSessionizeDecisions } from '../sessionizeEventMatching'

const log = getServiceLogger()

function parseArgs(rawArgv: string[]): { baselinePath: string } {
  const argv = rawArgv.filter((arg) => arg !== '--')
  const flagIndex = argv.indexOf('--baseline')
  const baselinePath = flagIndex === -1 ? undefined : argv[flagIndex + 1]
  if (!baselinePath || baselinePath.startsWith('--') || argv.length !== 2) {
    log.error('Usage: dry-run-sessionize-event-matching --baseline <csv>')
    process.exit(1)
  }
  return { baselinePath }
}

function loadCredential(): ReturnType<typeof getPlatformCredential> {
  try {
    return getPlatformCredential('sessionize')
  } catch (err) {
    log.error({ err }, 'The sessionize organizer credential is not configured')
    process.exit(1)
  }
}

async function main(): Promise<void> {
  const { baselinePath } = parseArgs(process.argv.slice(2))
  const baselineRows = parseBaseline(readFileSync(baselinePath, 'utf8'))
  const credential = loadCredential()
  const qx = pgpQx(await getDbConnection(WRITE_DB_CONFIG()))

  const snowflake = SnowflakeClient.fromEnv({ parentLog: log, maxConnections: 1 })
  try {
    const decisions = await computeSessionizeDecisions({
      http: createSessionizeSessionHttp(credential),
      catalogue: snowflake,
      qx,
      log,
    })
    const heldByReason = new Map<string, number>()
    for (const decision of decisions) {
      if (decision.holdReason) {
        heldByReason.set(decision.holdReason, (heldByReason.get(decision.holdReason) ?? 0) + 1)
      }
    }
    const matched = decisions.filter((decision) => decision.status === 'matched').length
    const baselineDirect = baselineRows.filter((row) => row.class === 'direct').length
    const heldText = [...heldByReason].map(([reason, count]) => `${reason}=${count}`).join(' ')

    console.log(`listed=${decisions.length} matched=${matched} held: ${heldText || 'none'}`)
    console.log(`baseline direct=${baselineDirect} of ${baselineRows.length}`)

    const diff = diffAgainstBaseline(decisions, baselineRows)
    console.log(`\nDifferences (${diff.differences.length})`)
    diff.differences.forEach((difference) => console.log(formatDifference(difference)))
    console.log(`\nIn the baseline but not listed now (${diff.missingNow.length})`)
    diff.missingNow.forEach((key) => console.log(key))
    console.log(`\nListed now but not in the baseline (${diff.newNow.length})`)
    diff.newNow.forEach((key) => console.log(key))
  } finally {
    await snowflake.destroy()
  }
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    log.error({ err }, 'sessionize event matching dry run failed')
    process.exit(1)
  })
