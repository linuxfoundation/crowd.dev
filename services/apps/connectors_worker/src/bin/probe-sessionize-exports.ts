import { timeout } from '@crowd/common'
import { getPlatformCredential } from '@crowd/connectors'
import type { ConnectorHttp } from '@crowd/connectors'
import {
  downloadSessionizeExport,
  readExportSheet,
} from '@crowd/connectors/src/connectors/sessionize/exportFile'
import { signInToSessionize } from '@crowd/connectors/src/connectors/sessionize/session'
import { createSessionizeSessionHttp } from '@crowd/connectors/src/connectors/sessionize/sessionHttp'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { listSessionizeEventMappings } from '@crowd/data-access-layer/src/sessionize'
import { getServiceLogger } from '@crowd/logging'

import {
  ExportProbeArgs,
  ExportProbeFailure,
  ExportProbeSummary,
  PROBE_RUN_START,
  describeProbeFailure,
  exportPath,
  nextProbeRunState,
  parseExportProbeArgs,
  summarizeExport,
} from '../sessionizeExportProbe'

const log = getServiceLogger()

const USAGE =
  'Usage: probe-sessionize-exports [--event <key>]... [--path <template with {eventKey}>] [--date-header <name>]... [--pace-ms <n>]'

type ProbeLine =
  | { eventKey: string; outcome: 'xlsx'; summary: ExportProbeSummary }
  | ({ eventKey: string; outcome: 'failed' } & ExportProbeFailure)

function parseArgsOrExit(): ExportProbeArgs {
  try {
    return parseExportProbeArgs(process.argv.slice(2))
  } catch (err) {
    log.error({ errMsg: err instanceof Error ? err.message : String(err) }, USAGE)
    process.exit(1)
  }
}

function loadCredential(): ReturnType<typeof getPlatformCredential> {
  try {
    return getPlatformCredential('sessionize')
  } catch (err) {
    log.error({ err }, 'The sessionize organizer credential is not configured')
    process.exit(1)
  }
}

async function matchedEventKeys(): Promise<string[]> {
  const qx = pgpQx(await getDbConnection(WRITE_DB_CONFIG()))
  const mappings = await listSessionizeEventMappings(qx)
  return mappings
    .filter((mapping) => mapping.status === 'matched' && mapping.deactivatedAt === null)
    .map((mapping) => mapping.eventKey)
}

async function eventKeysOrExit(args: ExportProbeArgs): Promise<string[]> {
  if (args.eventKeys.length > 0) {
    return args.eventKeys
  }
  try {
    return await matchedEventKeys()
  } catch (err) {
    log.error({ err }, 'Could not list the matched sessionize events')
    process.exit(1)
  }
}

async function signInFailure(
  credential: ReturnType<typeof getPlatformCredential>,
): Promise<ExportProbeFailure | null> {
  try {
    await signInToSessionize(credential)
    return null
  } catch (err) {
    return describeProbeFailure(err)
  }
}

async function probeEvent(
  http: ConnectorHttp,
  args: ExportProbeArgs,
  eventKey: string,
): Promise<ProbeLine> {
  try {
    const file = await downloadSessionizeExport(http, exportPath(args.pathTemplate, eventKey))
    const sheet = await readExportSheet(file)
    return { eventKey, outcome: 'xlsx', summary: summarizeExport(sheet, args.dateHeaders) }
  } catch (err) {
    return { eventKey, outcome: 'failed', ...describeProbeFailure(err) }
  }
}

function printLine(line: object): void {
  process.stdout.write(`${JSON.stringify(line)}\n`)
}

async function main(): Promise<boolean> {
  const args = parseArgsOrExit()
  const credential = loadCredential()
  const eventKeys = await eventKeysOrExit(args)
  const totals: Record<ProbeLine['outcome'], number> = { xlsx: 0, failed: 0 }

  const failedSignIn = await signInFailure(credential)
  if (failedSignIn) {
    log.error(failedSignIn, 'sessionize sign-in failed, no export was requested')
    printLine({ totals: { ...totals, notProbed: eventKeys.length }, signIn: failedSignIn })
    return false
  }
  log.info({ events: eventKeys.length, pathTemplate: args.pathTemplate }, 'probing exports')

  const http = createSessionizeSessionHttp(credential)
  let runState = PROBE_RUN_START
  for (const [index, eventKey] of eventKeys.entries()) {
    if (index > 0) {
      await timeout(args.paceMs)
    }
    const line = await probeEvent(http, args, eventKey)
    totals[line.outcome] += 1
    printLine(line)
    const failureClass = line.outcome === 'failed' ? line.errorClass : undefined
    runState = nextProbeRunState(runState, failureClass)
    if (runState.stop) {
      const notProbed = eventKeys.length - index - 1
      log.error({ errorClass: failureClass, notProbed }, 'stopping the probe run')
      printLine({ totals: { ...totals, notProbed } })
      return false
    }
  }
  printLine({ totals: { ...totals, notProbed: 0 } })
  return totals.failed === 0
}

main()
  .then((allSucceeded) => process.exit(allSucceeded ? 0 : 1))
  .catch((err) => {
    log.error(describeProbeFailure(err), 'sessionize export probe failed')
    process.exit(1)
  })
