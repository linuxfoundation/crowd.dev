import { appendFile, mkdir, readFile, readdir, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

import { timeout } from '@crowd/common'
import { getPlatformCredential } from '@crowd/connectors'
import type { ConnectorHttp } from '@crowd/connectors'
import {
  downloadSessionizeExport,
  readExportSheet,
} from '@crowd/connectors/src/connectors/sessionize/exportFile'
import { signInToSessionize } from '@crowd/connectors/src/connectors/sessionize/session'
import { createSessionizeFixedSessionHttp } from '@crowd/connectors/src/connectors/sessionize/sessionHttp'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { listSessionizeEventMappings } from '@crowd/data-access-layer/src/sessionize'
import { getServiceLogger } from '@crowd/logging'

import {
  DownloadResultLine,
  ExportDownloadArgs,
  ExportProbeArgs,
  ExportProbeFailure,
  ExportSummaryArgs,
  PROBE_RUN_START,
  SummaryResultLine,
  describeProbeFailure,
  downloadTotalsLine,
  eventKeysInDirectory,
  exportFileName,
  exportPath,
  failedLine,
  nextProbeRunState,
  parseExportProbeArgs,
  savedLine,
  summarizeExport,
  summarizedLine,
  summaryTotalsLine,
} from '../sessionizeExportProbe'

const log = getServiceLogger()

const EXPORT_MAX_ATTEMPTS = 2
const RESULTS_FILE = 'results.jsonl'
const SUMMARY_FILE = 'summary.jsonl'

const USAGE = [
  'Usage: probe-sessionize-exports --out-dir <dir> [--event <key>]... [--path <template with {eventKey}>] [--pace-ms <n>]',
  '       probe-sessionize-exports --from-dir <dir> [--date-header <name>]...',
].join('\n')

function parseArgs(): ExportProbeArgs | null {
  try {
    return parseExportProbeArgs(process.argv.slice(2))
  } catch (err) {
    log.error({ errMsg: err instanceof Error ? err.message : String(err) }, USAGE)
    return null
  }
}

function loadCredential(): ReturnType<typeof getPlatformCredential> | null {
  try {
    return getPlatformCredential('sessionize')
  } catch (err) {
    log.error({ err }, 'The sessionize organizer credential is not configured')
    return null
  }
}

async function matchedEventKeys(): Promise<string[]> {
  const qx = pgpQx(await getDbConnection(WRITE_DB_CONFIG()))
  const mappings = await listSessionizeEventMappings(qx)
  return mappings
    .filter((mapping) => mapping.status === 'matched' && mapping.deactivatedAt === null)
    .map((mapping) => mapping.eventKey)
}

async function listEventKeys(args: ExportDownloadArgs): Promise<string[] | null> {
  if (args.eventKeys.length > 0) {
    return args.eventKeys
  }
  try {
    return await matchedEventKeys()
  } catch (err) {
    log.error({ err }, 'Could not list the matched sessionize events')
    return null
  }
}

async function signInOnce(
  credential: ReturnType<typeof getPlatformCredential>,
): Promise<{ cookie: string } | { failure: ExportProbeFailure }> {
  try {
    return { cookie: (await signInToSessionize(credential, 1)).token }
  } catch (err) {
    return { failure: describeProbeFailure(err) }
  }
}

async function appendLine(file: string, line: object): Promise<void> {
  await appendFile(file, `${JSON.stringify(line)}\n`, { mode: 0o600 })
}

async function saveExport(
  http: ConnectorHttp,
  args: ExportDownloadArgs,
  eventKey: string,
): Promise<DownloadResultLine> {
  try {
    const file = await downloadSessionizeExport(
      http,
      exportPath(args.pathTemplate, eventKey),
      EXPORT_MAX_ATTEMPTS,
    )
    await writeFile(join(args.outDir, exportFileName(eventKey)), file, { mode: 0o600 })
    return savedLine(eventKey, file.length)
  } catch (err) {
    return failedLine(eventKey, err)
  }
}

async function download(args: ExportDownloadArgs): Promise<number> {
  const credential = loadCredential()
  if (!credential) {
    return 1
  }
  const eventKeys = await listEventKeys(args)
  if (!eventKeys) {
    return 1
  }
  await mkdir(args.outDir, { recursive: true, mode: 0o700 })
  const resultsFile = join(args.outDir, RESULTS_FILE)

  const session = await signInOnce(credential)
  if ('failure' in session) {
    log.error(session.failure, 'sessionize sign-in failed, no export was requested')
    await appendLine(resultsFile, downloadTotalsLine([], eventKeys.length, session.failure))
    return 1
  }
  log.info({ events: eventKeys.length, pathTemplate: args.pathTemplate }, 'downloading exports')

  const http = createSessionizeFixedSessionHttp(session.cookie)
  const lines: DownloadResultLine[] = []
  let runState = PROBE_RUN_START
  for (const [index, eventKey] of eventKeys.entries()) {
    if (index > 0) {
      await timeout(args.paceMs)
    }
    const line = await saveExport(http, args, eventKey)
    lines.push(line)
    await appendLine(resultsFile, line)
    log.info(
      { eventKey, outcome: line.outcome, bytes: line.outcome === 'saved' ? line.bytes : undefined },
      'export probed',
    )
    const failureClass = line.outcome === 'failed' ? line.errorClass : undefined
    runState = nextProbeRunState(runState, failureClass)
    if (runState.stop) {
      const notProbed = eventKeys.length - index - 1
      log.error({ errorClass: failureClass, notProbed }, 'stopping the probe run')
      await appendLine(resultsFile, downloadTotalsLine(lines, notProbed))
      return 1
    }
  }
  const totals = downloadTotalsLine(lines, 0)
  await appendLine(resultsFile, totals)
  return totals.totals.failed === 0 ? 0 : 1
}

async function summarizeFile(
  args: ExportSummaryArgs,
  eventKey: string,
): Promise<SummaryResultLine> {
  try {
    const sheet = await readExportSheet(
      await readFile(join(args.fromDir, exportFileName(eventKey))),
    )
    return summarizedLine(eventKey, summarizeExport(sheet, args.dateHeaders))
  } catch (err) {
    return failedLine(eventKey, err)
  }
}

async function summarize(args: ExportSummaryArgs): Promise<number> {
  const summaryFile = join(args.fromDir, SUMMARY_FILE)
  const lines: SummaryResultLine[] = []
  for (const eventKey of eventKeysInDirectory(await readdir(args.fromDir))) {
    const line = await summarizeFile(args, eventKey)
    lines.push(line)
    await appendLine(summaryFile, line)
  }
  const totals = summaryTotalsLine(lines)
  await appendLine(summaryFile, totals)
  log.info(totals, 'exports summarized')
  return totals.totals.failed === 0 ? 0 : 1
}

async function main(): Promise<number> {
  const args = parseArgs()
  if (!args) {
    return 1
  }
  return args.mode === 'download' ? download(args) : summarize(args)
}

main()
  .catch((err) => {
    log.error(describeProbeFailure(err), 'sessionize export probe failed')
    return 1
  })
  .then((code) => process.exit(code))
