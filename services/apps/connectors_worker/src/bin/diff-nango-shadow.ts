import { readFileSync } from 'node:fs'

import { getServiceLogger } from '@crowd/logging'
import { NangoIntegration, getNangoCloudRecords, initNangoCloudClient } from '@crowd/nango'
import type { INangoRecord } from '@crowd/nango'

import { fetchNangoRecordsInWindow } from '../nangoWindowFetch'

const log = getServiceLogger()

function usage(): never {
  log.error(
    'Usage: diff-nango-shadow --connection-id <id> --model <NangoModel> --shadow-csv <path> --window-start <iso> --window-end <iso>',
  )
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

function parseArgs(rawArgv: string[]): {
  connectionId: string
  model: string
  shadowCsvPath: string
  windowStart: Date
  windowEnd: Date
} {
  const argv = rawArgv.filter((arg) => arg !== '--')
  const connectionId = takeFlag(argv, '--connection-id')
  const model = takeFlag(argv, '--model')
  const shadowCsvPath = takeFlag(argv, '--shadow-csv')
  const windowStartArg = takeFlag(argv, '--window-start')
  const windowEndArg = takeFlag(argv, '--window-end')
  if (!connectionId || !model || !shadowCsvPath || !windowStartArg || !windowEndArg) {
    usage()
  }
  const windowStart = new Date(windowStartArg)
  const windowEnd = new Date(windowEndArg)
  if (Number.isNaN(windowStart.getTime()) || Number.isNaN(windowEnd.getTime())) {
    usage()
  }
  return { connectionId, model, shadowCsvPath, windowStart, windowEnd }
}

function readShadowSourceIds(csvPath: string): Set<string> {
  const lines = readFileSync(csvPath, 'utf8').split('\n').slice(1)
  return new Set(
    lines.map((line) => line.split(',')[0]?.trim()).filter((id): id is string => Boolean(id)),
  )
}

setImmediate(async () => {
  try {
    const { connectionId, model, shadowCsvPath, windowStart, windowEnd } = parseArgs(
      process.argv.slice(2),
    )

    const shadowIds = readShadowSourceIds(shadowCsvPath)
    log.info({ shadowIds: shadowIds.size }, 'loaded shadow sourceIds')

    await initNangoCloudClient()

    const nangoInWindow = await fetchNangoRecordsInWindow<INangoRecord>(
      (cursor) =>
        getNangoCloudRecords(
          NangoIntegration.GITHUB,
          connectionId,
          model,
          cursor,
          undefined,
          windowStart.toISOString(),
        ),
      windowStart,
      windowEnd,
    )
    log.info({ nangoInWindow: nangoInWindow.length }, 'fetched nango records in window')

    const missingInShadow = nangoInWindow.filter((record) => {
      const activity = record.activity as Record<string, unknown> | null | undefined
      const sourceId = activity?.sourceId
      return typeof sourceId === 'string' && !shadowIds.has(sourceId)
    })

    log.info({ missingInShadow: missingInShadow.length }, 'diffed nango against shadow')
    for (const record of missingInShadow) {
      const activity = record.activity as Record<string, unknown>
      process.stdout.write(
        `${activity.sourceId}\t${new Date(record.timestamp).toISOString()}\tlastModifiedAt=${record.metadata.lastModifiedAt}\n`,
      )
    }

    process.exit(0)
  } catch (err) {
    log.error(err, 'diff-nango-shadow failed')
    process.exit(1)
  }
})
