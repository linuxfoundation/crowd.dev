import { writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import type { Credential } from '@crowd/connectors'
import { assertCredentialKind, createHttpClient } from '@crowd/connectors'
import type { CredlyDeps } from '@crowd/connectors/src/connectors/credly/deps'
import {
  CREDLY_POOL_ENTRY_ID,
  mintCredlyAccessToken,
} from '@crowd/connectors/src/connectors/credly/oauth'
import { runBadgePoll } from '@crowd/connectors/src/connectors/credly/syncs/badgePoll'
import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

interface DryRunOptions {
  badgeTemplateId: string
  segmentId: string
  integrationId: string
  since?: string
  maxRequests: number
  maxActivities: number
  outFile: string
}

function usage(): never {
  log.error(
    'Usage: dry-run-credly-badge-poll --template <badge template id> --segment-id <uuid> --integration-id <uuid> [--since <iso>] [--max-requests <n>] [--max-activities <n>] [--out <file>]',
  )
  process.exit(1)
}

function takeFlag(argv: string[], flag: string): string | undefined {
  const flagIndex = argv.indexOf(flag)
  if (flagIndex === -1) return undefined
  const value = argv[flagIndex + 1]
  if (value === undefined || value.startsWith('--')) usage()
  argv.splice(flagIndex, 2)
  return value
}

function positiveInt(raw: string | undefined, fallback: number): number {
  if (raw === undefined) return fallback
  const value = Number(raw)
  if (!Number.isInteger(value) || value < 1) usage()
  return value
}

function parseArgs(rawArgv: string[]): DryRunOptions {
  const argv = rawArgv.filter((arg) => arg !== '--')
  const badgeTemplateId = takeFlag(argv, '--template')
  const segmentId = takeFlag(argv, '--segment-id')
  const integrationId = takeFlag(argv, '--integration-id')
  const since = takeFlag(argv, '--since')
  const maxRequests = positiveInt(takeFlag(argv, '--max-requests'), 1)
  const maxActivities = positiveInt(takeFlag(argv, '--max-activities'), 10)
  const outFile =
    takeFlag(argv, '--out') ??
    join(tmpdir(), `credly-dry-run-${new Date().toISOString().replace(/[:.]/g, '-')}.json`)
  if (
    !badgeTemplateId ||
    !segmentId ||
    !integrationId ||
    argv.length > 0 ||
    (since && !Number.isFinite(Date.parse(since)))
  ) {
    usage()
  }
  return { badgeTemplateId, segmentId, integrationId, since, maxRequests, maxActivities, outFile }
}

function credentialFromEnv(): Credential {
  const clientId = process.env.CROWD_CREDLY_CLIENT_ID
  const clientSecret = process.env.CROWD_CREDLY_CLIENT_SECRET
  const organizationId = process.env.CROWD_CREDLY_ORGANIZATION_ID
  if (!clientId || !clientSecret || !organizationId) {
    log.error(
      'set CROWD_CREDLY_CLIENT_ID, CROWD_CREDLY_CLIENT_SECRET and CROWD_CREDLY_ORGANIZATION_ID first',
    )
    process.exit(1)
  }
  return {
    platform: 'credly',
    kind: 'credly-oauth-client',
    data: { clientId, clientSecret, organizationId },
  }
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2))
  const credential = credentialFromEnv()
  assertCredentialKind(credential, 'credly-oauth-client')

  const { token, expiresAt } = await mintCredlyAccessToken(credential)
  log.info({ expiresAt }, 'oauth token exchange ok')

  const http = createHttpClient({
    acquireToken: async () => ({ id: CREDLY_POOL_ENTRY_ID, value: token }),
    parkToken: async () => undefined,
    invalidateToken: async () => undefined,
    log,
  })

  const watermarks: Record<string, unknown>[] = []
  const emits: { count: number; records: unknown[] }[] = []
  let activitiesEmitted = 0
  const deps: CredlyDeps = {
    segmentId: options.segmentId,
    findBadges: async () => [],
    saveBadges: async () => undefined,
    listDueExpiries: async () => [],
    markExpiriesEmitted: async () => 0,
  }

  const outcome = await runBadgePoll({
    channel: { channelId: options.badgeTemplateId, channelName: options.badgeTemplateId },
    integrationId: options.integrationId,
    watermark: options.since ? { since: new Date(options.since).toISOString() } : null,
    emitEnabled: false,
    emit: async (records) => {
      const keep = Math.max(0, options.maxActivities - activitiesEmitted)
      activitiesEmitted += records.length
      emits.push({ count: records.length, records: records.slice(0, keep) })
      log.info({ records: records.length, kept: Math.min(keep, records.length) }, 'emit')
    },
    commitWatermark: async (watermark) => {
      watermarks.push(watermark)
      log.info(watermark, 'commitWatermark')
    },
    hasRunBudget: () =>
      http.requestCount() < options.maxRequests && activitiesEmitted < options.maxActivities,
    http,
    log,
    getCredential: async () => credential,
    deps,
  })

  writeFileSync(
    options.outFile,
    JSON.stringify(
      { options, outcome, requests: http.requestCount(), activitiesEmitted, watermarks, emits },
      null,
      2,
    ),
  )
  log.info(
    {
      outcome,
      requests: http.requestCount(),
      activitiesEmitted,
      activitiesSaved: Math.min(activitiesEmitted, options.maxActivities),
      lastWatermark: watermarks[watermarks.length - 1] ?? null,
      outFile: options.outFile,
    },
    'dry run complete',
  )
}

main().catch((err) => {
  log.error({ err }, 'credly dry run failed')
  process.exit(1)
})
