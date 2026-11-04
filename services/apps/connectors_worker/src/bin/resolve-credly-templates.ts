import { writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { DataSinkWorkerEmitter } from '@crowd/common_services'
import {
  type Credential,
  assertCredentialKind,
  createHttpClient,
  getCredential,
} from '@crowd/connectors'
import { createCredlyDeps } from '@crowd/connectors/src/connectors/credly/deps'
import { credlyActivitySchema } from '@crowd/connectors/src/connectors/credly/mapper'
import { credlyTemplateOverrides } from '@crowd/connectors/src/connectors/credly/mapping/overrides'
import { resolveTemplates } from '@crowd/connectors/src/connectors/credly/mapping/resolver'
import {
  CREDLY_POOL_ENTRY_ID,
  mintCredlyAccessToken,
} from '@crowd/connectors/src/connectors/credly/oauth'
import { drainCredlyReleases } from '@crowd/connectors/src/connectors/credly/syncs/badgePoll'
import { createEmit } from '@crowd/connectors/src/emit'
import { findSyncUnitByChannelName } from '@crowd/data-access-layer/src/connectors'
import { recordShadowRecords } from '@crowd/data-access-layer/src/connectors/shadowRecords'
import {
  listActiveCredlyTemplateMappings,
  listCredlyTemplateMappings,
  listHeldCredlyTemplateMappings,
} from '@crowd/data-access-layer/src/credly'
import {
  listMappedCredlyTemplatesWithoutRelease,
  queueCredlyTemplateRelease,
} from '@crowd/data-access-layer/src/credly/releases'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import IntegrationStreamRepository from '@crowd/data-access-layer/src/old/apps/integration_stream_worker/integrationStream.repo'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { DbStore } from '@crowd/database'
import { getServiceLogger } from '@crowd/logging'
import { QUEUE_CONFIG, QueueFactory } from '@crowd/queue'
import { SnowflakeClient } from '@crowd/snowflake'

import { activeHeld, buildHeldQueueReport, postHeldQueueReport } from '../credlyHeldQueueReport'
import {
  CREDLY_HISTORY_START,
  applyDecisions,
  buildPlan,
  loadCatalogue,
  loadCredlyTemplates,
  loadSubprojectSegments,
  releasesToQueue,
  unreleasedTemplatesToQueue,
} from '../credlyTemplateResolve'

const log = getServiceLogger()
const HELD_QUEUE_PRINTED = 20
const DEFAULT_DRAIN_PACING_MS = 2_000

function usage(): never {
  log.error(
    'Usage: resolve-credly-templates --integration-id <uuid> [--apply [--notify | --digest]] [--out <file>] | --release <badge template id> | --release-unreleased --integration-id <uuid> | --drain-releases --integration-id <uuid> [--pacing-ms <n>]',
  )
  process.exit(1)
}

// Re-mints on invalidation instead of capturing one token for the process lifetime: the drain
// command can run long enough (paced release completions, a large backlog) to outlive a token's TTL.
function credlyHttpClient(credential: Credential): ReturnType<typeof createHttpClient> {
  let token: string | null = null
  return createHttpClient({
    acquireToken: async () => {
      if (!token) ({ token } = await mintCredlyAccessToken(credential))
      return { id: CREDLY_POOL_ENTRY_ID, value: token }
    },
    parkToken: async () => undefined,
    invalidateToken: async () => {
      token = null
    },
    log,
  })
}

function takeFlag(argv: string[], flag: string): string | undefined {
  const flagIndex = argv.indexOf(flag)
  if (flagIndex === -1) return undefined
  const value = argv[flagIndex + 1]
  if (value === undefined || value.startsWith('--')) usage()
  argv.splice(flagIndex, 2)
  return value
}

function takeSwitch(argv: string[], flag: string): boolean {
  const flagIndex = argv.indexOf(flag)
  if (flagIndex === -1) return false
  argv.splice(flagIndex, 1)
  return true
}

interface Args {
  release?: string
  releaseUnreleased: boolean
  drainReleases: boolean
  pacingMs: number
  integrationId: string
  apply: boolean
  notify: 'changes' | 'digest' | null
  outFile: string
}

function parseArgs(rawArgv: string[]): Args {
  const argv = rawArgv.filter((arg) => arg !== '--')
  const release = takeFlag(argv, '--release')
  if (release) {
    if (argv.length > 0) usage()
    return {
      release,
      releaseUnreleased: false,
      drainReleases: false,
      pacingMs: DEFAULT_DRAIN_PACING_MS,
      integrationId: '',
      apply: false,
      notify: null,
      outFile: '',
    }
  }
  const releaseUnreleased = takeSwitch(argv, '--release-unreleased')
  if (releaseUnreleased) {
    const integrationId = takeFlag(argv, '--integration-id')
    if (!integrationId || argv.length > 0) usage()
    return {
      releaseUnreleased,
      drainReleases: false,
      pacingMs: DEFAULT_DRAIN_PACING_MS,
      integrationId,
      apply: false,
      notify: null,
      outFile: '',
    }
  }
  const drainReleases = takeSwitch(argv, '--drain-releases')
  if (drainReleases) {
    const integrationId = takeFlag(argv, '--integration-id')
    const pacingMsRaw = takeFlag(argv, '--pacing-ms')
    const pacingMs = pacingMsRaw ? Number(pacingMsRaw) : DEFAULT_DRAIN_PACING_MS
    if (!integrationId || !Number.isFinite(pacingMs) || pacingMs < 0 || argv.length > 0) usage()
    return {
      drainReleases,
      pacingMs,
      releaseUnreleased: false,
      integrationId,
      apply: false,
      notify: null,
      outFile: '',
    }
  }
  const integrationId = takeFlag(argv, '--integration-id')
  const apply = takeSwitch(argv, '--apply')
  const digest = takeSwitch(argv, '--digest')
  const notify = digest ? 'digest' : takeSwitch(argv, '--notify') ? 'changes' : null
  if (notify && !apply) usage()
  const outFile =
    takeFlag(argv, '--out') ??
    join(tmpdir(), `credly-resolve-${new Date().toISOString().replace(/[:.]/g, '-')}.json`)
  if (!integrationId || argv.length > 0) usage()
  return {
    releaseUnreleased: false,
    drainReleases: false,
    pacingMs: DEFAULT_DRAIN_PACING_MS,
    integrationId,
    apply,
    notify,
    outFile,
  }
}

async function main(): Promise<void> {
  const {
    release,
    releaseUnreleased,
    drainReleases,
    pacingMs,
    integrationId,
    apply,
    notify,
    outFile,
  } = parseArgs(process.argv.slice(2))
  const db = await getDbConnection(WRITE_DB_CONFIG())
  const qx = pgpQx(db)

  if (drainReleases) {
    const credential = await getCredential(qx, integrationId)
    assertCredentialKind(credential, 'credly-oauth-client')
    const { organizationId } = credential.data
    const http = credlyHttpClient(credential)

    const unit = await findSyncUnitByChannelName(
      qx,
      integrationId,
      'credly',
      organizationId,
      'badgePoll',
    )
    if (!unit) {
      log.error(
        { integrationId, organizationId },
        'no credly badgePoll sync unit found for this integration',
      )
      process.exit(1)
    }

    const queueClient = QueueFactory.createQueueService(QUEUE_CONFIG())
    const sinkEmitter = new DataSinkWorkerEmitter(queueClient, log)
    await sinkEmitter.init()
    try {
      const streamRepo = new IntegrationStreamRepository(new DbStore(log, db), log)
      const emitter = createEmit({
        publishResult: streamRepo.publishExternalResult.bind(streamRepo),
        sinkEmitter,
        recordShadow: (records) => recordShadowRecords(qx, unit.id, records),
        unit,
        segmentId: null,
        emitsOwnSegment: true,
        schema: credlyActivitySchema,
        log,
      })

      let stopped = false
      const stop = (signal: string) => {
        log.info({ signal }, 'credly release drain stopping, finishing the current page')
        stopped = true
      }
      process.once('SIGINT', () => stop('SIGINT'))
      process.once('SIGTERM', () => stop('SIGTERM'))

      const result = await drainCredlyReleases(
        {
          channel: { channelId: unit.channelId, channelName: unit.channelName },
          integrationId,
          watermark: null,
          emitEnabled: unit.emitEnabled,
          emit: emitter.emit,
          commitWatermark: async () => undefined,
          hasRunBudget: () => !stopped,
          http,
          log,
          getCredential: async () => credential,
          deps: createCredlyDeps({ qx, integrationId }),
        },
        { pacingMs, shouldStop: () => stopped },
      )
      log.info({ ...result, emitted: emitter.emittedCount() }, 'credly release drain finished')
    } finally {
      await queueClient.disconnectProducer()
    }
    return
  }

  if (release) {
    const active = await listActiveCredlyTemplateMappings(qx)
    if (!active.some((mapping) => mapping.badgeTemplateId === release)) {
      log.error(
        { badgeTemplateId: release },
        'credly template is not mapped to a live integration, nothing to release',
      )
      process.exit(1)
    }
    await queueCredlyTemplateRelease(qx, {
      badgeTemplateId: release,
      floor: CREDLY_HISTORY_START,
      top: new Date().toISOString(),
    })
    log.info({ badgeTemplateId: release }, 'credly template history queued for release')
    return
  }

  if (releaseUnreleased) {
    const credential = await getCredential(qx, integrationId)
    assertCredentialKind(credential, 'credly-oauth-client')
    const http = credlyHttpClient(credential)
    const [listing, unreleased] = await Promise.all([
      loadCredlyTemplates(http, credential.data.organizationId, log),
      listMappedCredlyTemplatesWithoutRelease(qx),
    ])
    if (!listing.complete) {
      log.warn(
        'the credly listing was not complete; templates missing from it fall back to the full history floor',
      )
    }
    const releases = unreleasedTemplatesToQueue(listing.templates, unreleased, new Date())
    for (const queued of releases) {
      await queueCredlyTemplateRelease(qx, queued)
    }
    log.info(
      { queued: releases.length, lightest: releases[0], heaviest: releases[releases.length - 1] },
      'credly mapped templates without a release were queued, lightest first',
    )
    return
  }

  const credential = await getCredential(qx, integrationId)
  assertCredentialKind(credential, 'credly-oauth-client')
  const http = credlyHttpClient(credential)

  const snowflake = SnowflakeClient.fromEnv({ parentLog: log, maxConnections: 3 })
  try {
    const [listing, catalogue, segmentIdBySlug, stored] = await Promise.all([
      loadCredlyTemplates(http, credential.data.organizationId, log),
      loadCatalogue(snowflake),
      loadSubprojectSegments(qx, log),
      listCredlyTemplateMappings(qx),
    ])
    const { templates } = listing
    log.info(
      {
        templates: templates.length,
        listingComplete: listing.complete,
        courses: catalogue.courses.length,
        courseGroups: catalogue.courseGroups.length,
        enrollmentCodes: catalogue.enrollments.length,
        segments: segmentIdBySlug.size,
        storedMappings: stored.length,
      },
      'credly resolve inputs loaded',
    )

    const decisions = resolveTemplates({
      templates,
      catalogue,
      segmentIdBySlug,
      overrides: credlyTemplateOverrides,
    })
    const plan = buildPlan(decisions, stored)
    const byStatus = { mapped: 0, held: 0, excluded: 0 }
    const byHoldReason: Record<string, number> = {}
    for (const decision of decisions) {
      byStatus[decision.status] += 1
      if (decision.holdReason) {
        byHoldReason[decision.holdReason] = (byHoldReason[decision.holdReason] ?? 0) + 1
      }
    }
    log.info({ ...byStatus, byHoldReason, plan: plan.counts }, 'credly resolve plan')

    writeFileSync(outFile, JSON.stringify({ decisions, plan }, null, 2))

    if (!apply) {
      const held = decisions
        .filter((decision) => decision.status === 'held')
        .sort((a, b) => (b.badgeCount ?? 0) - (a.badgeCount ?? 0))
        .slice(0, HELD_QUEUE_PRINTED)
        .map((d) => ({
          id: d.badgeTemplateId,
          name: d.badgeTemplateName,
          badges: d.badgeCount,
          reason: d.holdReason,
          slug: d.projectSlug,
        }))
      log.info({ held, outFile }, 'dry run: nothing written, top of the held queue')
      return
    }

    const missing = plan.entries.filter((entry) => entry.kind === 'missing')
    if (!listing.complete && missing.length > 0) {
      log.warn(
        { missing: missing.length },
        'the credly listing changed while paging: templates not seen are left as they are',
      )
    }
    const result = await applyDecisions(
      qx,
      decisions,
      listing.complete ? missing : [],
      log,
      releasesToQueue(plan, templates, new Date()),
    )
    const heldQueue = await listHeldCredlyTemplateMappings(qx)
    log.info(
      {
        ...result,
        heldQueue: heldQueue.length,
        topHeld: heldQueue.slice(0, HELD_QUEUE_PRINTED),
        outFile,
      },
      'credly resolve applied',
    )

    if (notify) {
      const report = buildHeldQueueReport(
        activeHeld(stored),
        heldQueue,
        plan,
        new Map(templates.map((template) => [template.id, template.url ?? null])),
      )
      if (notify === 'digest' || report.changed) {
        const posted = await postHeldQueueReport(report)
        if (!posted) {
          process.exitCode = 1
          log.error({ title: report.title }, 'credly held queue report was not posted to slack')
        } else {
          log.info(
            { title: report.title, changed: report.changed },
            'credly held queue report posted',
          )
        }
      } else {
        log.info({ title: report.title }, 'credly held queue unchanged, nothing posted')
      }
    }
  } finally {
    await snowflake.destroy()
  }
}

main()
  .then(() => process.exit(process.exitCode ?? 0))
  .catch((err) => {
    log.error({ err }, 'credly resolve failed')
    process.exit(1)
  })
