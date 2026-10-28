import { writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { assertCredentialKind, createHttpClient, getCredential } from '@crowd/connectors'
import { credlyTemplateOverrides } from '@crowd/connectors/src/connectors/credly/mapping/overrides'
import { resolveTemplates } from '@crowd/connectors/src/connectors/credly/mapping/resolver'
import {
  CREDLY_POOL_ENTRY_ID,
  mintCredlyAccessToken,
} from '@crowd/connectors/src/connectors/credly/oauth'
import {
  listCredlyTemplateMappings,
  listHeldCredlyTemplateMappings,
} from '@crowd/data-access-layer/src/credly'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { SnowflakeClient } from '@crowd/snowflake'

import { activeHeld, buildHeldQueueReport, postHeldQueueReport } from '../credlyHeldQueueReport'
import {
  applyDecisions,
  buildPlan,
  loadCatalogue,
  loadCredlyTemplates,
  loadSubprojectSegments,
} from '../credlyTemplateResolve'

const log = getServiceLogger()
const HELD_QUEUE_PRINTED = 20

function usage(): never {
  log.error(
    'Usage: resolve-credly-templates --integration-id <uuid> [--apply [--notify | --digest]] [--out <file>]',
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

function takeSwitch(argv: string[], flag: string): boolean {
  const flagIndex = argv.indexOf(flag)
  if (flagIndex === -1) return false
  argv.splice(flagIndex, 1)
  return true
}

interface Args {
  integrationId: string
  apply: boolean
  notify: 'changes' | 'digest' | null
  outFile: string
}

function parseArgs(rawArgv: string[]): Args {
  const argv = rawArgv.filter((arg) => arg !== '--')
  const integrationId = takeFlag(argv, '--integration-id')
  const apply = takeSwitch(argv, '--apply')
  const digest = takeSwitch(argv, '--digest')
  const notify = digest ? 'digest' : takeSwitch(argv, '--notify') ? 'changes' : null
  if (notify && !apply) usage()
  const outFile =
    takeFlag(argv, '--out') ??
    join(tmpdir(), `credly-resolve-${new Date().toISOString().replace(/[:.]/g, '-')}.json`)
  if (!integrationId || argv.length > 0) usage()
  return { integrationId, apply, notify, outFile }
}

async function main(): Promise<void> {
  const { integrationId, apply, notify, outFile } = parseArgs(process.argv.slice(2))
  const db = await getDbConnection(WRITE_DB_CONFIG())
  const qx = pgpQx(db)

  const credential = await getCredential(qx, integrationId)
  assertCredentialKind(credential, 'credly-oauth-client')
  const { token } = await mintCredlyAccessToken(credential)
  const http = createHttpClient({
    acquireToken: async () => ({ id: CREDLY_POOL_ENTRY_ID, value: token }),
    parkToken: async () => undefined,
    invalidateToken: async () => undefined,
    log,
  })

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
    const result = await applyDecisions(qx, decisions, listing.complete ? missing : [], log)
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
