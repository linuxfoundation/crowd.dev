import { WorkflowIdReusePolicy } from '@temporalio/client'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'
import { TEMPORAL_CONFIG, getTemporalClient } from '@crowd/temporal'

const log = getServiceChildLogger('recalculate-all-affiliations')

interface BrokenMember {
  memberId: string
  activeOrgIds: string[]
  staleOrgIds: string[]
}

interface ScriptOptions {
  pageSize: number
  concurrency: number
  pageDelayMs: number
  workflowDelayMs: number
  startAfter: string | null
  dryRun: boolean
  limit: number | null
  maxPages: number | null
  emptyPageDelayMs: number | null
}

function parseArgs(): ScriptOptions {
  const args = process.argv.slice(2)

  const getArg = (flag: string): string | undefined => {
    const idx = args.indexOf(flag)
    if (idx !== -1 && idx + 1 < args.length) return args[idx + 1]
    return undefined
  }

  const pageSize = parseInt(getArg('--page-size') ?? '100', 10)
  const concurrency = parseInt(getArg('--concurrency') ?? '20', 10)
  const pageDelayMs = parseInt(getArg('--page-delay') ?? '2000', 10)
  const workflowDelayMs = parseInt(getArg('--workflow-delay') ?? '0', 10)
  const startAfter = getArg('--start-after') ?? null
  const dryRun = args.includes('--dry-run')
  const emptyPageDelayRaw = getArg('--empty-page-delay')
  const emptyPageDelayMs = emptyPageDelayRaw !== undefined ? parseInt(emptyPageDelayRaw, 10) : null
  const limitRaw = getArg('--limit')
  const limit = limitRaw !== undefined ? parseInt(limitRaw, 10) : null
  const maxPagesRaw = getArg('--max-pages')
  const maxPages = maxPagesRaw !== undefined ? parseInt(maxPagesRaw, 10) : null

  if (isNaN(pageSize) || pageSize <= 0) {
    log.error('--page-size must be a positive integer')
    process.exit(1)
  }
  if (isNaN(concurrency) || concurrency <= 0) {
    log.error('--concurrency must be a positive integer')
    process.exit(1)
  }
  if (isNaN(pageDelayMs) || pageDelayMs < 0) {
    log.error('--page-delay must be a non-negative integer')
    process.exit(1)
  }
  if (isNaN(workflowDelayMs) || workflowDelayMs < 0) {
    log.error('--workflow-delay must be a non-negative integer')
    process.exit(1)
  }
  if (emptyPageDelayMs !== null && (isNaN(emptyPageDelayMs) || emptyPageDelayMs < 0)) {
    log.error('--empty-page-delay must be a non-negative integer')
    process.exit(1)
  }
  if (limit !== null && (isNaN(limit) || limit <= 0)) {
    log.error('--limit must be a positive integer')
    process.exit(1)
  }
  if (maxPages !== null && (isNaN(maxPages) || maxPages <= 0)) {
    log.error('--max-pages must be a positive integer')
    process.exit(1)
  }

  return {
    pageSize,
    concurrency,
    pageDelayMs,
    workflowDelayMs,
    startAfter,
    dryRun,
    limit,
    maxPages,
    emptyPageDelayMs,
  }
}

async function fetchMemberIdPage(
  qx: ReturnType<typeof pgpQx>,
  afterMemberId: string | null,
  pageSize: number,
): Promise<string[]> {
  const cursorClause = afterMemberId ? `AND "memberId" > $(afterMemberId)` : ''

  const rows = await qx.select(
    `
    SELECT "memberId"
    FROM "memberOrganizations"
    WHERE TRUE ${cursorClause}
    GROUP BY "memberId"
    ORDER BY "memberId"
    LIMIT $(pageSize)
    `,
    { afterMemberId, pageSize },
  )

  return rows.map((r: Record<string, unknown>) => r.memberId as string)
}

async function findBrokenMembers(
  qx: ReturnType<typeof pgpQx>,
  memberIds: string[],
): Promise<BrokenMember[]> {
  // Deduplicate to (memberId, organizationId) pairs first — a member may have thousands
  // of activities but only a handful of distinct org attributions.
  const staleRows = await qx.select(
    `
    WITH pairs AS (
      SELECT DISTINCT "memberId", "organizationId"
      FROM "activityRelations"
      WHERE "memberId" = ANY($(memberIds)::uuid[])
        AND "organizationId" IS NOT NULL
    )
    SELECT p."memberId", p."organizationId" AS "staleOrgId"
    FROM pairs p
    WHERE NOT EXISTS (
      SELECT 1 FROM "memberOrganizations" mo
      WHERE mo."memberId" = p."memberId"
        AND mo."organizationId" = p."organizationId"
        AND mo."deletedAt" IS NULL
    )
    `,
    { memberIds },
  )

  if (staleRows.length === 0) {
    return []
  }

  const staleMap = new Map<string, string[]>()
  for (const r of staleRows as Record<string, string>[]) {
    const existing = staleMap.get(r.memberId) ?? []
    existing.push(r.staleOrgId)
    staleMap.set(r.memberId, existing)
  }

  const brokenMemberIds = [...staleMap.keys()]

  const orgRows = await qx.select(
    `
    SELECT "memberId", array_agg(DISTINCT "organizationId") AS "activeOrgIds"
    FROM "memberOrganizations"
    WHERE "memberId" = ANY($(brokenMemberIds)::uuid[])
      AND "deletedAt" IS NULL
    GROUP BY "memberId"
    `,
    { brokenMemberIds },
  )

  const activeOrgMap = new Map<string, string[]>(
    orgRows.map((r: Record<string, unknown>) => [
      r.memberId as string,
      (r.activeOrgIds as string[] | null) ?? [],
    ]),
  )

  return brokenMemberIds.map((memberId: string) => ({
    memberId,
    activeOrgIds: activeOrgMap.get(memberId) ?? [],
    staleOrgIds: staleMap.get(memberId) ?? [],
  }))
}

async function main() {
  const opts = parseArgs()

  log.info(
    {
      pageSize: opts.pageSize,
      concurrency: opts.concurrency,
      pageDelayMs: opts.pageDelayMs,
      workflowDelayMs: opts.workflowDelayMs,
      startAfter: opts.startAfter,
      dryRun: opts.dryRun,
      limit: opts.limit,
      maxPages: opts.maxPages,
      emptyPageDelayMs: opts.emptyPageDelayMs,
    },
    'Starting recalculate-all-affiliations',
  )

  const dbConnection = await getDbConnection(WRITE_DB_CONFIG())
  const qx = pgpQx(dbConnection)
  const temporal = await getTemporalClient(TEMPORAL_CONFIG())

  let cursor: string | null = opts.startAfter
  let pageNum = 0
  let totalScanned = 0
  let totalBroken = 0
  let totalSucceeded = 0
  let totalFailed = 0

  let hasMore = true
  while (hasMore) {
    pageNum++

    const memberIds = await fetchMemberIdPage(qx, cursor, opts.pageSize)

    if (memberIds.length === 0) {
      log.info('No more members to process.')
      hasMore = false
      continue
    }

    const lastMemberId = memberIds[memberIds.length - 1]
    totalScanned += memberIds.length

    const brokenMembers = await findBrokenMembers(qx, memberIds)
    totalBroken += brokenMembers.length

    log.info(
      `Page ${pageNum}: scanned ${memberIds.length} | broken: ${brokenMembers.length} | cursor: ${lastMemberId}`,
    )

    if (brokenMembers.length > 0) {
      if (opts.dryRun) {
        const loggedSoFar = totalBroken - brokenMembers.length
        const remaining = opts.limit !== null ? opts.limit - loggedSoFar : brokenMembers.length
        const toLog = brokenMembers.slice(0, remaining)
        for (const { memberId, activeOrgIds, staleOrgIds } of toLog) {
          log.info(
            `[DRY RUN] broken member: ${memberId} | stale orgs: [${staleOrgIds.join(', ')}] | active orgs: ${activeOrgIds.length}`,
          )
        }
        if (opts.limit !== null && loggedSoFar + toLog.length >= opts.limit) {
          log.info(`Limit of ${opts.limit} members reached.`)
          hasMore = false
          cursor = lastMemberId
          continue
        }
      } else {
        const triggeredSoFar = totalSucceeded + totalFailed
        const remaining = opts.limit !== null ? opts.limit - triggeredSoFar : brokenMembers.length
        if (remaining <= 0) {
          log.info(`Limit of ${opts.limit} workflows reached.`)
          hasMore = false
          continue
        }

        const toProcess = brokenMembers.slice(0, remaining)

        let index = 0
        const worker = async () => {
          while (index < toProcess.length) {
            const { memberId, activeOrgIds, staleOrgIds } = toProcess[index++]
            try {
              log.info(
                `Triggering memberUpdate: ${memberId} | stale orgs: [${staleOrgIds.join(', ')}] | active orgs: ${activeOrgIds.length}`,
              )
              await temporal.workflow.start('memberUpdate', {
                taskQueue: 'profiles',
                workflowId: `member-update/${DEFAULT_TENANT_ID}/${memberId}`,
                workflowIdReusePolicy:
                  WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
                retry: { maximumAttempts: 10 },
                args: [
                  {
                    member: { id: memberId },
                    memberOrganizationIds: activeOrgIds,
                    syncToOpensearch: true,
                  },
                ],
                searchAttributes: { TenantId: [DEFAULT_TENANT_ID] },
              })
              if (opts.workflowDelayMs > 0) {
                await new Promise((resolve) => setTimeout(resolve, opts.workflowDelayMs))
              }
              totalSucceeded++
            } catch (err) {
              totalFailed++
              log.error({ err }, 'Failed to process member')
            }
          }
        }

        await Promise.all(
          Array.from({ length: Math.min(opts.concurrency, toProcess.length) }, worker),
        )
        log.info(`Page ${pageNum} done: ${totalSucceeded} ok, ${totalFailed} failed`)

        if (opts.limit !== null && totalSucceeded + totalFailed >= opts.limit) {
          log.info(`Limit of ${opts.limit} workflows reached.`)
          hasMore = false
          continue
        }
      }
    }

    log.info(`Resume with: --start-after ${lastMemberId}`)
    cursor = lastMemberId

    if (memberIds.length < opts.pageSize) {
      hasMore = false
    }

    if (opts.maxPages !== null && pageNum >= opts.maxPages) {
      log.info(`Max pages of ${opts.maxPages} reached.`)
      hasMore = false
    }

    const delayMs =
      brokenMembers.length === 0 && opts.emptyPageDelayMs !== null
        ? opts.emptyPageDelayMs
        : opts.pageDelayMs
    if (delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, delayMs))
    }
  }

  const brokenPct = totalScanned > 0 ? ((totalBroken / totalScanned) * 100).toFixed(2) : '0.00'
  log.info(
    {
      pagesProcessed: pageNum,
      membersScanned: totalScanned,
      membersBroken: `${totalBroken} (${brokenPct}%)`,
      ...(opts.dryRun ? {} : { workflowsSucceeded: totalSucceeded, workflowsFailed: totalFailed }),
    },
    'Done',
  )

  process.exit(totalFailed > 0 ? 1 : 0)
}

main().catch((err) => {
  log.error({ err }, 'Unexpected error')
  process.exit(1)
})
