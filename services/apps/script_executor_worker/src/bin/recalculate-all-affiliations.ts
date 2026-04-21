/**
 * Recalculate All Affiliations Script
 *
 * PROBLEM:
 * activityRelations.organizationId may be stale for members who have activities
 * attributed to an organization they no longer have an active work experience for
 * (no matching memberOrganizations row with deletedAt IS NULL). This can happen
 * regardless of the source of the work experience change (enrichment, manual, etc.).
 *
 * SOLUTION:
 * Paginates over distinct memberIds from memberOrganizations. For each batch,
 * detects members with stale org attributions by checking activityRelations via
 * ix_activityRelations_memberId (no full table scan). Triggers a memberUpdate
 * Temporal workflow only for affected members.
 *
 * Usage:
 *   pnpm run recalculate-all-affiliations -- [options]
 *   npx tsx src/bin/recalculate-all-affiliations.ts [options]
 *
 * Options:
 *   --page-size <n>       Number of memberIds per page (default: 100)
 *                         Kept small intentionally: each page triggers an
 *                         activityRelations lookup per member via memberId index.
 *   --concurrency <n>     Max concurrent Temporal workflow starts per page (default: 20)
 *   --page-delay <ms>     Milliseconds to wait between pages (default: 5000)
 *   --start-after <id>    Resume from a specific memberId (exclusive, for restarts)
 *   --dry-run             Log what would be processed without starting workflows
 *   --limit <n>           Stop after triggering at most N workflows (for testing)
 *   --workflow-delay <ms> Milliseconds to wait after each workflow start (default: 0)
 *
 * Environment Variables Required:
 *   CROWD_DB_WRITE_HOST        - Postgres write host
 *   CROWD_DB_PORT              - Postgres port
 *   CROWD_DB_USERNAME          - Postgres username
 *   CROWD_DB_PASSWORD          - Postgres password
 *   CROWD_DB_DATABASE          - Postgres database name
 *   CROWD_TEMPORAL_SERVER_URL  - Temporal server URL
 *   CROWD_TEMPORAL_NAMESPACE   - Temporal namespace
 *   SERVICE                    - Service identifier (used by Temporal client)
 */
import { WorkflowIdReusePolicy } from '@temporalio/client'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'
import { TEMPORAL_CONFIG, getTemporalClient } from '@crowd/temporal'

const log = getServiceChildLogger('recalculate-all-affiliations')

interface MemberWithActiveOrgs {
  memberId: string
  activeOrgIds: string[]
}

interface ScriptOptions {
  pageSize: number
  concurrency: number
  pageDelayMs: number
  workflowDelayMs: number
  startAfter: string | null
  dryRun: boolean
  limit: number | null
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
  const pageDelayMs = parseInt(getArg('--page-delay') ?? '5000', 10)
  const workflowDelayMs = parseInt(getArg('--workflow-delay') ?? '0', 10)
  const startAfter = getArg('--start-after') ?? null
  const dryRun = args.includes('--dry-run')
  const limitRaw = getArg('--limit')
  const limit = limitRaw !== undefined ? parseInt(limitRaw, 10) : null

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
  if (limit !== null && (isNaN(limit) || limit <= 0)) {
    log.error('--limit must be a positive integer')
    process.exit(1)
  }

  return { pageSize, concurrency, pageDelayMs, workflowDelayMs, startAfter, dryRun, limit }
}

// Returns a page of distinct memberIds from memberOrganizations, cursor-based.
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

// Finds members in the batch whose activityRelations.organizationId is stale —
// i.e. attributed to an org they no longer have an active memberOrganization for.
// Uses ix_activityRelations_memberId for the activityRelations lookup (no seq scan).
async function findBrokenMembers(
  qx: ReturnType<typeof pgpQx>,
  memberIds: string[],
): Promise<MemberWithActiveOrgs[]> {
  // First reduce to distinct (memberId, organizationId) pairs — a member may have
  // thousands of activities but only a handful of distinct org attributions.
  // The NOT EXISTS then runs on the small deduplicated set, not on every activity row.
  const brokenRows = await qx.select(
    `
    WITH pairs AS (
      SELECT DISTINCT "memberId", "organizationId"
      FROM "activityRelations"
      WHERE "memberId" = ANY($(memberIds)::uuid[])
        AND "organizationId" IS NOT NULL
    )
    SELECT DISTINCT p."memberId"
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

  if (brokenRows.length === 0) {
    return []
  }

  const brokenMemberIds = brokenRows.map((r: Record<string, unknown>) => r.memberId as string)

  // Fetch currently active org IDs to pass to memberUpdate
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

  const orgMap = new Map<string, string[]>(
    orgRows.map((r: Record<string, unknown>) => [
      r.memberId as string,
      (r.activeOrgIds as string[] | null) ?? [],
    ]),
  )

  return brokenMemberIds.map((memberId: string) => ({
    memberId,
    activeOrgIds: orgMap.get(memberId) ?? [],
  }))
}

async function runWithConcurrency<T>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<void>,
): Promise<{ succeeded: number; failed: number }> {
  let succeeded = 0
  let failed = 0
  let index = 0

  async function worker() {
    while (index < items.length) {
      const item = items[index++]
      try {
        await fn(item)
        succeeded++
      } catch (err) {
        failed++
        log.error({ err }, 'Failed to process member')
      }
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, worker)
  await Promise.all(workers)

  return { succeeded, failed }
}

async function main() {
  const opts = parseArgs()

  log.info('='.repeat(80))
  log.info('Recalculate All Affiliations Script')
  log.info('='.repeat(80))
  log.info(`Page size:       ${opts.pageSize}`)
  log.info(`Concurrency:     ${opts.concurrency}`)
  log.info(`Page delay:      ${opts.pageDelayMs}ms`)
  log.info(`Workflow delay:  ${opts.workflowDelayMs}ms`)
  log.info(`Start after:     ${opts.startAfter ?? '(beginning)'}`)
  log.info(`Mode:            ${opts.dryRun ? 'DRY RUN' : 'LIVE'}`)
  log.info(`Limit:           ${opts.limit ?? '(none)'}`)
  log.info('='.repeat(80))

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
        for (const { memberId, activeOrgIds } of toLog) {
          log.info(
            `[DRY RUN] memberUpdate | memberId: ${memberId} | activeOrgs: ${activeOrgIds.length}`,
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
        const { succeeded, failed } = await runWithConcurrency(
          toProcess,
          opts.concurrency,
          async ({ memberId, activeOrgIds }) => {
            log.info({ memberId, activeOrgs: activeOrgIds.length }, 'Triggering memberUpdate workflow')
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
          },
        )

        totalSucceeded += succeeded
        totalFailed += failed
        log.info(`Page ${pageNum} done: ${succeeded} ok, ${failed} failed`)

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

    if (opts.pageDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, opts.pageDelayMs))
    }
  }

  log.info('='.repeat(80))
  log.info('Summary')
  log.info('='.repeat(80))
  const brokenPct = totalScanned > 0 ? ((totalBroken / totalScanned) * 100).toFixed(2) : '0.00'
  log.info(`Pages processed:     ${pageNum}`)
  log.info(`Members scanned:     ${totalScanned}`)
  log.info(`Members broken:      ${totalBroken} (${brokenPct}%)`)
  if (!opts.dryRun) {
    log.info(`Workflows succeeded: ${totalSucceeded}`)
    log.info(`Workflows failed:    ${totalFailed}`)
  }

  process.exit(totalFailed > 0 ? 1 : 0)
}

main().catch((err) => {
  log.error({ err }, 'Unexpected error')
  process.exit(1)
})
