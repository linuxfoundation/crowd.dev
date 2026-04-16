/**
 * Recalculate Enrichment Affiliations Script
 *
 * PROBLEM:
 * Before fix CM-1118, the members_enrichment_worker modified work experiences
 * (memberOrganizations) without triggering affiliation recalculation. As a result,
 * activityRelations.organizationId may be stale for members whose work experiences
 * were created, updated, or deleted by enrichment.
 *
 * SOLUTION:
 * This script finds all members with enrichment-sourced work experiences and triggers
 * a memberUpdate Temporal workflow for each, which recalculates affiliations and syncs
 * to OpenSearch.
 *
 * Usage:
 *   # Via package.json script (recommended):
 *   pnpm run recalculate-enrichment-affiliations -- [options]
 *
 *   # Or directly with tsx:
 *   npx tsx src/bin/recalculate-enrichment-affiliations.ts [options]
 *
 * Options:
 *   --page-size <n>      Number of members to fetch per DB page (default: 1000)
 *   --concurrency <n>    Max concurrent Temporal workflow starts per page (default: 20)
 *   --page-delay <ms>    Milliseconds to wait between pages (default: 5000)
 *   --start-after <id>   Resume from a specific memberId (exclusive, for restarts)
 *   --dry-run            Log what would be processed without starting workflows
 *   --limit <n>          Stop after processing at most N members total (for testing)
 *   --workflow-delay <ms> Milliseconds to wait after each workflow start, to avoid overwhelming Temporal (default: 0)
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

const ENRICHMENT_SOURCES = ['enrichment-progai', 'enrichment-clearbit', 'enrichment-crustdata']

const log = getServiceChildLogger('recalculate-enrichment-affiliations')

interface MemberWithOrgs {
  memberId: string
  activeOrgIds: string[]
  deletedOrgCount: number
  activeOrgCount: number
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

  const pageSize = parseInt(getArg('--page-size') ?? '1000', 10)
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

async function fetchPage(
  qx: ReturnType<typeof pgpQx>,
  afterMemberId: string | null,
  pageSize: number,
): Promise<MemberWithOrgs[]> {
  const cursorClause = afterMemberId ? `AND "memberId" > $(afterMemberId)` : ''

  // Selects only members that have at least one work experience created by enrichment
  // (source IN enrichment-progai, enrichment-clearbit, enrichment-crustdata).
  // activeOrgIds contains only the active enrichment-sourced org IDs for that member —
  // used by the memberUpdate workflow to determine which orgs to sync to OpenSearch.
  // deletedOrgCount and activeOrgCount are used for impact logging only.
  const rows = await qx.select(
    `
    SELECT
      "memberId",
      array_agg(DISTINCT "organizationId") FILTER (WHERE "deletedAt" IS NULL) AS "activeOrgIds",
      COUNT(*) FILTER (WHERE "deletedAt" IS NOT NULL) AS "deletedOrgCount",
      COUNT(*) FILTER (WHERE "deletedAt" IS NULL)     AS "activeOrgCount"
    FROM "memberOrganizations"
    WHERE source = ANY($(sources))
    ${cursorClause}
    GROUP BY "memberId"
    ORDER BY "memberId"
    LIMIT $(pageSize)
    `,
    { sources: ENRICHMENT_SOURCES, afterMemberId, pageSize },
  )

  return rows.map((r: Record<string, unknown>) => ({
    memberId: r.memberId,
    activeOrgIds: (r.activeOrgIds as string[] | null) ?? [],
    deletedOrgCount: Number(r.deletedOrgCount),
    activeOrgCount: Number(r.activeOrgCount),
  })) as MemberWithOrgs[]
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
  log.info('Recalculate Enrichment Affiliations Script')
  log.info('='.repeat(80))
  log.info(`Page size:   ${opts.pageSize}`)
  log.info(`Concurrency: ${opts.concurrency}`)
  log.info(`Page delay:      ${opts.pageDelayMs}ms`)
  log.info(`Workflow delay:  ${opts.workflowDelayMs}ms`)
  log.info(`Start after:     ${opts.startAfter ?? '(beginning)'}`)
  log.info(`Mode:            ${opts.dryRun ? 'DRY RUN' : 'LIVE'}`)
  log.info(`Limit:       ${opts.limit ?? '(none)'}`)
  log.info('='.repeat(80))

  // Init DB
  const dbConnection = await getDbConnection(WRITE_DB_CONFIG())
  const qx = pgpQx(dbConnection)

  // Init Temporal
  const temporal = await getTemporalClient(TEMPORAL_CONFIG())

  let cursor: string | null = opts.startAfter
  let pageNum = 0
  let totalSucceeded = 0
  let totalFailed = 0
  let totalProcessed = 0
  let totalActiveOrgs = 0
  let totalDeletedOrgs = 0
  let totalMembersWithDeletedOrgs = 0

  let hasMore = true
  while (hasMore) {
    pageNum++

    const remaining = opts.limit !== null ? opts.limit - totalProcessed : opts.pageSize
    if (remaining <= 0) {
      log.info(`Limit of ${opts.limit} members reached.`)
      hasMore = false
      continue
    }

    const fetchSize = Math.min(opts.pageSize, remaining)
    const membersPage = await fetchPage(qx, cursor, fetchSize)

    if (membersPage.length === 0) {
      log.info('No more members to process.')
      hasMore = false
      continue
    }

    const lastMemberId = membersPage[membersPage.length - 1].memberId
    const pageActiveOrgs = membersPage.reduce((sum, m) => sum + m.activeOrgCount, 0)
    const pageDeletedOrgs = membersPage.reduce((sum, m) => sum + m.deletedOrgCount, 0)
    const membersWithDeletedOrgs = membersPage.filter((m) => m.deletedOrgCount > 0).length
    totalActiveOrgs += pageActiveOrgs
    totalDeletedOrgs += pageDeletedOrgs
    totalMembersWithDeletedOrgs += membersWithDeletedOrgs
    log.info(
      `Page ${pageNum}: ${membersPage.length} members | active orgs: ${pageActiveOrgs} | deleted orgs: ${pageDeletedOrgs} (${membersWithDeletedOrgs} members affected) | cursor: ${lastMemberId}`,
    )

    if (opts.dryRun) {
      log.info(`[DRY RUN] Would trigger ${membersPage.length} workflows`)
      for (const { memberId, activeOrgIds, deletedOrgCount } of membersPage) {
        log.info(
          `[DRY RUN] memberUpdate | memberId: ${memberId} | activeOrgs: ${activeOrgIds.length} | deletedOrgs: ${deletedOrgCount}`,
        )
      }
      totalProcessed += membersPage.length
    } else {
      const { succeeded, failed } = await runWithConcurrency(
        membersPage,
        opts.concurrency,
        async ({ memberId, activeOrgIds, deletedOrgCount }) => {
          log.debug(
            { memberId, activeOrgs: activeOrgIds.length, deletedOrgs: deletedOrgCount },
            'Triggering memberUpdate workflow',
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
        },
      )

      totalSucceeded += succeeded
      totalFailed += failed
      totalProcessed += succeeded + failed
      log.info(`Page ${pageNum} done: ${succeeded} ok, ${failed} failed`)
    }

    log.info(`Resume with: --start-after ${lastMemberId}`)
    cursor = lastMemberId

    if (membersPage.length < fetchSize) {
      // Last page (fewer results than requested means no more data)
      hasMore = false
    }

    if (opts.pageDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, opts.pageDelayMs))
    }
  }

  log.info('='.repeat(80))
  log.info('Summary')
  log.info('='.repeat(80))
  log.info(`Pages processed:              ${pageNum}`)
  log.info(
    `Members with active orgs:     ${totalProcessed} (active enrichment orgs: ${totalActiveOrgs})`,
  )
  log.info(
    `Members with deleted orgs:    ${totalMembersWithDeletedOrgs} (deleted enrichment orgs: ${totalDeletedOrgs})`,
  )
  if (!opts.dryRun) {
    log.info(`Workflows succeeded:          ${totalSucceeded}`)
    log.info(`Workflows failed:             ${totalFailed}`)
  }

  process.exit(totalFailed > 0 ? 1 : 0)
}

main().catch((err) => {
  log.error({ err }, 'Unexpected error')
  process.exit(1)
})
