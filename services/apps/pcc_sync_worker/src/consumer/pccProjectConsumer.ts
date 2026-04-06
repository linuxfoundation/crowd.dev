/**
 * PCC project consumer: polls snowflakeExportJobs for platform='pcc' jobs,
 * streams each Parquet file, runs the matching cascade, and writes to DB.
 *
 * One DB transaction per job — all segment + insightsProject writes roll back
 * together on any failure. Errors that can't be auto-resolved are written to
 * pcc_projects_sync_errors for manual review.
 */
import { DEFAULT_TENANT_ID } from '@crowd/common'
import { DbConnOrTx, DbConnection, WRITE_DB_CONFIG, getDbConnection } from '@crowd/database'
import { getServiceChildLogger } from '@crowd/logging'
import { MetadataStore, S3Service, SnowflakeExportJob } from '@crowd/snowflake'

import { parsePccRow } from '../parser'
import type { CdpHierarchyTarget, ParsedPccProject } from '../parser'

const log = getServiceChildLogger('pccProjectConsumer')

const PLATFORM = 'pcc'
const MAX_POLLING_INTERVAL_MS = 30 * 60 * 1000 // 30 minutes

// ─────────────────────────────────────────────────────────────────────────────
// Consumer loop
// ─────────────────────────────────────────────────────────────────────────────

export class PccProjectConsumer {
  private running = false
  private currentPollingIntervalMs: number

  constructor(
    private readonly metadataStore: MetadataStore,
    private readonly s3Service: S3Service,
    private readonly db: DbConnection,
    private readonly pollingIntervalMs: number,
    readonly dryRun: boolean = false,
  ) {
    this.currentPollingIntervalMs = pollingIntervalMs
  }

  async start(): Promise<void> {
    this.running = true
    log.info({ dryRun: this.dryRun }, 'PCC project consumer started')

    while (this.running) {
      try {
        const job = await this.metadataStore.claimOldestPendingJob(PLATFORM)

        if (job) {
          this.currentPollingIntervalMs = this.pollingIntervalMs
          await this.processJob(job)
          await new Promise<void>((resolve) => setImmediate(resolve))
          continue
        }
      } catch (err) {
        log.error({ err }, 'Error in consumer loop')
        await this.sleep(this.pollingIntervalMs)
        continue
      }

      log.info({ currentPollingIntervalMs: this.currentPollingIntervalMs }, 'No pending PCC jobs')
      await this.sleep(this.currentPollingIntervalMs)
      this.currentPollingIntervalMs = Math.min(
        this.currentPollingIntervalMs * 2,
        MAX_POLLING_INTERVAL_MS,
      )
    }

    log.info('PCC project consumer stopped')
  }

  stop(): void {
    this.running = false
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Job processing
  // ─────────────────────────────────────────────────────────────────────────

  private async processJob(job: SnowflakeExportJob): Promise<void> {
    log.info({ jobId: job.id, s3Path: job.s3Path, dryRun: this.dryRun }, 'Processing PCC job')

    const startTime = Date.now()
    let upsertedCount = 0
    let skippedCount = 0
    let mismatchCount = 0
    let errorCount = 0

    try {
      await this.db.tx(async (tx) => {
        for await (const raw of this.s3Service.streamParquetRows(job.s3Path)) {
          const parsed = parsePccRow(raw)

          if (parsed.ok === false) {
            errorCount++
            log.warn({ jobId: job.id, details: parsed.details }, 'Row schema mismatch — skipping')
            if (!this.dryRun) {
              await insertSyncError(tx, null, null, 'SCHEMA_MISMATCH', parsed.details)
            }
            continue
          }

          const { project } = parsed
          const result = await this.processRow(tx, project)

          switch (result.action) {
            case 'UPSERTED':
              upsertedCount++
              break
            case 'SKIPPED':
              skippedCount++
              break
            case 'MISMATCH':
              mismatchCount++
              if (!this.dryRun) {
                await insertSyncError(
                  tx,
                  project.pccProjectId,
                  project.pccSlug,
                  'HIERARCHY_MISMATCH',
                  result.details,
                )
              }
              break
          }
        }
      })

      const metrics = { upsertedCount, skippedCount, mismatchCount, errorCount }
      log.info({ jobId: job.id, ...metrics, dryRun: this.dryRun }, 'PCC job completed')

      await this.metadataStore.markCompleted(job.id, {
        transformedCount: upsertedCount,
        skippedCount: skippedCount + mismatchCount + errorCount,
        processingDurationMs: Date.now() - startTime,
      })
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : String(err)
      log.error({ jobId: job.id, err }, 'PCC job failed')

      try {
        await this.metadataStore.markFailed(job.id, errorMessage, {
          processingDurationMs: Date.now() - startTime,
        })
      } catch (updateErr) {
        log.error({ jobId: job.id, updateErr }, 'Failed to mark job as failed')
      }
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Per-row matching cascade + writes
  // ─────────────────────────────────────────────────────────────────────────

  private async processRow(
    tx: DbConnOrTx,
    project: ParsedPccProject,
  ): Promise<
    | { action: 'UPSERTED' }
    | { action: 'SKIPPED' }
    | { action: 'MISMATCH'; details: Record<string, unknown> }
  > {
    // Step 1: segment_id from Snowflake ACTIVE_SEGMENTS JOIN
    let segment = project.segmentIdFromSnowflake
      ? await findSegmentById(tx, project.segmentIdFromSnowflake)
      : null

    // Step 2: sourceId fallback
    if (!segment) {
      segment = await findSegmentBySourceId(tx, project.pccProjectId)
    }

    // Step 3: derived slug match
    if (!segment && project.pccSlug) {
      segment = await findSegmentBySlug(tx, project.pccSlug)
    }

    // Step 4: no match → SKIP (Phase 1: project doesn't exist in CDP yet)
    if (!segment) {
      return { action: 'SKIPPED' }
    }

    // Hierarchy mismatch check: segment was matched but parent/group differs
    const mismatchFields = detectHierarchyMismatch(segment, project.cdpTarget)
    if (mismatchFields.length > 0) {
      return {
        action: 'MISMATCH',
        details: {
          segmentId: segment.id,
          segmentName: segment.name,
          pccProjectId: project.pccProjectId,
          mismatchFields,
          cdpTarget: project.cdpTarget,
          currentHierarchy: {
            group: segment.grandparentName ?? segment.parentName ?? segment.name,
            project: segment.parentName ?? segment.name,
            subproject: segment.name,
          },
        },
      }
    }

    if (!this.dryRun) {
      await upsertSegment(tx, segment.id, project)
      const nameConflict = await upsertInsightsProject(tx, segment.id, project)
      if (nameConflict) {
        log.warn(
          { segmentId: segment.id, name: project.name },
          'insightsProject name conflict — segment synced, insights project skipped',
        )
        await insertSyncError(tx, project.pccProjectId, project.pccSlug, 'INSIGHTS_NAME_CONFLICT', {
          segmentId: segment.id,
          name: project.name,
        })
      }
    } else {
      log.info(
        {
          segmentId: segment.id,
          pccProjectId: project.pccProjectId,
          name: project.name,
          status: project.status,
          maturity: project.maturity,
        },
        '[dry-run] Would upsert segment',
      )
    }

    return { action: 'UPSERTED' }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms))
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// DB helpers
// ─────────────────────────────────────────────────────────────────────────────

interface SegmentRow {
  id: string
  name: string
  parentName: string | null
  grandparentName: string | null
}

async function findSegmentById(db: DbConnOrTx, segmentId: string): Promise<SegmentRow | null> {
  return db.oneOrNone<SegmentRow>(
    `SELECT id, name, "parentName", "grandparentName"
     FROM segments
     WHERE id = $(segmentId) AND type = 'subproject' AND "tenantId" = $(tenantId)`,
    { segmentId, tenantId: DEFAULT_TENANT_ID },
  )
}

async function findSegmentBySourceId(db: DbConnOrTx, sourceId: string): Promise<SegmentRow | null> {
  return db.oneOrNone<SegmentRow>(
    `SELECT id, name, "parentName", "grandparentName"
     FROM segments
     WHERE "sourceId" = $(sourceId) AND type = 'subproject' AND "tenantId" = $(tenantId)`,
    { sourceId, tenantId: DEFAULT_TENANT_ID },
  )
}

async function findSegmentBySlug(db: DbConnOrTx, slug: string): Promise<SegmentRow | null> {
  const rows = await db.manyOrNone<SegmentRow>(
    `SELECT id, name, "parentName", "grandparentName"
     FROM segments
     WHERE slug = $(slug) AND type = 'subproject' AND "tenantId" = $(tenantId)`,
    { slug, tenantId: DEFAULT_TENANT_ID },
  )
  if (rows.length === 1) return rows[0]
  if (rows.length > 1) {
    log.warn({ slug, count: rows.length }, 'Ambiguous slug match — skipping')
  }
  return null
}

function detectHierarchyMismatch(segment: SegmentRow, cdpTarget: CdpHierarchyTarget): string[] {
  // Only check structural hierarchy (parent/grandparent placement), not the leaf name.
  // The leaf name is a metadata field we're here to sync — a difference there is an UPDATE,
  // not a mismatch. Mismatches indicate the project is in the wrong place in the hierarchy,
  // which requires manual review before auto-fixing (per Phase 1 spec).
  const mismatches: string[] = []
  if (segment.grandparentName && segment.grandparentName !== cdpTarget.group) {
    mismatches.push('group_name')
  }
  if (segment.parentName && segment.parentName !== cdpTarget.project) {
    mismatches.push('project_name')
  }
  return mismatches
}

async function upsertSegment(
  db: DbConnOrTx,
  segmentId: string,
  project: ParsedPccProject,
): Promise<void> {
  await db.none(
    `UPDATE segments
     SET name        = $(name),
         status      = $(status)::"segmentsStatus_type",
         maturity    = $(maturity),
         description = $(description),
         "updatedAt" = NOW()
     WHERE id = $(segmentId) AND "tenantId" = $(tenantId)`,
    {
      segmentId,
      name: project.name,
      status: project.status ?? 'active',
      maturity: project.maturity,
      description: project.description,
      tenantId: DEFAULT_TENANT_ID,
    },
  )
}

// Returns true if a name conflict prevented creating the insightsProject row.
async function upsertInsightsProject(
  db: DbConnOrTx,
  segmentId: string,
  project: ParsedPccProject,
): Promise<boolean> {
  // Partial unique index on segmentId WHERE deletedAt IS NULL means
  // ON CONFLICT won't fire for soft-deleted rows. Use UPDATE-then-INSERT.
  // Slug is intentionally not updated on name changes — it is a stable identifier
  // referenced by FK from securityInsightsEvaluations and related tables.
  // Guard the UPDATE against the partial unique index on (name) WHERE deletedAt IS NULL.
  // If another active row already holds the new name, the NOT EXISTS subquery causes the
  // UPDATE to match 0 rows instead of throwing a 23505 unique violation.
  const updated = await db.result(
    `UPDATE "insightsProjects"
     SET name        = $(name),
         description = $(description),
         "logoUrl"   = $(logoUrl),
         "updatedAt" = NOW()
     WHERE "segmentId" = $(segmentId) AND "deletedAt" IS NULL
       AND NOT EXISTS (
         SELECT 1 FROM "insightsProjects"
         WHERE name = $(name) AND "deletedAt" IS NULL AND "segmentId" != $(segmentId)
       )`,
    { segmentId, name: project.name, description: project.description, logoUrl: project.logoUrl },
  )

  if (updated.rowCount === 0) {
    // Either (a) no active row exists yet → proceed to INSERT,
    // or (b) a row exists but its name collides with another segment → return conflict.
    const exists = await db.oneOrNone<{ id: string }>(
      `SELECT id FROM "insightsProjects" WHERE "segmentId" = $(segmentId) AND "deletedAt" IS NULL`,
      { segmentId },
    )
    if (exists) return true

    const inserted = await db.result(
      `INSERT INTO "insightsProjects" (name, slug, description, "segmentId", "logoUrl", "isLF")
       VALUES ($(name), generate_slug('insightsProjects', $(name)), $(description), $(segmentId), $(logoUrl), TRUE)
       ON CONFLICT (name) WHERE "deletedAt" IS NULL DO NOTHING`,
      { name: project.name, description: project.description, segmentId, logoUrl: project.logoUrl },
    )
    if (inserted.rowCount === 0) return true
  }

  return false
}

async function insertSyncError(
  db: DbConnOrTx,
  externalProjectId: string | null,
  externalProjectSlug: string | null,
  errorType: string,
  details: Record<string, unknown>,
): Promise<void> {
  await db.none(
    `INSERT INTO pcc_projects_sync_errors
       (external_project_id, external_project_slug, error_type, details)
     VALUES ($(externalProjectId), $(externalProjectSlug), $(errorType), $(details)::jsonb)
     ON CONFLICT (external_project_id, error_type)
       WHERE NOT resolved AND external_project_id IS NOT NULL
     DO UPDATE SET details = EXCLUDED.details, run_at = NOW()`,
    { externalProjectId, externalProjectSlug, errorType, details: JSON.stringify(details) },
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Factory
// ─────────────────────────────────────────────────────────────────────────────

export async function createPccProjectConsumer(dryRun = false): Promise<PccProjectConsumer> {
  const db = await getDbConnection(WRITE_DB_CONFIG())
  const metadataStore = new MetadataStore(db)
  const s3Service = new S3Service()
  const pollingIntervalMs = 10_000 // 10 seconds

  return new PccProjectConsumer(metadataStore, s3Service, db, pollingIntervalMs, dryRun)
}
