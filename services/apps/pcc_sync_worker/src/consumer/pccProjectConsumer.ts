/**
 * PCC project consumer: polls snowflakeExportJobs for platform='pcc' jobs,
 * streams each Parquet file, runs the matching cascade, and writes to DB.
 *
 * One DB transaction per job — all segment + insightsProject writes roll back
 * together on any failure. Sync error records are written on a separate
 * connection (via `this.db`, not `tx`) so they survive a tx rollback — otherwise
 * a single failing row would lose all diagnostic breadcrumbs for the batch.
 */
import { DEFAULT_TENANT_ID } from '@crowd/common'
import { DbConnOrTx, DbConnection, WRITE_DB_CONFIG, getDbConnection } from '@crowd/database'
import { getServiceChildLogger } from '@crowd/logging'
import { MetadataStore, S3Service, SnowflakeExportJob, buildPlatformFilter } from '@crowd/snowflake'

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
  private readonly shutdownAbort = new AbortController()
  // Jobs already processed in this dry-run lifetime. Dry-run releases the
  // claim so nothing is persisted, which means the same "oldest pending" job
  // would otherwise be re-claimed on every loop iteration → endless reprocessing.
  private readonly dryRunProcessedJobIds = new Set<number>()

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
        const job = await this.metadataStore.claimOldestPendingJob(buildPlatformFilter([PLATFORM]))

        if (job) {
          if (this.dryRun && this.dryRunProcessedJobIds.has(job.id)) {
            // Already processed in this dry-run lifetime — the claim is about to be
            // released again; fall through to the "no pending jobs" path so we back
            // off instead of churning the same job forever.
            await this.releaseClaimBestEffort(job.id)
          } else {
            this.currentPollingIntervalMs = this.pollingIntervalMs
            await this.processJob(job)
            if (this.dryRun) this.dryRunProcessedJobIds.add(job.id)
            await new Promise<void>((resolve) => setImmediate(resolve))
            continue
          }
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
    // Interrupt any in-flight backoff sleep so shutdown isn't delayed by
    // the current polling interval (up to MAX_POLLING_INTERVAL_MS).
    this.shutdownAbort.abort()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Job processing
  // ─────────────────────────────────────────────────────────────────────────

  private async processJob(job: SnowflakeExportJob): Promise<void> {
    log.info({ jobId: job.id, s3Path: job.s3Path, dryRun: this.dryRun }, 'Processing PCC job')

    const startTime = Date.now()
    let totalCount = 0
    let upsertedCount = 0
    let skippedCount = 0
    let mismatchCount = 0
    let schemaMismatchCount = 0
    let schemaMismatchMatchedCount = 0 // SCHEMA_MISMATCH rows that still have a CDP segment match
    let missingProjectIdCount = 0

    try {
      // Stream all rows and group by PROJECT_ID before processing.
      // The export emits one row per (leaf, hierarchy_level) from the PROJECT_SPINE
      // JOIN, so each leaf project produces N rows (one per ancestor level).
      // PROJECT_ID is trimmed at the group-key boundary (PCC source data occasionally
      // carries surrounding whitespace) so the same logical project never splits
      // into multiple groups.
      const groups = new Map<string, Record<string, unknown>[]>()
      for await (const raw of this.s3Service.streamParquetRows(job.s3Path)) {
        const rawId = (raw as Record<string, unknown>).PROJECT_ID
        const projectId = rawId == null ? '' : String(rawId).trim()
        if (!projectId) {
          missingProjectIdCount++
          continue
        }
        if (!groups.has(projectId)) groups.set(projectId, [])
        const group = groups.get(projectId)
        if (group) group.push(raw)
      }

      // Record a single SCHEMA_MISMATCH row aggregating all rows dropped for
      // missing PROJECT_ID — unidentifiable rows dedup on (error_type, reason)
      // so repeated daily exports don't accumulate duplicates. Kept as a
      // separate counter (not folded into schemaMismatchCount) because the
      // two track different granularities: rows vs project groups.
      if (missingProjectIdCount > 0) {
        log.warn(
          { jobId: job.id, count: missingProjectIdCount },
          'Dropped Parquet rows with missing PROJECT_ID',
        )
        await this.recordSyncError(null, null, 'SCHEMA_MISMATCH', {
          reason: 'missing PROJECT_ID',
          count: missingProjectIdCount,
        })
      }

      await this.db.tx(async (tx) => {
        for (const [, rows] of groups) {
          const parsed = parsePccRow(rows)

          totalCount++

          if (parsed.ok === false) {
            schemaMismatchCount++
            const errorDetails: Record<string, unknown> = { ...parsed.details }

            // If the row had identifiable fields (depth-range errors), attempt a segment
            // match so the error record reflects whether a CDP segment exists for this
            // project — useful for triage even when the depth rule is unsupported.
            if (parsed.pccProjectId) {
              const matched = await findSegmentBySourceId(tx, parsed.pccProjectId)
              if (matched) {
                schemaMismatchMatchedCount++
                errorDetails.matchedSegmentId = matched.id
                errorDetails.matchedSegmentName = matched.name
                errorDetails.matchedVia = 'sourceId'
              }
            }

            log.warn(
              {
                pccProjectId: parsed.pccProjectId ?? null,
                pccSlug: parsed.pccSlug ?? null,
                ...errorDetails,
              },
              'Schema mismatch in PCC row',
            )
            await this.recordSyncError(
              parsed.pccProjectId ?? null,
              parsed.pccSlug ?? null,
              'SCHEMA_MISMATCH',
              errorDetails,
            )
            continue
          }

          const { project } = parsed
          const result = await this.processRow(tx, project)

          switch (result.action) {
            case 'UPSERTED':
              upsertedCount++
              if (result.hierarchyMismatch) mismatchCount++
              break
            case 'SKIPPED':
              skippedCount++
              break
          }
        }
      })

      const durationMs = Date.now() - startTime
      log.info(
        {
          jobId: job.id,
          dryRun: this.dryRun,
          durationMs,
          total: totalCount,
          upserted: upsertedCount,
          skipped: skippedCount,
          hierarchyMismatch: mismatchCount,
          schemaMismatch: schemaMismatchCount,
          schemaMismatchWithCdpMatch: schemaMismatchMatchedCount,
          schemaMismatchNoCdpMatch: schemaMismatchCount - schemaMismatchMatchedCount,
          missingProjectId: missingProjectIdCount,
        },
        'PCC job completed',
      )

      if (this.dryRun) {
        // Dry-run must leave no trace: the job was claimed (processingStartedAt
        // set by claimOldestPendingJob), so release it so a real run can pick
        // it up later. Otherwise dry-run jobs are permanently stuck.
        await this.releaseClaimBestEffort(job.id)
      } else {
        await this.metadataStore.markCompleted(job.id, {
          transformedCount: upsertedCount,
          // schemaMismatchCount counts project groups; missingProjectIdCount
          // counts raw rows dropped before grouping — both are "not synced"
          // and belong in skippedCount.
          skippedCount: skippedCount + schemaMismatchCount + missingProjectIdCount,
          processingDurationMs: durationMs,
        })
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : String(err)
      log.error({ jobId: job.id, err }, 'PCC job failed')

      if (this.dryRun) {
        // Same rationale as the success path — release the claim so the job
        // can be retried on a real run.
        await this.releaseClaimBestEffort(job.id)
      } else {
        try {
          await this.metadataStore.markFailed(job.id, errorMessage, {
            processingDurationMs: Date.now() - startTime,
          })
        } catch (updateErr) {
          log.error({ jobId: job.id, updateErr }, 'Failed to mark job as failed')
        }
      }
    }
  }

  private async releaseClaimBestEffort(jobId: number): Promise<void> {
    try {
      await this.metadataStore.releaseClaim(jobId)
    } catch (err) {
      log.error({ jobId, err }, 'Failed to release dry-run claim')
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Per-row matching cascade + writes
  // ─────────────────────────────────────────────────────────────────────────

  private async processRow(
    tx: DbConnOrTx,
    project: ParsedPccProject,
  ): Promise<{ action: 'UPSERTED'; hierarchyMismatch: boolean } | { action: 'SKIPPED' }> {
    // Step 1: segment_id from Snowflake ACTIVE_SEGMENTS JOIN
    let segment = project.segmentIdFromSnowflake
      ? await findSegmentById(tx, project.segmentIdFromSnowflake)
      : null

    // Step 2: sourceId fallback
    if (!segment) {
      segment = await findSegmentBySourceId(tx, project.pccProjectId)
    }

    // Step 3: no match → SKIP (Phase 1: project doesn't exist in CDP yet)
    if (!segment) {
      return { action: 'SKIPPED' }
    }

    // Hierarchy mismatch detection: segment matched but parent/group differs.
    // Phase 1 does NOT re-parent segments — hierarchy fields (parent/grandparent id,
    // name, slug) are never written. We record the mismatch for manual review but
    // still sync the metadata fields (name, status, maturity, description, logo).
    const mismatchFields = detectHierarchyMismatch(segment, project.cdpTarget)
    const hasHierarchyMismatch = mismatchFields.length > 0

    if (hasHierarchyMismatch) {
      log.warn(
        {
          segmentId: segment.id,
          segmentName: segment.name,
          pccProjectId: project.pccProjectId,
          mismatchFields,
          cdpTarget: project.cdpTarget,
        },
        'Hierarchy mismatch — recorded for manual review, metadata still synced (Phase 1 scope)',
      )
      await this.recordSyncError(project.pccProjectId, project.pccSlug, 'HIERARCHY_MISMATCH', {
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
      })
    }

    // Slug drift detection: log when PCC slug differs from the CDP segment slug.
    // We do NOT update the slug — it is a stable identifier referenced by FK from
    // securityInsightsEvaluations and related tables. The mismatch is recorded for
    // manual review but does not block the sync.
    if (project.pccSlug && segment.slug && project.pccSlug !== segment.slug) {
      log.warn(
        { segmentId: segment.id, pccSlug: project.pccSlug, cdpSlug: segment.slug },
        'Slug drift detected — PCC slug differs from CDP segment slug',
      )
      await this.recordSyncError(project.pccProjectId, project.pccSlug, 'SLUG_CHANGED', {
        segmentId: segment.id,
        pccSlug: project.pccSlug,
        cdpSlug: segment.slug,
      })
    }

    if (!this.dryRun) {
      await upsertSegment(tx, project.pccProjectId, project)
      const nameConflict = await upsertInsightsProject(
        tx,
        segment.id,
        project.pccProjectId,
        project,
      )
      if (nameConflict) {
        log.warn(
          { segmentId: segment.id, name: project.name },
          'insightsProject name conflict — segment synced, insights project skipped',
        )
        await this.recordSyncError(
          project.pccProjectId,
          project.pccSlug,
          'INSIGHTS_NAME_CONFLICT',
          {
            segmentId: segment.id,
            name: project.name,
          },
        )
      }
    } else {
      log.info(
        {
          segmentId: segment.id,
          pccProjectId: project.pccProjectId,
          name: project.name,
          status: project.status,
          maturity: project.maturity,
          hierarchyMismatch: hasHierarchyMismatch,
        },
        '[dry-run] Would upsert segment',
      )
    }

    return { action: 'UPSERTED', hierarchyMismatch: hasHierarchyMismatch }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise<void>((resolve) => {
      if (this.shutdownAbort.signal.aborted) {
        resolve()
        return
      }
      const timer = setTimeout(resolve, ms)
      this.shutdownAbort.signal.addEventListener(
        'abort',
        () => {
          clearTimeout(timer)
          resolve()
        },
        { once: true },
      )
    })
  }

  // Records a sync-error row on a separate connection (`this.db`, not the job's
  // tx) so the diagnostic survives a tx rollback. Wrapped in try/catch so a
  // write failure here never cascades into the enclosing tx.
  private async recordSyncError(
    externalProjectId: string | null,
    externalProjectSlug: string | null,
    errorType: string,
    details: Record<string, unknown>,
  ): Promise<void> {
    if (this.dryRun) return
    try {
      await insertSyncError(this.db, externalProjectId, externalProjectSlug, errorType, details)
    } catch (err) {
      log.error(
        { err, externalProjectId, externalProjectSlug, errorType },
        'Failed to record sync error (best-effort)',
      )
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// DB helpers
// ─────────────────────────────────────────────────────────────────────────────

interface SegmentRow {
  id: string
  name: string
  slug: string | null
  parentName: string | null
  grandparentName: string | null
}

async function findSegmentById(db: DbConnOrTx, segmentId: string): Promise<SegmentRow | null> {
  return db.oneOrNone<SegmentRow>(
    `SELECT id, name, slug, "parentName", "grandparentName"
     FROM segments
     WHERE id = $(segmentId) AND "tenantId" = $(tenantId)`,
    { segmentId, tenantId: DEFAULT_TENANT_ID },
  )
}

async function findSegmentBySourceId(db: DbConnOrTx, sourceId: string): Promise<SegmentRow | null> {
  return db.oneOrNone<SegmentRow>(
    `SELECT id, name, slug, "parentName", "grandparentName"
     FROM segments
     WHERE "sourceId" = $(sourceId) AND type = 'subproject' AND "tenantId" = $(tenantId)`,
    { sourceId, tenantId: DEFAULT_TENANT_ID },
  )
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
  sourceId: string,
  project: ParsedPccProject,
): Promise<void> {
  // Update all CDP segments whose sourceId equals this PCC PROJECT_ID.
  // Each PCC node has its own PROJECT_ID. In CDP, how many segment levels share this
  // sourceId depends on the effective depth:
  //   eff=1 → group+project+subproject all share the same PROJECT_ID (same name for all)
  //   eff=2 → project+subproject share the leaf's PROJECT_ID; group has a different one
  //   eff=3 or 4 → only the subproject segment carries this PROJECT_ID
  // So this UPDATE always writes the correct name and never touches unrelated levels.
  await db.none(
    `UPDATE segments
     SET name        = $(name),
         status      = COALESCE($(status)::"segmentsStatus_type", status),
         maturity    = $(maturity),
         description = $(description),
         "updatedAt" = NOW()
     WHERE "sourceId" = $(sourceId) AND "tenantId" = $(tenantId)`,
    {
      sourceId,
      name: project.name,
      status: project.status,
      maturity: project.maturity,
      description: project.description,
      tenantId: DEFAULT_TENANT_ID,
    },
  )
}

// Returns true if a name conflict prevented creating the insightsProject row.
// Updates insightsProject rows for ALL segment levels sharing the same sourceId
// (group, project, subproject). The INSERT is restricted to the matched subproject
// segment (identified by segmentId) to avoid duplicating insights projects for
// hierarchy-only segments.
async function upsertInsightsProject(
  db: DbConnOrTx,
  segmentId: string,
  sourceId: string,
  project: ParsedPccProject,
): Promise<boolean> {
  // Check for a name conflict upfront — an active insightsProject belonging to a segment
  // outside this PCC project's sourceId group already holds this name.
  // We must exclude all segments sharing the same sourceId (not just the subproject),
  // because on repeat syncs the group/project levels already carry the same name and
  // would produce false positives if only the subproject segmentId were excluded.
  const conflicting = await db.oneOrNone<{ id: string }>(
    `SELECT ip.id
     FROM "insightsProjects" ip
     JOIN segments s ON s.id = ip."segmentId"
     WHERE ip.name = $(name)
       AND ip."deletedAt" IS NULL
       AND s."sourceId" != $(sourceId)
       AND s."tenantId" = $(tenantId)`,
    { name: project.name, sourceId, tenantId: DEFAULT_TENANT_ID },
  )
  if (conflicting) return true

  // No conflict — update all active insightsProject rows linked to any segment that
  // shares the PCC sourceId (group, project, subproject levels).
  // Slug is intentionally not updated — it is a stable identifier referenced by FK from
  // securityInsightsEvaluations and related tables.
  // logoUrl won't be updated in InsightsProject until we confirm that the format is
  // compatible with the Insights Squared standard. Do NOT reintroduce it as a
  // `--`-commented SQL line: pg-promise scans placeholders textually and would still
  // require the `logoUrl` param, triggering "Property 'logoUrl' doesn't exist".
  await db.none(
    `UPDATE "insightsProjects" ip
     SET name        = $(name),
         description = $(description),
         "updatedAt" = NOW()
     FROM segments s
     WHERE ip."segmentId" = s.id
       AND s."sourceId" = $(sourceId)
       AND s."tenantId" = $(tenantId)
       AND ip."deletedAt" IS NULL`,
    {
      sourceId,
      tenantId: DEFAULT_TENANT_ID,
      name: project.name,
      description: project.description,
    },
  )

  // INSERT for the subproject segment only (the matched leaf).
  // Partial unique index on segmentId WHERE deletedAt IS NULL means ON CONFLICT won't fire
  // for soft-deleted rows — use UPDATE-then-INSERT pattern (UPDATE already done above).
  const exists = await db.oneOrNone<{ id: string }>(
    `SELECT id FROM "insightsProjects" WHERE "segmentId" = $(segmentId) AND "deletedAt" IS NULL`,
    { segmentId },
  )
  if (exists) return false

  // logoUrl intentionally omitted from the INSERT column list — see note above.
  const inserted = await db.result(
    `INSERT INTO "insightsProjects" (name, slug, description, "segmentId", "isLF")
     VALUES ($(name), generate_slug('insightsProjects', $(name)), $(description), $(segmentId), TRUE)
     ON CONFLICT (name) WHERE "deletedAt" IS NULL DO NOTHING`,
    { name: project.name, description: project.description, segmentId },
  )

  if (inserted.rowCount === 0) {
    // INSERT was a no-op on the partial unique index (name) WHERE "deletedAt" IS NULL.
    // The pre-check above already ruled out cross-sourceId conflicts, so the row holding
    // the name must be a same-sourceId sibling — shallow hierarchies (eff=1/2) where
    // group/project/subproject share both name and sourceId. Verify before concluding
    // it's not a conflict (guards against a hypothetical race with another writer).
    const holder = await db.oneOrNone<{ sameFamily: boolean }>(
      `SELECT s."sourceId" = $(sourceId) AS "sameFamily"
       FROM "insightsProjects" ip
       JOIN segments s ON s.id = ip."segmentId"
       WHERE ip.name = $(name)
         AND ip."deletedAt" IS NULL
         AND s."tenantId" = $(tenantId)
       LIMIT 1`,
      { name: project.name, sourceId, tenantId: DEFAULT_TENANT_ID },
    )
    // Same-family holder (or holder vanished between INSERT and re-check) → not a real
    // conflict; the project family is already represented via the sibling row.
    if (!holder || holder.sameFamily) return false
    return true
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
  const serialized = JSON.stringify(details)
  if (externalProjectId !== null) {
    // Known project: deduplicate on (external_project_id, error_type).
    await db.none(
      `INSERT INTO pcc_projects_sync_errors
         (external_project_id, external_project_slug, error_type, details)
       VALUES ($(externalProjectId), $(externalProjectSlug), $(errorType), $(details)::jsonb)
       ON CONFLICT (external_project_id, error_type)
         WHERE NOT resolved AND external_project_id IS NOT NULL
       DO UPDATE SET details = EXCLUDED.details, external_project_slug = EXCLUDED.external_project_slug, run_at = NOW()`,
      { externalProjectId, externalProjectSlug, errorType, details: serialized },
    )
  } else {
    // Unidentifiable row (no PROJECT_ID): deduplicate on (error_type, details->>'reason')
    // so repeated daily exports don't accumulate duplicate rows for the same class of
    // malformed input. Each distinct failure reason gets one unresolved row.
    await db.none(
      `INSERT INTO pcc_projects_sync_errors
         (external_project_slug, error_type, details)
       VALUES ($(externalProjectSlug), $(errorType), $(details)::jsonb)
       ON CONFLICT (error_type, (details->>'reason'))
         WHERE NOT resolved AND external_project_id IS NULL
       DO UPDATE SET details = EXCLUDED.details, external_project_slug = EXCLUDED.external_project_slug, run_at = NOW()`,
      { externalProjectSlug, errorType, details: serialized },
    )
  }
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
