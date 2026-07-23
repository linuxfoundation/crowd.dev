import type { Request, Response } from 'express'

import { generateUUIDv4 } from '@crowd/common'
import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { Client } from '@crowd/temporal'
import { ITriggerBlastRadiusAnalysis, TemporalWorkflowId } from '@crowd/types'

import { getPackagesQx } from '@/db/packagesDb'
import { getPackagesTemporalClient } from '@/db/packagesTemporal'
import { validateOrThrow } from '@/utils/validation'

import {
  type BlastRadiusJobEntry,
  type BlastRadiusJobRequest,
  getCachedJobEntry,
  toBlastRadiusJobEntry,
} from './blastRadius'
import { blastRadiusJobBatchRequestSchema } from './blastRadiusBatch'

// 2a bulk — submit multiple blast-radius analysis jobs in one request, one per
// array entry. Same lifecycle as the single-job submit, just looped: each entry
// gets its own analysisId, its own pending row, and its own Temporal workflow
// start — unless a 'done' analysis for the same (advisoryId, package, ecosystem)
// is still within the advisory cache window (see BLAST_RADIUS_CACHE_MAX_AGE_DAYS),
// in which case that entry reuses the cached analysis instead. Unlike the
// read-only batch endpoints (packages/advisories/contacts),
// this multiplies workflow starts per request, so the batch size is capped much
// lower (see MAX_BLAST_RADIUS_JOBS_PER_BATCH) and the route stays behind the same
// strict blastRadiusRateLimiter as the single-job route.
//
// A per-job failure (e.g. workflow.start throwing) does not fail the whole
// batch — that job's entry comes back status: 'failed' and the rest still
// submit, matching the partial-result shape of the other batch endpoints.
export async function submitBlastRadiusJobBatch(req: Request, res: Response): Promise<void> {
  const { jobs } = validateOrThrow(blastRadiusJobBatchRequestSchema, req.body)

  const qx = await getPackagesQx()
  const packagesTemporal = await getPackagesTemporalClient()

  const results: BlastRadiusJobEntry[] = await Promise.all(
    jobs.map((body) => submitOneJob(qx, packagesTemporal, body)),
  )

  res.status(202).json({ results })
}

async function submitOneJob(
  qx: QueryExecutor,
  packagesTemporal: Client,
  body: BlastRadiusJobRequest,
): Promise<BlastRadiusJobEntry> {
  const jobPackage = body.package ?? null
  const jobEcosystem = body.ecosystem
  const analysisId = generateUUIDv4()
  const analysisInput = {
    id: analysisId,
    advisoryOsvId: body.advisoryId,
    packageName: jobPackage,
    ecosystem: jobEcosystem,
    force: body.force,
  }

  try {
    // Cache lookup is inside the try too — like createAnalysis/workflow.start below,
    // a DB error here must resolve this job's entry as 'failed', not reject the whole
    // batch's Promise.all and 500 every other job in it.
    const cached = await getCachedJobEntry(qx, {
      advisoryId: body.advisoryId,
      package: jobPackage,
      ecosystem: jobEcosystem,
      force: body.force,
    })
    if (cached) {
      return cached
    }

    // Create the pending row synchronously, before starting the workflow — see the
    // same comment on submitBlastRadiusJob for why (avoids a poll-race 404).
    await blastRadiusDal.createAnalysis(qx, analysisInput)

    await packagesTemporal.workflow.start('analyzeBlastRadius', {
      taskQueue: 'blast-radius-worker',
      workflowId: `${TemporalWorkflowId.BLAST_RADIUS_ANALYSIS}/${analysisId}`,
      retry: { maximumAttempts: 1 },
      args: [
        {
          analysisId,
          advisoryId: body.advisoryId,
          package: jobPackage,
          ecosystem: jobEcosystem,
          force: body.force,
        } satisfies ITriggerBlastRadiusAnalysis,
      ],
    })

    return toBlastRadiusJobEntry({
      analysisId,
      advisoryId: body.advisoryId,
      package: jobPackage,
      ecosystem: jobEcosystem,
    })
  } catch (err) {
    // Unlike the single-job submit, this does not rethrow — one job's workflow
    // failing to start must not take the rest of the batch down with it. The
    // failAnalysis call below is deliberately its own try/catch too — if marking
    // the row failed also fails (e.g. DB unreachable), that must still resolve
    // this job's entry rather than reject the whole Promise.all and 500 the batch.
    const errorMessage = err instanceof Error ? err.message : String(err)
    try {
      await blastRadiusDal.failAnalysis(qx, analysisInput, errorMessage)
    } catch {
      // best-effort — the job is already being reported as failed below
    }

    return toBlastRadiusJobEntry({
      analysisId,
      advisoryId: body.advisoryId,
      package: jobPackage,
      ecosystem: jobEcosystem,
      status: 'failed',
    })
  }
}
