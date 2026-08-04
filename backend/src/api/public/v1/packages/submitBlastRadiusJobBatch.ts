import type { Request, Response } from 'express'

import { generateUUIDv4 } from '@crowd/common'
import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
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
// array entry (each may hit the cache via getCachedJobEntry). Unlike the
// read-only batch endpoints (packages/advisories/contacts), this multiplies
// workflow starts per request, so the batch size is capped much lower (see
// MAX_BLAST_RADIUS_JOBS_PER_BATCH) and the route stays behind the same strict
// blastRadiusRateLimiter as the single-job route. A per-job failure does not
// fail the whole batch — that job's entry comes back status: 'failed'.
export async function submitBlastRadiusJobBatch(req: Request, res: Response): Promise<void> {
  const { jobs } = validateOrThrow(blastRadiusJobBatchRequestSchema, req.body)

  const qx = await getPackagesQx()

  const results: BlastRadiusJobEntry[] = await Promise.all(
    jobs.map((body) => submitOneJob(qx, body)),
  )

  res.status(202).json({ results })
}

async function submitOneJob(
  qx: QueryExecutor,
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
    // Cache lookup is inside the try too, so a DB error here resolves this
    // job's entry as 'failed' instead of rejecting the whole batch.
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

    // Acquired per job (inside the try), not once up front — getPackagesTemporalClient
    // caches its connection in a module-level singleton, so this is cheap once
    // connected, but a first-ever connection failure must fail this job's entry only,
    // not reject the whole batch before any per-job try/catch is in play.
    const packagesTemporal = await getPackagesTemporalClient()

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
    // failing to start must not take the rest of the batch down with it. Same
    // reasoning applies to failAnalysis itself: if marking the row failed also
    // fails (e.g. transient DB error), that must not reject this job's promise
    // and take Promise.all (and the whole batch response) down with it.
    const errorMessage = err instanceof Error ? err.message : String(err)
    try {
      await blastRadiusDal.failAnalysis(qx, analysisInput, errorMessage)
    } catch {
      // best-effort — the job's entry below still reports status: 'failed'
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
