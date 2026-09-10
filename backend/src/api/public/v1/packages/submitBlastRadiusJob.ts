import type { Request, Response } from 'express'

import { generateUUIDv4 } from '@crowd/common'
import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { ITriggerBlastRadiusAnalysis, TemporalWorkflowId } from '@crowd/types'

import { getPackagesQx } from '@/db/packagesDb'
import { getPackagesTemporalClient } from '@/db/packagesTemporal'
import { validateOrThrow } from '@/utils/validation'

import { blastRadiusJobRequestSchema, toBlastRadiusJobEntry } from './blastRadius'

// 2a — submit a blast-radius analysis job. Always exactly one job per request.
// Every submission gets a fresh analysisId and status pending.
export async function submitBlastRadiusJob(req: Request, res: Response): Promise<void> {
  const body = validateOrThrow(blastRadiusJobRequestSchema, req.body)

  const jobPackage = body.package ?? null
  const jobEcosystem = body.ecosystem

  const analysisId = generateUUIDv4()

  // Create the pending row synchronously, before starting the workflow — otherwise a
  // client that polls GET /jobs/:analysisId immediately after this 202 can race
  // blastRadiusStart's own createAnalysis call and get a 404 for a job that was, in
  // fact, accepted. blastRadiusStart's createAnalysis upserts the same row, so this
  // is safe to run again from the workflow.
  const qx = await getPackagesQx()
  await blastRadiusDal.createAnalysis(qx, {
    id: analysisId,
    advisoryOsvId: body.advisoryId,
    packageName: jobPackage,
    ecosystem: jobEcosystem,
    force: body.force,
  })

  // blast-radius-worker polls the packages Temporal namespace, not the API's default
  // one (req.temporal) — starting it there would leave the workflow queued forever.
  const packagesTemporal = await getPackagesTemporalClient()

  try {
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
  } catch (err) {
    // Without this, a workflow.start failure (Temporal unreachable, task queue
    // misconfigured, etc.) leaves the row created above stuck 'pending' forever —
    // no workflow ever runs to mark it failed, so poll never reaches a terminal state.
    const errorMessage = err instanceof Error ? err.message : String(err)
    await blastRadiusDal.failAnalysis(
      qx,
      {
        id: analysisId,
        advisoryOsvId: body.advisoryId,
        packageName: jobPackage,
        ecosystem: jobEcosystem,
        force: body.force,
      },
      errorMessage,
    )
    throw err
  }

  // 202, not the shared ok() helper (200) — the contract accepts the job, it does
  // not return a completed result.
  res.status(202).json(
    toBlastRadiusJobEntry({
      analysisId,
      advisoryId: body.advisoryId,
      package: jobPackage,
      ecosystem: jobEcosystem,
    }),
  )
}
