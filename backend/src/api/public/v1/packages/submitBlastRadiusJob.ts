import type { Request, Response } from 'express'

import { BadRequestError, generateUUIDv4 } from '@crowd/common'
import { ITriggerBlastRadiusAnalysis, TemporalWorkflowId } from '@crowd/types'

import { validateOrThrow } from '@/utils/validation'

import {
  blastRadiusJobRequestSchema,
  isSupportedBlastRadiusEcosystem,
  toBlastRadiusJobEntry,
} from './blastRadius'

// 2a — submit a blast-radius analysis job. Always exactly one job per request; no
// persistence/caching/pipeline yet (see analyzeBlastRadius in packages_worker,
// which currently reports every ecosystem as unsupported). Every submission gets a
// fresh analysisId and status pending — the 7-day cache and GET poll endpoint land
// in a follow-up PR once the analysis is actually persisted.
export async function submitBlastRadiusJob(req: Request, res: Response): Promise<void> {
  const body = validateOrThrow(blastRadiusJobRequestSchema, req.body)

  const jobPackage = body.package ?? null
  const jobEcosystem = body.ecosystem ?? null

  // Reachability pipeline is npm-only for now — reject before touching Temporal so
  // unsupported/missing ecosystems never spawn a workflow run.
  if (!isSupportedBlastRadiusEcosystem(jobEcosystem)) {
    throw new BadRequestError(
      `Ecosystem "${jobEcosystem ?? 'unknown'}" is not supported for blast-radius analysis`,
    )
  }

  const analysisId = generateUUIDv4()

  await req.temporal.workflow.start('analyzeBlastRadius', {
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
      } as ITriggerBlastRadiusAnalysis,
    ],
  })

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
