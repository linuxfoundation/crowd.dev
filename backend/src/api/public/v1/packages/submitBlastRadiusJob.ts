import type { Request, Response } from 'express'

import { generateUUIDv4 } from '@crowd/common'
import { ITriggerBlastRadiusAnalysis, TemporalWorkflowId } from '@crowd/types'

import { getPackagesTemporalClient } from '@/db/packagesTemporal'
import { validateOrThrow } from '@/utils/validation'

import { blastRadiusJobRequestSchema, toBlastRadiusJobEntry } from './blastRadius'

// 2a — submit a blast-radius analysis job. Always exactly one job per request; no
// persistence/caching/pipeline yet (see analyzeBlastRadius in packages_worker,
// which currently reports every ecosystem as unsupported). Every submission gets a
// fresh analysisId and status pending — the 7-day cache and GET poll endpoint land
// in a follow-up PR once the analysis is actually persisted.
export async function submitBlastRadiusJob(req: Request, res: Response): Promise {
  const body = validateOrThrow(blastRadiusJobRequestSchema, req.body)

  const jobPackage = body.package ?? null
  const jobEcosystem = body.ecosystem

  const analysisId = generateUUIDv4()

  // blast-radius-worker polls the packages Temporal namespace, not the API's default
  // one (req.temporal) — starting it there would leave the workflow queued forever.
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
