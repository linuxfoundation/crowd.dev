import { Context } from '@temporalio/activity'

import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'

import { runDependentsStage } from './stages/dependents'
import { runIntelStage } from './stages/intel'
import { runReachabilityStage } from './stages/reachability'
import { runReportStage } from './stages/report'

const log = getServiceChildLogger('blast-radius')

export interface BlastRadiusActivityInput {
  analysisId: string
  advisoryOsvId: string
}

export interface BlastRadiusStartInput {
  analysisId: string
  advisoryOsvId: string
  packageName: string | null
  ecosystem: string
  force: boolean
}

// Activity: create (or reuse, on retrigger) the analysis row and mark it running.
// All DB access for the workflow lives in activities — Temporal workflow code
// must stay deterministic and cannot talk to the database directly.
export async function blastRadiusStart(input: BlastRadiusStartInput): Promise<void> {
  log.info({ analysisId: input.analysisId }, 'blast-radius: starting analysis')
  const qx = await getPackagesDb()
  await blastRadiusDal.createAnalysis(qx, {
    id: input.analysisId,
    advisoryOsvId: input.advisoryOsvId,
    packageName: input.packageName,
    ecosystem: input.ecosystem,
    force: input.force,
  })
  await blastRadiusDal.markAnalysisRunning(qx, input.analysisId)
  log.info({ analysisId: input.analysisId }, 'blast-radius: analysis marked running')
}

// Activity: mark the analysis row failed with the given error message. Takes the
// same fields as blastRadiusStart so failAnalysis can create the row itself if
// blastRadiusStart never got far enough to (see failAnalysis for why).
export async function blastRadiusFail(
  input: BlastRadiusStartInput & { error: string },
): Promise<void> {
  log.warn({ analysisId: input.analysisId, error: input.error }, 'blast-radius: analysis failed')
  const qx = await getPackagesDb()
  await blastRadiusDal.failAnalysis(
    qx,
    {
      id: input.analysisId,
      advisoryOsvId: input.advisoryOsvId,
      packageName: input.packageName,
      ecosystem: input.ecosystem,
      force: input.force,
    },
    input.error,
  )
}

// Activity: Stage 1 — vulnerability intelligence
export async function blastRadiusIntel(input: BlastRadiusActivityInput): Promise<void> {
  log.info({ analysisId: input.analysisId }, 'blast-radius: intel stage starting')
  const qx = await getPackagesDb()
  await runIntelStage(qx, input.analysisId, input.advisoryOsvId, () =>
    Context.current().heartbeat(),
  )
  log.info({ analysisId: input.analysisId }, 'blast-radius: intel stage done')
}

// Activity: Stage 2 — dependents scan
export async function blastRadiusDependents(input: BlastRadiusActivityInput): Promise<void> {
  log.info({ analysisId: input.analysisId }, 'blast-radius: dependents stage starting')
  const qx = await getPackagesDb()
  await runDependentsStage(qx, input.analysisId, () => Context.current().heartbeat())
  log.info({ analysisId: input.analysisId }, 'blast-radius: dependents stage done')
}

// Activity: Stage 3 — reachability analysis
export async function blastRadiusReachability(input: BlastRadiusActivityInput): Promise<void> {
  log.info({ analysisId: input.analysisId }, 'blast-radius: reachability stage starting')
  const qx = await getPackagesDb()
  await runReachabilityStage(qx, input.analysisId, () => Context.current().heartbeat())
  log.info({ analysisId: input.analysisId }, 'blast-radius: reachability stage done')
}

// Activity: Stage 4 — report aggregation
export async function blastRadiusReport(input: BlastRadiusActivityInput): Promise<void> {
  log.info({ analysisId: input.analysisId }, 'blast-radius: report stage starting')
  const qx = await getPackagesDb()
  await runReportStage(qx, input.analysisId)
  Context.current().heartbeat()
  log.info({ analysisId: input.analysisId }, 'blast-radius: report stage done')
}
