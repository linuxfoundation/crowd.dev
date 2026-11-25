import type { SegmentStatus } from '@crowd/types'

import { creationStepKey } from './creationSteps'
import type { CreationStep } from './creationSteps'
import type { SegmentCreationResult } from './segmentCreationClient'

export interface ProjectToCreate {
  pccProjectId: string
  pccSlug: string | null
  name: string
  status: SegmentStatus | null
  steps: CreationStep[]
}

export type ProjectCreationOutcome =
  | { outcome: 'created'; project: ProjectToCreate }
  | {
      outcome: 'failed'
      project: ProjectToCreate
      step: CreationStep
      httpStatus: number | null
      error: string
    }
  | { outcome: 'waiting_for_failed_level'; project: ProjectToCreate; step: CreationStep }

export interface CreationRunResult {
  outcomes: ProjectCreationOutcome[]
  notAttempted: number
  stoppedWithoutAnswers: boolean
}

export interface CreationRunOptions {
  maxProjects: number
  maxConsecutiveUnanswered?: number
  createStep: (step: CreationStep, status: SegmentStatus | null) => Promise<SegmentCreationResult>
}

const DEFAULT_MAX_CONSECUTIVE_UNANSWERED = 3

async function createProject(
  project: ProjectToCreate,
  createdSteps: Set<string>,
  failedSteps: Set<string>,
  statusByPccProjectId: Map<string, SegmentStatus | null>,
  createStep: CreationRunOptions['createStep'],
): Promise<{ outcome: ProjectCreationOutcome; calledBackend: boolean }> {
  let calledBackend = false

  for (const step of project.steps) {
    const key = creationStepKey(step)
    if (createdSteps.has(key)) continue
    if (failedSteps.has(key)) {
      return { outcome: { outcome: 'waiting_for_failed_level', project, step }, calledBackend }
    }

    calledBackend = true
    const result = await createStep(step, statusByPccProjectId.get(step.node.pccProjectId) ?? null)

    if (result.outcome === 'failed') {
      failedSteps.add(key)
      return {
        outcome: {
          outcome: 'failed',
          project,
          step,
          httpStatus: result.httpStatus,
          error: result.error,
        },
        calledBackend,
      }
    }
    createdSteps.add(key)
  }

  return { outcome: { outcome: 'created', project }, calledBackend }
}

export async function runCreations(
  projects: ProjectToCreate[],
  options: CreationRunOptions,
): Promise<CreationRunResult> {
  const maxConsecutiveUnanswered =
    options.maxConsecutiveUnanswered ?? DEFAULT_MAX_CONSECUTIVE_UNANSWERED
  const createdSteps = new Set<string>()
  const failedSteps = new Set<string>()
  const statusByPccProjectId = new Map(projects.map((p) => [p.pccProjectId, p.status]))
  const outcomes: ProjectCreationOutcome[] = []
  let projectsSentToBackend = 0
  let consecutiveUnanswered = 0
  let handled = 0

  for (const project of projects) {
    if (projectsSentToBackend >= options.maxProjects) break
    if (consecutiveUnanswered >= maxConsecutiveUnanswered) break

    const { outcome, calledBackend } = await createProject(
      project,
      createdSteps,
      failedSteps,
      statusByPccProjectId,
      options.createStep,
    )
    outcomes.push(outcome)
    handled++
    if (calledBackend) projectsSentToBackend++
    if (calledBackend) {
      const unanswered = outcome.outcome === 'failed' && outcome.httpStatus === null
      consecutiveUnanswered = unanswered ? consecutiveUnanswered + 1 : 0
    }
  }

  return {
    outcomes,
    notAttempted: projects.length - handled,
    stoppedWithoutAnswers: consecutiveUnanswered >= maxConsecutiveUnanswered,
  }
}
