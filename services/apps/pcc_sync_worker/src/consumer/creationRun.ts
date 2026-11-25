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

interface RunState {
  createdSteps: Set<string>
  failedSteps: Set<string>
  statusByPccProjectId: Map<string, SegmentStatus | null>
  consecutiveUnanswered: number
}

async function createProject(
  project: ProjectToCreate,
  state: RunState,
  createStep: CreationRunOptions['createStep'],
): Promise<{ outcome: ProjectCreationOutcome; calledBackend: boolean }> {
  let calledBackend = false

  for (const step of project.steps) {
    const key = creationStepKey(step)
    if (state.createdSteps.has(key)) continue
    if (state.failedSteps.has(key)) {
      return { outcome: { outcome: 'waiting_for_failed_level', project, step }, calledBackend }
    }

    calledBackend = true
    const result = await createStep(
      step,
      state.statusByPccProjectId.get(step.node.pccProjectId) ?? null,
    )

    if (result.outcome === 'failed') {
      state.failedSteps.add(key)
      state.consecutiveUnanswered = result.httpStatus === null ? state.consecutiveUnanswered + 1 : 0
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
    state.createdSteps.add(key)
    state.consecutiveUnanswered = 0
  }

  return { outcome: { outcome: 'created', project }, calledBackend }
}

export async function runCreations(
  projects: ProjectToCreate[],
  options: CreationRunOptions,
): Promise<CreationRunResult> {
  const maxConsecutiveUnanswered =
    options.maxConsecutiveUnanswered ?? DEFAULT_MAX_CONSECUTIVE_UNANSWERED
  const state: RunState = {
    createdSteps: new Set(),
    failedSteps: new Set(),
    statusByPccProjectId: new Map(projects.map((p) => [p.pccProjectId, p.status])),
    consecutiveUnanswered: 0,
  }
  const outcomes: ProjectCreationOutcome[] = []
  let projectsSentToBackend = 0
  let stoppedWithoutAnswers = false

  for (const project of projects) {
    if (state.consecutiveUnanswered >= maxConsecutiveUnanswered) {
      stoppedWithoutAnswers = true
      break
    }
    if (projectsSentToBackend >= options.maxProjects) break

    const { outcome, calledBackend } = await createProject(project, state, options.createStep)
    outcomes.push(outcome)
    if (calledBackend) projectsSentToBackend++
  }

  return {
    outcomes,
    notAttempted: projects.length - outcomes.length,
    stoppedWithoutAnswers,
  }
}
