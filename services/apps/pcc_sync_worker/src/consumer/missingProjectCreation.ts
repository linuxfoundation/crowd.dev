import { getServiceChildLogger } from '@crowd/logging'
import { SegmentStatus } from '@crowd/types'

import type { ParsedPccProject } from '../parser'
import type { CdpSegmentIndex, CreationPlan } from './creationPlan'
import { runCreations } from './creationRun'
import type { CreationRunOptions, ProjectCreationOutcome, ProjectToCreate } from './creationRun'
import { buildCreationSteps, creationStepKey } from './creationSteps'
import type { CreationStep } from './creationSteps'
import { buildSegmentCreationPayload } from './segmentCreationClient'

const log = getServiceChildLogger('missingProjectCreation')

export const PROJECT_CREATION_FAILED = 'PROJECT_CREATION_FAILED'

export type CreationMode = 'off' | 'dry-run' | 'on'

export interface CreationSettings {
  mode: CreationMode
  maxProjectsPerRun: number
}

const DEFAULT_MAX_PROJECTS_PER_RUN = 30
const SEGMENT_STATUSES = new Set<string>(Object.values(SegmentStatus))

export function readCreationSettings(env: NodeJS.ProcessEnv = process.env): CreationSettings {
  const rawMode = env.PCC_PROJECT_CREATION
  const mode: CreationMode = rawMode === 'on' || rawMode === 'dry-run' ? rawMode : 'off'

  const max = Number(env.PCC_PROJECT_CREATION_MAX_PER_RUN)
  const maxProjectsPerRun = Number.isInteger(max) && max > 0 ? max : DEFAULT_MAX_PROJECTS_PER_RUN

  return { mode, maxProjectsPerRun }
}

export function effectiveCreationMode(
  mode: CreationMode,
  syncIsDryRun: boolean,
  hasBackendApi: boolean,
): CreationMode {
  if (mode === 'off') return 'off'
  return syncIsDryRun || !hasBackendApi ? 'dry-run' : mode
}

export function toProjectToCreate(
  project: ParsedPccProject,
  plan: CreationPlan,
  index: CdpSegmentIndex,
): ProjectToCreate | null {
  if (plan.action !== 'CREATE' || !project.creationTarget) return null

  const steps = buildCreationSteps(project.creationTarget, plan, index)
  if (steps.length === 0) return null

  return {
    pccProjectId: project.pccProjectId,
    pccSlug: project.pccSlug,
    name: project.name,
    status:
      project.status && SEGMENT_STATUSES.has(project.status)
        ? (project.status as SegmentStatus)
        : null,
    steps,
  }
}

export interface CreationFailure {
  pccProjectId: string
  pccSlug: string | null
  details: Record<string, unknown>
}

export interface MissingProjectCreationOptions {
  mode: Exclude<CreationMode, 'off'>
  maxProjects: number
  createStep: CreationRunOptions['createStep']
  recordFailure: (failure: CreationFailure) => Promise<void>
}

export interface MissingProjectCreationSummary {
  mode: Exclude<CreationMode, 'off'>
  creatable: number
  created: number
  createdPccProjectIds: string[]
  failed: number
  waitingForFailedLevel: number
  notAttempted: number
  stoppedWithoutAnswers: boolean
}

function describeStep(step: CreationStep) {
  return { endpoint: step.endpoint, pccProjectId: step.node.pccProjectId, slug: step.node.slug }
}

function describeProject(project: ProjectToCreate) {
  return { pccProjectId: project.pccProjectId, pccSlug: project.pccSlug, name: project.name }
}

function logDryRun(projects: ProjectToCreate[], maxProjects: number): number {
  const statusByPccProjectId = new Map(projects.map((p) => [p.pccProjectId, p.status]))
  const alreadyListed = new Set<string>()
  let projectsSentToBackend = 0
  let listed = 0

  for (const project of projects) {
    if (projectsSentToBackend >= maxProjects) break

    const requests = project.steps
      .filter((step) => !alreadyListed.has(creationStepKey(step)))
      .map((step) => ({
        endpoint: step.endpoint,
        payload: buildSegmentCreationPayload(
          step,
          statusByPccProjectId.get(step.node.pccProjectId) ?? null,
        ),
      }))
    for (const step of project.steps) alreadyListed.add(creationStepKey(step))
    if (requests.length > 0) projectsSentToBackend++
    listed++
    log.info({ ...describeProject(project), requests }, '[dry-run] Would create PCC project')
  }

  return listed
}

async function reportOutcome(
  outcome: ProjectCreationOutcome,
  recordFailure: MissingProjectCreationOptions['recordFailure'],
): Promise<void> {
  const project = describeProject(outcome.project)

  if (outcome.outcome === 'created') {
    log.info(project, 'Created PCC project in CDP')
    return
  }

  if (outcome.outcome === 'waiting_for_failed_level') {
    log.warn(
      { ...project, failedLevel: describeStep(outcome.step) },
      'PCC project not created: a level it needs failed earlier in this run',
    )
    return
  }

  const details = {
    name: outcome.project.name,
    failedLevel: describeStep(outcome.step),
    httpStatus: outcome.httpStatus,
    error: outcome.error,
  }
  log.error({ ...project, ...details }, 'Failed to create PCC project in CDP')
  await recordFailure({
    pccProjectId: outcome.project.pccProjectId,
    pccSlug: outcome.project.pccSlug,
    details,
  })
}

export async function createMissingProjects(
  projects: ProjectToCreate[],
  options: MissingProjectCreationOptions,
): Promise<MissingProjectCreationSummary> {
  const summary: MissingProjectCreationSummary = {
    mode: options.mode,
    creatable: projects.length,
    created: 0,
    createdPccProjectIds: [],
    failed: 0,
    waitingForFailedLevel: 0,
    notAttempted: projects.length,
    stoppedWithoutAnswers: false,
  }

  if (options.mode === 'dry-run') {
    summary.notAttempted = projects.length - logDryRun(projects, options.maxProjects)
    return summary
  }

  const result = await runCreations(projects, {
    maxProjects: options.maxProjects,
    createStep: options.createStep,
  })

  for (const outcome of result.outcomes) {
    await reportOutcome(outcome, options.recordFailure)
    if (outcome.outcome === 'created') {
      summary.created++
      summary.createdPccProjectIds.push(outcome.project.pccProjectId)
    }
    if (outcome.outcome === 'failed') summary.failed++
    if (outcome.outcome === 'waiting_for_failed_level') summary.waitingForFailedLevel++
  }
  summary.notAttempted = result.notAttempted
  summary.stoppedWithoutAnswers = result.stoppedWithoutAnswers

  return summary
}
