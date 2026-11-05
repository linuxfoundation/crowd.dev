import {
  IDbProjectCatalog,
  IDbProjectCatalogClassification,
  ProjectCatalogRoutedAction,
  isReviewAlertProvenance,
} from '@crowd/data-access-layer/src/project-catalog/types'
import { getServiceLogger } from '@crowd/logging'
import {
  ICatalogRowToClassify,
  OnboardingConfigurationError,
  organizationGroupKey,
} from '@crowd/project-onboarding'

import { IClassifyAndRouteResult } from '../types'
import { organizationMainRepositoriesFirst } from './organizationEvaluation'

const log = getServiceLogger()

export interface IClassifyAndRouteIo {
  findFresh: (id: string) => Promise<IDbProjectCatalog | null>
  classify: (row: ICatalogRowToClassify) => Promise<IDbProjectCatalogClassification>
  markClassified: (
    id: string,
    classification: IDbProjectCatalogClassification,
  ) => Promise<IDbProjectCatalog | null>
  markFailed: (id: string, reason: string) => Promise<number>
  findTrackedRepoUrls: (repoUrls: string[]) => Promise<Set<string>>
  onRowStart?: (project: IDbProjectCatalog) => void
}

function emptyRoutedCounts(): Record<ProjectCatalogRoutedAction, number> {
  return { evaluate: 0, ask: 0, review: 0, onboard: 0, skip: 0 }
}

function routedByPreviousAttempt(project: IDbProjectCatalog): ProjectCatalogRoutedAction | null {
  if (!project.classifiedAt) {
    return null
  }

  return project.action === 'review' ||
    project.action === 'ask' ||
    project.action === 'onboard' ||
    project.action === 'skip'
    ? project.action
    : null
}

// A retried batch must still owe the thread reply and the alert for a failure it recorded before
// the activity attempt died; only unclassified error rows can come from this stage.
function failedByEarlierAttempt(project: IDbProjectCatalog): boolean {
  return project.action === 'error' && project.evaluationResult === 'error' && !project.classifiedAt
}

function toRowToClassify(project: IDbProjectCatalog): ICatalogRowToClassify {
  return {
    repoUrl: project.repoUrl,
    projectName: project.projectName,
    requestText: project.requestText,
    provenance: project.provenance,
    requestedBy: project.requestedBy,
    requestHints: project.requestHints,
  }
}

async function trackedGroupedRepoUrls(
  projects: IDbProjectCatalog[],
  io: Pick<IClassifyAndRouteIo, 'findTrackedRepoUrls'>,
): Promise<Set<string>> {
  const grouped = projects.filter((project) => organizationGroupKey(project) !== null)
  if (grouped.length === 0) {
    return new Set()
  }
  return io.findTrackedRepoUrls(grouped.map((project) => project.repoUrl))
}

// A question already posted but not answered yet belongs to its message; sharing it would
// attach late siblings to a message whose count never changes.
function awaitsAnswer(row: IDbProjectCatalog): boolean {
  return row.action === 'ask' && !!row.requestQuestion?.messageTs && !row.requestQuestion.answer
}

function persistedClassification(row: IDbProjectCatalog): IDbProjectCatalogClassification {
  return {
    projectName: row.projectName,
    classificationKind: row.classificationKind ?? 'ambiguous',
    proposedAction: row.proposedAction,
    pccProjectId: row.pccProjectId,
    matchedProjectName: row.matchedProjectName,
    matchedSegmentId: row.matchedSegmentId,
    action: routedByPreviousAttempt(row) ?? 'evaluate',
    skipReason: row.skipReason,
    reviewReason: row.reviewReason,
    requestQuestion: row.requestQuestion,
  }
}

export async function classifyAndRouteProjects(
  projects: IDbProjectCatalog[],
  io: IClassifyAndRouteIo,
): Promise<IClassifyAndRouteResult> {
  const result: IClassifyAndRouteResult = {
    remaining: [],
    routed: emptyRoutedCounts(),
    breakdown: {},
    skippedHumanRequests: [],
    erroredHumanRequests: [],
    discarded: 0,
    failed: 0,
  }

  const noteFailure = (project: IDbProjectCatalog, reason: string) => {
    result.failed++
    if (isReviewAlertProvenance(project.provenance)) {
      result.erroredHumanRequests.push({ project, reason })
    }
  }

  const recordFailure = async (project: IDbProjectCatalog, reason: string) => {
    const marked = await io.markFailed(project.id, reason)
    log.warn({ id: project.id, repoUrl: project.repoUrl, reason, marked }, 'Classification failed.')
    if (marked > 0) {
      noteFailure(project, reason)
    }
  }

  const record = (project: IDbProjectCatalog, action: ProjectCatalogRoutedAction) => {
    const kind = project.classificationKind ?? 'unclassified'
    result.routed[action]++
    result.breakdown[kind] = (result.breakdown[kind] ?? 0) + 1

    if (action === 'evaluate') {
      result.remaining.push(project)
    } else if (action === 'skip' && isReviewAlertProvenance(project.provenance)) {
      result.skippedHumanRequests.push({
        project,
        reason: project.skipReason ?? '(no reason provided)',
      })
    }
  }

  // Tracking is the one per-repository fact of a group: a sibling already in LFX Insights
  // gets its own classification, and a tracked lead never speaks for the others.
  const tracked = await trackedGroupedRepoUrls(projects, io)
  const classifiedByOrganization = new Map<string, IDbProjectCatalogClassification>()
  // Only a persisted classification speaks for the group, so two overlapping runs cannot
  // give one organization two different proposals.
  const shareClassified = (row: IDbProjectCatalog) => {
    const key = organizationGroupKey(row)
    if (
      key &&
      row.classifiedAt &&
      !awaitsAnswer(row) &&
      !tracked.has(row.repoUrl) &&
      !classifiedByOrganization.has(key)
    ) {
      classifiedByOrganization.set(key, persistedClassification(row))
    }
  }
  const classifyRow = async (fresh: IDbProjectCatalog) => {
    const key = organizationGroupKey(fresh)
    const shared = key ? classifiedByOrganization.get(key) : undefined
    return shared && !tracked.has(fresh.repoUrl) ? shared : io.classify(toRowToClassify(fresh))
  }

  for (const project of projects) {
    io.onRowStart?.(project)

    const fresh = await io.findFresh(project.id)
    if (fresh) {
      shareClassified(fresh)
    }
    const previousRoute = fresh ? routedByPreviousAttempt(fresh) : null
    if (fresh && previousRoute) {
      record(fresh, previousRoute)
      continue
    }

    if (fresh && failedByEarlierAttempt(fresh)) {
      noteFailure(fresh, fresh.evaluationReason ?? '(no reason recorded)')
      continue
    }

    if (!fresh || fresh.action !== 'evaluate') {
      result.discarded++
      log.info(
        { id: project.id, repoUrl: project.repoUrl, action: fresh?.action ?? null },
        'Project was moved out of evaluate by a manual request before classification, discarding.',
      )
      continue
    }

    if (fresh.classifiedAt) {
      record(fresh, 'evaluate')
      continue
    }

    let classification: IDbProjectCatalogClassification
    try {
      classification = await classifyRow(fresh)
    } catch (err) {
      if (err instanceof OnboardingConfigurationError) {
        await recordFailure(fresh, err.message)
        continue
      }
      throw err
    }
    const updated = await io.markClassified(fresh.id, classification)
    if (updated) {
      shareClassified(updated)
    }

    if (!updated) {
      result.discarded++
      log.info(
        { id: project.id, repoUrl: project.repoUrl },
        'Project was moved out of evaluate by a manual request while classifying, discarding.',
      )
      continue
    }

    log.info(
      {
        id: updated.id,
        repoUrl: updated.repoUrl,
        classificationKind: classification.classificationKind,
        proposedAction: classification.proposedAction,
        action: classification.action,
        skipReason: classification.skipReason,
      },
      'Project classified and routed.',
    )

    record(updated, classification.action)
  }

  result.remaining = organizationMainRepositoriesFirst(result.remaining)
  return result
}
