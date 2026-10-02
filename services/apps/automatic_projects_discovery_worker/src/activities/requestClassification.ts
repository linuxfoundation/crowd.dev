import { getErrorMessage } from '@crowd/common'
import { IDbProjectCatalogCreate } from '@crowd/data-access-layer/src/project-catalog/types'
import { getServiceLogger } from '@crowd/logging'
import {
  CdpIntegrationAction,
  IOnboardingRequestLookups,
  OnboardingResolution,
  OnboardingRequestLlm,
  parseOnboardingRequest,
  resolveOnboardingRequest,
} from '@crowd/project-onboarding'

import {
  ClassificationNode,
  IClassificationTrace,
  buildClassificationLogEntry,
  createClassificationTrace,
  toClassificationNode,
  traceLookups,
} from './requestClassificationTrace'

const log = getServiceLogger()

export interface IRequestClassificationDeps {
  queryLlm: OnboardingRequestLlm
  lookups: IOnboardingRequestLookups
}

export interface IRequestClassificationAlert {
  sourceUrl: string
  repoUrls: string[]
  resolution: OnboardingResolution
}

export interface IClassifiedRows {
  rows: IDbProjectCatalogCreate[]
  alerts: IRequestClassificationAlert[]
  nodes: ClassificationNode[]
}

export const NON_GITHUB_SOURCE_SKIP_REASON = 'onboarding request has no GitHub repository'
export const LF_NOT_IN_PCC_SKIP_REASON = 'LF project not found in PCC, needs human review'
export const LF_NOT_IN_CDP_SKIP_REASON = 'LF project not in CDP yet, pending PCC sync'
export const AMBIGUOUS_SKIP_REASON = 'onboarding request could not be classified automatically'

const LF_IN_CDP_SKIP_REASONS: Record<CdpIntegrationAction, string> = {
  create_integration: 'LF project already in CDP, GitHub integration to be created',
  update_integration: 'LF project already in CDP, GitHub connection to be updated',
  human_review: 'LF project already in CDP with a GitHub v1 integration, needs human review',
}

function toSkipReason(resolution: Exclude<OnboardingResolution, { kind: 'non_lf_new_project' }>) {
  switch (resolution.kind) {
    case 'non_github_source':
      return NON_GITHUB_SOURCE_SKIP_REASON
    case 'lf_not_in_pcc':
      return LF_NOT_IN_PCC_SKIP_REASON
    case 'lf_not_in_cdp':
      return LF_NOT_IN_CDP_SKIP_REASON
    case 'lf_in_cdp':
      return LF_IN_CDP_SKIP_REASONS[resolution.action]
    case 'ambiguous':
      return `${AMBIGUOUS_SKIP_REASON}: ${resolution.reason}`
  }
}

function unresolved(reason: string): OnboardingResolution {
  return { kind: 'ambiguous', reason, candidates: [] }
}

async function resolveRequestText(
  requestText: string,
  deps: IRequestClassificationDeps,
  trace: IClassificationTrace,
): Promise<OnboardingResolution> {
  try {
    const parsed = await parseOnboardingRequest(requestText, deps.queryLlm)
    if (parsed.ok === false) {
      trace.failure = { stage: 'parse', reason: parsed.reason }
      return unresolved(`Request could not be parsed: ${parsed.reason}`)
    }

    trace.parsed = parsed.request
    return await resolveOnboardingRequest(parsed.request, traceLookups(deps.lookups, trace))
  } catch (err) {
    const reason = getErrorMessage(err)
    trace.failure = { stage: 'resolve', reason }
    return unresolved(`Classification failed: ${reason}`)
  }
}

export async function classifyDiscussionRows(
  rows: IDbProjectCatalogCreate[],
  requestText: string,
  deps: IRequestClassificationDeps,
): Promise<IClassifiedRows> {
  const trace = createClassificationTrace()
  const resolution = await resolveRequestText(requestText, deps, trace)
  const node = toClassificationNode(resolution)

  log.info(
    buildClassificationLogEntry(rows[0].sourceUrl ?? '', resolution, trace),
    'Onboarding request classified.',
  )

  if (resolution.kind === 'non_lf_new_project') {
    return { rows, alerts: [], nodes: [node] }
  }

  const skipReason = toSkipReason(resolution)

  return {
    rows: rows.map((row) => ({ ...row, action: 'skip', skipReason })),
    alerts: [
      {
        sourceUrl: rows[0].sourceUrl ?? '',
        repoUrls: rows.map((row) => row.repoUrl),
        resolution,
      },
    ],
    nodes: [node],
  }
}

function groupRowsBySourceUrl(
  rows: IDbProjectCatalogCreate[],
): Map<string, IDbProjectCatalogCreate[]> {
  const groups = new Map<string, IDbProjectCatalogCreate[]>()
  for (const row of rows) {
    if (!row.sourceUrl) {
      continue
    }
    groups.set(row.sourceUrl, [...(groups.get(row.sourceUrl) ?? []), row])
  }
  return groups
}

export async function classifyDiscussions(
  rows: IDbProjectCatalogCreate[],
  requestTextBySourceUrl: Map<string, string>,
  deps: IRequestClassificationDeps,
  onDiscussionClassified: () => void = () => undefined,
): Promise<IClassifiedRows> {
  const classifiedRows = new Map<IDbProjectCatalogCreate, IDbProjectCatalogCreate>()
  const alerts: IRequestClassificationAlert[] = []
  const nodes: ClassificationNode[] = []

  for (const [sourceUrl, discussionRows] of groupRowsBySourceUrl(rows)) {
    const requestText = requestTextBySourceUrl.get(sourceUrl)
    if (!requestText) {
      continue
    }

    const classified = await classifyDiscussionRows(discussionRows, requestText, deps)
    classified.rows.forEach((row, index) => classifiedRows.set(discussionRows[index], row))
    alerts.push(...classified.alerts)
    nodes.push(...classified.nodes)
    onDiscussionClassified()
  }

  return { rows: rows.map((row) => classifiedRows.get(row) ?? row), alerts, nodes }
}
