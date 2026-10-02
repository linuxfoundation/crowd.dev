import { IDbProjectCatalogCreate } from '@crowd/data-access-layer/src/project-catalog/types'
import { getServiceLogger } from '@crowd/logging'
import {
  CdpIntegrationAction,
  ClassificationNode,
  IRequestClassificationDeps,
  OnboardingResolution,
  buildClassificationLogEntry,
  classifyOnboardingRequest,
} from '@crowd/project-onboarding'

const log = getServiceLogger()

export interface IRequestClassificationAlert {
  sourceUrl: string
  repoUrls: string[]
  resolution: OnboardingResolution
  dryRun: boolean
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

export async function classifyDiscussionRows(
  rows: IDbProjectCatalogCreate[],
  requestText: string,
  deps: IRequestClassificationDeps,
  dryRun = false,
): Promise<IClassifiedRows> {
  const { resolution, node, trace } = await classifyOnboardingRequest(requestText, deps)

  log.info(
    buildClassificationLogEntry(rows[0].sourceUrl ?? '', resolution, trace),
    'Onboarding request classified.',
  )

  const alert: IRequestClassificationAlert = {
    sourceUrl: rows[0].sourceUrl ?? '',
    repoUrls: rows.map((row) => row.repoUrl),
    resolution,
    dryRun,
  }

  if (resolution.kind === 'non_lf_new_project') {
    return { rows, alerts: dryRun ? [alert] : [], nodes: [node] }
  }

  const skipReason = toSkipReason(resolution)

  return {
    rows: rows.map((row) => ({ ...row, action: 'skip', skipReason })),
    alerts: [alert],
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
  dryRun = false,
): Promise<IClassifiedRows> {
  const classifiedRows = new Map<IDbProjectCatalogCreate, IDbProjectCatalogCreate>()
  const alerts: IRequestClassificationAlert[] = []
  const nodes: ClassificationNode[] = []

  for (const [sourceUrl, discussionRows] of groupRowsBySourceUrl(rows)) {
    const requestText = requestTextBySourceUrl.get(sourceUrl)
    if (!requestText) {
      continue
    }

    const classified = await classifyDiscussionRows(discussionRows, requestText, deps, dryRun)
    classified.rows.forEach((row, index) => classifiedRows.set(discussionRows[index], row))
    alerts.push(...classified.alerts)
    nodes.push(...classified.nodes)
    onDiscussionClassified()
  }

  return { rows: rows.map((row) => classifiedRows.get(row) ?? row), alerts, nodes }
}
