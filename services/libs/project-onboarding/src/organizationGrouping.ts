import type { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'

export type OrganizationGroupRow = Pick<IDbProjectCatalog, 'id' | 'sourceUrl' | 'requestHints'>

export type IRequestGroup =
  | { kind: 'organization'; owner: string }
  | { kind: 'project'; name: string }

const REVIEW_PROPOSAL_FIELDS = [
  'classificationKind',
  'proposedAction',
  'pccProjectId',
  'matchedSegmentId',
] as const

type ReviewProposalRow = Pick<IDbProjectCatalog, (typeof REVIEW_PROPOSAL_FIELDS)[number]>

export function sameReviewProposal(row: ReviewProposalRow, lead: ReviewProposalRow): boolean {
  return REVIEW_PROPOSAL_FIELDS.every((field) => row[field] === lead[field])
}

export function joinsExistingProject(row: Pick<IDbProjectCatalog, 'proposedAction'>): boolean {
  return row.proposedAction === 'create_integration' || row.proposedAction === 'update_integration'
}

export function createsNewProject(row: Pick<IDbProjectCatalog, 'proposedAction'>): boolean {
  return row.proposedAction === 'create_segment'
}

export function onboardsWithOrganization(row: Pick<IDbProjectCatalog, 'proposedAction'>): boolean {
  return joinsExistingProject(row) || createsNewProject(row)
}

export function organizationOf(row: Pick<IDbProjectCatalog, 'requestHints'>): string | null {
  return row.requestHints?.organization ?? null
}

export function requestGroupOf(row: Pick<IDbProjectCatalog, 'requestHints'>): IRequestGroup | null {
  const owner = organizationOf(row)
  if (owner) {
    return { kind: 'organization', owner }
  }
  const name = row.requestHints?.project?.trim()
  return name ? { kind: 'project', name } : null
}

export function requestGroupId(group: IRequestGroup): string {
  return group.kind === 'organization'
    ? `org:${group.owner}`
    : `project:${group.name.toLowerCase()}`
}

export function requestGroupKey(row: OrganizationGroupRow): string | null {
  const group = requestGroupOf(row)
  return group && row.sourceUrl ? `${row.sourceUrl}|${requestGroupId(group)}` : null
}
