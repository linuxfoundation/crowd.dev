import { canonicalizeRepoUrl } from '@crowd/common'
import { findProjectCatalogBySourceUrl } from '@crowd/data-access-layer'
import {
  IDbProjectCatalog,
  IRequestQuestion,
} from '@crowd/data-access-layer/src/project-catalog/types'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import {
  IReviewOrganization,
  escapeSlackText,
  repositoriesWord,
  reviewGroupLabel,
} from '@crowd/slack'

import { OrganizationGroupRow, requestGroupKey, requestGroupOf } from './organizationGrouping'

export {
  createsNewProject,
  joinsExistingProject,
  onboardsWithOrganization,
  organizationOf,
  requestGroupId,
  requestGroupKey,
  requestGroupOf,
  sameReviewProposal,
  type IRequestGroup,
  type OrganizationGroupRow,
} from './organizationGrouping'

export function sameQuestion(
  question: IRequestQuestion | null,
  lead: IRequestQuestion | null,
): boolean {
  if (!question || !lead) {
    return false
  }
  return (
    question.kind === lead.kind &&
    question.text === lead.text &&
    question.options.length === lead.options.length &&
    question.options.every((option, index) => option.id === lead.options[index].id)
  )
}

export function requestGroupSiblings<T extends OrganizationGroupRow>(rows: T[], lead: T): T[] {
  const key = requestGroupKey(lead)
  if (!key) {
    return []
  }
  return rows.filter((row) => row.id !== lead.id && requestGroupKey(row) === key)
}

export async function findRequestGroupSiblings(
  qx: QueryExecutor,
  lead: IDbProjectCatalog,
  keep: (row: IDbProjectCatalog) => boolean,
): Promise<IDbProjectCatalog[]> {
  if (!lead.sourceUrl || !requestGroupKey(lead)) {
    return []
  }
  const rows = await findProjectCatalogBySourceUrl(qx, lead.sourceUrl)
  return requestGroupSiblings(rows, lead).filter(keep)
}

export type IOrganizationReview = IReviewOrganization

export function requestGroupReviewOf(
  lead: Pick<IDbProjectCatalog, 'requestHints' | 'repoUrl'>,
  siblings: number,
): IOrganizationReview | null {
  const group = requestGroupOf(lead)
  if (!group || siblings === 0) {
    return null
  }
  const repositories = siblings + 1
  if (group.kind === 'organization') {
    return { owner: group.owner, repositories }
  }
  return { owner: canonicalizeRepoUrl(lead.repoUrl)?.owner ?? '', name: group.name, repositories }
}

export function organizationSubject(organization: IOrganizationReview): string {
  return `*${escapeSlackText(reviewGroupLabel(organization))}* (${organization.repositories} ${repositoriesWord(organization.repositories)})`
}

export function hasManyRepositories(organization: IOrganizationReview | null): boolean {
  return !!organization && organization.repositories > 1
}
