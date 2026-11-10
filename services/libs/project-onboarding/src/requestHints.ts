import {
  IDbProjectCatalog,
  IRequestHints,
  ProjectCatalogProvenance,
  isGithubDiscussionProvenance,
} from '@crowd/data-access-layer/src/project-catalog/types'

import { IIngestProjectGroup } from './botIngestReply'
import { requestGroupOf } from './organizationGrouping'
import { IExpandedOrganization } from './organizationRequest'
import { IParsedOnboardingRequest } from './requestParser'

export type IRequestGroupInput = Pick<
  IParsedOnboardingRequest,
  'projectName' | 'suggestedSlug' | 'confidence'
> &
  Partial<Pick<IParsedOnboardingRequest, 'declaredLf'>>

function groupHints({
  projectName,
  suggestedSlug,
  confidence,
  declaredLf,
}: IRequestGroupInput): IRequestHints {
  return {
    ...(projectName ? { project: projectName.trim() } : {}),
    ...(suggestedSlug ? { suggestedSlug } : {}),
    ...(confidence !== null && confidence !== undefined ? { confidence } : {}),
    ...(declaredLf !== null && declaredLf !== undefined ? { declaredLf } : {}),
  }
}

export function requestHintsByRepoUrl(
  repoUrls: string[],
  organizations: IExpandedOrganization[],
  request: IRequestGroupInput,
): Map<string, IRequestHints> {
  const hints = new Map<string, IRequestHints>(
    organizations.flatMap((org) =>
      org.repoUrls.map((repoUrl) => [repoUrl, { organization: org.owner }] as const),
    ),
  )
  const listed = repoUrls.filter((repoUrl) => !hints.has(repoUrl))
  const name = request.projectName?.trim()
  if (name && listed.length > 1) {
    const group = { ...groupHints(request), mainRepoUrl: listed[0] }
    for (const repoUrl of listed) {
      hints.set(repoUrl, group)
    }
  }
  return hints
}

export function ingestProjectGroup(
  hints: Map<string, IRequestHints>,
  repoUrls: Iterable<string>,
): IIngestProjectGroup | null {
  const grouped = [...repoUrls].flatMap((repoUrl) => hints.get(repoUrl)?.project ?? [])
  return grouped.length > 1 ? { name: grouped[0], repositories: grouped.length } : null
}

export function isReviewedDiscussionGroup(row: {
  provenance?: ProjectCatalogProvenance | null
  requestHints?: IDbProjectCatalog['requestHints']
}): boolean {
  return (
    isGithubDiscussionProvenance(row.provenance ?? null) &&
    requestGroupOf({ requestHints: row.requestHints ?? null }) !== null
  )
}
