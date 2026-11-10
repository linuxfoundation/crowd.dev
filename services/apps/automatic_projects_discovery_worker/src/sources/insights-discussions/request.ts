import { IRequestHints } from '@crowd/data-access-layer/src/project-catalog/types'
import {
  IExpandedOrganization,
  IParsedOnboardingRequest,
  ParseOnboardingRequestResult,
  requestHintsByRepoUrl,
  requestedOrganizations,
} from '@crowd/project-onboarding'

import { buildDiscussionRequestText, extractDiscussionRepoUrls } from './parse'

export interface IDiscussionText {
  title: string
  body: string
}

export interface IDiscussionRequest {
  repoUrls: string[]
  bareRepoUrls: string[]
  organizations: string[]
  linkedRepoCount: number
  unlistedRepoUrls: string[]
  projectName: string | null
  requestText: string | null
  parsed: IParsedOnboardingRequest | null
}

function isProjectHint(hint: IRequestHints | undefined): boolean {
  return hint !== undefined && hint.organization === undefined
}

export function discussionHints(
  repoUrls: string[],
  parsed: IParsedOnboardingRequest | null,
  organizations: IExpandedOrganization[] = [],
): Map<string, IRequestHints> {
  const listedRepoUrls = parsed?.githubRepoUrls ?? []
  const byRepoUrl = requestHintsByRepoUrl(
    listedRepoUrls,
    organizations,
    parsed ?? { projectName: null, suggestedSlug: null, confidence: null },
  )
  const listedSet = new Set(listedRepoUrls)
  const mainRepoUrl = repoUrls.find((repoUrl) => isProjectHint(byRepoUrl.get(repoUrl)))
  const hints = new Map<string, IRequestHints>()
  for (const repoUrl of repoUrls) {
    const hint = byRepoUrl.get(repoUrl)
    if (hint?.organization) {
      hints.set(repoUrl, hint)
    } else if (hint) {
      hints.set(repoUrl, { ...hint, mainRepoUrl })
    } else if (parsed && parsed.declaredLf !== null && listedSet.has(repoUrl)) {
      hints.set(repoUrl, { declaredLf: parsed.declaredLf })
    }
  }
  return hints
}

function isClassifiedFromText(request: IParsedOnboardingRequest, organizations: string[]): boolean {
  return (
    request.asksAboutHierarchy ||
    (request.githubRepoUrls.length === 0 && organizations.length === 0)
  )
}

export function toDiscussionRequest(
  discussion: IDiscussionText,
  parsed: ParseOnboardingRequestResult | null,
): IDiscussionRequest {
  const linked = extractDiscussionRepoUrls(discussion).repoUrls
  const requestText = buildDiscussionRequestText(discussion)
  const request = parsed?.ok === true ? parsed.request : null
  const organizations = requestedOrganizations(requestText, request)
  if (!request || isClassifiedFromText(request, organizations)) {
    return {
      repoUrls: linked,
      bareRepoUrls: [],
      organizations,
      linkedRepoCount: linked.length,
      unlistedRepoUrls: [],
      projectName: null,
      requestText,
      parsed: null,
    }
  }

  const listed = request.githubRepoUrls
  const linkedSet = new Set(linked)
  const listedSet = new Set(listed)
  const unlistedRepoUrls = linked.filter((repoUrl) => !listedSet.has(repoUrl))
  return {
    repoUrls: listed,
    bareRepoUrls: listed.filter((repoUrl) => !linkedSet.has(repoUrl)),
    organizations,
    linkedRepoCount: linked.length,
    unlistedRepoUrls,
    projectName: request.projectName,
    requestText: null,
    parsed: request,
  }
}
