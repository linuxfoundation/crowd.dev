import { IRequestHints } from '@crowd/data-access-layer/src/project-catalog/types'
import {
  IParsedOnboardingRequest,
  ParseOnboardingRequestResult,
  requestHintsByRepoUrl,
} from '@crowd/project-onboarding'

import { buildDiscussionRequestText, extractDiscussionRepoUrls } from './parse'

export interface IDiscussionText {
  title: string
  body: string
}

export interface IDiscussionRequest {
  repoUrls: string[]
  bareRepoUrls: string[]
  linkedRepoCount: number
  unlistedRepoUrls: string[]
  projectName: string | null
  requestText: string | null
  parsed: IParsedOnboardingRequest | null
}

export function discussionHints(
  repoUrls: string[],
  parsed: IParsedOnboardingRequest | null,
): Map<string, IRequestHints> {
  if (!parsed) {
    return new Map()
  }
  const listed = requestHintsByRepoUrl(parsed.githubRepoUrls, [], parsed)
  const listedSet = new Set(parsed.githubRepoUrls)
  const mainRepoUrl = repoUrls.find((repoUrl) => listed.has(repoUrl))
  const hints = new Map<string, IRequestHints>()
  for (const repoUrl of repoUrls) {
    const hint = listed.get(repoUrl)
    if (hint) {
      hints.set(repoUrl, { ...hint, mainRepoUrl })
    } else if (parsed.declaredLf !== null && listedSet.has(repoUrl)) {
      hints.set(repoUrl, { declaredLf: parsed.declaredLf })
    }
  }
  return hints
}

function isClassifiedFromText(request: IParsedOnboardingRequest): boolean {
  return request.githubRepoUrls.length === 0 || request.asksAboutHierarchy
}

export function toDiscussionRequest(
  discussion: IDiscussionText,
  parsed: ParseOnboardingRequestResult | null,
): IDiscussionRequest {
  const linked = extractDiscussionRepoUrls(discussion).repoUrls
  const request = parsed?.ok === true ? parsed.request : null
  if (!request || isClassifiedFromText(request)) {
    return {
      repoUrls: linked,
      bareRepoUrls: [],
      linkedRepoCount: linked.length,
      unlistedRepoUrls: [],
      projectName: null,
      requestText: buildDiscussionRequestText(discussion),
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
    linkedRepoCount: linked.length,
    unlistedRepoUrls,
    projectName: request.projectName,
    requestText: null,
    parsed: request,
  }
}
