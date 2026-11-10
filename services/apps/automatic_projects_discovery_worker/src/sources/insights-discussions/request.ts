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
  linkedRepoUrls: number
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
  const hints = requestHintsByRepoUrl(repoUrls, [], parsed)
  if (parsed.declaredLf !== null) {
    for (const repoUrl of repoUrls) {
      if (!hints.has(repoUrl)) {
        hints.set(repoUrl, { declaredLf: parsed.declaredLf })
      }
    }
  }
  return hints
}

// A question about where the project belongs is answered at classification from the text, as
// before; a parsed request is read once and its rows carry the name and the hints instead.
export function toDiscussionRequest(
  discussion: IDiscussionText,
  parsed: ParseOnboardingRequestResult | null,
): IDiscussionRequest {
  const linked = extractDiscussionRepoUrls(discussion).repoUrls
  const request = parsed?.ok === true ? parsed.request : null
  const repoUrls = request?.githubRepoUrls ?? []
  if (!request || repoUrls.length === 0 || request.asksAboutHierarchy) {
    return {
      repoUrls: linked,
      bareRepoUrls: [],
      linkedRepoUrls: linked.length,
      unlistedRepoUrls: [],
      projectName: null,
      requestText: buildDiscussionRequestText(discussion),
      parsed: null,
    }
  }

  const linkedSet = new Set(linked)
  const listedSet = new Set(repoUrls)
  return {
    repoUrls,
    bareRepoUrls: repoUrls.filter((repoUrl) => !linkedSet.has(repoUrl)),
    linkedRepoUrls: linked.length,
    unlistedRepoUrls: linked.filter((repoUrl) => !listedSet.has(repoUrl)),
    projectName: request.projectName,
    requestText: null,
    parsed: request,
  }
}
