import {
  GithubPublicClientError,
  IPublicOrgRepos,
  extractGithubOrganizations,
  getErrorMessage,
} from '@crowd/common'
import {
  escapeSlackText,
  organizationLabel as plainOrganizationLabel,
  repositoriesWord,
} from '@crowd/slack'

import { IParsedOnboardingRequest } from './requestParser'

export const MAX_ORGANIZATION_REPOSITORIES = 50

export type OrganizationRepoLookup = (owner: string, limit: number) => Promise<IPublicOrgRepos>

export interface IExpandedOrganization {
  owner: string
  repoUrls: string[]
}

export type OrganizationExpansion =
  | { ok: true; organizations: IExpandedOrganization[] }
  | { ok: false; reply: string; reason: string }

type ParsedLinks = Pick<IParsedOnboardingRequest, 'linksToFollow'>

// A parsed request decides which links count, so an organization the requester excluded is
// never expanded; the raw text is read only when the parser was unavailable.
export function requestedOrganizations(requestText: string, parsed: ParsedLinks | null): string[] {
  return extractGithubOrganizations(parsed ? parsed.linksToFollow.join(' ') : requestText)
}

function organizationLabel(owner: string): string {
  return `*${escapeSlackText(plainOrganizationLabel(owner))}*`
}

const CDP_FALLBACK =
  'Or ask the CDP team in this channel to connect the whole organization from CDP.'

export function buildBotOrganizationNotFoundReply(owner: string): string {
  return `I couldn't find a GitHub organization called ${organizationLabel(owner)}. Check the link, or reply here mentioning me with the repository links themselves.`
}

export function buildBotOrganizationUnavailableReply(owner: string): string {
  return `GitHub isn't answering right now, so I couldn't list the repositories of ${organizationLabel(owner)}. Please try again in a few minutes.`
}

export function buildBotOrganizationTooLargeReply(owner: string, limit: number): string {
  return [
    `${organizationLabel(owner)} has more than ${limit} public repositories, and for now I take up to ${limit} at a time.`,
    `Reply here mentioning me with the repositories to add first. ${CDP_FALLBACK}`,
  ].join('\n')
}

export function buildBotOrganizationsTooLargeReply(limit: number): string {
  return [
    `Together, these organizations have more than ${limit} public repositories, and for now I take up to ${limit} per request.`,
    `Reply here mentioning me with one organization or the repositories to add first. ${CDP_FALLBACK}`,
  ].join('\n')
}

export function buildBotOrganizationEmptyReply(owner: string): string {
  return `${organizationLabel(owner)} has no public repositories I can add: archived repositories and forks are left out.`
}

export function organizationNote({ owner, repoUrls }: IExpandedOrganization): string {
  return `${organizationLabel(owner)} is a GitHub organization, so I'm adding its ${repoUrls.length} public ${repositoriesWord(repoUrls.length)} (archived ones and forks left out).`
}

export async function expandOrganizations(
  owners: string[],
  lookup: OrganizationRepoLookup,
  limit = MAX_ORGANIZATION_REPOSITORIES,
): Promise<OrganizationExpansion> {
  const organizations: IExpandedOrganization[] = []
  let remaining = limit
  for (const owner of owners) {
    if (remaining === 0) {
      return {
        ok: false,
        reply: buildBotOrganizationsTooLargeReply(limit),
        reason: `more than ${limit} repositories`,
      }
    }
    let repos: IPublicOrgRepos
    try {
      repos = await lookup(owner, remaining)
    } catch (err) {
      if (err instanceof GithubPublicClientError && err.kind === 'NOT_FOUND') {
        return { ok: false, reply: buildBotOrganizationNotFoundReply(owner), reason: err.message }
      }
      return {
        ok: false,
        reply: buildBotOrganizationUnavailableReply(owner),
        reason: getErrorMessage(err),
      }
    }
    if (repos.exceededLimit) {
      return {
        ok: false,
        reply:
          remaining === limit
            ? buildBotOrganizationTooLargeReply(owner, limit)
            : buildBotOrganizationsTooLargeReply(limit),
        reason: `more than ${limit} repositories`,
      }
    }
    if (repos.repoUrls.length === 0) {
      return {
        ok: false,
        reply: buildBotOrganizationEmptyReply(owner),
        reason: 'no public repositories',
      }
    }
    organizations.push({ owner, repoUrls: repos.repoUrls })
    remaining -= repos.repoUrls.length
  }
  return { ok: true, organizations }
}
