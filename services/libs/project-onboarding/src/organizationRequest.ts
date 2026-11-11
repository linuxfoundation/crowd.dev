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
export const MAX_LF_ORGANIZATION_REPOSITORIES = 500

export function organizationRepositoryLimit(declaredLf: boolean | null | undefined): number {
  return declaredLf === true ? MAX_LF_ORGANIZATION_REPOSITORIES : MAX_ORGANIZATION_REPOSITORIES
}

export type OrganizationRepoLookup = (owner: string, limit: number) => Promise<IPublicOrgRepos>

export interface IExpandedOrganization {
  owner: string
  repoUrls: string[]
}

export type OrganizationExpansionFailureKind =
  | 'not_found'
  | 'unavailable'
  | 'too_large'
  | 'too_large_together'
  | 'empty'

export interface IOrganizationExpansionFailure {
  ok: false
  kind: OrganizationExpansionFailureKind
  owner: string
  limit: number
  reply: string
  reason: string
}

export type OrganizationExpansion =
  | { ok: true; organizations: IExpandedOrganization[] }
  | IOrganizationExpansionFailure

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
  limit: number,
): Promise<OrganizationExpansion> {
  const organizations: IExpandedOrganization[] = []
  const failure = (
    owner: string,
    kind: OrganizationExpansionFailureKind,
    reply: string,
    reason: string,
  ): IOrganizationExpansionFailure => ({ ok: false, kind, owner, limit, reply, reason })
  const tooMany = `more than ${limit} repositories`
  let remaining = limit
  for (const owner of owners) {
    if (remaining === 0) {
      return failure(
        owner,
        'too_large_together',
        buildBotOrganizationsTooLargeReply(limit),
        tooMany,
      )
    }
    let repos: IPublicOrgRepos
    try {
      repos = await lookup(owner, remaining)
    } catch (err) {
      if (err instanceof GithubPublicClientError && err.kind === 'NOT_FOUND') {
        return failure(owner, 'not_found', buildBotOrganizationNotFoundReply(owner), err.message)
      }
      return failure(
        owner,
        'unavailable',
        buildBotOrganizationUnavailableReply(owner),
        getErrorMessage(err),
      )
    }
    if (repos.exceededLimit) {
      return remaining === limit
        ? failure(owner, 'too_large', buildBotOrganizationTooLargeReply(owner, limit), tooMany)
        : failure(owner, 'too_large_together', buildBotOrganizationsTooLargeReply(limit), tooMany)
    }
    if (repos.repoUrls.length === 0) {
      return failure(
        owner,
        'empty',
        buildBotOrganizationEmptyReply(owner),
        'no public repositories',
      )
    }
    organizations.push({ owner, repoUrls: repos.repoUrls })
    remaining -= repos.repoUrls.length
  }
  return { ok: true, organizations }
}
