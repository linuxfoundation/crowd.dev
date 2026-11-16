import { organizationLabel } from '@crowd/slack'

import { escapeGithubMarkdown } from './githubMarkdown'
import { IOrganizationExpansionFailure } from './organizationRequest'

const CDP_FALLBACK = 'or ask the CDP team to connect the whole organization from CDP.'

function boldOrganization(owner: string): string {
  return `**${escapeGithubMarkdown(organizationLabel(owner))}**`
}

export function organizationFailureNote({
  kind,
  owner,
  limit,
}: IOrganizationExpansionFailure): string {
  switch (kind) {
    case 'not_found':
      return `I couldn't find a GitHub organization called ${boldOrganization(owner)}. Check the link, or edit this discussion with the repository links themselves.`
    case 'unavailable':
      return `GitHub isn't answering right now, so I couldn't list the repositories of ${boldOrganization(owner)}. I'll try again at the next run.`
    case 'too_large':
      return `${boldOrganization(owner)} has more than ${limit} public repositories, and for now I take up to ${limit} at a time. Edit this discussion with the repositories to add first, ${CDP_FALLBACK}`
    case 'too_large_together':
      return `Together, these organizations have more than ${limit} public repositories, and for now I take up to ${limit} per request. Edit this discussion with one organization or the repositories to add first, ${CDP_FALLBACK}`
    case 'empty':
      return `${boldOrganization(owner)} has no public repositories I can add: archived repositories and forks are left out.`
  }
}
