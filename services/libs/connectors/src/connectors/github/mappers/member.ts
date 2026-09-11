import type { GithubMember, GithubOrganization } from '../schemas'

export interface GithubOrgNode {
  databaseId?: number | null
  login: string
  name?: string | null
  url?: string | null
  websiteUrl?: string | null
  description?: string | null
  avatarUrl?: string | null
  twitterUsername?: string | null
  location?: string | null
}

export interface GithubUserNode {
  __typename?: string
  login?: string | null
  name?: string | null
  avatarUrl?: string | null
  isHireable?: boolean | null
  url?: string | null
  bio?: string | null
  company?: string | null
  location?: string | null
  email?: string | null
  websiteUrl?: string | null
  databaseId?: number | null
  id?: string | number | null
  organizations?: { nodes?: (GithubOrgNode | null)[] | null } | null
}

// https://github.com/ghost
const GHOST_MEMBER: GithubMember = {
  displayName: 'ghost',
  identities: [
    {
      platform: 'github',
      type: 'username',
      verified: true,
      value: 'ghost',
      sourceId: '10137',
    },
  ],
  attributes: {
    url: { github: 'https://github.com/ghost' },
    avatarUrl: { github: 'https://avatars.githubusercontent.com/u/10137?v=4' },
    bio: {
      github:
        "Hi, I'm @ghost! I take the place of user accounts that have been deleted.\n:ghost:\n",
    },
  },
}

function toAttributes(user: GithubUserNode): GithubMember['attributes'] {
  if (user.__typename === 'Bot') {
    return {
      url: { github: user.url ?? '' },
      avatarUrl: { github: user.avatarUrl ?? '' },
      sourceId: { github: user.id?.toString() ?? '' },
      isBot: { github: true },
    }
  }

  const attributes: GithubMember['attributes'] = {
    isHireable: { github: user.isHireable ?? false },
    url: { github: user.url ?? `https://github.com/${user.login ?? ''}` },
    bio: { github: user.bio ?? '' },
    location: { github: user.location ?? '' },
    avatarUrl: { github: user.avatarUrl ?? '' },
    company: { github: user.company ?? '' },
  }
  if (user.websiteUrl) {
    attributes.websiteUrl = { github: user.websiteUrl }
  }
  return attributes
}

function toOrganizations(user: GithubUserNode): GithubOrganization[] {
  const nodes = user.organizations?.nodes?.filter((node) => node !== null) ?? []

  return nodes.map((org) => {
    const organization: GithubOrganization = {
      displayName: org.name || org.login,
      names: [org.name, org.login].filter((name): name is string => Boolean(name)),
      description: org.description ?? null,
      location: org.location ?? null,
      logo: org.avatarUrl ?? null,
      source: 'github',
      identities: [
        {
          platform: 'github',
          type: 'username',
          value: org.login,
          verified: true,
          sourceId: org.databaseId?.toString() ?? '',
        },
      ],
    }

    if (org.websiteUrl) {
      organization.identities.push({
        platform: 'github',
        type: 'primary-domain',
        value: org.websiteUrl,
        verified: false,
      })
    }

    if (org.twitterUsername) {
      organization.identities.push({
        platform: 'twitter',
        type: 'username',
        value: org.twitterUsername,
        verified: false,
      })
    }

    return organization
  })
}

export function toMember(user: GithubUserNode | null | undefined): GithubMember {
  if (!user || !user.login) {
    return GHOST_MEMBER
  }

  if ((user.__typename !== 'User' || !user.databaseId) && user.__typename !== 'Bot') {
    return {
      displayName: user.name || user.login,
      identities: [
        {
          platform: 'github',
          type: 'username',
          verified: true,
          value: user.login,
          sourceId: user.id?.toString() ?? '',
        },
      ],
      attributes: toAttributes(user),
      organizations: [],
    }
  }

  return {
    displayName: user.login,
    identities: [
      {
        platform: 'github',
        type: 'username',
        verified: true,
        value: user.login,
        sourceId: user.databaseId?.toString() ?? '',
      },
    ],
    attributes: toAttributes(user),
    organizations: toOrganizations(user),
  }
}
