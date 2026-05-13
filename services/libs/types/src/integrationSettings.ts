import { PlatformType } from './enums/platforms'

// ── GitHub / GitHub-Nango ──

export interface IGithubRepo {
  url: string
  name: string
  createdAt: string
  owner: string
  available?: boolean
  fork?: boolean
  private?: boolean
  cloneUrl?: string
}

export interface IGithubOrg {
  name: string
  logo: string
  url: string
  fullSync: boolean
  updatedAt: string
  /** @deprecated Repos are stored in public.repositories table for github-nango.
   * Only used by the legacy github platform. */
  repos?: IGithubRepo[]
}

export interface IGithubIntegrationSettings {
  orgs: IGithubOrg[]
  /** @deprecated Only used by legacy github platform (processStream.ts) */
  repos?: IGithubRepo[]
  /** @deprecated Only used by legacy github platform (processStream.ts) */
  unavailableRepos?: IGithubRepo[]
  updateMemberAttributes?: boolean
}

// ── GitLab ──

export interface IGitlabProject {
  id: number
  name: string
  path_with_namespace: string
  enabled: boolean
}

export interface IGitlabGroupProject extends IGitlabProject {
  groupId: number
  groupName: string
  groupPath: string
}

export interface IGitlabIntegrationSettings {
  user: {
    id: number
    name: string
    username: string
    email: string
    avatar_url: string
    web_url: string
    [key: string]: unknown
  }
  groups: Array<{
    id: number
    name: string
    path: string
  }>
  userProjects: IGitlabProject[]
  groupProjects: Record<string, IGitlabGroupProject[]>
  updateMemberAttributes: boolean
}

// ── Discord ──

export interface IDiscordIntegrationSettings {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  channels: any[]
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  forumChannels: any[]
}

// ── Slack ──

export interface ISlackIntegrationSettings {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  channels: any[]
}

// ── Dev.to ──

export interface IDevtoArticleSettings {
  id: number
  lastCommentAt: string | null
}

export interface IDevtoIntegrationSettings {
  organizations: string[]
  users: string[]
  articles: IDevtoArticleSettings[]
}

// ── Hacker News ──

export interface IHackernewsIntegrationSettings {
  keywords: string[]
  urls: string[]
}

// ── Stack Overflow ──

export interface IStackoverflowIntegrationSettings {
  tags: string[]
  keywords: string[]
}

// ── Reddit ──

export interface IRedditIntegrationSettings {
  subreddits: string[]
}

// ── LinkedIn ──

export interface ILinkedinIntegrationSettings {
  organizations: Array<{
    name: string
    organizationUrn: string
    inUse?: boolean
  }>
  nangoId: string
}

// ── Discourse ──

export interface IDiscourseIntegrationSettings {
  apiKey: string
  apiUsername: string
  forumHostname: string
  webhookSecret: string
  updateMemberAttributes?: boolean
}

// ── Groups.io ──

export interface IGroupsioGroupDetails {
  id: number
  name: string
  slug: string
  groupAddedOn: string
}

export interface IGroupsioIntegrationSettings {
  email: string
  token: string
  groups: IGroupsioGroupDetails[]
}

// ── Git ──

export interface IGitIntegrationSettings {
  remotes: string[]
}

// ── Gerrit ──

export interface IGerritIntegrationSettings {
  remote: {
    orgURL: string
    repoNames: string[]
  }
}

// ── Jira ──

export interface IJiraIntegrationSettings {
  url: string
  auth: {
    username: string
    personalAccessToken?: string
    apiToken?: string
  }
  nangoIntegrationName: string
  adminConnectionId?: string
  projects: string[]
}

// ── Confluence ──

export interface IConfluenceIntegrationSettings {
  url: string
  spaces: string[]
  username: string
  apiToken: string
  orgAdminApiToken: string
  orgAdminId: string
  nangoIntegrationName?: string
  adminConnectionId?: string
}

// ── Settings map ──

export type IntegrationSettingsMap = {
  [PlatformType.GITHUB]: IGithubIntegrationSettings
  [PlatformType.GITHUB_NANGO]: IGithubIntegrationSettings
  [PlatformType.GITLAB]: IGitlabIntegrationSettings
  [PlatformType.DISCORD]: IDiscordIntegrationSettings
  [PlatformType.SLACK]: ISlackIntegrationSettings
  [PlatformType.DEVTO]: IDevtoIntegrationSettings
  [PlatformType.HACKERNEWS]: IHackernewsIntegrationSettings
  [PlatformType.STACKOVERFLOW]: IStackoverflowIntegrationSettings
  [PlatformType.REDDIT]: IRedditIntegrationSettings
  [PlatformType.LINKEDIN]: ILinkedinIntegrationSettings
  [PlatformType.DISCOURSE]: IDiscourseIntegrationSettings
  [PlatformType.GROUPSIO]: IGroupsioIntegrationSettings
  [PlatformType.GIT]: IGitIntegrationSettings
  [PlatformType.GERRIT]: IGerritIntegrationSettings
  [PlatformType.JIRA]: IJiraIntegrationSettings
  [PlatformType.CONFLUENCE]: IConfluenceIntegrationSettings
}

export type IntegrationSettingsFor<P extends PlatformType> = P extends keyof IntegrationSettingsMap
  ? IntegrationSettingsMap[P]
  : unknown
