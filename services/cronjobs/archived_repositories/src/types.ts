export const GITHUB_QUEUE_NAME = 'github-archived-repo-checker'
export const GITLAB_QUEUE_NAME = 'gitlab-archived-repo-checker'
export const GERRIT_QUEUE_NAME = 'gerrit-archived-repo-checker'

export enum Platform {
  GITHUB = 'github',
  GITLAB = 'gitlab',
  GERRIT = 'gerrit',
}

export interface RepositoryStatus {
  archived: boolean
  excluded: boolean
}

export interface RepositoryToCheck {
  url: string
  gerritBaseUrl: string | null
}

export interface JobData {
  name: string
  data: {
    url: string
    platform: Platform
    gerritBaseUrl?: string
  }
}
