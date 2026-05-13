export interface IProcessNangoWebhookArguments {
  connectionId: string
  providerConfigKey: string
  model: string
  syncType: 'INITIAL' | 'INCREMENTAL'
  nextPageCursor?: string
}

export interface ISyncGithubIntegrationArguments {
  integrationId: string
}
export interface IGithubIntegrationSyncInstructions {
  providerConfigKey: string

  reposToDelete: {
    repo: IGithubRepoData
    connectionId: string
  }[]
  duplicatesToDelete: {
    repo: IGithubRepoData
    connectionId: string
  }[]
  reposToSync: IGithubRepoData[]
}

export interface IGithubRepoData {
  owner: string
  repoName: string
}

export interface IDeleteGithubRepoConnectionArgs {
  integrationId: string
  providerConfigKey: string
  connectionId: string
  repo: IGithubRepoData
}

export interface IDeleteDuplicateGithubConnectionArgs {
  integrationId: string
  providerConfigKey: string
  connectionId: string
  repo: IGithubRepoData
}

export interface ISyncGithubRepoArgs {
  integrationId: string
  providerConfigKey: string
  repo: IGithubRepoData
}

export interface ISyncGithubRepoResult {
  skipped: boolean
}
