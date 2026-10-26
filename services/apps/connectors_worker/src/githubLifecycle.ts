import type { ILifecycleChannel } from '@crowd/data-access-layer/src/connectors'

import { RUN_START_TO_CLOSE_TIMEOUT_MS } from './runLimits'

export const LIFECYCLE_RESOLVE_CONCURRENCY = 5
export const LIFECYCLE_SWEEP_CONCURRENCY = 5
export const LIFECYCLE_STALE_LOCK_MS = RUN_START_TO_CLOSE_TIMEOUT_MS + 5 * 60_000

const GITHUB_REPO_URL = /^https:\/\/github\.com\/[A-Za-z0-9-]+\/[A-Za-z0-9_.-]+$/

export interface IGithubLifecycleDiffInput {
  deleted: boolean
  repoUrls: string[]
  mappingKeys: string[]
  channels: ILifecycleChannel[]
  ownedElsewhere: string[]
}

export interface IGithubLifecycleDiff {
  toCreate: string[]
  toDeleteKeys: string[]
  invalidUrls: string[]
  ownedElsewhere: string[]
  deletesSuppressed: boolean
}

export type LifecycleSkipReason =
  | 'invalid_url'
  | 'owned_by_other_integration'
  | 'repo_not_found'
  | 'repo_renamed'
  | 'rate_limited'
  | 'github_error'
  | 'token_mint_failed'
  | 'already_present'
  | 'segment_not_ready'
  | 'integration_deleted'
  | 'org_not_full_sync'
  | 'no_longer_desired'
  | 'incomplete_listing'

export interface ILifecycleSkip {
  url: string
  reason: LifecycleSkipReason
  message?: string
}

export interface IGithubLifecycleSummary {
  integrationId: string
  deleted: boolean
  desired: number
  discovered: number
  created: number
  unitsInserted: number
  deletedUnits: number
  repositoriesSoftDeleted: number
  undrainedChannels: number
  deletesSuppressed: boolean
  failed: boolean
  skipped: number
  durationMs: number
}

export function githubRepoKey(url: string): string | null {
  if (!GITHUB_REPO_URL.test(url) || url.endsWith('.git')) {
    return null
  }
  const repoSegment = url.slice(url.lastIndexOf('/') + 1)
  return repoSegment === '.' || repoSegment === '..' ? null : url.toLowerCase()
}

export function diffGithubLifecycle(input: IGithubLifecycleDiffInput): IGithubLifecycleDiff {
  const owned = new Set(input.ownedElsewhere)
  const mapped = new Set(input.mappingKeys)
  const desired = new Set<string>()
  const invalidUrls: string[] = []
  const orderedKeys: string[] = []

  for (const url of input.repoUrls) {
    const key = githubRepoKey(url)
    if (key === null) {
      invalidUrls.push(url)
    } else if (!desired.has(key)) {
      desired.add(key)
      orderedKeys.push(key)
    }
  }

  const deletesAllowed = input.deleted || invalidUrls.length === 0
  const channelKeys = new Set<string>()
  const toDelete = new Set<string>()
  for (const channel of input.channels) {
    const key = githubRepoKey(channel.channelName)
    if (key === null) {
      continue
    }
    channelKeys.add(key)
    if (input.deleted || channel.hasDecommissioned || (deletesAllowed && !desired.has(key))) {
      toDelete.add(key)
    }
  }

  const toCreate = input.deleted
    ? []
    : orderedKeys.filter((key) => !channelKeys.has(key) && !mapped.has(key) && !owned.has(key))

  return {
    toCreate,
    toDeleteKeys: [...toDelete],
    invalidUrls,
    ownedElsewhere: orderedKeys.filter((key) => owned.has(key)),
    deletesSuppressed: !deletesAllowed,
  }
}

export function isRenamedRepo(key: string, channelName: string): boolean {
  return channelName.toLowerCase() !== key
}

export function reconcileGithubIntegrationWorkflowId(integrationId: string): string {
  return `connectors-reconcile-github-integration/${integrationId}`
}
