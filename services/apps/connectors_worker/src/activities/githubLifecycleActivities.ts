import { Context } from '@temporalio/activity'

import { generateUUIDv4 } from '@crowd/common'
import { type Credential, getCredential } from '@crowd/connectors'
import {
  mintInstallationToken,
  resolveInstallationId,
} from '@crowd/connectors/src/connectors/github/appToken'
import {
  type GithubOrgRepo,
  listOrgRepos,
  resolveRepo,
} from '@crowd/connectors/src/connectors/github/discover'
import { parseRepoChannel } from '@crowd/connectors/src/connectors/github/paging'
import type { Channel } from '@crowd/connectors/src/types'
import {
  countUndrainedDecommissionedSyncUnits,
  decommissionAllSyncUnits,
  decommissionSyncUnitsForChannels,
  deleteDrainedSyncUnits,
  listDecommissionedSyncUnitChannels,
  listSyncUnitChannelsForIntegration,
} from '@crowd/data-access-layer/src/connectors'
import {
  type IGithubLifecycleIntegration,
  type NangoMappingEntry,
  addRepoToGitIntegration,
  fetchGithubLifecycleIntegration,
  getNangoMappingsForIntegrations,
  listGithubChannelsOwnedElsewhere,
  listGithubIntegrationsNeedingReconcile,
  listGithubRepoKeysUnderOwners,
} from '@crowd/data-access-layer/src/integrations'
import { type QueryExecutor, dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import {
  findIntegrationRepositoryUrl,
  findRepositoryByUrlInsensitive,
  findRepositoryOwnersForSegment,
  getReposForGithubIntegration,
  softDeleteRepositoriesForIntegration,
  upsertRepository,
} from '@crowd/data-access-layer/src/repositories'
import { githubChannelName } from '@crowd/nango'
import { PlatformType } from '@crowd/types'

import {
  type IGithubLifecycleDiff,
  type IGithubLifecycleSummary,
  type ILifecycleSkip,
  LIFECYCLE_RESOLVE_CONCURRENCY,
  type LifecycleSkipReason,
  LIFECYCLE_STALE_LOCK_MS,
  diffGithubLifecycle,
  githubRepoKey,
} from '../githubLifecycle'
import {
  githubResolveSkipReason,
  lifecycleSkipReasonFor,
  renameProbeFailure,
} from '../githubSkipReasons'
import { svc } from '../main'
import { chunk } from '../mirrorSweepLimits'
import { type OnboardRepoResult, onboardGithubRepo } from '../onboardGithubRepo'

const MAX_INITIAL_DELAY_SECONDS = 3600
const INITIAL_DELAY_STEP_SECONDS = 2
const MAX_RATE_LIMIT_WAIT_MS = 15 * 60_000
const RATE_LIMIT_HEARTBEAT_SLICE_MS = 20_000
const TOKEN_REFRESH_MARGIN_MS = 5 * 60_000
const GITHUB_ORG_NAME = /^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$/
const ORG_RENAME_PROBE_MAX_KEYS = 10
const ONBOARD_RECHECK_EVERY_N_CHUNKS = 20

export interface IDiscoverFullSyncReposResult {
  discovered: Channel[]
  skipped: ILifecycleSkip[]
  renameValidationComplete: boolean
}

export interface IGithubLifecyclePlan extends IGithubLifecycleDiff {
  found: boolean
  githubNango: boolean
  deleted: boolean
  desired: number
}

export interface IOnboardReposResult {
  created: number
  unitsInserted: number
  skipped: number
  integrationDeleted: boolean
}

interface IRepositoryOwners {
  segmentId: string
  gitIntegrationId: string
  insightsProjectId: string
}

interface IFreshRepo {
  repo: GithubOrgRepo
  forkedFrom: string | null
}

type TokenSource = () => Promise<string>

interface IDiscoveryContext {
  qx: QueryExecutor
  token: TokenSource
  integrationId: string
  owners: IRepositoryOwners
  result: IDiscoverFullSyncReposResult
}

interface IListedOrg {
  org: string
  repos: GithubOrgRepo[]
}

interface IOrgListing {
  listed: IListedOrg[]
  failed: string[]
  gone: string[]
  rateLimitedBy: string | null
}

interface IOrgRenames {
  names: Map<string, string>
  movedKeys: string[]
}

interface IOwnerProbe {
  probed: string[]
  url: string | null
}

interface IDiscoveryAbort {
  reason: LifecycleSkipReason
  message?: string
  key?: string
}

type DiscoveryState = 'full_sync' | 'org_not_full_sync' | 'integration_deleted'

type RenameProbe =
  | { outcome: 'resolved'; url: string }
  | { outcome: 'gone' }
  | { outcome: 'failed'; message: string }
  | { outcome: 'rate_limited'; message?: string }

type RateLimitedResult = Extract<OnboardRepoResult, { outcome: 'skipped' }>

const NO_DISCOVERY: IDiscoverFullSyncReposResult = {
  discovered: [],
  skipped: [],
  renameValidationComplete: true,
}

function writerQx(): QueryExecutor {
  return dbStoreQx(svc.postgres.writer)
}

function staleLockBefore(): Date {
  return new Date(Date.now() - LIFECYCLE_STALE_LOCK_MS)
}

function errorMessage(err: unknown): string | undefined {
  return err instanceof Error ? err.message : undefined
}

function heartbeat(): void {
  Context.current().heartbeat()
}

async function sleepWithHeartbeats(ms: number): Promise<void> {
  for (let remaining = ms; remaining > 0; remaining -= RATE_LIMIT_HEARTBEAT_SLICE_MS) {
    heartbeat()
    await Context.current().sleep(Math.min(remaining, RATE_LIMIT_HEARTBEAT_SLICE_MS))
  }
}

async function resolveInstallation(
  qx: QueryExecutor,
  integrationId: string,
): Promise<{ credential: Credential; installationId: string }> {
  const credential = await getCredential(qx, integrationId)
  return { credential, installationId: await resolveInstallationId(credential) }
}

function installationTokenSource(qx: QueryExecutor, integrationId: string): TokenSource {
  let installation: ReturnType<typeof resolveInstallation> | null = null
  let current: { token: string; expiresAt: string } | null = null
  return async () => {
    if (current && Date.parse(current.expiresAt) - Date.now() >= TOKEN_REFRESH_MARGIN_MS) {
      return current.token
    }
    installation ??= resolveInstallation(qx, integrationId).catch((err: unknown) => {
      installation = null
      throw err
    })
    const { credential, installationId } = await installation
    current = await mintInstallationToken(credential, installationId)
    return current.token
  }
}

function isLiveGithubNango(
  integration: IGithubLifecycleIntegration | null,
): integration is IGithubLifecycleIntegration {
  return (
    integration !== null &&
    integration.platform === PlatformType.GITHUB_NANGO &&
    integration.deletedAt === null
  )
}

function liveRepoKeys(urls: string[]): string[] {
  return [...new Set(urls.map(githubRepoKey).filter((key): key is string => key !== null))]
}

function nangoMappingUrls(
  mappings: Record<string, Record<string, NangoMappingEntry>>,
  integrationId: string,
): string[] {
  return Object.values(mappings[integrationId] ?? {}).map(({ owner, repoName }) =>
    githubChannelName(owner, repoName),
  )
}

function fullSyncOrgs(integration: IGithubLifecycleIntegration): string[] {
  return (integration.settings?.orgs ?? [])
    .filter((org) => org.fullSync === true)
    .map((org) => org.name)
}

async function discoveryState(
  qx: QueryExecutor,
  integrationId: string,
  org: string,
): Promise<DiscoveryState> {
  const integration = await fetchGithubLifecycleIntegration(qx, integrationId)
  if (!isLiveGithubNango(integration)) {
    return 'integration_deleted'
  }
  return fullSyncOrgs(integration).includes(org) ? 'full_sync' : 'org_not_full_sync'
}

async function mint(token: TokenSource): Promise<string | IDiscoveryAbort> {
  try {
    return await token()
  } catch (err) {
    return { reason: lifecycleSkipReasonFor('mint', err), message: errorMessage(err) }
  }
}

function ownerOf(key: string): string {
  return (key.split('/')[3] ?? '').toLowerCase()
}

async function findOwners(
  qx: QueryExecutor,
  segmentId: string | null,
): Promise<IRepositoryOwners | null> {
  if (!segmentId) {
    return null
  }
  const owners = await findRepositoryOwnersForSegment(qx, segmentId)
  return owners ? { segmentId, ...owners } : null
}

async function resolveForkedFrom(
  token: string,
  repo: GithubOrgRepo,
): Promise<IFreshRepo | ILifecycleSkip> {
  if (!repo.fork) {
    return { repo, forkedFrom: null }
  }
  const { owner, repo: name } = parseRepoChannel(repo.channelName)
  try {
    return { repo, forkedFrom: (await resolveRepo(token, owner, name)).forkedFrom }
  } catch (err) {
    return {
      url: repo.channelName,
      reason: lifecycleSkipReasonFor('resolve', err),
      message: errorMessage(err),
    }
  }
}

async function persistRepo(
  qx: QueryExecutor,
  integrationId: string,
  owners: IRepositoryOwners,
  { repo, forkedFrom }: IFreshRepo,
): Promise<ILifecycleSkip | null> {
  const url = repo.channelName
  const ownedElsewhere = {
    url,
    reason: 'owned_by_other_integration' as const,
    message: `repository ${url} belongs to another integration`,
  }
  return qx.tx(async (txQx) => {
    const existing = await findRepositoryByUrlInsensitive(txQx, url)
    if (existing && existing.sourceIntegrationId !== integrationId) {
      return ownedElsewhere
    }
    if (!existing) {
      const outcome = await upsertRepository(txQx, {
        id: generateUUIDv4(),
        url,
        segmentId: owners.segmentId,
        gitIntegrationId: owners.gitIntegrationId,
        sourceIntegrationId: integrationId,
        insightsProjectId: owners.insightsProjectId,
        forkedFrom,
      })
      if (outcome === 'exists') {
        return ownedElsewhere
      }
    }
    const storedUrl = existing ? await findIntegrationRepositoryUrl(txQx, integrationId, url) : null
    await addRepoToGitIntegration(txQx, integrationId, storedUrl ?? url)
    return null
  })
}

function freshOrgRepos(
  repos: GithubOrgRepo[],
  known: Set<string>,
  renameSkips: Map<string, string>,
  skipped: ILifecycleSkip[],
): GithubOrgRepo[] {
  const fresh: GithubOrgRepo[] = []
  for (const repo of repos) {
    const key = githubRepoKey(repo.channelName)
    if (key === null) {
      skipped.push({ url: repo.channelName, reason: 'invalid_url' })
    } else if (renameSkips.has(key)) {
      skipped.push({ url: repo.channelName, reason: 'repo_renamed', message: renameSkips.get(key) })
    } else if (!known.has(key)) {
      known.add(key)
      fresh.push(repo)
    }
  }
  return fresh
}

async function probeRepoName(
  token: string,
  key: string,
  skipped: ILifecycleSkip[],
): Promise<RenameProbe> {
  let parsed: ReturnType<typeof parseRepoChannel>
  try {
    parsed = parseRepoChannel(key)
  } catch (err) {
    skipped.push({ url: key, reason: 'invalid_url', message: errorMessage(err) })
    return { outcome: 'gone' }
  }
  const { owner, repo } = parsed
  try {
    return {
      outcome: 'resolved',
      url: (await resolveRepo(token, owner, repo)).channelName.toLowerCase(),
    }
  } catch (err) {
    const failure = renameProbeFailure(err)
    if (failure === 'rate_limited') {
      return { outcome: 'rate_limited', message: errorMessage(err) }
    }
    if (failure === 'unknown') {
      return {
        outcome: 'failed',
        message: `could not resolve ${key}: ${errorMessage(err) ?? 'unknown error'}`,
      }
    }
    const reason = githubResolveSkipReason(err)
    if (reason !== 'repo_not_found') {
      skipped.push({ url: key, reason, message: errorMessage(err) })
    }
    return { outcome: 'gone' }
  }
}

function probeAbort(probe: RenameProbe, key: string): IDiscoveryAbort | null {
  switch (probe.outcome) {
    case 'rate_limited':
      return { reason: 'rate_limited', message: probe.message, key }
    case 'failed':
      return { reason: 'incomplete_listing', message: probe.message, key }
    default:
      return null
  }
}

async function resolveCurrentNames(
  token: TokenSource,
  keys: string[],
  skipped: ILifecycleSkip[],
): Promise<Map<string, string> | IDiscoveryAbort> {
  const names = new Map<string, string>()
  for (const group of chunk(keys, LIFECYCLE_RESOLVE_CONCURRENCY)) {
    const current = await mint(token)
    if (typeof current !== 'string') {
      return current
    }
    const probes = await Promise.all(group.map((key) => probeRepoName(current, key, skipped)))
    for (const [i, probe] of probes.entries()) {
      const abort = probeAbort(probe, group[i])
      if (abort) {
        return abort
      }
      if (probe.outcome === 'resolved') {
        names.set(group[i], probe.url)
      }
    }
    heartbeat()
  }
  return names
}

function keysByOwner(keys: string[]): Map<string, string[]> {
  const byOwner = new Map<string, string[]>()
  for (const key of keys) {
    byOwner.set(ownerOf(key), [...(byOwner.get(ownerOf(key)) ?? []), key])
  }
  return byOwner
}

async function probeOwner(
  token: TokenSource,
  keys: string[],
  skipped: ILifecycleSkip[],
): Promise<IOwnerProbe | IDiscoveryAbort> {
  const probed: string[] = []
  for (const key of keys.slice(0, ORG_RENAME_PROBE_MAX_KEYS)) {
    const current = await mint(token)
    if (typeof current !== 'string') {
      return current
    }
    const probe = await probeRepoName(current, key, skipped)
    heartbeat()
    probed.push(key)
    const abort = probeAbort(probe, key)
    if (abort) {
      return abort
    }
    if (probe.outcome === 'resolved') {
      return { probed, url: probe.url }
    }
  }
  return { probed, url: null }
}

async function renamedOrgKeys(
  token: TokenSource,
  ownKeys: string[],
  listedOwners: Set<string>,
  goneOwners: Set<string>,
  skipped: ILifecycleSkip[],
): Promise<IOrgRenames | IDiscoveryAbort> {
  const renames: IOrgRenames = { names: new Map(), movedKeys: [] }
  for (const [owner, keys] of keysByOwner(
    ownKeys.filter((key) => !listedOwners.has(ownerOf(key))),
  )) {
    const probe = await probeOwner(token, keys, skipped)
    if ('reason' in probe) {
      return probe
    }
    if (probe.url === null) {
      if (!goneOwners.has(owner)) {
        skipped.push({
          url: owner,
          reason: 'repo_not_found',
          message: 'none of the probed repositories exist on GitHub',
        })
      }
      continue
    }
    const probedKey = probe.probed[probe.probed.length - 1]
    if (probe.url !== probedKey) {
      renames.names.set(probedKey, probe.url)
    }
    if (ownerOf(probe.url) !== owner) {
      renames.movedKeys.push(...keys.filter((key) => !probe.probed.includes(key)))
    }
  }
  return renames
}

async function renamedTargets(
  { qx, token, result }: IDiscoveryContext,
  { listed, gone }: IOrgListing,
  ownKeys: string[],
): Promise<Map<string, string> | IDiscoveryAbort> {
  const listedOwners = new Set(listed.map(({ org }) => org.toLowerCase()))
  const listedKeys = new Set(
    listed.flatMap(({ repos }) => repos.map((repo) => repo.channelName.toLowerCase())),
  )
  const candidates = liveRepoKeys(await listGithubRepoKeysUnderOwners(qx, [...listedOwners]))
  const orgRenames = await renamedOrgKeys(
    token,
    ownKeys,
    listedOwners,
    new Set(gone.map((org) => org.toLowerCase())),
    result.skipped,
  )
  if ('reason' in orgRenames) {
    return orgRenames
  }
  const missing = [
    ...new Set([...candidates.filter((key) => !listedKeys.has(key)), ...orgRenames.movedKeys]),
  ]
  const names = await resolveCurrentNames(token, missing, result.skipped)
  if (!(names instanceof Map)) {
    return names
  }
  const renamedFrom = new Map<string, string>()
  for (const [key, url] of [...orgRenames.names, ...names]) {
    if (url !== key) {
      renamedFrom.set(url, key)
    }
  }
  return renamedFrom
}

function renameSkipMessages(
  renamedFrom: Map<string, string>,
  ownKeys: Set<string>,
): Map<string, string> {
  return new Map(
    [...renamedFrom].map(([newKey, oldKey]) => [
      newKey,
      ownKeys.has(oldKey)
        ? `${newKey} is the new name of ${oldKey}, which this integration already tracks`
        : `${newKey} is the new name of ${oldKey}, a repository tracked elsewhere in CDP`,
    ]),
  )
}

function listingAbort({ failed, rateLimitedBy }: IOrgListing): IDiscoveryAbort | null {
  if (rateLimitedBy !== null) {
    return { reason: 'rate_limited', message: `rate limited while listing ${rateLimitedBy}` }
  }
  return failed.length > 0
    ? { reason: 'incomplete_listing', message: `listing failed for ${failed.join(', ')}` }
    : null
}

function skipOrgs(
  orgs: string[],
  { reason, message }: IDiscoveryAbort,
  skipped: ILifecycleSkip[],
): void {
  skipped.push(...orgs.map((org) => ({ url: org, reason, message })))
}

function unlistedRenameSkips(
  renamedFrom: Map<string, string>,
  listed: IListedOrg[],
): ILifecycleSkip[] {
  const listedKeys = new Set(
    listed.flatMap(({ repos }) => repos.map((repo) => repo.channelName.toLowerCase())),
  )
  return [...renamedFrom]
    .filter(([newKey]) => !listedKeys.has(newKey))
    .map(([newKey, oldKey]) => ({
      url: newKey,
      reason: 'repo_renamed',
      message: `renamed from ${oldKey}`,
    }))
}

async function discoverOrgRepos(
  { qx, token, integrationId, owners, result }: IDiscoveryContext,
  org: string,
  repos: GithubOrgRepo[],
): Promise<DiscoveryState> {
  const groups = chunk(repos, LIFECYCLE_RESOLVE_CONCURRENCY)
  for (const [index, group] of groups.entries()) {
    const current = await token()
    const resolutions = await Promise.all(group.map((repo) => resolveForkedFrom(current, repo)))
    const state = await discoveryState(qx, integrationId, org)
    if (state !== 'full_sync') {
      return state
    }
    const rateLimited = resolutions.some(
      (resolution) => !('repo' in resolution) && resolution.reason === 'rate_limited',
    )
    for (const resolution of resolutions) {
      if (!('repo' in resolution)) {
        result.skipped.push(resolution)
        continue
      }
      const skip = await persistRepo(qx, integrationId, owners, resolution)
      if (skip) {
        result.skipped.push(skip)
      } else {
        result.discovered.push({
          channelId: resolution.repo.channelId,
          channelName: resolution.repo.channelName,
        })
      }
    }
    heartbeat()
    if (rateLimited) {
      result.skipped.push(
        ...groups
          .slice(index + 1)
          .flat()
          .map((repo) => ({
            url: repo.channelName,
            reason: 'rate_limited' as const,
            message: `rate limited while resolving forks of ${org}`,
          })),
      )
      return 'full_sync'
    }
  }
  return 'full_sync'
}

async function listFullSyncOrgs(
  token: TokenSource,
  orgs: string[],
  skipped: ILifecycleSkip[],
): Promise<IOrgListing> {
  const listing: IOrgListing = { listed: [], failed: [], gone: [], rateLimitedBy: null }
  for (const [index, org] of orgs.entries()) {
    heartbeat()
    if (!GITHUB_ORG_NAME.test(org)) {
      skipped.push({ url: org, reason: 'invalid_url', message: 'invalid github org name' })
      continue
    }

    const current = await mint(token)
    if (typeof current !== 'string') {
      skipped.push({ url: org, ...current })
      listing.failed.push(org)
      continue
    }

    try {
      listing.listed.push({ org, repos: await listOrgRepos(current, org, heartbeat) })
    } catch (err) {
      if (renameProbeFailure(err) === 'gone') {
        skipped.push({ url: org, reason: 'repo_not_found', message: 'org not found on GitHub' })
        listing.gone.push(org)
        continue
      }
      const reason = githubResolveSkipReason(err)
      skipped.push({ url: org, reason, message: errorMessage(err) })
      listing.failed.push(org)
      if (reason === 'rate_limited') {
        listing.rateLimitedBy = org
        skipped.push(
          ...orgs.slice(index + 1).map((remaining) => ({
            url: remaining,
            reason,
            message: `rate limited while listing ${org}`,
          })),
        )
        break
      }
    }
  }
  return listing
}

async function persistListedOrgs(
  context: IDiscoveryContext,
  listed: IListedOrg[],
  known: Set<string>,
  renameSkips: Map<string, string>,
): Promise<void> {
  const { skipped } = context.result
  for (const [index, { org, repos }] of listed.entries()) {
    const state = await discoverOrgRepos(
      context,
      org,
      freshOrgRepos(repos, known, renameSkips, skipped),
    )
    if (state === 'org_not_full_sync') {
      skipped.push({ url: org, reason: state, message: 'org no longer fullSync during discovery' })
    }
    if (state === 'integration_deleted') {
      skipped.push(
        ...listed.slice(index).map((remaining) => ({
          url: remaining.org,
          reason: state,
          message: 'integration deleted during discovery',
        })),
      )
      return
    }
  }
}

export async function discoverFullSyncRepos({
  integrationId,
}: {
  integrationId: string
}): Promise<IDiscoverFullSyncReposResult> {
  const qx = writerQx()
  const integration = await fetchGithubLifecycleIntegration(qx, integrationId)
  if (!isLiveGithubNango(integration)) {
    return NO_DISCOVERY
  }
  const orgs = fullSyncOrgs(integration)
  if (orgs.length === 0) {
    return NO_DISCOVERY
  }

  const owners = await findOwners(qx, integration.segmentId)
  if (!owners) {
    return {
      discovered: [],
      skipped: orgs.map((org) => ({
        url: org,
        reason: 'segment_not_ready',
        message: 'segment has no git integration or insights project',
      })),
      renameValidationComplete: false,
    }
  }

  const [repoRows, mappings] = await Promise.all([
    getReposForGithubIntegration(qx, integrationId),
    getNangoMappingsForIntegrations(qx, [integrationId]),
  ])
  const repoKeys = liveRepoKeys(repoRows.map((r) => r.url))
  const ownKeys = liveRepoKeys([...repoKeys, ...nangoMappingUrls(mappings, integrationId)])
  const token = installationTokenSource(qx, integrationId)
  const result: IDiscoverFullSyncReposResult = {
    discovered: [],
    skipped: [],
    renameValidationComplete: true,
  }
  const context: IDiscoveryContext = { qx, token, integrationId, owners, result }

  const listing = await listFullSyncOrgs(token, orgs, result.skipped)
  const { listed } = listing
  const incomplete = listingAbort(listing)
  const listedOrgs = listed.map(({ org }) => org)
  if (incomplete) {
    skipOrgs(listedOrgs, incomplete, result.skipped)
    return { ...result, renameValidationComplete: false }
  }

  const renamedFrom = await renamedTargets(context, listing, ownKeys)
  if (!(renamedFrom instanceof Map)) {
    skipOrgs(
      listed.length > 0 ? listedOrgs : [renamedFrom.key ?? integrationId],
      renamedFrom,
      result.skipped,
    )
    return { ...result, renameValidationComplete: false }
  }
  result.skipped.push(...unlistedRenameSkips(renamedFrom, listed))

  await persistListedOrgs(
    context,
    listed,
    new Set(repoKeys),
    renameSkipMessages(renamedFrom, new Set(ownKeys)),
  )
  return result
}

export async function planGithubLifecycle({
  integrationId,
}: {
  integrationId: string
}): Promise<IGithubLifecyclePlan> {
  const qx = writerQx()
  const integration = await fetchGithubLifecycleIntegration(qx, integrationId)
  const nothing = {
    desired: 0,
    toCreate: [],
    toDeleteKeys: [],
    invalidUrls: [],
    ownedElsewhere: [],
    deletesSuppressed: false,
  }
  if (!integration) {
    return { found: false, githubNango: false, deleted: false, ...nothing }
  }
  const deleted = integration.deletedAt !== null
  if (integration.platform !== PlatformType.GITHUB_NANGO) {
    return { found: true, githubNango: false, deleted, ...nothing }
  }

  const [repos, mappings, channels] = await Promise.all([
    getReposForGithubIntegration(qx, integrationId),
    getNangoMappingsForIntegrations(qx, [integrationId]),
    listSyncUnitChannelsForIntegration(qx, integrationId),
  ])
  const repoUrls = repos.map((repo) => repo.url)
  const desiredKeys = liveRepoKeys(repoUrls)
  const ownedElsewhere = deleted
    ? []
    : await listGithubChannelsOwnedElsewhere(qx, integrationId, desiredKeys)

  const diff = diffGithubLifecycle({
    deleted,
    repoUrls,
    mappingKeys: nangoMappingUrls(mappings, integrationId).map((url) => url.toLowerCase()),
    channels,
    ownedElsewhere,
  })

  return { found: true, githubNango: true, deleted, desired: desiredKeys.length, ...diff }
}

function isRateLimited(result: OnboardRepoResult): result is RateLimitedResult {
  return result.outcome === 'skipped' && result.reason === 'rate_limited'
}

function rateLimitedUntil(results: RateLimitedResult[]): number {
  const now = Date.now()
  const cap = now + MAX_RATE_LIMIT_WAIT_MS
  const resets = results.map(({ resetAt }) => (resetAt === undefined ? cap : Date.parse(resetAt)))
  return Math.min(Math.max(...resets, now), cap)
}

function initialDelaySeconds(index: number): number {
  return Math.min(index * INITIAL_DELAY_STEP_SECONDS, MAX_INITIAL_DELAY_SECONDS)
}

function logRepoSkipped(integrationId: string, skip: ILifecycleSkip): void {
  svc.log.warn(
    {
      event: 'lifecycle_repo_skipped',
      integrationId,
      url: skip.url,
      reason: skip.reason,
      message: skip.message,
    },
    'repo skipped by github lifecycle',
  )
}

function recordOnboardOutcomes(
  integrationId: string,
  keys: string[],
  outcomes: OnboardRepoResult[],
  result: IOnboardReposResult,
): void {
  outcomes.forEach((outcome, i) => {
    if (outcome.outcome === 'created') {
      result.created++
      result.unitsInserted += outcome.unitsInserted
      return
    }
    result.skipped++
    logRepoSkipped(integrationId, {
      url: keys[i],
      reason: outcome.reason,
      message: outcome.message,
    })
  })
}

async function desiredReposIfLive(
  qx: QueryExecutor,
  integrationId: string,
): Promise<Map<string, string> | null> {
  if (!isLiveGithubNango(await fetchGithubLifecycleIntegration(qx, integrationId))) {
    return null
  }
  const stored = new Map<string, string>()
  for (const { url } of await getReposForGithubIntegration(qx, integrationId)) {
    const key = githubRepoKey(url)
    if (key !== null && !stored.has(key)) {
      stored.set(key, url)
    }
  }
  return stored
}

function recordSkips(
  integrationId: string,
  keys: string[],
  reason: LifecycleSkipReason,
  result: IOnboardReposResult,
): void {
  recordOnboardOutcomes(
    integrationId,
    keys,
    keys.map(() => ({ outcome: 'skipped', reason })),
    result,
  )
}

interface IOnboardContext {
  qx: QueryExecutor
  integrationId: string
  result: IOnboardReposResult
  token: TokenSource
  onboard: (token: string, key: string) => Promise<OnboardRepoResult>
}

interface IRetryOutcome {
  outcomes: Map<string, OnboardRepoResult>
  desired: Set<string> | null
  limitedUntil: number | null
}

function narrowDesired(
  { integrationId, result }: Pick<IOnboardContext, 'integrationId' | 'result'>,
  desired: Set<string>,
  latest: ReadonlyMap<string, string> | null,
  pending: string[],
): Set<string> | null {
  const stillPending = pending.filter((key) => desired.has(key))
  if (latest === null) {
    recordSkips(integrationId, stillPending, 'integration_deleted', result)
    result.integrationDeleted = true
    return null
  }
  recordSkips(
    integrationId,
    stillPending.filter((key) => !latest.has(key)),
    'no_longer_desired',
    result,
  )
  return new Set([...desired].filter((key) => latest.has(key)))
}

async function refreshDesired(
  context: IOnboardContext,
  desired: Set<string>,
  pending: string[],
): Promise<Set<string> | null> {
  let latest: Map<string, string> | null
  try {
    latest = await desiredReposIfLive(context.qx, context.integrationId)
  } catch (err) {
    svc.log.warn(
      { event: 'lifecycle_onboard_recheck_failed', integrationId: context.integrationId, err },
      'could not re-check desired repos, keeping the previous set',
    )
    return desired
  }
  return narrowDesired(context, desired, latest, pending)
}

async function onboardGroup(
  { token, onboard }: IOnboardContext,
  group: string[],
): Promise<Map<string, OnboardRepoResult>> {
  const current = await token()
  return new Map(
    await Promise.all(group.map(async (key) => [key, await onboard(current, key)] as const)),
  )
}

async function retryRateLimited(
  context: IOnboardContext,
  outcomes: Map<string, OnboardRepoResult>,
  desired: Set<string>,
  later: string[],
): Promise<IRetryOutcome> {
  const limited = [...outcomes].flatMap(([key, outcome]) =>
    isRateLimited(outcome) ? [{ key, outcome }] : [],
  )
  if (limited.length === 0) {
    return { outcomes, desired, limitedUntil: null }
  }
  heartbeat()
  await sleepWithHeartbeats(rateLimitedUntil(limited.map(({ outcome }) => outcome)) - Date.now())
  const limitedKeys = limited.map(({ key }) => key)
  const refreshed = await refreshDesired(context, desired, [...limitedKeys, ...later])
  const retryable = refreshed === null ? [] : limitedKeys.filter((key) => refreshed.has(key))
  const merged = new Map(
    [...outcomes].filter(([key]) => !limitedKeys.includes(key) || retryable.includes(key)),
  )
  if (retryable.length === 0) {
    return { outcomes: merged, desired: refreshed, limitedUntil: null }
  }
  const retried = await onboardGroup(context, retryable)
  for (const [key, outcome] of retried) {
    merged.set(key, outcome)
  }
  const stillLimited = [...retried.values()].filter(isRateLimited)
  return {
    outcomes: merged,
    desired: refreshed,
    limitedUntil: stillLimited.length > 0 ? rateLimitedUntil(stillLimited) : null,
  }
}

export async function onboardGithubRepos({
  integrationId,
  keys,
  resolved,
  startIndex = 0,
}: {
  integrationId: string
  keys: string[]
  resolved: Record<string, Channel>
  startIndex?: number
}): Promise<IOnboardReposResult> {
  const qx = writerQx()
  const result: IOnboardReposResult = {
    created: 0,
    unitsInserted: 0,
    skipped: 0,
    integrationDeleted: false,
  }
  const storedUrls = await desiredReposIfLive(qx, integrationId)
  const initial = narrowDesired({ integrationId, result }, new Set(keys), storedUrls, keys)
  if (initial === null || storedUrls === null) {
    return result
  }
  let desired = initial
  const desiredKeys = keys.filter((key) => desired.has(key))
  const positions = new Map(desiredKeys.map((key, index) => [key, startIndex + index]))
  const context: IOnboardContext = {
    qx,
    integrationId,
    result,
    token: installationTokenSource(qx, integrationId),
    onboard: async (current, key) => {
      const storedUrl = storedUrls.get(key)
      if (storedUrl === undefined) {
        return { outcome: 'skipped', reason: 'no_longer_desired' }
      }
      return onboardGithubRepo(qx, current, integrationId, key, {
        storedUrl,
        initialDelaySeconds: initialDelaySeconds(positions.get(key)),
        resolved: resolved[key],
      })
    },
  }

  let limitedUntil: number | null = null
  const chunks = chunk(desiredKeys, LIFECYCLE_RESOLVE_CONCURRENCY)
  for (const [chunkIndex, chunkKeys] of chunks.entries()) {
    const pending = desiredKeys.slice(chunkIndex * LIFECYCLE_RESOLVE_CONCURRENCY)
    if (limitedUntil !== null && limitedUntil > Date.now()) {
      const message = `rate limited until ${new Date(limitedUntil).toISOString()}`
      const remaining = pending.filter((key) => desired.has(key))
      recordOnboardOutcomes(
        integrationId,
        remaining,
        remaining.map(() => ({ outcome: 'skipped', reason: 'rate_limited', message })),
        result,
      )
      return result
    }
    if (chunkIndex > 0 && chunkIndex % ONBOARD_RECHECK_EVERY_N_CHUNKS === 0) {
      const refreshed = await refreshDesired(context, desired, pending)
      if (refreshed === null) {
        return result
      }
      desired = refreshed
    }

    const group = chunkKeys.filter((key) => desired.has(key))
    const retry = await retryRateLimited(
      context,
      await onboardGroup(context, group),
      desired,
      pending.slice(chunkKeys.length),
    )
    recordOnboardOutcomes(
      integrationId,
      [...retry.outcomes.keys()],
      [...retry.outcomes.values()],
      result,
    )
    heartbeat()
    if (retry.desired === null) {
      return result
    }
    desired = retry.desired
    limitedUntil = retry.limitedUntil
  }

  return result
}

export async function softDeleteIntegrationRepositories({
  integrationId,
}: {
  integrationId: string
}): Promise<number> {
  return softDeleteRepositoriesForIntegration(writerQx(), integrationId)
}

export async function decommissionChannels({
  integrationId,
  channelKeys,
}: {
  integrationId: string
  channelKeys: string[]
}): Promise<number> {
  return decommissionSyncUnitsForChannels(writerQx(), integrationId, channelKeys)
}

export async function decommissionAllUnits({
  integrationId,
}: {
  integrationId: string
}): Promise<number> {
  return decommissionAllSyncUnits(writerQx(), integrationId)
}

export async function countUndrainedUnits({
  integrationId,
}: {
  integrationId: string
}): Promise<number> {
  return countUndrainedDecommissionedSyncUnits(writerQx(), integrationId, staleLockBefore())
}

export async function deleteDrainedUnits({
  integrationId,
}: {
  integrationId: string
}): Promise<number> {
  return deleteDrainedSyncUnits(writerQx(), integrationId, staleLockBefore())
}

export async function listUndeletedChannels({
  integrationId,
}: {
  integrationId: string
}): Promise<string[]> {
  return listDecommissionedSyncUnitChannels(writerQx(), integrationId)
}

export async function logLifecycleSkips({
  integrationId,
  skipped,
}: {
  integrationId: string
  skipped: ILifecycleSkip[]
}): Promise<void> {
  for (const skip of skipped) {
    logRepoSkipped(integrationId, skip)
  }
}

export async function logUndrainedChannels({
  integrationId,
  channelKeys,
}: {
  integrationId: string
  channelKeys: string[]
}): Promise<void> {
  svc.log.warn(
    { event: 'lifecycle_units_undrained', integrationId, channelKeys },
    'decommissioned sync units still locked by a running sync',
  )
}

export async function logGithubLifecycleSummary(summary: IGithubLifecycleSummary): Promise<void> {
  svc.log.info({ event: 'lifecycle_reconcile_summary', ...summary }, 'github lifecycle summary')
}

export async function listIntegrationsNeedingReconcile(): Promise<string[]> {
  return listGithubIntegrationsNeedingReconcile(writerQx())
}
