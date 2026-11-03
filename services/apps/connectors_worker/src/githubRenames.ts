import { resolveRepo } from '@crowd/connectors/src/connectors/github/discover'
import { parseRepoChannel } from '@crowd/connectors/src/connectors/github/paging'
import { fetchGithubLifecycleIntegration } from '@crowd/data-access-layer/src/integrations'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { findRepositoryByUrlInsensitive } from '@crowd/data-access-layer/src/repositories'
import {
  BackendRequestError,
  type IBackendApiConfig,
  buildGithubIntegrationPayload,
  fetchGithubIntegration,
  fetchGithubIntegrationMapping,
  fetchGithubOrgLogo,
  mergeGithubOrgSettings,
  postGithubNangoConnect,
  readBackendApiConfig,
} from '@crowd/project-onboarding/src/onboarder'
import type { IGithubOrgSettings } from '@crowd/project-onboarding/src/types'
import { PlatformType } from '@crowd/types'

import type { TokenSource } from './githubInstallationToken'
import { githubResolveSkipReason } from './githubSkipReasons'
import { svc } from './main'

export interface IGithubRename {
  from: string
  to: string
}

export type GithubRenameAction = 'swapped' | 'disconnected'

export interface IRenameDecision {
  rename: IGithubRename
  action: GithubRenameAction
  forkedFrom: string | null
  orgLogo: string
}

export interface IGithubRenamesResult {
  applied: number
  refused: number
  failed: number
}

type DisconnectReason = 'new_name_already_live' | 'new_name_unreachable'

interface IResolvedDecision extends IRenameDecision {
  reason?: DisconnectReason
}

type RenameFailureReason =
  | 'missing_config'
  | 'token_mint_failed'
  | 'rate_limited'
  | 'github_error'
  | 'backend_error'

interface IRenameFailure {
  rename: IGithubRename
  reason: RenameFailureReason
  message?: string
}

interface IRenameContext {
  qx: QueryExecutor
  integrationId: string
  segmentId: string
  config: IBackendApiConfig
  token: TokenSource
}

const NO_RENAMES: IGithubRenamesResult = { applied: 0, refused: 0, failed: 0 }

function urlKey(url: string): string {
  return url.toLowerCase()
}

function errorMessage(err: unknown): string | undefined {
  return err instanceof Error ? err.message : undefined
}

function hydratedUrls(orgs: IGithubOrgSettings[]): Set<string> {
  return new Set(orgs.flatMap((org) => org.repos).map((repo) => urlKey(repo.url)))
}

export function hydratedReposMatch(
  orgs: IGithubOrgSettings[],
  mapping: Record<string, string>,
): boolean {
  const hydrated = hydratedUrls(orgs)
  const mapped = new Set(Object.keys(mapping).map(urlKey))
  return hydrated.size === mapped.size && [...hydrated].every((url) => mapped.has(url))
}

function segmentOf(mapping: Record<string, string>, url: string): string | undefined {
  const key = urlKey(url)
  return Object.entries(mapping).find(([mappedUrl]) => urlKey(mappedUrl) === key)?.[1]
}

function withoutRepo(orgs: IGithubOrgSettings[], url: string): IGithubOrgSettings[] {
  const key = urlKey(url)
  return orgs.map((org) => ({
    ...org,
    repos: org.repos.filter((repo) => urlKey(repo.url) !== key),
  }))
}

function swappedOrg(decision: IRenameDecision, now: Date): IGithubOrgSettings {
  const { owner, repo } = parseRepoChannel(decision.rename.to)
  const updatedAt = now.toISOString()
  return {
    name: owner,
    url: `https://github.com/${owner}`,
    logo: decision.orgLogo,
    fullSync: false,
    updatedAt,
    repos: [{ name: repo, url: decision.rename.to, forkedFrom: decision.forkedFrom, updatedAt }],
  }
}

export function buildRenamePayload(input: {
  integrationId: string
  segmentId: string
  orgs: IGithubOrgSettings[]
  mapping: Record<string, string>
  decisions: IRenameDecision[]
  now: Date
}): Record<string, unknown> {
  const mapping = { ...input.mapping }
  const orgs = input.decisions.reduce((current, decision) => {
    const remaining = withoutRepo(current, decision.rename.from)
    if (decision.action === 'disconnected') {
      return remaining
    }
    const segment = segmentOf(input.mapping, decision.rename.from)
    if (segment !== undefined) {
      mapping[decision.rename.to] = segment
    }
    return mergeGithubOrgSettings(remaining, swappedOrg(decision, input.now))
  }, input.orgs)

  return buildGithubIntegrationPayload({
    orgs,
    segmentId: input.segmentId,
    integrationId: input.integrationId,
    existingMapping: mapping,
  })
}

function logApplied(integrationId: string, decision: IResolvedDecision): void {
  svc.log.info(
    {
      event: 'github_rename_applied',
      integrationId,
      ...decision.rename,
      action: decision.action,
      ...(decision.reason ? { reason: decision.reason } : {}),
    },
    'github repo rename applied',
  )
}

function logRefused(integrationId: string, rename: IGithubRename, cause: string): void {
  svc.log.warn(
    {
      event: 'lifecycle_repo_skipped',
      integrationId,
      url: rename.from,
      reason: 'repo_renamed',
      message: `rename to ${rename.to} refused: ${cause}`,
    },
    'repo skipped by github lifecycle',
  )
}

function logNoop(integrationId: string, rename: IGithubRename): void {
  svc.log.info(
    { event: 'github_rename_noop', integrationId, ...rename, reason: 'old_repo_not_connected' },
    'github repo rename skipped, old repository is not connected',
  )
}

function logFailed(integrationId: string, failure: IRenameFailure): void {
  svc.log.error(
    {
      event: 'github_rename_failed',
      integrationId,
      ...failure.rename,
      reason: failure.reason,
      message: failure.message,
    },
    'github repo rename failed',
  )
}

function refuseAll(
  integrationId: string,
  renames: IGithubRename[],
  cause: string,
): IGithubRenamesResult {
  renames.forEach((rename) => logRefused(integrationId, rename, cause))
  return { ...NO_RENAMES, refused: renames.length }
}

function failAll(
  integrationId: string,
  renames: IGithubRename[],
  reason: RenameFailureReason,
  message?: string,
): IGithubRenamesResult {
  renames.forEach((rename) => logFailed(integrationId, { rename, reason, message }))
  return { ...NO_RENAMES, failed: renames.length }
}

function ownsOrg(orgs: IGithubOrgSettings[], owner: string): boolean {
  const name = owner.toLowerCase()
  return orgs.some((org) => org.name.toLowerCase() === name)
}

function disconnect(rename: IGithubRename, reason: DisconnectReason): IResolvedDecision {
  return { rename, action: 'disconnected', forkedFrom: null, orgLogo: '', reason }
}

async function decideRename(
  { qx, token }: IRenameContext,
  orgs: IGithubOrgSettings[],
  rename: IGithubRename,
): Promise<IResolvedDecision | IRenameFailure> {
  if (await findRepositoryByUrlInsensitive(qx, rename.to)) {
    return disconnect(rename, 'new_name_already_live')
  }

  let githubToken: string
  try {
    githubToken = await token()
  } catch (err) {
    return { rename, reason: 'token_mint_failed', message: errorMessage(err) }
  }

  try {
    const { owner, repo } = parseRepoChannel(rename.to)
    const resolved = await resolveRepo(githubToken, owner, repo)
    const orgLogo = ownsOrg(orgs, owner) ? '' : await fetchGithubOrgLogo(owner, githubToken)
    return { rename, action: 'swapped', forkedFrom: resolved.forkedFrom, orgLogo }
  } catch (err) {
    const reason = githubResolveSkipReason(err)
    if (reason === 'repo_not_found') {
      return disconnect(rename, 'new_name_unreachable')
    }
    return { rename, reason, message: errorMessage(err) }
  }
}

function isFailure(outcome: IResolvedDecision | IRenameFailure): outcome is IRenameFailure {
  return !('action' in outcome)
}

async function decideRenames(
  context: IRenameContext,
  orgs: IGithubOrgSettings[],
  renames: IGithubRename[],
): Promise<{ decisions: IResolvedDecision[]; failures: IRenameFailure[] }> {
  const decisions: IResolvedDecision[] = []
  const failures: IRenameFailure[] = []
  for (const rename of renames) {
    const outcome = await decideRename(context, orgs, rename)
    if (isFailure(outcome)) {
      failures.push(outcome)
    } else {
      decisions.push(outcome)
    }
  }
  return { decisions, failures }
}

async function postDecisions(
  context: IRenameContext,
  orgs: IGithubOrgSettings[],
  mapping: Record<string, string>,
  decisions: IResolvedDecision[],
): Promise<IGithubRenamesResult> {
  const { integrationId, segmentId, config } = context
  const renames = decisions.map((decision) => decision.rename)
  const payload = buildRenamePayload({
    integrationId,
    segmentId,
    orgs,
    mapping,
    decisions,
    now: new Date(),
  })

  try {
    await postGithubNangoConnect(payload, config.apiUrl, config.apiToken)
  } catch (err) {
    if (err instanceof BackendRequestError && err.status >= 400 && err.status < 500) {
      return refuseAll(integrationId, renames, err.message)
    }
    return failAll(integrationId, renames, 'backend_error', errorMessage(err))
  }

  decisions.forEach((decision) => logApplied(integrationId, decision))
  return { ...NO_RENAMES, applied: decisions.length }
}

async function readIntegrationState(
  context: IRenameContext,
): Promise<{ orgs: IGithubOrgSettings[]; mapping: Record<string, string> }> {
  const { integrationId, segmentId, config } = context
  const [integration, mapping] = await Promise.all([
    fetchGithubIntegration(integrationId, segmentId, config),
    fetchGithubIntegrationMapping(integrationId, segmentId, config.apiUrl, config.apiToken),
  ])
  return { orgs: integration.orgs, mapping }
}

export async function applyGithubRenames(
  qx: QueryExecutor,
  integrationId: string,
  renames: IGithubRename[],
  token: TokenSource,
): Promise<IGithubRenamesResult> {
  if (renames.length === 0) {
    return NO_RENAMES
  }

  const config = readBackendApiConfig()
  if (!config) {
    return failAll(
      integrationId,
      renames,
      'missing_config',
      'CROWD_API_SERVICE_URL or CROWD_LF_AGENT_USER_TOKEN is not set',
    )
  }

  const integration = await fetchGithubLifecycleIntegration(qx, integrationId)
  if (
    !integration ||
    integration.platform !== PlatformType.GITHUB_NANGO ||
    integration.deletedAt !== null ||
    !integration.segmentId
  ) {
    return refuseAll(integrationId, renames, 'integration not live')
  }

  const context: IRenameContext = {
    qx,
    integrationId,
    segmentId: integration.segmentId,
    config,
    token,
  }

  let state: { orgs: IGithubOrgSettings[]; mapping: Record<string, string> }
  try {
    state = await readIntegrationState(context)
  } catch (err) {
    return failAll(integrationId, renames, 'backend_error', errorMessage(err))
  }

  if (!hydratedReposMatch(state.orgs, state.mapping)) {
    return refuseAll(
      integrationId,
      renames,
      'state_mismatch: integration settings differ from its repositories',
    )
  }

  const hydrated = hydratedUrls(state.orgs)
  const connected = renames.filter((rename) => hydrated.has(urlKey(rename.from)))
  renames
    .filter((rename) => !hydrated.has(urlKey(rename.from)))
    .forEach((rename) => logNoop(integrationId, rename))

  const { decisions, failures } = await decideRenames(context, state.orgs, connected)
  failures.forEach((failure) => logFailed(integrationId, failure))
  if (decisions.length === 0) {
    return { ...NO_RENAMES, failed: failures.length }
  }

  const posted = await postDecisions(context, state.orgs, state.mapping, decisions)
  return { ...posted, failed: posted.failed + failures.length }
}
