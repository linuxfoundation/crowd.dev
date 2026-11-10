import { canonicalizeRepoUrl, groupBy, mapWithConcurrency } from '@crowd/common'
import { organizationLabel } from '@crowd/slack'

import {
  IGithubIntegrationSnapshot,
  IGithubOrgSettings,
  IOnboardingInput,
  IOnboardingResult,
  IOrganizationProjectOnboardingInput,
  ISegmentOnboardingInput,
  ISegmentReposOnboardingInput,
} from './types'

const LF_OSS_INDEX_PROJECT_GROUP_SLUG = 'lf-oss-index'
const LINUX_KERNEL_GITHUB_URL = 'https://github.com/torvalds/linux'
const LINUX_KERNEL_GIT_URL = 'https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux'

interface ISegmentQueryResponse {
  rows?: Array<{ subprojects?: Array<{ id: string; name: string }> }>
  count?: number
}

interface IIntegrationRow {
  id: string
  settings?: { orgs?: Array<Partial<IGithubOrgSettings>> }
}

interface IIntegrationQueryResponse {
  rows?: IIntegrationRow[]
}

interface IIntegrationRepositoryMapping {
  url: string
  segment: { id: string }
}

export interface IBackendApiConfig {
  apiUrl: string
  apiToken: string
}

interface IOnboardingApiConfig extends IBackendApiConfig {
  githubToken: string
}

export class BackendRequestError extends Error {
  readonly status: number

  constructor(message: string, status: number) {
    super(message)
    this.name = 'BackendRequestError'
    this.status = status
  }
}

const GITHUB_NANGO_PLATFORM = 'github-nango'
const SEGMENT_QUERY_PAGE_SIZE = 20
const BACKEND_REQUEST_TIMEOUT_MS = 30_000
const GITHUB_REQUEST_TIMEOUT_MS = 10_000
const GITHUB_LOOKUP_CONCURRENCY = 5
const ERROR_BODY_MAX_LENGTH = 500
const MISSING_CONFIG_ERROR =
  'Missing API configuration: CROWD_API_SERVICE_URL, CROWD_LF_AGENT_USER_TOKEN, or CROWD_GITHUB_PERSONAL_ACCESS_TOKENS'

export function readBackendApiConfig(): IBackendApiConfig | null {
  const apiUrl = process.env.CROWD_API_SERVICE_URL
  const apiToken = process.env.CROWD_LF_AGENT_USER_TOKEN

  if (!apiUrl || !apiToken) {
    return null
  }

  return { apiUrl, apiToken }
}

function readOnboardingApiConfig(): IOnboardingApiConfig | null {
  const backendConfig = readBackendApiConfig()
  const githubTokens = process.env.CROWD_GITHUB_PERSONAL_ACCESS_TOKENS

  if (!backendConfig || !githubTokens) {
    return null
  }

  return { ...backendConfig, githubToken: githubTokens.split(',')[0].trim() }
}

function errorResult(error: string, segmentId: string | null = null): IOnboardingResult {
  return { outcome: 'error', segmentId, error }
}

export async function readErrorBody(response: Response): Promise<string> {
  try {
    const body = await response.text()
    return body.length > ERROR_BODY_MAX_LENGTH ? `${body.slice(0, ERROR_BODY_MAX_LENGTH)}…` : body
  } catch {
    return ''
  }
}

export function deriveProjectName(repoName: string): string {
  return repoName
    .replace(/[-_]+/g, ' ')
    .trim()
    .split(' ')
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ')
}

export function deriveProjectSlug(projectSlug: string): string {
  return projectSlug
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
}

export function parseGithubUrl(repoUrl: string): { owner: string; repo: string; url: string } {
  const canonical = canonicalizeRepoUrl(repoUrl)
  if (!canonical?.isGithub) {
    throw new Error(`Invalid GitHub URL format: ${repoUrl}`)
  }

  return { owner: canonical.owner, repo: canonical.repo, url: canonical.url }
}

export function buildGithubOrgSettings(params: {
  owner: string
  repo: string
  repoUrl: string
  orgLogo: string
  forkedFrom: string | null
  now?: Date
}): IGithubOrgSettings {
  const { owner, repo, repoUrl, orgLogo, forkedFrom, now = new Date() } = params
  const updatedAt = now.toISOString()

  return {
    name: owner,
    url: `https://github.com/${owner}`,
    logo: orgLogo,
    fullSync: false,
    updatedAt,
    repos: [{ name: repo, url: repoUrl, forkedFrom, updatedAt }],
  }
}

function hasRepo(org: IGithubOrgSettings, repoUrl: string): boolean {
  const requested = comparableRepoUrl(repoUrl)
  return org.repos.some((repo) => comparableRepoUrl(repo.url) === requested)
}

export function mergeGithubOrgSettings(
  existing: IGithubOrgSettings[],
  incoming: IGithubOrgSettings,
): IGithubOrgSettings[] {
  const incomingName = incoming.name.toLowerCase()
  const current = existing.find((org) => org.name.toLowerCase() === incomingName)
  if (!current) {
    return [...existing, incoming]
  }

  const newRepos = incoming.repos.filter((repo) => !hasRepo(current, repo.url))
  return existing.map((org) =>
    org === current ? { ...org, repos: [...org.repos, ...newRepos] } : org,
  )
}

function toComparableMapping(mapping: Record<string, string>): Map<string, string> {
  return new Map(Object.entries(mapping).map(([url, segment]) => [comparableRepoUrl(url), segment]))
}

export function buildGithubIntegrationPayload(params: {
  orgs: IGithubOrgSettings[]
  segmentId: string
  integrationId?: string
  existingMapping?: Record<string, string>
}): Record<string, unknown> {
  const { orgs, segmentId, integrationId, existingMapping = {} } = params
  const currentSegments = toComparableMapping(existingMapping)
  const mapping = Object.fromEntries(
    orgs
      .flatMap((org) => org.repos)
      .map((repo) => [repo.url, currentSegments.get(comparableRepoUrl(repo.url)) ?? segmentId]),
  )

  return {
    ...(integrationId ? { integrationId } : {}),
    settings: { orgs, updateMemberAttributes: true },
    mapping,
    segments: [segmentId],
  }
}

async function queryProjectByName(
  name: string,
  apiUrl: string,
  token: string,
): Promise<string | null> {
  const normalizedName = name.toLowerCase()
  let offset = 0
  let hasMorePages = true

  while (hasMorePages) {
    const response = await fetch(`${apiUrl}/segment/project/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({
        filter: { name, parentSlug: LF_OSS_INDEX_PROJECT_GROUP_SLUG },
        limit: SEGMENT_QUERY_PAGE_SIZE,
        offset,
      }),
      signal: AbortSignal.timeout(BACKEND_REQUEST_TIMEOUT_MS),
    })

    if (!response.ok) {
      throw new Error(
        `Segment query returned HTTP ${response.status}: ${response.statusText} - ${await readErrorBody(response)}`,
      )
    }

    const body = (await response.json()) as ISegmentQueryResponse
    const rows = body.rows ?? []
    const subprojects = rows.flatMap((row) => row.subprojects ?? [])

    const exactMatch = subprojects.find(
      (subproject) => subproject.name.toLowerCase() === normalizedName,
    )
    if (exactMatch) {
      return exactMatch.id
    }

    offset += SEGMENT_QUERY_PAGE_SIZE
    hasMorePages = offset < (body.count ?? 0)
  }

  return null
}

function comparableRepoUrl(url: string): string {
  return canonicalizeRepoUrl(url)?.url ?? url.toLowerCase()
}

function toOrgSettings(org: Partial<IGithubOrgSettings>): IGithubOrgSettings {
  return {
    name: org.name ?? '',
    url: org.url ?? '',
    logo: org.logo ?? '',
    fullSync: org.fullSync ?? false,
    updatedAt: org.updatedAt ?? new Date().toISOString(),
    repos: org.repos ?? [],
  }
}

async function queryGithubIntegration(
  segmentId: string,
  apiUrl: string,
  token: string,
): Promise<IGithubIntegrationSnapshot | null> {
  const response = await fetch(`${apiUrl}/integration/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({
      segments: [segmentId],
      filter: { and: [{ platform: { eq: GITHUB_NANGO_PLATFORM } }] },
      limit: 1,
      offset: 0,
    }),
    signal: AbortSignal.timeout(BACKEND_REQUEST_TIMEOUT_MS),
  })

  if (!response.ok) {
    throw new Error(
      `Integration query returned HTTP ${response.status}: ${response.statusText} - ${await readErrorBody(response)}`,
    )
  }

  const [integration] = ((await response.json()) as IIntegrationQueryResponse).rows ?? []
  if (!integration) {
    return null
  }

  return { id: integration.id, orgs: (integration.settings?.orgs ?? []).map(toOrgSettings) }
}

export async function fetchGithubIntegration(
  integrationId: string,
  segmentId: string,
  config: IBackendApiConfig,
): Promise<IGithubIntegrationSnapshot> {
  const query = new URLSearchParams({ 'segments[]': segmentId })
  const response = await fetch(`${config.apiUrl}/integration/${integrationId}?${query}`, {
    headers: { Authorization: `Bearer ${config.apiToken}` },
    signal: AbortSignal.timeout(BACKEND_REQUEST_TIMEOUT_MS),
  })

  if (!response.ok) {
    throw new Error(
      `Integration returned HTTP ${response.status}: ${response.statusText} - ${await readErrorBody(response)}`,
    )
  }

  const integration = (await response.json()) as IIntegrationRow
  return { id: integration.id, orgs: (integration.settings?.orgs ?? []).map(toOrgSettings) }
}

export async function fetchGithubIntegrationMapping(
  integrationId: string,
  segmentId: string,
  apiUrl: string,
  token: string,
): Promise<Record<string, string>> {
  const query = new URLSearchParams({ 'segments[]': segmentId })
  const response = await fetch(`${apiUrl}/integration/${integrationId}/repositories?${query}`, {
    headers: { Authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(BACKEND_REQUEST_TIMEOUT_MS),
  })

  if (!response.ok) {
    throw new Error(
      `Integration repositories returned HTTP ${response.status}: ${response.statusText} - ${await readErrorBody(response)}`,
    )
  }

  const rows = (await response.json()) as IIntegrationRepositoryMapping[]
  return Object.fromEntries(rows.map((row) => [row.url, row.segment.id]))
}

async function queryGithubIntegrationRepoUrls(
  segmentId: string,
  apiUrl: string,
  token: string,
): Promise<string[] | null> {
  const integration = await queryGithubIntegration(segmentId, apiUrl, token)
  if (!integration) {
    return null
  }

  return integration.orgs.flatMap((org) => org.repos).map((repo) => comparableRepoUrl(repo.url))
}

export function deriveProjectNameCandidates(owner: string, repoName: string): string[] {
  return [deriveProjectName(repoName), deriveProjectName(`${owner} ${repoName}`)]
}

export function deriveOrganizationProjectNameCandidates(
  owner: string,
  projectName: string | null,
): string[] {
  const name = projectName?.trim() || deriveProjectName(owner)
  return [name, `${name} (${organizationLabel(owner)})`]
}

export interface IProjectCandidate {
  name: string
  slug: string
  fallbackSlug?: string
}

export function deriveOrganizationProjectCandidates(
  owner: string,
  projectName: string | null,
): IProjectCandidate[] {
  return deriveOrganizationProjectNameCandidates(owner, projectName).map((name) => ({
    name,
    slug: deriveProjectSlug(name),
  }))
}

export function withSuggestedSlug(
  candidates: IProjectCandidate[],
  slug: string | null | undefined,
): IProjectCandidate[] {
  const suggested = slug ? deriveProjectSlug(slug) : ''
  const [first, ...rest] = candidates
  if (!suggested || !first || suggested === first.slug) {
    return candidates
  }
  return [{ ...first, slug: suggested, fallbackSlug: first.slug }, ...rest]
}

function belongsToOwner(repoUrl: string, owner: string): boolean {
  return canonicalizeRepoUrl(repoUrl)?.owner?.toLowerCase() === owner.toLowerCase()
}

// A project already connected to repositories of the same organization is that organization's
// project, whichever run or attempt created it.
export async function resolveProjectSegment(
  candidates: IProjectCandidate[],
  repoUrls: string[],
  apiUrl: string,
  token: string,
  organizationOwner: string | null = null,
): Promise<string> {
  const requested = new Set(repoUrls.map(comparableRepoUrl))
  const belongsToRequest = (url: string) =>
    requested.has(url) || (organizationOwner !== null && belongsToOwner(url, organizationOwner))

  for (const candidate of candidates) {
    const existingSegmentId = await queryProjectByName(candidate.name, apiUrl, token)
    if (!existingSegmentId) {
      return createProjectSegmentWithFallback(candidate, apiUrl, token)
    }

    const connectedRepoUrls = await queryGithubIntegrationRepoUrls(existingSegmentId, apiUrl, token)
    const connectsOnlyRequestedRepos =
      connectedRepoUrls !== null &&
      connectedRepoUrls.length > 0 &&
      connectedRepoUrls.every(belongsToRequest)
    if (connectedRepoUrls === null || connectsOnlyRequestedRepos) {
      return existingSegmentId
    }
  }

  throw new Error(
    `Every candidate project name (${candidates.map(({ name }) => `"${name}"`).join(', ')}) is already used by a project with a GitHub integration`,
  )
}

function isSlugRejected(err: unknown): boolean {
  return err instanceof BackendRequestError && err.status === 400
}

// Only a rejected slug may fall back to the derived one: after a timeout or a 5xx the first
// segment may exist already, and a second POST would only add a misleading name conflict.
async function createProjectSegmentWithFallback(
  { name, slug, fallbackSlug }: IProjectCandidate,
  apiUrl: string,
  token: string,
): Promise<string> {
  try {
    return await createProjectSegment(name, slug, apiUrl, token)
  } catch (err) {
    if (!fallbackSlug || !isSlugRejected(err)) {
      throw err
    }
    return createProjectSegment(name, fallbackSlug, apiUrl, token)
  }
}

async function createProjectSegment(
  name: string,
  slug: string,
  apiUrl: string,
  token: string,
): Promise<string> {
  const response = await fetch(`${apiUrl}/segment/project`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ name, slug, isLF: false, parentSlug: LF_OSS_INDEX_PROJECT_GROUP_SLUG }),
    signal: AbortSignal.timeout(BACKEND_REQUEST_TIMEOUT_MS),
  })

  if (!response.ok) {
    throw new BackendRequestError(
      `Segment creation returned HTTP ${response.status}: ${response.statusText} - ${await readErrorBody(response)}`,
      response.status,
    )
  }

  // POST /segment/project does not return the created segment's id in its response body; re-query by name to get it.
  const segmentId = await queryProjectByName(name, apiUrl, token)
  if (!segmentId) {
    throw new Error(`Segment created but could not be found by name "${name}" afterwards`)
  }

  return segmentId
}

export async function fetchGithubOrgLogo(owner: string, githubToken: string): Promise<string> {
  try {
    const response = await fetch(`https://api.github.com/users/${owner}`, {
      headers: { Authorization: `Bearer ${githubToken}`, Accept: 'application/json' },
      signal: AbortSignal.timeout(GITHUB_REQUEST_TIMEOUT_MS),
    })
    if (!response.ok) return ''

    const body = (await response.json()) as { avatar_url?: string }
    return body.avatar_url ?? ''
  } catch {
    return ''
  }
}

async function fetchGithubForkedFrom(
  owner: string,
  repo: string,
  githubToken: string,
): Promise<string | null> {
  try {
    const response = await fetch(`https://api.github.com/repos/${owner}/${repo}`, {
      headers: { Authorization: `Bearer ${githubToken}`, Accept: 'application/json' },
      signal: AbortSignal.timeout(GITHUB_REQUEST_TIMEOUT_MS),
    })
    if (!response.ok) return null

    const body = (await response.json()) as { fork?: boolean; parent?: { html_url?: string } }
    if (!body.fork || !body.parent?.html_url) return null

    // The Linux kernel's GitHub mirror is a fork; the platform onboards the canonical git.kernel.org repo instead.
    return body.parent.html_url === LINUX_KERNEL_GITHUB_URL
      ? LINUX_KERNEL_GIT_URL
      : body.parent.html_url
  } catch {
    return null
  }
}

async function fetchGithubOrgSettings(
  input: { owner: string; repo: string; repoUrl: string },
  githubToken: string,
): Promise<IGithubOrgSettings> {
  const [orgLogo, forkedFrom] = await Promise.all([
    fetchGithubOrgLogo(input.owner, githubToken),
    fetchGithubForkedFrom(input.owner, input.repo, githubToken),
  ])

  return buildGithubOrgSettings({ ...input, orgLogo, forkedFrom })
}

export async function postGithubNangoConnect(
  payload: Record<string, unknown>,
  apiUrl: string,
  token: string,
): Promise<void> {
  const response = await fetch(`${apiUrl}/github-nango-connect`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(BACKEND_REQUEST_TIMEOUT_MS),
  })

  if (!response.ok) {
    throw new BackendRequestError(
      `GitHub integration returned HTTP ${response.status}: ${response.statusText} - ${await readErrorBody(response)}`,
      response.status,
    )
  }
}

async function createGithubIntegration(
  input: { owner: string; repo: string; repoUrl: string; segmentId: string },
  config: IOnboardingApiConfig,
): Promise<void> {
  const org = await fetchGithubOrgSettings(input, config.githubToken)
  await connectOrgsToSegment([org], input.segmentId, null, config)
}

interface IParsedRepo {
  owner: string
  repo: string
  repoUrl: string
}

async function fetchGithubOrgsSettings(
  repos: IParsedRepo[],
  githubToken: string,
): Promise<IGithubOrgSettings[]> {
  const byOwner = groupBy(repos, (repo) => repo.owner.toLowerCase())
  return Promise.all(
    [...byOwner.values()].map(async (ownerRepos) => {
      const [first, ...rest] = ownerRepos
      const [org, forkedFroms] = await Promise.all([
        fetchGithubOrgSettings(first, githubToken),
        mapWithConcurrency(rest, GITHUB_LOOKUP_CONCURRENCY, (repo) =>
          fetchGithubForkedFrom(repo.owner, repo.repo, githubToken),
        ),
      ])
      const updatedAt = org.updatedAt
      return {
        ...org,
        repos: [
          ...org.repos,
          ...rest.map((repo, index) => ({
            name: repo.repo,
            url: repo.repoUrl,
            forkedFrom: forkedFroms[index],
            updatedAt,
          })),
        ],
      }
    }),
  )
}

// The backend replaces settings.orgs, applies the mapping to every repo and soft-deletes repos
// missing from it, so existing repos must be sent back on their current (sub)project segments.
async function connectOrgsToSegment(
  orgs: IGithubOrgSettings[],
  segmentId: string,
  integration: IGithubIntegrationSnapshot | null,
  { apiUrl, apiToken }: IOnboardingApiConfig,
): Promise<void> {
  if (!integration) {
    await postGithubNangoConnect(
      buildGithubIntegrationPayload({ orgs, segmentId }),
      apiUrl,
      apiToken,
    )
    return
  }

  const existingMapping = await fetchGithubIntegrationMapping(
    integration.id,
    segmentId,
    apiUrl,
    apiToken,
  )
  await postGithubNangoConnect(
    buildGithubIntegrationPayload({
      orgs: orgs.reduce(mergeGithubOrgSettings, integration.orgs),
      segmentId,
      integrationId: integration.id,
      existingMapping,
    }),
    apiUrl,
    apiToken,
  )
}

export async function onboardProject(input: IOnboardingInput): Promise<IOnboardingResult> {
  const config = readOnboardingApiConfig()
  if (!config) {
    return errorResult(MISSING_CONFIG_ERROR)
  }

  const slug = deriveProjectSlug(input.projectSlug)

  let owner: string
  let repo: string
  let repoUrl: string
  try {
    const parsed = parseGithubUrl(input.repoUrl)
    owner = parsed.owner
    repo = parsed.repo
    repoUrl = parsed.url
  } catch (err) {
    return errorResult((err as Error).message)
  }

  let segmentId: string
  try {
    segmentId = await resolveProjectSegment(
      deriveProjectNameCandidates(owner, input.repoName).map((name) => ({ name, slug })),
      [input.repoUrl],
      config.apiUrl,
      config.apiToken,
    )
  } catch (err) {
    return errorResult((err as Error).message)
  }

  try {
    await createGithubIntegration({ owner, repo, repoUrl, segmentId }, config)
  } catch (err) {
    return errorResult((err as Error).message, segmentId)
  }

  return { outcome: 'onboarded', segmentId, error: null }
}

function parseRepos(repoUrls: string[]): IParsedRepo[] {
  return repoUrls.map((repoUrl) => {
    const parsed = parseGithubUrl(repoUrl)
    return { owner: parsed.owner, repo: parsed.repo, repoUrl: parsed.url }
  })
}

async function connectParsedRepos(
  segmentId: string,
  repos: IParsedRepo[],
  config: IOnboardingApiConfig,
): Promise<IOnboardingResult> {
  try {
    const [orgs, integration] = await Promise.all([
      fetchGithubOrgsSettings(repos, config.githubToken),
      queryGithubIntegration(segmentId, config.apiUrl, config.apiToken),
    ])
    await connectOrgsToSegment(orgs, segmentId, integration, config)
  } catch (err) {
    return errorResult((err as Error).message, segmentId)
  }

  return { outcome: 'onboarded', segmentId, error: null }
}

export async function connectReposToSegment({
  segmentId,
  repoUrls,
}: ISegmentReposOnboardingInput): Promise<IOnboardingResult> {
  const config = readOnboardingApiConfig()
  if (!config) {
    return errorResult(MISSING_CONFIG_ERROR, segmentId)
  }
  if (repoUrls.length === 0) {
    return errorResult('No repository to connect', segmentId)
  }

  let repos: IParsedRepo[]
  try {
    repos = parseRepos(repoUrls)
  } catch (err) {
    return errorResult((err as Error).message, segmentId)
  }

  return connectParsedRepos(segmentId, repos, config)
}

export async function onboardOrganizationProject(
  input: IOrganizationProjectOnboardingInput,
): Promise<IOnboardingResult> {
  const config = readOnboardingApiConfig()
  if (!config) {
    return errorResult(MISSING_CONFIG_ERROR)
  }
  if (input.repoUrls.length === 0) {
    return errorResult('No repository to onboard')
  }

  let repos: IParsedRepo[]
  try {
    repos = parseRepos(input.repoUrls)
  } catch (err) {
    return errorResult((err as Error).message)
  }

  let segmentId: string
  try {
    segmentId = await resolveProjectSegment(
      withSuggestedSlug(
        deriveOrganizationProjectCandidates(repos[0].owner, input.projectName),
        input.slug,
      ),
      input.repoUrls,
      config.apiUrl,
      config.apiToken,
      input.owner,
    )
  } catch (err) {
    return errorResult((err as Error).message)
  }

  return connectParsedRepos(segmentId, repos, config)
}

export async function connectRepoToSegment({
  segmentId,
  repoUrl,
}: ISegmentOnboardingInput): Promise<IOnboardingResult> {
  return connectReposToSegment({ segmentId, repoUrls: [repoUrl] })
}
