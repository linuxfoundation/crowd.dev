import { IOnboardingInput, IOnboardingResult } from './types'

const LF_OSS_INDEX_PROJECT_GROUP_SLUG = 'lf-oss-index'
const LINUX_KERNEL_GITHUB_URL = 'https://github.com/torvalds/linux'
const LINUX_KERNEL_GIT_URL = 'https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux'

interface ISegmentQueryResponse {
  rows?: Array<{ subprojects?: Array<{ id: string }> }>
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

export function parseGithubUrl(repoUrl: string): { owner: string; repo: string } {
  const url = new URL(repoUrl.replace('git@github.com:', 'https://github.com/'))
  const pathParts = url.pathname
    .replace(/^\//, '')
    .replace(/\.git$/, '')
    .split('/')

  if (pathParts.length < 2 || !pathParts[0] || !pathParts[1]) {
    throw new Error(`Invalid GitHub URL format: ${repoUrl}`)
  }

  return { owner: pathParts[0], repo: pathParts[1] }
}

export function buildGithubIntegrationPayload(params: {
  owner: string
  repo: string
  repoUrl: string
  segmentId: string
  orgLogo: string
  forkedFrom: string | null
  now?: Date
}): Record<string, unknown> {
  const { owner, repo, repoUrl, segmentId, orgLogo, forkedFrom, now = new Date() } = params
  const updatedAt = now.toISOString()

  return {
    settings: {
      orgs: [
        {
          name: owner,
          url: repoUrl,
          logo: orgLogo,
          fullSync: false,
          updatedAt,
          repos: [{ name: repo, url: repoUrl, forkedFrom, updatedAt }],
        },
      ],
      updateMemberAttributes: true,
    },
    mapping: { [repoUrl]: segmentId },
    segments: [segmentId],
  }
}

async function queryProjectByName(
  name: string,
  apiUrl: string,
  token: string,
): Promise<string | null> {
  const response = await fetch(`${apiUrl}/segment/project/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({
      filter: { name, parentSlug: LF_OSS_INDEX_PROJECT_GROUP_SLUG },
      limit: 1,
      offset: 0,
    }),
  })

  if (!response.ok) {
    return null
  }

  const body = (await response.json()) as ISegmentQueryResponse
  return body.rows?.[0]?.subprojects?.[0]?.id ?? null
}

async function createProjectSegment(
  name: string,
  slug: string,
  apiUrl: string,
  token: string,
): Promise<string> {
  const existingSegmentId = await queryProjectByName(name, apiUrl, token)
  if (existingSegmentId) {
    return existingSegmentId
  }

  const response = await fetch(`${apiUrl}/segment/project`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ name, slug, isLF: false, parentSlug: LF_OSS_INDEX_PROJECT_GROUP_SLUG }),
  })

  if (!response.ok) {
    throw new Error(`Segment creation returned HTTP ${response.status}: ${response.statusText}`)
  }

  // The create response payload is unreliable — always re-query for the real segment id.
  const segmentId = await queryProjectByName(name, apiUrl, token)
  if (!segmentId) {
    throw new Error(`Segment created but could not be found by name "${name}" afterwards`)
  }

  return segmentId
}

async function fetchGithubOrgLogo(owner: string, githubToken: string): Promise<string> {
  try {
    const response = await fetch(`https://api.github.com/users/${owner}`, {
      headers: { Authorization: `Bearer ${githubToken}`, Accept: 'application/json' },
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

async function createGithubIntegration(
  input: { owner: string; repo: string; repoUrl: string; segmentId: string },
  apiUrl: string,
  token: string,
  githubToken: string,
): Promise<void> {
  const [orgLogo, forkedFrom] = await Promise.all([
    fetchGithubOrgLogo(input.owner, githubToken),
    fetchGithubForkedFrom(input.owner, input.repo, githubToken),
  ])

  const payload = buildGithubIntegrationPayload({ ...input, orgLogo, forkedFrom })

  const response = await fetch(`${apiUrl}/github-nango-connect`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify(payload),
  })

  if (!response.ok) {
    throw new Error(`GitHub integration returned HTTP ${response.status}: ${response.statusText}`)
  }
}

export async function onboardProject(input: IOnboardingInput): Promise<IOnboardingResult> {
  const apiUrl = process.env.CROWD_API_SERVICE_URL
  const apiToken = process.env.CROWD_LF_AGENT_USER_TOKEN
  const githubTokens = process.env.CROWD_GITHUB_PERSONAL_ACCESS_TOKENS

  if (!apiUrl || !apiToken || !githubTokens) {
    return {
      outcome: 'error',
      segmentId: null,
      error:
        'Missing API configuration: CROWD_API_SERVICE_URL, CROWD_LF_AGENT_USER_TOKEN, or CROWD_GITHUB_PERSONAL_ACCESS_TOKENS',
    }
  }
  const githubToken = githubTokens.split(',')[0].trim()

  const name = deriveProjectName(input.repoName)
  const slug = deriveProjectSlug(input.projectSlug)

  let owner: string
  let repo: string
  try {
    const parsed = parseGithubUrl(input.repoUrl)
    owner = parsed.owner
    repo = parsed.repo
  } catch (err) {
    return { outcome: 'error', segmentId: null, error: (err as Error).message }
  }

  let segmentId: string
  try {
    segmentId = await createProjectSegment(name, slug, apiUrl, apiToken)
  } catch (err) {
    return { outcome: 'error', segmentId: null, error: (err as Error).message }
  }

  try {
    await createGithubIntegration(
      { owner, repo, repoUrl: input.repoUrl, segmentId },
      apiUrl,
      apiToken,
      githubToken,
    )
  } catch (err) {
    return { outcome: 'error', segmentId, error: (err as Error).message }
  }

  return { outcome: 'onboarded', segmentId, error: null }
}
