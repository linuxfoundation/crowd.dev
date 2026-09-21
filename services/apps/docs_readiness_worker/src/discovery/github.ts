import { canonicalizeRepoUrl } from '@crowd/common'

import { USER_AGENT } from './http'

export function parseGithubRepo(url: string): { owner: string; repo: string } | null {
  const canonical = canonicalizeRepoUrl(url)
  return canonical?.isGithub ? { owner: canonical.owner, repo: canonical.repo } : null
}

function isGithubUrl(repo: string): boolean {
  const rewritten = repo.trim().replace(/^git@([a-zA-Z0-9.-]+):/, 'https://$1/')
  const withScheme = /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(rewritten)
    ? rewritten
    : `https://${rewritten}`

  try {
    const host = new URL(withScheme).hostname.toLowerCase()
    return host === 'github.com' || host === 'www.github.com'
  } catch {
    return false
  }
}

export function primaryRepo(repos: string[]): string | null {
  const githubRepos = repos.filter(isGithubUrl)
  if (githubRepos.length === 0) {
    return null
  }

  return githubRepos.find((repo) => parseGithubRepo(repo)) ?? githubRepos[0]
}

async function githubRequest(path: string, token: string, accept: string): Promise<Response> {
  return fetch(`https://api.github.com${path}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      'X-GitHub-Api-Version': '2022-11-28',
      'User-Agent': USER_AGENT,
      Accept: accept,
    },
    signal: AbortSignal.timeout(10_000),
  })
}

export async function getRepoHomepage(
  owner: string,
  repo: string,
  token: string,
): Promise<string | null> {
  try {
    const response = await githubRequest(
      `/repos/${owner}/${repo}`,
      token,
      'application/vnd.github+json',
    )
    if (!response.ok) {
      return null
    }
    const body = (await response.json()) as { homepage?: string }
    return body.homepage ?? null
  } catch {
    return null
  }
}

export async function getReadme(
  owner: string,
  repo: string,
  token: string,
): Promise<string | null> {
  try {
    const response = await githubRequest(
      `/repos/${owner}/${repo}/readme`,
      token,
      'application/vnd.github.raw+json',
    )
    if (!response.ok) {
      return null
    }
    return await response.text()
  } catch {
    return null
  }
}

export async function getPackageJson(
  owner: string,
  repo: string,
  token: string,
): Promise<{ documentation?: string; homepage?: string } | null> {
  try {
    const response = await githubRequest(
      `/repos/${owner}/${repo}/contents/package.json`,
      token,
      'application/vnd.github.raw+json',
    )
    if (!response.ok) {
      return null
    }
    const parsed = JSON.parse(await response.text())
    return { documentation: parsed.documentation, homepage: parsed.homepage }
  } catch {
    return null
  }
}
