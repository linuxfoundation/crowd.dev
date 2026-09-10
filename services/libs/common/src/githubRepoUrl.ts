export interface ICanonicalRepoUrl {
  url: string
  host: string
  isGithub: boolean
  owner: string | null
  repo: string | null
}

const GITHUB_HOST = 'github.com'

// Owners that are GitHub product surfaces, not repo owners — a link to any of
// these is never a repo reference even though it matches /github.com/{a}/{b}/.
const GITHUB_NON_REPO_OWNERS = new Set([
  'user-attachments',
  'orgs',
  'apps',
  'marketplace',
  'sponsors',
  'topics',
  'collections',
  'settings',
  'login',
  'about',
])

function toParsableUrl(raw: string): string {
  const trimmed = raw.trim()
  const sshRewritten = trimmed
    .replace(/^git@github\.com:/, 'https://github.com/')
    .replace(/^ssh:\/\/git@github\.com\//, 'https://github.com/')

  return /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(sshRewritten)
    ? sshRewritten
    : `https://${sshRewritten}`
}

function stripSlashesAndGitSuffix(pathname: string): string {
  return pathname
    .replace(/^\/+/, '')
    .replace(/\/+$/, '')
    .replace(/\.git$/i, '')
}

export function canonicalizeRepoUrl(raw: string | null | undefined): ICanonicalRepoUrl | null {
  if (!raw || typeof raw !== 'string') {
    return null
  }

  let parsed: URL
  try {
    parsed = new URL(toParsableUrl(raw))
  } catch {
    return null
  }

  const host = parsed.hostname.toLowerCase().replace(/^www\./, '')
  if (!host) {
    return null
  }

  const path = stripSlashesAndGitSuffix(parsed.pathname)
  if (!path) {
    return null
  }

  if (host === GITHUB_HOST) {
    const segments = path.split('/')
    if (segments.length !== 2 || !segments[0] || !segments[1]) {
      return null
    }

    const owner = segments[0].toLowerCase()
    const repo = segments[1].toLowerCase()
    if (GITHUB_NON_REPO_OWNERS.has(owner)) {
      return null
    }

    return {
      url: `https://${GITHUB_HOST}/${owner}/${repo}`,
      host: GITHUB_HOST,
      isGithub: true,
      owner,
      repo,
    }
  }

  // Non-GitHub hosts (GitLab, Gerrit, Googlesource, git.kernel.org, ...) keep
  // their path case as-is — several of these hosts are case-sensitive.
  return {
    url: `https://${host}/${path}`,
    host,
    isGithub: false,
    owner: null,
    repo: null,
  }
}

export function canonicalizeGithubRepoUrl(raw: string | null | undefined): string | null {
  const canonical = canonicalizeRepoUrl(raw)
  return canonical?.isGithub ? canonical.url : null
}

export function githubRepoPath(raw: string | null | undefined): string | null {
  const canonical = canonicalizeRepoUrl(raw)
  return canonical?.isGithub ? `${canonical.owner}/${canonical.repo}` : null
}
