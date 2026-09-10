export type ICanonicalRepoUrl =
  | { url: string; host: string; isGithub: true; owner: string; repo: string }
  | { url: string; host: string; isGithub: false; owner: null; repo: null }

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
    // scp-style path wrapped in an ssh:// scheme, e.g. ssh://git@github.com:owner/repo.git —
    // left alone when followed by digits/ (an actual port, e.g. ssh://git@github.com:2222/owner/repo.git).
    .replace(/^ssh:\/\/git@github\.com:(?!\d+(?:\/|$))/, 'https://github.com/')
    .replace(/^ssh:\/\/git@github\.com\//, 'https://github.com/')
    .replace(/^git@github\.com:/, 'https://github.com/')

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
    // Deep links (/tree/<branch>, /blob/<branch>/<path>, /pull/<n>, ...) still
    // unambiguously reference the repo at the first two segments — take those
    // instead of rejecting, matching how the rest of the repo already treats them.
    const segments = path.split('/')
    if (segments.length < 2 || !segments[0] || !segments[1]) {
      return null
    }

    const owner = segments[0].toLowerCase()
    const repo = segments[1].toLowerCase().replace(/\.git$/i, '')
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
