export type RepoHost = 'github' | 'gitlab' | 'bitbucket' | 'other'

export interface CanonicalRepo {
  // Canonical https://<host>/<owner>/<name>
  url: string
  // Coarse host classification stored in repos.host.
  host: RepoHost
}

const SHORTHAND_HOSTS: Record<string, string> = {
  github: 'github.com',
  gitlab: 'gitlab.com',
  bitbucket: 'bitbucket.org',
  gist: 'gist.github.com',
}

const HOST_ENUM: Record<string, RepoHost> = {
  'github.com': 'github',
  'gitlab.com': 'gitlab',
  'bitbucket.org': 'bitbucket',
}

// GitHub/GitLab repo paths are case-insensitive — lowercase owner/name so the
// same repo never produces two distinct repos.url keys.
const CASE_INSENSITIVE_HOSTS = new Set(['github.com', 'gitlab.com'])

/**
 * Canonicalize a source-repository URL to `{ url, host }` where url is
 * `https://<host>/<owner>/<name>` and host is the coarse classification stored
 * in `repos.host`.
 *
 * Shared across the registry sub-workers (npm, Maven, …) and the GitHub
 * enricher so `repos.url` keys never diverge per ADR 0001. Handles npm
 * shorthand (`github:owner/repo`, bare `owner/repo`), SSH scp form, `ssh://`,
 * `git+`, `git://`, `www.`, and monorepo `/tree/<branch>/<path>` deep-links
 * (only the first two path segments are kept). Returns null when the input
 * cannot be reduced to an owner/name pair.
 */
export function canonicalizeRepoUrl(raw: string): CanonicalRepo | null {
  let s = raw.trim().replace(/#.*$/, '')
  if (!s) return null

  const sh = s.match(/^(github|gitlab|bitbucket|gist):(.+)$/)
  if (sh) {
    s = `https://${SHORTHAND_HOSTS[sh[1]]}/${sh[2]}`
  } else if (!s.includes('://') && !s.includes('@') && /^[\w.-]+\/[\w.-]+$/.test(s)) {
    s = `https://github.com/${s}`
  }

  s = s.replace(/^git\+/, '')

  const scp = s.match(/^git@([^:]+):(.+)$/)
  if (scp) {
    s = `https://${scp[1]}/${scp[2]}`
  }

  s = s.replace(/^ssh:\/\/git@([^/]+)\//, 'https://$1/')
  s = s.replace(/^git:\/\//, 'https://')

  let u: URL
  try {
    u = new URL(s)
  } catch {
    return null
  }

  const hostname = u.hostname.toLowerCase().replace(/^www\./, '')
  const segments = u.pathname.split('/').filter(Boolean)
  if (segments.length < 2) return null

  const isKnownHost = hostname in HOST_ENUM
  let ownerPath = isKnownHost ? [segments[0]] : segments.slice(0, -1)
  let name = (isKnownHost ? segments[1] : segments[segments.length - 1]).replace(/\.git$/, '')
  if (!name || ownerPath.length === 0 || ownerPath.some((seg) => !seg)) return null

  if (CASE_INSENSITIVE_HOSTS.has(hostname)) {
    ownerPath = ownerPath.map((seg) => seg.toLowerCase())
    name = name.toLowerCase()
  }

  return {
    url: `https://${hostname}/${[...ownerPath, name].join('/')}`,
    host: HOST_ENUM[hostname] ?? 'other',
  }
}
