const SHORTHAND_HOSTS: Record<string, string> = {
  github: 'github.com',
  gitlab: 'gitlab.com',
  bitbucket: 'bitbucket.org',
  gist: 'gist.github.com',
}

// GitHub/GitLab repo paths are case-insensitive — lowercase owner/name so the
// same repo never produces two distinct repos.url keys.
const CASE_INSENSITIVE_HOSTS = new Set(['github.com', 'gitlab.com'])

/**
 * Canonicalize a source-repository URL to `https://<host>/<owner>/<name>`.
 *
 * Shared across the registry sub-workers (npm, Maven, …) and the GitHub
 * enricher so `repos.url` keys never diverge per ADR 0001. Handles npm
 * shorthand (`github:owner/repo`, bare `owner/repo`), SSH scp form, `ssh://`,
 * `git+`, `git://`, `www.`, and monorepo `/tree/<branch>/<path>` deep-links
 * (only the first two path segments are kept). Returns null when the input
 * cannot be reduced to an owner/name pair.
 */
export function canonicalizeRepoUrl(raw: string): string | null {
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

  let owner = segments[0]
  let name = segments[1].replace(/\.git$/, '')
  if (!owner || !name) return null

  if (CASE_INSENSITIVE_HOSTS.has(hostname)) {
    owner = owner.toLowerCase()
    name = name.toLowerCase()
  }

  return `https://${hostname}/${owner}/${name}`
}
