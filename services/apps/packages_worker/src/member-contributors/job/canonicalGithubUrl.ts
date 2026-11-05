const GITHUB_HOSTS = new Set(['github.com', 'www.github.com'])
const SEGMENT = /^[a-z0-9._-]+$/i

export type GithubUrlParse =
  | { kind: 'github'; url: string; owner: string; name: string }
  | { kind: 'other'; host: string }
  | { kind: 'invalid' }

function parseUrl(raw: string): URL | null {
  const trimmed = raw.trim()
  const withScheme = /^[a-z][a-z0-9+.-]*:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`
  try {
    return new URL(withScheme)
  } catch {
    return null
  }
}

export function canonicalGithubUrl(raw: string): GithubUrlParse {
  const url = parseUrl(raw)
  if (!url) return { kind: 'invalid' }
  const host = url.hostname.toLowerCase()
  if (!GITHUB_HOSTS.has(host)) return { kind: 'other', host }
  const [owner, repo] = url.pathname.split('/').filter(Boolean)
  const name = repo?.replace(/\.git$/i, '')
  if (!owner || !name || !SEGMENT.test(owner) || !SEGMENT.test(name)) return { kind: 'invalid' }
  const lowerOwner = owner.toLowerCase()
  const lowerName = name.toLowerCase()
  return {
    kind: 'github',
    url: `https://github.com/${lowerOwner}/${lowerName}`,
    owner: lowerOwner,
    name: lowerName,
  }
}
