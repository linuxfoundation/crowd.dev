import type { Packument } from './types'

export function parseNpmName(raw: string): { namespace: string | null; name: string } {
  if (raw.startsWith('@')) {
    const slash = raw.indexOf('/')
    if (slash !== -1) {
      return { namespace: raw.slice(0, slash), name: raw.slice(slash + 1) }
    }
  }
  return { namespace: null, name: raw }
}

export function buildPurl(raw: string): string {
  const { namespace, name } = parseNpmName(raw)
  return namespace ? `pkg:npm/${namespace}/${name}` : `pkg:npm/${name}`
}

export function normalizeLicenses(packument: Packument): string[] {
  const rawArr = packument.licenses
  if (rawArr && Array.isArray(rawArr)) {
    return dedup(rawArr.map((l) => clean(l.type)).filter(Boolean))
  }

  const raw = packument.license
  if (!raw) return []
  if (typeof raw === 'object') {
    return raw.type ? [clean(raw.type)] : []
  }

  if (!raw || raw === 'UNLICENSED' || raw.startsWith('SEE LICENSE')) return []

  return dedup(
    raw
      .split(/\s+(?:OR|AND)\s+/i)
      .map((s) => clean(s))
      .filter(Boolean),
  )
}

function clean(s: string): string {
  return s.replace(/[()]/g, '').trim()
}

function dedup(arr: string[]): string[] {
  return [...new Set(arr)]
}

export function extractRepoUrl(packument: Packument): string | null {
  const repo = packument.repository
  if (!repo) return null
  const raw = typeof repo === 'string' ? repo : repo.url
  if (!raw) return null
  return canonicalizeRepoUrl(raw)
}

const SHORTHAND_HOSTS: Record<string, string> = {
  github: 'github.com',
  gitlab: 'gitlab.com',
  bitbucket: 'bitbucket.org',
  gist: 'gist.github.com',
}

function canonicalizeRepoUrl(raw: string): string | null {
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

  const owner = segments[0]
  const name = segments[1].replace(/\.git$/, '')
  if (!owner || !name) return null

  return `https://${hostname}/${owner}/${name}`
}

export function collectMaintainers(packument: Packument): Array<{
  username: string
  displayName: string | null
  email: string | null
  role: 'author' | 'maintainer'
}> {
  const map = new Map<
    string,
    {
      username: string
      displayName: string | null
      email: string | null
      role: 'author' | 'maintainer'
    }
  >()

  for (const m of packument.maintainers ?? []) {
    if (!m.name) continue
    map.set(m.name, {
      username: m.name,
      displayName: m.name,
      email: m.email ?? null,
      role: 'maintainer',
    })
  }

  const author = packument.author
  if (author) {
    const parsed =
      typeof author === 'string'
        ? parseAuthorString(author)
        : { name: author.name, email: author.email ?? null }
    if (parsed.name) {
      map.set(parsed.name, {
        username: parsed.name,
        displayName: parsed.name,
        email: parsed.email,
        role: 'author',
      })
    }
  }

  return [...map.values()]
}

function parseAuthorString(s: string): { name: string; email: string | null } {
  const emailMatch = s.match(/<([^>]+)>/)
  const name = s
    .replace(/<[^>]+>/, '')
    .replace(/\([^)]+\)/, '')
    .trim()
  return { name, email: emailMatch ? emailMatch[1] : null }
}

export function isPrerelease(version: string): boolean {
  return /^[0-9]+\.[0-9]+\.[0-9]+-.+/.test(version)
}
