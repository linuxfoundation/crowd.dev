import { canonicalizeRepoUrl } from '../utils/canonicalizeRepoUrl'
import type { CanonicalRepo } from '../utils/canonicalizeRepoUrl'

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

// A version's `license` field comes in several shapes: a plain SPDX string ("MIT"),
// an object ({ type, url }), or the legacy array form ([{ type, file }, ...]). Passing those
// raw into a text column would persist objects/arrays, so collapse every shape to a single
// string (OR-joined for the array form) or null. Non-string or blank `type` values are dropped.
export function versionLicense(raw: unknown): string | null {
  if (raw == null) return null
  if (typeof raw === 'string') return blankToNull(raw)
  if (Array.isArray(raw)) {
    const types = raw
      .map((l) => (typeof l === 'string' ? blankToNull(l) : licenseType(l)))
      .filter((t): t is string => t !== null)
    return types.length ? types.join(' OR ') : null
  }
  return licenseType(raw)
}

function licenseType(v: unknown): string | null {
  if (typeof v !== 'object' || v === null) return null
  const type = (v as { type?: unknown }).type
  return typeof type === 'string' ? blankToNull(type) : null
}

function blankToNull(s: string): string | null {
  const trimmed = s.trim()
  return trimmed || null
}

function clean(s: string): string {
  return s.replace(/[()]/g, '').trim()
}

function dedup(arr: string[]): string[] {
  return [...new Set(arr)]
}

export function extractRepo(packument: Packument): CanonicalRepo | null {
  const repo = packument.repository
  if (!repo) return null
  const raw = typeof repo === 'string' ? repo : repo.url
  if (!raw) return null
  return canonicalizeRepoUrl(raw)
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
  const name = s.split(/[<(]/)[0].trim()
  return { name, email: emailMatch ? emailMatch[1] : null }
}

export function isPrerelease(version: string): boolean {
  return /^[0-9]+\.[0-9]+\.[0-9]+-.+/.test(version)
}
