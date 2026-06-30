import type { PyPiInfo } from './types'

const PURL_PYPI_PREFIX = 'pkg:pypi/'

// Postgres text columns cannot store NUL (U+0000). Built at runtime so there is no
// NUL literal in the source.
const NUL_GLOBAL = new RegExp(String.fromCharCode(0), 'g')

// The PyPI project name from a purl. PyPI purls are `pkg:pypi/<name>` (no namespace);
// the name segment is percent-encoded per the purl spec, so decode it to get the
// registry name used by the JSON API.
export function pypiNameFromPurl(purl: string): string {
  return decodeURIComponent(purl.slice(PURL_PYPI_PREFIX.length))
}

// Strip NUL bytes in place from every string before persisting — otherwise the
// inlined value breaks the PostgreSQL wire protocol ("invalid message format").
export function stripNullBytesDeep<T>(value: T): T {
  if (typeof value === 'string') {
    return value.replace(NUL_GLOBAL, '') as T
  }
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) value[i] = stripNullBytesDeep(value[i])
    return value
  }
  if (value !== null && typeof value === 'object') {
    const obj = value as Record<string, unknown>
    for (const k of Object.keys(obj)) obj[k] = stripNullBytesDeep(obj[k])
    return value
  }
  return value
}

function blankToNull(s: string | null | undefined): string | null {
  if (s == null) return null
  const t = s.trim()
  return t || null
}

function dedup(arr: string[]): string[] {
  return [...new Set(arr)]
}

// Split an SPDX-ish expression ("MIT", "MIT OR Apache-2.0", "(MIT AND BSD-3-Clause)")
// into individual license tokens.
function splitSpdx(raw: string): string[] {
  return raw
    .split(/\s+(?:OR|AND|WITH)\s+/i)
    .map((s) => s.replace(/[()]/g, '').trim())
    .filter(Boolean)
}

// Common trove "License :: ..." classifiers → SPDX identifiers.
const TROVE_TO_SPDX: Record<string, string> = {
  'License :: OSI Approved :: MIT License': 'MIT',
  'License :: OSI Approved :: MIT No Attribution License (MIT-0)': 'MIT-0',
  'License :: OSI Approved :: Apache Software License': 'Apache-2.0',
  'License :: OSI Approved :: BSD License': 'BSD-3-Clause',
  'License :: OSI Approved :: ISC License (ISCL)': 'ISC',
  'License :: OSI Approved :: GNU General Public License v2 (GPLv2)': 'GPL-2.0-only',
  'License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)': 'GPL-2.0-or-later',
  'License :: OSI Approved :: GNU General Public License v3 (GPLv3)': 'GPL-3.0-only',
  'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)': 'GPL-3.0-or-later',
  'License :: OSI Approved :: GNU Lesser General Public License v2 (LGPLv2)': 'LGPL-2.0-only',
  'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)':
    'LGPL-2.1-or-later',
  'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)': 'LGPL-3.0-only',
  'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)':
    'LGPL-3.0-or-later',
  'License :: OSI Approved :: GNU Affero General Public License v3': 'AGPL-3.0-only',
  'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)':
    'AGPL-3.0-or-later',
  'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)': 'MPL-2.0',
  'License :: OSI Approved :: Python Software Foundation License': 'PSF-2.0',
  'License :: OSI Approved :: The Unlicense (Unlicense)': 'Unlicense',
  'License :: OSI Approved :: zlib/libpng License': 'Zlib',
  'License :: OSI Approved :: Boost Software License 1.0 (BSL-1.0)': 'BSL-1.0',
  'License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication': 'CC0-1.0',
}

// Grouping/ambiguous classifier nodes that carry no usable license identifier.
const TROVE_IGNORE = new Set([
  'License :: OSI Approved',
  'License :: Other/Proprietary License',
  'License :: Public Domain',
  'License :: Freely Distributable',
  'License :: Freeware',
  'License :: DFSG approved',
])

function licensesFromClassifiers(classifiers: string[]): string[] {
  const out: string[] = []
  for (const raw of classifiers) {
    const c = raw.trim()
    if (!c.startsWith('License ::')) continue
    if (TROVE_IGNORE.has(c)) continue
    const mapped = TROVE_TO_SPDX[c]
    if (mapped) {
      out.push(mapped)
      continue
    }
    // Fallback: classifier leaf (after the last "::"), stripping a trailing "License".
    const leaf = c.split('::').pop()?.trim()
    if (leaf) out.push(leaf.replace(/\s+License$/i, '').trim() || leaf)
  }
  return out
}

function isShortLicenseToken(s: string): boolean {
  return !s.includes('\n') && s.length <= 60
}

// PyPI license priority: PEP 639 SPDX expression → "License ::" classifiers → a short
// `license` token. A long/multiline `license` value is full license text (not an
// identifier), so it only feeds licenses_raw. Returns SPDX-ish tokens for the
// `licenses` array plus the original string for `licenses_raw`.
export function resolvePypiLicenses(info: PyPiInfo): {
  licenses: string[]
  licensesRaw: string | null
} {
  const expr = blankToNull(info.license_expression)
  const licenseStr = blankToNull(info.license)
  const licensesRaw = expr ?? licenseStr

  if (expr) return { licenses: dedup(splitSpdx(expr)), licensesRaw }

  const fromClassifiers = licensesFromClassifiers(info.classifiers ?? [])
  if (fromClassifiers.length) return { licenses: dedup(fromClassifiers), licensesRaw }

  if (licenseStr && isShortLicenseToken(licenseStr)) {
    return { licenses: dedup(splitSpdx(licenseStr)), licensesRaw }
  }
  return { licenses: [], licensesRaw }
}

// PEP 440 pre-release / dev-release detection. True when the version carries an
// alpha/beta/rc marker (a, b, c/rc, or spelled out) or a dev segment. Post-releases
// (".postN") are NOT prereleases. Local ("+...") and epoch ("N!") parts are ignored.
export function isPypiPrerelease(version: string): boolean {
  const v = version.trim().toLowerCase().split('+')[0].replace(/^\d+!/, '')
  if (/(?:^|[\d._-])dev[._-]?\d*$/.test(v)) return true
  if (/[\d._-](?:alpha|beta|preview|pre|rc|a|b|c)[._-]?\d*$/.test(v)) return true
  return false
}

export interface PypiPerson {
  username: string
  displayName: string | null
  email: string | null
  role: 'author' | 'maintainer'
}

function parseNameEmail(s: string): { name: string | null; email: string | null } {
  const trimmed = s.trim()
  const m = trimmed.match(/^(.*?)\s*<([^>]+)>$/)
  if (m) return { name: blankToNull(m[1]), email: blankToNull(m[2]) }
  if (/^[^\s@]+@[^\s@]+$/.test(trimmed)) return { name: null, email: trimmed }
  return { name: blankToNull(trimmed), email: null }
}

function splitList(s: string): string[] {
  return s
    .split(',')
    .map((p) => p.trim())
    .filter(Boolean)
}

function peopleForRole(
  nameField: string | null | undefined,
  emailField: string | null | undefined,
  role: 'author' | 'maintainer',
): PypiPerson[] {
  const emailParts = emailField ? splitList(emailField) : []
  const nameParts = nameField ? splitList(nameField) : []
  const raw: Array<{ name: string | null; email: string | null }> = []

  if (emailParts.length) {
    // Modern packages put "Name <email>" (often several, comma-separated) in *_email.
    emailParts.forEach((part, i) => {
      const pe = parseNameEmail(part)
      raw.push({ name: pe.name ?? nameParts[i] ?? null, email: pe.email })
    })
  } else if (nameParts.length) {
    nameParts.forEach((n) => raw.push({ name: blankToNull(n), email: null }))
  }

  const out: PypiPerson[] = []
  for (const p of raw) {
    const username = p.name ?? p.email
    if (!username) continue
    out.push({ username, displayName: p.name, email: p.email, role })
  }
  return out
}

// Build maintainers from PyPI's free-text author/maintainer fields. PyPI exposes no
// account usernames, so the parsed name (falling back to email) is the synthetic
// username. Author overwrites maintainer on a username collision
export function collectPypiMaintainers(info: PyPiInfo): PypiPerson[] {
  const map = new Map<string, PypiPerson>()
  for (const p of peopleForRole(info.maintainer, info.maintainer_email, 'maintainer')) {
    map.set(p.username, p)
  }
  for (const p of peopleForRole(info.author, info.author_email, 'author')) {
    map.set(p.username, p)
  }
  return [...map.values()]
}

export interface PypiFundingLink {
  type: string
  url: string
}

function inferFundingType(url: string): string {
  if (/github\.com/i.test(url)) return 'github'
  if (/patreon\.com/i.test(url)) return 'patreon'
  if (/opencollective\.com/i.test(url)) return 'opencollective'
  if (/ko-fi\.com/i.test(url)) return 'ko-fi'
  if (/tidelift\.com/i.test(url)) return 'tidelift'
  return 'other'
}

export function classifyProjectUrls(
  projectUrls: Record<string, string> | null | undefined,
  homePage: string | null | undefined,
): {
  homepage: string | null
  declaredRepositoryUrl: string | null
  fundingLinks: PypiFundingLink[]
} {
  const entries = Object.entries(projectUrls ?? {}).map(
    ([k, v]) => [k.trim(), (v ?? '').trim()] as const,
  )
  const findByKey = (re: RegExp): string | null =>
    entries.find(([k, v]) => re.test(k) && v)?.[1] ?? null

  const homepage =
    blankToNull(homePage) ??
    findByKey(/^homepage$/i) ??
    findByKey(/^home[\s-]*page$/i) ??
    findByKey(/^home$/i)

  const REPO_HOST = /github\.com|gitlab\.com|bitbucket\.org/i
  let declaredRepositoryUrl =
    findByKey(/^source(\s*code)?$/i) ??
    findByKey(/^repository$/i) ??
    findByKey(/^repo$/i) ??
    findByKey(/^code$/i) ??
    entries.find(([k, v]) => /source|repo|code|git/i.test(k) && REPO_HOST.test(v))?.[1] ??
    null
  // Many projects only declare a Homepage that is itself the repo.
  if (!declaredRepositoryUrl && homepage && REPO_HOST.test(homepage)) {
    declaredRepositoryUrl = homepage
  }

  const seen = new Set<string>()
  const fundingLinks: PypiFundingLink[] = []
  for (const [k, v] of entries) {
    if (!v || !/fund|donate|sponsor/i.test(k) || seen.has(v)) continue
    seen.add(v)
    fundingLinks.push({ type: inferFundingType(v), url: v })
  }

  return { homepage, declaredRepositoryUrl, fundingLinks }
}

export function parseKeywords(raw: string | null | undefined): string[] {
  if (!raw) return []
  return dedup(
    raw
      .split(/[,\s]+/)
      .map((k) => k.trim())
      .filter(Boolean),
  )
}
