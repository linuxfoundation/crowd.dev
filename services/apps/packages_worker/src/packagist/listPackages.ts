import type { Dispatcher } from 'undici'

import { buildPackagistUserAgent, describeFetchFailure, packagistDispatcher } from './fetchPackage'
import type { FetchError } from './types'

const PACKAGIST_LIST = 'https://packagist.org/packages/list.json'
// Non-backtracking form: a mandatory separator per repetition (vs. an optional one)
// removes the ambiguity that let the old `([_.-]?[a-z0-9]+)*` pattern backtrack
// exponentially on a long run of the same character class (CodeQL js/redos).
// Vendor and project segments have different rules per Composer's own name spec: the
// vendor allows only a single separator between alnum runs, but the project segment
// additionally allows up to two consecutive hyphens (e.g. `vendor/my--package` is a
// valid, real Composer name) — verified against Composer's ArrayLoader name pattern.
const COMPOSER_VENDOR_REGEX = /^[a-z0-9]+(?:[_.-][a-z0-9]+)*$/
const COMPOSER_PROJECT_REGEX = /^[a-z0-9]+(?:(?:[_.]|-{1,2})[a-z0-9]+)*$/

export interface PackagistListEntry {
  vendor: string
  name: string
  purl: string
}

export function parsePackagistPackageList(json: unknown): {
  entries: PackagistListEntry[]
  invalid: number
} {
  if (typeof json !== 'object' || json === null) {
    throw new TypeError('list.json must be an object')
  }

  const root = json as { packageNames?: unknown }
  if (!Array.isArray(root.packageNames)) {
    throw new TypeError('packageNames must be an array')
  }

  const seen = new Set<string>()
  const entries: PackagistListEntry[] = []
  let invalid = 0

  for (const item of root.packageNames) {
    if (typeof item !== 'string') {
      invalid++
      continue
    }

    const lowercased = item.toLowerCase()
    const parts = lowercased.split('/')

    // Validate: exactly one slash, each side non-empty and matches its composer name pattern
    if (
      parts.length !== 2 ||
      !parts[0] ||
      !parts[1] ||
      !COMPOSER_VENDOR_REGEX.test(parts[0]) ||
      !COMPOSER_PROJECT_REGEX.test(parts[1])
    ) {
      invalid++
      continue
    }

    // Dedup on the lowercased form
    if (seen.has(lowercased)) {
      continue
    }
    seen.add(lowercased)

    entries.push({
      vendor: parts[0],
      name: parts[1],
      purl: `pkg:composer/${parts[0]}/${parts[1]}`,
    })
  }

  return { entries, invalid }
}

export async function fetchPackagistPackageList(): Promise<unknown | FetchError> {
  const abort = new AbortController()
  // 30s timer covering the body read too
  const timer = setTimeout(() => abort.abort(), 30_000)

  try {
    let res: Response
    try {
      const init: RequestInit & { dispatcher?: Dispatcher } = {
        headers: {
          Accept: 'application/json',
          'User-Agent': buildPackagistUserAgent(),
        },
        signal: abort.signal,
        dispatcher: packagistDispatcher,
      }
      res = await fetch(PACKAGIST_LIST, init as RequestInit)
    } catch (err) {
      return { kind: 'TRANSIENT', message: describeFetchFailure(err) }
    }

    // Status classification: 404 NOT_FOUND, 429 RATE_LIMIT, other non-ok TRANSIENT
    if (res.status === 404) return { kind: 'NOT_FOUND', message: 'list not found', statusCode: 404 }
    if (res.status === 429) return { kind: 'RATE_LIMIT', message: 'rate limited', statusCode: 429 }
    if (!res.ok) return { kind: 'TRANSIENT', message: `HTTP ${res.status}`, statusCode: res.status }

    let json: unknown
    try {
      json = await res.json()
    } catch {
      if (abort.signal.aborted) return { kind: 'TRANSIENT', message: 'body read timed out' }
      return { kind: 'MALFORMED', message: 'invalid JSON' }
    }

    return json
  } finally {
    clearTimeout(timer)
  }
}
