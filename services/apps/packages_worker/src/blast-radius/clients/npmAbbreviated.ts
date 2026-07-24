import type { FetchError } from '../../npm/types'

const REGISTRY = 'https://registry.npmjs.org'
const USER_AGENT = 'lfx-packages-worker/0.1 (+https://lfx.linuxfoundation.org)'

function encodeNpmName(name: string): string {
  return name.startsWith('@') ? `@${encodeURIComponent(name.slice(1))}` : encodeURIComponent(name)
}

export interface AbbreviatedVersion {
  dependencies?: Record<string, string>
  peerDependencies?: Record<string, string>
  optionalDependencies?: Record<string, string>
  dist?: { tarball?: string }
}

export interface AbbreviatedPackument {
  'dist-tags': Record<string, string>
  versions: Record<string, AbbreviatedVersion>
}

// npm's abbreviated metadata format (dependencies/dist-tags only, no readme/maintainers/etc.) —
// used for the high-volume candidate pre-scan in dependentsScan.ts, where only the declared
// dependency ranges matter and the full packument payload would be wasted bandwidth.
export async function fetchAbbreviatedPackument(
  name: string,
): Promise<AbbreviatedPackument | FetchError> {
  const url = `${REGISTRY}/${encodeNpmName(name)}`
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)
  let res: Response
  try {
    res = await fetch(url, {
      headers: {
        Accept: 'application/vnd.npm.install-v1+json; q=1.0, application/json; q=0.8',
        'User-Agent': USER_AGENT,
      },
      signal: abort.signal,
    })
  } catch (err) {
    return { kind: 'TRANSIENT', message: String(err) }
  } finally {
    clearTimeout(timer)
  }

  if (res.status === 404)
    return { kind: 'NOT_FOUND', message: `${name} not found`, statusCode: 404 }
  if (res.status === 429) return { kind: 'RATE_LIMIT', message: 'rate limited', statusCode: 429 }
  if (!res.ok) return { kind: 'TRANSIENT', message: `HTTP ${res.status}`, statusCode: res.status }

  let json: unknown
  try {
    json = await res.json()
  } catch {
    return { kind: 'MALFORMED', message: 'invalid JSON' }
  }

  if (
    typeof json !== 'object' ||
    json === null ||
    !('versions' in json) ||
    !('dist-tags' in json)
  ) {
    return { kind: 'MALFORMED', message: 'unexpected shape' }
  }

  return json as AbbreviatedPackument
}
