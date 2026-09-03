import type { PackageRepoOwnershipMatch } from '@crowd/data-access-layer/src/packages/repoConfidence'

import type { CanonicalRepo } from './canonicalizeRepoUrl'

export interface OwnershipEvidence {
  // Registry namespace: npm scope, Maven groupId, packagist vendor, ... — null when the
  // ecosystem has no namespace concept (cargo, rubygems, nuget).
  namespace?: string | null
  maintainers?: Array<string | null | undefined>
  repoOwner: string | null
}

const VANITY_SUFFIXES = ['-ai', '-io', '-team', '-labs', '-oss', '-dev']

function normalizeIdentity(raw: string): string {
  let s = raw.trim().toLowerCase()
  if (!s) return ''
  s = s.replace(/^@/, '')
  for (const suffix of VANITY_SUFFIXES) {
    if (s.endsWith(suffix) && s.length > suffix.length + 1) {
      s = s.slice(0, -suffix.length)
      break
    }
  }
  return s.replace(/[^a-z0-9]/g, '')
}

// Structural labels in reverse-DNS namespaces (TLDs, VCS hostnames) are not owner identities.
// `io.github.attacker` must not produce `github` as a candidate — only `attacker` is identity-bearing.
const STRUCTURAL_SEGMENTS = new Set([
  'com',
  'org',
  'net',
  'io',
  'dev',
  'app',
  'co',
  // common country-code TLDs that appear in Maven group IDs (e.g. uk.co.foo, au.com.bar)
  'uk',
  'au',
  'us',
  'de',
  'fr',
  'in',
  'jp',
  'cn',
  'br',
  'eu',
  'ru',
  'nl',
  'it',
  'es',
  'pl',
  'se',
  'nz',
  'za',
  'mx',
  'ar',
  'github',
  'gitlab',
  'bitbucket',
  'sourceforge',
  'codeberg',
])

// Reverse-DNS namespaces (Maven `org.apache.commons`) carry the owner in one of their segments.
// For multi-segment namespaces, if the first segment is a structural label (TLD like `org`,
// `com`, `io`) skip it — otherwise keep it (e.g., Packagist vendors like `foo.bar` or NuGet
// namespaces like `Microsoft.Extensions` where the first segment is identity-bearing). Flat
// scopes (`@vercel`, `tokio-rs`) are single-segment — normalise the whole string.
// normalizeIdentity already strips vanity suffixes, so `github-ai` → `github` hits the
// set-membership check directly; isSameIdentity is not needed here and would over-eagerly
// discard legitimate identities like `github-tools` (normalises to `githubtools`).
function namespaceCandidates(namespace: string): string[] {
  const segments = namespace.split(/[./]/).filter(Boolean)
  if (segments.length === 1) {
    return [normalizeIdentity(namespace)].filter(Boolean)
  }
  const firstNorm = normalizeIdentity(segments[0])
  const firstIsStructural = STRUCTURAL_SEGMENTS.has(firstNorm)
  return segments
    .slice(firstIsStructural ? 1 : 0)
    .map(normalizeIdentity)
    .filter((normalized) => normalized && !STRUCTURAL_SEGMENTS.has(normalized))
}

function isSameIdentity(a: string, b: string): boolean {
  if (!a || !b) return false
  if (a === b) return true
  const [short, long] = a.length <= b.length ? [a, b] : [b, a]
  return short.length >= 4 && long.startsWith(short)
}

export function matchOwnership(evidence: OwnershipEvidence): PackageRepoOwnershipMatch {
  const repoOwner = evidence.repoOwner ? normalizeIdentity(evidence.repoOwner) : ''
  if (!repoOwner) return 'no_evidence'

  const candidates = [
    ...(evidence.namespace ? namespaceCandidates(evidence.namespace) : []),
    ...(evidence.maintainers ?? [])
      .filter((m) => m && !/\S@\S/.test(m))
      .map((m) => normalizeIdentity(m!)),
  ].filter(Boolean)

  if (candidates.length === 0) return 'no_evidence'
  return candidates.some((c) => isSameIdentity(c, repoOwner)) ? 'matched' : 'unmatched'
}

// GitLab subgroups make the owner the first path segment, not the second-to-last one.
// Returns null for host=other: those URLs preserve full path segments (e.g. sourceforge /p/foo/code)
// so path[0] is not the owner.
export function repoOwnerFromCanonical(repo: CanonicalRepo): string | null {
  if (repo.host === 'other') return null
  const path = repo.url.replace(/^https?:\/\/[^/]+\//, '').split('/')
  return path.length >= 2 ? path[0] : null
}
