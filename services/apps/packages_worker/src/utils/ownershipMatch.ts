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
  'github',
  'gitlab',
  'bitbucket',
  'sourceforge',
  'codeberg',
])

// Reverse-DNS namespaces (Maven `org.apache.commons`, NuGet-style `Com.Foo.Bar`) carry the
// owner in one of their segments, not in the whole string — `io.github.<owner>` even puts it
// last. For multi-segment namespaces, compare only identity-bearing segments (not the joined
// form, which would prefix-match structural prefixes against lookalike owners). Flat scopes
// such as `@vercel` or `tokio-rs` are single-segment — normalise the whole string so vanity
// suffix stripping applies.
function namespaceCandidates(namespace: string): string[] {
  const segments = namespace.split(/[./]/).filter(Boolean)
  if (segments.length === 1) {
    return [normalizeIdentity(namespace)].filter(Boolean)
  }
  const identityBearing = segments.filter((s) => !STRUCTURAL_SEGMENTS.has(s.toLowerCase()))
  return identityBearing.map(normalizeIdentity).filter(Boolean)
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
    ...(evidence.maintainers ?? []).map((m) => (m ? normalizeIdentity(m) : '')),
  ].filter(Boolean)

  if (candidates.length === 0) return 'no_evidence'
  return candidates.some((c) => isSameIdentity(c, repoOwner)) ? 'matched' : 'unmatched'
}

// GitLab subgroups make the owner the first path segment, not the second-to-last one.
export function repoOwnerFromCanonical(repo: CanonicalRepo): string | null {
  const path = repo.url.replace(/^https?:\/\/[^/]+\//, '').split('/')
  return path.length >= 2 ? path[0] : null
}
