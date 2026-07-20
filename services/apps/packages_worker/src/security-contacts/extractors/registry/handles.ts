import { ProvenanceEntry, RawContact } from '../../types'

// Registry usernames are only *guessed* to be GitHub logins — callers emit them as candidates
// that must pass verifyHandleCandidates corroboration before becoming contacts.
export function toHandleCandidates(
  handles: unknown[],
  source: string,
  sourceUrl: string,
  fetchedAt: string,
): RawContact[] {
  const prov = (): ProvenanceEntry[] => [{ source, sourceTier: 'B', path: sourceUrl, fetchedAt }]
  const candidates: RawContact[] = []
  const seen = new Set<string>()
  for (const handle of handles) {
    if (typeof handle !== 'string' || handle.length === 0) continue
    const key = handle.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    candidates.push({
      channel: 'github-handle',
      value: handle,
      role: 'maintainer',
      tier: 'B',
      provenance: prov(),
    })
  }
  return candidates
}
