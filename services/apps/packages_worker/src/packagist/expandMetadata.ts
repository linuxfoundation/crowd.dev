import type { PackagistExpandedVersion, PackagistMinifiedVersion } from './types'

export function expandComposerMetadata(
  versions: PackagistMinifiedVersion[],
): PackagistExpandedVersion[] {
  if (versions.length === 0) return []

  const result: PackagistExpandedVersion[] = []
  let current: Record<string, unknown> = {}

  for (const minified of versions) {
    // Merge: start with previous expanded, apply the minified diff
    const next = { ...current }
    for (const [key, value] of Object.entries(minified)) {
      if (value === '__unset') {
        delete next[key]
      } else {
        next[key] = value
      }
    }

    // `next` is never mutated after this — the following iteration spreads it
    // into a fresh object — so it can be pushed directly.
    result.push(next as PackagistExpandedVersion)
    current = next
  }

  return result
}
