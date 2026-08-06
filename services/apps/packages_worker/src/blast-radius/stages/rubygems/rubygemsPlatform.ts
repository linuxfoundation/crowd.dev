import { fetchVersions } from '../../../rubygems/client'
import { isRubyGemsFetchError } from '../../../rubygems/types'
import { RubyGemsVersionItem } from '../../../rubygems/types'

// Multiple platform-specific artifacts can share the same version `number`; prefer the
// universal `ruby` platform, falling back since some gems are never published for it.
export function pickPlatform(entries: RubyGemsVersionItem[], version: string): string | null {
  const matches = entries.filter((v) => v.number === version)
  if (matches.some((v) => (v.platform ?? 'ruby') === 'ruby')) return 'ruby'
  return matches[0]?.platform ?? null
}

export async function resolveGemPlatform(name: string, version: string): Promise<string | null> {
  const versionsResult = await fetchVersions(name)
  if (isRubyGemsFetchError(versionsResult)) return null
  return pickPlatform(versionsResult, version)
}
