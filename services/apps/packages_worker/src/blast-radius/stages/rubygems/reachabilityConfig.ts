import { fetchVersions } from '../../../rubygems/client'
import { isRubyGemsFetchError } from '../../../rubygems/types'
import {
  RUBYGEMS_REACHABILITY_PROMPT,
  RUBYGEMS_VERDICT_SCHEMA,
  buildRubyGemsReachabilitySystemPrompt,
} from '../../agent/rubygemsPrompts'
import { downloadAndExtractRubyGemsSource } from '../../clients/rubygemsSource'
import { highestVersion } from '../ecosystemVersions'
import { ReachabilitySourceConfig } from '../reachabilityStage'

// deps.dev never resolves a concrete version for RubyGems edges (see
// dependentsScanRubyGems.ts), so dep.version is always null — fall back to the package's
// current highest published version.
async function resolveRubyGemsVersion(packageName: string): Promise<string | null> {
  const versionsResult = await fetchVersions(packageName)
  if (isRubyGemsFetchError(versionsResult)) return null
  return highestVersion(
    'rubygems',
    versionsResult.map((v) => v.number),
  )
}

export const rubygemsReachabilityConfig: ReachabilitySourceConfig = {
  prompt: RUBYGEMS_REACHABILITY_PROMPT,
  schema: RUBYGEMS_VERDICT_SCHEMA,
  buildSystemPrompt: buildRubyGemsReachabilitySystemPrompt,
  prepareSource: async (dep) => {
    const version = dep.version ?? (await resolveRubyGemsVersion(dep.name))
    if (!version) return null
    return {
      download: (destDir) => downloadAndExtractRubyGemsSource(dep.name, version, destDir),
    }
  },
  noSourceMessage: 'Could not resolve a concrete RubyGems package version',
  downloadErrorPrefix: 'RubyGems source download failed',
}
