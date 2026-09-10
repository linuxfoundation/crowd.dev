import {
  RUBYGEMS_REACHABILITY_PROMPT,
  RUBYGEMS_VERDICT_SCHEMA,
  buildRubyGemsReachabilitySystemPrompt,
} from '../../agent/rubygemsPrompts'
import { downloadAndExtractRubyGemsSource } from '../../clients/rubygemsSource'
import { ReachabilitySourceConfig } from '../reachabilityStage'

import { resolveGemPlatform } from './rubygemsPlatform'

// Unlike Go/NuGet, RubyGems dependent rows carry the dependent's own declared version
// (never null) — noSourceMessage below is only reachable if that guarantee is violated.
export const rubygemsReachabilityConfig: ReachabilitySourceConfig = {
  prompt: RUBYGEMS_REACHABILITY_PROMPT,
  schema: RUBYGEMS_VERDICT_SCHEMA,
  buildSystemPrompt: buildRubyGemsReachabilitySystemPrompt,
  prepareSource: async (dep) => {
    if (!dep.version) return null
    const version = dep.version
    return {
      download: async (destDir: string) => {
        const platform = await resolveGemPlatform(dep.name, version)
        await downloadAndExtractRubyGemsSource(dep.name, version, destDir, platform)
      },
    }
  },
  noSourceMessage: 'Dependent has no recorded version',
  downloadErrorPrefix: 'RubyGems source download failed',
}
