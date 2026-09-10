import {
  REACHABILITY_PROMPT,
  VERDICT_SCHEMA,
  buildReachabilitySystemPrompt,
} from '../../agent/prompts'
import { downloadAndExtractTarball } from '../../clients/npmTarball'
import { ReachabilitySourceConfig } from '../reachabilityStage'

export const npmReachabilityConfig: ReachabilitySourceConfig = {
  prompt: REACHABILITY_PROMPT,
  schema: VERDICT_SCHEMA,
  buildSystemPrompt: buildReachabilitySystemPrompt,
  // npm already has the tarball URL persisted from the dependents stage — no version
  // resolution needed at reachability time.
  prepareSource: async (dep) => {
    if (!dep.tarball_url) return null
    return { download: (destDir) => downloadAndExtractTarball(dep.tarball_url, destDir) }
  },
  noSourceMessage: 'No tarball URL available',
  downloadErrorPrefix: 'Tarball download failed',
}
