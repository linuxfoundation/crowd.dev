import {
  PYPI_REACHABILITY_PROMPT,
  PYPI_VERDICT_SCHEMA,
  buildPyPiReachabilitySystemPrompt,
} from '../../agent/pypiPrompts'
import { downloadAndExtractPypiSource } from '../../clients/pypiSource'
import { toPypiNormalizedName } from '../../packageIdentifier'
import { ReachabilitySourceConfig } from '../reachabilityStage'

// PyPI is a deps.dev EDGE ecosystem; dep.version is the dependent's resolved version.
export const pypiReachabilityConfig: ReachabilitySourceConfig = {
  prompt: PYPI_REACHABILITY_PROMPT,
  schema: PYPI_VERDICT_SCHEMA,
  buildSystemPrompt: buildPyPiReachabilitySystemPrompt,
  prepareSource: async (dep) => {
    if (!dep.version) return null
    const normalizedName = toPypiNormalizedName(dep.name)
    return {
      download: (destDir) => downloadAndExtractPypiSource(normalizedName, dep.version, destDir),
    }
  },
  noSourceMessage: 'Could not resolve a concrete PyPI project version',
  downloadErrorPrefix: 'PyPI source download failed',
}
