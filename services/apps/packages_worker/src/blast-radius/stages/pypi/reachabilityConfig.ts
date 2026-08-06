import {
  PYPI_REACHABILITY_PROMPT,
  PYPI_VERDICT_SCHEMA,
  buildPyPiReachabilitySystemPrompt,
} from '../../agent/pypiPrompts'
import { downloadAndExtractPypiSource } from '../../clients/pypiSource'
import { toPypiNormalizedName } from '../../packageIdentifier'
import { ReachabilitySourceConfig } from '../reachabilityStage'

// PyPI is a deps.dev EDGE ecosystem, so dep.version is the dependent's own resolved
// version and is never null — no canonical-name round trip is needed the way Cargo's
// resolveCargoCanonical requires, since the PyPI JSON API accepts normalized names.
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
