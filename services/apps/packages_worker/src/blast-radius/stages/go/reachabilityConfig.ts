import { fetchLatest, fetchVersionList } from '../../../go/proxyClient'
import {
  GO_REACHABILITY_PROMPT,
  GO_VERDICT_SCHEMA,
  buildGoReachabilitySystemPrompt,
} from '../../agent/goPrompts'
import { downloadAndExtractGoModule } from '../../clients/goModuleZip'
import { highestVersion } from '../../semverRange'
import { ReachabilitySourceConfig } from '../reachabilityStage'

const GO_PROXY_FETCH_TIMEOUT_MS = 15_000

// Go dependents only carry a require-directive FLOOR (see goConstraint.ts), not an
// installed version like npm's packument-resolved tarball — resolve a concrete version
// to analyze here, preferring the module's current @latest and falling back to the
// highest entry in @v/list if @latest is unreachable/rate-limited.
async function resolveGoVersion(module: string): Promise<string | null> {
  const latest = await fetchLatest(module, GO_PROXY_FETCH_TIMEOUT_MS)
  if ('version' in latest) return latest.version

  const versionList = await fetchVersionList(module, GO_PROXY_FETCH_TIMEOUT_MS)
  if (Array.isArray(versionList)) return highestVersion(versionList)

  return null
}

export const goReachabilityConfig: ReachabilitySourceConfig = {
  prompt: GO_REACHABILITY_PROMPT,
  schema: GO_VERDICT_SCHEMA,
  buildSystemPrompt: buildGoReachabilitySystemPrompt,
  prepareSource: async (dep) => {
    // Prefer the exact version the reverse-dependency edge was resolved against (see
    // getReverseDependents) — falling back to @latest would analyze different code than
    // the go.mod that produced this constraint.
    const version = dep.version ?? (await resolveGoVersion(dep.name))
    if (!version) return null
    return { download: (destDir) => downloadAndExtractGoModule(dep.name, version, destDir) }
  },
  noSourceMessage: 'Could not resolve a concrete Go module version',
  downloadErrorPrefix: 'Go module download failed',
}
