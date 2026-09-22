import { fetchVersionList } from '../../../nuget/client'
import { isNuGetFetchError } from '../../../nuget/types'
import {
  NUGET_REACHABILITY_PROMPT,
  NUGET_VERDICT_SCHEMA,
  buildNuGetReachabilitySystemPrompt,
} from '../../agent/nugetPrompts'
import { downloadAndExtractNuGetSource } from '../../clients/nugetSource'
import { highestVersion } from '../ecosystemVersions'
import { ReachabilitySourceConfig } from '../reachabilityStage'

// deps.dev never resolves a concrete version for NuGet edges (see dependentsScanNuGet.ts),
// so dep.version is always null — fall back to the package's current highest listed version.
async function resolveNuGetVersion(packageId: string): Promise<string | null> {
  const versionList = await fetchVersionList(packageId)
  if (isNuGetFetchError(versionList)) return null
  return highestVersion('nuget', versionList)
}

export const nugetReachabilityConfig: ReachabilitySourceConfig = {
  prompt: NUGET_REACHABILITY_PROMPT,
  schema: NUGET_VERDICT_SCHEMA,
  buildSystemPrompt: buildNuGetReachabilitySystemPrompt,
  prepareSource: async (dep) => {
    const version = dep.version ?? (await resolveNuGetVersion(dep.name))
    if (!version) return null
    return {
      download: (destDir) => downloadAndExtractNuGetSource(dep.name, version, destDir),
    }
  },
  noSourceMessage: 'Could not resolve a concrete NuGet package version with GitHub source',
  downloadErrorPrefix: 'NuGet source download failed',
}
