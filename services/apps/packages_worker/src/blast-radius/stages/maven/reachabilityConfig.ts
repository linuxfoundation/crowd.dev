import { resolveLatestVersion } from '../../../maven/metadata'
import { resolveBlastRadiusMavenBaseUrl } from '../../../maven/registry'
import {
  MAVEN_REACHABILITY_PROMPT,
  MAVEN_VERDICT_SCHEMA,
  buildMavenReachabilitySystemPrompt,
} from '../../agent/mavenPrompts'
import { downloadAndExtractMavenSources } from '../../clients/mavenSourcesJar'
import { toBareMavenCoordinate } from '../../packageIdentifier'
import { ReachabilitySourceConfig } from '../reachabilityStage'

// Prefer the version the reverse-dependency edge resolved against; fall back to
// Maven Central's current release when the dependent only declared a range/floor.
async function resolveMavenVersion(groupId: string, artifactId: string): Promise<string | null> {
  return resolveLatestVersion(groupId, artifactId, resolveBlastRadiusMavenBaseUrl(groupId))
}

export const mavenReachabilityConfig: ReachabilitySourceConfig = {
  prompt: MAVEN_REACHABILITY_PROMPT,
  schema: MAVEN_VERDICT_SCHEMA,
  buildSystemPrompt: buildMavenReachabilitySystemPrompt,
  prepareSource: async (dep) => {
    const { groupId, artifactId } = toBareMavenCoordinate(dep.name)
    const version = dep.version ?? (await resolveMavenVersion(groupId, artifactId))
    if (!version) return null
    return {
      download: (destDir) => downloadAndExtractMavenSources(groupId, artifactId, version, destDir),
    }
  },
  noSourceMessage: 'Could not resolve a concrete Maven artifact version',
  downloadErrorPrefix: 'Maven sources jar download failed',
}
