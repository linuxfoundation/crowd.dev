import {
  CARGO_REACHABILITY_PROMPT,
  CARGO_VERDICT_SCHEMA,
  buildCargoReachabilitySystemPrompt,
} from '../../agent/cargoPrompts'
import { downloadAndExtractTarball } from '../../clients/npmTarball'
import {
  crateSourceUrl,
  fetchCrateLatestVersion,
  fetchCrateVersions,
} from '../../crates/registryClient'
import { highestVersion } from '../../semverRange'
import { ReachabilitySourceConfig } from '../reachabilityStage'

const CRATES_IO_FETCH_TIMEOUT_MS = 15_000

// Resolve concrete crate version: try fetchCrateLatestVersion first, then full list.
async function resolveCargoVersion(name: string): Promise<string | null> {
  const latest = await fetchCrateLatestVersion(name, CRATES_IO_FETCH_TIMEOUT_MS)
  if (typeof latest === 'string') return latest

  const versions = await fetchCrateVersions(name, CRATES_IO_FETCH_TIMEOUT_MS)
  if (Array.isArray(versions)) return highestVersion(versions)
  return null
}

export const cargoReachabilityConfig: ReachabilitySourceConfig = {
  prompt: CARGO_REACHABILITY_PROMPT,
  schema: CARGO_VERDICT_SCHEMA,
  buildSystemPrompt: buildCargoReachabilitySystemPrompt,
  prepareSource: async (dep) => {
    // Use resolved version; fall back to highest published (ensures Cargo.lock consistency).
    const version = dep.version ?? (await resolveCargoVersion(dep.name))
    if (!version) return null
    return {
      download: (destDir) => downloadAndExtractTarball(crateSourceUrl(dep.name, version), destDir),
    }
  },
  noSourceMessage: 'Could not resolve a concrete crate version',
  downloadErrorPrefix: 'Crate download failed',
}
