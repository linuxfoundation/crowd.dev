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

// dep.name is packages.name ('-'/'_' normalized, see cargo/loadDump.ts), not necessarily
// crates.io's published spelling — static.crates.io needs the latter, so always resolve it.
async function resolveCargoCanonical(
  name: string,
): Promise<{ name: string; version: string | null } | null> {
  const latest = await fetchCrateLatestVersion(name, CRATES_IO_FETCH_TIMEOUT_MS)
  if ('name' in latest) return { name: latest.name, version: latest.version }

  const versionsResult = await fetchCrateVersions(name, CRATES_IO_FETCH_TIMEOUT_MS)
  if ('name' in versionsResult) {
    return { name: versionsResult.name, version: highestVersion(versionsResult.versions) }
  }
  return null
}

export const cargoReachabilityConfig: ReachabilitySourceConfig = {
  prompt: CARGO_REACHABILITY_PROMPT,
  schema: CARGO_VERDICT_SCHEMA,
  buildSystemPrompt: buildCargoReachabilitySystemPrompt,
  prepareSource: async (dep) => {
    const canonical = await resolveCargoCanonical(dep.name)
    if (!canonical) return null
    // Prefer the already-resolved dependency version; fall back to highest published.
    const version = dep.version ?? canonical.version
    if (!version) return null
    return {
      download: (destDir) =>
        downloadAndExtractTarball(crateSourceUrl(canonical.name, version), destDir),
    }
  },
  noSourceMessage: 'Could not resolve a concrete crate version',
  downloadErrorPrefix: 'Crate download failed',
}
