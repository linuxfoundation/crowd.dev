import type { Manifest, SyncDefinition } from './types'

const manifests = new Map<string, Manifest>()

export function registerConnector(manifest: Manifest): void {
  manifests.set(manifest.platform, manifest)
}

export function getManifest(platform: string): Manifest {
  const manifest = manifests.get(platform)
  if (!manifest) {
    throw new Error(`unknown platform ${platform}`)
  }
  return manifest
}

export function getSync(platform: string, syncName: string): SyncDefinition {
  const sync = getManifest(platform).syncs.find((s) => s.name === syncName)
  if (!sync) {
    throw new Error(`unknown sync ${platform}/${syncName}`)
  }
  return sync
}
