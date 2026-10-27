import type { IMirrorSweepInput } from './mirrorSweep'

export const MIRROR_RESOLVE_CONCURRENCY = 5

export function mirrorRoom(input: IMirrorSweepInput, inShadow: number): number {
  return Math.max(0, Math.min(input.batchSize, input.shadowCap - inShadow))
}

export function chunk<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}
