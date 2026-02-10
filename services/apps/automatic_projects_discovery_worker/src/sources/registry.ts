import { OssfCriticalityScoreSource } from './ossf-criticality-score/source'
import { IDiscoverySource } from './types'

// To add a new source: instantiate it here.
const sources: IDiscoverySource[] = [new OssfCriticalityScoreSource()]

export function getSource(name: string): IDiscoverySource {
  const source = sources.find((s) => s.name === name)
  if (!source) {
    throw new Error(`Unknown source: ${name}. Available: ${sources.map((s) => s.name).join(', ')}`)
  }
  return source
}

export function getAvailableSourceNames(): string[] {
  return sources.map((s) => s.name)
}
