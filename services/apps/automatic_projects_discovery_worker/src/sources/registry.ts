import { InsightsDiscussionsSource } from './insights-discussions/source'
import { LfCriticalityScoreSource } from './lf-criticality-score/source'
import { IDiscoverySource } from './types'

const allSources: IDiscoverySource[] = [
  new InsightsDiscussionsSource(),
  new LfCriticalityScoreSource(),
]

function resolveEnabledSources(): IDiscoverySource[] {
  const raw = process.env.CROWD_DISCOVERY_SOURCES
  if (!raw) {
    return allSources
  }

  const requestedNames = raw
    .split(',')
    .map((name) => name.trim())
    .filter(Boolean)

  const knownNames = allSources.map((s) => s.name)
  const unknownNames = requestedNames.filter((name) => !knownNames.includes(name))
  if (unknownNames.length > 0) {
    throw new Error(
      `Unknown source(s) in CROWD_DISCOVERY_SOURCES: ${unknownNames.join(', ')}. Available: ${knownNames.join(', ')}`,
    )
  }

  return allSources.filter((s) => requestedNames.includes(s.name))
}

const sources = resolveEnabledSources()

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
