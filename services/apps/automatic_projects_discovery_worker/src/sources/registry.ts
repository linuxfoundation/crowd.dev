import { InsightsDiscussionsSource } from './insights-discussions/source'
// import { LfCriticalityScoreSource } from './lf-criticality-score/source'
import { IDiscoverySource } from './types'

// lf-criticality-score disabled locally: no production deployment yet, will be reintegrated later
const sources: IDiscoverySource[] = [
  // new LfCriticalityScoreSource(),
  new InsightsDiscussionsSource(),
]

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
