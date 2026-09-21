import type { IDocCandidate } from '@crowd/data-access-layer'

import { normalizedDomain } from './http'
import { candidateHasDocsSignal, rankCandidates } from './rank'
import { type IDiscoveryContext, STRATEGIES, serpStrategy } from './strategies'

export interface IDiscoverDocsResult {
  docsUrl: string | null
  discoveryMethod: IDocCandidate['method'] | null
  confidence: IDocCandidate['confidence'] | null
  allCandidates: IDocCandidate[]
}

function dedupeByUrl(candidates: IDocCandidate[]): IDocCandidate[] {
  const byUrl = new Map<string, IDocCandidate>()
  for (const c of candidates) {
    const existing = byUrl.get(c.url)
    if (!existing || (c.livenessOk && !existing.livenessOk)) {
      byUrl.set(c.url, c)
    }
  }
  return [...byUrl.values()]
}

async function runStrategies(
  strategies: Array<(ctx: IDiscoveryContext) => Promise<IDocCandidate[]>>,
  ctx: IDiscoveryContext,
): Promise<IDocCandidate[]> {
  const results = await Promise.allSettled(strategies.map((fn) => fn(ctx)))
  const candidates: IDocCandidate[] = []
  for (const result of results) {
    if (result.status === 'fulfilled') {
      candidates.push(...result.value)
    }
  }
  return candidates
}

export async function discoverDocs(ctx: IDiscoveryContext): Promise<IDiscoverDocsResult> {
  const baseCandidates = dedupeByUrl(await runStrategies(STRATEGIES, ctx))

  const hasLiveCandidate = baseCandidates.some((c) => c.livenessOk && candidateHasDocsSignal(c))
  const allCandidates =
    !hasLiveCandidate && ctx.serpApiKey
      ? dedupeByUrl([...baseCandidates, ...(await runStrategies([serpStrategy], ctx))])
      : baseCandidates

  const projectDomain = ctx.website ? normalizedDomain(ctx.website) : null
  const winner = rankCandidates(allCandidates, projectDomain)

  return {
    docsUrl: winner?.url ?? null,
    discoveryMethod: winner?.method ?? null,
    confidence: winner?.confidence ?? null,
    allCandidates,
  }
}
