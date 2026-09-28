import type { IDocCandidate } from '@crowd/data-access-layer'

import { isGithubWebsite, normalizedDomain } from './http'
import { candidateHasDocsSignal, methodPriority, rankCandidates, repoNameAnchor } from './rank'
import { type IDiscoveryContext, STRATEGIES, serpStrategy } from './strategies'

export interface IDiscoverDocsResult {
  docsUrl: string | null
  discoveryMethod: IDocCandidate['method'] | null
  confidence: IDocCandidate['confidence'] | null
  allCandidates: IDocCandidate[]
}

// A live duplicate always wins over a dead one; between two live duplicates, keep the one whose
// method ranking treats as more authoritative rather than whichever strategy happened to run first.
function dedupeByUrl(candidates: IDocCandidate[]): IDocCandidate[] {
  const byUrl = new Map<string, IDocCandidate>()
  for (const c of candidates) {
    const existing = byUrl.get(c.url)
    const dominatesOnLiveness = c.livenessOk && !existing?.livenessOk
    const dominatesOnMethod =
      c.livenessOk === existing?.livenessOk &&
      methodPriority(c.method) > methodPriority(existing.method)
    if (!existing || dominatesOnLiveness || dominatesOnMethod) {
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

  // GitHub and shared websites are not this project's domain, so they give no affinity anchor.
  const websiteDomain = ctx.website ? normalizedDomain(ctx.website) : null
  const githubWebsite = !!ctx.website && isGithubWebsite(ctx.website)
  const projectDomain = githubWebsite || ctx.websiteShared ? null : websiteDomain
  const projectNameHint = ctx.website && githubWebsite ? repoNameAnchor(ctx.website) : null
  const winner = rankCandidates(allCandidates, projectDomain, projectNameHint, ctx.sharedDocsUrls)

  return {
    docsUrl: winner?.url ?? null,
    discoveryMethod: winner?.method ?? null,
    confidence: winner?.confidence ?? null,
    allCandidates,
  }
}
