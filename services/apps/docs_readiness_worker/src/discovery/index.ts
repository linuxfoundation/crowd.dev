import type { IDocCandidate } from '@crowd/data-access-layer'

import { normalizedDomain } from './http'
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

  // github.com is a shared host, not a project domain — using it for affinity would wrongly
  // treat GitHub's own docs as on-domain when a project's website is just its repo URL.
  const websiteDomain = ctx.website ? normalizedDomain(ctx.website) : null
  const isGithubWebsite = websiteDomain === 'github.com'
  const projectDomain = isGithubWebsite ? null : websiteDomain
  const projectNameHint = ctx.website && isGithubWebsite ? repoNameAnchor(ctx.website) : null
  const winner = rankCandidates(allCandidates, projectDomain, projectNameHint)

  return {
    docsUrl: winner?.url ?? null,
    discoveryMethod: winner?.method ?? null,
    confidence: winner?.confidence ?? null,
    allCandidates,
  }
}
