import { Context } from '@temporalio/activity'

import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'
import { buildGraph, computePageRank } from './graph'
import { loadDirectEdges, mergeCentralityScores } from './queries'
import { CentralityInput, CentralityResult } from './types'

const log = getServiceChildLogger('criticality')

function getConfig() {
  return {
    damping:     Number(process.env.CRITICALITY_PAGERANK_DAMPING)                  || 0.85,
    maxIter:     parseInt(process.env.CRITICALITY_PAGERANK_MAX_ITER   ?? '', 10)   || 100,
    convergence: Number(process.env.CRITICALITY_PAGERANK_CONVERGENCE)              || 1e-6,
  }
}

export async function criticalityComputePageRank(input: CentralityInput): Promise<CentralityResult> {
  const { ecosystem } = input
  const { damping, maxIter, convergence } = getConfig()
  const start = Date.now()
  const qx = await getPackagesDb()

  // ── Step 1: build CSR graph
  const edges = await loadDirectEdges(qx, ecosystem)
  const edgeCount = edges.length
  const graph = buildGraph(edges)
  edges.length = 0  // release JS edge objects — CSR holds all graph data
  log.info({ ecosystem, nodeCount: graph.N, edgeCount }, 'graph loaded')

  // ── Step 2 & 3: PageRank
  const { scores, iterations } = computePageRank(
    graph, damping, maxIter, convergence,
    (iter, delta) => {
      try { Context.current().heartbeat({ ecosystem, iter, delta }) } catch { /* standalone */ }
    },
  )
  log.info({ ecosystem, iterations, nodeCount: graph.N }, 'PageRank converged')

  // ── Step 4: merge centrality_score into packages_universe
  const batch = Array.from(graph.nodeIndex.entries()).map(([packageId, idx]) => ({
    packageId,
    centralityScore: scores[idx],
  }))

  const CHUNK = 10_000
  for (let i = 0; i < batch.length; i += CHUNK) {
    await mergeCentralityScores(qx, batch.slice(i, i + CHUNK))
  }

  return { ecosystem, nodeCount: graph.N, edgeCount, iterations, durationMs: Date.now() - start }
}
