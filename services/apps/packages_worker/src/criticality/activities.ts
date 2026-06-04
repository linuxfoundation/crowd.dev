import { Context } from '@temporalio/activity'

import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'

import { buildGraph, computePageRank } from './graph'
import { loadDirectEdges, mergeCentralityScores } from './queries'
import { CentralityInput, CentralityResult } from './types'

const log = getServiceChildLogger('criticality')

const PAGERANK_DAMPING = 0.85
const PAGERANK_MAX_ITER = 100
const PAGERANK_CONVERGENCE = 1e-6

export async function criticalityComputePageRank(
  input: CentralityInput,
): Promise<CentralityResult> {
  const { ecosystem } = input
  const damping = PAGERANK_DAMPING
  const maxIter = PAGERANK_MAX_ITER
  const convergence = PAGERANK_CONVERGENCE
  const start = Date.now()
  const qx = await getPackagesDb()

  // ── Step 1: build CSR graph
  const edges = await loadDirectEdges(qx, ecosystem)
  const edgeCount = edges.length
  const graph = buildGraph(edges)
  edges.length = 0 // release JS edge objects — CSR holds all graph data
  log.info({ ecosystem, nodeCount: graph.N, edgeCount }, 'graph loaded')

  // ── Step 2 & 3: PageRank
  const { scores, iterations } = computePageRank(
    graph,
    damping,
    maxIter,
    convergence,
    (iter, delta) => {
      try {
        Context.current().heartbeat({ ecosystem, iter, delta })
      } catch {
        /* standalone */
      }
    },
  )
  log.info({ ecosystem, iterations, nodeCount: graph.N }, 'PageRank converged')

  // ── Step 4: merge centrality_score into packages_universe
  // Stream map entries into fixed-size chunks — O(CHUNK) extra memory, not O(N).
  const CHUNK = 10_000
  let buffer: Array<{ packageId: number; centralityScore: number }> = []

  for (const [packageId, idx] of graph.nodeIndex) {
    buffer.push({ packageId, centralityScore: scores[idx] })
    if (buffer.length === CHUNK) {
      await mergeCentralityScores(qx, buffer)
      buffer = []
    }
  }
  if (buffer.length > 0) await mergeCentralityScores(qx, buffer)

  return { ecosystem, nodeCount: graph.N, edgeCount, iterations, durationMs: Date.now() - start }
}
