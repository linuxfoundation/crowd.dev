import { DirectEdge } from './queries'

export interface Graph {
  nodeIndex: Map<number, number>
  nodeIds: number[]
  numDeps: Int32Array // how many packages each node depends on (divides its vote)
  rowPtr: Int32Array // rowPtr[v] = start of v's dependents in colData
  colData: Int32Array // flat array of dependent node indices
  N: number
}

export function buildGraph(edges: DirectEdge[]): Graph {
  // Pass 0: assign contiguous indices
  const nodeIndex = new Map<number, number>()
  const nodeIds: number[] = []

  for (const { packageId, dependsOnId } of edges) {
    if (!nodeIndex.has(packageId)) {
      nodeIndex.set(packageId, nodeIds.length)
      nodeIds.push(packageId)
    }
    if (!nodeIndex.has(dependsOnId)) {
      nodeIndex.set(dependsOnId, nodeIds.length)
      nodeIds.push(dependsOnId)
    }
  }

  const N = nodeIds.length

  // Pass 1: count
  const depCount = new Int32Array(N)
  const numDeps = new Int32Array(N)

  for (const { packageId, dependsOnId } of edges) {
    depCount[nodeIndex.get(dependsOnId) as number]++
    numDeps[nodeIndex.get(packageId) as number]++
  }

  // Build rowPtr (prefix sum of depCount)
  const rowPtr = new Int32Array(N + 1)
  for (let i = 0; i < N; i++) rowPtr[i + 1] = rowPtr[i] + depCount[i]

  // Pass 2: fill colData
  const colData = new Int32Array(edges.length)
  const fillIdx = new Int32Array(N)

  for (const { packageId, dependsOnId } of edges) {
    const v = nodeIndex.get(dependsOnId) as number
    colData[rowPtr[v] + fillIdx[v]++] = nodeIndex.get(packageId) as number
  }

  return { nodeIndex, nodeIds, numDeps, rowPtr, colData, N }
}

export function computePageRank(
  { numDeps, rowPtr, colData, N }: Graph,
  damping = 0.85,
  maxIter = 100,
  convergence = 1e-6,
  onIteration?: (iter: number, delta: number) => void,
): { scores: Float64Array; iterations: number } {
  if (N === 0) return { scores: new Float64Array(0), iterations: 0 }

  const teleportation = (1 - damping) / N // base score every node gets regardless of links
  let scores = new Float64Array(N).fill(1 / N) // start: equal weight for all nodes
  let next = new Float64Array(N) // scratch buffer, swapped each iteration
  let iters = 0

  for (iters = 1; iters <= maxIter; iters++) {
    // Every node starts with the teleportation floor
    next.fill(teleportation)

    // Each node v collects votes from packages that depend on it.
    // numDeps[dependent] is always >= 1 here — only packages with at least one
    // outgoing edge appear in colData, so division by zero cannot occur.
    // Dangling nodes (numDeps = 0) never appear in colData; their score
    // accumulates but never redistributes. This is acceptable because scores
    // are used for relative ranking via pct_rank(), not as absolute values.
    for (let v = 0; v < N; v++) {
      let incoming = 0
      for (let j = rowPtr[v]; j < rowPtr[v + 1]; j++) {
        const dependent = colData[j]
        incoming += scores[dependent] / numDeps[dependent]
      }
      next[v] = teleportation + damping * incoming
    }

    // L1 delta: total change in scores across all nodes
    let delta = 0
    for (let i = 0; i < N; i++) delta += Math.abs(next[i] - scores[i])
    ;[scores, next] = [next, scores] // swap buffers — no data copy
    onIteration?.(iters, delta)

    if (delta < convergence) break // scores have stabilised
  }

  return { scores, iterations: iters }
}

export function getDependents(graph: Graph, packageId: number): number[] {
  const idx = graph.nodeIndex.get(packageId)
  if (idx === undefined) return []
  const { rowPtr, colData, nodeIds } = graph
  const result: number[] = []
  for (let j = rowPtr[idx]; j < rowPtr[idx + 1]; j++) {
    result.push(nodeIds[colData[j]])
  }
  return result
}
