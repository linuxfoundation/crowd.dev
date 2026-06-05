export interface CentralityInput {
  ecosystem: string
}

export interface CentralityResult {
  ecosystem: string
  nodeCount: number
  edgeCount: number
  iterations: number
  durationMs: number
}
