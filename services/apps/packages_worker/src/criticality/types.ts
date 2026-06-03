export interface CentralityInput {
  ecosystem: string
}

export interface CentralityResult {
  ecosystem:  string
  nodeCount:  number
  edgeCount:  number
  iterations: number
  durationMs: number
}

// Five signals per ADR-0001 Criticality scoring methodology. Must sum to 1.0.
export interface CriticalityWeights {
  wCentrality: number  // 0.40
  wTransitive: number  // 0.10
  wDepPkgs:    number  // 0.20
  wDepRepos:   number  // 0.15
  wDownloads:  number  // 0.15
}
