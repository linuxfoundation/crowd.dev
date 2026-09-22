export const DOC_DISCOVERY_METHODS = [
  'llms-txt-probe',
  'docs-subdomain',
  'docs-path',
  'package-manifest',
  'readme-scrape',
  'github-homepage',
  'serp',
  'project-website',
  'override',
] as const

export type DocDiscoveryMethod = (typeof DOC_DISCOVERY_METHODS)[number]

export const DOC_DISCOVERY_CONFIDENCES = ['authoritative', 'high', 'medium', 'low'] as const

export type DocDiscoveryConfidence = (typeof DOC_DISCOVERY_CONFIDENCES)[number]

export interface IDocCandidate {
  url: string
  method: DocDiscoveryMethod
  confidence: DocDiscoveryConfidence
  livenessOk: boolean
}

export interface IDbProjectDocDiscovery {
  projectId: string
  docsUrl: string | null
  discoveryMethod: DocDiscoveryMethod | null
  confidence: DocDiscoveryConfidence | null
  candidates: IDocCandidate[]
  discoveredAt: string
  createdAt: string
  updatedAt: string
}

export interface IProjectDocDiscoveryUpsert {
  projectId: string
  docsUrl: string | null
  discoveryMethod: DocDiscoveryMethod | null
  confidence: DocDiscoveryConfidence | null
  candidates: IDocCandidate[]
}
