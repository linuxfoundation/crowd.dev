export type ProtocolMethodType =
  | 'github-pvr'
  | 'email'
  | 'web-form'
  | 'bounty-platform'
  | 'security-txt'
  | 'mailing-list'

export type ProtocolMethodStatus = 'preferred' | 'accepted' | 'fallback' | 'prohibited'

export type MethodConfidence = 'declared' | 'inferred'

export interface ParsedMethod {
  type: ProtocolMethodType
  status: ProtocolMethodStatus
  endpoint: string
  condition: string | null
}

export interface ProtocolGuidelines {
  generalPrinciples: string[]
  avoid: string[]
  recommend: Array<{ scenario: string; action: string }>
}

export interface ParsedProtocol {
  methods: ParsedMethod[]
  guidelines: ProtocolGuidelines | null
}

export type ParseRowStatus = 'ok' | 'template' | 'degraded'

export interface ClassifierVerdict {
  clean: boolean
  isTemplate: boolean
  pointerOnly: boolean
  methods: ParsedMethod[]
  linkedUrls: string[]
}

export interface ProtocolMethod extends ParsedMethod {
  confidence: MethodConfidence
  provenance: {
    path?: string
    blobOid?: string
    url?: string
    parser?: 'deterministic' | 'llm'
    api?: string
    channel?: string
  }
}

export interface AssembledProtocol {
  declared: boolean
  methods: ProtocolMethod[]
  guidelines: ProtocolGuidelines | null
  sources: Array<Record<string, string>>
}

export interface ParseStageResult {
  blobsParsed: number
  deterministic: number
  llm: number
  degraded: number
  template: number
  linkedPages: number
  failed: number
  llmCostUsd: number
}

export interface AssembleStageResult {
  reposAssembled: number
  declaredCount: number
}
