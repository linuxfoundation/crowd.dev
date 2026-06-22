export const STEWARDSHIP_STATUS_VALUES = [
  'unassigned',
  'open',
  'assessing',
  'active',
  'needs_attention',
  'escalated',
  'blocked',
  'inactive',
] as const

export type StewardshipStatus = (typeof STEWARDSHIP_STATUS_VALUES)[number]

export type Lifecycle = 'active' | 'stable' | 'declining' | 'abandoned'

export type SeverityLevel = 'critical' | 'high' | 'medium' | 'low'

export interface OpenVulns {
  low: number
  medium: number
  high: number
  critical: number
}

export interface Steward {
  userId: string
  username: string | null
  displayName: string | null
  role: 'lead' | 'co_steward'
  assignedAt: string
}

export interface StewardshipSummary {
  name: string
  ecosystem: string
  lifecycle: Lifecycle | null
  health: number | null
  impact: number | null
  openVulns: OpenVulns | null
  stewardship: StewardshipStatus | null
  stewards: Steward[] | null
  lastActivityAt: string | null
  lastActivityDescription: string | null
}
