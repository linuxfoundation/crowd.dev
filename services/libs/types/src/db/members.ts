import type { IMemberContribution } from '../members'

export interface MemberDbRow {
  id: string
  displayName: string
  joinedAt: string
  tenantId: string
  createdAt: string
  updatedAt: string
  manuallyCreated: boolean
  attributes: Record<string, unknown> | null
  reach: Record<string, number> | null
  score: number | null
  contributions: IMemberContribution[] | null
  enrichedBy: string[] | null
  manuallyChangedFields: string[] | null
  importHash: string | null
  deletedAt: string | null
  createdById: string | null
  updatedById: string | null
}

export type MemberDbInsert = Pick<MemberDbRow, 'displayName' | 'joinedAt'> &
  Partial<
    Omit<
      MemberDbRow,
      'displayName' | 'joinedAt' | 'tenantId' | 'createdAt' | 'updatedAt' | 'deletedAt'
    >
  >
