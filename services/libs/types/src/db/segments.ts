import type { SegmentStatus, SegmentType } from '../segments'

export interface SegmentDbRow {
  id: string
  slug: string
  tenantId: string
  status: SegmentStatus
  isLF: boolean
  createdAt: string
  updatedAt: string
  url: string | null
  name: string | null
  parentName: string | null
  grandparentName: string | null
  parentSlug: string | null
  grandparentSlug: string | null
  description: string | null
  sourceId: string | null
  sourceParentId: string | null
  customActivityTypes: Record<string, unknown> | null
  activityChannels: Record<string, unknown> | null
  dashboardCacheLastRefreshedAt: string | null
  parentId: string | null
  grandparentId: string | null
  type: SegmentType
  maturity: string | null
}

export type SegmentDbInsert = Pick<SegmentDbRow, 'slug'> &
  Partial<
    Omit<
      SegmentDbRow,
      'slug' | 'tenantId' | 'createdAt' | 'updatedAt' | 'dashboardCacheLastRefreshedAt' | 'type' // GENERATED ALWAYS from parentSlug/grandparentSlug
    >
  >
