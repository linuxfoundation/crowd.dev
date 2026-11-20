import { SegmentType } from '@crowd/types'

import type { PccParquetRow } from '../parser'
import type { CdpSegment, CdpSegmentIndex } from './creationPlan'

export const PROJECT_NOT_IN_PCC = 'PROJECT_NOT_IN_PCC'

const PCC_PROJECT_ID_PATTERN = /^(a09[A-Za-z0-9]{15}|lf[A-Za-z0-9]{16})$/

export interface ProjectMissingFromPcc {
  pccProjectId: string
  subprojects: Pick<CdpSegment, 'id' | 'name' | 'slug'>[]
}

// The export lists leaf projects only: a parent shows up as an ancestor of its leaves.
export function addPccProjectIdsOfRow(
  pccProjectIds: Set<string>,
  row: Record<string, unknown>,
): void {
  const { PROJECT_ID, MAPPED_PROJECT_ID } = row as Partial<PccParquetRow>
  for (const value of [PROJECT_ID, MAPPED_PROJECT_ID]) {
    const pccProjectId = value == null ? '' : String(value).trim()
    if (pccProjectId) pccProjectIds.add(pccProjectId)
  }
}

export function findProjectsMissingFromPcc(
  cdpSegments: CdpSegmentIndex,
  exportedPccProjectIds: Set<string>,
): ProjectMissingFromPcc[] {
  if (exportedPccProjectIds.size === 0) return []

  const missing: ProjectMissingFromPcc[] = []
  for (const [sourceId, segments] of cdpSegments.byAnyLevelSourceId) {
    if (exportedPccProjectIds.has(sourceId) || !PCC_PROJECT_ID_PATTERN.test(sourceId)) continue
    const subprojects = segments
      .filter((segment) => segment.type === SegmentType.SUB_PROJECT)
      .map(({ id, name, slug }) => ({ id, name, slug }))
    if (subprojects.length > 0) missing.push({ pccProjectId: sourceId, subprojects })
  }
  return missing
}
