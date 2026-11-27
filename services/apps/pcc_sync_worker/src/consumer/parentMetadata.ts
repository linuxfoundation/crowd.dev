import { DEFAULT_TENANT_ID } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'

import type { PccParentMetadata } from '../parser'

export type ParentMetadataMode = 'off' | 'dry-run' | 'on'

export function readParentMetadataMode(env: NodeJS.ProcessEnv = process.env): ParentMetadataMode {
  const raw = env.PCC_PARENT_METADATA_SYNC
  return raw === 'on' || raw === 'dry-run' ? raw : 'off'
}

export function effectiveParentMetadataMode(
  mode: ParentMetadataMode,
  syncIsDryRun: boolean,
): ParentMetadataMode {
  if (mode === 'off') return 'off'
  return syncIsDryRun ? 'dry-run' : mode
}

export interface CdpParentSegment {
  id: string
  type: string
  name: string
  sourceId: string
  status: string | null
  maturity: string | null
  description: string | null
}

export interface CdpParentInsightsProject {
  id: string
  segmentId: string
  description: string | null
  logoUrl: string | null
}

export interface FieldChange {
  from: string | null
  to: string
}

export interface ParentSegmentUpdate {
  segmentId: string
  segmentType: string
  segmentName: string
  pccProjectId: string
  status?: FieldChange
  maturity?: FieldChange
  description?: FieldChange
}

export interface ParentInsightsProjectUpdate {
  insightsProjectId: string
  segmentId: string
  pccProjectId: string
  description?: FieldChange
  logoUrl?: FieldChange
}

export interface ParentMetadataPlan {
  segments: ParentSegmentUpdate[]
  insightsProjects: ParentInsightsProjectUpdate[]
}

export function addParents(
  parentsByPccProjectId: Map<string, PccParentMetadata>,
  parents: PccParentMetadata[],
): void {
  for (const parent of parents) {
    if (!parentsByPccProjectId.has(parent.pccProjectId)) {
      parentsByPccProjectId.set(parent.pccProjectId, parent)
    }
  }
}

function changeTo(current: string | null, next: string | null): FieldChange | undefined {
  if (!next || next === current) return undefined
  return { from: current, to: next }
}

// PCC leaves a parent's field empty more often than not: an empty value never replaces what CDP holds.
export function planParentMetadata(
  parentsByPccProjectId: Map<string, PccParentMetadata>,
  segments: CdpParentSegment[],
  insightsProjects: CdpParentInsightsProject[],
): ParentMetadataPlan {
  const plan: ParentMetadataPlan = { segments: [], insightsProjects: [] }
  const parentOfSegment = new Map<string, PccParentMetadata>()

  for (const segment of segments) {
    const parent = parentsByPccProjectId.get(segment.sourceId)
    if (!parent) continue
    parentOfSegment.set(segment.id, parent)

    const update: ParentSegmentUpdate = {
      segmentId: segment.id,
      segmentType: segment.type,
      segmentName: segment.name,
      pccProjectId: parent.pccProjectId,
      status: changeTo(segment.status, parent.status),
      maturity: changeTo(segment.maturity, parent.maturity),
      description: changeTo(segment.description, parent.description),
    }
    if (update.status || update.maturity || update.description) plan.segments.push(update)
  }

  for (const insightsProject of insightsProjects) {
    const parent = parentOfSegment.get(insightsProject.segmentId)
    if (!parent) continue

    const update: ParentInsightsProjectUpdate = {
      insightsProjectId: insightsProject.id,
      segmentId: insightsProject.segmentId,
      pccProjectId: parent.pccProjectId,
      description: changeTo(insightsProject.description, parent.description),
      logoUrl: insightsProject.logoUrl === null ? changeTo(null, parent.logoUrl) : undefined,
    }
    if (update.description || update.logoUrl) plan.insightsProjects.push(update)
  }

  return plan
}

export async function loadParentSegments(
  qx: QueryExecutor,
  pccProjectIds: string[],
): Promise<CdpParentSegment[]> {
  if (pccProjectIds.length === 0) return []
  return qx.select(
    `
    SELECT id, type, name, "sourceId", status::text AS status, maturity, description
    FROM segments
    WHERE "sourceId" = ANY($(pccProjectIds)::text[])
      AND "tenantId" = $(tenantId)
    `,
    { pccProjectIds, tenantId: DEFAULT_TENANT_ID },
  )
}

export async function loadInsightsProjectsOfSegments(
  qx: QueryExecutor,
  segmentIds: string[],
): Promise<CdpParentInsightsProject[]> {
  if (segmentIds.length === 0) return []
  return qx.select(
    `
    SELECT id, "segmentId", description, "logoUrl"
    FROM "insightsProjects"
    WHERE "segmentId" = ANY($(segmentIds)::uuid[])
      AND "deletedAt" IS NULL
    `,
    { segmentIds },
  )
}

export async function applyParentMetadata(
  qx: QueryExecutor,
  plan: ParentMetadataPlan,
): Promise<void> {
  await qx.tx(async (tx) => {
    for (const update of plan.segments) {
      await tx.result(
        `
        UPDATE segments
        SET status      = COALESCE($(status)::"segmentsStatus_type", status),
            maturity    = COALESCE($(maturity), maturity),
            description = COALESCE($(description), description),
            "updatedAt" = NOW()
        WHERE id = $(segmentId)
        `,
        {
          segmentId: update.segmentId,
          status: update.status?.to ?? null,
          maturity: update.maturity?.to ?? null,
          description: update.description?.to ?? null,
        },
      )
    }

    for (const update of plan.insightsProjects) {
      await tx.result(
        `
        UPDATE "insightsProjects"
        SET description = COALESCE($(description), description),
            "logoUrl"   = COALESCE("logoUrl", $(logoUrl)),
            "updatedAt" = NOW()
        WHERE id = $(insightsProjectId)
          AND "deletedAt" IS NULL
        `,
        {
          insightsProjectId: update.insightsProjectId,
          description: update.description?.to ?? null,
          logoUrl: update.logoUrl?.to ?? null,
        },
      )
    }
  })
}
