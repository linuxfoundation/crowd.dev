import { isDeepStrictEqual } from 'node:util'

export const PASS_THROUGH_FIELDS = [
  'type',
  'sourceId',
  'sourceParentId',
  'score',
  'title',
  'body',
  'url',
  'attributes',
  'timestamp',
] as const

const LOW_SEVERITY_TYPES = new Set(['pull_request-review-requested'])

export type ShadowDiffSeverity = 'high' | 'low'

export type ShadowDiffMismatchKind = 'missing_in_nango' | 'missing_in_shadow' | 'field_mismatch'

export interface IDiffableRecord {
  sourceId: string
  type: string
  data: Record<string, unknown>
}

export interface IFieldMismatch {
  field: (typeof PASS_THROUGH_FIELDS)[number]
  shadowValue: unknown
  nangoValue: unknown
}

export interface IShadowDiffMismatch {
  sourceId: string
  type: string
  kind: ShadowDiffMismatchKind
  severity: ShadowDiffSeverity
  fields?: IFieldMismatch[]
}

function severityForType(type: string): ShadowDiffSeverity {
  return LOW_SEVERITY_TYPES.has(type) ? 'low' : 'high'
}

function comparePassThroughFields(
  shadowData: Record<string, unknown>,
  nangoData: Record<string, unknown>,
): IFieldMismatch[] {
  const mismatches: IFieldMismatch[] = []
  for (const field of PASS_THROUGH_FIELDS) {
    const shadowValue = shadowData[field]
    const nangoValue = nangoData[field]
    if (!isDeepStrictEqual(shadowValue, nangoValue)) {
      mismatches.push({ field, shadowValue, nangoValue })
    }
  }
  return mismatches
}

export function diffShadowAgainstNango(
  shadowRecords: IDiffableRecord[],
  nangoRecords: IDiffableRecord[],
): IShadowDiffMismatch[] {
  const shadowBySourceId = new Map(shadowRecords.map((r) => [r.sourceId, r]))
  const nangoBySourceId = new Map(nangoRecords.map((r) => [r.sourceId, r]))
  const mismatches: IShadowDiffMismatch[] = []

  for (const [sourceId, shadowRecord] of shadowBySourceId) {
    const nangoRecord = nangoBySourceId.get(sourceId)
    if (!nangoRecord) {
      mismatches.push({
        sourceId,
        type: shadowRecord.type,
        kind: 'missing_in_nango',
        severity: severityForType(shadowRecord.type),
      })
      continue
    }

    const fields = comparePassThroughFields(shadowRecord.data, nangoRecord.data)
    if (fields.length > 0) {
      mismatches.push({
        sourceId,
        type: shadowRecord.type,
        kind: 'field_mismatch',
        severity: 'high',
        fields,
      })
    }
  }

  for (const [sourceId, nangoRecord] of nangoBySourceId) {
    if (!shadowBySourceId.has(sourceId)) {
      mismatches.push({
        sourceId,
        type: nangoRecord.type,
        kind: 'missing_in_shadow',
        severity: severityForType(nangoRecord.type),
      })
    }
  }

  return mismatches
}
