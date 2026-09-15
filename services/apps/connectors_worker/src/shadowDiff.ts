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
const MAX_FIELD_VALUE_LENGTH = 500

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

function truncateFieldValue(value: unknown): unknown {
  const serialized = typeof value === 'string' ? value : JSON.stringify(value)
  if (serialized === undefined || serialized.length <= MAX_FIELD_VALUE_LENGTH) {
    return value
  }
  return `${serialized.slice(0, MAX_FIELD_VALUE_LENGTH)}… [truncated]`
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
      mismatches.push({
        field,
        shadowValue: truncateFieldValue(shadowValue),
        nangoValue: truncateFieldValue(nangoValue),
      })
    }
  }
  return mismatches
}

function diffKey(record: IDiffableRecord): string {
  return `${record.type}::${record.sourceId}`
}

export function diffShadowAgainstNango(
  shadowRecords: IDiffableRecord[],
  nangoRecords: IDiffableRecord[],
): IShadowDiffMismatch[] {
  const shadowByKey = new Map(shadowRecords.map((r) => [diffKey(r), r]))
  const nangoByKey = new Map(nangoRecords.map((r) => [diffKey(r), r]))
  const mismatches: IShadowDiffMismatch[] = []

  for (const [key, shadowRecord] of shadowByKey) {
    const nangoRecord = nangoByKey.get(key)
    if (!nangoRecord) {
      mismatches.push({
        sourceId: shadowRecord.sourceId,
        type: shadowRecord.type,
        kind: 'missing_in_nango',
        severity: severityForType(shadowRecord.type),
      })
      continue
    }

    const fields = comparePassThroughFields(shadowRecord.data, nangoRecord.data)
    if (fields.length > 0) {
      mismatches.push({
        sourceId: shadowRecord.sourceId,
        type: shadowRecord.type,
        kind: 'field_mismatch',
        severity: 'high',
        fields,
      })
    }
  }

  for (const [key, nangoRecord] of nangoByKey) {
    if (!shadowByKey.has(key)) {
      mismatches.push({
        sourceId: nangoRecord.sourceId,
        type: nangoRecord.type,
        kind: 'missing_in_shadow',
        severity: severityForType(nangoRecord.type),
      })
    }
  }

  return mismatches
}
