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

// these fields are read off the parent PR at capture time: shadow snapshots them once per
// activity, Nango re-embeds the PR's current value on every resync, so they drift whenever
// the PR changes afterward — an architecture difference, not a data gap (see #4692/#4693).
const SNAPSHOT_AT_EXTRACTION_ATTRIBUTE_FIELDS = new Set([
  'additions',
  'deletions',
  'changedFiles',
  'state',
  'labels',
  'authorAssociation',
])
const SNAPSHOT_AT_EXTRACTION_TOP_LEVEL_FIELDS = new Set(['title'])
const TYPES_WITH_SNAPSHOT_ATTRIBUTES = new Set([
  'pull_request-opened',
  'pull_request-closed',
  'pull_request-review-requested',
  'pull_request-reviewed',
  'pull_request-assigned',
  'pull_request-merged',
  'pull_request-review-thread-comment',
  'pull_request-comment',
  'issue-comment',
])
const THREAD_RESOLUTION_PREFIX = /^\[Thread (?:RESOLVED|OPEN)\] /
const TYPES_WITH_THREAD_RESOLUTION_PREFIX = new Set(['pull_request-review-thread-comment'])

export type ShadowDiffSeverity = 'high' | 'low'

export type ShadowDiffMismatchKind =
  | 'missing_in_nango'
  | 'missing_in_shadow'
  | 'field_mismatch'
  | 'unsupported_sync'

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
  syncName?: string
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

function withoutSnapshotAttributeFields(attributes: unknown, type: string): unknown {
  if (
    !TYPES_WITH_SNAPSHOT_ATTRIBUTES.has(type) ||
    typeof attributes !== 'object' ||
    attributes === null
  ) {
    return attributes
  }
  return Object.fromEntries(
    Object.entries(attributes as Record<string, unknown>).filter(
      ([key]) => !SNAPSHOT_AT_EXTRACTION_ATTRIBUTE_FIELDS.has(key),
    ),
  )
}

function withoutThreadResolutionPrefix(body: unknown, type: string): unknown {
  if (!TYPES_WITH_THREAD_RESOLUTION_PREFIX.has(type) || typeof body !== 'string') {
    return body
  }
  return body.replace(THREAD_RESOLUTION_PREFIX, '')
}

function comparableFieldValue(
  data: Record<string, unknown>,
  field: (typeof PASS_THROUGH_FIELDS)[number],
  type: string,
): unknown {
  if (field === 'attributes') {
    return withoutSnapshotAttributeFields(data[field], type)
  }
  if (field === 'body') {
    return withoutThreadResolutionPrefix(data[field], type)
  }
  return data[field]
}

function isSnapshotDriftTopLevelField(field: string, type: string): boolean {
  return (
    TYPES_WITH_SNAPSHOT_ATTRIBUTES.has(type) && SNAPSHOT_AT_EXTRACTION_TOP_LEVEL_FIELDS.has(field)
  )
}

function comparePassThroughFields(
  shadowData: Record<string, unknown>,
  nangoData: Record<string, unknown>,
  type: string,
): IFieldMismatch[] {
  const mismatches: IFieldMismatch[] = []
  for (const field of PASS_THROUGH_FIELDS) {
    if (isSnapshotDriftTopLevelField(field, type)) {
      continue
    }
    const shadowValue = comparableFieldValue(shadowData, field, type)
    const nangoValue = comparableFieldValue(nangoData, field, type)
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

    const fields = comparePassThroughFields(shadowRecord.data, nangoRecord.data, shadowRecord.type)
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
