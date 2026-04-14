/**
 * PCC project row parser and hierarchy mapper.
 *
 * Transforms a raw Parquet row from the PCC Snowflake export into a
 * structured ParsedPccProject, applying the CDP hierarchy mapping rules.
 *
 * Pure function — no DB access, no I/O. Fully unit-testable against
 * CDP__PCC Integration - proposal-for-migration.csv (project root).
 *
 * Mapping rules (effective_depth = raw DEPTH - 1, stripping TLF root):
 *   Rule 1 (eff=1): group=D2, project=D2, subproject=D2
 *   Rule 2 (eff=2): group=D2, project=D3, subproject=D3
 *   Rule 3 (eff=3): group=D2, project=D3, subproject=D4
 *   Rule 4 (eff=4): group=D3, project=D4, subproject=D5  (drops D2 intermediate)
 *
 * Depth > 4 (raw > 5): SCHEMA_MISMATCH — surfaced to pcc_projects_sync_errors.
 */
import type { MappingRule, ParseResult, PccParquetRow } from './types'

/**
 * PCC PROJECT_STATUS → CDP segmentsStatus_type enum.
 * CDP enum values: active | archived | formation | prospect
 */
const STATUS_MAP: Record<string, string> = {
  Active: 'active',
  Archived: 'archived',
  'Formation - Disengaged': 'formation',
  'Formation - Engaged': 'formation',
  'Formation - Exploratory': 'formation',
  'Formation - On Hold': 'formation',
  Prospect: 'prospect',
}

/**
 * Intermediate PCC nodes that are transparent in the CDP hierarchy.
 * When D2 equals one of these, it is skipped and D1 ("The Linux Foundation")
 * is used as the CDP project group instead.
 */
const TRANSPARENT_INTERMEDIATES = new Set(['LF Projects, LLC'])

/**
 * Parquet serializes Snowflake NUMBER columns as fixed-width big-endian Buffers.
 * Handle both the plain number case and the Buffer case.
 */
function parseParquetInt(value: unknown): number {
  if (typeof value === 'number') return value
  // Parquet serializes Snowflake NUMBER as a Node.js Buffer (big-endian bytes)
  if (Buffer.isBuffer(value)) {
    let result = 0
    for (const byte of value) {
      result = result * 256 + byte
    }
    return result
  }
  return Number(value)
}

/**
 * Parse and validate a raw Parquet row from the PCC export.
 * Returns ok=false with SCHEMA_MISMATCH if the row is malformed or
 * has an unsupported depth (> 5 raw / > 4 effective).
 */
export function parsePccRow(raw: Record<string, unknown>): ParseResult {
  const row = raw as Partial<PccParquetRow>

  const projectId = row.PROJECT_ID
  const name = row.NAME
  const depth = parseParquetInt(row.DEPTH)

  if (!projectId || !name || !Number.isFinite(depth)) {
    return {
      ok: false,
      errorType: 'SCHEMA_MISMATCH',
      details: {
        reason: 'missing required fields',
        missingFields: [
          ...(!projectId ? ['PROJECT_ID'] : []),
          ...(!name ? ['NAME'] : []),
          ...(!Number.isFinite(depth) ? ['DEPTH'] : []),
        ],
      },
    }
  }

  const effectiveDepth = depth - 1

  if (effectiveDepth < 1 || effectiveDepth > 4) {
    return {
      ok: false,
      errorType: 'SCHEMA_MISMATCH',
      pccProjectId: String(projectId),
      pccSlug: (row.SLUG ?? null) as string | null,
      details: {
        reason: effectiveDepth < 1 ? 'unexpected root node (depth=1)' : 'unsupported depth > 5',
        rawDepth: depth,
        effectiveDepth,
        projectId,
        name,
      },
    }
  }

  const d1 = row.DEPTH_1 ?? null
  const d2 = row.DEPTH_2 ?? null
  const d3 = row.DEPTH_3 ?? null
  const d4 = row.DEPTH_4 ?? null
  const d5 = row.DEPTH_5 ?? null

  const cdpTargetResult = buildCdpTarget(effectiveDepth as 1 | 2 | 3 | 4, name, d1, d2, d3, d4, d5)
  if (cdpTargetResult.ok === false) {
    return {
      ok: false,
      errorType: 'SCHEMA_MISMATCH',
      pccProjectId: String(projectId),
      pccSlug: (row.SLUG ?? null) as string | null,
      details: { reason: cdpTargetResult.reason, rawDepth: depth, effectiveDepth, projectId, name },
    }
  }

  const rawStatus = row.PROJECT_STATUS ?? null
  const mappedStatus = rawStatus ? (STATUS_MAP[rawStatus] ?? null) : null

  return {
    ok: true,
    project: {
      pccProjectId: projectId,
      pccSlug: row.SLUG ?? null,
      name,
      status: mappedStatus,
      maturity: row.PROJECT_MATURITY_LEVEL ?? null,
      description: row.DESCRIPTION ?? null,
      logoUrl: row.PROJECT_LOGO ?? null,
      repositoryUrl: row.REPOSITORY_URL ?? null,
      segmentIdFromSnowflake: row.SEGMENT_ID ?? null,
      effectiveDepth,
      mappingRule: effectiveDepth as MappingRule,
      cdpTarget: cdpTargetResult.target,
    },
  }
}

function buildCdpTarget(
  effectiveDepth: 1 | 2 | 3 | 4,
  _leafName: string,
  d1: string | null,
  d2: string | null,
  d3: string | null,
  d4: string | null,
  d5: string | null,
):
  | { ok: true; target: { group: string; project: string; subproject: string } }
  | { ok: false; reason: string } {
  // When D2 is a transparent intermediate (e.g. "LF Projects, LLC"), skip it
  // and promote D1 ("The Linux Foundation") to be the CDP project group.
  const d2IsTransparent = !!d2 && TRANSPARENT_INTERMEDIATES.has(d2)
  const group2 = d2IsTransparent ? d1 : d2

  switch (effectiveDepth) {
    case 1:
      // D1=TLF (stripped), leaf=D2 → all three CDP levels are the same node.
      // Apply transparency: if D2 is a transparent intermediate, promote D1 as the group.
      if (!group2) return { ok: false, reason: 'missing DEPTH_2 for effective_depth=1' }
      return { ok: true, target: { group: group2, project: group2, subproject: group2 } }

    case 2:
      // D1=TLF, D2=group (or transparent→D1), leaf=D3
      if (!group2) return { ok: false, reason: 'missing DEPTH_2 for effective_depth=2' }
      if (!d3) return { ok: false, reason: 'missing DEPTH_3 for effective_depth=2' }
      return { ok: true, target: { group: group2, project: d3, subproject: d3 } }

    case 3:
      // D1=TLF, D2=group (or transparent→D1), D3=project, leaf=D4
      if (!group2) return { ok: false, reason: 'missing DEPTH_2 for effective_depth=3' }
      if (!d3) return { ok: false, reason: 'missing DEPTH_3 for effective_depth=3' }
      if (!d4) return { ok: false, reason: 'missing DEPTH_4 for effective_depth=3' }
      return { ok: true, target: { group: group2, project: d3, subproject: d4 } }

    case 4:
      // D1=TLF, D2=intermediate (always dropped at this depth), D3=group, D4=project, leaf=D5
      if (!d3) return { ok: false, reason: 'missing DEPTH_3 for effective_depth=4' }
      if (!d4) return { ok: false, reason: 'missing DEPTH_4 for effective_depth=4' }
      if (!d5) return { ok: false, reason: 'missing DEPTH_5 for effective_depth=4' }
      return { ok: true, target: { group: d3, project: d4, subproject: d5 } }
  }
}
