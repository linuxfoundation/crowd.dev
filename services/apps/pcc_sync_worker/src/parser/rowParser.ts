/**
 * PCC project row parser and hierarchy mapper.
 *
 * Transforms a group of raw Parquet rows (one per hierarchy level) for a single
 * PCC leaf project into a structured ParsedPccProject, applying the CDP hierarchy
 * mapping rules.
 *
 * Pure function — no DB access, no I/O.
 *
 * Mapping rules (effective_depth = max HIERARCHY_LEVEL - 1, stripping TLF root):
 *   Rule 1 (eff=1): group=level[1], project=level[1], subproject=level[1]
 *   Rule 2 (eff=2): group=level[2], project=level[1], subproject=level[1]
 *   Rule 3 (eff=3): group=level[3], project=level[2], subproject=level[1]
 *   Rule 4 (eff=4): group=level[3], project=level[2], subproject=level[1]  (level[4] intermediate dropped)
 *
 * Effective depth > 4: SCHEMA_MISMATCH — surfaced to pcc_projects_sync_errors.
 */
import { SegmentStatus } from '@crowd/types'

import type { MappingRule, ParseResult, PccParquetRow } from './types'

/**
 * PCC PROJECT_STATUS → CDP SegmentStatus enum.
 */
const STATUS_MAP: Record<string, SegmentStatus> = {
  Active: SegmentStatus.ACTIVE,
  Archived: SegmentStatus.ARCHIVED,
  'Formation - Disengaged': SegmentStatus.FORMATION,
  'Formation - Engaged': SegmentStatus.FORMATION,
  'Formation - Exploratory': SegmentStatus.FORMATION,
  'Formation - On Hold': SegmentStatus.FORMATION,
  Prospect: SegmentStatus.PROSPECT,
}

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
 * Normalize a string field from PCC: coerce to string, trim surrounding
 * whitespace, and collapse empty results to null. PCC source data sometimes
 * carries accidental leading/trailing whitespace — trimming here prevents
 * spurious hierarchy mismatches, failed STATUS_MAP lookups, and padded names
 * being persisted to segments.
 */
function trimOrNull(value: unknown): string | null {
  if (value == null) return null
  const s = String(value).trim()
  return s === '' ? null : s
}

/**
 * Parse and validate all raw Parquet rows for a single PCC leaf project.
 *
 * Each call receives all rows that share the same PROJECT_ID (one row per
 * hierarchy level from the PROJECT_SPINE JOIN). Returns ok=false with
 * SCHEMA_MISMATCH if the group is malformed or has an unsupported depth (> 4).
 */
export function parsePccRow(rawRows: Record<string, unknown>[]): ParseResult {
  if (rawRows.length === 0) {
    return {
      ok: false,
      errorType: 'SCHEMA_MISMATCH',
      details: { reason: 'empty row group' },
    }
  }

  // All rows share the same leaf-level fields — use the first row for them.
  const firstRaw = rawRows[0] as Partial<PccParquetRow>
  const projectId = trimOrNull(firstRaw.PROJECT_ID)
  const name = trimOrNull(firstRaw.NAME)

  if (!projectId || !name) {
    return {
      ok: false,
      errorType: 'SCHEMA_MISMATCH',
      details: {
        reason: 'missing required fields',
        missingFields: [...(!projectId ? ['PROJECT_ID'] : []), ...(!name ? ['NAME'] : [])],
      },
    }
  }

  // Parse HIERARCHY_LEVEL for each row and sort ascending (level=1 is the leaf).
  // Filter out rows with non-finite levels before sorting to avoid NaN in the comparator,
  // which would violate the sort contract and produce unpredictable ordering.
  const levelRows = rawRows
    .map((r) => {
      const row = r as Partial<PccParquetRow>
      return {
        level: parseParquetInt(row.HIERARCHY_LEVEL),
        name: trimOrNull(row.MAPPED_PROJECT_NAME),
        slug: trimOrNull(row.MAPPED_PROJECT_SLUG),
      }
    })
    .filter((r) => Number.isFinite(r.level) && Number.isInteger(r.level))
    .sort((a, b) => a.level - b.level)

  if (levelRows.length === 0) {
    return {
      ok: false,
      errorType: 'SCHEMA_MISMATCH',
      pccProjectId: String(projectId),
      pccSlug: null,
      details: { reason: 'no rows with valid HIERARCHY_LEVEL', projectId, name },
    }
  }

  const maxLevel = levelRows[levelRows.length - 1].level
  const effectiveDepth = maxLevel - 1
  // Slug of the leaf project itself (hierarchy_level=1 row). Select by level rather
  // than array position — if the level-1 row was filtered out, we want null, not an
  // ancestor's slug.
  const leafSlug = levelRows.find((r) => r.level === 1)?.slug ?? null

  if (
    !Number.isFinite(effectiveDepth) ||
    !Number.isInteger(effectiveDepth) ||
    effectiveDepth < 1 ||
    effectiveDepth > 4
  ) {
    const depthReason =
      !Number.isFinite(effectiveDepth) || !Number.isInteger(effectiveDepth)
        ? 'invalid hierarchy level (non-finite or fractional depth)'
        : effectiveDepth < 1
          ? 'unexpected root node (maxHierarchyLevel≤1)'
          : 'unsupported depth > 4'
    return {
      ok: false,
      errorType: 'SCHEMA_MISMATCH',
      pccProjectId: String(projectId),
      pccSlug: leafSlug,
      details: {
        reason: depthReason,
        maxLevel,
        effectiveDepth,
        projectId,
        name,
      },
    }
  }

  // Build hierarchy_level → MAPPED_PROJECT_NAME lookup.
  const nameAt: Record<number, string | null> = {}
  for (const row of levelRows) {
    nameAt[row.level] = row.name
  }

  const cdpTargetResult = buildCdpTarget(effectiveDepth as MappingRule, nameAt)
  if (cdpTargetResult.ok === false) {
    return {
      ok: false,
      errorType: 'SCHEMA_MISMATCH',
      pccProjectId: String(projectId),
      pccSlug: leafSlug,
      details: { reason: cdpTargetResult.reason, maxLevel, effectiveDepth, projectId, name },
    }
  }

  const rawStatus = trimOrNull(firstRaw.PROJECT_STATUS)
  const mappedStatus = rawStatus ? (STATUS_MAP[rawStatus] ?? null) : null

  return {
    ok: true,
    project: {
      pccProjectId: projectId,
      pccSlug: leafSlug,
      name,
      status: mappedStatus,
      maturity: trimOrNull(firstRaw.PROJECT_MATURITY_LEVEL),
      description: trimOrNull(firstRaw.DESCRIPTION),
      logoUrl: trimOrNull(firstRaw.PROJECT_LOGO),
      segmentIdFromSnowflake: trimOrNull(firstRaw.SEGMENT_ID),
      effectiveDepth,
      mappingRule: effectiveDepth as MappingRule,
      cdpTarget: cdpTargetResult.target,
    },
  }
}

function buildCdpTarget(
  effectiveDepth: MappingRule,
  nameAt: Record<number, string | null>,
):
  | { ok: true; target: { group: string; project: string; subproject: string } }
  | { ok: false; reason: string } {
  switch (effectiveDepth) {
    case 1: {
      // TLF at level 2 (stripped), leaf at level 1 → all three CDP levels share the leaf.
      const n1 = nameAt[1]
      if (!n1) return { ok: false, reason: 'missing MAPPED_PROJECT_NAME at hierarchy_level=1' }
      return { ok: true, target: { group: n1, project: n1, subproject: n1 } }
    }
    case 2: {
      // TLF at level 3, group at level 2, leaf at level 1.
      const n2 = nameAt[2]
      const n1 = nameAt[1]
      if (!n2) return { ok: false, reason: 'missing MAPPED_PROJECT_NAME at hierarchy_level=2' }
      if (!n1) return { ok: false, reason: 'missing MAPPED_PROJECT_NAME at hierarchy_level=1' }
      return { ok: true, target: { group: n2, project: n1, subproject: n1 } }
    }
    case 3: {
      // TLF at level 4, group at level 3, project at level 2, leaf at level 1.
      const n3 = nameAt[3]
      const n2 = nameAt[2]
      const n1 = nameAt[1]
      if (!n3) return { ok: false, reason: 'missing MAPPED_PROJECT_NAME at hierarchy_level=3' }
      if (!n2) return { ok: false, reason: 'missing MAPPED_PROJECT_NAME at hierarchy_level=2' }
      if (!n1) return { ok: false, reason: 'missing MAPPED_PROJECT_NAME at hierarchy_level=1' }
      return { ok: true, target: { group: n3, project: n2, subproject: n1 } }
    }
    case 4: {
      // TLF at level 5, intermediate at level 4 (dropped), group at level 3, project at level 2, leaf at level 1.
      const n3 = nameAt[3]
      const n2 = nameAt[2]
      const n1 = nameAt[1]
      if (!n3) return { ok: false, reason: 'missing MAPPED_PROJECT_NAME at hierarchy_level=3' }
      if (!n2) return { ok: false, reason: 'missing MAPPED_PROJECT_NAME at hierarchy_level=2' }
      if (!n1) return { ok: false, reason: 'missing MAPPED_PROJECT_NAME at hierarchy_level=1' }
      return { ok: true, target: { group: n3, project: n2, subproject: n1 } }
    }
  }
}
