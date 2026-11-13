/**
 * PCC project row parser and hierarchy mapper.
 *
 * Transforms a group of raw Parquet rows (one per hierarchy level) for a single
 * PCC leaf project into a structured ParsedPccProject, applying the CDP hierarchy
 * mapping rules.
 *
 * Pure function — no DB access, no I/O.
 *
 * Mapping rules (HIERARCHY_LEVEL 1 is the leaf, the highest level is the top ancestor):
 *   group      = top ancestor (The Linux Foundation for Community Projects)
 *   project    = level just below the top ancestor, or the top ancestor when it is the leaf
 *   subproject = leaf
 */
import { SegmentStatus } from '@crowd/types'

import type { CdpHierarchyTarget, ParseResult, PccParquetRow } from './types'

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
 * SCHEMA_MISMATCH if the group is malformed or has no usable hierarchy rows.
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
  // A leaf missing from PROJECT_SPINE comes through the LEFT JOIN with a null level, which
  // parses to 0, so levels below 1 are dropped together with non-integer ones.
  const levelRows = rawRows
    .map((r) => {
      const row = r as Partial<PccParquetRow>
      return {
        level: parseParquetInt(row.HIERARCHY_LEVEL),
        name: trimOrNull(row.MAPPED_PROJECT_NAME),
        slug: trimOrNull(row.MAPPED_PROJECT_SLUG),
      }
    })
    .filter((r) => Number.isSafeInteger(r.level) && r.level >= 1)
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
  // Slug of the leaf project itself (hierarchy_level=1 row). Select by level rather
  // than array position — if the level-1 row was filtered out, we want null, not an
  // ancestor's slug.
  const leafSlug = levelRows.find((r) => r.level === 1)?.slug ?? null

  const nameAt: Record<number, string | null> = {}
  for (const row of levelRows) {
    nameAt[row.level] = row.name
  }

  const cdpTargetResult = buildCdpTarget(maxLevel, nameAt)
  if (cdpTargetResult.ok === false) {
    return {
      ok: false,
      errorType: 'SCHEMA_MISMATCH',
      pccProjectId: String(projectId),
      pccSlug: leafSlug,
      details: { reason: cdpTargetResult.reason, maxLevel, projectId, name },
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
      maturity: trimOrNull(firstRaw.PROJECT_ADMIN_CATEGORY),
      description: trimOrNull(firstRaw.DESCRIPTION),
      logoUrl: trimOrNull(firstRaw.PROJECT_LOGO),
      segmentIdFromSnowflake: trimOrNull(firstRaw.SEGMENT_ID),
      cdpTarget: cdpTargetResult.target,
    },
  }
}

function buildCdpTarget(
  maxLevel: number,
  nameAt: Record<number, string | null>,
): { ok: true; target: CdpHierarchyTarget } | { ok: false; reason: string } {
  const projectLevel = Math.max(maxLevel - 1, 1)
  const group = nameAt[maxLevel]
  const project = nameAt[projectLevel]
  const subproject = nameAt[1]

  if (!group) return missingNameAt(maxLevel)
  if (!project) return missingNameAt(projectLevel)
  if (!subproject) return missingNameAt(1)

  return { ok: true, target: { group, project, subproject } }
}

function missingNameAt(level: number): { ok: false; reason: string } {
  return { ok: false, reason: `missing MAPPED_PROJECT_NAME at hierarchy_level=${level}` }
}
