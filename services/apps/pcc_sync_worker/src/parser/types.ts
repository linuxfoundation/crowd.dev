/**
 * Types for the PCC project parser.
 *
 * Parquet rows come from Snowflake COPY INTO with HEADER=TRUE,
 * so all column names are uppercase.
 */

/**
 * Raw Parquet row from the PCC PROJECT_SPINE export.
 *
 * One row is emitted per (leaf project, hierarchy level). A leaf project at
 * depth N produces N rows: hierarchy_level=1 is the leaf itself,
 * hierarchy_level=N is the topmost ancestor. All rows for the same leaf share
 * the same PROJECT_ID, NAME, PROJECT_STATUS, etc.
 */
export interface PccParquetRow {
  PROJECT_ID: string
  NAME: string
  DESCRIPTION: string | null
  PROJECT_LOGO: string | null
  PROJECT_STATUS: string | null
  PROJECT_MATURITY_LEVEL: string | null
  /** ID of the ancestor at this hierarchy level (hierarchy_level=1 → leaf itself). */
  MAPPED_PROJECT_ID: string | null
  /** Name of the ancestor at this hierarchy level. */
  MAPPED_PROJECT_NAME: string | null
  /** Slug of the ancestor at this hierarchy level. */
  MAPPED_PROJECT_SLUG: string | null
  /** 1 = leaf, N = topmost ancestor. */
  HIERARCHY_LEVEL: number
  SEGMENT_ID: string | null
}

/**
 * CDP hierarchy target derived from PCC depth levels.
 * Phase 1 only updates existing segments — all three levels refer to
 * existing CDP segment names (group / project / subproject).
 */
export interface CdpHierarchyTarget {
  group: string
  project: string
  subproject: string
}

/** Which depth-mapping rule was applied (effective depth 1–4). */
export type MappingRule = 1 | 2 | 3 | 4

/** Structured result after parsing and transforming a single Parquet row. */
export interface ParsedPccProject {
  pccProjectId: string
  /** Raw PCC slug — used for step-3 segment matching in the consumer. */
  pccSlug: string | null
  name: string
  /** Mapped to CDP segmentsStatus_type enum value, or null if unknown. */
  status: string | null
  maturity: string | null
  description: string | null
  logoUrl: string | null
  /** segment_id from Snowflake ACTIVE_SEGMENTS JOIN — used for step-1 matching. */
  segmentIdFromSnowflake: string | null
  effectiveDepth: number
  mappingRule: MappingRule
  cdpTarget: CdpHierarchyTarget
}

export type ParseResult =
  | { ok: true; project: ParsedPccProject }
  | {
      ok: false
      errorType: 'SCHEMA_MISMATCH'
      details: Record<string, unknown>
      /** Present when the row had a valid PROJECT_ID (depth-range errors). Used for segment lookup. */
      pccProjectId?: string
      /** Present when the row had a valid SLUG (depth-range errors). Used for segment lookup. */
      pccSlug?: string | null
    }
