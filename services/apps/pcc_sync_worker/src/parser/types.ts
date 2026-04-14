/**
 * Types for the PCC project parser.
 *
 * Parquet rows come from Snowflake COPY INTO with HEADER=TRUE,
 * so all column names are uppercase.
 */

/** Raw Parquet row from the PCC recursive CTE export. */
export interface PccParquetRow {
  PROJECT_ID: string
  NAME: string
  SLUG: string | null
  DESCRIPTION: string | null
  PROJECT_LOGO: string | null
  REPOSITORY_URL: string | null
  PROJECT_STATUS: string | null
  PROJECT_MATURITY_LEVEL: string | null
  DEPTH: number
  DEPTH_1: string | null
  DEPTH_2: string | null
  DEPTH_3: string | null
  DEPTH_4: string | null
  DEPTH_5: string | null
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
  repositoryUrl: string | null
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
