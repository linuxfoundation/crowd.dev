/**
 * Types for the PCC project parser.
 *
 * Parquet rows come from Snowflake COPY INTO with HEADER=TRUE,
 * so all column names are uppercase.
 */

/**
 * Raw Parquet row from the PCC project hierarchy export.
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
  PROJECT_ADMIN_CATEGORY: string | null
  /** ID of the ancestor at this hierarchy level (hierarchy_level=1 → leaf itself). */
  MAPPED_PROJECT_ID: string | null
  /** Name of the ancestor at this hierarchy level. */
  MAPPED_PROJECT_NAME: string | null
  /** Slug of the ancestor at this hierarchy level. */
  MAPPED_PROJECT_SLUG: string | null
  MAPPED_PROJECT_MODEL: string | null
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

export interface PccHierarchyNode {
  pccProjectId: string
  name: string
  slug: string
}

/** `subprojects` goes from the project's own sub-project down to the leaf. */
export interface CdpCreationTarget {
  group: PccHierarchyNode
  project: PccHierarchyNode
  subprojects: PccHierarchyNode[]
}

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
  /** Distinct segment_ids from the Snowflake ACTIVE_SEGMENTS JOIN — step-1 matching needs exactly one. */
  segmentIdsFromSnowflake: string[]
  cdpTarget: CdpHierarchyTarget
  /** Null when a level the creation needs has no PCC id or slug in the export. */
  creationTarget: CdpCreationTarget | null
}

export type ParseResult =
  | { ok: true; project: ParsedPccProject }
  | {
      ok: false
      errorType: 'SCHEMA_MISMATCH'
      details: Record<string, unknown>
      /** Present when the row had a valid PROJECT_ID. Used for segment lookup. */
      pccProjectId?: string
      /** Present when the row had a valid SLUG. Used for segment lookup. */
      pccSlug?: string | null
    }
