/**
 * Export activity: Execute PCC COPY INTO + write metadata.
 *
 * Full daily export of leaf projects from ANALYTICS.SILVER_DIM.PROJECTS with their
 * ancestors, one row per (leaf, hierarchy_level) pair up to the PCC root.
 * No incremental logic — at ~1,538 leaf rows, a full daily export is simpler
 * and more reliable than incremental (a parent name change would require
 * re-exporting all descendants).
 */
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/database'
import { getServiceChildLogger } from '@crowd/logging'
import { MetadataStore, SnowflakeExporter, buildS3FilenamePrefix } from '@crowd/snowflake'

const log = getServiceChildLogger('exportActivity')

const PLATFORM = 'pcc'
const SOURCE_NAME = 'project-hierarchy'

const MAX_HIERARCHY_LEVELS = 20

function buildSourceQuery(): string {
  // The level cap ends the walk if PCC ever holds a cycle of parents. A walk that does not
  // reach a root exports null levels, which the parser reports as a schema mismatch.
  return `
    WITH RECURSIVE leaves AS (
      SELECT
        p.project_id,
        p.parent_id,
        p.name,
        p.description,
        p.project_logo,
        p.project_status,
        p.project_admin_category
      FROM ANALYTICS.SILVER_DIM.PROJECTS p
      WHERE p.project_id NOT IN (
        SELECT DISTINCT parent_id
        FROM ANALYTICS.SILVER_DIM.PROJECTS
        WHERE parent_id IS NOT NULL
      )
        AND COALESCE(p.is_internal_project, FALSE) = FALSE
        AND COALESCE(p.category, '') <> 'Internal Allocation'
    ),
    hierarchy (base_project_id, mapped_project_id, parent_id, hierarchy_level) AS (
      SELECT project_id, project_id, parent_id, 1
      FROM leaves
      UNION ALL
      SELECT h.base_project_id, parent.project_id, parent.parent_id, h.hierarchy_level + 1
      FROM hierarchy h
      JOIN ANALYTICS.SILVER_DIM.PROJECTS parent ON parent.project_id = h.parent_id
      WHERE h.hierarchy_level < ${MAX_HIERARCHY_LEVELS}
    ),
    rooted AS (
      SELECT base_project_id
      FROM hierarchy
      WHERE parent_id IS NULL
    )
    SELECT
      p.project_id,
      p.name,
      p.description,
      p.project_logo,
      p.project_status,
      p.project_admin_category,
      h.mapped_project_id,
      ancestor.name AS mapped_project_name,
      ancestor.slug AS mapped_project_slug,
      CASE WHEN r.base_project_id IS NOT NULL THEN h.hierarchy_level END AS hierarchy_level,
      ancestor.project_model AS mapped_project_model,
      ancestor.description AS mapped_project_description,
      ancestor.project_logo AS mapped_project_logo,
      ancestor.project_status AS mapped_project_status,
      ancestor.project_admin_category AS mapped_project_admin_category,
      s.segment_id
    FROM leaves p
    JOIN hierarchy h ON h.base_project_id = p.project_id
    LEFT JOIN rooted r ON r.base_project_id = p.project_id
    JOIN ANALYTICS.SILVER_DIM.PROJECTS ancestor ON ancestor.project_id = h.mapped_project_id
    LEFT JOIN ANALYTICS.SILVER_DIM.ACTIVE_SEGMENTS s
      ON s.source_id = p.project_id
      AND s.project_type = 'subproject'
    ORDER BY p.name, p.project_id, h.hierarchy_level ASC
  `
}

export async function executeExport(): Promise<void> {
  log.info({ platform: PLATFORM, sourceName: SOURCE_NAME }, 'Starting PCC export')

  const exporter = new SnowflakeExporter()
  const db = await getDbConnection(WRITE_DB_CONFIG())

  try {
    const metadataStore = new MetadataStore(db)
    const sourceQuery = buildSourceQuery()
    const s3FilenamePrefix = buildS3FilenamePrefix(PLATFORM, SOURCE_NAME)
    const exportStartedAt = new Date()

    const onBatchComplete = async (s3Path: string, totalRows: number, totalBytes: number) => {
      await metadataStore.insertExportJob(
        PLATFORM,
        SOURCE_NAME,
        s3Path,
        totalRows,
        totalBytes,
        exportStartedAt,
      )
    }

    await exporter.executeBatchedCopyInto(sourceQuery, s3FilenamePrefix, onBatchComplete)

    log.info({ platform: PLATFORM, sourceName: SOURCE_NAME }, 'PCC export completed')
  } catch (err) {
    log.error({ platform: PLATFORM, sourceName: SOURCE_NAME, err }, 'PCC export failed')
    throw err
  } finally {
    await exporter
      .destroy()
      .catch((err) => log.warn({ err }, 'Failed to close Snowflake connection'))
  }
}
