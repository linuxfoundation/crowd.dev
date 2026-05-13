/**
 * Export activity: Execute PCC COPY INTO + write metadata.
 *
 * Full daily export of leaf projects from ANALYTICS.SILVER_DIM.PROJECTS joined
 * with PROJECT_SPINE to produce one row per (leaf, hierarchy_level) pair.
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

function buildSourceQuery(): string {
  return `
    SELECT
      p.project_id,
      p.name,
      p.description,
      p.project_logo,
      p.project_status,
      p.project_maturity_level,
      ps.mapped_project_id,
      ps.mapped_project_name,
      ps.mapped_project_slug,
      ps.hierarchy_level,
      s.segment_id
    FROM ANALYTICS.SILVER_DIM.PROJECTS p
    LEFT JOIN ANALYTICS.SILVER_DIM.PROJECT_SPINE ps ON ps.base_project_id = p.project_id
    LEFT JOIN ANALYTICS.SILVER_DIM.ACTIVE_SEGMENTS s
      ON s.source_id = p.project_id
      AND s.project_type = 'subproject'
    WHERE p.project_id NOT IN (
      SELECT DISTINCT parent_id
      FROM ANALYTICS.SILVER_DIM.PROJECTS
      WHERE parent_id IS NOT NULL
    )
    ORDER BY p.name, ps.hierarchy_level ASC
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
