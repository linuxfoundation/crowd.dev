/**
 * Export activity: Execute PCC recursive CTE COPY INTO + write metadata.
 *
 * Full daily export of ANALYTICS.SILVER_DIM.PROJECTS via recursive CTE.
 * No incremental logic — at ~1,538 leaf rows, a full daily export is simpler
 * and more reliable than incremental (a parent name change would require
 * re-exporting all descendants).
 */
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/database'
import { getServiceChildLogger } from '@crowd/logging'
import { MetadataStore, SnowflakeExporter } from '@crowd/snowflake'

const log = getServiceChildLogger('exportActivity')

const PLATFORM = 'pcc'
const SOURCE_NAME = 'project-hierarchy'

function buildSourceQuery(): string {
  return `
    WITH RECURSIVE project_hierarchy AS (
      SELECT project_id, name, description, project_logo, project_status,
             project_maturity_level, repository_url, slug, parent_id,
             1 AS depth,
             name AS depth_1, NULL::VARCHAR AS depth_2, NULL::VARCHAR AS depth_3,
             NULL::VARCHAR AS depth_4, NULL::VARCHAR AS depth_5
      FROM ANALYTICS.SILVER_DIM.PROJECTS
      WHERE parent_id IS NULL
      UNION ALL
      SELECT p.project_id, p.name, p.description, p.project_logo, p.project_status,
             p.project_maturity_level, p.repository_url, p.slug, p.parent_id,
             h.depth + 1,
             h.depth_1,
             CASE WHEN h.depth + 1 = 2 THEN p.name ELSE h.depth_2 END,
             CASE WHEN h.depth + 1 = 3 THEN p.name ELSE h.depth_3 END,
             CASE WHEN h.depth + 1 = 4 THEN p.name ELSE h.depth_4 END,
             CASE WHEN h.depth + 1 = 5 THEN p.name ELSE h.depth_5 END
      FROM ANALYTICS.SILVER_DIM.PROJECTS p
      INNER JOIN project_hierarchy h ON p.parent_id = h.project_id
    )
    SELECT ph.project_id, ph.name, ph.slug, ph.description, ph.project_logo, ph.repository_url,
           ph.project_status, ph.project_maturity_level, ph.depth,
           ph.depth_1, ph.depth_2, ph.depth_3, ph.depth_4, ph.depth_5,
           s.segment_id
    FROM project_hierarchy ph
    LEFT JOIN ANALYTICS.SILVER_DIM.ACTIVE_SEGMENTS s
      ON s.source_id = ph.project_id AND s.project_type = 'subproject'
    WHERE ph.project_id NOT IN (
      SELECT DISTINCT parent_id FROM ANALYTICS.SILVER_DIM.PROJECTS
      WHERE parent_id IS NOT NULL
    )
  `
}

function buildS3FilenamePrefix(): string {
  const now = new Date()
  const year = now.getFullYear()
  const month = String(now.getMonth() + 1).padStart(2, '0')
  const day = String(now.getDate()).padStart(2, '0')
  const s3BucketPath = process.env.CROWD_SNOWFLAKE_S3_BUCKET_PATH
  if (!s3BucketPath) {
    throw new Error('Missing required env var CROWD_SNOWFLAKE_S3_BUCKET_PATH')
  }
  return `${s3BucketPath}/${PLATFORM}/${SOURCE_NAME}/${year}/${month}/${day}`
}

export async function executeExport(): Promise<void> {
  log.info({ platform: PLATFORM, sourceName: SOURCE_NAME }, 'Starting PCC export')

  const exporter = new SnowflakeExporter()
  const db = await getDbConnection(WRITE_DB_CONFIG())

  try {
    const metadataStore = new MetadataStore(db)
    const sourceQuery = buildSourceQuery()
    const s3FilenamePrefix = buildS3FilenamePrefix()
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
