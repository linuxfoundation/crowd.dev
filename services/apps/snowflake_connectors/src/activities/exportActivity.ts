/**
 * Export activity: Execute COPY INTO + write metadata.
 *
 * This activity is invoked by the exportWorkflow and performs
 * the actual Snowflake export and metadata bookkeeping.
 */
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/database'
import { getServiceChildLogger } from '@crowd/logging'
import { MetadataStore, SnowflakeExporter, buildS3FilenamePrefix } from '@crowd/snowflake'
import { PlatformType } from '@crowd/types'

import {
  getDataSourceNames as _getDataSourceNames,
  getEnabledPlatforms as _getEnabledPlatforms,
  getDataSource,
} from '../integrations'
import type { DataSourceName } from '../integrations/types'

export async function getEnabledPlatforms(): Promise<PlatformType[]> {
  return _getEnabledPlatforms()
}

export async function getDataSourceNamesForPlatform(platform: PlatformType): Promise<string[]> {
  return _getDataSourceNames(platform)
}

const log = getServiceChildLogger('exportActivity')

export async function executeExport(
  platform: PlatformType,
  sourceName: DataSourceName,
): Promise<void> {
  log.info({ platform, sourceName }, 'Starting export')

  const exporter = new SnowflakeExporter()
  const db = await getDbConnection(WRITE_DB_CONFIG())

  try {
    const metadataStore = new MetadataStore(db)
    const source = getDataSource(platform, sourceName)

    const lastSuccessfulExportTimestamp = await metadataStore.getLatestExportStartedAt(
      platform,
      sourceName,
    )
    const sinceTimestamp = lastSuccessfulExportTimestamp
      ? new Date(lastSuccessfulExportTimestamp).toISOString()
      : undefined
    const sourceQuery = source.buildSourceQuery(sinceTimestamp)
    const s3FilenamePrefix = buildS3FilenamePrefix(platform, sourceName)

    const exportStartedAt = new Date()

    const onBatchComplete = async (s3Path: string, totalRows: number, totalBytes: number) => {
      await metadataStore.insertExportJob(
        platform,
        sourceName,
        s3Path,
        totalRows,
        totalBytes,
        exportStartedAt,
      )
    }

    await exporter.executeBatchedCopyInto(sourceQuery, s3FilenamePrefix, onBatchComplete)

    log.info({ platform, sourceName }, 'Export completed')
  } catch (err) {
    log.error({ platform, sourceName, err }, 'Export failed')
    throw err
  } finally {
    await exporter
      .destroy()
      .catch((err) => log.warn({ err }, 'Failed to close Snowflake connection'))
  }
}
