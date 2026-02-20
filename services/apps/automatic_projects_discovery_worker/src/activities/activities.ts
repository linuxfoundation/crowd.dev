import { parse } from 'csv-parse'

import { bulkUpsertProjectCatalog } from '@crowd/data-access-layer'
import { IDbProjectCatalogCreate } from '@crowd/data-access-layer/src/project-catalog/types'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'

import { svc } from '../main'
import { getAvailableSourceNames, getSource } from '../sources/registry'
import { IDatasetDescriptor } from '../sources/types'

const log = getServiceLogger()

const BATCH_SIZE = 5000

export async function listSources(): Promise<string[]> {
  return getAvailableSourceNames()
}

export async function listDatasets(sourceName: string): Promise<IDatasetDescriptor[]> {
  const source = getSource(sourceName)
  const datasets = await source.listAvailableDatasets()

  log.info({ sourceName, count: datasets.length, newest: datasets[0]?.id }, 'Datasets listed.')

  return datasets
}

export async function processDataset(
  sourceName: string,
  dataset: IDatasetDescriptor,
): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const startTime = Date.now()

  log.info({ sourceName, datasetId: dataset.id, url: dataset.url }, 'Processing dataset...')

  const source = getSource(sourceName)
  const stream = await source.fetchDatasetStream(dataset)

  stream.on('error', (err: Error) => {
    log.error({ datasetId: dataset.id, error: err.message }, 'Stream error.')
  })

  // For CSV sources: pipe through csv-parse to get Record<string, string> objects.
  // For JSON sources: the stream already emits pre-parsed objects in object mode.
  const records =
    source.format === 'json'
      ? stream
      : stream.pipe(
          parse({
            columns: true,
            skip_empty_lines: true,
            trim: true,
          }),
        )

  if (source.format !== 'json') {
    ;(records as ReturnType<typeof parse>).on('error', (err) => {
      log.error({ datasetId: dataset.id, error: err.message }, 'CSV parser error.')
    })
  }

  let batch: IDbProjectCatalogCreate[] = []
  let totalProcessed = 0
  let totalSkipped = 0
  let batchNumber = 0
  let totalRows = 0

  for await (const rawRow of records) {
    totalRows++

    const parsed = source.parseRow(rawRow as Record<string, unknown>)
    if (!parsed) {
      totalSkipped++
      continue
    }

    batch.push({
      projectSlug: parsed.projectSlug,
      repoName: parsed.repoName,
      repoUrl: parsed.repoUrl,
      ossfCriticalityScore: parsed.ossfCriticalityScore,
      lfCriticalityScore: parsed.lfCriticalityScore,
    })

    if (batch.length >= BATCH_SIZE) {
      batchNumber++

      await bulkUpsertProjectCatalog(qx, batch)
      totalProcessed += batch.length
      batch = []

      log.info({ totalProcessed, batchNumber, datasetId: dataset.id }, 'Batch upserted.')
    }
  }

  // Flush remaining rows that didn't fill a complete batch
  if (batch.length > 0) {
    batchNumber++
    await bulkUpsertProjectCatalog(qx, batch)
    totalProcessed += batch.length
  }

  const elapsedSeconds = ((Date.now() - startTime) / 1000).toFixed(1)

  log.info(
    {
      sourceName,
      datasetId: dataset.id,
      totalRows,
      totalProcessed,
      totalSkipped,
      totalBatches: batchNumber,
      elapsedSeconds,
    },
    'Dataset processing complete.',
  )
}
