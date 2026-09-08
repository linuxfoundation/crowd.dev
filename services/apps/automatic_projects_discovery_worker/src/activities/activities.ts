import { Context } from '@temporalio/activity'
import { parse } from 'csv-parse'

import {
  bulkInsertProjectCatalog,
  findExistingProjectCatalogRepoUrls,
} from '@crowd/data-access-layer'
import { IDbProjectCatalogCreate } from '@crowd/data-access-layer/src/project-catalog/types'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'

import { DISCOVERY_NEW_PROJECTS_LIMIT } from '../config'
import { svc } from '../main'
import { getAvailableSourceNames, getSource } from '../sources/registry'
import { IDatasetDescriptor } from '../sources/types'

const log = getServiceLogger()

// Candidates are collected in chunks this size before checking which repoUrls
// already exist in projectCatalog, so we don't do one DB round-trip per row.
const CANDIDATE_CHUNK_SIZE = 500

export async function listSources(): Promise<string[]> {
  return getAvailableSourceNames()
}

export async function listDatasets(sourceName: string): Promise<IDatasetDescriptor[]> {
  const source = getSource(sourceName)

  log.info({ sourceName }, 'Listing datasets.')

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
  log.info({ sourceName, datasetId: dataset.id }, 'Opening dataset stream...')
  const stream = await source.fetchDatasetStream(dataset)
  log.info({ sourceName, datasetId: dataset.id }, 'Dataset stream opened.')

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

  // pipe() does not forward source errors to the destination automatically, so we
  // destroy records explicitly — this surfaces the error in the for-await loop and
  // lets Temporal mark the activity as failed and retry it.
  stream.on('error', (err: Error) => {
    log.error({ datasetId: dataset.id, error: err.message }, 'Stream error.')
    records.destroy(err)
  })

  if (source.format !== 'json') {
    const csvRecords = records as ReturnType<typeof parse>
    csvRecords.on('error', (err) => {
      log.error({ datasetId: dataset.id, error: err.message }, 'CSV parser error.')
    })
  }

  const accepted: IDbProjectCatalogCreate[] = []
  const acceptedRepoUrls = new Set<string>()
  let chunk: IDbProjectCatalogCreate[] = []
  let totalRows = 0
  let totalSkipped = 0

  async function acceptNewRows(candidates: IDbProjectCatalogCreate[]): Promise<void> {
    const seenInChunk = new Set<string>()
    const unseen = candidates.filter(
      (c) =>
        !acceptedRepoUrls.has(c.repoUrl) &&
        !seenInChunk.has(c.repoUrl) &&
        seenInChunk.add(c.repoUrl),
    )
    if (unseen.length === 0) {
      return
    }

    const existingRepoUrls = await findExistingProjectCatalogRepoUrls(
      qx,
      unseen.map((c) => c.repoUrl),
    )

    for (const candidate of unseen) {
      if (accepted.length >= DISCOVERY_NEW_PROJECTS_LIMIT) {
        break
      }
      if (existingRepoUrls.has(candidate.repoUrl)) {
        continue
      }
      accepted.push(candidate)
      acceptedRepoUrls.add(candidate.repoUrl)
    }
  }

  for await (const rawRow of records) {
    totalRows++

    const parsed = source.parseRow(rawRow as Record<string, unknown>)
    if (!parsed) {
      totalSkipped++
      continue
    }

    chunk.push({
      projectSlug: parsed.projectSlug,
      repoName: parsed.repoName,
      repoUrl: parsed.repoUrl,
      source: sourceName,
      action: parsed.action ?? 'auto',
      lfCriticalityScore: parsed.lfCriticalityScore,
    })

    if (chunk.length >= CANDIDATE_CHUNK_SIZE) {
      await acceptNewRows(chunk)
      chunk = []

      Context.current().heartbeat({ totalRows, accepted: accepted.length })

      if (accepted.length >= DISCOVERY_NEW_PROJECTS_LIMIT) {
        log.info(
          { sourceName, datasetId: dataset.id, totalRows, accepted: accepted.length },
          'Discovery limit reached, stopping stream.',
        )
        break
      }
    }
  }

  // Flush a final partial chunk, unless the limit was already hit above.
  if (chunk.length > 0 && accepted.length < DISCOVERY_NEW_PROJECTS_LIMIT) {
    await acceptNewRows(chunk)
  }

  records.destroy()
  if (stream !== records) {
    stream.destroy()
  }

  if (accepted.length > 0) {
    await bulkInsertProjectCatalog(qx, accepted)
  }

  const elapsedSeconds = ((Date.now() - startTime) / 1000).toFixed(1)

  log.info(
    {
      sourceName,
      datasetId: dataset.id,
      totalRows,
      totalSkipped,
      totalAccepted: accepted.length,
      elapsedSeconds,
    },
    'Dataset processing complete.',
  )
}
