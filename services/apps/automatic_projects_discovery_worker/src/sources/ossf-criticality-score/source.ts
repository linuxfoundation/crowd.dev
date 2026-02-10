import { Readable } from 'stream'

import { IDatasetDescriptor, IDiscoverySource, IDiscoverySourceRow } from '../types'

import { buildDatasetUrl, getHttpsStream, listDatePrefixes, listTimePrefixes } from './bucketClient'

export class OssfCriticalityScoreSource implements IDiscoverySource {
  public readonly name = 'ossf-criticality-score'

  async listAvailableDatasets(): Promise<IDatasetDescriptor[]> {
    const datePrefixes = await listDatePrefixes()

    const datasets: IDatasetDescriptor[] = []

    for (const datePrefix of datePrefixes) {
      const timePrefixes = await listTimePrefixes(datePrefix)

      for (const timePrefix of timePrefixes) {
        const date = datePrefix.replace(/\/$/, '')
        const url = buildDatasetUrl(timePrefix)

        datasets.push({
          id: timePrefix.replace(/\/$/, ''),
          date,
          url,
        })
      }
    }

    // Sort newest-first by date
    datasets.sort((a, b) => b.date.localeCompare(a.date))

    return datasets
  }

  async fetchDatasetStream(dataset: IDatasetDescriptor): Promise<Readable> {
    const stream = await getHttpsStream(dataset.url)
    return stream as Readable
  }

  // CSV columns use dot notation (e.g. "repo.url", "default_score")
  parseRow(rawRow: Record<string, string>): IDiscoverySourceRow | null {
    const repoUrl = rawRow['repo.url']
    if (!repoUrl) {
      return null
    }

    let repoName = ''
    let projectSlug = ''

    try {
      const urlPath = new URL(repoUrl).pathname.replace(/^\//, '').replace(/\/$/, '')
      projectSlug = urlPath
      repoName = urlPath.split('/').pop() || ''
    } catch {
      const parts = repoUrl.replace(/\/$/, '').split('/')
      projectSlug = parts.slice(-2).join('/')
      repoName = parts.pop() || ''
    }

    if (!projectSlug || !repoName) {
      return null
    }

    const criticalityScoreRaw = rawRow['default_score']
    const criticalityScore = criticalityScoreRaw ? parseFloat(criticalityScoreRaw) : undefined

    return {
      projectSlug,
      repoName,
      repoUrl,
      criticalityScore: Number.isNaN(criticalityScore) ? undefined : criticalityScore,
    }
  }
}
