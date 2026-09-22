import { Readable } from 'stream'

import { ProjectCatalogAction } from '@crowd/data-access-layer/src/project-catalog/types'

export interface IDatasetDescriptor {
  id: string
  date: string
  url: string
  // Stamped by listAvailableDatasets so it survives into fetchDatasetStream via
  // workflow history — processDataset receives the descriptor, not the original options.
  since?: string
}

export interface IDiscoverySource {
  name: string
  /**
   * 'csv' (default): fetchDatasetStream returns a raw text stream, piped through csv-parse.
   * 'json': fetchDatasetStream returns an object-mode Readable that emits pre-parsed records.
   */
  format?: 'csv' | 'json'
  listAvailableDatasets(options?: { since?: string }): Promise<IDatasetDescriptor[]>
  fetchDatasetStream(dataset: IDatasetDescriptor): Promise<Readable>
  parseRow(rawRow: Record<string, unknown>): IDiscoverySourceRow | null
}

export interface IDiscoverySourceRow {
  projectSlug: string
  repoName: string
  repoUrl: string
  action?: ProjectCatalogAction
  lfCriticalityScore?: number
}
