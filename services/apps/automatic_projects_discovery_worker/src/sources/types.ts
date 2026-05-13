import { Readable } from 'stream'

export interface IDatasetDescriptor {
  id: string
  date: string
  url: string
}

export interface IDiscoverySource {
  name: string
  /**
   * 'csv' (default): fetchDatasetStream returns a raw text stream, piped through csv-parse.
   * 'json': fetchDatasetStream returns an object-mode Readable that emits pre-parsed records.
   */
  format?: 'csv' | 'json'
  listAvailableDatasets(): Promise<IDatasetDescriptor[]>
  fetchDatasetStream(dataset: IDatasetDescriptor): Promise<Readable>
  parseRow(rawRow: Record<string, unknown>): IDiscoverySourceRow | null
}

export interface IDiscoverySourceRow {
  projectSlug: string
  repoName: string
  repoUrl: string
  ossfCriticalityScore?: number
  lfCriticalityScore?: number
}
