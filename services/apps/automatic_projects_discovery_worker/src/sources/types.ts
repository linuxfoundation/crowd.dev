import { Readable } from 'stream'

export interface IDatasetDescriptor {
  id: string
  date: string
  url: string
}

export interface IDiscoverySource {
  name: string
  listAvailableDatasets(): Promise<IDatasetDescriptor[]>
  fetchDatasetStream(dataset: IDatasetDescriptor): Promise<Readable>
  parseRow(rawRow: Record<string, string>): IDiscoverySourceRow | null
}

export interface IDiscoverySourceRow {
  projectSlug: string
  repoName: string
  repoUrl: string
  criticalityScore?: number
}
