import { TransformerBase } from '../core/transformerBase'

export type BuildSourceQuery = (sinceTimestamp?: string) => string

// Each data source maps to a distinct Snowflake table (or joined set of tables) that is exported and transformed independently.
export enum DataSourceName {
  CVENT_EVENT_REGISTRATIONS = 'event-registrations',
  TNC_ENROLLMENTS = 'enrollments',
  TNC_CERTIFICATES = 'certificates',
  TNC_COURSES = 'courses',
}

export interface DataSource {
  name: DataSourceName
  buildSourceQuery: BuildSourceQuery
  transformer: TransformerBase
}

export interface PlatformDefinition {
  sources: DataSource[]
}
