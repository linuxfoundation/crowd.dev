import { BigQuery } from '@google-cloud/bigquery'
import { Storage } from '@google-cloud/storage'

function requireEnv(name: string): string {
  const val = process.env[name]
  if (!val) throw new Error(`Missing required environment variable: ${name}`)
  return val
}

export const GCP_PROJECT = requireEnv('OSSPCKGS_GCP_PROJECT')
export const GCS_BUCKET = requireEnv('OSSPCKGS_GCS_BUCKET')
export const DEPS_DEV_DATASET = 'bigquery-public-data.deps_dev_v1'

// ADR-0003: Option A = DependencyGraphEdgesLatest (prod default, has version_constraint).
// Set OSSPCKGS_DEPS_TABLE=B locally to use DependenciesLatest (cheaper, no version_constraint).
export const DEPS_TABLE_OPTION: 'A' | 'B' = process.env.OSSPCKGS_DEPS_TABLE === 'B' ? 'B' : 'A'

const credentials = JSON.parse(
  Buffer.from(requireEnv('OSSPCKGS_GCP_CREDENTIALS_B64'), 'base64').toString('utf8'),
)

export const bigquery = new BigQuery({ projectId: GCP_PROJECT, credentials })
export const storage = new Storage({ projectId: GCP_PROJECT, credentials })
export const bucket = storage.bucket(GCS_BUCKET)
