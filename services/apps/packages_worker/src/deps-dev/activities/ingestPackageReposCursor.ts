import {
  createIngestJob,
  findPackageIdsByPurl,
  findRepoIdsByUrl,
  markJobStatus,
} from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'
import { bigquery } from '../config'
import { canonicalRepoUrl } from '../canonicalRepoUrl'
import { formatValue } from '../sqlUtils'

const log = getServiceChildLogger('ingestPackageReposCursor')

const BATCH_SIZE = 5_000

// TODO(Step 2a): verify RelationType enum values against BQ console before prod run.
const PACKAGE_REPOS_SQL = `
WITH purl_map AS (
  SELECT System, Name, ANY_VALUE(Purl) AS purl
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
  WHERE System IN ('NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO')
    AND Purl IS NOT NULL
  GROUP BY System, Name
)
SELECT
  LOWER(pvp.System)  AS ecosystem,
  pvp.Name           AS raw_name,
  pm.purl            AS purl,
  pvp.ProjectType,
  pvp.ProjectName,
  pvp.RelationProvenance
FROM \`bigquery-public-data.deps_dev_v1.PackageVersionToProjectLatest\` pvp
JOIN purl_map pm ON pm.System = pvp.System AND pm.Name = pvp.Name
WHERE pvp.System IN ('NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO')
  AND pvp.RelationType = 'SOURCE_REPO'
  AND pvp.ProjectType  IN ('GITHUB', 'GITLAB', 'BITBUCKET')
GROUP BY 1, 2, 3, 4, 5, 6
`

const CONFIDENCE_BY_PROVENANCE: Record<string, number> = {
  SLSA_ATTESTATION: 0.99,
  RUBYGEMS_PUBLISH_ATTESTATION: 0.95,
  PYPI_PUBLISH_ATTESTATION: 0.95,
  GO_ORIGIN: 0.9,
  UNVERIFIED_METADATA: 0.5,
}
const DEFAULT_CONFIDENCE = 0.4

interface BqPkgRepoRow {
  ecosystem: string
  raw_name: string
  purl: string
  ProjectType: string
  ProjectName: string
  RelationProvenance: string
}

async function processBatch(
  qx: Awaited<ReturnType<typeof getPackagesDb>>,
  rows: BqPkgRepoRow[],
): Promise<number> {
  const purls = [...new Set(rows.map((r) => r.purl))]
  const canonicalUrls = rows
    .map((r) => canonicalRepoUrl(r.ProjectType, r.ProjectName))
    .filter((u): u is string => u !== null)
  const uniqueUrls = [...new Set(canonicalUrls)]

  const [packageIds, repoIds] = await Promise.all([
    findPackageIdsByPurl(qx, purls),
    findRepoIdsByUrl(qx, uniqueUrls),
  ])

  const bestConfidence = new Map<string, { packageId: number; repoId: number; confidence: number }>()

  for (const row of rows) {
    const packageId = packageIds.get(row.purl)
    const url = canonicalRepoUrl(row.ProjectType, row.ProjectName)
    if (!packageId || !url) continue
    const repoId = repoIds.get(url)
    if (!repoId) continue

    const key = `${packageId}:${repoId}`
    const confidence = CONFIDENCE_BY_PROVENANCE[row.RelationProvenance] ?? DEFAULT_CONFIDENCE
    const existing = bestConfidence.get(key)
    if (!existing || confidence > existing.confidence) {
      bestConfidence.set(key, { packageId, repoId, confidence })
    }
  }

  const insertRows = [...bestConfidence.values()]

  if (insertRows.length === 0) return 0

  const values = insertRows
    .map(
      (r) =>
        `(${formatValue(r.packageId)}, ${formatValue(r.repoId)}, 'deps_dev', ${formatValue(r.confidence)}, NOW())`,
    )
    .join(',\n')

  await qx.result(`
    INSERT INTO package_repos (package_id, repo_id, source, confidence, verified_at)
    VALUES
    ${values}
    ON CONFLICT (package_id, repo_id) DO UPDATE SET
      confidence  = GREATEST(package_repos.confidence, EXCLUDED.confidence),
      verified_at = NOW()
  `)

  return insertRows.length
}

export interface IngestPackageReposCursorInput {
  runId: string
}

export interface IngestPackageReposCursorOutput {
  rowsInserted: number
}

export async function ingestPackageReposCursor(
  _input: IngestPackageReposCursorInput,
): Promise<IngestPackageReposCursorOutput> {
  const qx = await getPackagesDb()
  // M8: cursor jobs use current date as provisional watermark (not a partition date, but a
  // monotonic "last completed run" timestamp so getLastSuccessfulSnapshot can still return a value).
  const jobId = await createIngestJob(qx, 'package_repos', 'full', new Date())
  await markJobStatus(qx, jobId, 'loading')

  const stream = bigquery.createQueryStream({ query: PACKAGE_REPOS_SQL })

  let batch: BqPkgRepoRow[] = []
  let totalInserted = 0

  for await (const row of stream as AsyncIterable<BqPkgRepoRow>) {
    batch.push(row)
    if (batch.length >= BATCH_SIZE) {
      totalInserted += await processBatch(qx, batch)
      batch = []
      log.info({ jobId, totalInserted }, 'package_repos cursor progress')
    }
  }

  if (batch.length > 0) {
    totalInserted += await processBatch(qx, batch)
  }

  await markJobStatus(qx, jobId, 'done', { finishedAt: new Date(), rowCountPg: totalInserted })
  log.info({ jobId, totalInserted }, 'package_repos cursor complete')

  return { rowsInserted: totalInserted }
}
