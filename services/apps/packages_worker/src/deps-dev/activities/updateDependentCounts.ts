import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'
import { bigquery } from '../config'
import { formatValue } from '../sqlUtils'

const log = getServiceChildLogger('updateDependentCounts')

const BATCH_SIZE = 5000

const SQL = `
WITH purl_map AS (
  SELECT System, Name, ANY_VALUE(Purl) AS purl
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
  WHERE System IN ('NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO')
    AND Purl IS NOT NULL
  GROUP BY System, Name
)
SELECT
  pm.purl AS purl,
  COUNT(DISTINCT CONCAT(d.Dependent.System, ':', d.Dependent.Name)) AS dependent_packages_count
FROM \`bigquery-public-data.deps_dev_v1.DependentsLatest\` d
JOIN purl_map pm ON pm.System = d.System AND pm.Name = d.Name
WHERE d.System IN ('NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO')
  AND d.MinimumDepth = 1
  AND d.DependentIsHighestReleaseWithResolution = TRUE
GROUP BY pm.purl
`

interface DependentRow {
  purl: string
  dependent_packages_count: bigint | number
}

export interface UpdateDependentCountsInput {
  runId: string
}

export interface UpdateDependentCountsOutput {
  rowsUpdated: number
}

export async function updateDependentCounts(
  _input: UpdateDependentCountsInput,
): Promise<UpdateDependentCountsOutput> {
  const qx = await getPackagesDb()

  const stream = bigquery.createQueryStream({ query: SQL })

  let batch: DependentRow[] = []
  let totalUpdated = 0

  const flush = async () => {
    if (batch.length === 0) return
    const valuesClause = batch
      .map((r) => `(${formatValue(r.purl)}, ${formatValue(BigInt(r.dependent_packages_count))})`)
      .join(',\n')
    const sql = `
      UPDATE packages SET
        dependent_packages_count = data.count::bigint,
        last_synced_at = NOW()
      FROM (VALUES ${valuesClause}) AS data(purl, count)
      WHERE packages.purl = data.purl
    `
    const updated = await qx.result(sql)
    totalUpdated += updated
    batch = []
  }

  for await (const row of stream) {
    batch.push(row as DependentRow)
    if (batch.length >= BATCH_SIZE) {
      await flush()
    }
  }
  await flush()

  log.info({ totalUpdated }, 'dependent_packages_count update complete')
  return { rowsUpdated: totalUpdated }
}
