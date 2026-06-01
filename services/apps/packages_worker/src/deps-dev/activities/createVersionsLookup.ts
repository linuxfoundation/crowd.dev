import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'

const log = getServiceChildLogger('createVersionsLookup')

// Builds a persistent UNLOGGED lookup table in the staging schema for the root-version JOIN in
// ingestDependencies. Using a temp table per chunk would rebuild 4GB for every chunk on npm
// (150+ chunks × rebuild cost). An UNLOGGED table in the staging schema survives across
// transactions and connections for the lifetime of the workflow run.
//
// The JOIN key (ecosystem, ns, name, number) does not match the versions partition key (package_id),
// so a direct JOIN fans out to all 32 partitions per row. The lookup table is unpartitioned — with
// ANALYZE the planner sees accurate row counts and chooses a hash join instead.
//
// Index on (ecosystem, ns, name, number) supports index scans for small staging batches
// where a full hash join would be wasteful.
export async function createVersionsLookup(input: {
  ecosystems?: string[]
}): Promise<{ rowCount: number }> {
  const { ecosystems } = input
  const qx = await getPackagesDb()

  // versions.ecosystem is lowercase (BQ SQL uses LOWER(System)) — lowercase the filter values.
  const ecosystemFilter =
    ecosystems && ecosystems.length > 0
      ? `WHERE v.ecosystem = ANY(ARRAY[${ecosystems.map((e) => `'${e.toLowerCase()}'`).join(', ')}])`
      : ''

  log.info({ ecosystems: ecosystems ?? 'all' }, 'Building versions lookup table')

  await qx.result(`DROP TABLE IF EXISTS staging.osspckgs_versions_lookup`)

  await qx.result(`
    CREATE UNLOGGED TABLE staging.osspckgs_versions_lookup AS
    SELECT v.id, v.package_id, v.ecosystem, COALESCE(v.namespace, '') AS ns, v.name, v.number
    FROM versions v
    ${ecosystemFilter}
  `)

  await qx.result(`CREATE INDEX ON staging.osspckgs_versions_lookup (ecosystem, ns, name, number)`)

  await qx.result(`ANALYZE staging.osspckgs_versions_lookup`)

  const row = await qx.selectOne(`SELECT COUNT(*) AS count FROM staging.osspckgs_versions_lookup`)
  const rowCount = parseInt(row.count, 10)

  log.info({ ecosystems: ecosystems ?? 'all', rowCount }, 'Versions lookup table ready')

  return { rowCount }
}
