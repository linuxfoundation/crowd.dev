import { QueryExecutor } from '../queryExecutor'

export async function findPackageIdsByPurl(
  qx: QueryExecutor,
  purls: string[],
): Promise<Map<string, number>> {
  if (purls.length === 0) return new Map()
  const rows = await qx.select(`SELECT id, purl FROM packages WHERE purl = ANY($(purls))`, {
    purls,
  })
  return new Map(rows.map((r: { purl: string; id: number }) => [r.purl, r.id]))
}
