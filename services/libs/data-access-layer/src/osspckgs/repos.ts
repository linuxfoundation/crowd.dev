import { QueryExecutor } from '../queryExecutor'

export async function findRepoIdsByUrl(
  qx: QueryExecutor,
  urls: string[],
): Promise<Map<string, number>> {
  if (urls.length === 0) return new Map()
  const rows = await qx.select(`SELECT id, url FROM repos WHERE url = ANY($(urls))`, { urls })
  return new Map(rows.map((r: { url: string; id: number }) => [r.url, r.id]))
}
