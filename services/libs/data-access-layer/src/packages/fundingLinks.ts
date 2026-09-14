import { QueryExecutor } from '../queryExecutor'

export interface NpmFundingLinkInput {
  type?: string | null
  url: string
}

export async function upsertNpmFundingLinks(
  qx: QueryExecutor,
  packageId: string,
  links: NpmFundingLinkInput[],
): Promise<string[]> {
  const before: Array<{ url: string; type: string | null }> = await qx.select(
    `SELECT url, type FROM package_funding_links WHERE package_id = $(packageId)::bigint`,
    { packageId },
  )
  const beforeMap = new Map(before.map((r) => [r.url, r.type]))

  await qx.result(`DELETE FROM package_funding_links WHERE package_id = $(packageId)::bigint`, {
    packageId,
  })

  const afterMap = new Map<string, string | null>()
  for (const link of links) {
    const type = link.type ?? null
    await qx.result(
      `INSERT INTO package_funding_links (package_id, type, url, created_at, updated_at)
       VALUES ($(packageId)::bigint, $(type), $(url), NOW(), NOW())
       ON CONFLICT (package_id, url) DO NOTHING`,
      { packageId, type, url: link.url },
    )
    if (!afterMap.has(link.url)) afterMap.set(link.url, type)
  }

  const changed = new Set<string>()
  for (const url of beforeMap.keys()) {
    if (!afterMap.has(url)) changed.add('package_funding_links.url')
  }
  for (const [url, type] of afterMap) {
    if (!beforeMap.has(url)) changed.add('package_funding_links.url')
    else if (beforeMap.get(url) !== type) changed.add('package_funding_links.type')
  }
  return Array.from(changed)
}
