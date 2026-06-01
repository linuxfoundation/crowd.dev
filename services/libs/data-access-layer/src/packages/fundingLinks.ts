import { QueryExecutor } from '../queryExecutor'

export interface NpmFundingLinkInput {
  type?: string | null
  url: string
}

export async function upsertNpmFundingLinks(
  qx: QueryExecutor,
  packageId: string,
  links: NpmFundingLinkInput[],
): Promise<void> {
  await qx.result(`DELETE FROM package_funding_links WHERE package_id = $(packageId)`, { packageId })
  for (const link of links) {
    await qx.result(
      `INSERT INTO package_funding_links (package_id, type, url)
       VALUES ($(packageId), $(type), $(url))
       ON CONFLICT (package_id, url) DO NOTHING`,
      { packageId, type: link.type ?? null, url: link.url },
    )
  }
}
