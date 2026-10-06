import { QueryExecutor } from '../queryExecutor'

export interface IOctolensKeywordMapping {
  id: string
  integrationId: string
  segmentId: string
  keywordId: number
  keyword: string
}

export async function findOctolensKeywordMapping(
  qx: QueryExecutor,
  integrationId: string,
  keywordId: number,
): Promise<IOctolensKeywordMapping | null> {
  return qx.selectOneOrNone(
    `
      SELECT id, "integrationId", "segmentId", "keywordId", keyword
      FROM integration."octolensKeywordMappings"
      WHERE "integrationId" = $(integrationId) AND "keywordId" = $(keywordId)
    `,
    { integrationId, keywordId },
  )
}

export async function listOctolensKeywordMappings(
  qx: QueryExecutor,
  integrationId: string,
): Promise<IOctolensKeywordMapping[]> {
  return qx.select(
    `
      SELECT id, "integrationId", "segmentId", "keywordId", keyword
      FROM integration."octolensKeywordMappings"
      WHERE "integrationId" = $(integrationId)
      ORDER BY keyword
    `,
    { integrationId },
  )
}

export interface IAddOctolensKeywordMapping {
  integrationId: string
  segmentId: string
  keywordId: number
  keyword: string
}

export async function addOctolensKeywordMapping(
  qx: QueryExecutor,
  data: IAddOctolensKeywordMapping,
): Promise<string> {
  const result = await qx.selectOne(
    `
      INSERT INTO integration."octolensKeywordMappings" ("integrationId", "segmentId", "keywordId", keyword)
      VALUES ($(integrationId), $(segmentId), $(keywordId), $(keyword))
      RETURNING id
    `,
    data,
  )

  return result.id
}

export async function removeOctolensKeywordMapping(
  qx: QueryExecutor,
  integrationId: string,
  keywordId: number,
): Promise<void> {
  await qx.result(
    `
      DELETE FROM integration."octolensKeywordMappings"
      WHERE "integrationId" = $(integrationId) AND "keywordId" = $(keywordId)
    `,
    { integrationId, keywordId },
  )
}
