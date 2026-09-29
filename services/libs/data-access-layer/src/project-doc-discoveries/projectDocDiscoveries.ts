import { QueryExecutor } from '../queryExecutor'
import { IDbProjectDocDiscovery, IProjectDocDiscoveryUpsert } from './types'

const DISCOVERY_COLUMNS = [
  'projectId',
  'docsUrl',
  'discoveryMethod',
  'confidence',
  'candidates',
  'discoveredAt',
  'createdAt',
  'updatedAt',
]
  .map((c) => `"${c}"`)
  .join(',\n')

export async function upsertProjectDocDiscovery(
  qx: QueryExecutor,
  data: IProjectDocDiscoveryUpsert,
): Promise<IDbProjectDocDiscovery> {
  return qx.selectOne(
    `
    INSERT INTO "projectDocDiscoveries" (
      "projectId",
      "docsUrl",
      "discoveryMethod",
      "confidence",
      "candidates",
      "discoveredAt",
      "createdAt",
      "updatedAt"
    )
    VALUES (
      $(projectId),
      $(docsUrl),
      $(discoveryMethod),
      $(confidence),
      $(candidates)::jsonb,
      NOW(),
      NOW(),
      NOW()
    )
    ON CONFLICT ("projectId") DO UPDATE SET
      "docsUrl" = EXCLUDED."docsUrl",
      "discoveryMethod" = EXCLUDED."discoveryMethod",
      "confidence" = EXCLUDED."confidence",
      "candidates" = EXCLUDED."candidates",
      "discoveredAt" = NOW(),
      "updatedAt" = NOW()
    RETURNING ${DISCOVERY_COLUMNS}
    `,
    {
      projectId: data.projectId,
      docsUrl: data.docsUrl,
      discoveryMethod: data.discoveryMethod,
      confidence: data.confidence,
      candidates: JSON.stringify(data.candidates),
    },
  )
}

export async function findProjectDocDiscovery(
  qx: QueryExecutor,
  projectId: string,
): Promise<IDbProjectDocDiscovery | null> {
  return qx.selectOneOrNone(
    `
    SELECT ${DISCOVERY_COLUMNS}
    FROM "projectDocDiscoveries"
    WHERE "projectId" = $(projectId)
    `,
    { projectId },
  )
}

// A coarse host prefilter: callers still compare exact URLs.
export async function findSharedDocsUrls(
  qx: QueryExecutor,
  excludeProjectId: string,
  hosts: string[],
): Promise<string[]> {
  if (hosts.length === 0) {
    return []
  }

  const rows: { docsUrl: string }[] = await qx.select(
    `
    SELECT DISTINCT d."docsUrl"
    FROM "projectDocDiscoveries" d
    JOIN "insightsProjects" p ON p."id" = d."projectId" AND p."enabled" AND p."deletedAt" IS NULL
    WHERE d."docsUrl" IS NOT NULL
      AND d."projectId" <> $(excludeProjectId)
      AND d."docsUrl" ILIKE ANY ($(hostPatterns))
    `,
    { excludeProjectId, hostPatterns: hosts.map((host) => `%${host}%`) },
  )
  return rows.map((r) => r.docsUrl)
}
