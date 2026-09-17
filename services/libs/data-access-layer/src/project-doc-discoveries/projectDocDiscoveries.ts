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
