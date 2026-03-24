import { QueryExecutor } from '../queryExecutor'

export interface IApiKey {
  id: string
  name: string
  scopes: string[]
  expiresAt: Date | null
  revokedAt: Date | null
}

export async function findApiKeyByHash(
  qx: QueryExecutor,
  keyHash: string,
): Promise<IApiKey | null> {
  return qx.selectOneOrNone(
    `
      SELECT id, name, scopes, "expiresAt", "revokedAt"
      FROM "apiKeys"
      WHERE "keyHash" = $(keyHash)
    `,
    { keyHash },
  )
}

export async function touchApiKeyLastUsed(qx: QueryExecutor, id: string): Promise<void> {
  await qx.result(
    `
      UPDATE "apiKeys"
      SET "lastUsedAt" = now(), "updatedAt" = now()
      WHERE id = $(id)
    `,
    { id },
  )
}
