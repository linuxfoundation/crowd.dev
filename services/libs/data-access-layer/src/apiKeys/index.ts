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

export interface ICreateApiKey {
  name: string
  keyHash: string
  keyPrefix: string
  scopes: string[]
  expiresAt: Date | null
  createdById: string | null
}

export async function createApiKey(qx: QueryExecutor, data: ICreateApiKey): Promise<string> {
  const result = await qx.selectOne(
    `
      INSERT INTO "apiKeys" (name, "keyHash", "keyPrefix", scopes, "expiresAt", "createdById")
      VALUES ($(name), $(keyHash), $(keyPrefix), $(scopes), $(expiresAt), $(createdById))
      RETURNING id
    `,
    data,
  )

  return result.id
}
