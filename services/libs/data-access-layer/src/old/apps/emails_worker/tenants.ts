import { DbStore } from '@crowd/database'

export async function dbWeeklyGetNextEmails(db: DbStore) {
  return db.connection().query(`
    SELECT id as "tenantId", name as "tenantName"
    FROM tenants WHERE "deletedAt" IS NULL;
  `)
}

export async function fetchSegments(db: DbStore, tenantId: string) {
  return db.connection().query(
    `SELECT
      s.*
    FROM segments s
    WHERE s."grandparentSlug" IS NOT NULL
      AND s."parentSlug" IS NOT NULL
      AND s."tenantId" = $1
    ORDER BY s.name;`,
    [tenantId],
  )
}

export async function fetchTenantUsers(db: DbStore, tenantId: string) {
  return db.connection().query(
    `SELECT tu."tenantId", tu."userId", u.email, u."firstName"
     FROM "tenantUsers" tu
     INNER JOIN users u ON tu."userId" = u.id
     WHERE tu."tenantId" = $1
       AND tu."status" = 'active'
       AND tu."deletedAt" IS NULL
       AND u."emailVerified" IS TRUE
       AND u."deletedAt" IS NULL;`,
    [tenantId],
  )
}

export async function fetchActiveIntegrations(db: DbStore, tenantId: string) {
  return db.connection().query(
    `select * from integrations i
    where i."tenantId" = $1
    and i.status = 'done'
    and i."deletedAt" is null
    limit 1;`,
    [tenantId],
  )
}
