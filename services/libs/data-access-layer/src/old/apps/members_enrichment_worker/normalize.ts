import { DEFAULT_TENANT_ID, generateUUIDv4 } from '@crowd/common'
import { DbTransaction } from '@crowd/database'

export async function setMemberAttributeSettings(tx: DbTransaction, options: string[], id: string) {
  return tx.query(
    `UPDATE "memberAttributeSettings"
        SET options = $1
        WHERE id = $2`,
    [options, id],
  )
}

export async function updateMemberAttributeSettings(
  tx: DbTransaction,
  attributeName: string,
  option: string,
) {
  return tx.query(
    `UPDATE "memberAttributeSettings"
          SET options = array_append(options, $1), "updatedAt" = NOW()
          WHERE name = $2
          AND $1 <> ALL(options);`,
    [option, attributeName],
  )
}

export async function insertMemberAttributeSettings(
  tx: DbTransaction,
  attributeName: string,
  label: string,
  attributeType: string,
  show: boolean,
  options: string,
) {
  return tx.query(
    `INSERT INTO "memberAttributeSettings" (id, "tenantId", name, label, type, show, "canDelete", options, "createdAt", "updatedAt")
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
        ON CONFLICT (name,"tenantId") DO UPDATE
        SET options = EXCLUDED.options, "updatedAt" = $9;`,
    [
      generateUUIDv4(),
      DEFAULT_TENANT_ID,
      attributeName,
      label,
      attributeType,
      show,
      false,
      options,
      new Date(Date.now()),
    ],
  )
}
