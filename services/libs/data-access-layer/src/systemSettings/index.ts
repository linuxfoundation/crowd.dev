import { QueryExecutor } from '../queryExecutor'
import { SystemSettingKey, SystemSettingTypes } from './types'

export async function getSystemSettingValue<K extends SystemSettingKey>(
  qx: QueryExecutor,
  key: K,
): Promise<SystemSettingTypes[K] | null> {
  const result = await qx.selectOneOrNone(
    `SELECT value FROM "systemSettings" WHERE name = $(key)`,
    { key },
  )

  return result ? (result.value as SystemSettingTypes[K]) : null
}

export async function setSystemSettingValue<K extends SystemSettingKey>(
  qx: QueryExecutor,
  key: K,
  value: SystemSettingTypes[K],
): Promise<void> {
  await qx.result(
    `INSERT INTO "systemSettings" (name, value)
     VALUES ($(key), $(value)::jsonb)
     ON CONFLICT (name) DO UPDATE SET value = $(value)::jsonb, "updatedAt" = now()`,
    { key, value },
  )
}
