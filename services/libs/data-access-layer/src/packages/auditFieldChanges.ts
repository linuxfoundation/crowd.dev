import { QueryExecutor } from '../queryExecutor'

export async function logAuditFieldChanges(
  qx: QueryExecutor,
  worker: string,
  purl: string,
  changedFields: string[],
): Promise<void> {
  if (changedFields.length === 0) return
  await qx.result(
    `INSERT INTO audit_field_changes (worker, purl, changed_fields)
     VALUES ($(worker), $(purl), $(changedFields)::text[])`,
    { worker, purl, changedFields },
  )
}
