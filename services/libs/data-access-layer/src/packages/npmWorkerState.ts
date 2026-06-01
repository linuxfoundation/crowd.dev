import { QueryExecutor } from '../queryExecutor'

export async function getNpmChangesLastSeq(qx: QueryExecutor): Promise<string | null> {
  const row = await qx.selectOneOrNone(`SELECT value FROM npm_worker_state WHERE name = 'changes_last_seq'`)
  return row ? (row.value as string) : null
}

export async function setNpmChangesLastSeq(qx: QueryExecutor, seq: string): Promise<void> {
  await qx.result(
    `INSERT INTO npm_worker_state (name, value, updated_at)
     VALUES ('changes_last_seq', $(seq), NOW())
     ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
    { seq },
  )
}
