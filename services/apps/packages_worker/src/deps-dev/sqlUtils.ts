export function formatValue(val: unknown): string {
  if (val === null || val === undefined) return 'NULL'
  if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE'
  if (typeof val === 'bigint') return String(val)
  if (typeof val === 'number') return isFinite(val) ? String(val) : 'NULL'
  if (val instanceof Date) return `'${val.toISOString()}'`
  if (Buffer.isBuffer(val)) return `'\\x${val.toString('hex')}'`
  if (Array.isArray(val)) {
    const items = val.map((v) => {
      if (v === null || v === undefined) return 'NULL'
      if (typeof v === 'string') return `"${v.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`
      return String(v)
    })
    return `'{${items.join(',')}}'`
  }
  return `'${String(val).replace(/'/g, "''")}'`
}

export function buildInsert(
  table: string,
  columns: string[],
  rows: Record<string, unknown>[],
): string {
  const cols = columns.map((c) => `"${c}"`).join(', ')
  const values = rows
    .map((row) => `(${columns.map((c) => formatValue(row[c])).join(', ')})`)
    .join(',\n')
  return `INSERT INTO ${table} (${cols}) VALUES\n${values}`
}
