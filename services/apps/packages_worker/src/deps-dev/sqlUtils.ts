// Parquetjs represents Parquet LIST fields as {list: [{element: value}, ...]}
// rather than plain arrays. Unwrap to a flat array before formatting.
function unwrapParquetList(val: unknown): unknown {
  if (typeof val === 'object' && val !== null && !Buffer.isBuffer(val) && !Array.isArray(val)) {
    const obj = val as Record<string, unknown>
    if ('list' in obj && Array.isArray(obj.list)) {
      return (obj.list as Array<Record<string, unknown>>)
        .map((item) => item.element)
        .filter((v) => v !== null && v !== undefined)
    }
  }
  return val
}

export function formatValue(val: unknown): string {
  val = unwrapParquetList(val)
  if (val === null || val === undefined) return 'NULL'
  if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE'
  if (typeof val === 'bigint') return String(val)
  if (typeof val === 'number') return isFinite(val) ? String(val) : 'NULL'
  if (val instanceof Date) return `'${val.toISOString()}'`
  if (Buffer.isBuffer(val)) return `'\\x${val.toString('hex')}'`
  if (Array.isArray(val)) {
    const items = val.map((v) => {
      if (v === null || v === undefined) return 'NULL'
      // Recursively unwrap nested parquet list structures before type checks
      v = unwrapParquetList(v)
      if (Array.isArray(v)) v = (v as unknown[]).find((x) => x != null) ?? null
      if (v === null || v === undefined) return 'NULL'
      if (typeof v === 'string') return `"${v.replace(/\0/g, '').replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`
      if (Buffer.isBuffer(v)) return `"${(v as Buffer).toString('utf8').replace(/\0/g, '').replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`
      if (typeof v === 'number' || typeof v === 'bigint') return String(v)
      // Unexpected type — surface it clearly rather than emitting [object Object]
      throw new Error(`Unexpected array element: ${JSON.stringify(v)}`)
    })
    return `'{${items.join(',')}}'`
  }
  // Parquetjs returns {} (plain object, no 'list' key) for null/empty REPEATED fields.
  // unwrapParquetList left it unchanged. Treat as NULL rather than String() → '[object Object]'.
  if (typeof val === 'object') return 'NULL'
  // Strip null bytes — PostgreSQL wire protocol treats \0 as string terminator,
  // causing "invalid message format". NPM registry data frequently contains them.
  return `'${String(val).replace(/\0/g, '').replace(/'/g, "''")}'`
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
