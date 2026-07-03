export function parseCsv(content: string): Record<string, string>[] {
  const rows: string[][] = []
  let cur = ''
  let inQuote = false
  let row: string[] = []

  for (let i = 0; i < content.length; i++) {
    const ch = content[i]
    const next = content[i + 1]

    if (inQuote) {
      if (ch === '"' && next === '"') {
        cur += '"'
        i++
      } else if (ch === '"') {
        inQuote = false
      } else {
        cur += ch
      }
    } else {
      if (ch === '"') {
        inQuote = true
      } else if (ch === ',') {
        row.push(cur)
        cur = ''
      } else if (ch === '\r' && next === '\n') {
        row.push(cur)
        cur = ''
        rows.push(row)
        row = []
        i++
      } else if (ch === '\n') {
        row.push(cur)
        cur = ''
        rows.push(row)
        row = []
      } else {
        cur += ch
      }
    }
  }
  if (cur || row.length) {
    row.push(cur)
    rows.push(row)
  }

  if (rows.length === 0) return []
  const headers = rows[0]
  return rows.slice(1).map((r) => {
    const obj: Record<string, string> = {}
    headers.forEach((h, i) => {
      obj[h.trim()] = (r[i] ?? '').trim()
    })
    return obj
  })
}
