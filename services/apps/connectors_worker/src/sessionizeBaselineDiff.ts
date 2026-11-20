import type { SessionizeEventDecision } from '@crowd/connectors/src/connectors/sessionize/matching'

export interface BaselineRow {
  class: string
  sessionizeKey: string
  sessionizeName: string
  cventEventId: string
}

export interface BaselineDifference {
  key: string
  name: string
  baseline: BaselineRow
  decision: SessionizeEventDecision
}

export interface BaselineDiff {
  differences: BaselineDifference[]
  missingNow: string[]
  newNow: string[]
}

const BASELINE_COLUMNS = [
  'class',
  'rule',
  'sessionize_key',
  'sessionize_name',
  'start',
  'cvent_event_id',
  'cvent_name',
  'cvent_date',
  'cdp_project_slug',
  'cdp_source_id',
]

export function parseCsv(text: string): string[][] {
  const rows: string[][] = []
  let row: string[] = []
  let field = ''
  let quoted = false
  for (let i = 0; i < text.length; i++) {
    const char = text[i]
    if (quoted) {
      if (char === '"' && text[i + 1] === '"') {
        field += '"'
        i++
      } else if (char === '"') {
        quoted = false
      } else {
        field += char
      }
    } else if (char === '"') {
      quoted = true
    } else if (char === ',') {
      row.push(field)
      field = ''
    } else if (char === '\n' || char === '\r') {
      if (char === '\r' && text[i + 1] === '\n') i++
      row.push(field)
      field = ''
      rows.push(row)
      row = []
    } else {
      field += char
    }
  }
  if (field !== '' || row.length > 0) {
    row.push(field)
    rows.push(row)
  }
  return rows.filter((r) => r.length > 1 || r[0] !== '')
}

export function parseBaseline(text: string): BaselineRow[] {
  const [header, ...records] = parseCsv(text)
  if (!header || BASELINE_COLUMNS.some((column, index) => header[index] !== column)) {
    throw new Error(`The baseline header must be ${BASELINE_COLUMNS.join(',')}`)
  }
  return records.map((record) => ({
    class: record[0],
    sessionizeKey: record[2],
    sessionizeName: record[3],
    cventEventId: record[5] ?? '',
  }))
}

function differs(baseline: BaselineRow, decision: SessionizeEventDecision): boolean {
  const baselineMatched = baseline.class === 'direct'
  const nowMatched = decision.status === 'matched'
  if (baselineMatched !== nowMatched) return true
  return nowMatched && baseline.cventEventId !== (decision.cventEventId ?? '')
}

export function diffAgainstBaseline(
  decisions: SessionizeEventDecision[],
  baselineRows: BaselineRow[],
): BaselineDiff {
  const baselineByKey = new Map(baselineRows.map((row) => [row.sessionizeKey, row]))
  const decisionKeys = new Set(decisions.map((decision) => decision.event.eventKey))
  const differences: BaselineDifference[] = []
  const newNow: string[] = []
  for (const decision of decisions) {
    const key = decision.event.eventKey
    const baseline = baselineByKey.get(key)
    if (!baseline) newNow.push(key)
    else if (differs(baseline, decision)) {
      differences.push({ key, name: decision.event.eventName, baseline, decision })
    }
  }
  const missingNow = baselineRows
    .map((row) => row.sessionizeKey)
    .filter((k) => !decisionKeys.has(k))
  return { differences, missingNow, newNow }
}

export function formatDifference({ key, name, baseline, decision }: BaselineDifference): string {
  const now = [decision.status, decision.holdReason ?? '-', decision.cventEventId ?? '-'].join('/')
  return `${key} | ${name} | baseline ${baseline.class}/${baseline.cventEventId || '-'} | now ${now}`
}
