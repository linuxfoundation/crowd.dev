import { ConnectorError } from '@crowd/connectors'
import { SESSIONIZE_ALL_SESSIONS_PATH } from '@crowd/connectors/src/connectors/sessionize/exportFile'
import type {
  ExportCell,
  ExportSheet,
} from '@crowd/connectors/src/connectors/sessionize/exportFile'

export const EVENT_KEY_PLACEHOLDER = '{eventKey}'

const REQUIRED_COLUMNS = [
  'Session Id',
  'Title',
  'Status',
  'Date Submitted',
  'Speaker Id',
  'FirstName',
  'LastName',
  'Email',
]

export interface ExportProbeSummary {
  sheetNames: string[]
  rowCount: number
  missingRequiredColumns: string[]
  headers: string[]
  statusCounts: Record<string, number>
  dateColumns: Record<string, { cellTypes: Record<string, number>; shapes: Record<string, number> }>
  linkedin: { filled: number; withInPath: number }
  emailFilled: number
}

export interface ExportProbeArgs {
  eventKeys: string[]
  pathTemplate: string
  dateHeaders: string[]
  paceMs: number
}

export interface ExportProbeFailure {
  errorClass: string
  message?: string
}

export function valueShape(value: string): string {
  return value.replace(/\p{Nd}/gu, '9').replace(/\p{L}/gu, 'A')
}

function normalizeHeader(header: string): string {
  return header.trim().toLowerCase()
}

function columnIndex(headers: string[], matches: (normalized: string) => boolean): number {
  return headers.findIndex((header) => matches(normalizeHeader(header)))
}

function columnCells(sheet: ExportSheet, index: number): ExportCell[] {
  return index === -1 ? [] : sheet.rows.map((row) => row[index])
}

function increment(counts: Record<string, number>, key: string): void {
  counts[key] = (counts[key] ?? 0) + 1
}

function cellType(cell: ExportCell): string {
  if (cell === null) {
    return 'empty'
  }
  if (cell instanceof Date) {
    return 'date'
  }
  return typeof cell
}

function isUtcMidnight(date: Date): boolean {
  return date.getTime() % 86_400_000 === 0
}

function summarizeDateColumn(cells: ExportCell[]): ExportProbeSummary['dateColumns'][string] {
  const cellTypes: Record<string, number> = {}
  const shapes: Record<string, number> = {}
  for (const cell of cells) {
    increment(cellTypes, cellType(cell))
    if (cell instanceof Date) {
      increment(shapes, isUtcMidnight(cell) ? 'date:midnight' : 'date:time')
    } else if (cell !== null) {
      increment(shapes, valueShape(String(cell)))
    }
  }
  return { cellTypes, shapes }
}

function hasLinkedinProfilePath(value: string): boolean {
  const withScheme = /^[a-z][a-z0-9+.-]*:\/\//i.test(value) ? value : `https://${value}`
  try {
    return new URL(withScheme).pathname.toLowerCase().startsWith('/in/')
  } catch {
    return false
  }
}

function filled(cells: ExportCell[]): ExportCell[] {
  return cells.filter((cell) => cell !== null)
}

export function summarizeExport(sheet: ExportSheet, dateHeaders: string[]): ExportProbeSummary {
  const normalizedHeaders = new Set(sheet.headers.map(normalizeHeader))
  const statusCounts: Record<string, number> = {}
  const statusIndex = columnIndex(sheet.headers, (header) => header === 'status')
  for (const status of filled(columnCells(sheet, statusIndex))) {
    increment(statusCounts, String(status))
  }

  const dateColumns: ExportProbeSummary['dateColumns'] = {}
  for (const dateHeader of dateHeaders) {
    const index = columnIndex(sheet.headers, (header) => header === normalizeHeader(dateHeader))
    if (index !== -1) {
      dateColumns[dateHeader] = summarizeDateColumn(columnCells(sheet, index))
    }
  }

  const linkedinIndex = columnIndex(sheet.headers, (header) => header.includes('linkedin'))
  const linkedinValues = filled(columnCells(sheet, linkedinIndex)).map(String)
  const emailIndex = columnIndex(sheet.headers, (header) => header === 'email')

  return {
    sheetNames: sheet.sheetNames,
    rowCount: sheet.rows.length,
    missingRequiredColumns: REQUIRED_COLUMNS.filter(
      (column) => !normalizedHeaders.has(normalizeHeader(column)),
    ),
    headers: sheet.headers,
    statusCounts,
    dateColumns,
    linkedin: {
      filled: linkedinValues.length,
      withInPath: linkedinValues.filter(hasLinkedinProfilePath).length,
    },
    emailFilled: filled(columnCells(sheet, emailIndex)).length,
  }
}

export function describeProbeFailure(err: unknown): ExportProbeFailure {
  if (err instanceof ConnectorError) {
    return { errorClass: err.errorClass, message: err.message }
  }
  return { errorClass: err instanceof Error ? err.name : typeof err }
}

function requireValue(args: string[], index: number, flag: string): string {
  const value = args[index + 1]
  if (value === undefined || value.startsWith('--')) {
    throw new Error(`${flag} needs a value`)
  }
  return value
}

export function parseExportProbeArgs(argv: string[]): ExportProbeArgs {
  const eventKeys: string[] = []
  const dateHeaders: string[] = []
  let pathTemplate: string | undefined
  let paceMs: number | undefined

  const args = argv.filter((arg) => arg !== '--')
  for (let i = 0; i < args.length; i += 2) {
    const flag = args[i]
    if (!['--event', '--path', '--date-header', '--pace-ms'].includes(flag)) {
      throw new Error(`unknown argument "${flag}"`)
    }
    const value = requireValue(args, i, flag)
    if (flag === '--event') {
      if (!/^\d+$/.test(value)) {
        throw new Error(`event key "${value}" is not numeric`)
      }
      eventKeys.push(value)
    } else if (flag === '--date-header') {
      dateHeaders.push(value)
    } else if (flag === '--path') {
      if (pathTemplate !== undefined) {
        throw new Error('--path given more than once')
      }
      if (!value.startsWith('/') || !value.includes(EVENT_KEY_PLACEHOLDER)) {
        throw new Error(`--path must start with / and contain ${EVENT_KEY_PLACEHOLDER}`)
      }
      pathTemplate = value
    } else {
      if (paceMs !== undefined) {
        throw new Error('--pace-ms given more than once')
      }
      if (!/^\d+$/.test(value)) {
        throw new Error('--pace-ms must be a non-negative integer')
      }
      paceMs = Number(value)
    }
  }

  return {
    eventKeys,
    pathTemplate: pathTemplate ?? SESSIONIZE_ALL_SESSIONS_PATH(EVENT_KEY_PLACEHOLDER),
    dateHeaders: dateHeaders.length > 0 ? dateHeaders : ['Date Submitted'],
    paceMs: paceMs ?? 5000,
  }
}

export function exportPath(pathTemplate: string, eventKey: string): string {
  return pathTemplate.split(EVENT_KEY_PLACEHOLDER).join(eventKey)
}
