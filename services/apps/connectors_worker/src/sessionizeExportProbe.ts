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
  headersTrusted: boolean
  headers: string[]
  statusCounts: Record<string, number>
  dateColumns: Record<string, { cellTypes: Record<string, number>; shapes: Record<string, number> }>
  linkedin: { filled: number; withInPath: number }
  emailFilled: number
}

export interface ExportDownloadArgs {
  mode: 'download'
  outDir: string
  eventKeys: string[]
  pathTemplate: string
  paceMs: number
}

export interface ExportSummaryArgs {
  mode: 'summarize'
  fromDir: string
  dateHeaders: string[]
}

export type ExportProbeArgs = ExportDownloadArgs | ExportSummaryArgs

export interface ExportProbeFailure {
  errorClass: string
  message?: string
}

const SHAPE_PUNCTUATION = new Set(`-_.:/@+,()#&'" `)

export function valueShape(value: string): string {
  return value
    .replace(/\p{L}\p{M}*|\p{M}+/gu, 'A')
    .replace(/\p{N}/gu, '9')
    .replace(/[^A9\s]/gu, (char) => (SHAPE_PUNCTUATION.has(char) ? char : '?'))
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
  const missingRequiredColumns = REQUIRED_COLUMNS.filter(
    (column) => !normalizedHeaders.has(normalizeHeader(column)),
  )
  const headersTrusted = missingRequiredColumns.length === 0
  const statusCounts: Record<string, number> = {}
  const statusIndex = columnIndex(sheet.headers, (header) => header === 'status')
  if (headersTrusted) {
    for (const status of filled(columnCells(sheet, statusIndex))) {
      increment(statusCounts, String(status))
    }
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
    missingRequiredColumns,
    headersTrusted,
    headers: headersTrusted ? sheet.headers : sheet.headers.map(valueShape),
    statusCounts,
    dateColumns,
    linkedin: {
      filled: linkedinValues.length,
      withInPath: linkedinValues.filter(hasLinkedinProfilePath).length,
    },
    emailFilled: filled(columnCells(sheet, emailIndex)).length,
  }
}

const RATE_LIMIT_ERROR_CLASS = 'provider.rate_limit'
const MAX_CONSECUTIVE_BY_ERROR_CLASS: Record<string, number> = {
  'provider.auth': 5,
  'provider.unavailable': 2,
}

export interface ProbeRunState {
  failureClass: string | undefined
  consecutive: number
  stop: boolean
}

export const PROBE_RUN_START: ProbeRunState = {
  failureClass: undefined,
  consecutive: 0,
  stop: false,
}

export function nextProbeRunState(
  state: ProbeRunState,
  failureClass: string | undefined,
): ProbeRunState {
  const consecutive = failureClass === state.failureClass ? state.consecutive + 1 : 1
  const limit =
    failureClass === undefined ? undefined : MAX_CONSECUTIVE_BY_ERROR_CLASS[failureClass]
  return {
    failureClass,
    consecutive,
    stop: failureClass === RATE_LIMIT_ERROR_CLASS || (limit !== undefined && consecutive >= limit),
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

const PROBE_FLAGS = ['--out-dir', '--from-dir', '--event', '--path', '--date-header', '--pace-ms']
const REPEATABLE_FLAGS = new Set(['--event', '--date-header'])
const DOWNLOAD_ONLY_FLAGS = ['--event', '--path', '--pace-ms']
const SUMMARY_ONLY_FLAGS = ['--date-header']
const EXACTLY_ONE_MODE = 'give exactly one of --out-dir and --from-dir'

function collectFlagValues(argv: string[]): Map<string, string[]> {
  const values = new Map<string, string[]>()
  const args = argv.filter((arg) => arg !== '--')
  for (let i = 0; i < args.length; i += 2) {
    const flag = args[i]
    if (!PROBE_FLAGS.includes(flag)) {
      throw new Error(`unknown argument "${flag}"`)
    }
    const value = requireValue(args, i, flag)
    const existing = values.get(flag) ?? []
    if (existing.length > 0 && !REPEATABLE_FLAGS.has(flag)) {
      throw new Error(`${flag} given more than once`)
    }
    values.set(flag, [...existing, value])
  }
  return values
}

function rejectFlags(values: Map<string, string[]>, flags: string[], validMode: string): void {
  const misplaced = flags.find((flag) => values.has(flag))
  if (misplaced) {
    throw new Error(`${misplaced} is only valid with ${validMode}`)
  }
}

function parsePathTemplate(value: string | undefined): string {
  if (value === undefined) {
    return SESSIONIZE_ALL_SESSIONS_PATH(EVENT_KEY_PLACEHOLDER)
  }
  if (!value.startsWith('/') || !value.includes(EVENT_KEY_PLACEHOLDER)) {
    throw new Error(`--path must start with / and contain ${EVENT_KEY_PLACEHOLDER}`)
  }
  return value
}

function parsePaceMs(value: string | undefined): number {
  if (value === undefined) {
    return 5000
  }
  if (!/^\d+$/.test(value)) {
    throw new Error('--pace-ms must be a non-negative integer')
  }
  return Number(value)
}

export function parseExportProbeArgs(argv: string[]): ExportProbeArgs {
  const values = collectFlagValues(argv)
  const outDir = values.get('--out-dir')?.[0]
  const fromDir = values.get('--from-dir')?.[0]
  if (outDir !== undefined && fromDir !== undefined) {
    throw new Error(EXACTLY_ONE_MODE)
  }

  if (fromDir !== undefined) {
    rejectFlags(values, DOWNLOAD_ONLY_FLAGS, '--out-dir')
    const dateHeaders = values.get('--date-header') ?? []
    return {
      mode: 'summarize',
      fromDir,
      dateHeaders: dateHeaders.length > 0 ? dateHeaders : ['Date Submitted'],
    }
  }

  if (outDir === undefined) {
    throw new Error(EXACTLY_ONE_MODE)
  }
  rejectFlags(values, SUMMARY_ONLY_FLAGS, '--from-dir')
  const eventKeys = values.get('--event') ?? []
  const invalidKey = eventKeys.find((key) => !isEventKey(key))
  if (invalidKey !== undefined) {
    throw new Error(`event key "${invalidKey}" is not numeric`)
  }
  return {
    mode: 'download',
    outDir,
    eventKeys,
    pathTemplate: parsePathTemplate(values.get('--path')?.[0]),
    paceMs: parsePaceMs(values.get('--pace-ms')?.[0]),
  }
}

export function exportPath(pathTemplate: string, eventKey: string): string {
  return pathTemplate.split(EVENT_KEY_PLACEHOLDER).join(eventKey)
}

function isEventKey(value: string): boolean {
  return /^\d+$/.test(value)
}

export function exportFileName(eventKey: string): string {
  if (!isEventKey(eventKey)) {
    throw new Error(`event key "${eventKey}" is not numeric`)
  }
  return `${eventKey}.xlsx`
}

export function eventKeysInDirectory(fileNames: string[]): string[] {
  return fileNames
    .map((name) => /^(\d+)\.xlsx$/.exec(name)?.[1])
    .filter((key): key is string => key !== undefined)
    .sort((a, b) => a.length - b.length || (a < b ? -1 : a > b ? 1 : 0))
}

export type DownloadResultLine =
  | { eventKey: string; outcome: 'saved'; bytes: number }
  | ({ eventKey: string; outcome: 'failed' } & ExportProbeFailure)

export type SummaryResultLine =
  | { eventKey: string; outcome: 'xlsx'; summary: ExportProbeSummary }
  | ({ eventKey: string; outcome: 'failed' } & ExportProbeFailure)

export function savedLine(eventKey: string, bytes: number): DownloadResultLine {
  return { eventKey, outcome: 'saved', bytes }
}

export function summarizedLine(eventKey: string, summary: ExportProbeSummary): SummaryResultLine {
  return { eventKey, outcome: 'xlsx', summary }
}

export function failedLine(
  eventKey: string,
  err: unknown,
): { eventKey: string; outcome: 'failed' } & ExportProbeFailure {
  return { eventKey, outcome: 'failed', ...describeProbeFailure(err) }
}

function countOutcome(lines: { outcome: string }[], outcome: string): number {
  return lines.filter((line) => line.outcome === outcome).length
}

export function downloadTotalsLine(
  lines: DownloadResultLine[],
  notProbed: number,
  signIn?: ExportProbeFailure,
): {
  totals: { saved: number; failed: number; notProbed: number }
  signIn?: ExportProbeFailure
} {
  const totals = {
    saved: countOutcome(lines, 'saved'),
    failed: countOutcome(lines, 'failed'),
    notProbed,
  }
  return signIn ? { totals, signIn } : { totals }
}

export function summaryTotalsLine(lines: SummaryResultLine[]): {
  totals: { xlsx: number; failed: number }
} {
  return { totals: { xlsx: countOutcome(lines, 'xlsx'), failed: countOutcome(lines, 'failed') } }
}
