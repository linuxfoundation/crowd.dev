import { ProviderContractError } from '../../http/errors'
import type { ExportCell, ExportSheet } from './exportFile'

export const SESSIONIZE_STATUSES = [
  'Accepted',
  'Declined',
  'Nominated',
  'Waitlisted',
  'Accept Queue',
  'Decline Queue',
] as const

export type SessionizeStatus = (typeof SESSIONIZE_STATUSES)[number]

export type ExportVariant = 'all' | 'accepted'

export interface SessionizeExportRow {
  sessionId: string
  title: string
  status: SessionizeStatus
  dateSubmitted: string | null
  speakerId: string
  firstName: string | null
  lastName: string | null
  email: string | null
  linkedin: string | null
  bio: string | null
  tagline: string | null
  photoUrl: string | null
  company: string | null
  companyWebsite: string | null
  jobTitle: string | null
  room: string | null
  scheduledAt: string | null
  durationMinutes: number | null
}

export interface ExportProfile {
  columns: string[]
  rowCount: number
  emailFillRate: number
  linkedinFillRate: number
}

export interface ParsedExport {
  rows: SessionizeExportRow[]
  profile: ExportProfile
  skipped: { testSubmissions: number; noSpeaker: number }
  sensitiveColumns: string[]
}

export interface ExportWarning {
  kind: 'missing_optional_column' | 'fill_rate_drop' | 'row_count_drop' | 'sensitive_column'
  detail: Record<string, unknown>
}

const COLUMNS = {
  sessionId: 'Session Id',
  title: 'Title',
  status: 'Status',
  dateSubmitted: 'Date Submitted',
  speakerId: 'Speaker Id',
  firstName: 'FirstName',
  lastName: 'LastName',
  email: 'Email',
  linkedin: 'LinkedIn',
  bio: 'Bio',
  tagline: 'TagLine',
  photoUrl: 'Profile Picture',
  company: 'Company',
  companyWebsite: 'Company Website',
  jobTitle: 'Speaker Title',
  room: 'Room',
  scheduledAt: 'Scheduled At',
  durationMinutes: 'Scheduled Duration',
} as const

type ColumnKey = keyof typeof COLUMNS

interface VariantContract {
  sheetName: string
  required: ColumnKey[]
  optional: ColumnKey[]
}

const OPTIONAL_SPEAKER_COLUMNS: ColumnKey[] = [
  'linkedin',
  'bio',
  'tagline',
  'photoUrl',
  'company',
  'companyWebsite',
  'jobTitle',
]

const SPEAKER_COLUMNS: ColumnKey[] = ['speakerId', 'firstName', 'lastName', 'email']

const VARIANT_CONTRACTS: Record<ExportVariant, VariantContract> = {
  all: {
    sheetName: 'Sessions and speakers',
    required: ['sessionId', 'title', 'status', 'dateSubmitted', ...SPEAKER_COLUMNS],
    optional: OPTIONAL_SPEAKER_COLUMNS,
  },
  accepted: {
    sheetName: 'Accepted sessions and speakers',
    required: ['sessionId', 'title', ...SPEAKER_COLUMNS, 'room', 'scheduledAt'],
    optional: [...OPTIONAL_SPEAKER_COLUMNS, 'durationMinutes'],
  },
}

const SENSITIVE_HEADER_WORDS = [
  'gender',
  'ethnic',
  'race',
  'under-represent',
  'underrepresent',
  'country',
  'travel',
  'disab',
  'pronoun',
  'nationality',
  'birth',
  'visa',
  'person of color',
]

const TEST_SUBMISSION_TITLE = 'this is my test submission'
const GUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const FILL_RATE_DROP = 0.2

type ColumnIndexes = Partial<Record<ColumnKey, number>>

interface SessionFields {
  sessionId: string
  title: string
  status: SessionizeStatus
  dateSubmitted: string | null
  room: string | null
  scheduledAt: string | null
  durationMinutes: number | null
}

function contractError(message: string): ProviderContractError {
  return new ProviderContractError(`sessionize export: ${message}`)
}

function normalizeHeader(header: string): string {
  return header.trim().toLowerCase()
}

function cellKind(cell: ExportCell): string {
  return cell instanceof Date ? 'a date' : typeof cell === 'number' ? 'a number' : 'text'
}

function readTimestamp(cell: ExportCell, where: string): string | null {
  if (cell === null) {
    return null
  }
  if (!(cell instanceof Date)) {
    throw contractError(`${where} holds ${cellKind(cell)}, not a date`)
  }
  if (Number.isNaN(cell.getTime())) {
    throw contractError(`${where} holds an invalid date`)
  }
  if (cell.getTime() < 0) {
    throw contractError(`${where} holds a date before 1970`)
  }
  return cell.toISOString()
}

export function toExportTimestamp(cell: ExportCell): string | null {
  return readTimestamp(cell, 'a date cell')
}

function columnIndexes(headers: string[], contract: VariantContract): ColumnIndexes {
  const indexes: ColumnIndexes = {}
  for (const key of [...contract.required, ...contract.optional]) {
    const wanted = normalizeHeader(COLUMNS[key])
    const matches = headers.flatMap((header, index) =>
      normalizeHeader(header) === wanted ? [index] : [],
    )
    if (matches.length > 1) {
      throw contractError(`column "${COLUMNS[key]}" appears ${matches.length} times`)
    }
    if (matches.length === 1) {
      indexes[key] = matches[0]
    } else if (contract.required.includes(key)) {
      throw contractError(`required column "${COLUMNS[key]}" is missing`)
    }
  }
  return indexes
}

function sensitiveColumns(headers: string[]): string[] {
  return headers
    .map((header) => header.trim())
    .filter((header) => {
      const normalized = header.toLowerCase()
      return SENSITIVE_HEADER_WORDS.some((word) => normalized.includes(word))
    })
}

function where(rowNumber: number, key: ColumnKey): string {
  return `row ${rowNumber} column "${COLUMNS[key]}"`
}

function rowReader(row: ExportCell[], indexes: ColumnIndexes, rowNumber: number) {
  const cell = (key: ColumnKey): ExportCell => {
    const index = indexes[key]
    return index === undefined ? null : row[index]
  }
  const text = (key: ColumnKey): string | null => {
    const value = cell(key)
    if (value instanceof Date) {
      throw contractError(`${where(rowNumber, key)} holds a date, not text`)
    }
    return value === null ? null : String(value)
  }
  const timestamp = (key: ColumnKey): string | null =>
    readTimestamp(cell(key), where(rowNumber, key))
  const minutes = (key: ColumnKey): number | null => {
    const value = cell(key)
    if (value === null) {
      return null
    }
    const number = typeof value === 'string' ? Number(value) : value
    if (typeof number !== 'number' || !Number.isFinite(number) || number < 0) {
      throw contractError(`${where(rowNumber, key)} holds ${cellKind(value)}, not minutes`)
    }
    return number
  }
  return { cell, text, timestamp, minutes }
}

type RowReader = ReturnType<typeof rowReader>

function readSessionId(read: RowReader, rowNumber: number): string | null {
  const value = read.cell('sessionId')
  if (value === null) {
    throw contractError(`row ${rowNumber} has no Session Id`)
  }
  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw contractError(`${where(rowNumber, 'sessionId')} holds a number that is not an id`)
    }
    return String(value)
  }
  if (value instanceof Date) {
    throw contractError(`${where(rowNumber, 'sessionId')} holds a date`)
  }
  return /^\d+$/.test(value) ? value : null
}

function readStatus(read: RowReader, rowNumber: number, inherited: SessionizeStatus | undefined) {
  const status = read.text('status')
  if (status === null) {
    if (inherited === undefined) {
      throw contractError(`row ${rowNumber} starts a session without a Status`)
    }
    return inherited
  }
  const known = SESSIONIZE_STATUSES.find((candidate) => candidate === status)
  if (known === undefined) {
    throw contractError(`row ${rowNumber} has the unknown Status "${status}"`)
  }
  return known
}

function readSessionFields(
  read: RowReader,
  rowNumber: number,
  sessionId: string,
  variant: ExportVariant,
  inherited: SessionFields | undefined,
): SessionFields {
  const title = read.text('title') ?? inherited?.title
  if (title === undefined) {
    throw contractError(`row ${rowNumber} starts a session without a Title`)
  }
  const isAll = variant === 'all'
  return {
    sessionId,
    title,
    status: isAll ? readStatus(read, rowNumber, inherited?.status) : 'Accepted',
    dateSubmitted: isAll
      ? (read.timestamp('dateSubmitted') ?? inherited?.dateSubmitted ?? null)
      : null,
    room: read.text('room') ?? inherited?.room ?? null,
    scheduledAt: read.timestamp('scheduledAt') ?? inherited?.scheduledAt ?? null,
    durationMinutes: read.minutes('durationMinutes') ?? inherited?.durationMinutes ?? null,
  }
}

function readSpeakerId(read: RowReader, rowNumber: number): string | null {
  const speakerId = read.cell('speakerId')
  if (speakerId === null) {
    return null
  }
  if (typeof speakerId !== 'string' || !GUID.test(speakerId)) {
    throw contractError(`${where(rowNumber, 'speakerId')} is not a GUID`)
  }
  return speakerId
}

function isTestSubmission(title: string): boolean {
  return title.trim().toLowerCase() === TEST_SUBMISSION_TITLE
}

function speakerFields(read: RowReader) {
  return {
    firstName: read.text('firstName'),
    lastName: read.text('lastName'),
    email: read.text('email')?.trim().toLowerCase() || null,
    linkedin: read.text('linkedin'),
    bio: read.text('bio'),
    tagline: read.text('tagline'),
    photoUrl: read.text('photoUrl'),
    company: read.text('company'),
    companyWebsite: read.text('companyWebsite'),
    jobTitle: read.text('jobTitle'),
  }
}

function fillRate(rows: SessionizeExportRow[], filled: (row: SessionizeExportRow) => boolean) {
  return rows.length === 0 ? 0 : rows.filter(filled).length / rows.length
}

function emptyExport(): ParsedExport {
  return {
    rows: [],
    profile: { columns: [], rowCount: 0, emailFillRate: 0, linkedinFillRate: 0 },
    skipped: { testSubmissions: 0, noSpeaker: 0 },
    sensitiveColumns: [],
  }
}

export function parseSessionizeExport(sheet: ExportSheet, variant: ExportVariant): ParsedExport {
  if (sheet.headers.length === 0) {
    return emptyExport()
  }
  const contract = VARIANT_CONTRACTS[variant]
  if (sheet.sheetNames[0] !== contract.sheetName) {
    throw contractError(
      `the ${variant} export must come from worksheet "${contract.sheetName}", not "${sheet.sheetNames[0]}"`,
    )
  }
  const indexes = columnIndexes(sheet.headers, contract)
  const rows: SessionizeExportRow[] = []
  const skipped = { testSubmissions: 0, noSpeaker: 0 }
  const seenPairs = new Set<string>()
  let session: SessionFields | undefined

  for (const [index, row] of sheet.rows.entries()) {
    const rowNumber = index + 1
    const read = rowReader(row, indexes, rowNumber)
    const sessionId = readSessionId(read, rowNumber)
    if (sessionId === null) {
      skipped.testSubmissions++
      continue
    }
    const inherited = session?.sessionId === sessionId ? session : undefined
    session = readSessionFields(read, rowNumber, sessionId, variant, inherited)
    if (isTestSubmission(session.title)) {
      skipped.testSubmissions++
      continue
    }
    const speakerId = readSpeakerId(read, rowNumber)
    if (speakerId === null) {
      skipped.noSpeaker++
      continue
    }
    const pair = `${sessionId}-${speakerId}`
    if (seenPairs.has(pair)) {
      throw contractError(
        `row ${rowNumber} repeats the Session Id and Speaker Id of an earlier row`,
      )
    }
    seenPairs.add(pair)
    rows.push({ ...session, speakerId, ...speakerFields(read) })
  }

  return {
    rows,
    profile: {
      columns: [...contract.required, ...contract.optional]
        .filter((key) => indexes[key] !== undefined)
        .map((key) => COLUMNS[key]),
      rowCount: rows.length,
      emailFillRate: fillRate(rows, (row) => row.email !== null),
      linkedinFillRate: fillRate(rows, (row) => row.linkedin !== null),
    },
    skipped,
    sensitiveColumns: sensitiveColumns(sheet.headers),
  }
}

function fillRateWarnings(current: ExportProfile, last: ExportProfile): ExportWarning[] {
  const rates = [
    ['email', last.emailFillRate, current.emailFillRate],
    ['linkedin', last.linkedinFillRate, current.linkedinFillRate],
  ] as const
  return rates
    .filter(([, lastRate, currentRate]) => lastRate - currentRate > FILL_RATE_DROP)
    .map(([field, lastRate, currentRate]) => ({
      kind: 'fill_rate_drop',
      detail: { field, last: lastRate, current: currentRate },
    }))
}

function columnWarnings(current: ExportProfile, last: ExportProfile): ExportWarning[] {
  return last.columns
    .filter((column) => !current.columns.includes(column))
    .map((column) => ({ kind: 'missing_optional_column', detail: { column } }))
}

export function compareWithLastGoodRun(
  current: ParsedExport,
  last: ExportProfile | null,
): ExportWarning[] {
  const warnings: ExportWarning[] = current.sensitiveColumns.map((column) => ({
    kind: 'sensitive_column',
    detail: { column },
  }))
  if (last === null) {
    return warnings
  }
  if (current.profile.rowCount > 0) {
    warnings.push(
      ...columnWarnings(current.profile, last),
      ...fillRateWarnings(current.profile, last),
    )
  }
  if (current.profile.rowCount < last.rowCount / 2) {
    warnings.push({
      kind: 'row_count_drop',
      detail: { last: last.rowCount, current: current.profile.rowCount },
    })
  }
  return warnings
}
