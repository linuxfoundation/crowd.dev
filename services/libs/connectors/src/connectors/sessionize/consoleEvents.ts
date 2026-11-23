import type { ConnectorHttp } from '../../http/client'
import { ProviderContractError } from '../../http/errors'
import { SESSIONIZE_BASE_URL } from './session'

export const SESSIONIZE_EVENTS_PATH = '/app/organizer/events'

export interface SessionizeConsoleEvent {
  eventKey: string
  eventName: string
  eventStart: string
  eventEnd: string
  cfpStart: string | null
  cfpEnd: string | null
}

interface DateParts {
  day: number
  month: number
  year: number | null
  minutes: number | null
}

const ACTIVATED_PANE_PATTERN = /<div[^>]*id="tab-activated"[^>]*>([\s\S]*?)<div[^>]*id="tab-test"/
const ROW_PATTERN = /<tr[\s>][\s\S]*?<\/tr>/g
const ROW_OPENING_PATTERN = /<tr[\s>]/g
const CELL_PATTERN = /<td[^>]*>([\s\S]*?)<\/td>/g
const EVENT_LINK_PATTERN = /<a[^>]*href="\/app\/organizer\/event\/(\d+)"[^>]*>([\s\S]*?)<\/a\s*>/
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
const DATE_TOKEN = String.raw`(\d{1,2})\s+([A-Za-z]{3})(?:\s+(\d{4}))?`
const TIME_TOKEN = String.raw`(?:\s+(\d{1,2}):(\d{2})\s+(am|pm))?`
const RANGE_TOKEN = String.raw`${DATE_TOKEN}${TIME_TOKEN}`
const EVENT_DATES_PATTERN = new RegExp(String.raw`^${DATE_TOKEN}(?:\s*-\s*${DATE_TOKEN})?$`)
const CFP_DATES_PATTERN = new RegExp(String.raw`^${RANGE_TOKEN}\s*-\s*${RANGE_TOKEN}$`)
const NAMED_ENTITIES: Record<string, string> = {
  amp: '&',
  lt: '<',
  gt: '>',
  quot: '"',
  apos: "'",
  nbsp: ' ',
}
const EXPECTED_CELL_COUNT = 6
const NAME_CELL = 1
const EVENT_DATES_CELL = 3
const CFP_DATES_CELL = 4

function contractError(message: string): ProviderContractError {
  return new ProviderContractError(`sessionize console: ${message}`)
}

function decodeEntities(text: string): string {
  return text.replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (match, entity: string) => {
    if (entity[0] === '#') {
      const isHex = entity[1].toLowerCase() === 'x'
      const codePoint = parseInt(entity.slice(isHex ? 2 : 1), isHex ? 16 : 10)
      return codePoint <= 0x10ffff ? String.fromCodePoint(codePoint) : match
    }
    return NAMED_ENTITIES[entity.toLowerCase()] ?? match
  })
}

function plainText(html: string): string {
  return decodeEntities(html.replace(/<[^>]*>/g, ' '))
    .replace(/\s+/g, ' ')
    .trim()
}

function leadingText(cellHtml: string): string {
  return plainText(cellHtml.replace(/<div[\s\S]*$/, ''))
}

function pad(value: number, length = 2): string {
  return String(value).padStart(length, '0')
}

function monthNumber(name: string, context: string): number {
  const index = MONTHS.indexOf(name)
  if (index < 0) {
    throw contractError(`unknown month "${name}" in "${context}"`)
  }
  return index + 1
}

function toMinutes(
  hour: string | undefined,
  minute: string | undefined,
  meridiem: string | undefined,
  context: string,
): number | null {
  if (hour === undefined) {
    return null
  }
  const hours = Number(hour)
  if (hours < 1 || hours > 12 || Number(minute) > 59) {
    throw contractError(`invalid time in "${context}"`)
  }
  return ((hours % 12) + (meridiem === 'pm' ? 12 : 0)) * 60 + Number(minute)
}

function toParts(tokens: (string | undefined)[], hasTime: boolean, context: string): DateParts {
  const [day, month, year, hour, minute, meridiem] = tokens
  return {
    day: Number(day),
    month: monthNumber(month as string, context),
    year: year === undefined ? null : Number(year),
    minutes: hasTime ? toMinutes(hour, minute, meridiem, context) : null,
  }
}

function resolveYear(start: DateParts, end: DateParts, context: string): number {
  if (start.year !== null) {
    return start.year
  }
  if (end.year === null) {
    throw contractError(`no year in "${context}"`)
  }
  return start.month > end.month ? end.year - 1 : end.year
}

function formatDate(parts: DateParts, year: number, context: string): string {
  const probe = new Date(Date.UTC(year, parts.month - 1, parts.day))
  if (probe.getUTCMonth() !== parts.month - 1 || probe.getUTCDate() !== parts.day) {
    throw contractError(`invalid date in "${context}"`)
  }
  return `${pad(year, 4)}-${pad(parts.month)}-${pad(parts.day)}`
}

function formatDateTime(parts: DateParts, year: number, context: string): string {
  if (parts.minutes === null) {
    throw contractError(`missing time in "${context}"`)
  }
  const time = `${pad(Math.floor(parts.minutes / 60))}:${pad(parts.minutes % 60)}:00`
  return `${formatDate(parts, year, context)}T${time}`
}

function parseEventDates(text: string): { eventStart: string; eventEnd: string } {
  const match = EVENT_DATES_PATTERN.exec(text)
  if (!match) {
    throw contractError(`unparseable event dates "${text}"`)
  }
  const first = toParts(match.slice(1, 4), false, text)
  const end = match[4] === undefined ? first : toParts(match.slice(4, 7), false, text)
  if (end.year === null) {
    throw contractError(`no year in "${text}"`)
  }
  return {
    eventStart: formatDate(first, resolveYear(first, end, text), text),
    eventEnd: formatDate(end, end.year, text),
  }
}

function parseCfpDates(text: string): { cfpStart: string | null; cfpEnd: string | null } {
  if (text === '') {
    return { cfpStart: null, cfpEnd: null }
  }
  const match = CFP_DATES_PATTERN.exec(text)
  if (!match) {
    throw contractError(`unparseable call for speakers dates "${text}"`)
  }
  const start = toParts(match.slice(1, 7), true, text)
  const end = toParts(match.slice(7, 13), true, text)
  const startYear = resolveYear(start, end, text)
  if (end.year === null) {
    throw contractError(`no year in "${text}"`)
  }
  return {
    cfpStart: formatDateTime(start, startYear, text),
    cfpEnd: formatDateTime(end, end.year, text),
  }
}

function parseRow(rowHtml: string): SessionizeConsoleEvent {
  const cells = Array.from(rowHtml.matchAll(CELL_PATTERN), (match) => match[1])
  if (cells.length !== EXPECTED_CELL_COUNT) {
    throw contractError(`event row has ${cells.length} cells, expected ${EXPECTED_CELL_COUNT}`)
  }
  const link = EVENT_LINK_PATTERN.exec(cells[NAME_CELL])
  if (!link) {
    throw contractError('event row has no event link')
  }
  const eventName = plainText(link[2])
  if (eventName === '') {
    throw contractError(`event ${link[1]} has an empty name`)
  }
  return {
    eventKey: link[1],
    eventName,
    ...parseEventDates(leadingText(cells[EVENT_DATES_CELL])),
    ...parseCfpDates(leadingText(cells[CFP_DATES_CELL])),
  }
}

export function parseActivatedEvents(html: string): SessionizeConsoleEvent[] {
  const pane = ACTIVATED_PANE_PATTERN.exec(html)
  if (!pane) {
    throw contractError('activated events pane not found')
  }
  const completeRows = pane[1].match(ROW_PATTERN) ?? []
  const openedRows = pane[1].match(ROW_OPENING_PATTERN) ?? []
  if (openedRows.length !== completeRows.length) {
    throw contractError('activated events pane has a row that does not close')
  }
  const rows = completeRows.filter((row) => /<td[\s>]/.test(row))
  if (rows.length === 0) {
    throw contractError('activated events pane has no rows')
  }
  const events = rows.map(parseRow)
  const keys = new Set<string>()
  for (const event of events) {
    if (keys.has(event.eventKey)) {
      throw contractError(`duplicate event key ${event.eventKey}`)
    }
    keys.add(event.eventKey)
  }
  return events
}

const EVENTS_PAGE_MAX_ATTEMPTS = 3

export async function fetchActivatedEvents(http: ConnectorHttp): Promise<SessionizeConsoleEvent[]> {
  const url = SESSIONIZE_BASE_URL + SESSIONIZE_EVENTS_PATH
  const body = await http.request<unknown>(
    { method: 'GET', url, responseType: 'text' },
    undefined,
    EVENTS_PAGE_MAX_ATTEMPTS,
  )
  if (typeof body !== 'string') {
    throw contractError('events page body is not text').withContext({
      request: { method: 'GET', url },
    })
  }
  return parseActivatedEvents(body)
}
