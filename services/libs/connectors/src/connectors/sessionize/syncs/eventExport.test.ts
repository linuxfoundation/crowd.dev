import ExcelJS from 'exceljs'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { SessionizeActivityType } from '@crowd/integrations/src/integrations/sessionize/types'
import type { Logger } from '@crowd/logging'

import type { EmitOverrides } from '../../../emit'
import type { ConnectorHttp } from '../../../http/client'
import { ConnectorCodeError, ProviderContractError } from '../../../http/errors'
import type { SyncContext, SyncOutcome } from '../../../types'
import type { SessionizeDeps } from '../deps'
import type { ExportCell } from '../exportFile'
import type { SessionizeEventContext } from '../mapper'
import { EVENTS_TEAM_EMAILS } from '../mapper'
import { SESSIONIZE_BASE_URL } from '../session'
import {
  EMIT_CHUNK_SIZE,
  SESSIONIZE_DOWNLOAD_GAP_MS,
  hasEventEnded,
  runEventExport,
} from './eventExport'

const steps = vi.hoisted((): string[] => [])

vi.mock('@crowd/common', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/common')>()),
  timeout: vi.fn(async (ms: number) => {
    steps.push(`wait ${ms}`)
    vi.advanceTimersByTime(ms)
  }),
}))

const DAY_MS = 24 * 60 * 60 * 1000
const EVENT: SessionizeEventContext = {
  eventKey: '91001',
  eventName: 'Example Open Source Days',
  eventStart: '2026-03-20',
  eventEnd: '2026-03-22',
}
const BEFORE_EVENT = new Date('2026-03-10T08:00:00.000Z')
const ENDED = new Date('2026-03-24T00:00:00.000Z')
const ALL_PATH = `/app/organizer/export/excel/${EVENT.eventKey}/Flattened_sessions`
const ACCEPTED_PATH = `/app/organizer/export/excel/${EVENT.eventKey}/Flattened_accepted_sessions`

const SPEAKER_ONE = '1f0c5a52-3c55-4d7e-9a61-6f1a3c0d2b01'
const SPEAKER_TWO = '1f0c5a52-3c55-4d7e-9a61-6f1a3c0d2b02'
const SPEAKER_THREE = '1f0c5a52-3c55-4d7e-9a61-6f1a3c0d2b03'
const SPEAKER_FOUR = '1f0c5a52-3c55-4d7e-9a61-6f1a3c0d2b04'
const SPEAKER_FIVE = '1f0c5a52-3c55-4d7e-9a61-6f1a3c0d2b05'
const [EVENTS_TEAM_EMAIL] = [...EVENTS_TEAM_EMAILS]
const PERSONAL_VALUES = [
  'Adaeze',
  'Okonkwo',
  'adaeze@example.org',
  'Brannoc',
  'brannoc@example.org',
  'Cyrena',
  'cyrena@example.org',
  'adaeze-okonkwo-test',
  'dov@example.org',
]

const ALL_HEADERS = [
  'Session Id',
  'Title',
  'Status',
  'Date Submitted',
  'Speaker Id',
  'FirstName',
  'LastName',
  'Email',
  'LinkedIn',
]
const ACCEPTED_HEADERS = [
  'Session Id',
  'Title',
  'Speaker Id',
  'FirstName',
  'LastName',
  'Email',
  'Room',
  'Scheduled At',
]

const ALL_ROWS: ExportCell[][] = [
  [
    5001,
    'Scaling Example Pipelines',
    'Accepted',
    new Date('2025-11-02T10:15:00.000Z'),
    SPEAKER_ONE,
    'Adaeze',
    'Okonkwo',
    'adaeze@example.org',
    'linkedin.com/in/adaeze-okonkwo-test',
  ],
  [5001, null, null, null, SPEAKER_TWO, 'Brannoc', 'Vale', 'brannoc@example.org', null],
  [
    5002,
    'A Declined Example Talk',
    'Declined',
    new Date('2025-11-03T09:00:00.000Z'),
    SPEAKER_THREE,
    'Cyrena',
    'Holt',
    'cyrena@example.org',
    null,
  ],
  [
    5003,
    'An Undated Example Talk',
    'Waitlisted',
    null,
    SPEAKER_FOUR,
    'Dov',
    'Marsh',
    'dov@example.org',
    null,
  ],
  [5004, 'A Talk Without Speaker', 'Declined', new Date('2025-11-04T09:00:00.000Z'), null, null],
]

const ACCEPTED_ROWS: ExportCell[][] = [
  [
    5001,
    'Scaling Example Pipelines',
    SPEAKER_ONE,
    'Adaeze',
    'Okonkwo',
    'adaeze@example.org',
    'Room A',
    new Date('2026-03-21T14:00:00.000Z'),
  ],
  [5001, null, SPEAKER_TWO, 'Brannoc', 'Vale', 'brannoc@example.org', null, null],
]

const ALL_PROFILE = {
  columns: [
    'Session Id',
    'Title',
    'Status',
    'Date Submitted',
    'Speaker Id',
    'FirstName',
    'LastName',
    'Email',
    'LinkedIn',
  ],
  rowCount: 4,
  emailFillRate: 1,
  linkedinFillRate: 0.25,
}
const ACCEPTED_PROFILE = {
  columns: ACCEPTED_HEADERS,
  rowCount: 2,
  emailFillRate: 1,
  linkedinFillRate: 0,
}

interface SheetSpec {
  name: string
  headers: string[]
  rows: ExportCell[][]
}

async function xlsx({ name, headers, rows }: SheetSpec): Promise<Buffer> {
  const workbook = new ExcelJS.Workbook()
  const sheet = workbook.addWorksheet(name)
  sheet.addRow(headers)
  for (const row of rows) {
    sheet.addRow(row)
  }
  return Buffer.from(await workbook.xlsx.writeBuffer())
}

const allSheet = (rows = ALL_ROWS, headers = ALL_HEADERS): SheetSpec => ({
  name: 'Sessions and speakers',
  headers,
  rows,
})
const acceptedSheet = (rows = ACCEPTED_ROWS, headers = ACCEPTED_HEADERS): SheetSpec => ({
  name: 'Accepted sessions and speakers',
  headers,
  rows,
})

interface Files {
  all: SheetSpec
  accepted?: SheetSpec
}

async function fakeHttp(files: Files): Promise<ConnectorHttp> {
  const bodies = new Map<string, Buffer>([[SESSIONIZE_BASE_URL + ALL_PATH, await xlsx(files.all)]])
  if (files.accepted) {
    bodies.set(SESSIONIZE_BASE_URL + ACCEPTED_PATH, await xlsx(files.accepted))
  }
  return {
    request: vi.fn(async (config: { url?: string }) => {
      steps.push(`get ${config.url?.slice(SESSIONIZE_BASE_URL.length)}`)
      const body = bodies.get(config.url ?? '')
      if (!body) {
        throw new Error(`unexpected request ${config.url}`)
      }
      return body
    }) as unknown as ConnectorHttp['request'],
    requestCount: () => 0,
  }
}

function fakeLog() {
  return { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() }
}

interface CtxOptions {
  files?: Files
  watermark?: Record<string, unknown> | null
  hasRunBudget?: () => boolean
  event?: SessionizeEventContext | null
  channelId?: string
}

async function makeCtx(options: CtxOptions = {}) {
  const emits: { records: unknown[]; overrides?: EmitOverrides }[] = []
  const watermarks: Record<string, unknown>[] = []
  const log = fakeLog()
  const deps: SessionizeDeps = {
    getEvent: vi.fn(async () => (options.event === undefined ? EVENT : options.event)),
  }
  const ctx: SyncContext<SessionizeDeps> = {
    channel: { channelId: options.channelId ?? EVENT.eventKey, channelName: EVENT.eventName },
    integrationId: 'integration-1',
    watermark: options.watermark ?? null,
    emit: async (records, overrides) => {
      emits.push({ records, overrides })
    },
    commitWatermark: async (next) => {
      watermarks.push(next)
    },
    hasRunBudget: options.hasRunBudget ?? (() => true),
    http: await fakeHttp(options.files ?? { all: allSheet(), accepted: acceptedSheet() }),
    log: log as unknown as Logger,
    getCredential: async () => {
      throw new Error('the eventExport sync reads no credential')
    },
    deps,
  }
  return { ctx, emits, watermarks, log }
}

function types(emits: { records: unknown[] }[]): string[] {
  return emits.flatMap(({ records }) => records.map((record) => (record as { type: string }).type))
}

function watermarkAt(
  lastRunStartedAt: Date,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    lastRunStartedAt: lastRunStartedAt.toISOString(),
    lastGoodRun: { all: ALL_PROFILE },
    ...overrides,
  }
}

const ENDED_WATERMARK = (startedAt: Date) =>
  watermarkAt(startedAt, {
    finished: true,
    lastGoodRun: { all: ALL_PROFILE, accepted: ACCEPTED_PROFILE },
  })

async function failure(run: Promise<SyncOutcome>): Promise<unknown> {
  return run.then(
    () => undefined,
    (err: unknown) => err,
  )
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] })
  steps.length = 0
})

afterEach(() => {
  vi.useRealTimers()
})

describe('hasEventEnded', () => {
  it.each([
    ['2026-03-23T23:59:59.999Z', false],
    ['2026-03-24T00:00:00.000Z', true],
    ['2026-03-01T00:00:00.000Z', false],
  ])('for an event ending 2026-03-22, at %s: %s', (now, ended) => {
    expect(hasEventEnded('2026-03-22', new Date(now))).toBe(ended)
  })

  it('refuses an event end that is not a date', () => {
    expect(() => hasEventEnded('22/03/2026', ENDED)).toThrow(ConnectorCodeError)
  })
})

describe('runEventExport', () => {
  it.each<[string, Record<string, unknown> | null, string[], EmitOverrides | undefined]>([
    [
      'sends every submission on the first run, as a backfill',
      null,
      [`5001-${SPEAKER_ONE}`, `5001-${SPEAKER_TWO}`, `5002-${SPEAKER_THREE}`],
      { backfill: true },
    ],
    [
      'treats an empty watermark as a first run',
      {},
      [`5001-${SPEAKER_ONE}`, `5001-${SPEAKER_TWO}`, `5002-${SPEAKER_THREE}`],
      { backfill: true },
    ],
    [
      'sends only submissions from 24 hours before the last run started, as live data',
      watermarkAt(new Date('2025-11-04T09:00:00.000Z')),
      [`5002-${SPEAKER_THREE}`],
      undefined,
    ],
  ])('for an ongoing event, %s', async (_, watermark, sourceIds, expectedOverrides) => {
    vi.setSystemTime(BEFORE_EVENT)
    const { ctx, emits, watermarks } = await makeCtx({ watermark })

    const outcome = await runEventExport(ctx)

    expect(steps).toEqual([`wait ${SESSIONIZE_DOWNLOAD_GAP_MS}`, `get ${ALL_PATH}`])
    expect(types(emits)).toEqual(
      Array(sourceIds.length).fill(SessionizeActivityType.SUBMITTED_SESSION),
    )
    expect(emits.map(({ overrides }) => overrides)).toEqual([expectedOverrides])
    expect(emits[0].records.map((record) => (record as { sourceId: string }).sourceId)).toEqual(
      sourceIds,
    )
    expect(emits[0].records[0]).toMatchObject({ channel: '91001' })
    expect(watermarks).toEqual([watermarkAt(BEFORE_EVENT)])
    expect(outcome).toEqual({
      complete: true,
      nextRunAt: new Date(BEFORE_EVENT.getTime() + DAY_MS),
    })
  })

  it.each([
    ['already ended at its first run', null],
    ['loaded while ongoing', watermarkAt(new Date('2026-03-21T08:00:00.000Z'))],
    ['restarted by hand after it finished', ENDED_WATERMARK(new Date('2026-03-24T00:00:00.000Z'))],
  ])(
    'loads an ended event %s once in full, as a backfill, and stops its unit',
    async (_, watermark) => {
      vi.setSystemTime(ENDED)
      const { ctx, emits, watermarks } = await makeCtx({ watermark })

      const outcome = await runEventExport(ctx)

      expect(steps).toEqual([
        `wait ${SESSIONIZE_DOWNLOAD_GAP_MS}`,
        `get ${ALL_PATH}`,
        `wait ${SESSIONIZE_DOWNLOAD_GAP_MS}`,
        `get ${ACCEPTED_PATH}`,
      ])
      expect(types(emits)).toEqual([
        ...Array(3).fill(SessionizeActivityType.SUBMITTED_SESSION),
        ...Array(2).fill(SessionizeActivityType.SPOKE_AT_SESSION),
      ])
      expect(emits[0].records.slice(3)).toMatchObject([
        { sourceId: `5001-${SPEAKER_ONE}`, timestamp: '2026-03-21T14:00:00.000Z' },
        { sourceId: `5001-${SPEAKER_TWO}`, timestamp: '2026-03-21T14:00:00.000Z' },
      ])
      expect(emits.map(({ overrides }) => overrides)).toEqual([{ backfill: true }])
      expect(watermarks).toEqual([ENDED_WATERMARK(ENDED)])
      expect(outcome).toEqual({ complete: true, finished: true })
    },
  )

  it('counts accepted rows without an email or a slot and sends them no spoke-at', async () => {
    vi.setSystemTime(ENDED)
    const { ctx, emits, log } = await makeCtx({
      files: {
        all: allSheet(),
        accepted: acceptedSheet([
          ...ACCEPTED_ROWS,
          [
            5005,
            'An Unscheduled Example Talk',
            SPEAKER_THREE,
            'Cyrena',
            'Holt',
            'cyrena@example.org',
          ],
          [5005, null, SPEAKER_FOUR, 'Dov', 'Marsh', null],
          [5006, 'Example Badge Pick-Up', SPEAKER_FIVE, 'Example', 'Events', EVENTS_TEAM_EMAIL],
        ]),
      },
    })

    await runEventExport(ctx)

    expect(
      types(emits).filter((type) => type === SessionizeActivityType.SPOKE_AT_SESSION),
    ).toHaveLength(2)
    expect(log.info.mock.calls.map(([fields]) => fields)).toContainEqual({
      event: 'sessionize_export_skipped',
      eventKey: EVENT.eventKey,
      variant: 'accepted',
      testSubmissions: 0,
      noSpeaker: 0,
      withoutEmail: 1,
      eventsTeam: 1,
      withoutSlot: 1,
      notAccepted: 0,
    })
  })

  it('finishes an ended event that never had a session', async () => {
    vi.setSystemTime(ENDED)
    const { ctx, emits, watermarks } = await makeCtx({
      files: { all: allSheet([]), accepted: acceptedSheet([]) },
    })

    expect(await runEventExport(ctx)).toEqual({ complete: true, finished: true })
    expect(types(emits)).toEqual([])
    expect(watermarks).toMatchObject([{ finished: true }])
  })

  it.each([
    [
      'the all sessions file',
      BEFORE_EVENT,
      { all: { ...allSheet(), name: 'Sheet1' }, accepted: acceptedSheet() },
      [`get ${ALL_PATH}`],
    ],
    [
      'the accepted file',
      ENDED,
      {
        all: allSheet(),
        accepted: acceptedSheet(
          ACCEPTED_ROWS.map((row) => row.slice(0, 6)),
          ACCEPTED_HEADERS.slice(0, 6),
        ),
      },
      [`get ${ALL_PATH}`, `get ${ACCEPTED_PATH}`],
    ],
    [
      'the all sessions file of an ended event lost more than half of its rows',
      ENDED,
      { all: allSheet(ALL_ROWS.slice(0, 1)), accepted: acceptedSheet() },
      [`get ${ALL_PATH}`, `get ${ACCEPTED_PATH}`],
      watermarkAt(new Date('2026-03-21T08:00:00.000Z')),
    ],
    [
      'the accepted file of an ended event is empty while sessions are accepted',
      ENDED,
      { all: allSheet(), accepted: acceptedSheet([]) },
      [`get ${ALL_PATH}`, `get ${ACCEPTED_PATH}`],
    ],
  ])(
    'emits nothing and commits nothing when %s breaks the contract',
    async (_, now, files, requests, watermark: Record<string, unknown> | null = null) => {
      vi.setSystemTime(now)
      const { ctx, emits, watermarks } = await makeCtx({ files, watermark })

      expect(await failure(runEventExport(ctx))).toBeInstanceOf(ProviderContractError)
      expect(steps.filter((step) => step.startsWith('get'))).toEqual(requests)
      expect(emits).toEqual([])
      expect(watermarks).toEqual([])
    },
  )

  it('logs warnings and skipped counts with counts and column names only', async () => {
    vi.setSystemTime(new Date('2026-03-21T12:00:00.000Z'))
    const headersWithExtraColumn = [...ALL_HEADERS, 'Gender identity']
    const { ctx, log, watermarks } = await makeCtx({
      files: {
        all: allSheet(
          ALL_ROWS.map((row, index) => [
            ...(index === 2 ? [...row.slice(0, 7), null, ...row.slice(8)] : row),
            ...Array(ALL_HEADERS.length - row.length).fill(null),
            'x',
          ]),
          headersWithExtraColumn,
        ),
        accepted: acceptedSheet(),
      },
      watermark: watermarkAt(new Date('2026-03-21T08:00:00.000Z'), {
        lastGoodRun: {
          all: {
            ...ALL_PROFILE,
            columns: [...ALL_PROFILE.columns, 'Company'],
            rowCount: 10,
            emailFillRate: 0.75,
          },
          accepted: ACCEPTED_PROFILE,
        },
      }),
    })

    await runEventExport(ctx)

    const warnings = log.warn.mock.calls.map(([fields]) => fields)
    expect(warnings).toEqual([
      {
        event: 'sessionize_export_warning',
        eventKey: EVENT.eventKey,
        variant: 'all',
        kind: 'missing_optional_column',
        detail: { column: 'Company' },
      },
      {
        event: 'sessionize_export_warning',
        eventKey: EVENT.eventKey,
        variant: 'all',
        kind: 'row_count_drop',
        detail: { last: 10, current: 4 },
      },
    ])
    expect(watermarks).toEqual([
      expect.objectContaining({
        lastGoodRun: expect.objectContaining({ all: expect.objectContaining({ rowCount: 10 }) }),
      }),
    ])
    expect(log.info.mock.calls.map(([fields]) => fields)).toContainEqual({
      event: 'sessionize_export_skipped',
      eventKey: EVENT.eventKey,
      variant: 'all',
      testSubmissions: 0,
      noSpeaker: 1,
      withoutEmail: 1,
      eventsTeam: 0,
      withoutSubmissionDate: 1,
    })
    const logged = JSON.stringify(
      Object.values(log).flatMap((mock) => mock.mock.calls as unknown[]),
    )
    for (const value of PERSONAL_VALUES) {
      expect(logged).not.toContain(value)
    }
  })

  it('stops without committing or finishing when the run budget runs out between chunks', async () => {
    vi.setSystemTime(new Date(Date.parse('2026-03-22T00:00:00.000Z') + 400 * DAY_MS))
    const rows: ExportCell[][] = Array.from({ length: EMIT_CHUNK_SIZE + 1 }, (_, index) => [
      6000 + index,
      `Example Talk ${index}`,
      'Declined',
      new Date('2025-11-05T09:00:00.000Z'),
      `00000000-0000-4000-8000-${String(index).padStart(12, '0')}`,
      'Example',
      'Speaker',
      `speaker${index}@example.org`,
      null,
    ])
    const hasRunBudget = vi.fn().mockReturnValueOnce(true).mockReturnValue(false)
    const { ctx, emits, watermarks } = await makeCtx({
      files: { all: allSheet(rows), accepted: acceptedSheet() },
      hasRunBudget,
    })

    const outcome = await runEventExport(ctx)

    expect(outcome).toEqual({ complete: false })
    expect(emits.map(({ records }) => records.length)).toEqual([EMIT_CHUNK_SIZE])
    expect(watermarks).toEqual([])
  })

  it.each<[string, CtxOptions]>([
    ['whose event has no mapping row', { event: null }],
    ['whose event key is not numeric', { channelId: '91001/../export' }],
    ['whose watermark is malformed', { watermark: { lastRunStartedAt: 'yesterday' } }],
  ])('refuses a unit %s, before any download', async (_, options) => {
    vi.setSystemTime(ENDED)
    const { ctx, emits } = await makeCtx(options)

    expect(await failure(runEventExport(ctx))).toBeInstanceOf(ConnectorCodeError)
    expect(steps).toEqual([])
    expect(emits).toEqual([])
  })
})
