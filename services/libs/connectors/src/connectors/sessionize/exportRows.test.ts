import { describe, expect, it } from 'vitest'

import { ProviderContractError } from '../../http/errors'
import type { ExportCell, ExportSheet } from './exportFile'
import {
  ExportProfile,
  ParsedExport,
  compareWithLastGoodRun,
  parseSessionizeExport,
  toExportTimestamp,
} from './exportRows'

const ALL_HEADERS = [
  'Session Id',
  'Title',
  'Description',
  'Owner',
  'Owner Email',
  'Status',
  'Date Submitted',
  'Owner Notes',
  'Speaker Id',
  'FirstName',
  'LastName',
  'Email',
  'TagLine',
  'Bio',
  'LinkedIn',
  'Profile Picture',
  'Company',
  'Company Website',
  'Speaker Title',
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
  'Scheduled Duration',
]

const SPEAKER_A = '0a1b2c3d-1111-4aaa-8bbb-000000000001'
const SPEAKER_B = '0a1b2c3d-1111-4aaa-8bbb-000000000002'
const SPEAKER_C = '0a1b2c3d-1111-4aaa-8bbb-000000000003'

const SUBMITTED = new Date(Date.UTC(2027, 2, 4, 14, 30))

type AllRow = Partial<Record<(typeof ALL_HEADERS)[number], ExportCell>>

function allSheet(
  rows: AllRow[],
  headers: string[] = ALL_HEADERS,
  sheetName = 'Sessions and speakers',
): ExportSheet {
  return {
    sheetNames: [sheetName],
    headers,
    rows: rows.map((row) => headers.map((header) => row[header.trim()] ?? null)),
  }
}

function talk(overrides: AllRow = {}): AllRow {
  return {
    'Session Id': 501,
    Title: 'A made-up talk',
    Status: 'Accepted',
    'Date Submitted': SUBMITTED,
    'Speaker Id': SPEAKER_A,
    FirstName: 'Ada',
    LastName: 'Example',
    Email: 'ada@example.org',
    ...overrides,
  }
}

function parseError(run: () => unknown): Error {
  try {
    run()
  } catch (err) {
    expect(err).toBeInstanceOf(ProviderContractError)
    return err as Error
  }
  throw new Error('expected a contract error')
}

describe('parseSessionizeExport', () => {
  it('maps every contract column of an all sessions export into a row', () => {
    const parsed = parseSessionizeExport(
      allSheet([
        talk({
          'Owner Email': 'owner@example.org',
          Email: '  Ada.Example@Example.ORG ',
          TagLine: 'Made-up engineer',
          Bio: 'Writes made-up code',
          LinkedIn: 'https://www.linkedin.com/in/ada-example',
          'Profile Picture': 'https://example.org/ada.png',
          Company: 'Example Corp',
          'Company Website': 'https://example.com',
          'Speaker Title': 'Staff Engineer',
        }),
      ]),
      'all',
    )

    expect(parsed.rows).toEqual([
      {
        sessionId: '501',
        title: 'A made-up talk',
        status: 'Accepted',
        dateSubmitted: '2027-03-04T14:30:00.000Z',
        speakerId: SPEAKER_A,
        firstName: 'Ada',
        lastName: 'Example',
        email: 'ada.example@example.org',
        linkedin: 'https://www.linkedin.com/in/ada-example',
        bio: 'Writes made-up code',
        tagline: 'Made-up engineer',
        photoUrl: 'https://example.org/ada.png',
        company: 'Example Corp',
        companyWebsite: 'https://example.com',
        jobTitle: 'Staff Engineer',
        room: null,
        scheduledAt: null,
        durationMinutes: null,
      },
    ])
    expect(parsed.profile).toEqual({
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
        'Bio',
        'TagLine',
        'Profile Picture',
        'Company',
        'Company Website',
        'Speaker Title',
      ],
      rowCount: 1,
      emailFillRate: 1,
      linkedinFillRate: 1,
    })
    expect(parsed.skipped).toEqual({ testSubmissions: 0, noSpeaker: 0 })
    expect(parsed.sensitiveColumns).toEqual([])
  })

  it('matches the required columns trimmed and case-insensitive', () => {
    const headers = ALL_HEADERS.map((header) => ` ${header.toUpperCase()} `)
    const sheet = allSheet([talk()])

    const parsed = parseSessionizeExport({ ...sheet, headers }, 'all')

    expect(parsed.rows).toHaveLength(1)
    expect(parsed.rows[0].email).toBe('ada@example.org')
  })

  it.each([
    'Session Id',
    'Title',
    'Status',
    'Date Submitted',
    'Speaker Id',
    'FirstName',
    'LastName',
    'Email',
  ])('refuses an all sessions export without %s', (column) => {
    const headers = ALL_HEADERS.filter((header) => header !== column)

    const err = parseError(() => parseSessionizeExport(allSheet([talk()], headers), 'all'))

    expect(err.message).toContain(`"${column}"`)
  })

  it.each([
    ['all', 'Accepted sessions and speakers'],
    ['accepted', 'Sessions and speakers'],
  ] as const)('refuses a %s export from the worksheet "%s"', (variant, sheetName) => {
    const err = parseError(() =>
      parseSessionizeExport(allSheet([talk()], ALL_HEADERS, sheetName), variant),
    )

    expect(err.message).toContain(sheetName)
  })

  it('parses a sheet without a header row as an empty export', () => {
    const empty: ParsedExport = {
      rows: [],
      profile: { columns: [], rowCount: 0, emailFillRate: 0, linkedinFillRate: 0 },
      skipped: { testSubmissions: 0, noSpeaker: 0 },
      sensitiveColumns: [],
    }

    for (const variant of ['all', 'accepted'] as const) {
      expect(
        parseSessionizeExport({ sheetNames: ['Sheet1'], headers: [], rows: [] }, variant),
      ).toEqual(empty)
    }
  })

  it('groups speaker rows by Session Id, inheriting the empty session cells', () => {
    const parsed = parseSessionizeExport(
      allSheet([
        talk(),
        {
          'Session Id': 501,
          'Speaker Id': SPEAKER_B,
          FirstName: 'Bob',
          LastName: 'Sample',
          Email: 'bob@example.org',
        },
        talk({
          'Session Id': '502',
          Title: 'Undated talk',
          Status: 'Waitlisted',
          'Date Submitted': null,
          'Speaker Id': SPEAKER_C,
        }),
      ]),
      'all',
    )

    expect(
      parsed.rows.map(({ sessionId, title, status, dateSubmitted, speakerId }) => ({
        sessionId,
        title,
        status,
        dateSubmitted,
        speakerId,
      })),
    ).toEqual([
      {
        sessionId: '501',
        title: 'A made-up talk',
        status: 'Accepted',
        dateSubmitted: '2027-03-04T14:30:00.000Z',
        speakerId: SPEAKER_A,
      },
      {
        sessionId: '501',
        title: 'A made-up talk',
        status: 'Accepted',
        dateSubmitted: '2027-03-04T14:30:00.000Z',
        speakerId: SPEAKER_B,
      },
      {
        sessionId: '502',
        title: 'Undated talk',
        status: 'Waitlisted',
        dateSubmitted: null,
        speakerId: SPEAKER_C,
      },
    ])
  })

  it('drops test submissions and rows without a speaker, counting them', () => {
    const parsed = parseSessionizeExport(
      allSheet([
        talk({ Title: ' this is my TEST submission ' }),
        { 'Session Id': 501, 'Speaker Id': SPEAKER_B },
        talk({ 'Session Id': 'test-1' }),
        talk({ 'Session Id': 502, 'Speaker Id': null }),
        talk({ 'Session Id': 503 }),
      ]),
      'all',
    )

    expect(parsed.rows.map((row) => row.sessionId)).toEqual(['503'])
    expect(parsed.skipped).toEqual({ testSubmissions: 3, noSpeaker: 1 })
  })

  it.each([
    ['an empty Session Id', [talk({ 'Session Id': null })], 'row 1', 'Session Id'],
    [
      'a new session without a Status',
      [talk(), talk({ 'Session Id': 502, Status: null, 'Speaker Id': SPEAKER_B })],
      'row 2',
      'Status',
    ],
    ['an unknown status', [talk({ Status: 'Maybe Later' })], 'row 1', '"Maybe Later"'],
    ['a Speaker Id that is not a GUID', [talk({ 'Speaker Id': 'spk-1' })], 'row 1', 'Speaker Id'],
    [
      'a date column holding text',
      [talk({ 'Date Submitted': '2027-03-04 14:30' })],
      'row 1',
      'Date Submitted',
    ],
    [
      'a duplicate session and speaker pair',
      [talk(), { 'Session Id': 501, 'Speaker Id': SPEAKER_A }],
      'row 2',
      'Speaker Id',
    ],
  ])('refuses %s, naming the row and column', (_, rows, rowLabel, detail) => {
    const err = parseError(() => parseSessionizeExport(allSheet(rows), 'all'))

    expect(err.message).toContain(rowLabel)
    expect(err.message).toContain(detail)
    expect(err.message).not.toContain('ada')
    expect(err.message).not.toContain('2027-03-04 14:30')
  })

  it('reads only contract columns and lists the sensitive ones by header', () => {
    const headers = [
      ...ALL_HEADERS,
      'Gender Identity',
      'Person of color',
      'Racial/ethnic background',
      'Country of residence',
      'Do you require travel funding?',
      'T-shirt size',
    ]

    const parsed = parseSessionizeExport(
      allSheet([talk({ 'Gender Identity': 'made-up answer', 'T-shirt size': 'M' })], headers),
      'all',
    )

    expect(JSON.stringify(parsed.rows)).not.toContain('made-up answer')
    expect(parsed.sensitiveColumns).toEqual([
      'Gender Identity',
      'Person of color',
      'Racial/ethnic background',
      'Country of residence',
      'Do you require travel funding?',
    ])
  })

  it('computes fill rates over the kept rows', () => {
    const parsed = parseSessionizeExport(
      allSheet([
        talk({ LinkedIn: 'https://www.linkedin.com/in/ada-example' }),
        { 'Session Id': 501, 'Speaker Id': SPEAKER_B, Email: null },
        { 'Session Id': 501, 'Speaker Id': SPEAKER_C, Email: 'cy@example.net' },
        talk({ 'Session Id': 502, 'Speaker Id': null }),
      ]),
      'all',
    )

    expect(parsed.profile).toMatchObject({
      rowCount: 3,
      emailFillRate: 2 / 3,
      linkedinFillRate: 1 / 3,
    })
  })

  describe('accepted variant', () => {
    function acceptedSheet(rows: ExportCell[][], headers = ACCEPTED_HEADERS): ExportSheet {
      return { sheetNames: ['Accepted sessions and speakers'], headers, rows }
    }

    const scheduled = new Date(Date.UTC(2027, 5, 10, 9, 0))

    it('marks every row Accepted and inherits room, start and duration', () => {
      const parsed = parseSessionizeExport(
        acceptedSheet([
          [601, 'Keynote', SPEAKER_A, 'Ada', 'Example', 'ADA@example.org', 'Hall A', scheduled, 45],
          [601, null, SPEAKER_B, 'Bob', 'Sample', null, null, null, null],
          [602, 'Unscheduled talk', SPEAKER_C, 'Cy', 'Test', null, null, null, null],
        ]),
        'accepted',
      )

      expect(
        parsed.rows.map(
          ({ sessionId, title, status, dateSubmitted, room, scheduledAt, durationMinutes }) => ({
            sessionId,
            title,
            status,
            dateSubmitted,
            room,
            scheduledAt,
            durationMinutes,
          }),
        ),
      ).toEqual([
        {
          sessionId: '601',
          title: 'Keynote',
          status: 'Accepted',
          dateSubmitted: null,
          room: 'Hall A',
          scheduledAt: '2027-06-10T09:00:00.000Z',
          durationMinutes: 45,
        },
        {
          sessionId: '601',
          title: 'Keynote',
          status: 'Accepted',
          dateSubmitted: null,
          room: 'Hall A',
          scheduledAt: '2027-06-10T09:00:00.000Z',
          durationMinutes: 45,
        },
        {
          sessionId: '602',
          title: 'Unscheduled talk',
          status: 'Accepted',
          dateSubmitted: null,
          room: null,
          scheduledAt: null,
          durationMinutes: null,
        },
      ])
    })

    it.each(['Room', 'Scheduled At'])('refuses an accepted export without %s', (column) => {
      const headers = ACCEPTED_HEADERS.filter((header) => header !== column)

      const err = parseError(() => parseSessionizeExport(acceptedSheet([], headers), 'accepted'))

      expect(err.message).toContain(`"${column}"`)
    })

    it('refuses a duration that is not a number of minutes', () => {
      const err = parseError(() =>
        parseSessionizeExport(
          acceptedSheet([[601, 'Keynote', SPEAKER_A, 'Ada', 'Example', null, 'Hall A', null, 'x']]),
          'accepted',
        ),
      )

      expect(err.message).toContain('Scheduled Duration')
    })
  })
})

describe('toExportTimestamp', () => {
  it.each([
    ['a date with a time', new Date(Date.UTC(2027, 2, 4, 14, 30, 15)), '2027-03-04T14:30:15.000Z'],
    ['a date at midnight', new Date(Date.UTC(2027, 2, 4)), '2027-03-04T00:00:00.000Z'],
    ['an empty cell', null, null],
  ])('returns %s as written', (_, cell, expected) => {
    expect(toExportTimestamp(cell)).toBe(expected)
  })

  it.each([
    ['text', '2027-03-04 14:30'],
    ['a number', 46012.5],
    ['a date before 1970', new Date(Date.UTC(1969, 11, 31, 23, 59))],
    ['an invalid date', new Date(Number.NaN)],
  ])('refuses %s', (_, cell) => {
    const err = parseError(() => toExportTimestamp(cell))

    expect(err.message).not.toContain('2027-03-04')
  })
})

describe('compareWithLastGoodRun', () => {
  const COLUMNS = ['Session Id', 'Title', 'Email', 'LinkedIn', 'Company', 'Speaker Title']

  function parsed(profile: Partial<ExportProfile>, sensitiveColumns: string[] = []): ParsedExport {
    return {
      rows: [],
      profile: {
        columns: COLUMNS,
        rowCount: 100,
        emailFillRate: 0.9,
        linkedinFillRate: 0.5,
        ...profile,
      },
      skipped: { testSubmissions: 0, noSpeaker: 0 },
      sensitiveColumns,
    }
  }

  const last: ExportProfile = {
    columns: COLUMNS,
    rowCount: 100,
    emailFillRate: 0.9,
    linkedinFillRate: 0.5,
  }

  it('warns only about sensitive columns without a last good run', () => {
    expect(
      compareWithLastGoodRun(parsed({ rowCount: 1, emailFillRate: 0 }, ['Gender Identity']), null),
    ).toEqual([{ kind: 'sensitive_column', detail: { column: 'Gender Identity' } }])
  })

  it('stays quiet when nothing dropped', () => {
    expect(
      compareWithLastGoodRun(
        parsed({ rowCount: 51, emailFillRate: 0.71, linkedinFillRate: 0.31 }),
        last,
      ),
    ).toEqual([])
  })

  it('warns about missing optional columns, fill rate drops and a row count drop', () => {
    const current = parsed({
      columns: ['Session Id', 'Title', 'Email', 'LinkedIn'],
      rowCount: 49,
      emailFillRate: 0.6,
      linkedinFillRate: 0.2,
    })

    expect(compareWithLastGoodRun(current, last)).toEqual([
      { kind: 'missing_optional_column', detail: { column: 'Company' } },
      { kind: 'missing_optional_column', detail: { column: 'Speaker Title' } },
      { kind: 'fill_rate_drop', detail: { field: 'email', last: 0.9, current: 0.6 } },
      { kind: 'fill_rate_drop', detail: { field: 'linkedin', last: 0.5, current: 0.2 } },
      { kind: 'row_count_drop', detail: { last: 100, current: 49 } },
    ])
  })

  it('reports only the row count drop for an empty export', () => {
    const current = parsed({ columns: [], rowCount: 0, emailFillRate: 0, linkedinFillRate: 0 })

    expect(compareWithLastGoodRun(current, last)).toEqual([
      { kind: 'row_count_drop', detail: { last: 100, current: 0 } },
    ])
  })
})
