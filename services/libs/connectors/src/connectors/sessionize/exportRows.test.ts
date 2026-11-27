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
const SPEAKER_D = '0a1b2c3d-1111-4aaa-8bbb-00000000000d'

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
          TagLine: '   ',
          Bio: ' Writes made-up code ',
          LinkedIn: 'https://www.linkedin.com/in/ada-example',
          'Profile Picture': 'https://example.org/ada.png',
          Company: 1234,
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
        tagline: null,
        photoUrl: 'https://example.org/ada.png',
        company: '1234',
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
    expect(parsed.duplicateOptionalColumns).toEqual([])
  })

  it('matches the required columns trimmed and case-insensitive', () => {
    const headers = ALL_HEADERS.map((header) => ` ${header.toUpperCase()} `)
    const sheet = allSheet([talk()])

    const parsed = parseSessionizeExport({ ...sheet, headers }, 'all')

    expect(parsed.rows).toHaveLength(1)
    expect(parsed.rows[0].email).toBe('ada@example.org')
  })

  it.each(
    [
      'Session Id',
      'Title',
      'Status',
      'Date Submitted',
      'Speaker Id',
      'FirstName',
      'LastName',
      'Email',
    ].flatMap((column) => [
      ['without', column, ALL_HEADERS.filter((header) => header !== column)],
      ['with two', column, [...ALL_HEADERS, ` ${column.toLowerCase()}`]],
    ]),
  )('refuses an all sessions export %s %s columns', (_, column, headers) => {
    const err = parseError(() => parseSessionizeExport(allSheet([talk()], headers), 'all'))

    expect(err.message).toContain(`"${column}"`)
  })

  it.each([
    ['all', 'Accepted sessions and speakers', ALL_HEADERS, 'Sessions and speakers'],
    ['accepted', 'Sessions and speakers', ALL_HEADERS, 'Accepted sessions and speakers'],
    ['all', 'Sheet1', [], 'Sessions and speakers'],
    ['accepted', 'Sheet1', [], 'Accepted sessions and speakers'],
  ] as const)(
    'refuses a %s export from the worksheet "%s"',
    (variant, sheetName, headers, expected) => {
      const err = parseError(() =>
        parseSessionizeExport(allSheet([], [...headers], sheetName), variant),
      )

      expect(err.message).toContain(`"${expected}"`)
      expect(err.message).not.toContain(`"${sheetName}"`)
    },
  )

  it('parses a sheet without a header row as an empty export', () => {
    const empty: ParsedExport = {
      rows: [],
      profile: { columns: [], rowCount: 0, emailFillRate: 0, linkedinFillRate: 0 },
      skipped: { testSubmissions: 0, noSpeaker: 0 },
      duplicateOptionalColumns: [],
    }

    for (const [variant, sheetName] of [
      ['all', 'Sessions and speakers'],
      ['accepted', 'Accepted sessions and speakers'],
    ] as const) {
      expect(
        parseSessionizeExport({ sheetNames: [sheetName], headers: [], rows: [] }, variant),
      ).toEqual(empty)
    }
  })

  it('groups rows by Session Id, taking the session cells from its first row', () => {
    const parsed = parseSessionizeExport(
      allSheet([
        talk(),
        {
          'Session Id': '0501',
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
        {
          'Session Id': 501,
          Title: 'A made-up talk',
          Status: 'Accepted',
          'Speaker Id': SPEAKER_D.toUpperCase(),
        },
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
      {
        sessionId: '501',
        title: 'A made-up talk',
        status: 'Accepted',
        dateSubmitted: '2027-03-04T14:30:00.000Z',
        speakerId: SPEAKER_D,
      },
    ])
  })

  it('drops test submissions and rows without a speaker, counting them', () => {
    const parsed = parseSessionizeExport(
      allSheet([
        talk({ Title: ' this is my TEST submission ', Status: 'Maybe Later' }),
        { 'Session Id': 501, 'Speaker Id': SPEAKER_B },
        talk({ 'Session Id': 504, Title: 'This is my test submission', Status: null }),
        talk({ 'Session Id': 502, 'Speaker Id': null }),
        talk({ 'Session Id': 503 }),
      ]),
      'all',
    )

    expect(parsed.rows.map((row) => row.sessionId)).toEqual(['503'])
    expect(parsed.skipped).toEqual({ testSubmissions: 3, noSpeaker: 1 })
  })

  it.each([
    ['an empty Session Id', [talk({ 'Session Id': null })], 'data row 1', 'Session Id'],
    [
      'a non-numeric Session Id',
      [talk({ 'Session Id': 'test-1' })],
      'data row 1',
      'column "Session Id"',
    ],
    [
      'a new session without a Status',
      [talk(), talk({ 'Session Id': 502, Status: null, 'Speaker Id': SPEAKER_B })],
      'data row 2',
      'Status',
    ],
    ['an unknown status', [talk({ Status: 'Maybe Later' })], 'data row 1', 'column "Status"'],
    [
      'a Speaker Id that is not a GUID',
      [talk({ 'Speaker Id': 'spk-1' })],
      'data row 1',
      'Speaker Id',
    ],
    [
      'a date column holding text',
      [talk({ 'Date Submitted': '2027-03-04 14:30' })],
      'data row 1',
      'Date Submitted',
    ],
    [
      'a later row with another status',
      [talk(), { 'Session Id': 501, 'Speaker Id': SPEAKER_B, Status: 'Declined' }],
      'data row 2',
      'column "Status"',
    ],
    [
      'a later row dating an undated session',
      [
        talk({ 'Date Submitted': null }),
        { 'Session Id': 501, 'Speaker Id': SPEAKER_B, 'Date Submitted': SUBMITTED },
      ],
      'data row 2',
      'column "Date Submitted"',
    ],
    [
      'a session and speaker pair repeated in another letter case',
      [
        talk(),
        talk({ 'Session Id': 502, 'Speaker Id': SPEAKER_B }),
        { 'Session Id': 501, 'Speaker Id': SPEAKER_A.toUpperCase() },
      ],
      'data row 3',
      'Speaker Id',
    ],
    [
      'a session and speaker pair repeated with a zero-padded Session Id',
      [talk(), { 'Session Id': '0501', 'Speaker Id': SPEAKER_A }],
      'data row 2',
      'Speaker Id',
    ],
  ])('refuses %s, naming the row and column', (_, rows, rowLabel, detail) => {
    const err = parseError(() => parseSessionizeExport(allSheet(rows), 'all'))

    expect(err.message).toContain(rowLabel)
    expect(err.message).toContain(detail)
    for (const cellText of ['ada', '2027-03-04', 'Maybe Later', 'Declined', 'test-1', 'spk-1']) {
      expect(err.message).not.toContain(cellText)
    }
  })

  it('reads only contract columns and skips duplicated optional ones', () => {
    const headers = [
      ...ALL_HEADERS,
      ' company ',
      'Gender Identity',
      'Person of color',
      'Racial/ethnic background',
      'Country of residence',
      'Do you require travel funding?',
      'T-shirt size',
    ]

    const parsed = parseSessionizeExport(
      allSheet(
        [
          talk({
            'Gender Identity': 'made-up answer',
            'T-shirt size': 'M',
            Company: 'Example Corp',
          }),
        ],
        headers,
      ),
      'all',
    )

    expect(JSON.stringify(parsed.rows)).not.toContain('made-up answer')
    expect(parsed.rows[0].company).toBeNull()
    expect(parsed.profile.columns).not.toContain('Company')
    expect(parsed.duplicateOptionalColumns).toEqual(['Company'])
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

  function parsed(
    profile: Partial<ExportProfile>,
    duplicateOptionalColumns: string[] = [],
  ): ParsedExport {
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
      duplicateOptionalColumns,
    }
  }

  const last: ExportProfile = {
    columns: COLUMNS,
    rowCount: 100,
    emailFillRate: 0.9,
    linkedinFillRate: 0.5,
  }

  it('warns only about duplicated columns without a last good run', () => {
    expect(
      compareWithLastGoodRun(parsed({ rowCount: 1, emailFillRate: 0 }, ['Company']), null),
    ).toEqual([{ kind: 'duplicate_optional_column', detail: { column: 'Company' } }])
  })

  it('stays quiet when nothing dropped', () => {
    expect(
      compareWithLastGoodRun(
        parsed({ rowCount: 50, emailFillRate: 0.7, linkedinFillRate: 0.31 }),
        last,
      ),
    ).toEqual([])
  })

  it('warns about missing optional columns, fill rate drops and a row count drop', () => {
    const current = parsed(
      {
        columns: ['Session Id', 'Title', 'Email', 'LinkedIn'],
        rowCount: 49,
        emailFillRate: 0.6,
        linkedinFillRate: 0.2,
      },
      ['Speaker Title'],
    )

    expect(compareWithLastGoodRun(current, last)).toEqual([
      { kind: 'duplicate_optional_column', detail: { column: 'Speaker Title' } },
      { kind: 'missing_optional_column', detail: { column: 'Company' } },
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
