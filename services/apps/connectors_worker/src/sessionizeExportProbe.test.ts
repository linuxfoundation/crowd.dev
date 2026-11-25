import { describe, expect, it } from 'vitest'

import { ProviderContractError } from '@crowd/connectors'
import type { ExportSheet } from '@crowd/connectors/src/connectors/sessionize/exportFile'

import {
  describeProbeFailure,
  exportPath,
  PROBE_RUN_START,
  nextProbeRunState,
  parseExportProbeArgs,
  summarizeExport,
  valueShape,
} from './sessionizeExportProbe'

const HEADERS = [
  'Session Id',
  'Title',
  'Status',
  'Date Submitted',
  'Speaker Id',
  'FirstName',
  'LastName',
  'Email',
  'LinkedIn Profile',
]

function sheet(rows: ExportSheet['rows'], headers: string[] = HEADERS): ExportSheet {
  return { sheetNames: ['Sessions', 'Notes'], headers, rows }
}

describe('valueShape', () => {
  it('replaces every digit with 9 and every letter with A, keeping the rest', () => {
    expect(valueShape('2026-03-14T09:30:00Z')).toBe('9999-99-99A99:99:99A')
    expect(valueShape('Zoë 3/4')).toBe('AAA 9/9')
    expect(valueShape('ada.example@example.org')).toBe('AAA.AAAAAAA@AAAAAAA.AAA')
  })

  it.each([
    ['a Devanagari word', 'देवनागरी', 'AAAAA'],
    ['a decomposed accented word', 'Jose\u0301', 'AAAA'],
    ['non-decimal numerals', 'Ⅻ ½ ²', '9 9 9'],
    ['symbols outside the allow-list', 'a€b;c\u200d', 'A?A?A?'],
  ])('masks %s without keeping any of its characters', (_, value, shape) => {
    expect(valueShape(value)).toBe(shape)
    for (const char of value.replace(/\s/gu, '')) {
      expect(valueShape(value)).not.toContain(char)
    }
  })
})

describe('summarizeExport', () => {
  const rows: ExportSheet['rows'] = [
    [
      '101',
      'A made-up talk',
      'Accepted',
      new Date('2026-01-02T03:04:05Z'),
      'spk-1',
      'Ada',
      'Example',
      'ada.example@example.org',
      'https://www.linkedin.com/in/ada-example',
    ],
    [null, null, null, null, 'spk-2', 'Bob', 'Sample', null, 'linkedin.com/in/bob-sample'],
    [
      '102',
      'Another talk',
      'Declined',
      '2026-01-05 10:00:00 +01:00',
      'spk-3',
      'Cy',
      'Test',
      'cy@example.net',
      'https://www.linkedin.com/company/acme-example',
    ],
    [
      '103',
      'Third talk',
      'Accepted',
      '2027-11-25 23:59:59 +02:00',
      'spk-4',
      'Di',
      'Demo',
      null,
      null,
    ],
    ['104', 'Fourth talk', 'Accepted', 46012.5, 'spk-5', 'Ed', 'Mock', 'ed@example.com', null],
    [
      '105',
      'Fifth talk',
      'Accepted',
      new Date('2026-02-01T00:00:00.000Z'),
      'spk-6',
      'Fay',
      'Fake',
      null,
      null,
    ],
  ]

  it('summarizes counts, headers and shapes without the personal values', () => {
    const summary = summarizeExport(sheet(rows), ['Date Submitted'])

    expect(summary).toEqual({
      sheetNames: ['Sessions', 'Notes'],
      rowCount: 6,
      missingRequiredColumns: [],
      headersTrusted: true,
      headers: HEADERS,
      statusCounts: { Accepted: 4, Declined: 1 },
      dateColumns: {
        'Date Submitted': {
          cellTypes: { date: 2, empty: 1, string: 2, number: 1 },
          shapes: {
            'date:time': 1,
            'date:midnight': 1,
            '9999-99-99 99:99:99 +99:99': 2,
            '99999.9': 1,
          },
        },
      },
      linkedin: { filled: 3, withInPath: 2 },
      emailFilled: 3,
    })
    const printed = JSON.stringify(summary)
    for (const personal of ['ada.example@example.org', 'Ada', 'ada-example', 'spk-1']) {
      expect(printed).not.toContain(personal)
    }
  })

  it('prints only value shapes when row 1 is not a header row', () => {
    const summary = summarizeExport(
      sheet(
        [['Bob', 'bob.sample@example.com', 'Accepted']],
        ['Ada', 'ada.example@example.org', 'Status'],
      ),
      ['Date Submitted'],
    )

    expect(summary.headersTrusted).toBe(false)
    expect(summary.headers).toEqual(['AAA', 'AAA.AAAAAAA@AAAAAAA.AAA', 'AAAAAA'])
    expect(summary.statusCounts).toEqual({})
    const printed = JSON.stringify(summary)
    for (const personal of [
      'Ada',
      'ada.example@example.org',
      'Bob',
      'bob.sample@example.com',
      'Accepted',
    ]) {
      expect(printed).not.toContain(personal)
    }
  })

  it('lists required columns the sheet lacks and matches headers trimmed and case-insensitive', () => {
    const summary = summarizeExport(
      sheet([['1', 'x', '2026-01-01']], [' session id ', 'TITLE', 'date submitted ']),
      ['Date Submitted', 'Scheduled At'],
    )

    expect(summary.missingRequiredColumns).toEqual([
      'Status',
      'Speaker Id',
      'FirstName',
      'LastName',
      'Email',
    ])
    expect(summary.dateColumns).toEqual({
      'Date Submitted': { cellTypes: { string: 1 }, shapes: { '9999-99-99': 1 } },
    })
    expect(summary.statusCounts).toEqual({})
    expect(summary.linkedin).toEqual({ filled: 0, withInPath: 0 })
    expect(summary.emailFilled).toBe(0)
  })
})

describe('parseExportProbeArgs', () => {
  it('defaults to every matched event, the all sessions path, Date Submitted and 5 s pacing', () => {
    expect(parseExportProbeArgs([])).toEqual({
      eventKeys: [],
      pathTemplate: '/app/organizer/export/excel/{eventKey}/Flattened_sessions',
      dateHeaders: ['Date Submitted'],
      paceMs: 5000,
    })
  })

  it('collects repeated events and date headers, skipping the pnpm separator', () => {
    expect(
      parseExportProbeArgs([
        '--',
        '--event',
        '12662',
        '--event',
        '12663',
        '--path',
        '/app/organizer/export/excel/{eventKey}/Accepted_sessions',
        '--date-header',
        'Scheduled At',
        '--date-header',
        'Date Submitted',
        '--pace-ms',
        '0',
      ]),
    ).toEqual({
      eventKeys: ['12662', '12663'],
      pathTemplate: '/app/organizer/export/excel/{eventKey}/Accepted_sessions',
      dateHeaders: ['Scheduled At', 'Date Submitted'],
      paceMs: 0,
    })
  })

  it.each([
    [['--event'], '--event needs a value'],
    [['--event', '--path'], '--event needs a value'],
    [['--event', '12/34'], 'event key "12/34" is not numeric'],
    [['--path', '/app/export/excel'], '--path must start with / and contain {eventKey}'],
    [['--path', 'app/{eventKey}'], '--path must start with / and contain {eventKey}'],
    [['--path', '/a/{eventKey}', '--path', '/b/{eventKey}'], '--path given more than once'],
    [['--pace-ms', '-1'], '--pace-ms must be a non-negative integer'],
    [['--pace-ms', '1.5'], '--pace-ms must be a non-negative integer'],
    [['--pace-ms', '1', '--pace-ms', '2'], '--pace-ms given more than once'],
    [['--verbose'], 'unknown argument "--verbose"'],
  ])('rejects %j', (argv, message) => {
    expect(() => parseExportProbeArgs(argv)).toThrow(message)
  })
})

describe('describeProbeFailure', () => {
  it('keeps the message of connector errors only, whose messages carry no response body', () => {
    expect(describeProbeFailure(new ProviderContractError('sessionize export: not xlsx'))).toEqual({
      errorClass: 'provider.contract',
      message: 'sessionize export: not xlsx',
    })
    expect(describeProbeFailure(new TypeError('cell ada.example@example.org'))).toEqual({
      errorClass: 'TypeError',
    })
  })
})

describe('exportPath', () => {
  it('puts the event key in the template', () => {
    expect(exportPath('/app/organizer/export/excel/{eventKey}/Accepted', '12662')).toBe(
      '/app/organizer/export/excel/12662/Accepted',
    )
  })
})

describe('nextProbeRunState', () => {
  function stopsAfter(outcomes: (string | undefined)[]): boolean[] {
    let state = PROBE_RUN_START
    return outcomes.map((failureClass) => {
      state = nextProbeRunState(state, failureClass)
      return state.stop
    })
  }

  it.each([
    [['provider.auth'], [true]],
    [['provider.rate_limit'], [true]],
    [
      [undefined, 'provider.unavailable', undefined],
      [false, false, false],
    ],
    [
      ['provider.unavailable', 'provider.unavailable'],
      [false, true],
    ],
    [
      ['provider.unavailable', undefined, 'provider.unavailable'],
      [false, false, false],
    ],
    [
      ['provider.unavailable', 'provider.contract', 'provider.unavailable'],
      [false, false, false],
    ],
    [
      ['provider.contract', 'provider.contract', 'TypeError', 'TypeError'],
      [false, false, false, false],
    ],
  ])('after %j stops at %j', (outcomes, stops) => {
    expect(stopsAfter(outcomes)).toEqual(stops)
  })
})
