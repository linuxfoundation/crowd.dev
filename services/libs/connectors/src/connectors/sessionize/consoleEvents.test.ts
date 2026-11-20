import { readFileSync } from 'node:fs'
import { join } from 'node:path'

import { describe, expect, it, vi } from 'vitest'

import type { ConnectorHttp } from '../../http/client'
import { ProviderContractError } from '../../http/errors'
import { SESSIONIZE_EVENTS_PATH, fetchActivatedEvents, parseActivatedEvents } from './consoleEvents'
import { SESSIONIZE_BASE_URL } from './session'

const FIXTURE = readFileSync(join(__dirname, 'consoleEvents.fixture.html'), 'utf8')

function pane(rows: string): string {
  return `<div id="tab-activated" class="tab-pane"><table><tbody>${rows}</tbody></table></div><div id="tab-test"></div>`
}

function row(
  options: {
    key?: string
    name?: string
    eventDates?: string
    cfpDates?: string
    link?: boolean
  } = {},
): string {
  const {
    key = '1',
    name = 'Event',
    eventDates = '15 Mar 2027',
    cfpDates = '',
    link = true,
  } = options
  const nameCell = link ? `<a href="/app/organizer/event/${key}">${name}</a>` : name
  return `<tr><td>b</td><td>${nameCell}</td><td></td><td>${eventDates}<div>in 1 month</div></td><td>${cfpDates}<div>x</div></td><td></td></tr>`
}

describe('parseActivatedEvents', () => {
  it('parses every activated row in the fixture', () => {
    expect(parseActivatedEvents(FIXTURE)).toEqual([
      {
        eventKey: '90001',
        eventName: 'Example Agents Summit North America 2027',
        eventStart: '2027-04-28',
        eventEnd: '2027-04-29',
        cfpStart: '2026-11-02T09:00:00',
        cfpEnd: '2027-01-18T23:59:00',
      },
      {
        eventKey: '90002',
        eventName: 'Example Co-located Events Europe 2027',
        eventStart: '2027-03-15',
        eventEnd: '2027-03-15',
        cfpStart: '2026-10-26T00:00:00',
        cfpEnd: '2026-11-30T23:59:00',
      },
      {
        eventKey: '90003',
        eventName: 'Example Cloud Conference Europe 2027',
        eventStart: '2027-03-15',
        eventEnd: '2027-03-18',
        cfpStart: '2026-08-10T00:00:00',
        cfpEnd: '2026-09-19T23:59:00',
      },
      {
        eventKey: '90004',
        eventName: 'Example Security Day North America 2026',
        eventStart: '2026-05-21',
        eventEnd: '2026-05-21',
        cfpStart: '2025-12-16T00:00:00',
        cfpEnd: '2026-02-15T23:59:00',
      },
    ])
  })

  it('takes the previous year when the start month is after the end month', () => {
    const [event] = parseActivatedEvents(
      pane(
        row({
          eventDates: '30 Dec  - 2 Jan 2027',
          cfpDates: '14 Dec  11:59 pm - 10 Jan 2027 11:59 pm',
        }),
      ),
    )
    expect(event.eventStart).toBe('2026-12-30')
    expect(event.eventEnd).toBe('2027-01-02')
    expect(event.cfpStart).toBe('2026-12-14T23:59:00')
    expect(event.cfpEnd).toBe('2027-01-10T23:59:00')
  })

  it('converts 12 am and 12 pm', () => {
    const [event] = parseActivatedEvents(
      pane(row({ cfpDates: '1 Feb 2027 12:00 am - 2 Feb 2027 12:00 pm' })),
    )
    expect(event.cfpStart).toBe('2027-02-01T00:00:00')
    expect(event.cfpEnd).toBe('2027-02-02T12:00:00')
  })

  it('decodes entities in names', () => {
    const [event] = parseActivatedEvents(pane(row({ name: ' Example_dev &amp; ML ' })))
    expect(event.eventName).toBe('Example_dev & ML')
  })

  it('allows an empty CFP cell', () => {
    const [event] = parseActivatedEvents(pane(row()))
    expect(event.cfpStart).toBeNull()
    expect(event.cfpEnd).toBeNull()
  })

  it.each([
    ['no activated pane', '<div id="tab-current"></div><div id="tab-test"></div>'],
    ['a pane with zero rows', pane('')],
    ['a row without an event link', pane(row({ link: false }))],
    ['an unparseable event date', pane(row({ eventDates: 'soon' }))],
    ['an unknown month', pane(row({ eventDates: '15 Foo 2027' }))],
    ['an impossible day', pane(row({ eventDates: '31 Apr 2027' }))],
    ['a half-filled CFP', pane(row({ cfpDates: '1 Feb 2027 9:00 am' }))],
    ['a duplicate event key', pane(row() + row())],
  ])('rejects %s', (_label, html) => {
    expect(() => parseActivatedEvents(html)).toThrow(ProviderContractError)
  })
})

describe('fetchActivatedEvents', () => {
  function stubHttp(body: unknown): ConnectorHttp {
    return { request: vi.fn().mockResolvedValue(body), requestCount: vi.fn() }
  }

  it('requests the events page once as text', async () => {
    const http = stubHttp(FIXTURE)
    const events = await fetchActivatedEvents(http)
    expect(events).toHaveLength(4)
    expect(http.request).toHaveBeenCalledTimes(1)
    expect(http.request).toHaveBeenCalledWith(
      expect.objectContaining({
        url: SESSIONIZE_BASE_URL + SESSIONIZE_EVENTS_PATH,
        responseType: 'text',
      }),
    )
  })

  it('rejects a non-string body', async () => {
    await expect(fetchActivatedEvents(stubHttp({ html: FIXTURE }))).rejects.toBeInstanceOf(
      ProviderContractError,
    )
  })
})
