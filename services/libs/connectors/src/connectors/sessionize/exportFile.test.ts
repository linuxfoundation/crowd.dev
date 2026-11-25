import axios from 'axios'
import ExcelJS from 'exceljs'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { createHttpClient } from '../../http/client'
import type { ConnectorHttp } from '../../http/client'
import { ProviderContractError } from '../../http/errors'
import {
  SESSIONIZE_ALL_SESSIONS_PATH,
  downloadSessionizeExport,
  readExportSheet,
} from './exportFile'
import {
  SESSIONIZE_POOL_ENTRY_ID,
  applySessionizeCookie,
  interpretSessionizeResponse,
} from './session'

vi.mock('@crowd/common', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/common')>()),
  timeout: vi.fn().mockResolvedValue(undefined),
}))

const logMocks = vi.hoisted(() => ({
  debug: vi.fn(),
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
}))
vi.mock('@crowd/logging', () => ({
  getServiceChildLogger: () => logMocks,
}))

const mockedRequest = vi.spyOn(axios, 'request')

const SIGN_IN_PAGE = `<html><body>
  <form action="/login/classic" method="post">
    <input name="__RequestVerificationToken" type="hidden" value="form-token" />
  </form></body></html>`

function reply(status: number, data: unknown, headers: Record<string, unknown> = {}) {
  return { status, data, headers, statusText: '', config: {} as never }
}

async function workbookFile(build: (workbook: ExcelJS.Workbook) => void): Promise<Buffer> {
  const workbook = new ExcelJS.Workbook()
  build(workbook)
  return Buffer.from(await workbook.xlsx.writeBuffer())
}

function staticSessionHttp(invalidateToken = vi.fn()): ConnectorHttp {
  return createHttpClient({
    acquireToken: async () => ({ id: SESSIONIZE_POOL_ENTRY_ID, value: 'session-cookie' }),
    parkToken: vi.fn(),
    invalidateToken,
    applyToken: applySessionizeCookie,
    interpretResponse: interpretSessionizeResponse,
    log: logMocks as never,
  })
}

beforeEach(() => {
  mockedRequest.mockReset()
  Object.values(logMocks).forEach((mock) => mock.mockReset())
})

describe('readExportSheet', () => {
  it('returns the first worksheet headers and rows as plain values', async () => {
    const submitted = new Date(Date.UTC(2027, 0, 12, 9, 30))
    const file = await workbookFile((workbook) => {
      const sessions = workbook.addWorksheet('Accepted sessions')
      sessions.addRow([
        'Session Id',
        ' Title ',
        'Speakers',
        'Owner',
        'Submitted',
        'Confirmed',
        'Notes',
      ])
      sessions.addRow([
        101,
        { richText: [{ text: 'Scaling ' }, { font: { bold: true }, text: 'Example' }] },
        { text: 'Ada Example', hyperlink: 'https://example.org/ada' },
        '  ada@example.org  ',
        submitted,
        false,
        '',
      ])
      sessions.addRow(['  ', null, ''])
      sessions.addRow([102, 'Second talk', null, 'bob@example.org', submitted, true, 'keep'])
      sessions.getCell('I1').font = { bold: true }
      sessions.getCell('J2').fill = {
        type: 'pattern',
        pattern: 'solid',
        fgColor: { argb: 'FFFFFF00' },
      }
      workbook.addWorksheet('Speakers').addRow(['Speaker Id'])
    })

    const sheet = await readExportSheet(file)

    expect(sheet.sheetNames).toEqual(['Accepted sessions', 'Speakers'])
    expect(sheet.headers).toEqual([
      'Session Id',
      'Title',
      'Speakers',
      'Owner',
      'Submitted',
      'Confirmed',
      'Notes',
    ])
    expect(sheet.rows).toEqual([
      [101, 'Scaling Example', 'Ada Example', 'ada@example.org', submitted, 'false', null],
      [102, 'Second talk', null, 'bob@example.org', submitted, 'true', 'keep'],
    ])
    expect(sheet.rows[0][4]).toBeInstanceOf(Date)
  })

  const workbookWith = (build: (workbook: ExcelJS.Workbook) => void) => () => workbookFile(build)

  it.each([
    ['has no worksheet', workbookWith(() => undefined)],
    ['has a first worksheet without a header row', workbookWith((w) => w.addWorksheet('Empty'))],
    [
      'has a value beyond the header columns',
      workbookWith((w) => {
        const sheet = w.addWorksheet('Sessions')
        sheet.addRow(['Id'])
        sheet.addRow([1, 'stray'])
      }),
    ],
    [
      'has an error cell',
      workbookWith((w) => {
        const sheet = w.addWorksheet('Sessions')
        sheet.addRow(['Id'])
        sheet.addRow([
          { error: 'secret-export-detail@example.org' as ExcelJS.CellErrorValue['error'] },
        ])
      }),
    ],
    [
      'is a zip but not an xlsx',
      async () => Buffer.concat([Buffer.from('PK\x03\x04'), Buffer.from('secret-export-detail')]),
    ],
  ])('refuses a workbook that %s', async (_, file) => {
    const err = await readExportSheet(await file()).catch((e: unknown) => e)

    expect(err).toBeInstanceOf(ProviderContractError)
    expect(JSON.stringify({ message: (err as Error).message, err })).not.toContain(
      'secret-export-detail',
    )
  })
})

describe('readExportSheet error cells', () => {
  it('names only the row of an error cell, never its text', async () => {
    const file = await workbookFile((workbook) => {
      const sheet = workbook.addWorksheet('Sessions')
      sheet.addRow(['Id'])
      sheet.addRow([{ error: 'ada.example@example.org' as ExcelJS.CellErrorValue['error'] }])
    })

    const err = await readExportSheet(file).catch((e: unknown) => e)

    expect(err).toBeInstanceOf(ProviderContractError)
    expect((err as Error).message).toBe('sessionize export: row 2 has an error cell')
  })
})

describe('downloadSessionizeExport', () => {
  const path = SESSIONIZE_ALL_SESSIONS_PATH('90001')

  it('downloads the export as binary and returns the xlsx body', async () => {
    const file = await workbookFile((workbook) => workbook.addWorksheet('Sessions').addRow(['Id']))
    mockedRequest.mockResolvedValueOnce(reply(200, file))

    const body = await downloadSessionizeExport(staticSessionHttp(), path)

    expect(body.equals(file)).toBe(true)
    expect(mockedRequest).toHaveBeenCalledTimes(1)
    expect(mockedRequest.mock.calls[0][0]).toMatchObject({
      method: 'GET',
      url: 'https://sessionize.com/app/organizer/export/excel/90001/Flattened_sessions',
      responseType: 'arraybuffer',
    })
  })

  it.each([
    ['json', '{"error":"secret-export-detail"}', '7b226572'],
    ['empty', '', ''],
    ['plain text', 'secret-export-detail', '73656372'],
  ])('refuses a %s body without echoing it', async (_, text, hex) => {
    mockedRequest.mockResolvedValueOnce(reply(200, Buffer.from(text)))

    const err = await downloadSessionizeExport(staticSessionHttp(), path).catch((e: unknown) => e)

    expect(err).toBeInstanceOf(ProviderContractError)
    const message = (err as Error).message
    expect(message).toContain(`[${hex}]`)
    expect(JSON.stringify({ message, err })).not.toContain('secret-export-detail')
  })

  it('fails as an auth error after one renewal when sessionize keeps serving the sign-in page', async () => {
    const invalidateToken = vi.fn()
    mockedRequest.mockResolvedValue(reply(200, Buffer.from(SIGN_IN_PAGE)))

    const err = await downloadSessionizeExport(staticSessionHttp(invalidateToken), path).catch(
      (e: unknown) => e,
    )

    expect(err).toMatchObject({ errorClass: 'provider.auth' })
    expect(mockedRequest).toHaveBeenCalledTimes(2)
    expect(invalidateToken).toHaveBeenCalledTimes(2)
  })
  it('requests a failing export no more often than the attempts it is given', async () => {
    mockedRequest.mockResolvedValue(reply(503, Buffer.from('')))

    const err = await downloadSessionizeExport(staticSessionHttp(), path, 2).catch(
      (e: unknown) => e,
    )

    expect(err).toMatchObject({ errorClass: 'provider.unavailable' })
    expect(mockedRequest).toHaveBeenCalledTimes(2)
  })
})
