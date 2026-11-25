import ExcelJS from 'exceljs'

import type { ConnectorHttp } from '../../http/client'
import { ProviderContractError } from '../../http/errors'
import { SESSIONIZE_BASE_URL } from './session'

export const SESSIONIZE_ALL_SESSIONS_PATH = (eventKey: string) =>
  `/app/organizer/export/excel/${eventKey}/Flattened_sessions`

export type ExportCell = string | number | Date | null

export interface ExportSheet {
  sheetNames: string[]
  headers: string[]
  rows: ExportCell[][]
}

const ZIP_SIGNATURE = Buffer.from([0x50, 0x4b, 0x03, 0x04])

function contractError(message: string): ProviderContractError {
  return new ProviderContractError(`sessionize export: ${message}`)
}

function asBuffer(body: unknown): Buffer | null {
  if (Buffer.isBuffer(body)) {
    return body
  }
  if (body instanceof ArrayBuffer) {
    return Buffer.from(body)
  }
  return null
}

export async function downloadSessionizeExport(http: ConnectorHttp, path: string): Promise<Buffer> {
  const url = SESSIONIZE_BASE_URL + path
  const request = { method: 'GET', url }
  const body = asBuffer(await http.request<unknown>({ ...request, responseType: 'arraybuffer' }))
  if (!body) {
    throw contractError(`${path} body is not binary`).withContext({ request })
  }
  const signature = body.subarray(0, ZIP_SIGNATURE.length)
  if (!signature.equals(ZIP_SIGNATURE)) {
    throw contractError(
      `${path} is not an xlsx file, it starts with [${signature.toString('hex')}]`,
    ).withContext({ request })
  }
  return body
}

function normalizeCell(value: ExcelJS.CellValue): ExportCell {
  if (value === null || value === undefined) {
    return null
  }
  if (value instanceof Date || typeof value === 'number') {
    return value
  }
  if (typeof value === 'boolean') {
    return String(value)
  }
  if (typeof value === 'string') {
    const text = value.trim()
    return text === '' ? null : text
  }
  if ('richText' in value) {
    return normalizeCell(value.richText.map((run) => run.text).join(''))
  }
  if ('text' in value) {
    return normalizeCell(value.text)
  }
  if ('result' in value) {
    return normalizeCell(value.result)
  }
  if ('error' in value) {
    throw contractError(`cell holds the error ${value.error}`)
  }
  throw contractError('cell holds a formula without a result')
}

function readRow(row: ExcelJS.Row, width: number): ExportCell[] {
  return Array.from({ length: width }, (_, index) => normalizeCell(row.getCell(index + 1).value))
}

export async function readExportSheet(file: Buffer): Promise<ExportSheet> {
  const workbook = new ExcelJS.Workbook()
  await workbook.xlsx.load(file)
  const sheet = workbook.worksheets[0]
  if (!sheet) {
    throw contractError('workbook has no worksheet')
  }
  const headerRow = sheet.getRow(1)
  const width = headerRow.cellCount
  if (width === 0) {
    throw contractError(`worksheet "${sheet.name}" has no header row`)
  }
  const rows: ExportCell[][] = []
  for (let number = 2; number <= sheet.rowCount; number++) {
    const row = sheet.getRow(number)
    if (row.cellCount > width) {
      throw contractError(`row ${number} has cells beyond the ${width} header columns`)
    }
    const cells = readRow(row, width)
    if (cells.some((cell) => cell !== null)) {
      rows.push(cells)
    }
  }
  return {
    sheetNames: workbook.worksheets.map((worksheet) => worksheet.name),
    headers: readRow(headerRow, width).map((cell) => (cell === null ? '' : String(cell))),
    rows,
  }
}
