import ExcelJS from 'exceljs'

import type { ConnectorHttp } from '../../http/client'
import { ProviderContractError } from '../../http/errors'
import type { ConnectorErrorOptions } from '../../http/errors'
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

function contractError(message: string, options?: ConnectorErrorOptions): ProviderContractError {
  return new ProviderContractError(`sessionize export: ${message}`, options)
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

export async function downloadSessionizeExport(
  http: ConnectorHttp,
  path: string,
  maxAttempts?: number,
): Promise<Buffer> {
  const url = SESSIONIZE_BASE_URL + path
  const request = { method: 'GET', url }
  const body = asBuffer(
    await http.request<unknown>(
      { ...request, responseType: 'arraybuffer' },
      undefined,
      maxAttempts,
    ),
  )
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

function normalizeCell(value: ExcelJS.CellValue, rowNumber: number): ExportCell {
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
    return normalizeCell(value.richText.map((run) => run.text).join(''), rowNumber)
  }
  if ('text' in value) {
    return normalizeCell(value.text, rowNumber)
  }
  if ('result' in value) {
    return normalizeCell(value.result, rowNumber)
  }
  if ('error' in value) {
    throw contractError(`row ${rowNumber} has an error cell`)
  }
  throw contractError('cell holds a formula without a result')
}

function readCells(row: ExcelJS.Row, from: number, to: number): ExportCell[] {
  return Array.from({ length: Math.max(to - from + 1, 0) }, (_, index) =>
    normalizeCell(row.getCell(from + index).value, row.number),
  )
}

function headerWidth(headerRow: ExcelJS.Row): number {
  const cells = readCells(headerRow, 1, headerRow.cellCount)
  let width = cells.length
  while (width > 0 && cells[width - 1] === null) {
    width--
  }
  return width
}

async function loadWorkbook(file: Buffer): Promise<ExcelJS.Workbook> {
  const workbook = new ExcelJS.Workbook()
  try {
    await workbook.xlsx.load(file)
  } catch (err) {
    throw contractError('file is not a readable xlsx workbook', {
      cause: { name: err instanceof Error ? err.name : typeof err },
    })
  }
  return workbook
}

export async function readExportSheet(file: Buffer): Promise<ExportSheet> {
  const workbook = await loadWorkbook(file)
  const sheet = workbook.worksheets[0]
  if (!sheet) {
    throw contractError('workbook has no worksheet')
  }
  const headerRow = sheet.getRow(1)
  const width = headerWidth(headerRow)
  const rows: ExportCell[][] = []
  for (let number = 2; number <= sheet.rowCount; number++) {
    const row = sheet.getRow(number)
    if (readCells(row, width + 1, row.cellCount).some((cell) => cell !== null)) {
      throw contractError(`row ${number} has values beyond the ${width} header columns`)
    }
    const cells = readCells(row, 1, width)
    if (cells.some((cell) => cell !== null)) {
      rows.push(cells)
    }
  }
  return {
    sheetNames: workbook.worksheets.map((worksheet) => worksheet.name),
    headers: readCells(headerRow, 1, width).map((cell) => (cell === null ? '' : String(cell))),
    rows,
  }
}
