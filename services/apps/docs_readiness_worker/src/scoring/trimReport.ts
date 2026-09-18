import type { CheckStatus, ICheckResult } from './types'

const MAX_DETAILS_BYTES = 32 * 1024

export interface ITrimmedCheckRow {
  checkId: string
  category: string
  status: CheckStatus
  message: string
  details: string | null
}

function capDetails(details: Record<string, unknown> | undefined): string | null {
  if (details === undefined) {
    return null
  }
  const json = JSON.stringify(details)
  const buf = Buffer.from(json, 'utf-8')
  if (buf.byteLength <= MAX_DETAILS_BYTES) {
    return json
  }
  return buf.subarray(0, MAX_DETAILS_BYTES).toString('utf-8')
}

export function trimReport(results: ICheckResult[]): ITrimmedCheckRow[] {
  return results.map((result) => ({
    checkId: result.id,
    category: result.category,
    status: result.status,
    message: result.message,
    details: capDetails(result.details),
  }))
}
