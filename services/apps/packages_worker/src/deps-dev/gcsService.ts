import { ParquetReader } from '@dsnp/parquetjs'

import { bucket } from './config'

// BQ exports TIMESTAMP columns as INT64 microseconds since Unix epoch.
// Parquetjs surfaces these as BigInt. Never pass raw BigInt to pg — convert here.
export function toTimestampDate(val: unknown): Date | null {
  if (val == null) return null
  if (typeof val === 'bigint') return new Date(Number(val) / 1000)
  if (typeof val === 'number') return new Date(val / 1000)
  return null
}

// Parquet serializes some integer columns as fixed-width big-endian Buffers (e.g. Snowflake NUMBER).
// Handle both plain number and Buffer cases.
export function toParquetInt(val: unknown): number | null {
  if (val == null) return null
  if (typeof val === 'number') return val
  if (typeof val === 'bigint') return Number(val)
  if (Buffer.isBuffer(val)) {
    let result = 0
    for (const byte of val) result = result * 256 + byte
    return result
  }
  return null
}

export async function* streamParquetRowsFromGcs(
  objectName: string,
): AsyncGenerator<Record<string, unknown>> {
  const [buffer] = await bucket.file(objectName).download()
  const reader = await ParquetReader.openBuffer(buffer)
  const cursor = reader.getCursor()
  try {
    let row: Record<string, unknown> | null = null
    while ((row = (await cursor.next()) as Record<string, unknown> | null) !== null) {
      yield row
    }
  } finally {
    await reader.close()
  }
}
