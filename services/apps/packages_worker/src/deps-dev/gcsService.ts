import { ParquetEnvelopeReader, ParquetReader } from '@dsnp/parquetjs'

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

// BQ NUMERIC columns are exported as FIXED_LEN_BYTE_ARRAY (16-byte big-endian two's complement, scale=9).
// BQ FLOAT64 comes through as a JS number — this only fires for Buffer values.
export function toParquetDecimal(val: unknown, scale = 9): number | null {
  if (val == null) return null
  if (typeof val === 'number') return val
  if (typeof val === 'bigint') return Number(val) / Math.pow(10, scale)
  if (Buffer.isBuffer(val)) {
    let bigVal = BigInt(0)
    for (const byte of val) bigVal = (bigVal << BigInt(8)) | BigInt(byte)
    const bits = BigInt(val.length * 8)
    if (bigVal >= BigInt(1) << (bits - BigInt(1))) bigVal -= BigInt(1) << bits
    return Number(bigVal) / Math.pow(10, scale)
  }
  return null
}

// Read row count from the parquet footer only — no full file download.
// Two GCS range reads: last 8 bytes (trailer) + footer bytes (~1-50 KB).
export async function readParquetRowCount(objectName: string, fileSize: number): Promise<number> {
  const file = bucket.file(objectName)
  const readFn = async (offset: number, length: number): Promise<Buffer> => {
    const [buf] = await file.download({ start: offset, end: offset + length - 1 })
    return buf
  }
  const reader = new ParquetEnvelopeReader(readFn, () => undefined, fileSize)
  const metadata = await reader.readFooter()
  // parquetjs uses Int64 (Thrift) for num_rows; unary + converts it to JS number.
  return (metadata.row_groups as unknown as Array<{ num_rows: unknown }>).reduce(
    (sum, rg) => sum + +(rg.num_rows as number),
    0,
  )
}

export async function* streamParquetRowsFromGcs(
  objectName: string,
): AsyncGenerator<Record<string, unknown>> {
  const [buffer] = await bucket.file(objectName).download()
  const reader = await ParquetReader.openBuffer(buffer)
  const cursor = reader.getCursor()
  try {
    let row: Record<string, unknown> | null = null
    while ((row = (await cursor.next()) as Record<string, unknown> | null) != null) {
      yield row
    }
  } finally {
    await reader.close()
  }
}
