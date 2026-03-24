/**
 * S3 data consumption logic.
 *
 * Responsible for reading exported files from S3
 * (e.g., Parquet manifests or raw data) for downstream transformation.
 */
import { DeleteObjectCommand, GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { ParquetReader } from '@dsnp/parquetjs'

export interface S3Object {
  key: string
  size: number
  lastModified: Date
}

export class S3Service {
  private readonly s3: S3Client

  constructor() {
    const accessKeyId = process.env.CROWD_SNOWFLAKE_S3_ACCESS_KEY_ID
    const secretAccessKey = process.env.CROWD_SNOWFLAKE_S3_SECRET_ACCESS_KEY
    if (!accessKeyId || !secretAccessKey) {
      throw new Error(
        'Missing required env vars CROWD_SNOWFLAKE_S3_ACCESS_KEY_ID / CROWD_SNOWFLAKE_S3_SECRET_ACCESS_KEY',
      )
    }

    this.s3 = new S3Client({
      region: process.env.CROWD_SNOWFLAKE_S3_REGION,
      credentials: { accessKeyId, secretAccessKey },
    })
  }

  async downloadFile(s3Uri: string): Promise<Buffer> {
    const { bucket, key } = this.parseS3Uri(s3Uri)
    const response = await this.s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }))
    if (!response.Body) {
      throw new Error(`Empty response body for ${s3Uri}`)
    }
    const byteArray = await response.Body.transformToByteArray()
    return Buffer.from(byteArray)
  }

  async readParquetRows(s3Uri: string): Promise<Record<string, unknown>[]> {
    const buffer = await this.downloadFile(s3Uri)
    const reader = await ParquetReader.openBuffer(buffer)
    const cursor = reader.getCursor()
    const rows: Record<string, unknown>[] = []
    let row: Record<string, unknown> | null = null
    while ((row = (await cursor.next()) as Record<string, unknown> | null) !== null) {
      rows.push(row)
    }
    await reader.close()
    return rows
  }

  async deleteFile(s3Uri: string): Promise<void> {
    const { bucket, key } = this.parseS3Uri(s3Uri)
    await this.s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }))
  }

  private parseS3Uri(s3Uri: string): { bucket: string; key: string } {
    const url = new URL(s3Uri)
    return { bucket: url.hostname, key: url.pathname.slice(1) }
  }
}
