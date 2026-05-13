import https from 'https'

const BUCKET_URL = 'https://commondatastorage.googleapis.com/ossf-criticality-score'

function httpsGet(url: string): Promise<string> {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        if (
          res.statusCode &&
          res.statusCode >= 300 &&
          res.statusCode < 400 &&
          res.headers.location
        ) {
          httpsGet(res.headers.location).then(resolve, reject)
          return
        }

        if (res.statusCode && (res.statusCode < 200 || res.statusCode >= 300)) {
          reject(new Error(`HTTP ${res.statusCode} for ${url}`))
          return
        }

        const chunks: Uint8Array[] = []
        res.on('data', (chunk: Uint8Array) => chunks.push(chunk))
        res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')))
        res.on('error', reject)
      })
      .on('error', reject)
  })
}

function extractPrefixes(xml: string): string[] {
  const prefixes: string[] = []
  const regex = /<Prefix>([^<]+)<\/Prefix>/g
  let match: RegExpExecArray | null

  while ((match = regex.exec(xml)) !== null) {
    prefixes.push(match[1])
  }

  return prefixes
}

/**
 * List all date prefixes in the OSSF Criticality Score bucket.
 * Returns prefixes like ['2024.07.01/', '2024.07.08/', ...]
 */
export async function listDatePrefixes(): Promise<string[]> {
  const xml = await httpsGet(`${BUCKET_URL}?delimiter=/`)
  return extractPrefixes(xml).filter((p) => /^\d{4}\.\d{2}\.\d{2}\/$/.test(p))
}

/**
 * List time sub-prefixes for a given date prefix.
 * E.g., for '2024.07.01/' returns ['2024.07.01/060102/', ...]
 */
export async function listTimePrefixes(datePrefix: string): Promise<string[]> {
  const xml = await httpsGet(`${BUCKET_URL}?prefix=${encodeURIComponent(datePrefix)}&delimiter=/`)
  return extractPrefixes(xml).filter((p) => p !== datePrefix)
}

/**
 * Build the full URL for the all.csv file within a given dataset prefix.
 */
export function buildDatasetUrl(prefix: string): string {
  return `${BUCKET_URL}/${prefix}all.csv`
}

/**
 * Get an HTTPS readable stream for a given URL.
 */
export function getHttpsStream(url: string): Promise<NodeJS.ReadableStream> {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        if (
          res.statusCode &&
          res.statusCode >= 300 &&
          res.statusCode < 400 &&
          res.headers.location
        ) {
          getHttpsStream(res.headers.location).then(resolve, reject)
          return
        }

        if (res.statusCode && (res.statusCode < 200 || res.statusCode >= 300)) {
          reject(new Error(`HTTP ${res.statusCode} for ${url}`))
          return
        }

        resolve(res)
      })
      .on('error', reject)
  })
}
