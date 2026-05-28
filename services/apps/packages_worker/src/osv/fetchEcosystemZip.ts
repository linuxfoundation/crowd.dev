import { createWriteStream } from 'node:fs'
import { mkdir, rm } from 'node:fs/promises'
import * as path from 'node:path'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import unzipper from 'unzipper'

import { FetchError, OsvRecord } from './types'

const DOWNLOAD_TIMEOUT_MS = 10 * 60 * 1000
const MAX_ENTRY_BYTES = 10 * 1024 * 1024 // 10 MB; real OSV entries are < 100 KB

async function downloadZip(url: string, target: string): Promise<void> {
  const ac = new AbortController()
  const timer = setTimeout(() => ac.abort(), DOWNLOAD_TIMEOUT_MS)

  // The timer covers headers + body. fetch() only awaits headers, so the
  // pipeline that streams the body to disk must run inside the same try/finally
  // — otherwise a stalled CDN connection mid-transfer would hang past the
  // daily window with no timeout protection.
  try {
    let response: Response
    try {
      response = await fetch(url, { signal: ac.signal })
    } catch (err) {
      throw new FetchError('NETWORK', `Failed to GET ${url}: ${(err as Error).message}`)
    }

    if (response.status === 404) {
      throw new FetchError('NOT_FOUND', `404 fetching ${url}`)
    }
    if (response.status >= 500) {
      throw new FetchError('TRANSIENT', `${response.status} fetching ${url}`)
    }
    if (response.status !== 200) {
      throw new FetchError('NETWORK', `${response.status} fetching ${url}`)
    }
    if (!response.body) {
      throw new FetchError('NETWORK', `Empty body for ${url}`)
    }

    try {
      await pipeline(Readable.fromWeb(response.body), createWriteStream(target))
    } catch (err) {
      // pipeline rejects with an AbortError when the timer fires mid-stream;
      // surface that as a transient network failure so withRetry will retry.
      throw new FetchError('NETWORK', `Stream failed for ${url}: ${(err as Error).message}`)
    }
  } finally {
    clearTimeout(timer)
  }
}

export interface OsvZipEntry {
  filename: string
  json: OsvRecord
}

// fetchEcosystemZip downloads <baseUrl>/<ecosystem>/all.zip into tmpDir/<ecosystem>/
// and yields each contained OSV record. Memory stays bounded: we stream the
// download to disk, then iterate the central directory and decompress one
// entry at a time. The full zip is held on disk for the duration of the sync
// pass; callers are expected to clear tmpDir at the start of each pass.
export async function* fetchEcosystemZip(
  baseUrl: string,
  ecosystem: string,
  tmpDir: string,
): AsyncIterable<OsvZipEntry> {
  const ecoDir = path.join(tmpDir, ecosystem)
  await mkdir(ecoDir, { recursive: true })
  const zipPath = path.join(ecoDir, 'all.zip')

  const url = `${baseUrl.replace(/\/$/, '')}/${ecosystem}/all.zip`

  try {
    await downloadZip(url, zipPath)

    let directory: unzipper.CentralDirectory
    try {
      directory = await unzipper.Open.file(zipPath)
    } catch (err) {
      throw new FetchError('PARSE', `Malformed zip from ${url}: ${(err as Error).message}`)
    }

    for (const file of directory.files) {
      if (file.type !== 'File') continue
      if (!file.path.toLowerCase().endsWith('.json')) continue

      const buffer = await file.buffer()
      // Real OSV entries are well under 100 KB. A 10 MB cap is ~200x the
      // observed max and catches the (admittedly unlikely) case where a bad
      // upstream record or a zip-bomb-style payload would otherwise cause the
      // worker to OOM on file.buffer(). We surface it as PARSE so withRetry
      // gives up immediately — retrying the same payload won't help.
      if (buffer.length > MAX_ENTRY_BYTES) {
        throw new FetchError(
          'PARSE',
          `Entry ${ecosystem}/${file.path} exceeds ${MAX_ENTRY_BYTES} bytes (got ${buffer.length})`,
        )
      }
      let json: OsvRecord
      try {
        json = JSON.parse(buffer.toString('utf8')) as OsvRecord
      } catch (err) {
        throw new FetchError(
          'PARSE',
          `Bad JSON in ${ecosystem}/${file.path}: ${(err as Error).message}`,
        )
      }
      yield { filename: file.path, json }
    }
  } finally {
    // Best-effort cleanup; leave the directory if it fails so the next pass
    // can investigate.
    await rm(zipPath, { force: true }).catch(() => {
      /* best-effort cleanup; ignore failure */
    })
  }
}
