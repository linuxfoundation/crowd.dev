import { createWriteStream, mkdirSync, rmSync, writeFileSync } from 'fs'
import * as path from 'path'
import { Readable } from 'stream'
import { pipeline } from 'stream/promises'
import type { ReadableStream as NodeWebReadableStream } from 'stream/web'
import unzipper from 'unzipper'

import { escapeModulePath } from '../../go/proxyClient'

import {
  FETCH_TIMEOUT_MS,
  MAX_EXTRACTED_BYTES,
  MAX_EXTRACTED_FILES,
  createDownloadLimiter,
} from './downloadLimits'

const BASE = process.env.GO_PROXY_BASE_URL ?? 'https://proxy.golang.org'

// Zip's central directory sits at the end of the file, so unlike tar we can't
// stream-extract incrementally — download to a scratch file first, then extract.
export async function downloadAndExtractGoModule(
  module: string,
  version: string,
  destDir: string,
): Promise<void> {
  mkdirSync(destDir, { recursive: true })
  const zipPath = `${destDir}.zip`
  const url = `${BASE}/${escapeModulePath(module)}/@v/${escapeModulePath(version)}.zip`

  const controller = new AbortController()
  const timeoutHandle = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)

  try {
    let res: Response
    try {
      res = await fetch(url, { signal: controller.signal })
    } catch (e) {
      throw new Error(`Failed to fetch go module zip: ${(e as Error).message}`)
    }
    if (!res.ok) {
      throw new Error(`Failed to fetch go module zip: ${res.status} ${res.statusText}`)
    }
    if (!res.body) {
      throw new Error('No response body from go module zip fetch')
    }

    const downloadLimiter = createDownloadLimiter('Go module zip download exceeded size limit')

    try {
      await pipeline(
        Readable.fromWeb(res.body as unknown as NodeWebReadableStream<Uint8Array>),
        downloadLimiter,
        createWriteStream(zipPath),
      )

      let directory: unzipper.CentralDirectory
      try {
        directory = await unzipper.Open.file(zipPath)
      } catch (err) {
        throw new Error(`Malformed go module zip from ${url}: ${(err as Error).message}`)
      }

      const prefix = `${module}@${version}/`
      let extractedBytes = 0
      let extractedFiles = 0

      for (const entry of directory.files) {
        if (entry.type !== 'File') continue
        if (!entry.path.startsWith(prefix)) continue // skip anything outside the module's own wrapper dir

        const relativePath = entry.path.slice(prefix.length)
        if (!relativePath) continue

        // GOPROXY zips are well-formed but content originates from a third-party
        // module — guard path traversal defensively rather than trust the archive
        // (tar's preservePaths:false has no zip equivalent in this library).
        const resolvedPath = path.resolve(destDir, relativePath)
        if (resolvedPath !== destDir && !resolvedPath.startsWith(destDir + path.sep)) {
          throw new Error(`Go module zip entry escapes destination dir: ${entry.path}`)
        }

        extractedFiles++
        extractedBytes += entry.uncompressedSize ?? 0
        if (extractedFiles > MAX_EXTRACTED_FILES || extractedBytes > MAX_EXTRACTED_BYTES) {
          throw new Error('Go module zip extraction exceeded size/file limits')
        }

        mkdirSync(path.dirname(resolvedPath), { recursive: true })
        const content = await entry.buffer()
        writeFileSync(resolvedPath, new Uint8Array(content))
      }
    } finally {
      rmSync(zipPath, { force: true })
    }
  } finally {
    clearTimeout(timeoutHandle)
  }
}
