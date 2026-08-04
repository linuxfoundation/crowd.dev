import { createWriteStream, mkdirSync, rmSync } from 'fs'
import * as path from 'path'
import { Readable } from 'stream'
import { pipeline } from 'stream/promises'
import type { ReadableStream as NodeWebReadableStream } from 'stream/web'
import unzipper from 'unzipper'

import { resolveBlastRadiusMavenBaseUrl } from '../../maven/registry'

import {
  FETCH_TIMEOUT_MS,
  MAX_EXTRACTED_BYTES,
  MAX_EXTRACTED_FILES,
  createDownloadLimiter,
} from './downloadLimits'

// Thrown when the artifact has no `-sources.jar` published (404) — a common, expected
// case (many artifacts only ship a binary jar), distinct from a transient fetch failure.
export class MavenSourcesNotFoundError extends Error {
  constructor(url: string) {
    super(`No sources jar published at ${url}`)
    this.name = 'MavenSourcesNotFoundError'
  }
}

// Jars are zips — same central-directory-at-the-end constraint as Go module zips means
// we can't stream-extract incrementally either; download to a scratch file first.
export async function downloadAndExtractMavenSources(
  groupId: string,
  artifactId: string,
  version: string,
  destDir: string,
): Promise<void> {
  mkdirSync(destDir, { recursive: true })
  const jarPath = `${destDir}.jar`
  const groupPath = groupId.replace(/\./g, '/')
  const url = `${resolveBlastRadiusMavenBaseUrl(groupId)}/${groupPath}/${artifactId}/${version}/${artifactId}-${version}-sources.jar`

  const controller = new AbortController()
  const timeoutHandle = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)

  try {
    let res: Response
    try {
      res = await fetch(url, { signal: controller.signal })
    } catch (e) {
      throw new Error(`Failed to fetch Maven sources jar: ${(e as Error).message}`)
    }
    if (res.status === 404) {
      throw new MavenSourcesNotFoundError(url)
    }
    if (!res.ok) {
      throw new Error(`Failed to fetch Maven sources jar: ${res.status} ${res.statusText}`)
    }
    if (!res.body) {
      throw new Error('No response body from Maven sources jar fetch')
    }

    const downloadLimiter = createDownloadLimiter('Maven sources jar download exceeded size limit')

    try {
      await pipeline(
        Readable.fromWeb(res.body as unknown as NodeWebReadableStream<Uint8Array>),
        downloadLimiter,
        createWriteStream(jarPath),
      )

      let directory: unzipper.CentralDirectory
      try {
        directory = await unzipper.Open.file(jarPath)
      } catch (err) {
        throw new Error(`Malformed Maven sources jar from ${url}: ${(err as Error).message}`)
      }

      const extractedByteCounter = { bytes: 0 }
      let extractedFiles = 0

      for (const entry of directory.files) {
        if (entry.type !== 'File') continue
        if (!entry.path) continue

        // Sources jars are third-party content — guard path traversal defensively
        // rather than trust the archive (same rationale as goModuleZip.ts).
        const resolvedPath = path.resolve(destDir, entry.path)
        if (resolvedPath !== destDir && !resolvedPath.startsWith(destDir + path.sep)) {
          throw new Error(`Maven sources jar entry escapes destination dir: ${entry.path}`)
        }

        extractedFiles++
        if (extractedFiles > MAX_EXTRACTED_FILES) {
          throw new Error('Maven sources jar extraction exceeded size/file limits')
        }

        mkdirSync(path.dirname(resolvedPath), { recursive: true })

        // Stream to disk and count actual decompressed bytes as they flow, rather than
        // trusting entry.uncompressedSize or materializing the whole entry in memory.
        const extractionLimiter = createDownloadLimiter(
          'Maven sources jar extraction exceeded size/file limits',
          MAX_EXTRACTED_BYTES,
          extractedByteCounter,
        )

        await pipeline(entry.stream(), extractionLimiter, createWriteStream(resolvedPath))
      }
    } finally {
      rmSync(jarPath, { force: true })
    }
  } finally {
    clearTimeout(timeoutHandle)
  }
}
