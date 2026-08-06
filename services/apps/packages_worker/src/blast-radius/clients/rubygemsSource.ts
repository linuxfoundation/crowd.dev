import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { Readable, Writable } from 'stream'
import type { ReadableStream as NodeWebReadableStream } from 'stream/web'
import * as tar from 'tar'

import {
  FETCH_TIMEOUT_MS,
  MAX_EXTRACTED_BYTES,
  MAX_EXTRACTED_FILES,
  createDownloadLimiter,
} from './downloadLimits'

// Thrown when no downloadable gem exists for this name/version — the reachability
// stage turns this into a clean "no source" verdict rather than a retry.
export class RubyGemsSourceNotFoundError extends Error {
  constructor(packageName: string, version: string) {
    super(`No downloadable gem for ${packageName}@${version}`)
    this.name = 'RubyGemsSourceNotFoundError'
  }
}

function gemDownloadUrl(packageName: string, version: string): string {
  return `https://rubygems.org/downloads/${packageName}-${version}.gem`
}

// A .gem is an uncompressed POSIX tar containing metadata.gz, data.tar.gz and
// checksums.yaml.gz — unlike npm/cargo, the actual source lives one level deeper, inside
// data.tar.gz, whose entries sit at the archive root with no wrapper directory.
export async function downloadAndExtractRubyGemsSource(
  packageName: string,
  version: string,
  destDir: string,
): Promise<void> {
  fs.mkdirSync(destDir, { recursive: true })

  const url = gemDownloadUrl(packageName, version)
  const controller = new AbortController()
  const timeoutHandle = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)

  const outerDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gemouter-'))

  try {
    let res: Response
    try {
      res = await fetch(url, { signal: controller.signal })
    } catch {
      throw new RubyGemsSourceNotFoundError(packageName, version)
    }
    if (res.status === 404) {
      throw new RubyGemsSourceNotFoundError(packageName, version)
    }
    if (!res.ok || !res.body) {
      throw new Error(`Failed to fetch gem: ${res.status} ${res.statusText}`)
    }

    let outerExtractedFiles = 0
    let outerExtractedBytes = 0
    const outerExtract = tar.extract({
      cwd: outerDir,
      strict: true,
      filter: (entryPath) => entryPath === 'data.tar.gz',
      onentry: (entry) => {
        outerExtractedFiles++
        outerExtractedBytes += entry.size ?? 0
        if (
          outerExtractedFiles > MAX_EXTRACTED_FILES ||
          outerExtractedBytes > MAX_EXTRACTED_BYTES
        ) {
          ;(outerExtract as unknown as Writable).destroy(new Error('Gem exceeded size/file limits'))
        }
      },
    })

    await new Promise<void>((resolve, reject) => {
      Readable.fromWeb(res.body as unknown as NodeWebReadableStream<Uint8Array>)
        .on('error', reject)
        .pipe(createDownloadLimiter('Gem download exceeded size limit'))
        .on('error', reject)
        .pipe(outerExtract as unknown as NodeJS.WritableStream)
        .on('finish', resolve)
        .on('error', reject)
    })

    const dataTarPath = path.join(outerDir, 'data.tar.gz')
    if (!fs.existsSync(dataTarPath)) {
      throw new RubyGemsSourceNotFoundError(packageName, version)
    }

    let innerExtractedFiles = 0
    let innerExtractedBytes = 0
    const innerExtract = tar.extract({
      cwd: destDir,
      strict: true,
      filter: () => {
        innerExtractedFiles++
        if (innerExtractedFiles > MAX_EXTRACTED_FILES) return false
        return true
      },
      onentry: (entry) => {
        innerExtractedBytes += entry.size ?? 0
        if (innerExtractedBytes > MAX_EXTRACTED_BYTES) {
          ;(innerExtract as unknown as Writable).destroy(new Error('Gem exceeded size/file limits'))
        }
      },
    })

    await new Promise<void>((resolve, reject) => {
      fs.createReadStream(dataTarPath)
        .on('error', reject)
        .pipe(innerExtract as unknown as NodeJS.WritableStream)
        .on('finish', resolve)
        .on('error', reject)
    })
  } finally {
    clearTimeout(timeoutHandle)
    fs.rmSync(outerDir, { recursive: true, force: true })
  }
}
