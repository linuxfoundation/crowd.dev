import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { Readable } from 'stream'
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

// rubygems.org omits the platform suffix only for the default 'ruby' platform; every
// other platform (java, x86_64-linux, ...) requires it, and some gems/versions are only
// ever published for a non-'ruby' platform.
function gemDownloadUrl(packageName: string, version: string, platform?: string | null): string {
  const suffix = platform && platform !== 'ruby' ? `-${platform}` : ''
  return `https://rubygems.org/downloads/${packageName}-${version}${suffix}.gem`
}

// A .gem is an uncompressed POSIX tar wrapping metadata.gz/data.tar.gz/checksums.yaml.gz —
// the actual source lives one level deeper, inside data.tar.gz, with no wrapper directory.
export async function downloadAndExtractRubyGemsSource(
  packageName: string,
  version: string,
  destDir: string,
  platform?: string | null,
): Promise<void> {
  fs.mkdirSync(destDir, { recursive: true })

  const url = gemDownloadUrl(packageName, version, platform)
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
          outerExtract.abort(new Error('Gem exceeded size/file limits'))
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
      onentry: (entry) => {
        innerExtractedFiles++
        innerExtractedBytes += entry.size ?? 0
        if (
          innerExtractedFiles > MAX_EXTRACTED_FILES ||
          innerExtractedBytes > MAX_EXTRACTED_BYTES
        ) {
          innerExtract.abort(new Error('Gem exceeded size/file limits'))
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
