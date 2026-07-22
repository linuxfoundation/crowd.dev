import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { Readable, Transform, Writable } from 'stream'
import type { ReadableStream as NodeWebReadableStream } from 'stream/web'
import * as tar from 'tar'

// Tarball content here is third-party (dependent packages), not something we
// control — bound both the download and the extraction so a huge or maliciously
// crafted archive (decompression bomb) can't exhaust a worker's disk/CPU.
const FETCH_TIMEOUT_MS = 2 * 60 * 1000
const MAX_DOWNLOAD_BYTES = 200 * 1024 * 1024
const MAX_EXTRACTED_BYTES = 500 * 1024 * 1024
const MAX_EXTRACTED_FILES = 20_000

// Downloads an npm tarball and extracts it into destDir, stripping the
// package's own top-level directory (npm tarballs are always wrapped in a
// single `package/` dir) if all entries share one. Guards against path
// traversal since tarball content here is third-party (dependent packages),
// not something we control.
export async function downloadAndExtractTarball(
  tarballUrl: string,
  destDir: string,
): Promise<void> {
  fs.mkdirSync(destDir, { recursive: true })

  const controller = new AbortController()
  const timeoutHandle = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)

  try {
    const res = await fetch(tarballUrl, { signal: controller.signal })
    if (!res.ok) {
      throw new Error(`Failed to fetch tarball: ${res.status} ${res.statusText}`)
    }
    if (!res.body) {
      throw new Error('No response body from tarball fetch')
    }

    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tarball-extract-'))
    try {
      // tar rejects (preservePaths defaults to false) any entry whose path would resolve
      // outside cwd; strict:true makes that a thrown error instead of a silent warn+skip.
      let extractedBytes = 0
      let extractedFiles = 0
      const extractStream = tar.extract({
        cwd: tempDir,
        strict: true,
        filter: (_entryPath, entry) => {
          extractedFiles++
          extractedBytes += entry.size ?? 0
          if (extractedFiles > MAX_EXTRACTED_FILES || extractedBytes > MAX_EXTRACTED_BYTES) {
            ;(extractStream as unknown as Writable).destroy(
              new Error('Tarball extraction exceeded size/file limits'),
            )
            return false
          }
          return true
        },
      })

      let downloadedBytes = 0
      const downloadLimiter = new Transform({
        transform(chunk, _encoding, callback) {
          downloadedBytes += chunk.length
          if (downloadedBytes > MAX_DOWNLOAD_BYTES) {
            callback(new Error('Tarball download exceeded size limit'))
            return
          }
          callback(null, chunk)
        },
      })

      await new Promise<void>((resolve, reject) => {
        Readable.fromWeb(res.body as unknown as NodeWebReadableStream<Uint8Array>)
          .on('error', reject)
          .pipe(downloadLimiter)
          .on('error', reject)
          .pipe(extractStream as unknown as NodeJS.WritableStream)
          .on('finish', resolve)
          .on('error', reject)
      })

      const items = fs.readdirSync(tempDir)
      const commonPrefix =
        items.length === 1 && fs.statSync(path.join(tempDir, items[0])).isDirectory()
          ? items[0]
          : null
      const srcBase = commonPrefix ? path.join(tempDir, commonPrefix) : tempDir

      copyTreeGuarded(srcBase, destDir)
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true })
    }
  } finally {
    clearTimeout(timeoutHandle)
  }
}

// Copies every file under srcBase into destDir, rejecting any entry whose
// resolved path would land outside destDir.
function copyTreeGuarded(srcBase: string, destDir: string): void {
  const resolvedDest = path.resolve(destDir)
  const entries = fs.readdirSync(srcBase, { recursive: true }) as string[]

  for (const entry of entries) {
    const srcPath = path.join(srcBase, entry)
    const destPath = path.resolve(destDir, entry)
    if (destPath !== resolvedDest && !destPath.startsWith(resolvedDest + path.sep)) {
      continue
    }

    const stat = fs.statSync(srcPath)
    if (stat.isDirectory()) continue

    fs.mkdirSync(path.dirname(destPath), { recursive: true })
    fs.copyFileSync(srcPath, destPath)
  }
}
