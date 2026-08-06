import * as fs from 'fs'
import { Readable, Writable } from 'stream'
import type { ReadableStream as NodeWebReadableStream } from 'stream/web'
import * as tar from 'tar'

import {
  FETCH_TIMEOUT_MS,
  MAX_EXTRACTED_BYTES,
  MAX_EXTRACTED_FILES,
  createDownloadLimiter,
} from './downloadLimits'

// Downloads an npm tarball and extracts it directly into destDir, stripping
// the package's own top-level `package/` wrapper directory (npm tarballs are
// always wrapped in one). Guards against path traversal since tarball content
// here is third-party (dependent packages), not something we control.
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

    // tar rejects (preservePaths defaults to false) any entry whose path would resolve
    // outside cwd; strict:true makes that a thrown error instead of a silent warn+skip.
    // strip:1 drops the tarball's own `package/` wrapper directly during extraction —
    // npm tarballs are always wrapped in one, so this avoids a second synchronous
    // extract-to-tempdir-then-copy-every-file pass (that pass was the CPU/disk-I/O
    // bottleneck under concurrent load: see blast_radius_load_tests.xlsx Stage 1).
    let extractedBytes = 0
    let extractedFiles = 0
    const extractStream = tar.extract({
      cwd: destDir,
      strict: true,
      strip: 1,
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

    const downloadLimiter = createDownloadLimiter('Tarball download exceeded size limit')

    await new Promise<void>((resolve, reject) => {
      Readable.fromWeb(res.body as unknown as NodeWebReadableStream<Uint8Array>)
        .on('error', reject)
        .pipe(downloadLimiter)
        .on('error', reject)
        .pipe(extractStream as unknown as NodeJS.WritableStream)
        .on('finish', resolve)
        .on('error', reject)
    })
  } finally {
    clearTimeout(timeoutHandle)
  }
}
