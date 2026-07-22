import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { Readable } from 'stream'
import type { ReadableStream as NodeWebReadableStream } from 'stream/web'
import * as tar from 'tar'

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

  const res = await fetch(tarballUrl)
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
    const extractStream = tar.extract({
      cwd: tempDir,
      strict: true,
    }) as unknown as NodeJS.WritableStream
    await new Promise<void>((resolve, reject) => {
      Readable.fromWeb(res.body as unknown as NodeWebReadableStream<Uint8Array>)
        .on('error', reject)
        .pipe(extractStream)
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
