import { createWriteStream, mkdirSync, rmSync } from 'fs'
import * as path from 'path'
import { Readable } from 'stream'
import { pipeline } from 'stream/promises'
import type { ReadableStream as NodeWebReadableStream } from 'stream/web'
import * as tar from 'tar'
import unzipper from 'unzipper'

import type { PyPiProject, PyPiUrlInfo } from '../../pypi/types'

import {
  FETCH_TIMEOUT_MS,
  MAX_EXTRACTED_BYTES,
  MAX_EXTRACTED_FILES,
  createDownloadLimiter,
} from './downloadLimits'

// Thrown when no downloadable sdist/wheel exists for this name/version — the reachability
// stage turns this into a clean "no source" verdict rather than a retry.
export class PypiSourceNotFoundError extends Error {
  constructor(packageName: string, version: string) {
    super(`No downloadable sdist or wheel for ${packageName}@${version}`)
    this.name = 'PypiSourceNotFoundError'
  }
}

async function fetchVersionProject(name: string, version: string): Promise<PyPiProject> {
  const url = `https://pypi.org/pypi/${encodeURIComponent(name)}/${encodeURIComponent(version)}/json`
  const controller = new AbortController()
  const timeoutHandle = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)

  try {
    let res: Response
    try {
      res = await fetch(url, { signal: controller.signal })
    } catch {
      throw new PypiSourceNotFoundError(name, version)
    }
    if (res.status === 404) {
      throw new PypiSourceNotFoundError(name, version)
    }
    if (!res.ok) {
      throw new Error(
        `Failed to fetch PyPI project ${name}@${version}: ${res.status} ${res.statusText}`,
      )
    }
    return (await res.json()) as PyPiProject
  } finally {
    clearTimeout(timeoutHandle)
  }
}

function selectDistribution(urls: PyPiUrlInfo[] | undefined): PyPiUrlInfo | null {
  if (!urls || urls.length === 0) return null
  const sdist = urls.find((u) => u.packagetype === 'sdist')
  if (sdist) return sdist
  return urls.find((u) => u.packagetype === 'bdist_wheel') ?? null
}

// A sdist tarball wraps a single "name-version/" directory — strip:1 drops it so
// destDir ends up holding the package contents directly.
async function downloadSdist(
  url: string,
  destDir: string,
  packageName: string,
  version: string,
): Promise<void> {
  const controller = new AbortController()
  const timeoutHandle = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)

  try {
    let res: Response
    try {
      res = await fetch(url, { signal: controller.signal })
    } catch (e) {
      throw new Error(
        `Failed to fetch sdist for ${packageName}@${version}: ${(e as Error).message}`,
      )
    }
    if (!res.ok || !res.body) {
      throw new Error(
        `Failed to fetch sdist for ${packageName}@${version}: ${res.status} ${res.statusText}`,
      )
    }

    let extractedFiles = 0
    let extractedBytes = 0
    const extract = tar.extract({
      cwd: destDir,
      strip: 1,
      strict: true,
      onentry: (entry) => {
        extractedFiles++
        extractedBytes += entry.size ?? 0
        if (extractedFiles > MAX_EXTRACTED_FILES || extractedBytes > MAX_EXTRACTED_BYTES) {
          extract.abort(new Error('sdist exceeded size/file limits'))
        }
      },
    })

    await new Promise<void>((resolve, reject) => {
      Readable.fromWeb(res.body as unknown as NodeWebReadableStream<Uint8Array>)
        .on('error', reject)
        .pipe(createDownloadLimiter('sdist download exceeded size limit'))
        .on('error', reject)
        .pipe(extract as unknown as NodeJS.WritableStream)
        .on('finish', resolve)
        .on('error', reject)
    })
  } finally {
    clearTimeout(timeoutHandle)
  }
}

// A .whl is a zip with no wrapper directory. Zip's central directory sits at the end of
// the file, so unlike tar we can't stream-extract incrementally — download to a scratch
// file first, then extract, same as goModuleZip.ts.
async function downloadWheel(
  url: string,
  destDir: string,
  packageName: string,
  version: string,
): Promise<void> {
  const controller = new AbortController()
  const timeoutHandle = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)
  const zipPath = `${destDir}.whl`

  try {
    let res: Response
    try {
      res = await fetch(url, { signal: controller.signal })
    } catch (e) {
      throw new Error(
        `Failed to fetch wheel for ${packageName}@${version}: ${(e as Error).message}`,
      )
    }
    if (!res.ok || !res.body) {
      throw new Error(
        `Failed to fetch wheel for ${packageName}@${version}: ${res.status} ${res.statusText}`,
      )
    }

    await pipeline(
      Readable.fromWeb(res.body as unknown as NodeWebReadableStream<Uint8Array>),
      createDownloadLimiter('Wheel download exceeded size limit'),
      createWriteStream(zipPath),
    )

    let directory: unzipper.CentralDirectory
    try {
      directory = await unzipper.Open.file(zipPath)
    } catch (err) {
      throw new Error(`Malformed wheel from ${url}: ${(err as Error).message}`)
    }

    const extractedByteCounter = { bytes: 0 }
    let extractedFiles = 0

    for (const entry of directory.files) {
      if (entry.type !== 'File') continue

      // Wheel contents originate from a third-party package — guard path traversal
      // defensively rather than trust the archive (no tar-style preservePaths here).
      const resolvedPath = path.resolve(destDir, entry.path)
      if (resolvedPath !== destDir && !resolvedPath.startsWith(destDir + path.sep)) {
        throw new Error(`Wheel entry escapes destination dir: ${entry.path}`)
      }

      extractedFiles++
      if (extractedFiles > MAX_EXTRACTED_FILES) {
        throw new Error('Wheel extraction exceeded size/file limits')
      }

      mkdirSync(path.dirname(resolvedPath), { recursive: true })

      const extractionLimiter = createDownloadLimiter(
        'Wheel extraction exceeded size/file limits',
        MAX_EXTRACTED_BYTES,
        extractedByteCounter,
      )

      await pipeline(entry.stream(), extractionLimiter, createWriteStream(resolvedPath))
    }
  } finally {
    clearTimeout(timeoutHandle)
    rmSync(zipPath, { force: true })
  }
}

export async function downloadAndExtractPypiSource(
  packageName: string,
  version: string,
  destDir: string,
): Promise<void> {
  const project = await fetchVersionProject(packageName, version)
  const dist = selectDistribution(project.urls)
  if (!dist) {
    throw new PypiSourceNotFoundError(packageName, version)
  }

  mkdirSync(destDir, { recursive: true })

  if (dist.packagetype === 'sdist') {
    await downloadSdist(dist.url, destDir, packageName, version)
  } else {
    await downloadWheel(dist.url, destDir, packageName, version)
  }
}
