import { execFile } from 'node:child_process'
import { createWriteStream } from 'node:fs'
import { mkdir, rm } from 'node:fs/promises'
import * as path from 'node:path'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { promisify } from 'node:util'

const execFileAsync = promisify(execFile)

export const DUMP_DIR = '/tmp/cargo-dump'
const DOWNLOAD_TIMEOUT_MS = 60 * 60 * 1000 // 1 hour — dump is ~1.3 GB compressed

async function downloadTarball(url: string, target: string): Promise<void> {
  const ac = new AbortController()
  const timer = setTimeout(() => ac.abort(), DOWNLOAD_TIMEOUT_MS)

  try {
    let response: Response
    try {
      response = await fetch(url, { signal: ac.signal })
    } catch (err) {
      throw new Error(`Failed to GET ${url}: ${(err as Error).message}`)
    }

    if (!response.ok) {
      throw new Error(`Unexpected response from ${url}: HTTP ${response.status}`)
    }

    if (!response.body) {
      throw new Error(`Empty body from ${url}`)
    }

    try {
      await pipeline(Readable.fromWeb(response.body), createWriteStream(target))
    } catch (err) {
      throw new Error(`Stream failed for ${url}: ${(err as Error).message}`)
    }
  } finally {
    clearTimeout(timer)
  }
}

// downloadAndExtractDump downloads the crates.io db-dump tarball and extracts
// it into DUMP_DIR (stripping the dated subdirectory). Returns DUMP_DIR.
export async function downloadAndExtractDump(url: string): Promise<string> {
  await rm(DUMP_DIR, { recursive: true, force: true })
  await mkdir(DUMP_DIR, { recursive: true })

  const tarPath = path.join(DUMP_DIR, 'db-dump.tar.gz')

  await downloadTarball(url, tarPath)

  await execFileAsync('tar', ['-xzf', tarPath, '-C', DUMP_DIR, '--strip-components=1'])

  await rm(tarPath, { force: true })

  return DUMP_DIR
}
