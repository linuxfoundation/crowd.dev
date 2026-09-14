import { existsSync, mkdtempSync, readFileSync, rmSync } from 'fs'
import * as os from 'os'
import * as path from 'path'
import type { ReadableStreamDefaultController as WebReadableStreamDefaultController } from 'stream/web'
import { ReadableStream } from 'stream/web'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { downloadAndExtractGoModule } from '../goModuleZip'

import { buildStoredZip } from './zipFixture'

const MODULE = 'github.com/pubnub/go/v7'
const VERSION = 'v7.2.0'
const PREFIX = `${MODULE}@${VERSION}/`

function fakeZipResponse(buf: Buffer): Response {
  const stream = new ReadableStream<Uint8Array>({
    start(controller: WebReadableStreamDefaultController<Uint8Array>) {
      controller.enqueue(new Uint8Array(buf))
      controller.close()
    },
  })
  return {
    status: 200,
    ok: true,
    body: stream,
  } as unknown as Response
}

let destDir: string

afterEach(() => {
  vi.unstubAllGlobals()
  if (destDir && existsSync(destDir)) rmSync(destDir, { recursive: true, force: true })
})

describe('downloadAndExtractGoModule', () => {
  it('strips the module@version/ prefix and extracts file contents', async () => {
    const zip = buildStoredZip([
      { path: `${PREFIX}go.mod`, content: 'module github.com/pubnub/go/v7\n' },
      { path: `${PREFIX}pubnub/pubnub.go`, content: 'package pubnub\n' },
    ])
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeZipResponse(zip)))

    destDir = mkdtempSync(path.join(os.tmpdir(), 'gomodzip-test-'))
    await downloadAndExtractGoModule(MODULE, VERSION, destDir)

    expect(readFileSync(path.join(destDir, 'go.mod'), 'utf8')).toBe(
      'module github.com/pubnub/go/v7\n',
    )
    expect(readFileSync(path.join(destDir, 'pubnub/pubnub.go'), 'utf8')).toBe('package pubnub\n')
  })

  it('rejects entries whose relative path escapes the destination directory', async () => {
    const zip = buildStoredZip([{ path: `${PREFIX}../../evil.txt`, content: 'pwned' }])
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeZipResponse(zip)))

    destDir = mkdtempSync(path.join(os.tmpdir(), 'gomodzip-test-'))
    await expect(downloadAndExtractGoModule(MODULE, VERSION, destDir)).rejects.toThrow(
      /escapes destination dir/,
    )
  })

  it('rejects zips exceeding the extracted file-count cap', async () => {
    const entries = Array.from({ length: 20_001 }, (_, i) => ({
      path: `${PREFIX}file${i}.txt`,
      content: 'x',
    }))
    const zip = buildStoredZip(entries)
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeZipResponse(zip)))

    destDir = mkdtempSync(path.join(os.tmpdir(), 'gomodzip-test-'))
    await expect(downloadAndExtractGoModule(MODULE, VERSION, destDir)).rejects.toThrow(
      /size\/file limits/,
    )
  })

  // The download-size cap itself (200 MiB) is unit-tested directly against
  // createDownloadLimiter in downloadLimits.test.ts with a KiB-scale fixture —
  // building a fixture that large here would burn CI memory/time for no extra coverage.
})
