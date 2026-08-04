import { existsSync, mkdtempSync, readFileSync, rmSync } from 'fs'
import * as os from 'os'
import * as path from 'path'
import type { ReadableStreamDefaultController as WebReadableStreamDefaultController } from 'stream/web'
import { ReadableStream } from 'stream/web'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { downloadAndExtractMavenSources } from '../mavenSourcesJar'

import { buildStoredZip } from './zipFixture'

const GROUP_ID = 'com.example'
const ARTIFACT_ID = 'foo'
const VERSION = '1.2.3'

function fakeJarResponse(status: number, buf?: Buffer): Response {
  if (!buf) {
    return { status, ok: false, statusText: 'Not Found', body: null } as unknown as Response
  }
  const stream = new ReadableStream<Uint8Array>({
    start(controller: WebReadableStreamDefaultController<Uint8Array>) {
      controller.enqueue(new Uint8Array(buf))
      controller.close()
    },
  })
  return {
    status,
    ok: true,
    body: stream,
  } as unknown as Response
}

let destDir: string

afterEach(() => {
  vi.unstubAllGlobals()
  if (destDir && existsSync(destDir)) rmSync(destDir, { recursive: true, force: true })
})

describe('downloadAndExtractMavenSources', () => {
  it('extracts file contents from the sources jar', async () => {
    const zip = buildStoredZip([
      { path: 'com/example/Foo.java', content: 'package com.example;\n' },
      { path: 'com/example/util/Bar.java', content: 'package com.example.util;\n' },
    ])
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeJarResponse(200, zip)))

    destDir = mkdtempSync(path.join(os.tmpdir(), 'mavensourcesjar-test-'))
    await downloadAndExtractMavenSources(GROUP_ID, ARTIFACT_ID, VERSION, destDir)

    expect(readFileSync(path.join(destDir, 'com/example/Foo.java'), 'utf8')).toBe(
      'package com.example;\n',
    )
    expect(readFileSync(path.join(destDir, 'com/example/util/Bar.java'), 'utf8')).toBe(
      'package com.example.util;\n',
    )
  })

  it('rejects entries whose relative path escapes the destination directory', async () => {
    const zip = buildStoredZip([{ path: '../../evil.txt', content: 'pwned' }])
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeJarResponse(200, zip)))

    destDir = mkdtempSync(path.join(os.tmpdir(), 'mavensourcesjar-test-'))
    await expect(
      downloadAndExtractMavenSources(GROUP_ID, ARTIFACT_ID, VERSION, destDir),
    ).rejects.toThrow(/escapes destination dir/)
  })

  it('rejects jars exceeding the extracted file-count cap', async () => {
    const entries = Array.from({ length: 20_001 }, (_, i) => ({
      path: `file${i}.txt`,
      content: 'x',
    }))
    const zip = buildStoredZip(entries)
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeJarResponse(200, zip)))

    destDir = mkdtempSync(path.join(os.tmpdir(), 'mavensourcesjar-test-'))
    await expect(
      downloadAndExtractMavenSources(GROUP_ID, ARTIFACT_ID, VERSION, destDir),
    ).rejects.toThrow(/size\/file limits/)
  })

  it('throws MavenSourcesNotFoundError on a 404 response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeJarResponse(404)))

    destDir = mkdtempSync(path.join(os.tmpdir(), 'mavensourcesjar-test-'))
    await expect(
      downloadAndExtractMavenSources(GROUP_ID, ARTIFACT_ID, VERSION, destDir),
    ).rejects.toThrow(/No sources jar published/)
  })

  // The download-size cap itself (500 MiB) is unit-tested directly against
  // createDownloadLimiter in downloadLimits.test.ts with a KiB-scale fixture —
  // building a fixture that large here would burn CI memory/time for no extra coverage.
})
