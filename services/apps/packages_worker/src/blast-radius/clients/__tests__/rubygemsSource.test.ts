import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { Readable } from 'stream'
import * as tar from 'tar'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { RubyGemsSourceNotFoundError, downloadAndExtractRubyGemsSource } from '../rubygemsSource'

vi.mock('../downloadLimits', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../downloadLimits')>()
  return { ...actual, MAX_EXTRACTED_FILES: 5 }
})

async function buildFixtureGem(files: Record<string, string>): Promise<Buffer> {
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gemfixture-'))
  try {
    for (const [name, content] of Object.entries(files)) {
      const full = path.join(workDir, name)
      fs.mkdirSync(path.dirname(full), { recursive: true })
      fs.writeFileSync(full, content)
    }

    const dataTarGzPath = path.join(workDir, 'data.tar.gz')
    await tar.create({ gzip: true, cwd: workDir, file: dataTarGzPath }, Object.keys(files))

    fs.writeFileSync(path.join(workDir, 'metadata.gz'), 'fake-metadata')
    fs.writeFileSync(path.join(workDir, 'checksums.yaml.gz'), 'fake-checksums')

    const outerTarPath = path.join(workDir, 'fixture.gem')
    await tar.create({ cwd: workDir, file: outerTarPath }, [
      'metadata.gz',
      'data.tar.gz',
      'checksums.yaml.gz',
    ])

    return fs.readFileSync(outerTarPath)
  } finally {
    fs.rmSync(workDir, { recursive: true, force: true })
  }
}

function mockFetchOnce(response: { status?: number; ok?: boolean; body?: Buffer | null }): void {
  const body = response.body
  vi.stubGlobal(
    'fetch',
    vi.fn().mockResolvedValue({
      status: response.status ?? 200,
      ok: response.ok ?? true,
      statusText: 'OK',
      body: body ? Readable.toWeb(Readable.from(body)) : null,
    }),
  )
}

describe('downloadAndExtractRubyGemsSource', () => {
  let destDir: string

  beforeEach(() => {
    destDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gemdest-'))
  })

  afterEach(() => {
    fs.rmSync(destDir, { recursive: true, force: true })
    vi.unstubAllGlobals()
  })

  it('extracts data.tar.gz entries to destDir root, with no wrapper directory', async () => {
    const gemBuffer = await buildFixtureGem({
      'lib/rack.rb': 'module Rack; end',
      'README.md': '# rack',
    })
    mockFetchOnce({ body: gemBuffer })

    await downloadAndExtractRubyGemsSource('rack', '3.0.8', destDir)

    expect(fs.readFileSync(path.join(destDir, 'lib/rack.rb'), 'utf8')).toBe('module Rack; end')
    expect(fs.readFileSync(path.join(destDir, 'README.md'), 'utf8')).toBe('# rack')
  })

  it('throws RubyGemsSourceNotFoundError on a 404', async () => {
    mockFetchOnce({ status: 404, ok: false, body: null })

    await expect(
      downloadAndExtractRubyGemsSource('nonexistent-gem', '1.0.0', destDir),
    ).rejects.toThrow(RubyGemsSourceNotFoundError)
  })

  it('throws RubyGemsSourceNotFoundError when the fetch itself rejects', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('network error')))

    await expect(downloadAndExtractRubyGemsSource('rack', '3.0.8', destDir)).rejects.toThrow(
      RubyGemsSourceNotFoundError,
    )
  })

  it('requests the platform-suffixed URL when a non-ruby platform is passed', async () => {
    const gemBuffer = await buildFixtureGem({ 'lib/x.rb': 'X' })
    mockFetchOnce({ body: gemBuffer })

    await downloadAndExtractRubyGemsSource('nokogiri', '1.13.0', destDir, 'x86_64-linux')

    expect(fetch).toHaveBeenCalledWith(
      'https://rubygems.org/downloads/nokogiri-1.13.0-x86_64-linux.gem',
      expect.anything(),
    )
  })

  it('omits the platform suffix for the default "ruby" platform', async () => {
    const gemBuffer = await buildFixtureGem({ 'lib/x.rb': 'X' })
    mockFetchOnce({ body: gemBuffer })

    await downloadAndExtractRubyGemsSource('rack', '3.0.8', destDir, 'ruby')

    expect(fetch).toHaveBeenCalledWith(
      'https://rubygems.org/downloads/rack-3.0.8.gem',
      expect.anything(),
    )
  })

  it('destroys extraction instead of silently truncating when the file count exceeds the limit', async () => {
    const files: Record<string, string> = {}
    for (let i = 0; i < 7; i++) {
      files[`lib/file${i}.rb`] = `# file ${i}`
    }
    const gemBuffer = await buildFixtureGem(files)
    mockFetchOnce({ body: gemBuffer })

    await expect(downloadAndExtractRubyGemsSource('bloated-gem', '1.0.0', destDir)).rejects.toThrow(
      'Gem exceeded size/file limits',
    )
  })

  it('throws RubyGemsSourceNotFoundError when the .gem has no data.tar.gz entry', async () => {
    const workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gembroken-'))
    try {
      fs.writeFileSync(path.join(workDir, 'metadata.gz'), 'fake-metadata')
      const outerTarPath = path.join(workDir, 'broken.gem')
      await tar.create({ cwd: workDir, file: outerTarPath }, ['metadata.gz'])
      const gemBuffer = fs.readFileSync(outerTarPath)

      mockFetchOnce({ body: gemBuffer })

      await expect(downloadAndExtractRubyGemsSource('rack', '3.0.8', destDir)).rejects.toThrow(
        RubyGemsSourceNotFoundError,
      )
    } finally {
      fs.rmSync(workDir, { recursive: true, force: true })
    }
  })
})
