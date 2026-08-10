import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { Readable } from 'stream'
import * as tar from 'tar'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { PypiSourceNotFoundError, downloadAndExtractPypiSource } from '../pypiSource'

import { buildStoredZip } from './zipFixture'

vi.mock('../downloadLimits', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../downloadLimits')>()
  return { ...actual, MAX_EXTRACTED_FILES: 5 }
})

async function buildFixtureSdist(
  name: string,
  version: string,
  files: Record<string, string>,
): Promise<Buffer> {
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pypisdist-'))
  try {
    const wrapperDir = `${name}-${version}`
    for (const [relPath, content] of Object.entries(files)) {
      const full = path.join(workDir, wrapperDir, relPath)
      fs.mkdirSync(path.dirname(full), { recursive: true })
      fs.writeFileSync(full, content)
    }

    const tarPath = path.join(workDir, 'fixture.tar.gz')
    await tar.create({ gzip: true, cwd: workDir, file: tarPath }, [wrapperDir])
    return fs.readFileSync(tarPath)
  } finally {
    fs.rmSync(workDir, { recursive: true, force: true })
  }
}

interface FixtureResponse {
  status?: number
  ok?: boolean
  body?: Buffer | null
  json?: unknown
}

function mockFetchSequence(responses: FixtureResponse[]): void {
  let call = 0
  vi.stubGlobal(
    'fetch',
    vi.fn().mockImplementation(() => {
      const r = responses[Math.min(call, responses.length - 1)]
      call++
      return Promise.resolve({
        status: r.status ?? 200,
        ok: r.ok ?? true,
        statusText: 'OK',
        json: () => Promise.resolve(r.json),
        body: r.body ? Readable.toWeb(Readable.from(r.body)) : null,
      })
    }),
  )
}

describe('downloadAndExtractPypiSource', () => {
  let destDir: string

  beforeEach(() => {
    destDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pypidest-'))
  })

  afterEach(() => {
    fs.rmSync(destDir, { recursive: true, force: true })
    vi.unstubAllGlobals()
  })

  it('prefers sdist over wheel when both are present', async () => {
    const sdistBuf = await buildFixtureSdist('flask', '3.0.0', { 'src/flask/__init__.py': 'x=1' })
    mockFetchSequence([
      {
        json: {
          info: { name: 'flask' },
          urls: [
            {
              packagetype: 'bdist_wheel',
              url: 'https://files.pythonhosted.org/wheel.whl',
              filename: 'flask-3.0.0-py3-none-any.whl',
            },
            {
              packagetype: 'sdist',
              url: 'https://files.pythonhosted.org/sdist.tar.gz',
              filename: 'flask-3.0.0.tar.gz',
            },
          ],
        },
      },
      { body: sdistBuf },
    ])

    await downloadAndExtractPypiSource('flask', '3.0.0', destDir)

    expect(fs.readFileSync(path.join(destDir, 'src/flask/__init__.py'), 'utf8')).toBe('x=1')
    expect(fetch).toHaveBeenNthCalledWith(
      2,
      'https://files.pythonhosted.org/sdist.tar.gz',
      expect.anything(),
    )
  })

  it('falls back to a wheel when no sdist is present', async () => {
    const wheelBuf = buildStoredZip([
      { path: 'flask/__init__.py', content: 'x=1' },
      { path: 'flask-3.0.0.dist-info/METADATA', content: 'Metadata-Version: 2.1' },
    ])
    mockFetchSequence([
      {
        json: {
          info: { name: 'flask' },
          urls: [
            {
              packagetype: 'bdist_wheel',
              url: 'https://files.pythonhosted.org/wheel.whl',
              filename: 'flask-3.0.0-py3-none-any.whl',
            },
          ],
        },
      },
      { body: wheelBuf },
    ])

    await downloadAndExtractPypiSource('flask', '3.0.0', destDir)

    expect(fs.readFileSync(path.join(destDir, 'flask/__init__.py'), 'utf8')).toBe('x=1')
    expect(fs.readFileSync(path.join(destDir, 'flask-3.0.0.dist-info/METADATA'), 'utf8')).toBe(
      'Metadata-Version: 2.1',
    )
  })

  it('extracts a zip-format sdist, stripping the wrapper directory', async () => {
    const sdistBuf = buildStoredZip([{ path: 'flask-3.0.0/src/flask/__init__.py', content: 'x=1' }])
    mockFetchSequence([
      {
        json: {
          info: { name: 'flask' },
          urls: [
            {
              packagetype: 'sdist',
              url: 'https://files.pythonhosted.org/sdist.zip',
              filename: 'flask-3.0.0.zip',
            },
          ],
        },
      },
      { body: sdistBuf },
    ])

    await downloadAndExtractPypiSource('flask', '3.0.0', destDir)

    expect(fs.readFileSync(path.join(destDir, 'src/flask/__init__.py'), 'utf8')).toBe('x=1')
  })

  it('throws PypiSourceNotFoundError when neither sdist nor wheel is present', async () => {
    mockFetchSequence([{ json: { info: { name: 'flask' }, urls: [] } }])

    await expect(downloadAndExtractPypiSource('flask', '3.0.0', destDir)).rejects.toThrow(
      PypiSourceNotFoundError,
    )
  })

  it('throws PypiSourceNotFoundError on a 404 for the version metadata', async () => {
    mockFetchSequence([{ status: 404, ok: false }])

    await expect(downloadAndExtractPypiSource('flask', '999.999.999', destDir)).rejects.toThrow(
      PypiSourceNotFoundError,
    )
  })

  it('aborts sdist extraction when the file count exceeds the limit', async () => {
    const files: Record<string, string> = {}
    for (let i = 0; i < 7; i++) files[`src/file${i}.py`] = `# ${i}`
    const sdistBuf = await buildFixtureSdist('bloated', '1.0.0', files)
    mockFetchSequence([
      {
        json: {
          info: { name: 'bloated' },
          urls: [
            {
              packagetype: 'sdist',
              url: 'https://files.pythonhosted.org/bloated.tar.gz',
              filename: 'bloated-1.0.0.tar.gz',
            },
          ],
        },
      },
      { body: sdistBuf },
    ])

    await expect(downloadAndExtractPypiSource('bloated', '1.0.0', destDir)).rejects.toThrow(
      'sdist exceeded size/file limits',
    )
  })
})
