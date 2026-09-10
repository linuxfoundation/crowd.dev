import { beforeEach, describe, expect, it, vi } from 'vitest'

import { fetchNuspec } from '../../../nuget/client'
import { downloadAndExtractTarball } from '../npmTarball'
import { NuGetSourceNotFoundError, downloadAndExtractNuGetSource } from '../nugetSource'

vi.mock('../../../nuget/client', () => ({ fetchNuspec: vi.fn() }))
vi.mock('../npmTarball', () => ({ downloadAndExtractTarball: vi.fn() }))

const mockFetchNuspec = vi.mocked(fetchNuspec)
const mockDownloadAndExtractTarball = vi.mocked(downloadAndExtractTarball)

function nuspecWithRepository(url?: string, commit?: string): string {
  const attrs = [url ? `url="${url}"` : null, commit ? `commit="${commit}"` : null]
    .filter(Boolean)
    .join(' ')
  const repository = attrs ? `<repository type="git" ${attrs} />` : ''
  return `<?xml version="1.0"?><package><metadata>${repository}</metadata></package>`
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('downloadAndExtractNuGetSource', () => {
  it('downloads the exact commit recorded in the nuspec <repository> element', async () => {
    mockFetchNuspec.mockResolvedValue(
      nuspecWithRepository('https://github.com/owner/repo', 'abc123'),
    )
    mockDownloadAndExtractTarball.mockResolvedValue(undefined)

    await downloadAndExtractNuGetSource('Some.Package', '1.0.0', '/tmp/dest')

    expect(mockDownloadAndExtractTarball).toHaveBeenCalledWith(
      'https://codeload.github.com/owner/repo/tar.gz/abc123',
      '/tmp/dest',
    )
    expect(mockDownloadAndExtractTarball).toHaveBeenCalledTimes(1)
  })

  it('falls back to a version-tag guess when no commit is recorded', async () => {
    mockFetchNuspec.mockResolvedValue(nuspecWithRepository('https://github.com/owner/repo'))
    mockDownloadAndExtractTarball.mockResolvedValue(undefined)

    await downloadAndExtractNuGetSource('Some.Package', '1.0.0', '/tmp/dest')

    expect(mockDownloadAndExtractTarball).toHaveBeenCalledWith(
      'https://codeload.github.com/owner/repo/tar.gz/v1.0.0',
      '/tmp/dest',
    )
  })

  it('tries the next tag guess when the first candidate fails', async () => {
    mockFetchNuspec.mockResolvedValue(nuspecWithRepository('https://github.com/owner/repo'))
    mockDownloadAndExtractTarball
      .mockRejectedValueOnce(new Error('404'))
      .mockResolvedValueOnce(undefined)

    await downloadAndExtractNuGetSource('Some.Package', '1.0.0', '/tmp/dest')

    expect(mockDownloadAndExtractTarball).toHaveBeenNthCalledWith(
      1,
      'https://codeload.github.com/owner/repo/tar.gz/v1.0.0',
      '/tmp/dest',
    )
    expect(mockDownloadAndExtractTarball).toHaveBeenNthCalledWith(
      2,
      'https://codeload.github.com/owner/repo/tar.gz/1.0.0',
      '/tmp/dest',
    )
  })

  it('throws NuGetSourceNotFoundError when every candidate fails', async () => {
    mockFetchNuspec.mockResolvedValue(nuspecWithRepository('https://github.com/owner/repo'))
    mockDownloadAndExtractTarball.mockRejectedValue(new Error('404'))

    await expect(
      downloadAndExtractNuGetSource('Some.Package', '1.0.0', '/tmp/dest'),
    ).rejects.toThrow(NuGetSourceNotFoundError)
  })

  it('throws NuGetSourceNotFoundError when the nuspec has no repository url', async () => {
    mockFetchNuspec.mockResolvedValue(nuspecWithRepository())

    await expect(
      downloadAndExtractNuGetSource('Some.Package', '1.0.0', '/tmp/dest'),
    ).rejects.toThrow(NuGetSourceNotFoundError)
    expect(mockDownloadAndExtractTarball).not.toHaveBeenCalled()
  })

  it('throws NuGetSourceNotFoundError for a non-GitHub repository host', async () => {
    mockFetchNuspec.mockResolvedValue(nuspecWithRepository('https://gitlab.com/owner/repo'))

    await expect(
      downloadAndExtractNuGetSource('Some.Package', '1.0.0', '/tmp/dest'),
    ).rejects.toThrow(NuGetSourceNotFoundError)
    expect(mockDownloadAndExtractTarball).not.toHaveBeenCalled()
  })

  it('throws NuGetSourceNotFoundError when the nuspec fetch itself fails', async () => {
    mockFetchNuspec.mockResolvedValue({ kind: 'NOT_FOUND', message: 'not found' })

    await expect(
      downloadAndExtractNuGetSource('Some.Package', '1.0.0', '/tmp/dest'),
    ).rejects.toThrow(NuGetSourceNotFoundError)
    expect(mockDownloadAndExtractTarball).not.toHaveBeenCalled()
  })
})
