import { describe, expect, it } from 'vitest'

import {
  buildVersionRows,
  classifyProjectUrls,
  collectPypiMaintainers,
  isPypiPrerelease,
  parseKeywords,
  pypiNameFromPurl,
  resolvePypiLicenses,
} from '../normalize'
import type { PyPiInfo, PyPiReleaseFile } from '../types'

function info(partial: Partial<PyPiInfo>): PyPiInfo {
  return { name: 'demo', ...partial }
}

describe('pypiNameFromPurl', () => {
  it('strips the pkg:pypi/ prefix', () => {
    expect(pypiNameFromPurl('pkg:pypi/flask')).toBe('flask')
  })

  it('decodes percent-encoded segments', () => {
    expect(pypiNameFromPurl('pkg:pypi/zope.interface')).toBe('zope.interface')
    expect(pypiNameFromPurl('pkg:pypi/ruamel.yaml%2Bclib')).toBe('ruamel.yaml+clib')
  })
})

describe('buildVersionRows', () => {
  const file = (partial: Partial<PyPiReleaseFile>): PyPiReleaseFile => ({ ...partial })

  it('skips releases whose files were all deleted (empty array)', () => {
    const { versionRows, firstReleaseAt, latestReleaseAt } = buildVersionRows(
      {
        '1.0': [file({ upload_time_iso_8601: '2020-01-01T00:00:00Z' })],
        '1.1': [], // all files deleted
      },
      '1.1',
      'MIT',
    )
    expect(versionRows.map((r) => r.number)).toEqual(['1.0'])
    expect(firstReleaseAt).toBe('2020-01-01T00:00:00Z')
    expect(latestReleaseAt).toBe('2020-01-01T00:00:00Z')
  })

  it('marks a release yanked only when every file is yanked', () => {
    const { versionRows } = buildVersionRows(
      {
        '1.0': [file({ yanked: true }), file({ yanked: false })],
        '2.0': [file({ yanked: true }), file({ yanked: true })],
      },
      '2.0',
      null,
    )
    const byNumber = Object.fromEntries(versionRows.map((r) => [r.number, r]))
    expect(byNumber['1.0'].isYanked).toBe(false)
    expect(byNumber['2.0'].isYanked).toBe(true)
  })

  it('flags isLatest, derives publishedAt from the earliest file, and spans first/latest release', () => {
    const { versionRows, firstReleaseAt, latestReleaseAt } = buildVersionRows(
      {
        '1.0': [
          file({ upload_time_iso_8601: '2021-06-01T00:00:00Z' }),
          file({ upload_time_iso_8601: '2021-05-01T00:00:00Z' }),
        ],
        '2.0': [file({ upload_time_iso_8601: '2022-01-01T00:00:00Z' })],
      },
      '2.0',
      'Apache-2.0',
    )
    const byNumber = Object.fromEntries(versionRows.map((r) => [r.number, r]))
    expect(byNumber['1.0'].publishedAt).toBe('2021-05-01T00:00:00Z') // earliest file
    expect(byNumber['1.0'].isLatest).toBe(false)
    expect(byNumber['2.0'].isLatest).toBe(true)
    expect(byNumber['2.0'].license).toBe('Apache-2.0')
    expect(firstReleaseAt).toBe('2021-05-01T00:00:00Z')
    expect(latestReleaseAt).toBe('2022-01-01T00:00:00Z')
  })

  it('returns empty rows and null dates for no releases', () => {
    expect(buildVersionRows({}, null, null)).toEqual({
      versionRows: [],
      firstReleaseAt: null,
      latestReleaseAt: null,
    })
  })
})

describe('resolvePypiLicenses', () => {
  it('prefers the SPDX license_expression', () => {
    const r = resolvePypiLicenses(
      info({ license_expression: 'MIT OR Apache-2.0', license: 'ignored', classifiers: [] }),
    )
    expect(r.licenses).toEqual(['MIT', 'Apache-2.0'])
    expect(r.licensesRaw).toBe('MIT OR Apache-2.0')
  })

  it('falls back to License :: classifiers', () => {
    const r = resolvePypiLicenses(
      info({
        classifiers: [
          'Development Status :: 5 - Production/Stable',
          'License :: OSI Approved :: BSD License',
        ],
      }),
    )
    expect(r.licenses).toEqual(['BSD-3-Clause'])
  })

  it('uses the classifier leaf for unmapped licenses', () => {
    const r = resolvePypiLicenses(
      info({ classifiers: ['License :: OSI Approved :: Eclipse Public License 1.0 (EPL-1.0)'] }),
    )
    expect(r.licenses).toEqual(['Eclipse Public License 1.0 (EPL-1.0)'])
  })

  it('ignores grouping-only classifier nodes', () => {
    const r = resolvePypiLicenses(info({ classifiers: ['License :: OSI Approved'] }))
    expect(r.licenses).toEqual([])
  })

  it('uses a short license string when no expression/classifier', () => {
    const r = resolvePypiLicenses(info({ license: 'MIT' }))
    expect(r.licenses).toEqual(['MIT'])
    expect(r.licensesRaw).toBe('MIT')
  })

  it('keeps long license text out of the licenses array', () => {
    const text = 'Permission is hereby granted, free of charge, to any person...'.repeat(5)
    const r = resolvePypiLicenses(info({ license: text }))
    expect(r.licenses).toEqual([])
    expect(r.licensesRaw).toBe(text)
  })
})

describe('isPypiPrerelease', () => {
  it('returns false for stable releases', () => {
    expect(isPypiPrerelease('1.0.0')).toBe(false)
    expect(isPypiPrerelease('2023.9.1')).toBe(false)
    expect(isPypiPrerelease('3.1.3')).toBe(false)
  })

  it('detects alpha/beta/rc markers', () => {
    expect(isPypiPrerelease('1.0a1')).toBe(true)
    expect(isPypiPrerelease('1.0b2')).toBe(true)
    expect(isPypiPrerelease('1.0rc1')).toBe(true)
    expect(isPypiPrerelease('1.0c3')).toBe(true)
    expect(isPypiPrerelease('1.0.0alpha')).toBe(true)
    expect(isPypiPrerelease('2.0.0beta1')).toBe(true)
  })

  it('detects dev releases', () => {
    expect(isPypiPrerelease('1.0.dev0')).toBe(true)
    expect(isPypiPrerelease('1.0.0.dev3')).toBe(true)
    expect(isPypiPrerelease('1.0a1.dev1')).toBe(true)
  })

  it('does NOT treat post-releases as prereleases', () => {
    expect(isPypiPrerelease('1.0.post1')).toBe(false)
    expect(isPypiPrerelease('1.0.0.post2')).toBe(false)
  })

  it('ignores local and epoch parts', () => {
    expect(isPypiPrerelease('1.0.0+ubuntu1')).toBe(false)
    expect(isPypiPrerelease('1!2.0.0')).toBe(false)
    expect(isPypiPrerelease('1!2.0rc1')).toBe(true)
  })
})

describe('collectPypiMaintainers', () => {
  it('parses "Name <email>" from *_email', () => {
    const people = collectPypiMaintainers(
      info({ maintainer_email: 'Pallets <contact@palletsprojects.com>' }),
    )
    expect(people).toEqual([
      {
        username: 'Pallets',
        displayName: 'Pallets',
        email: 'contact@palletsprojects.com',
        role: 'maintainer',
      },
    ])
  })

  it('pairs separate name and email fields', () => {
    const people = collectPypiMaintainers(
      info({ author: 'Kenneth Reitz', author_email: 'me@kennethreitz.org' }),
    )
    expect(people).toEqual([
      {
        username: 'Kenneth Reitz',
        displayName: 'Kenneth Reitz',
        email: 'me@kennethreitz.org',
        role: 'author',
      },
    ])
  })

  it('splits comma-separated people', () => {
    const people = collectPypiMaintainers(
      info({ author_email: 'A One <a@x.com>, B Two <b@x.com>' }),
    )
    expect(people.map((p) => p.username)).toEqual(['A One', 'B Two'])
  })

  it('uses email as username when no name is present', () => {
    const people = collectPypiMaintainers(info({ author_email: 'solo@x.com' }))
    expect(people).toEqual([
      { username: 'solo@x.com', displayName: null, email: 'solo@x.com', role: 'author' },
    ])
  })

  it('keeps surplus names when there are fewer emails than names', () => {
    const people = collectPypiMaintainers(
      info({ author: 'Alice Smith, Bob Jones', author_email: 'alice@x.com' }),
    )
    expect(people).toEqual([
      { username: 'Alice Smith', displayName: 'Alice Smith', email: 'alice@x.com', role: 'author' },
      { username: 'Bob Jones', displayName: 'Bob Jones', email: null, role: 'author' },
    ])
  })

  it('returns nothing when author/maintainer are absent', () => {
    expect(collectPypiMaintainers(info({}))).toEqual([])
  })

  it('lets the author role win on a username collision', () => {
    const people = collectPypiMaintainers(
      info({ maintainer: 'Same Person', author: 'Same Person' }),
    )
    expect(people).toEqual([
      { username: 'Same Person', displayName: 'Same Person', email: null, role: 'author' },
    ])
  })
})

describe('classifyProjectUrls', () => {
  it('picks homepage, repo, and funding from project_urls', () => {
    const r = classifyProjectUrls(
      {
        Homepage: 'https://flask.palletsprojects.com/',
        Source: 'https://github.com/pallets/flask/',
        Donate: 'https://palletsprojects.com/donate',
      },
      null,
    )
    expect(r.homepage).toBe('https://flask.palletsprojects.com/')
    expect(r.declaredRepositoryUrl).toBe('https://github.com/pallets/flask/')
    expect(r.fundingLinks).toEqual([{ type: 'other', url: 'https://palletsprojects.com/donate' }])
  })

  it('prefers info.home_page for homepage', () => {
    const r = classifyProjectUrls({ Homepage: 'https://example.org' }, 'https://primary.example')
    expect(r.homepage).toBe('https://primary.example')
  })

  it('falls back to a repo-looking homepage when no explicit repo key', () => {
    const r = classifyProjectUrls({ Homepage: 'https://github.com/psf/requests' }, null)
    expect(r.declaredRepositoryUrl).toBe('https://github.com/psf/requests')
  })

  it('infers funding type from the host', () => {
    const r = classifyProjectUrls(
      {
        Funding: 'https://github.com/sponsors/foo',
        Sponsor: 'https://opencollective.com/bar',
      },
      null,
    )
    expect(r.fundingLinks).toEqual([
      { type: 'github', url: 'https://github.com/sponsors/foo' },
      { type: 'opencollective', url: 'https://opencollective.com/bar' },
    ])
  })

  it('returns nulls/empties when there are no urls', () => {
    const r = classifyProjectUrls(null, null)
    expect(r).toEqual({ homepage: null, declaredRepositoryUrl: null, fundingLinks: [] })
  })
})

describe('parseKeywords', () => {
  it('splits on commas and whitespace and dedupes', () => {
    expect(parseKeywords('web, async,  http web')).toEqual(['web', 'async', 'http'])
  })

  it('returns [] for empty/missing input', () => {
    expect(parseKeywords(null)).toEqual([])
    expect(parseKeywords('   ')).toEqual([])
  })
})
