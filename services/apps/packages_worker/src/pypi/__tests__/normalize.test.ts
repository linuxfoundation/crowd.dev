import { describe, expect, it } from 'vitest'

import {
  classifyProjectUrls,
  collectPypiMaintainers,
  isPypiPrerelease,
  parseKeywords,
  pypiNameFromPurl,
  resolvePypiLicenses,
  stripNullBytesDeep,
} from '../normalize'
import type { PyPiInfo } from '../types'

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

describe('stripNullBytesDeep', () => {
  it('removes NUL bytes from nested strings', () => {
    const nul = String.fromCharCode(0)
    const v = stripNullBytesDeep({ a: `x${nul}y`, b: [`p${nul}`, 'q'] })
    expect(v.a).toBe('xy')
    expect(v.b).toEqual(['p', 'q'])
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
