import { describe, expect, it } from 'vitest'

import {
  buildPackagistVersionRows,
  extractVersionDependencies,
  isPackagistDevVersion,
  isPackagistPrerelease,
  normalizePackagistStats,
  packagistNameFromPurl,
} from '../normalize'
import type { PackagistPackageInfo } from '../types'

describe('purl helpers', () => {
  it('strips the pkg:composer prefix to recover vendor/name', () => {
    expect(packagistNameFromPurl('pkg:composer/monolog/monolog')).toBe('monolog/monolog')
    expect(packagistNameFromPurl('pkg:composer/symfony/http-kernel')).toBe('symfony/http-kernel')
  })
})

// B2 — dynamic-endpoint normalization.
describe('normalizePackagistStats', () => {
  const full: PackagistPackageInfo = {
    name: 'monolog/monolog',
    description: 'Sends your logs to files and web services',
    time: '2011-02-15T15:11:52+00:00',
    maintainers: [{ name: 'seldaek', avatar_url: 'https://example.org/a.png' }, {}],
    type: 'library',
    repository: 'https://github.com/Seldaek/monolog',
    language: 'PHP',
    dependents: 5423,
    suggesters: 300,
    favers: 10000,
    downloads: { total: 500_000_000, monthly: 10_000_000, daily: 350_000 },
  }

  it('extracts description, repository, dependents and total downloads', () => {
    const stats = normalizePackagistStats(full)
    expect(stats).toEqual({
      name: 'monolog/monolog',
      description: 'Sends your logs to files and web services',
      repositoryUrl: 'https://github.com/Seldaek/monolog',
      status: 'active',
      dependents: 5423,
      downloadsTotal: 500_000_000,
      // downloads.monthly is deliberately not extracted — packages.downloads_last_30d
      // belongs exclusively to the dedicated downloads-30d lane
      // nameless maintainer entries are dropped
      maintainers: [{ username: 'seldaek', displayName: null, email: null, role: 'maintainer' }],
    })
  })

  it('maps abandoned → deprecated for both the boolean and replacement-name forms', () => {
    expect(normalizePackagistStats({ ...full, abandoned: true }).status).toBe('deprecated')
    expect(normalizePackagistStats({ ...full, abandoned: 'new/package' }).status).toBe('deprecated')
    expect(normalizePackagistStats({ ...full, abandoned: false }).status).toBe('active')
    expect(normalizePackagistStats(full).status).toBe('active')
  })

  it('drops a null/non-object maintainer entry instead of throwing', () => {
    // isPackagistStatsJson only guarantees maintainers is an array, not that every
    // element is a well-formed object — a rogue null must not crash `m.name` access.
    const stats = normalizePackagistStats({
      name: 'a/b',
      maintainers: [null, 'not-an-object', { name: 'seldaek' }] as never,
    })
    expect(stats.maintainers).toEqual([
      { username: 'seldaek', displayName: null, email: null, role: 'maintainer' },
    ])
  })

  it('tolerates missing optional fields with nulls and empty lists', () => {
    const stats = normalizePackagistStats({ name: 'a/b' })
    expect(stats).toEqual({
      name: 'a/b',
      description: null,
      repositoryUrl: null,
      status: 'active',
      dependents: null,
      downloadsTotal: null,
      maintainers: [],
    })
  })
})

describe('isPackagistDevVersion', () => {
  it('detects dev- branch versions', () => {
    expect(isPackagistDevVersion('dev-main')).toBe(true)
    expect(isPackagistDevVersion('dev-feature/foo')).toBe(true)
  })

  it('detects numbered branch versions via the -dev normalized suffix', () => {
    expect(isPackagistDevVersion('2.x-dev', '2.9999999.9999999.9999999-dev')).toBe(true)
    expect(isPackagistDevVersion('1.0.x-dev', '1.0.9999999.9999999-dev')).toBe(true)
  })

  it('keeps tagged releases', () => {
    expect(isPackagistDevVersion('1.0.0', '1.0.0.0')).toBe(false)
    expect(isPackagistDevVersion('v2.3.4', '2.3.4.0')).toBe(false)
  })
})

describe('isPackagistPrerelease', () => {
  it('flags alpha/beta/RC suffixes', () => {
    expect(isPackagistPrerelease('1.0.0.0-alpha2')).toBe(true)
    expect(isPackagistPrerelease('1.0.0.0-beta1')).toBe(true)
    expect(isPackagistPrerelease('1.0.0.0-RC1')).toBe(true)
  })

  it('treats plain and patch-suffixed versions as stable', () => {
    expect(isPackagistPrerelease('1.0.0.0')).toBe(false)
    expect(isPackagistPrerelease('1.0.0.0-patch1')).toBe(false)
  })
})

// C2 — version rows: dev branches skipped, latest by normalized numeric compare,
// stable preferred over prerelease, aggregates span the kept rows.
describe('buildPackagistVersionRows', () => {
  it('skips dev branches and derives rows and aggregates from tagged releases', () => {
    const { versionRows, latestVersion, firstReleaseAt, latestReleaseAt, licenses, homepage } =
      buildPackagistVersionRows([
        { version: 'dev-main', version_normalized: 'dev-main' },
        {
          version: '2.10.0',
          version_normalized: '2.10.0.0',
          time: '2024-03-01T00:00:00+00:00',
          license: ['MIT'],
          homepage: 'https://example.org',
        },
        {
          version: '2.9.1',
          version_normalized: '2.9.1.0',
          time: '2023-01-01T00:00:00+00:00',
          license: ['MIT'],
        },
        {
          version: '1.0.0',
          version_normalized: '1.0.0.0',
          time: '2020-06-01T00:00:00+00:00',
          license: ['BSD-3-Clause'],
          homepage: 'https://old.example.org',
        },
      ])

    expect(versionRows).toHaveLength(3)
    const byNumber = Object.fromEntries(versionRows.map((r) => [r.number, r]))
    // numeric compare: 2.10.0 > 2.9.1
    expect(byNumber['2.10.0']).toEqual({
      number: '2.10.0',
      publishedAt: '2024-03-01T00:00:00+00:00',
      isLatest: true,
      isPrerelease: false,
      licenses: ['MIT'],
    })
    expect(byNumber['2.9.1'].isLatest).toBe(false)
    expect(byNumber['1.0.0'].licenses).toEqual(['BSD-3-Clause'])
    expect(latestVersion).toBe('2.10.0')
    expect(firstReleaseAt).toBe('2020-06-01T00:00:00+00:00')
    expect(latestReleaseAt).toBe('2024-03-01T00:00:00+00:00')
    // package-level licenses and homepage come from the latest version
    expect(licenses).toEqual(['MIT'])
    expect(homepage).toBe('https://example.org')
  })

  it('sorts a multi-license array deterministically, regardless of registry order', () => {
    // versions.licenses is documented as deterministically sorted (schema comment) —
    // Composer's dual-licensing means the registry can report the same license set in
    // different orders across fetches, which must not register as a real change.
    const { versionRows, licenses } = buildPackagistVersionRows([
      {
        version: '1.0.0',
        version_normalized: '1.0.0.0',
        license: ['MIT', 'Apache-2.0', 'BSD-3-Clause'],
      },
    ])
    expect(versionRows[0].licenses).toEqual(['Apache-2.0', 'BSD-3-Clause', 'MIT'])
    expect(licenses).toEqual(['Apache-2.0', 'BSD-3-Clause', 'MIT'])
  })

  it('returns a null homepage when the latest version omits or blanks it', () => {
    const absent = buildPackagistVersionRows([{ version: '1.0.0', version_normalized: '1.0.0.0' }])
    expect(absent.homepage).toBeNull()

    const blank = buildPackagistVersionRows([
      { version: '1.0.0', version_normalized: '1.0.0.0', homepage: '   ' },
    ])
    expect(blank.homepage).toBeNull()
  })

  it('prefers a stable release over a newer prerelease for latest', () => {
    const { latestVersion, versionRows } = buildPackagistVersionRows([
      {
        version: '3.0.0-RC1',
        version_normalized: '3.0.0.0-RC1',
        time: '2026-01-01T00:00:00+00:00',
      },
      { version: '2.5.0', version_normalized: '2.5.0.0', time: '2025-01-01T00:00:00+00:00' },
    ])
    expect(latestVersion).toBe('2.5.0')
    const byNumber = Object.fromEntries(versionRows.map((r) => [r.number, r]))
    expect(byNumber['3.0.0-RC1'].isPrerelease).toBe(true)
    expect(byNumber['3.0.0-RC1'].isLatest).toBe(false)
    expect(byNumber['2.5.0'].isLatest).toBe(true)
  })

  it('falls back to the highest prerelease when no stable release exists', () => {
    const { latestVersion } = buildPackagistVersionRows([
      { version: '1.0.0-beta2', version_normalized: '1.0.0.0-beta2' },
      { version: '1.0.0-alpha3', version_normalized: '1.0.0.0-alpha3' },
    ])
    expect(latestVersion).toBe('1.0.0-beta2')
  })

  it('picks the higher base version among prereleases regardless of suffix rank', () => {
    const { latestVersion } = buildPackagistVersionRows([
      { version: '1.0.0-rc1', version_normalized: '1.0.0.0-rc1' },
      { version: '2.0.0-alpha1', version_normalized: '2.0.0.0-alpha1' },
    ])
    expect(latestVersion).toBe('2.0.0-alpha1')
  })

  it('breaks same-base prerelease ties by suffix rank, not array order', () => {
    // lower-precedence suffix listed FIRST — array order must not decide
    const { latestVersion } = buildPackagistVersionRows([
      { version: '1.0.0-alpha3', version_normalized: '1.0.0.0-alpha3' },
      { version: '1.0.0-beta2', version_normalized: '1.0.0.0-beta2' },
    ])
    expect(latestVersion).toBe('1.0.0-beta2')
  })

  it('distinguishes numbered suffixes of the same rank', () => {
    const { latestVersion } = buildPackagistVersionRows([
      { version: '1.0.0-rc1', version_normalized: '1.0.0.0-rc1' },
      { version: '1.0.0-rc2', version_normalized: '1.0.0.0-rc2' },
    ])
    expect(latestVersion).toBe('1.0.0-rc2')
  })

  it('returns empty rows and null aggregates when only dev branches exist', () => {
    expect(
      buildPackagistVersionRows([{ version: 'dev-main', version_normalized: 'dev-main' }]),
    ).toEqual({
      versionRows: [],
      latestVersion: null,
      firstReleaseAt: null,
      latestReleaseAt: null,
      licenses: null,
      homepage: null,
    })
  })
})

// C3 — dependency extraction: require→direct, require-dev→dev, platform packages excluded.
describe('extractVersionDependencies', () => {
  it('maps require to direct and require-dev to dev, preserving constraints', () => {
    const deps = extractVersionDependencies({
      version: '2.0.0',
      require: { php: '>=8.1', 'psr/log': '^2.0 || ^3.0' },
      'require-dev': { 'phpunit/phpunit': '^10.5' },
    })
    expect(deps).toEqual([
      { name: 'psr/log', constraint: '^2.0 || ^3.0', kind: 'direct' },
      { name: 'phpunit/phpunit', constraint: '^10.5', kind: 'dev' },
    ])
  })

  it('excludes platform packages (php, hhvm, composer-*, ext-*, lib-*)', () => {
    const deps = extractVersionDependencies({
      version: '1.0.0',
      require: {
        php: '>=7.4',
        hhvm: '*',
        'composer-plugin-api': '^2.0',
        'composer-runtime-api': '^2.2',
        'ext-json': '*',
        'lib-icu': '*',
        'real/dep': '^1.0',
      },
    })
    expect(deps).toEqual([{ name: 'real/dep', constraint: '^1.0', kind: 'direct' }])
  })

  it('returns [] when the version declares no dependencies', () => {
    expect(extractVersionDependencies({ version: '1.0.0' })).toEqual([])
  })
})
