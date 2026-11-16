import { describe, expect, it } from 'vitest'

import { gerritProjectName, isRetiredGerritProject } from '../src/clients/gerrit'

describe('gerritProjectName', () => {
  it('returns the path below the base URL', () => {
    expect(
      gerritProjectName('https://gerrit.onap.org/r/aai/aai-common', 'https://gerrit.onap.org/r'),
    ).toBe('aai/aai-common')
    expect(
      gerritProjectName('https://review.opendev.org/openstack/nova', 'https://review.opendev.org'),
    ).toBe('openstack/nova')
  })

  it('ignores trailing and doubled slashes', () => {
    expect(
      gerritProjectName(
        'https://review.opendev.org//airship/treasuremap',
        'https://review.opendev.org/',
      ),
    ).toBe('airship/treasuremap')
    expect(gerritProjectName('https://gerrit.fd.io/r/vpp/', 'https://gerrit.fd.io/r')).toBe('vpp')
  })

  it('throws when the URL is not under the base URL', () => {
    expect(() =>
      gerritProjectName('https://gerrit.fd.io/r/vpp', 'https://gerrit.onap.org/r'),
    ).toThrow()
    expect(() => gerritProjectName('https://gerrit.fd.io/rvpp', 'https://gerrit.fd.io/r')).toThrow()
    expect(() => gerritProjectName('https://gerrit.fd.io/r/', 'https://gerrit.fd.io/r')).toThrow()
  })
})

describe('isRetiredGerritProject', () => {
  const body = (state: string) => `)]}'\n${JSON.stringify({ id: 'vpp', name: 'vpp', state })}`

  it('treats read-only and hidden projects as retired', () => {
    expect(isRetiredGerritProject(body('READ_ONLY'))).toBe(true)
    expect(isRetiredGerritProject(body('HIDDEN'))).toBe(true)
  })

  it('treats active projects as not retired', () => {
    expect(isRetiredGerritProject(body('ACTIVE'))).toBe(false)
  })

  it('parses a body without the XSSI prefix', () => {
    expect(isRetiredGerritProject(JSON.stringify({ state: 'READ_ONLY' }))).toBe(true)
  })
})
