import { describe, expect, it } from 'vitest'

import { registrableDomain } from './domain'

describe('registrableDomain', () => {
  it('strips subdomains from hosts and URLs', () => {
    expect(registrableDomain('wiki.opendaylight.org')).toBe('opendaylight.org')
    expect(registrableDomain('https://wiki.opendaylight.org/view/Main')).toBe('opendaylight.org')
  })

  it('respects multi-part public suffixes', () => {
    expect(registrableDomain('docs.example.co.uk')).toBe('example.co.uk')
  })

  it('treats private suffixes as separate sites', () => {
    expect(registrableDomain('https://foo.github.io/repo/')).toBe('foo.github.io')
    expect(registrableDomain('x.readthedocs.io')).toBe('x.readthedocs.io')
  })

  it('lowercases the result', () => {
    expect(registrableDomain('https://Docs.Example.ORG')).toBe('example.org')
  })

  it('returns null for IPs and invalid input', () => {
    expect(registrableDomain('http://127.0.0.1:8080')).toBeNull()
    expect(registrableDomain('localhost')).toBeNull()
    expect(registrableDomain('')).toBeNull()
  })
})
