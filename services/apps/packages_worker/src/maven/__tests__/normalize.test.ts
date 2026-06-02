import { describe, expect, it } from 'vitest'

import { normalizeScmUrl } from '../extract'
import { isPrerelease, parseRepoUrl } from '../normalize'

describe('isPrerelease', () => {
  it('returns false for a stable version', () => {
    expect(isPrerelease('3.12.0')).toBe(false)
  })

  it('detects SNAPSHOT', () => {
    expect(isPrerelease('1.0.0-SNAPSHOT')).toBe(true)
  })

  it('detects alpha', () => {
    expect(isPrerelease('2.0.0-alpha')).toBe(true)
    expect(isPrerelease('2.0.0-ALPHA.1')).toBe(true)
  })

  it('detects beta', () => {
    expect(isPrerelease('1.5.0-beta.2')).toBe(true)
  })

  it('detects rc', () => {
    expect(isPrerelease('4.0.0-rc1')).toBe(true)
    expect(isPrerelease('4.0.0-RC.2')).toBe(true)
  })

  it('detects milestone (m1, m10)', () => {
    expect(isPrerelease('5.3.0-m1')).toBe(true)
    expect(isPrerelease('5.3.0-M10')).toBe(true)
  })

  it('returns false for versions with numbers that are not milestones', () => {
    expect(isPrerelease('1.2.3')).toBe(false)
    expect(isPrerelease('10.0.0')).toBe(false)
  })
})

describe('parseRepoUrl', () => {
  it('identifies github.com', () => {
    expect(parseRepoUrl('https://github.com/apache/commons-lang')).toEqual({
      host: 'github',
      owner: 'apache',
      name: 'commons-lang',
    })
  })

  it('identifies gitlab.com', () => {
    expect(parseRepoUrl('https://gitlab.com/owner/repo')).toEqual({
      host: 'gitlab',
      owner: 'owner',
      name: 'repo',
    })
  })

  it('identifies bitbucket.org', () => {
    expect(parseRepoUrl('https://bitbucket.org/owner/repo')).toEqual({
      host: 'bitbucket',
      owner: 'owner',
      name: 'repo',
    })
  })

  it('returns other for unknown hosts', () => {
    const result = parseRepoUrl('https://svn.example.com/repo')
    expect(result?.host).toBe('other')
  })

  it('returns null for invalid URLs', () => {
    expect(parseRepoUrl('not-a-url')).toBeNull()
  })

  it('handles URLs with no path segments', () => {
    const result = parseRepoUrl('https://github.com/')
    expect(result).toEqual({ host: 'github', owner: null, name: null })
  })
})

describe('normalizeScmUrl', () => {
  it('returns null for null input', () => {
    expect(normalizeScmUrl(null)).toBeNull()
  })

  it('strips scm:git: prefix', () => {
    expect(normalizeScmUrl('scm:git:https://github.com/apache/commons-lang')).toBe(
      'https://github.com/apache/commons-lang',
    )
  })

  it('converts SSH git@ to https', () => {
    expect(normalizeScmUrl('git@github.com:apache/commons-lang.git')).toBe(
      'https://github.com/apache/commons-lang',
    )
  })

  it('converts git:// to https://', () => {
    expect(normalizeScmUrl('git://github.com/apache/commons-lang.git')).toBe(
      'https://github.com/apache/commons-lang',
    )
  })

  it('strips trailing .git', () => {
    expect(normalizeScmUrl('https://github.com/apache/commons-lang.git')).toBe(
      'https://github.com/apache/commons-lang',
    )
  })

  it('strips /tree/... path suffix', () => {
    expect(normalizeScmUrl('https://github.com/apache/commons-lang/tree/master')).toBe(
      'https://github.com/apache/commons-lang',
    )
  })

  it('strips trailing slash', () => {
    expect(normalizeScmUrl('https://github.com/apache/commons-lang/')).toBe(
      'https://github.com/apache/commons-lang',
    )
  })

  it('handles combined scm:git: + SSH form', () => {
    expect(normalizeScmUrl('scm:git:git@github.com:apache/commons-lang.git')).toBe(
      'https://github.com/apache/commons-lang',
    )
  })

  it('returns null for non-https result', () => {
    expect(normalizeScmUrl('svn://svn.apache.org/repos/commons-lang')).toBeNull()
  })
})
