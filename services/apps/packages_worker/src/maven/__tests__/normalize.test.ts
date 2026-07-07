import { describe, expect, it } from 'vitest'

import { normalizeScmUrl } from '../extract'
import { pickStableRelease } from '../metadata'
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

  it('detects dev', () => {
    expect(isPrerelease('1.0-dev')).toBe(true)
    expect(isPrerelease('2.0.0-dev')).toBe(true)
  })

  it('returns false for versions with numbers that are not milestones', () => {
    expect(isPrerelease('1.2.3')).toBe(false)
    expect(isPrerelease('10.0.0')).toBe(false)
  })
})

describe('pickStableRelease', () => {
  it('returns stable candidate directly', () => {
    expect(pickStableRelease('2.0.18', ['2.0.17', '2.0.18'])).toBe('2.0.18')
  })

  it('falls back to latest stable in list when candidate is alpha', () => {
    const versions = ['2.0.16', '2.0.18', '2.1.0-alpha1']
    expect(pickStableRelease('2.1.0-alpha1', versions)).toBe('2.0.18')
  })

  it('falls back to latest stable in list when candidate is RC', () => {
    const versions = ['2.3.9', '2.4.0-RC']
    expect(pickStableRelease('2.4.0-RC', versions)).toBe('2.3.9')
  })

  it('falls back to latest stable in list when candidate is null', () => {
    const versions = ['1.0.0', '1.1.0', '1.2.0-beta']
    expect(pickStableRelease(null, versions)).toBe('1.1.0')
  })

  it('returns pre-release candidate when no stable version exists', () => {
    const versions = ['1.0.0-alpha', '1.0.0-beta']
    expect(pickStableRelease('1.0.0-beta', versions)).toBe('1.0.0-beta')
  })

  it('returns null when candidate is null and versions list is empty', () => {
    expect(pickStableRelease(null, [])).toBeNull()
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

  // Gap B — recover repository_url from inputs that were previously dropped
  it('recovers scm:git: without a scheme', () => {
    expect(normalizeScmUrl('scm:git:github.com/lum-ai/nxmlreader')).toBe(
      'https://github.com/lum-ai/nxmlreader',
    )
  })

  it('recovers a bare host/owner/repo without a scheme', () => {
    expect(normalizeScmUrl('github.com/agiledigital/kamon-play-extensions')).toBe(
      'https://github.com/agiledigital/kamon-play-extensions',
    )
  })

  it('upgrades http to https and lower-cases the github path', () => {
    expect(normalizeScmUrl('http://github.com/kevemueller/kTLSH/tree/master')).toBe(
      'https://github.com/kevemueller/ktlsh',
    )
  })

  // Gap C — reject non-repository URLs so they are never stored
  it('returns null for website-only URLs', () => {
    expect(normalizeScmUrl('https://meson.ai/')).toBeNull()
    expect(normalizeScmUrl('http://source.android.com')).toBeNull()
  })

  it('returns null for placeholders and free-form text', () => {
    expect(normalizeScmUrl('Private')).toBeNull()
    expect(normalizeScmUrl('${scm-url}')).toBeNull()
    expect(normalizeScmUrl('http://cvs.sourceforge.net/cgi-bin/viewcvs.cgi/foo')).toBeNull()
  })

  // SCP colon form: "host:owner/repo" where the colon is a path separator, not a port
  it('recovers bare host:owner/repo SCP colon form', () => {
    expect(normalizeScmUrl('github.com:japgolly/scalacss.git')).toBe(
      'https://github.com/japgolly/scalacss',
    )
  })

  it('recovers scheme://host:owner/repo SCP colon form', () => {
    expect(normalizeScmUrl('https://github.com:networknt/light-4j.git')).toBe(
      'https://github.com/networknt/light-4j',
    )
  })

  it('recovers ssh://git@host:owner/repo SCP colon form', () => {
    expect(normalizeScmUrl('ssh://git@github.com:apache/iotdb.git')).toBe(
      'https://github.com/apache/iotdb',
    )
    expect(normalizeScmUrl('ssh://git@bitbucket.org:eci-elements/web-services.git')).toBe(
      'https://bitbucket.org/eci-elements/web-services',
    )
  })

  it('does not treat a numeric port as an SCP separator', () => {
    expect(normalizeScmUrl('https://gitlab.com:443/foo/bar')).toBe('https://gitlab.com/foo/bar')
  })

  it('accepts allowlisted self-hosted GitLab/Gitea hosts', () => {
    expect(normalizeScmUrl('https://git.neckar.it/neckarit/neckar-hub')).toBe(
      'https://git.neckar.it/neckarit/neckar-hub',
    )
    expect(normalizeScmUrl('scm:git:https://gitlab.inria.fr/owner/repo.git')).toBe(
      'https://gitlab.inria.fr/owner/repo',
    )
  })

  it('still returns null for hosts not in the allowlist', () => {
    expect(normalizeScmUrl('https://git.corp.adobe.com/team/project')).toBeNull()
    expect(normalizeScmUrl('https://android.googlesource.com/platform/tools/base')).toBeNull()
  })
})
