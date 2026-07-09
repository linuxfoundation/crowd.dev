import { describe, expect, it } from 'vitest'

import { interpolateProperties, normalizeScmUrl } from '../extract'
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

  it('returns null when an unresolved placeholder is embedded in the repo path', () => {
    // Would otherwise parse to github.com/owner/%7BartifactId%7D and slip through.
    expect(normalizeScmUrl('https://github.com/owner/${artifactId}')).toBeNull()
    expect(normalizeScmUrl('scm:git:https://github.com/${owner}/repo.git')).toBeNull()
  })

  it('ignores an unresolved placeholder in a trailing suffix once owner/repo are valid', () => {
    // ${project.scm.tag} lands in the /tree/<ref> suffix, which is already ignored —
    // only segments[0]/[1] (owner/repo) are inspected.
    expect(
      normalizeScmUrl('https://github.com/apache/httpcomponents-client/tree/${project.scm.tag}'),
    ).toBe('https://github.com/apache/httpcomponents-client')
    expect(normalizeScmUrl('https://github.com/apache/maven/tree/${project.scm.tag}')).toBe(
      'https://github.com/apache/maven',
    )
  })

  it('recovers Apache gitweb hosts (gitbox/git-wip-us/git.apache.org) in path form', () => {
    expect(normalizeScmUrl('https://gitbox.apache.org/repos/asf/commons-lang.git')).toBe(
      'https://gitbox.apache.org/repos/asf/commons-lang',
    )
    expect(normalizeScmUrl('https://git.apache.org/repos/asf/ant.git')).toBe(
      'https://git.apache.org/repos/asf/ant',
    )
  })

  it('recovers Apache gitweb hosts in classic ?p= query-string form', () => {
    expect(normalizeScmUrl('https://gitbox.apache.org/repos/asf?p=commons-io.git')).toBe(
      'https://gitbox.apache.org/repos/asf/commons-io',
    )
    expect(normalizeScmUrl('https://git-wip-us.apache.org/repos/asf?p=commons-math.git')).toBe(
      'https://git-wip-us.apache.org/repos/asf/commons-math',
    )
  })

  it('returns null for Apache gitweb hosts with no recoverable repo name', () => {
    expect(normalizeScmUrl('https://gitbox.apache.org/')).toBeNull()
    expect(normalizeScmUrl('https://gitbox.apache.org/repos/asf/')).toBeNull()
    expect(normalizeScmUrl('https://gitbox.apache.org/some/other/path')).toBeNull()
  })

  it('recovers git.eclipse.org cgit URLs, keeping the /c/ prefix and dropping trailing tree paths', () => {
    expect(normalizeScmUrl('http://git.eclipse.org/c/eclipselink/javax.persistence.git')).toBe(
      'https://git.eclipse.org/c/eclipselink/javax.persistence',
    )
    expect(
      normalizeScmUrl('https://git.eclipse.org/c/eclipsescada/org.eclipse.scada.utils.git'),
    ).toBe('https://git.eclipse.org/c/eclipsescada/org.eclipse.scada.utils')
    expect(
      normalizeScmUrl('http://git.eclipse.org/c/jetty/org.eclipse.jetty.project.git/tree'),
    ).toBe('https://git.eclipse.org/c/jetty/org.eclipse.jetty.project')
    expect(
      normalizeScmUrl(
        'http://git.eclipse.org/c/jetty/org.eclipse.jetty.orbit.git/tree/jetty-orbit',
      ),
    ).toBe('https://git.eclipse.org/c/jetty/org.eclipse.jetty.orbit')
  })

  it('returns null for git.eclipse.org URLs with no recoverable repo path', () => {
    expect(normalizeScmUrl('https://git.eclipse.org/')).toBeNull()
    expect(normalizeScmUrl('https://git.eclipse.org/c/')).toBeNull()
    expect(normalizeScmUrl('https://git.eclipse.org/c/onlyowner')).toBeNull()
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

  it('recovers git://host:owner/repo SCP colon form', () => {
    expect(normalizeScmUrl('git://github.com:appendium/objectlabkit.git')).toBe(
      'https://github.com/appendium/objectlabkit',
    )
  })

  it('accepts allowlisted self-hosted GitLab/Gitea hosts', () => {
    expect(normalizeScmUrl('https://git.neckar.it/neckarit/neckar-hub')).toBe(
      'https://git.neckar.it/neckarit/neckar-hub',
    )
    expect(normalizeScmUrl('scm:git:https://gitlab.inria.fr/owner/repo.git')).toBe(
      'https://gitlab.inria.fr/owner/repo',
    )
    expect(normalizeScmUrl('https://git.iem.at/owner/repo')).toBe('https://git.iem.at/owner/repo')
  })

  it('still returns null for internal or non-allowlisted hosts', () => {
    expect(normalizeScmUrl('https://git.corp.adobe.com/team/project')).toBeNull()
    expect(normalizeScmUrl('https://gitlab.alibaba-inc.com/team/project')).toBeNull()
    expect(normalizeScmUrl('https://android.googlesource.com/platform/tools/base')).toBeNull()
  })
})

describe('interpolateProperties', () => {
  it('resolves a single ${...} placeholder from properties', () => {
    expect(
      interpolateProperties('${scm.github.url}', {
        'scm.github.url': 'https://github.com/owner/repo',
      }),
    ).toBe('https://github.com/owner/repo')
  })

  it('resolves multiple placeholders in one string', () => {
    expect(
      interpolateProperties('https://gitlab.com/${projectPath}', {
        projectPath: 'group/project',
      }),
    ).toBe('https://gitlab.com/group/project')
  })

  it('resolves nested placeholders recursively', () => {
    expect(
      interpolateProperties('${scm.url}', {
        'scm.url': '${scm.base}/repo',
        'scm.base': 'https://github.com/owner',
      }),
    ).toBe('https://github.com/owner/repo')
  })

  it('resolves built-in project.* style placeholders', () => {
    expect(
      interpolateProperties('https://github.com/acme/${project.artifactId}', {
        'project.artifactId': 'my-lib',
      }),
    ).toBe('https://github.com/acme/my-lib')
  })

  it('leaves unresolved placeholders literal (missing property / method call)', () => {
    expect(interpolateProperties('${scm.github.url}', {})).toBe('${scm.github.url}')
    expect(
      interpolateProperties('${pom.artifactId.substring(8)}', { 'pom.artifactId': 'foo' }),
    ).toBe('${pom.artifactId.substring(8)}')
  })

  it('does not loop forever on self-referential placeholders', () => {
    expect(interpolateProperties('${a}', { a: '${b}', b: '${a}' })).toContain('${')
  })

  it('composes with the normalizer end-to-end', () => {
    const resolved = interpolateProperties('${scm.github.url}', {
      'scm.github.url': 'scm:git:https://github.com/Owner/Repo.git',
    })
    expect(normalizeScmUrl(resolved)).toBe('https://github.com/owner/repo')
  })
})
