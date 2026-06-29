import { describe, expect, it } from 'vitest'

import { resolveRegistryBaseUrl, resolveRegistryPageUrl } from '../registry'

describe('resolveRegistryBaseUrl', () => {
  it('returns Google Maven for androidx namespace', () => {
    expect(resolveRegistryBaseUrl('androidx.annotation')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Google Maven for com.google.android namespace', () => {
    expect(resolveRegistryBaseUrl('com.google.android.gms')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Google Maven for com.google.firebase namespace', () => {
    expect(resolveRegistryBaseUrl('com.google.firebase')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Gradle Plugin Portal for gradle.plugin namespace', () => {
    expect(resolveRegistryBaseUrl('gradle.plugin.name.remal')).toBe(
      'https://plugins.gradle.org/m2',
    )
  })

  it('returns Jenkins repo for org.jenkins-ci namespace', () => {
    expect(resolveRegistryBaseUrl('org.jenkins-ci.main')).toBe(
      'https://repo.jenkins-ci.org/public',
    )
  })

  it('returns Jenkins repo for io.jenkins namespace', () => {
    expect(resolveRegistryBaseUrl('io.jenkins.plugins')).toBe(
      'https://repo.jenkins-ci.org/public',
    )
  })

  it('returns Maven Central for unknown namespace', () => {
    expect(resolveRegistryBaseUrl('org.apache.commons')).toBe(
      'https://repo1.maven.org/maven2',
    )
  })

  it('does not match androidx prefix for unrelated namespace', () => {
    expect(resolveRegistryBaseUrl('com.android.tools')).toBe(
      'https://repo1.maven.org/maven2',
    )
  })
})

describe('resolveRegistryPageUrl', () => {
  it('returns Google Maven browse URL for androidx', () => {
    expect(resolveRegistryPageUrl('androidx.annotation', 'annotation')).toBe(
      'https://maven.google.com/web/index.html#androidx.annotation:annotation',
    )
  })

  it('returns Gradle Plugin Portal URL for gradle.plugin', () => {
    expect(resolveRegistryPageUrl('gradle.plugin.name.remal', 'gradle-plugins')).toBe(
      'https://plugins.gradle.org/m2/gradle/plugin/name/remal/gradle-plugins/',
    )
  })

  it('returns Jenkins browse URL for org.jenkins-ci', () => {
    expect(resolveRegistryPageUrl('org.jenkins-ci.main', 'jenkins-core')).toBe(
      'https://repo.jenkins-ci.org/public/org/jenkins-ci/main/jenkins-core/',
    )
  })

  it('returns Maven Central URL for unknown namespace', () => {
    expect(resolveRegistryPageUrl('org.apache.commons', 'commons-lang3')).toBe(
      'https://central.sonatype.com/artifact/org.apache.commons/commons-lang3',
    )
  })
})
