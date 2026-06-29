import { describe, expect, it } from 'vitest'

import { resolveRegistryBaseUrl, resolveRegistryPageUrl } from '../registry'

describe('resolveRegistryBaseUrl', () => {
  it('returns Google Maven for androidx namespace', () => {
    expect(resolveRegistryBaseUrl('androidx.annotation')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Google Maven for com.google.android.gms sub-namespace', () => {
    expect(resolveRegistryBaseUrl('com.google.android.gms')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Maven Central for bare com.google.android (legacy SDK stubs on Central)', () => {
    expect(resolveRegistryBaseUrl('com.google.android')).toBe('https://repo1.maven.org/maven2')
  })

  it('returns Google Maven for android.arch (pre-AndroidX Architecture Components)', () => {
    expect(resolveRegistryBaseUrl('android.arch.core')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Google Maven for com.android.support (pre-AndroidX support library)', () => {
    expect(resolveRegistryBaseUrl('com.android.support')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Google Maven for com.google.testing.platform', () => {
    expect(resolveRegistryBaseUrl('com.google.testing.platform')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Google Maven for com.google.firebase namespace', () => {
    expect(resolveRegistryBaseUrl('com.google.firebase')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Google Maven for com.google.mlkit namespace', () => {
    expect(resolveRegistryBaseUrl('com.google.mlkit')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Gradle Plugin Portal for gradle.plugin namespace', () => {
    expect(resolveRegistryBaseUrl('gradle.plugin.name.remal')).toBe('https://plugins.gradle.org/m2')
  })

  it('returns Jenkins repo for org.kohsuke.stapler (Jenkins HTTP framework)', () => {
    expect(resolveRegistryBaseUrl('org.kohsuke.stapler')).toBe('https://repo.jenkins-ci.org/public')
  })

  it('returns Jenkins repo for org.jenkins-ci namespace', () => {
    expect(resolveRegistryBaseUrl('org.jenkins-ci.main')).toBe('https://repo.jenkins-ci.org/public')
  })

  it('returns Jenkins repo for io.jenkins.plugins namespace', () => {
    expect(resolveRegistryBaseUrl('io.jenkins.plugins')).toBe('https://repo.jenkins-ci.org/public')
  })

  it('returns Jenkins repo for io.jenkins.blueocean namespace', () => {
    expect(resolveRegistryBaseUrl('io.jenkins.blueocean')).toBe(
      'https://repo.jenkins-ci.org/public',
    )
  })

  it('returns Maven Central for io.jenkins.tools (publishes on Central, not Jenkins repo)', () => {
    expect(resolveRegistryBaseUrl('io.jenkins.tools')).toBe('https://repo1.maven.org/maven2')
  })

  it('returns Maven Central for unknown namespace', () => {
    expect(resolveRegistryBaseUrl('org.apache.commons')).toBe('https://repo1.maven.org/maven2')
  })

  it('returns Google Maven for com.android.tools.adblib (Android Debug Bridge Library)', () => {
    expect(resolveRegistryBaseUrl('com.android.tools.adblib')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Google Maven for com.android.tools.build (Android Gradle Plugin)', () => {
    expect(resolveRegistryBaseUrl('com.android.tools.build')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Google Maven for com.android.tools.build.jetifier (AndroidX migration tool)', () => {
    expect(resolveRegistryBaseUrl('com.android.tools.build.jetifier')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Google Maven for com.android.tools.utp (Android Unified Test Platform)', () => {
    expect(resolveRegistryBaseUrl('com.android.tools.utp')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Maven Central for bare com.android.tools (ddmlib, lint-api publish on Central)', () => {
    expect(resolveRegistryBaseUrl('com.android.tools')).toBe('https://repo1.maven.org/maven2')
  })

  it('returns Google Maven for com.android.identity', () => {
    expect(resolveRegistryBaseUrl('com.android.identity')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Google Maven for com.google.mediapipe', () => {
    expect(resolveRegistryBaseUrl('com.google.mediapipe')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('returns Jenkins repo for org.jenkinsci.plugins', () => {
    expect(resolveRegistryBaseUrl('org.jenkinsci.plugins')).toBe(
      'https://repo.jenkins-ci.org/public',
    )
  })

  it('returns Jenkins repo for com.cloudbees.plugins', () => {
    expect(resolveRegistryBaseUrl('com.cloudbees.plugins')).toBe(
      'https://repo.jenkins-ci.org/public',
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
