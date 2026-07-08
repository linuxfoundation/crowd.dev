import { describe, expect, it } from 'vitest'

import {
  GRADLE_PLUGIN_PORTAL_BASE_URL,
  JITPACK_BASE_URL,
  MAVEN_CENTRAL_BASE_URL,
  isJitpackNamespace,
  jitpackPageUrl,
  resolveRegistryBaseUrl,
  resolveRegistryPageUrl,
  resolveRegistryPageUrlFromBase,
} from '../registry'

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

  it('returns Google Maven for bare com.google.android (legacy Android SDK stubs)', () => {
    expect(resolveRegistryBaseUrl('com.google.android')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
  })

  it('does not match com.google.androidx — prefix boundary must end at a dot', () => {
    // com.google.android prefix must not bleed into unrelated com.google.android* strings
    expect(resolveRegistryBaseUrl('com.google.androidx')).toBe(MAVEN_CENTRAL_BASE_URL)
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

  it('returns Google Maven for com.android.tools.analytics-library (Android Studio analytics)', () => {
    expect(resolveRegistryBaseUrl('com.android.tools.analytics-library')).toBe(
      'https://dl.google.com/dl/android/maven2',
    )
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

  it('returns Maven Central for io.github namespace (JitPack is a fallback, not primary)', () => {
    expect(resolveRegistryBaseUrl('io.github.resilience4j')).toBe(MAVEN_CENTRAL_BASE_URL)
  })

  it('returns Maven Central for bare io.github', () => {
    expect(resolveRegistryBaseUrl('io.github')).toBe(MAVEN_CENTRAL_BASE_URL)
  })

  it('returns Maven Central for com.github namespace (JitPack is a fallback, not primary)', () => {
    expect(resolveRegistryBaseUrl('com.github.ben-manes')).toBe(MAVEN_CENTRAL_BASE_URL)
  })

  it('returns Maven Central for io.githubfoo (no dot boundary)', () => {
    expect(resolveRegistryBaseUrl('io.githubfoo')).toBe(MAVEN_CENTRAL_BASE_URL)
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

  it('returns Maven Central URL for io.github (Central is primary)', () => {
    expect(resolveRegistryPageUrl('io.github.resilience4j', 'resilience4j-core')).toBe(
      'https://central.sonatype.com/artifact/io.github.resilience4j/resilience4j-core',
    )
  })

  it('returns Maven Central URL for com.github (Central is primary)', () => {
    expect(resolveRegistryPageUrl('com.github.ben-manes', 'caffeine')).toBe(
      'https://central.sonatype.com/artifact/com.github.ben-manes/caffeine',
    )
  })
})

describe('resolveRegistryPageUrlFromBase', () => {
  it('returns Sonatype Central URL when resolvedBaseUrl is Maven Central', () => {
    expect(
      resolveRegistryPageUrlFromBase(
        'com.google.firebase',
        'firebase-admin',
        MAVEN_CENTRAL_BASE_URL,
      ),
    ).toBe('https://central.sonatype.com/artifact/com.google.firebase/firebase-admin')
  })

  it('returns alternative registry page URL when resolvedBaseUrl is not Central', () => {
    const googleMavenUrl = 'https://dl.google.com/dl/android/maven2'
    expect(
      resolveRegistryPageUrlFromBase('com.google.firebase', 'firebase-analytics', googleMavenUrl),
    ).toBe('https://maven.google.com/web/index.html#com.google.firebase:firebase-analytics')
  })

  it('returns Gradle Plugin Portal path when resolvedBaseUrl is Gradle Plugin Portal', () => {
    expect(
      resolveRegistryPageUrlFromBase(
        'io.github.trueangle',
        'gradle-plugin',
        GRADLE_PLUGIN_PORTAL_BASE_URL,
      ),
    ).toBe('https://plugins.gradle.org/m2/io/github/trueangle/gradle-plugin/')
  })

  it('returns JitPack browse URL when resolvedBaseUrl is JitPack', () => {
    expect(
      resolveRegistryPageUrlFromBase('io.github.trueangle', 'gradle-plugin', JITPACK_BASE_URL),
    ).toBe('https://jitpack.io/#trueangle/gradle-plugin')
  })
})

describe('isJitpackNamespace', () => {
  it('returns true for io.github.* packages', () => {
    expect(isJitpackNamespace('io.github.resilience4j')).toBe(true)
  })

  it('returns true for bare io.github', () => {
    expect(isJitpackNamespace('io.github')).toBe(true)
  })

  it('returns true for com.github.* packages', () => {
    expect(isJitpackNamespace('com.github.ben-manes')).toBe(true)
  })

  it('returns true for bare com.github', () => {
    expect(isJitpackNamespace('com.github')).toBe(true)
  })

  it('returns false for io.githubfoo (no dot boundary)', () => {
    expect(isJitpackNamespace('io.githubfoo')).toBe(false)
  })

  it('returns false for unrelated namespaces', () => {
    expect(isJitpackNamespace('org.apache.commons')).toBe(false)
    expect(isJitpackNamespace('io.jenkins.plugins')).toBe(false)
  })
})

describe('jitpackPageUrl', () => {
  it('strips io.github. prefix from groupId', () => {
    expect(jitpackPageUrl('io.github.resilience4j', 'resilience4j-core')).toBe(
      'https://jitpack.io/#resilience4j/resilience4j-core',
    )
  })

  it('strips com.github. prefix from groupId', () => {
    expect(jitpackPageUrl('com.github.ben-manes', 'caffeine')).toBe(
      'https://jitpack.io/#ben-manes/caffeine',
    )
  })

  it('handles bare io.github with no sub-namespace', () => {
    expect(jitpackPageUrl('io.github', 'some-lib')).toBe('https://jitpack.io/#/some-lib')
  })
})
