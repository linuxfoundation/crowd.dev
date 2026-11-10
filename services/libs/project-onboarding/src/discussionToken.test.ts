import { createAppAuth } from '@octokit/auth-app'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { GithubPublicClientError } from '@crowd/common'

import {
  mintInstallationToken,
  readGithubIssueReporterApp,
  resolveDiscussionToken,
} from './discussionToken'

vi.mock('@octokit/auth-app', () => ({ createAppAuth: vi.fn() }))

const PEM = '-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----'

function stubApp(appId: string, privateKey = Buffer.from(PEM).toString('base64')) {
  vi.stubEnv('CROWD_GITHUB_ISSUE_REPORTER_APP_ID', appId)
  vi.stubEnv('CROWD_GITHUB_ISSUE_REPORTER_PRIVATE_KEY', privateKey)
  vi.stubEnv('CROWD_GITHUB_ISSUE_REPORTER_INSTALLATION_ID', '777')
}

function clearApp() {
  vi.stubEnv('CROWD_GITHUB_ISSUE_REPORTER_APP_ID', '')
  vi.stubEnv('CROWD_GITHUB_ISSUE_REPORTER_PRIVATE_KEY', '')
  vi.stubEnv('CROWD_GITHUB_ISSUE_REPORTER_INSTALLATION_ID', '')
}

beforeEach(() => {
  vi.mocked(createAppAuth).mockReset()
})

afterEach(() => {
  vi.unstubAllEnvs()
})

describe('readGithubIssueReporterApp', () => {
  it('returns null when no app variable is set', () => {
    clearApp()
    expect(readGithubIssueReporterApp()).toBeNull()
  })

  it('decodes a base64 key and reads the installation id as a number', () => {
    stubApp('4375913')
    expect(readGithubIssueReporterApp()).toEqual({
      appId: '4375913',
      privateKey: PEM,
      installationId: 777,
    })
  })

  it('restores the newlines of a key stored on one line', () => {
    stubApp('4375913', PEM.replace(/\n/g, '\\n'))
    expect(readGithubIssueReporterApp()?.privateKey).toBe(PEM)
  })

  it('names the missing variables when the configuration is partial', () => {
    stubApp('4375913')
    vi.stubEnv('CROWD_GITHUB_ISSUE_REPORTER_PRIVATE_KEY', '')
    expect(() => readGithubIssueReporterApp()).toThrow(
      /Missing CROWD_GITHUB_ISSUE_REPORTER_PRIVATE_KEY/,
    )
  })

  it('rejects an installation id that is not a positive integer', () => {
    stubApp('4375913')
    vi.stubEnv('CROWD_GITHUB_ISSUE_REPORTER_INSTALLATION_ID', 'abc')
    expect(() => readGithubIssueReporterApp()).toThrow(GithubPublicClientError)
  })
})

describe('mintInstallationToken', () => {
  it('creates the app auth once and asks for an installation token', async () => {
    const auth = vi.fn().mockResolvedValue({ token: 'ghs_1' })
    vi.mocked(createAppAuth).mockReturnValue(auth as never)
    const app = { appId: 'app-once', privateKey: PEM, installationId: 777 }

    expect(await mintInstallationToken(app)).toBe('ghs_1')
    expect(await mintInstallationToken(app)).toBe('ghs_1')

    expect(createAppAuth).toHaveBeenCalledTimes(1)
    expect(createAppAuth).toHaveBeenCalledWith({ appId: 'app-once', privateKey: PEM })
    expect(auth).toHaveBeenCalledWith({ type: 'installation', installationId: 777 })
  })

  it('maps a rejected credential to an auth error and a missing installation to a config error', async () => {
    const auth = vi
      .fn()
      .mockRejectedValueOnce(Object.assign(new Error('Bad credentials'), { status: 401 }))
      .mockRejectedValueOnce(Object.assign(new Error('Not Found'), { status: 404 }))
      .mockRejectedValueOnce(new Error('socket hang up'))
    vi.mocked(createAppAuth).mockReturnValue(auth as never)
    const app = { appId: 'app-errors', privateKey: PEM, installationId: 777 }

    await expect(mintInstallationToken(app)).rejects.toMatchObject({ kind: 'AUTH' })
    await expect(mintInstallationToken(app)).rejects.toMatchObject({ kind: 'CONFIG' })
    await expect(mintInstallationToken(app)).rejects.toThrow('socket hang up')
  })
})

describe('resolveDiscussionToken', () => {
  it('mints an installation token when the app is configured', async () => {
    stubApp('4375913')
    const mint = vi.fn().mockResolvedValue('ghs_app')
    expect(await resolveDiscussionToken(mint)).toBe('ghs_app')
    expect(mint).toHaveBeenCalledWith(expect.objectContaining({ installationId: 777 }))
  })

  it('falls back to the static token when the app is not configured', async () => {
    clearApp()
    vi.stubEnv('CROWD_GITHUB_DISCUSSIONS_TOKEN', 'static-token')
    const mint = vi.fn()
    expect(await resolveDiscussionToken(mint)).toBe('static-token')
    expect(mint).not.toHaveBeenCalled()
  })
})
