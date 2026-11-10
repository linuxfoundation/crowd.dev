import { createAppAuth } from '@octokit/auth-app'

import { GithubPublicClientError, getErrorMessage, getGithubDiscussionsToken } from '@crowd/common'

const APP_ID_VAR = 'CROWD_GITHUB_ISSUE_REPORTER_APP_ID'
const PRIVATE_KEY_VAR = 'CROWD_GITHUB_ISSUE_REPORTER_PRIVATE_KEY'
const INSTALLATION_ID_VAR = 'CROWD_GITHUB_ISSUE_REPORTER_INSTALLATION_ID'
const APP_VARS = [APP_ID_VAR, PRIVATE_KEY_VAR, INSTALLATION_ID_VAR] as const
const INSTALLATION_TOKEN_TIMEOUT_MS = 10_000

export interface IGithubAppInstallation {
  appId: string
  privateKey: string
  installationId: number
}

export type InstallationTokenMinter = (app: IGithubAppInstallation) => Promise<string>

type AppAuth = ReturnType<typeof createAppAuth>

const appAuths = new Map<string, AppAuth>()

function normalizePrivateKey(rawKey: string): string {
  return rawKey.includes('BEGIN')
    ? rawKey.replace(/\\n/g, '\n')
    : Buffer.from(rawKey, 'base64').toString('utf8')
}

function configError(message: string): GithubPublicClientError {
  return new GithubPublicClientError('CONFIG', message)
}

export function readGithubIssueReporterApp(): IGithubAppInstallation | null {
  const values = APP_VARS.map((name) => process.env[name]?.trim() || null)
  const missing = APP_VARS.filter((_, index) => values[index] === null)
  if (missing.length === APP_VARS.length) {
    return null
  }
  if (missing.length > 0) {
    throw configError(
      `Missing ${missing.join(', ')} for the GitHub App that comments on discussions`,
    )
  }
  const [appId, privateKey, rawInstallationId] = values as [string, string, string]
  const installationId = Number(rawInstallationId)
  if (!Number.isInteger(installationId) || installationId <= 0) {
    throw configError(`${INSTALLATION_ID_VAR} must be a positive integer`)
  }
  return { appId, privateKey: normalizePrivateKey(privateKey), installationId }
}

function appAuthFor(app: IGithubAppInstallation): AppAuth {
  const key = `${app.appId}:${app.installationId}`
  const cached = appAuths.get(key)
  if (cached) {
    return cached
  }
  const auth = createAppAuth({ appId: app.appId, privateKey: app.privateKey })
  appAuths.set(key, auth)
  return auth
}

// @octokit/request wraps network failures in a RequestError with a status; only the JWT
// signing of an unusable private key rejects without one.
function installationTokenError(err: unknown, app: IGithubAppInstallation): unknown {
  if (err instanceof GithubPublicClientError) {
    return err
  }
  const status = (err as { status?: number }).status
  if (status === undefined) {
    return configError(
      `${PRIVATE_KEY_VAR} is not a usable key for GitHub App ${app.appId}: ${getErrorMessage(err)}`,
    )
  }
  if (status === 401 || status === 403) {
    return new GithubPublicClientError(
      'AUTH',
      `GitHub rejected the credentials of app ${app.appId} (HTTP ${status})`,
    )
  }
  if (status === 404) {
    return configError(`Installation ${app.installationId} of GitHub App ${app.appId} not found`)
  }
  return err
}

function withTimeout<T>(promise: Promise<T>, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  const expiry = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () =>
        reject(
          new GithubPublicClientError(
            'TRANSIENT',
            `Timed out after ${INSTALLATION_TOKEN_TIMEOUT_MS}ms ${what}`,
          ),
        ),
      INSTALLATION_TOKEN_TIMEOUT_MS,
    )
  })
  return Promise.race([promise, expiry]).finally(() => clearTimeout(timer))
}

export async function mintInstallationToken(app: IGithubAppInstallation): Promise<string> {
  try {
    const { token } = await withTimeout(
      appAuthFor(app)({ type: 'installation', installationId: app.installationId }),
      `minting an installation token for GitHub App ${app.appId}`,
    )
    return token
  } catch (err) {
    throw installationTokenError(err, app)
  }
}

export async function resolveDiscussionToken(
  mint: InstallationTokenMinter = mintInstallationToken,
): Promise<string> {
  const app = readGithubIssueReporterApp()
  return app ? mint(app) : getGithubDiscussionsToken()
}
