import axios from 'axios'

import { ICache } from '@crowd/types'
import { RateLimitError } from '@crowd/types'

interface TokenInfo {
  token: string
  remaining: number
  reset: number
}

export enum GithubAPIResource {
  GRAPHQL = 'graphql',
  CORE = 'core',
  SEARCH = 'search',
  SOURCE_IMPORT = 'source_import',
  INTEGRATION_MANIFEST = 'integration_manifest',
  CODE_SCANNING_UPLOAD = 'code_scanning_upload',
  ACTIONS_RUNNER_REGISTRATION = 'actions_runner_registration',
  SCIM = 'scim',
  AUDIT_LOG = 'audit_log',
  CODE_SEARCH = 'code_search',
}

export class GithubTokenRotator {
  static CACHE_KEY = 'integration-cache:github-token-rotator:tokens'
  constructor(
    private cache: ICache,
    private tokens: string[],
  ) {
    this.tokens = tokens ? [...new Set(tokens)] : []
    if (this.tokens.length > 0) {
      this.initializeTokens()
    }
  }

  private async initializeTokens(): Promise<void> {
    for (const token of this.tokens) {
      const tokenInfo: TokenInfo = JSON.parse(
        await this.cache.hget(GithubTokenRotator.CACHE_KEY, token),
      )
      if (!tokenInfo) {
        const newTokenInfo: TokenInfo = {
          token,
          remaining: 0,
          reset: 0,
        }
        await this.cache.hset(GithubTokenRotator.CACHE_KEY, token, JSON.stringify(newTokenInfo))
      }
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  public async getToken(integrationId?: string, err?: any): Promise<string | null> {
    const tokens = await this.cache.hgetall(GithubTokenRotator.CACHE_KEY)
    let minResetTime = Infinity

    for (const token in tokens) {
      const tokenInfo: TokenInfo = JSON.parse(tokens[token])
      if (tokenInfo.remaining > 0 || tokenInfo.reset < Math.floor(Date.now() / 1000)) {
        await this.cache.hset(GithubTokenRotator.CACHE_KEY, token, JSON.stringify(tokenInfo))
        return token
      }
      minResetTime = Math.min(minResetTime, tokenInfo.reset)
    }

    const waitTime =
      minResetTime - Math.floor(Date.now() / 1000) + Math.floor(Math.random() * 120) + 60

    // if we have to wait less than 60 seconds, let's wait and try again
    if (waitTime <= 60) {
      return new Promise((resolve) => {
        setTimeout(async () => {
          resolve(await this.getToken(integrationId))
        }, waitTime * 1000)
      })
    }

    // no tokens available, throwing error
    if (err) {
      throw err
    } else {
      throw new RateLimitError(
        waitTime,
        'token-rotator',
        `No tokens available for integration ${integrationId}, please try again later.`,
      )
    }
  }

  public async updateTokenInfo(token: string, remaining: number, reset: number): Promise<void> {
    if (this.tokens.length === 0) {
      throw new Error('No tokens configured in token rotator')
    }

    const tokenInfo: TokenInfo = JSON.parse(
      (await this.cache.hget(GithubTokenRotator.CACHE_KEY, token)) || '',
    )
    if (tokenInfo) {
      tokenInfo.remaining = remaining
      tokenInfo.reset = reset
      await this.cache.hset(GithubTokenRotator.CACHE_KEY, token, JSON.stringify(tokenInfo))
    }
  }

  public async updateRateLimitInfoFromApi(
    token: string,
    resource: GithubAPIResource = GithubAPIResource.GRAPHQL,
  ): Promise<void> {
    if (this.tokens.length === 0) {
      throw new Error('No tokens configured in token rotator')
    }

    // let's make API call to get the latest rate limit info
    const tokenInfo: TokenInfo = JSON.parse(
      (await this.cache.hget(GithubTokenRotator.CACHE_KEY, token)) || '',
    )
    if (tokenInfo) {
      const response = await axios({
        url: 'https://api.github.com/rate_limit',
        method: 'get',
        headers: { Authorization: `token ${token}` },
      })

      const remaining = parseInt(response.data.resources[resource].remaining)
      const reset = parseInt(response.data.resources[resource].reset)
      await this.updateTokenInfo(token, remaining, reset)
    }
  }

  public async getAllTokens(): Promise<string[]> {
    return this.tokens
  }

  public async removeToken(token: string): Promise<void> {
    this.tokens = this.tokens.filter((t) => t !== token)
    await this.initializeTokens()
  }
}
