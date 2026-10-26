import { timeout } from '@crowd/common'
import type { Logger } from '@crowd/logging'

import { summarizeBody } from '../../http/client'
import type { ConnectorHttp } from '../../http/client'
import { ProviderAccessDeniedError, ProviderContractError } from '../../http/errors'
import { isOrgPolicyRefusalMessage } from './interpret'

interface GraphqlEnvelope<T> {
  data?: T
  errors?: { type?: string; message?: string }[]
}

const NO_DATA_MAX_ATTEMPTS = 5
const NO_DATA_BACKOFF_MS = 2000

function hasResolvedRootField(data: unknown): boolean {
  return (
    typeof data === 'object' &&
    data !== null &&
    Object.values(data).some((value) => value !== null && value !== undefined)
  )
}

export async function githubGraphql<T>(
  http: ConnectorHttp,
  query: string,
  variables: Record<string, unknown>,
  log: Logger,
  maxAttempts?: number,
): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    const body = await http.request<GraphqlEnvelope<T>>(
      {
        method: 'post',
        url: 'https://api.github.com/graphql',
        data: { query, variables },
      },
      log,
      maxAttempts,
    )
    const isForbidden = body.errors?.some((e) => e.type?.includes('FORBIDDEN')) ?? false
    if (body.errors?.length) {
      const details = body.errors.map((e) => `${e.type ?? 'ERROR'}: ${e.message ?? ''}`).join('; ')
      if (isForbidden) {
        log.warn({ errors: body.errors }, `github graphql errors: ${details}`)
      } else {
        log.error({ errors: body.errors }, `github graphql errors: ${details}`)
      }
    }
    if (body.data && (!body.errors?.length || hasResolvedRootField(body.data))) {
      return body.data
    }
    const context = {
      request: {
        method: 'POST',
        url: 'https://api.github.com/graphql',
        body: summarizeBody(variables),
      },
      response: { status: 200, body: summarizeBody(body.errors) },
    }
    const policyRefusal = body.errors?.find(
      (e) => e.type?.includes('FORBIDDEN') && isOrgPolicyRefusalMessage(e.message),
    )
    if (policyRefusal) {
      throw new ProviderAccessDeniedError(
        `github access denied: ${policyRefusal.message}`,
      ).withContext(context)
    }
    if (isForbidden || attempt >= NO_DATA_MAX_ATTEMPTS) {
      throw new ProviderContractError('github graphql response has no data').withContext(context)
    }
    log.warn({ attempt }, 'github graphql empty data response, retrying')
    await timeout(NO_DATA_BACKOFF_MS)
  }
}
